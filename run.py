import asyncio
import json
import logging
import os
import sys
import threading
import time
from typing import Any

from flask import Flask, jsonify
from websockets.asyncio.client import connect

DERIV_WS_URL = "wss://ws.derivws.com/websockets/v3?app_id=1089"
SYMBOL = "R_100"
HEALTH_PORT = 8080

PING_INTERVAL_SECONDS = 25
PING_TIMEOUT_SECONDS = 10
AUTH_TIMEOUT_SECONDS = 15
INITIAL_RECONNECT_DELAY_SECONDS = 2
MAX_RECONNECT_DELAY_SECONDS = 60
STABLE_CONNECTION_SECONDS = 60

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("deriv-bot")

app = Flask(__name__)


@app.get("/health")
def health() -> Any:
    return jsonify(status="ok")


class DerivAPIError(RuntimeError):
    """Raised when Deriv returns an API-level error."""


def _format_api_error(message: dict[str, Any]) -> str:
    error = message.get("error")
    if isinstance(error, dict):
        code = error.get("code", "unknown")
        detail = error.get("message", "Deriv returned an API error")
        return f"{code}: {detail}"
    return "Deriv returned an API error"


async def _receive_json(websocket: Any) -> dict[str, Any]:
    raw_message = await asyncio.wait_for(
        websocket.recv(),
        timeout=AUTH_TIMEOUT_SECONDS,
    )
    message = json.loads(raw_message)
    if not isinstance(message, dict):
        raise DerivAPIError("Deriv returned an unexpected response")
    return message


async def _read_events(websocket: Any) -> None:
    async for raw_message in websocket:
        try:
            message = json.loads(raw_message)
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.warning("Received a message that was not valid JSON")
            continue

        if not isinstance(message, dict):
            logger.info("Received a non-object WebSocket event")
            continue

        if "error" in message:
            raise DerivAPIError(_format_api_error(message))

        event_type = message.get("msg_type", "unknown")
        if event_type == "tick":
            tick = message.get("tick")
            quote = tick.get("quote") if isinstance(tick, dict) else None
            if quote is not None:
                logger.info("%s last quote: %s", SYMBOL, quote)
            else:
                logger.warning("Received a tick event without a quote")
        else:
            logger.info("Received Deriv event: %s", event_type)


async def _ping_loop(websocket: Any) -> None:
    while True:
        await asyncio.sleep(PING_INTERVAL_SECONDS)
        pong_waiter = await websocket.ping()
        logger.info("Sent WebSocket ping")
        await asyncio.wait_for(pong_waiter, timeout=PING_TIMEOUT_SECONDS)
        logger.info("Received WebSocket pong")


async def _run_connection(token: str) -> float:
    connected_at = time.monotonic()

    # websockets 15 honors the proxy settings available in the environment.
    # Disabling its built-in keepalive lets the explicit 25-second ping loop
    # control idle traffic instead.
    async with connect(
        DERIV_WS_URL,
        proxy=True,
        ping_interval=None,
        open_timeout=AUTH_TIMEOUT_SECONDS,
        close_timeout=5,
        max_queue=64,
        compression=None,
    ) as websocket:
        logger.info("Connected to Deriv WebSocket")

        await websocket.send(json.dumps({"authorize": token}))
        logger.info("Sent authorization request")
        authorization = await _receive_json(websocket)
        if "error" in authorization:
            raise DerivAPIError(_format_api_error(authorization))
        if authorization.get("msg_type") != "authorize":
            raise DerivAPIError("Deriv did not confirm authorization")
        logger.info("Authorization successful")

        await websocket.send(json.dumps({"ticks": SYMBOL, "subscribe": 1}))
        logger.info("Subscribed to %s ticks", SYMBOL)

        reader = asyncio.create_task(_read_events(websocket), name="deriv-events")
        pinger = asyncio.create_task(_ping_loop(websocket), name="deriv-pings")
        tasks = (reader, pinger)
        try:
            done, _ = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in done:
                task.result()
            raise ConnectionError("WebSocket event loop ended")
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    return time.monotonic() - connected_at


async def run_bot(token: str) -> None:
    reconnect_delay = INITIAL_RECONNECT_DELAY_SECONDS

    while True:
        attempt_started_at = time.monotonic()
        try:
            await _run_connection(token)
            logger.warning("WebSocket session ended")
        except asyncio.CancelledError:
            logger.info("Bot shutdown requested")
            raise
        except Exception as error:
            logger.warning("WebSocket session failed: %s", error)

        session_duration = time.monotonic() - attempt_started_at
        if session_duration >= STABLE_CONNECTION_SECONDS:
            reconnect_delay = INITIAL_RECONNECT_DELAY_SECONDS

        logger.info("Reconnecting in %s seconds", reconnect_delay)
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(
            reconnect_delay * 2,
            MAX_RECONNECT_DELAY_SECONDS,
        )


def run_health_server() -> None:
    app.run(
        host="0.0.0.0",
        port=HEALTH_PORT,
        use_reloader=False,
        threaded=True,
    )


def main() -> int:
    health_thread = threading.Thread(
        target=run_health_server,
        name="health-server",
        daemon=True,
    )
    health_thread.start()
    logger.info("Health endpoint listening on port %s at /health", HEALTH_PORT)

    token = os.environ.get("DERIV_DEMO_TOKEN", "").strip()
    if not token:
        logger.error("DERIV_DEMO_TOKEN is not set; the bot cannot start")
        return 1

    try:
        asyncio.run(run_bot(token))
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
