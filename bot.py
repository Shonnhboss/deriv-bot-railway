import asyncio
import websockets
import json

async def main():
    # The correct public endpoint for market data
    url = "wss://api.derivws.com/trading/v1/options/ws/public"

    while True:
        try:
            print("Connecting to Deriv...")
            async with websockets.connect(url) as websocket:
                print("Connection established!")
                
                # Subscribe to R_100 ticks
                subscribe_msg = {"ticks": "R_100", "subscribe": 1}
                await websocket.send(json.dumps(subscribe_msg))
                print("Subscription sent. Waiting for ticks...")

                # Keep listening for messages
                async for message in websocket:
                    data = json.loads(message)
                    if 'tick' in data:
                        quote = data['tick']['quote']
                        print(f"[TICK] R_100 = {quote}")

        except Exception as e:
            print(f"Connection error: {e}")
            print("Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
