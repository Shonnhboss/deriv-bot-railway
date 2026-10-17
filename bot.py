import asyncio
import os
import logging
from deriv_sdk import DerivClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

APP_ID = "1089"
TOKEN = os.environ.get("DERIV_DEMO_TOKEN")
SYMBOL = "R_100"

async def main():
    if not TOKEN:
        logging.error("DERIV_DEMO_TOKEN environment variable is not set.")
        return

    while True:
        try:
            client = DerivClient(app_id=APP_ID, api_token=TOKEN)
            await client.start()
            logging.info("Authorization successful.")

            logging.info(f"Subscribing to {SYMBOL} ticks...")
            subscription = await client.market.subscribe_ticks(SYMBOL)
            
            async for tick in subscription:
                logging.info(f"[TICK] {tick.symbol} = {tick.quote}")
                
        except Exception as e:
            logging.error(f"Connection error: {e}")
            logging.info("Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
