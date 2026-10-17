import asyncio
import websockets
import json
import os
import time

TOKEN = os.environ.get("DERIV_DEMO_TOKEN")
# Authenticated Demo Endpoint
URL = "wss://api.derivws.com/trading/v1/options/ws/demo"

async def main():
    if not TOKEN:
        print("ERROR: DERIV_DEMO_TOKEN not found.")
        return

    while True:
        try:
            print("Connecting to Authenticated Deriv Endpoint...")
            async with websockets.connect(URL) as websocket:
                
                # 1. Authenticate
                auth_msg = {"authorize": TOKEN}
                await websocket.send(json.dumps(auth_msg))
                
                # Wait for auth response
                response = await websocket.recv()
                data = json.loads(response)
                if "error" in data:
                    print(f"Auth failed: {data['error']['message']}")
                    await asyncio.sleep(5)
                    continue
                
                print(f"Authenticated! Balance: {data['authorize']['balance']} USD")

                # 2. Subscribe to ticks
                await websocket.send(json.dumps({"ticks": "R_100", "subscribe": 1}))

                # 3. Trading Loop (Simple Test Strategy)
                last_trade_time = 0
                
                async for message in websocket:
                    msg = json.loads(message)
                    
                    if 'tick' in msg:
                        quote = msg['tick']['quote']
                        print(f"[TICK] R_100 = {quote}")
                        
                        # Every 60 seconds, place a $1 CALL trade
                        current_time = time.time()
                        if current_time - last_trade_time > 60:
                            print("Placing a $1 CALL trade...")
                            buy_msg = {
                                "buy": 1,
                                "price": 1.00,
                                "parameters": {
                                    "amount": 1.00,
                                    "basis": "stake",
                                    "contract_type": "CALL",
                                    "currency": "USD",
                                    "duration": 1,
                                    "duration_unit": "m",
                                    "symbol": "R_100"
                                }
                            }
                            await websocket.send(json.dumps(buy_msg))
                            last_trade_time = current_time

        except Exception as e:
            print(f"Connection error: {e}")
            print("Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
