import asyncio
import websockets
import json
import os
import time

TOKEN = os.environ.get("DERIV_DEMO_TOKEN")
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
                await websocket.send(json.dumps({"authorize": TOKEN}))
                response = await websocket.recv()
                data = json.loads(response)
                if "error" in data:
                    print(f"Auth failed: {data['error']['message']}")
                    await asyncio.sleep(5)
                    continue
                
                print(f"Authenticated! Balance: {data['authorize']['balance']} USD")

                # 2. Subscribe to ticks
                await websocket.send(json.dumps({"ticks": "R_100", "subscribe": 1}))

                # 3. Trading Loop
                last_trade_time = time.time() - 60  # Force a trade on the first tick
                
                async for message in websocket:
                    msg = json.loads(message)
                    
                    # Print API Errors so we aren't blind
                    if 'error' in msg:
                        print(f"API ERROR: {msg['error']['message']}")
                    
                    # Handle Ticks
                    if 'tick' in msg:
                        quote = msg['tick']['quote']
                        print(f"[TICK] R_100 = {quote}")
                        
                        current_time = time.time()
                        # Wait 60 seconds between trades
                        if current_time - last_trade_time > 60:
                            print("Requesting a $1 CALL proposal...")
                            proposal_msg = {
                                "proposal": 1,
                                "amount": 1.00,
                                "basis": "stake",
                                "contract_type": "CALL",
                                "currency": "USD",
                                "duration": 1,
                                "duration_unit": "m",
                                "symbol": "R_100"
                            }
                            await websocket.send(json.dumps(proposal_msg))
                            last_trade_time = current_time
                    
                    # Handle the Proposal Response
                    if 'proposal' in msg:
                        proposal_id = msg['proposal']['id']
                        print(f"Proposal received! ID: {proposal_id}")
                        print("Executing BUY order...")
                        
                        # Step 2: Buy using the Proposal ID
                        buy_msg = {
                            "buy": proposal_id,
                            "price": 1.00
                        }
                        await websocket.send(json.dumps(buy_msg))

                    # Handle the Buy Response
                    if 'buy' in msg:
                        contract_id = msg['buy']['contract_id']
                        print(f"✅ TRADE SUCCESSFUL! Contract ID: {contract_id}")

        except Exception as e:
            print(f"Connection error: {e}")
            print("Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
