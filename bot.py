import asyncio
import websockets
import json
import os
import time
import aiohttp

TOKEN = os.environ.get("DERIV_DEMO_TOKEN")
BASE_REST_URL = "https://api.derivws.com"
APP_ID = "1089"

async def get_otp_and_account(session):
    """Fetches the demo account ID and a new OTP."""
    # 1. Get the list of accounts to find the demo account ID
    # FIX: Added Deriv-App-ID header required for PAT authentication
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Deriv-App-ID": APP_ID
    }
    async with session.get(f"{BASE_REST_URL}/trading/v1/options/accounts", headers=headers) as resp:
        if resp.status != 200:
            print(f"Failed to get accounts: {await resp.text()}")
            return None, None
        accounts_data = await resp.json()
        demo_account_id = None
        for acc in accounts_data.get('data', []):
            if acc.get('account_type') == 'demo':
                demo_account_id = acc.get('account_id')
                break
        if not demo_account_id:
            print("No demo account found.")
            return None, None

    # 2. Request an OTP for the demo account
    async with session.post(f"{BASE_REST_URL}/trading/v1/options/accounts/{demo_account_id}/otp", headers=headers) as resp:
        if resp.status != 200:
            print(f"Failed to get OTP: {await resp.text()}")
            return None, None
        otp_data = await resp.json()
        return demo_account_id, otp_data.get('otp')

async def main():
    if not TOKEN:
        print("ERROR: DERIV_DEMO_TOKEN not found.")
        return

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                print("Requesting OTP...")
                account_id, otp = await get_otp_and_account(session)
                if not otp:
                    print("Failed to get OTP. Retrying in 10s...")
                    await asyncio.sleep(10)
                    continue

                print(f"Got OTP. Connecting to demo WebSocket...")
                ws_url = f"wss://api.derivws.com/trading/v1/options/ws/demo?otp={otp}"
                
                async with websockets.connect(ws_url) as websocket:
                    print("Connected to Authenticated Demo Endpoint!")
                    
                    # Subscribe to ticks
                    await websocket.send(json.dumps({"ticks": "R_100", "subscribe": 1}))

                    # Trading Loop
                    last_trade_time = time.time() - 60  # Force a trade on the first tick
                    
                    async for message in websocket:
                        msg = json.loads(message)
                        
                        if 'error' in msg:
                            print(f"API ERROR: {msg['error']['message']}")
                        
                        if 'tick' in msg:
                            quote = msg['tick']['quote']
                            print(f"[TICK] R_100 = {quote}")
                            
                            current_time = time.time()
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
                        
                        if 'proposal' in msg:
                            proposal_id = msg['proposal']['id']
                            print(f"Proposal received! ID: {proposal_id}")
                            print("Executing BUY order...")
                            buy_msg = {"buy": proposal_id, "price": 1.00}
                            await websocket.send(json.dumps(buy_msg))

                        if 'buy' in msg:
                            contract_id = msg['buy']['contract_id']
                            print(f"✅ TRADE SUCCESSFUL! Contract ID: {contract_id}")

            except Exception as e:
                print(f"Connection error: {e}")
                print("Reconnecting in 5 seconds...")
                await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
