# collector/kraken_ws.py

import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import asyncio
import json
import websockets
from datetime import datetime
import pandas as pd
from storage.db import save_candle  # We'll build this next


import ssl

ssl_context = ssl._create_unverified_context()

KRAKEN_WS_URL = "wss://ws.kraken.com"
PAIR = "XBT/USD"
INTERVAL = 15  # 60 minutes = 1-hour candles
opening = True


def start_collector():
    async def connect_kraken():
        async with websockets.connect(KRAKEN_WS_URL, ssl=ssl_context) as ws:
            subscribe_msg = {
                "event": "subscribe",
                "pair": [PAIR],
                "subscription": {"name": "ohlc", "interval": INTERVAL},
            }
            await ws.send(json.dumps(subscribe_msg))

            print(f"Subscribed to {PAIR} 1-hour OHLC feed")

            async for message in ws:
                await handle_message(message)

    async def handle_message(message):
        data = json.loads(message)

        # Ignore non-candle messages
        if isinstance(data, list) and len(data) > 1 and data[-2] == "ohlc-15":
            candle = data[1]
            # if candle[7] == 1:
            #       print("Here's we were Stand", candle[7])
            #      opening = False
            #     websockets.Close()
            # ts = datetime.utcfromtimestamp(float(candle[0]))
            ts = datetime.fromtimestamp(float(candle[0]))
            open_, high, low, close = map(float, candle[1:5])
            volume = float(candle[6])

            print(f"[{ts}] O: {open_}, H: {high}, L: {low}, C: {close}, V: {volume}")

            # Save to DuckDB
            save_candle(ts, open_, high, low, close, volume)

            # -------------------------EXPERIMENTING-------------------------------------
            # ohlcv = f"[{ts}] O: {open_}, H: {high}, L: {low}, C: {close}, V: {volume}"

            # data = [[open_, high, low, close, volume]]

            # df = pd.DataFrame(data=data,
            # columns=['O', 'H', 'L', 'C', 'V'],
            # index=[ts])

            # print(df)

            # import sys
            # import os
            # print("Current Working Directory:", os.getcwd())
            # print("sys.path:", sys.path)

            # try:
            # from storage.db import save_candle
            # print("✅ Successfully imported save_candle from storage.db")
            # except Exception as e:
            # print("❌ Failed to import:", e)

            print("🌀 kraken_ws.py is running...")

            # --------------------------------------------------------------------------

    async def main():
        while opening:
            try:
                await connect_kraken()
            except Exception as e:
                print(f"WebSocket error: {e}, reconnecting in 5s...")
                await asyncio.sleep(5)

    # if __name__ == "__main__":
    asyncio.run(main())
