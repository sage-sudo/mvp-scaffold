# collector/kraken_ws_v2.py

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import asyncio
import json
import websockets
from datetime import datetime
from storage.db import save_candle

import ssl
ssl_context = ssl._create_unverified_context()

KRAKEN_WS_V2_URL = "wss://ws.kraken.com/v2"
PAIR = "BTC/USD"
INTERVAL = 1  # Minutes

def v2_start_collector():
    async def connect_kraken():
        async with websockets.connect(KRAKEN_WS_V2_URL, ssl=ssl_context) as ws:
            subscribe_msg = {
                "method": "subscribe",
                "params": {
                    "channel": "ohlc",
                    "symbol": [PAIR],
                    "interval": INTERVAL
                }
            }
            await ws.send(json.dumps(subscribe_msg))
            print(f"📡 Subscribed to {PAIR} {INTERVAL}-minute OHLC feed (v2)")

            async for message in ws:
                #print("📥 RAW:", message)
                await handle_message(message)


    async def handle_message(message):
        data = json.loads(message)

        if data.get("channel") == "ohlc" and data.get("type") in ["snapshot", "update"]:
            for candle in data["data"]:
                try:
                    ts = datetime.fromisoformat(candle["interval_begin"].replace("Z", "+00:00"))
                    open_ = float(candle["open"])
                    high = float(candle["high"])
                    low = float(candle["low"])
                    close = float(candle["close"])
                    volume = float(candle["volume"])

                    print(f"[{ts}] O: {open_}, H: {high}, L: {low}, C: {close}, V: {volume}")
                    save_candle(ts, open_, high, low, close, volume)

                except Exception as e:
                    print(f"❌ Failed to process candle: {e}")


    async def main():
        while True:
            try:
                await connect_kraken()
            except Exception as e:
                print(f"⚠️ WebSocket error: {e} — reconnecting in 5s...")
                await asyncio.sleep(5)

    asyncio.run(main())
