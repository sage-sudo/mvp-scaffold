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

from dynamics.dynamic_params import ALL_INTERVAL, LIVE_PAIR

ssl_context = ssl._create_unverified_context()

KRAKEN_WS_V2_URL = "wss://ws.kraken.com/v2"
PAIR = LIVE_PAIR
INTERVAL = ALL_INTERVAL  # Minutes

def v2_start_collector():
    last_emitted_ts = None  # <-- Track last emitted candle time

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
                await handle_message(message)

    async def handle_message(message):
        nonlocal last_emitted_ts

        try:
            data = json.loads(message)
        except json.JSONDecodeError as e:
            print(f"❌ JSON decode error: {e}")
            return

        if data.get("channel") == "ohlc" and data.get("type") in ["snapshot", "update"]:
            for candle in data["data"]:
                try:
                    ts_str = candle["interval_begin"].replace("Z", "+00:00")
                    ts = datetime.fromisoformat(ts_str)

                    if last_emitted_ts and ts <= last_emitted_ts:
                        # Duplicate or stale update — skip it
                        continue


                    # Finalized candle — emit and update state
                    open_ = float(candle["open"])
                    high = float(candle["high"])
                    low = float(candle["low"])
                    close = float(candle["close"])
                    volume = float(candle["volume"])

                    print(f"[{ts.isoformat()}] O: {open_}, H: {high}, L: {low}, C: {close}, V: {volume}")
                    save_candle(ts, open_, high, low, close, volume)

                    last_emitted_ts = ts


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
