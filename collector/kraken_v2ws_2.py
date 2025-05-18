# collector/kraken_ws_v2.py

import asyncio
import json
import websockets
import ssl
from datetime import datetime
from dynamics.dynamic_params import ALL_INTERVAL, LIVE_PAIR

ssl_context = ssl._create_unverified_context()

KRAKEN_WS_V2_URL = "wss://ws.kraken.com/v2"
PAIR = LIVE_PAIR
INTERVAL = ALL_INTERVAL  # Minutes


async def run_kraken_collector(db):
    last_emitted_ts = None  # Track last emitted candle time

    async def connect_kraken():
        nonlocal last_emitted_ts

        async with websockets.connect(KRAKEN_WS_V2_URL, ssl=ssl_context) as ws:
            subscribe_msg = {
                "method": "subscribe",
                "params": {"channel": "ohlc", "symbol": [PAIR], "interval": INTERVAL},
            }
            await ws.send(json.dumps(subscribe_msg))
            print(f"📡 Subscribed to {PAIR} {INTERVAL}-minute OHLC feed (v2)")

            async for message in ws:
                try:
                    data = json.loads(message)
                except json.JSONDecodeError as e:
                    print(f"❌ JSON decode error: {e}")
                    continue

                if data.get("channel") == "ohlc" and data.get("type") in ["snapshot", "update"]:
                    for candle in data["data"]:
                        try:
                            ts_str = candle["interval_begin"].replace("Z", "+00:00")
                            ts = datetime.fromisoformat(ts_str)

                            if last_emitted_ts and ts <= last_emitted_ts:
                                continue  # Skip stale/duplicate

                            open_ = float(candle["open"])
                            high = float(candle["high"])
                            low = float(candle["low"])
                            close = float(candle["close"])
                            volume = float(candle["volume"])

                            print(f"[{ts.isoformat()}] O: {open_}, H: {high}, L: {low}, C: {close}, V: {volume}")

                            await db.save_candle({  # Expecting your DB manager to support this
                                "timestamp": ts,
                                "open": open_,
                                "high": high,
                                "low": low,
                                "close": close,
                                "volume": volume,
                                "pair": PAIR,
                                "interval": INTERVAL
                            })

                            last_emitted_ts = ts

                        except Exception as e:
                            print(f"❌ Failed to process candle: {e}")

    while True:
        try:
            await connect_kraken()
        except Exception as e:
            print(f"⚠️ WebSocket error: {e} — reconnecting in 5s...")
            await asyncio.sleep(5)
