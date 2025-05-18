# collector/test_kraken_ws_v2.py

import asyncio
import json
import websockets
import ssl
from datetime import datetime
from dynamics.dynamic_params import ALL_INTERVAL, LIVE_PAIR

ssl_context = ssl._create_unverified_context()

KRAKEN_WS_V2_URL = "wss://ws.kraken.com/v2"
PAIR = LIVE_PAIR
INTERVAL = ALL_INTERVAL  # e.g. "60" for 1-hour


async def run_kraken_collector(db):
    last_emitted_ts = None  # Avoid duplicate writes

    async def connect_kraken():
        nonlocal last_emitted_ts

        async with websockets.connect(KRAKEN_WS_V2_URL, ssl=ssl_context) as ws:
            subscribe_msg = {
                "method": "subscribe",
                "params": {"channel": "ohlc", "symbol": [PAIR], "interval": INTERVAL},
            }
            await ws.send(json.dumps(subscribe_msg))
            print(f"📡 Subscribed to {PAIR} {INTERVAL}-min OHLC (v2)")

            async for message in ws:
                try:
                    data = json.loads(message)
                except json.JSONDecodeError as e:
                    print(f"❌ JSON decode error: {e}")
                    continue

                if data.get("channel") == "ohlc" and data.get("type") == "snapshot":
                    for candle in data["data"]:
                        try:
                            ts_str = candle["interval_begin"].replace("Z", "+00:00")
                            ts = datetime.fromisoformat(ts_str)

                            if last_emitted_ts and ts <= last_emitted_ts:
                                continue  # Skip duplicates or old data

                            candle_dict = {
                                "timestamp": ts,
                                "open": float(candle["open"]),
                                "high": float(candle["high"]),
                                "low": float(candle["low"]),
                                "close": float(candle["close"]),
                                "volume": float(candle["volume"]),
                            }

                            # 🚨 Early filter for suspicious candles
                            if (
                                candle_dict["high"] == candle_dict["low"]
                                or candle_dict["volume"] == 0
                                or candle_dict["open"] == candle_dict["close"]
                            ):
                                print(f"⚠️ Skipping flat candle at {ts}:\n{json.dumps(candle, indent=2)}")
                                continue

                            print(
                                f"[{ts.isoformat()}] "
                                f"O: {candle_dict['open']}, H: {candle_dict['high']}, "
                                f"L: {candle_dict['low']}, C: {candle_dict['close']}, V: {candle_dict['volume']}"
                            )

                            await db.save_candle(candle_dict)
                            last_emitted_ts = ts

                        except Exception as e:
                            print(f"❌ Candle parse error: {e}\nCandle Data:\n{json.dumps(candle, indent=2)}")


    # Keep reconnecting on fail
    while True:
        try:
            await connect_kraken()
        except Exception as e:
            print(f"⚠️ Kraken WS error: {e} — retrying in 5s...")
            await asyncio.sleep(5)
