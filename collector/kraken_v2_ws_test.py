# collector/kraken_v2_ws_test.py

import asyncio
import json
import websockets
import ssl
from datetime import datetime
from dynamics.dynamics_params_test import ALL_INTERVAL, LIVE_PAIR

ssl_context = ssl._create_unverified_context()
KRAKEN_WS_V2_URL = "wss://ws.kraken.com/v2"
PAIR = LIVE_PAIR
INTERVAL = ALL_INTERVAL

async def run_kraken_collector(db):
    last_emitted_ts = None

    async def connect_kraken():
        nonlocal last_emitted_ts

        async with websockets.connect(KRAKEN_WS_V2_URL, ssl=ssl_context) as ws:
            subscribe_msg = {
                "method": "subscribe",
                "params": {"channel": "ohlc", "symbol": [PAIR], "interval": INTERVAL},
            }
            await ws.send(json.dumps(subscribe_msg))
            print("📡 Subscribed with message:\n" + json.dumps(subscribe_msg, indent=2))

            async for message in ws:
                try:
                    data = json.loads(message)
                    #print(f"The message part runs\n {data}")
                except json.JSONDecodeError as e:
                    print(f"❌ JSON decode error: {e}")
                    continue

                if data.get("channel") != "ohlc":
                    continue

                for candle in data.get("data", []):
                    try:
                        ts = datetime.fromisoformat(candle["interval_begin"].replace("Z", "+00:00"))
                        if last_emitted_ts and ts <= last_emitted_ts:
                            continue

                        candle_dict = {
                            "timestamp": ts,
                            "open": float(candle["open"]),
                            "high": float(candle["high"]),
                            "low": float(candle["low"]),
                            "close": float(candle["close"]),
                            "volume": float(candle["volume"]),
                            "pair": PAIR,
                            "interval": int(INTERVAL),
                        }

                        if candle_dict["volume"] == 0:
                            continue

                        print(f"✅ [{ts}] O: {candle_dict['open']}, C: {candle_dict['close']}, V: {candle_dict['volume']}")
                        await db.save_candle(candle_dict)
                        last_emitted_ts = ts

                    except Exception as e:
                        print(f"❌ Candle parse error: {e}\nCandle:\n{json.dumps(candle, indent=2)}")

    while True:
        try:
            await connect_kraken()
        except Exception as e:
            print(f"⚠️ Kraken WS error: {e} — reconnecting in 5s...")
            await asyncio.sleep(5)
