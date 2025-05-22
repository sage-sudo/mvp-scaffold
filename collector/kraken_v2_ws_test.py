# collector/kraken_ws_v2.py
import asyncio, json, websockets, ssl
from datetime import datetime
from dynamics.dynamics_params_test import ALL_INTERVAL, LIVE_PAIR
#from dynamics.dynamics_params_test import DB_PATH

ssl_context = ssl._create_unverified_context()
KRAKEN_WS_V2_URL = "wss://ws.kraken.com/v2"
PAIR = LIVE_PAIR
INTERVAL = ALL_INTERVAL

async def run_kraken_collector(db):
    async def connect():
        async with websockets.connect(KRAKEN_WS_V2_URL, ssl=ssl_context) as ws:
            msg = {
                "method": "subscribe",
                "params": {"channel": "ohlc", "symbol": [PAIR], "interval": INTERVAL}
            }
            await ws.send(json.dumps(msg))
            print(f"📡 Subscribed with message:\n{json.dumps(msg, indent=2)}")

            async for message in ws:
                try:
                    data = json.loads(message)
                except json.JSONDecodeError:
                    continue

                if data.get("channel") != "ohlc" or "data" not in data:
                    continue

                candles = data["data"]
                if not isinstance(candles, list):  # single update
                    candles = [candles]

                for candle in candles:
                    try:
                        ts = datetime.fromisoformat(candle["interval_begin"].replace("Z", "+00:00"))

                        c = {
                            "timestamp": ts,
                            "open": float(candle["open"]),
                            "high": float(candle["high"]),
                            "low": float(candle["low"]),
                            "close": float(candle["close"]),
                            "volume": float(candle["volume"])
                        }

                        # Print with more trust: We want *some* flat candles
                        print(f"✅ [{ts}] O: {c['open']}, H: {c['high']}, L: {c['low']}, C: {c['close']}, V: {c['volume']}")
                        await db._upsert_candle(c)

                    except Exception as e:
                        print(f"❌ Parse fail: {e}")

    # Auto reconnect forever
    while True:
        try:
            await connect()
        except Exception as e:
            print(f"⚠️ Kraken WS error: {e}, retrying in 5s...")
            await asyncio.sleep(5)
