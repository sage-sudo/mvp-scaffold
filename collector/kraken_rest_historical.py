# collector/kraken_rest_historical.py

import requests
from datetime import datetime, timedelta, timezone
from storage.db import save_candle
import time

from dynamics.dynamic_params import ALL_INTERVAL, HISTORICAL_PAIR

import ssl

ssl_context = ssl._create_unverified_context()

KRAKEN_REST_URL = "https://api.kraken.com/0/public/OHLC"
PAIR = HISTORICAL_PAIR  # Kraken's weird naming, BTC/USD is XBTUSD
INTERVAL = ALL_INTERVAL # in minutes

def fetch_ohlc_history(start_time: datetime, end_time: datetime):
    since_unix = int(start_time.timestamp())
    end_unix = int(end_time.timestamp())

    print(f"📜 Fetching candles from {start_time} to {end_time}...")

    while since_unix < end_unix:
        params = {
            "pair": PAIR,
            "interval": INTERVAL,
            "since": since_unix,
        }

        try:
            resp = requests.get(KRAKEN_REST_URL, params=params, verify=False)
            data = resp.json()

            if "error" in data and data["error"]:
                raise Exception(f"Kraken API error: {data['error']}")

            result = data["result"]
            
            # Find the actual candle key (Kraken likes to change it)
            candle_key = next((k for k in result.keys() if k != "last"), None)

            if not candle_key:
                print("❌ No candle data found in response")
                return

            candles = result[candle_key]

            if not candles:
                print("🛑 No more candles returned.")
                break

            for candle in candles:
                ts = datetime.fromtimestamp(float(candle[0]), tz=timezone.utc)
                if ts >= end_time:
                    return

                open_ = float(candle[1])
                high = float(candle[2])
                low = float(candle[3])
                close = float(candle[4])
                volume = float(candle[6])

                print(f"[{ts}] O: {open_}, H: {high}, L: {low}, C: {close}, V: {volume}")
                save_candle(ts, open_, high, low, close, volume)

            # Kraken limits 720 candles per call (12 hours @ 1m)
            since_unix = int(candles[-1][0]) + 60

            print("=>", since_unix)

            # Avoid being rate-limited
            time.sleep(1)

        except Exception as e:
            print(f"❌ Error fetching candles: {e}")
            time.sleep(5)  # back off and try again

# 🔁 Fetch past 200 1m candles (for 200 EMA bootstrapping)
# end = datetime.now(tz=timezone.utc).replace(second=0, microsecond=0)
# start = end - timedelta(minutes=200)
# fetch_ohlc_history(start, end)
