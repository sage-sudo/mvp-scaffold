# run_bot.py or main.py

from datetime import datetime, timedelta, timezone
from collector.kraken_rest_historical import fetch_ohlc_history
from collector.kraken_v2ws import v2_start_collector
from dynamics.dynamic_params import START_AT_MINUTES

import threading

def bootstrap_and_run():
    # Step 1: Bootstrap DB with 200 historical candles for EMA
    end = datetime.now(tz=timezone.utc).replace(second=0, microsecond=0)
    start = end - timedelta(minutes=START_AT_MINUTES)  # You can extend this if you need longer history
    print("Start:", start)

    print("⏳ Bootstrapping historical data...")
    fetch_ohlc_history(start, end)
    print("✅ Historical data loaded.")

    # Step 2: Start live collector (WebSocket) on a separate thread
    print("🚀 Starting live collector...")
    threading.Thread(target=v2_start_collector, daemon=True).start()

    # Keep the main thread alive
    while True:
        pass  # Replace this with your strategy loop or a proper scheduler if needed

if __name__ == "__main__":
    bootstrap_and_run()
