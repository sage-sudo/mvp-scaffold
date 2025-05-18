# run_bot.py or main.py

from datetime import datetime, timedelta, timezone
from collector.kraken_rest_historical import fetch_ohlc_history
from collector.kraken_v2ws import v2_start_collector
from dynamics.dynamic_params import START_AT_MINUTES

#from ui.dashboard import app

import threading


# main.py

import asyncio
from collector.kraken_v2ws_2 import run_kraken_collector
from mandb.database_manager import DatabaseManager
from ui.dashboard import create_dash_app
import threading
import sys
from dash import Dash

DB_PATH = "data/candles.duckdb"

async def main():
    print("🧠 Starting DatabaseManager...")
    db = DatabaseManager(DB_PATH)
    db_task = asyncio.create_task(db.run())

    print("📡 Starting Kraken collector...")
    collector_task = asyncio.create_task(run_kraken_collector(db))

    print("📊 Starting Dash dashboard...")

    # Dash must run in a separate thread (it blocks)
    def run_dash():
        app: Dash = create_dash_app(db)
        app.run_server(debug=True, port=8050)

    dash_thread = threading.Thread(target=run_dash, daemon=True)
    dash_thread.start()

    # Keep main alive
    try:
        await asyncio.gather(db_task, collector_task)
    except KeyboardInterrupt:
        print("⚠️  Shutting down gracefully...")
        db.stop()
        sys.exit(0)

if __name__ == "__main__":
    asyncio.run(main())

