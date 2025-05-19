import asyncio
from mandb.test_database_manager import DatabaseManager
from collector.test_kraken_ws_v2 import run_kraken_collector
from ui.test_dashboard import build_dash_app
#from dynamics.dynamic_params import DB_PATH
import threading
import dash

DB_PATH = "data/testing_app.duckdb"

# Initialize DB manager
print("🗄️ Initializing database manager...")
db = DatabaseManager(DB_PATH)


async def start_db():
    print("🚀 Starting DB manager task loop...")
    await db.start()


async def main():
    print("⚙️ Bootstrapping main app...")

    # Start DB manager
    db_task = asyncio.create_task(start_db())

    # Start Kraken collector
    collector_task = asyncio.create_task(run_kraken_collector(db))

    # Start Dash in a thread
    def run_dash():
        app = build_dash_app(db)
        app.run(debug=False, use_reloader=False)

    dash_thread = threading.Thread(target=run_dash, daemon=True)
    dash_thread.start()

    # Keep main alive
    await asyncio.gather(db_task, collector_task)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Shutdown requested by user.")
        db.stop()
