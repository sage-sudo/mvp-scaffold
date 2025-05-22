# main.py
import asyncio, threading
from mandb.database_manager_test import DatabaseManager
from collector.kraken_v2_ws_test import run_kraken_collector
from ui.dashboard_test import build_dash_app
from dynamics.dynamics_params_test import DB_PATH

db = DatabaseManager(DB_PATH)

async def main():
    await db.start()
    collector_task = asyncio.create_task(run_kraken_collector(db))

    def run_dash():
        app = build_dash_app()
        app.run(debug=False, use_reloader=False)

    threading.Thread(target=run_dash, daemon=True).start()
    await collector_task

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Shutting down...")
        asyncio.run(db.stop())
