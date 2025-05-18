import asyncio
import duckdb
from datetime import datetime
from pathlib import Path
from dynamics.dynamic_params import FILEX


class DatabaseManager:
    def __init__(self, db_name=None):
        self.db_name = db_name or f"test_testdb_{FILEX}.duckdb"
        self.db_path = Path(__file__).resolve().parent.parent / "data" / self.db_name
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.conn = duckdb.connect(str(self.db_path))
        self.cursor = self.conn.cursor()
        self._ensure_table()

        self.queue = asyncio.Queue()
        self.worker_task = None

    def _ensure_table(self):
        self.cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS candles (
            timestamp TIMESTAMP PRIMARY KEY,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE
        )
        """
        )

    async def start(self):
        if self.worker_task is None:
            self.worker_task = asyncio.create_task(self._worker())

    async def _worker(self):
        while True:
            query, params = await self.queue.get()
            try:
                ts = params[0]
                result = self.cursor.execute(
                    "SELECT COUNT(*) FROM candles WHERE timestamp = ?", (ts,)
                ).fetchone()

                if result[0] == 0:
                    self.cursor.execute(query, params)
                    print(f"✅ Saved: {ts}")
                else:
                    print(f"⏩ Skipped (duplicate): {ts}")
            except Exception as e:
                print(f"❌ Error saving candle: {e}")
            self.queue.task_done()

    async def submit_write(self, query, params):
        await self.queue.put((query, params))

    async def stop(self):
        if self.worker_task:
            self.worker_task.cancel()
            try:
                await self.worker_task
            except asyncio.CancelledError:
                pass
