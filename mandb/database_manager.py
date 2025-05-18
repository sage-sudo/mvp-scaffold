import asyncio
import duckdb
from datetime import datetime
from pathlib import Path
from dynamics.dynamic_params import FILEX
import pandas as pd


# storage/db_manager.py

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = duckdb.connect(str(db_path))
        self.queue = asyncio.Queue()
        self._running = False

        self._create_table()

    def _create_table(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS candles (
                timestamp TIMESTAMP PRIMARY KEY,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE
            )
        """)

    async def run(self):
        self._running = True
        while self._running:
            try:
                query, params = await self.queue.get()
                self.conn.execute(query, params)
            except Exception as e:
                print(f"DB Error: {e}")

    def stop(self):
        self._running = False
        self.conn.close()

    def insert_candle(self, ts, open_, high, low, close, volume):
        query = """
            INSERT INTO candles (timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(timestamp) DO NOTHING
        """
        self.queue.put_nowait((query, (ts, open_, high, low, close, volume)))

    def query_df(self, query: str) -> pd.DataFrame:
        return self.conn.execute(query).fetchdf()
