import asyncio
import duckdb
from datetime import datetime
from pathlib import Path
import pandas as pd
from dynamics.dynamic_params import FILEX

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = duckdb.connect(str(db_path), read_only=False)
        self.queue = asyncio.Queue()
        self._running = False
        self._task = None

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

    async def start(self):
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._writer_loop())

    async def _writer_loop(self):
        while self._running:
            try:
                query, params = await self.queue.get()
                self.conn.execute(query, params)
            except Exception as e:
                print(f"💥 DB Write Error: {e}")

    async def stop(self):
        self._running = False
        if self._task:
            await self._task
        self.conn.close()

    async def save_candle(self, candle: dict):
        """
        Save candle with keys: timestamp, open, high, low, close, volume
        """
        query = """
            INSERT INTO candles (timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(timestamp) DO NOTHING
        """
        values = (
            candle["timestamp"],
            candle["open"],
            candle["high"],
            candle["low"],
            candle["close"],
            candle["volume"]
        )
        await self.queue.put((query, values))

    def query_df(self, query: str) -> pd.DataFrame:
        return self.conn.execute(query).fetchdf()
