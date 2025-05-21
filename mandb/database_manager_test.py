# mandb/database_manager.py

import aiosqlite
import asyncio
import os

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.queue = asyncio.Queue()
        self.running = False

    async def start(self):
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        print(f"📂 Connecting to DB: {self.db_path}")

        # Create table if not exists (without pair and interval columns)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS candles (
                    timestamp TEXT PRIMARY KEY,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL
                )
            """)
            await db.commit()

        self.running = True
        asyncio.create_task(self._writer_loop())

    async def _writer_loop(self):
        while self.running:
            candle = await self.queue.get()
            try:
                await self._write_candle(candle)
            except Exception as e:
                print(f"❌ DB write error: {e}")

    async def _write_candle(self, candle):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT OR REPLACE INTO candles
                (timestamp, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    candle["timestamp"].isoformat(),
                    candle["open"],
                    candle["high"],
                    candle["low"],
                    candle["close"],
                    candle["volume"]
                )
            )
            await db.commit()

    async def save_candle(self, candle):
        # Deprecated: Use submit_write to queue writes safely
        await self.submit_write(candle)

    async def submit_write(self, candle):
        await self.queue.put(candle)

    async def stop(self):
        self.running = False
