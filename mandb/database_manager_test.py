# mandb/database_manager.py

import aiosqlite
import asyncio

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.queue = asyncio.Queue()
        self.running = False

    async def start(self):
        if self.running:
            return
        self.running = True
        # Create table on startup if not exists
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS candles (
                    timestamp TEXT PRIMARY KEY,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    pair TEXT,
                    interval INTEGER
                )
            """)
            await db.commit()
        # Start the write loop
        asyncio.create_task(self._writer_loop())

    async def _writer_loop(self):
        while self.running:
            item = await self.queue.get()
            try:
                await self._write_candle(item)
            except Exception as e:
                print(f"❌ DB write error: {e}")

    async def _write_candle(self, candle):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT OR REPLACE INTO candles 
                (timestamp, open, high, low, close, volume, pair, interval)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    candle["timestamp"].isoformat(),
                    candle["open"],
                    candle["high"],
                    candle["low"],
                    candle["close"],
                    candle["volume"],
                    candle.get("pair", "UNKNOWN"),
                    candle.get("interval", 0),
                )
            )
            await db.commit()

    async def submit_write(self, candle):
        await self.queue.put(candle)

    async def stop(self):
        self.running = False
