import asyncio
import sqlite3
from datetime import datetime

from dynamics.dynamics_params_test import DB_PATH

class DatabaseManager:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.queue = asyncio.Queue()
        self.running = False

    async def start(self):
        self.running = True
        asyncio.create_task(self._worker())

    async def stop(self):
        self.running = False
        await self.queue.put(None)

    async def save_candle(self, candle):
        await self.queue.put(candle)

    async def _worker(self):
        while self.running:
            candle = await self.queue.get()
            if candle is None:
                break
            self._upsert_candle(candle)

    def _upsert_candle(self, candle):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute('''
            CREATE TABLE IF NOT EXISTS candles (
                timestamp TEXT PRIMARY KEY,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL
            )
        ''')

        timestamp = candle["timestamp"]
        open_ = float(candle["open"])
        high = float(candle["high"])
        low = float(candle["low"])
        close = float(candle["close"])
        volume = float(candle["volume"])

        c.execute("SELECT * FROM candles WHERE timestamp = ?", (timestamp,))
        row = c.fetchone()

        if row:
            prev_open, prev_high, prev_low, prev_close, prev_volume = row[1:]
            high = max(prev_high, high)
            low = min(prev_low, low)
            close = close  # last seen close wins
            volume += prev_volume
            c.execute('''
                UPDATE candles SET
                    high = ?,
                    low = ?,
                    close = ?,
                    volume = ?
                WHERE timestamp = ?
            ''', (high, low, close, volume, timestamp))
        else:
            c.execute('''
                INSERT INTO candles (timestamp, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (timestamp, open_, high, low, close, volume))

        conn.commit()
        conn.close()

        print(f"✅ [{timestamp}] O: {open_}, H: {high}, L: {low}, C: {close}, V: {volume}")
