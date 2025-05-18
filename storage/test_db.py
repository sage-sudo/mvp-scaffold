# storage/test_db.py

import asyncio
from datetime import datetime
from db.database_manager import DatabaseManager

db_manager = DatabaseManager()
_started = False


async def _maybe_start():
    global _started
    if not _started:
        await db_manager.start()
        _started = True


def save_candle(ts: datetime, open_, high, low, close, volume):
    """Externally visible sync function, queues the write."""
    try:
        asyncio.get_event_loop().call_soon_threadsafe(
            lambda: asyncio.create_task(
                _async_save(ts, open_, high, low, close, volume)
            )
        )
    except Exception as e:
        print(f"❌ Error queuing candle: {e}")


async def _async_save(ts, open_, high, low, close, volume):
    await _maybe_start()
    await db_manager.submit_write(
        """
        INSERT INTO candles (timestamp, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (ts, open_, high, low, close, volume),
    )
