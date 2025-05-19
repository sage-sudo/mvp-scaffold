# storage/test_db.py

import asyncio
from mandb.database_manager_test import DatabaseManager

db_manager = DatabaseManager("data/crypto.sqlite")
_started = False

async def _maybe_start():
    global _started
    if not _started:
        await db_manager.start()
        _started = True

def save_candle(candle):
    """Queue candle dict for async saving."""
    try:
        loop = asyncio.get_event_loop()
        loop.call_soon_threadsafe(lambda: asyncio.create_task(_async_save(candle)))
    except Exception as e:
        print(f"❌ Error queuing candle: {e}")

async def _async_save(candle):
    await _maybe_start()
    await db_manager.submit_write(candle)
