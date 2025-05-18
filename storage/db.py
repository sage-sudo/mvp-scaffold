import duckdb  # or sqlite3 if that's what you're using
from datetime import datetime
from pathlib import Path

from dynamics.dynamic_params import FILEX


# Store DB in project_root/data/name.duckdb
DB_NAME = f"test_{FILEX}.duckdb"
DB_PATH = Path(__file__).resolve().parent.parent / "data" / DB_NAME
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# Connect to DuckDB
conn = duckdb.connect(str(DB_PATH))

# Open or connect to your DB
cursor = conn.cursor()

# Make sure the table exists
cursor.execute(
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


def save_candle(ts: datetime, open_, high, low, close, volume):
    try:
        # Check if the timestamp already exists
        result = cursor.execute(
            "SELECT COUNT(*) FROM candles WHERE timestamp = ?", (ts,)
        ).fetchone()

        print(result)  # to-delete

        if result[0] == 0:
            cursor.execute(
                """
                INSERT INTO candles (timestamp, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (ts, open_, high, low, close, volume),
            )

            print(f"✅ Saved: {ts}")
        else:
            print(f"⏩ Skipped (duplicate): {ts}")

    except Exception as e:
        print(f"❌ Error saving candle: {e}")
