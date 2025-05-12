# storage/db.py

#import sys
#import os
#sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import duckdb
from pathlib import Path

# Store DB in project_root/data/candles.duckdb
DB_PATH = Path(__file__).resolve().parent.parent / "data" / "candles.duckdb"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# Connect to DuckDB
con = duckdb.connect(str(DB_PATH))

# Create OHLC table if it doesn't exist
con.execute("""
CREATE TABLE IF NOT EXISTS candles (
    timestamp TIMESTAMP PRIMARY KEY,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE
)
""")

def save_candle(ts, open_, high, low, close, volume):
    try:
        con.execute("""
        INSERT INTO candles (timestamp, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(timestamp) DO NOTHING
        """, (ts, open_, high, low, close, volume))
    except Exception as e:
        print(f"Error saving candle: {e}")
