# strategy/ta.py
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import duckdb
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timedelta

DB_PATH = "data/candles.duckdb"


def load_recent_candles(hours=200):
    con = duckdb.connect(DB_PATH)
    query = f"""
    SELECT * FROM candles
    WHERE timestamp >= NOW() - INTERVAL {hours} HOUR
    ORDER BY timestamp ASC
    """
    df = con.execute(query).fetchdf()
    con.close()
    return df


def compute_indicators(df):
    df.set_index("timestamp", inplace=True)

    # Add 3 basic indicators
    df["rsi"] = ta.rsi(df["close"], length=14)
    df["ema_50"] = ta.ema(df["close"], length=50)
    df["ema_200"] = ta.ema(df["close"], length=200)

    return df


def generate_signal(df):
    latest = df.iloc[-1]

    signal = "hold"
    reason = ""

    # Strategy logic
    if latest["ema_50"] > latest["ema_200"] and latest["rsi"] < 30:
        signal = "buy"
        reason = "Golden cross + RSI oversold"
    elif latest["ema_50"] < latest["ema_200"] and latest["rsi"] > 70:
        signal = "sell"
        reason = "Death cross + RSI overbought"

    return signal, reason


def get_ta_signal():
    df = load_recent_candles()
    df = compute_indicators(df)
    signal, reason = generate_signal(df)
    print(f"TA Signal: {signal.upper()} | Reason: {reason}")
    return signal, reason
