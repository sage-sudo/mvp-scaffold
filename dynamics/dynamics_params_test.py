# dynamics/dynamics_params_test.py

# 💱 Kraken-compatible trading pair (check Kraken docs for symbols)
HISTORICAL_PAIR = "XBT/USD"  # For backtesting
LIVE_PAIR = "BTC/USD"       # For live collection (Kraken uses "XBT" not "BTC")

# 🕒 Timeframe settings
ALL_INTERVAL = 5  # must be a string for Kraken WS v2 compatibility
START_AT_MINUTES = 200  # For how far back to start backfilling, if needed

# 📁 DB + File naming
FILEX = f"{ALL_INTERVAL}_min"
DB_PATH = f"data/crypto5_{FILEX}.sqlite"
