# main.py
from collector.kraken_rest_historical import fetch_ohlc_history
#from collector.kraken_v2ws import v2_start_collector

#from collector.kraken_ws import start_collector
#from bot.decision_engine import run_strategy_loop

#run_strategy_loop()

#start_collector()

#v2_start_collector()

from datetime import datetime, timedelta, timezone

end = datetime.now(tz=timezone.utc).replace(second=0, microsecond=0)
start = end - timedelta(minutes=200)

fetch_ohlc_history(start, end)

#fetch_ohlc_history()
