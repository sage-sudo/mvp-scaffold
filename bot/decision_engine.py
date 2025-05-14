# bot/decision_engine.py
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import asyncio
from datetime import datetime
from strategy.ta import get_ta_signal
# from strategy.ml import get_ml_signal  <-- Later if we add ML
# from execution.binance_api import place_order  <-- Placeholder

async def run_strategy_loop(interval_minutes=60):
    while True:
        now = datetime.utcnow()
        print(f"\n🧠 Checking signal @ {now} UTC...")

        # Get TA signal
        ta_signal, ta_reason = get_ta_signal()

        # (Optional) Get ML signal later
        # ml_signal, ml_confidence = get_ml_signal()

        # Decision logic (TA only for now)
        if ta_signal in ["buy", "sell"]:
            print(f"⚡ Signal: {ta_signal.upper()} | Reason: {ta_reason}")
            # place_order(ta_signal)  <-- Stubbed
        else:
            print("⏳ No strong signal. Holding position.")

        await asyncio.sleep(interval_minutes * 60)

    #if __name__ == "__main__":
    asyncio.run(run_strategy_loop())
