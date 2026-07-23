"""
backtest/walk_forward/

Phase: Unified Backtest & Paper Trading Umbrella, Phase 2.5
(BacktestUmbrellaPlan.md at the repo root)

Historical forward-replay: start at a real historical date and step
forward period-by-period, retraining/refreshing the adapter (if it
implements one) at each horizon-bucket-driven cadence, using only data
available up to that point — never a future value. See runner.py's
module docstring for the design note on why this doesn't duplicate
backtest/core/engine.py's loop with a separate "day_driver.py" module.
"""
