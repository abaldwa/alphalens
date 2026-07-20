"""
backtest/paper_trading/

Phase: Unified Backtest & Paper Trading Umbrella, Phase 5
(BacktestUmbrellaPlan.md at the repo root)

Generalizes today's ML-only paper trading (systems/ml_signal_engine/
inference/paper_trading_step.py, scripts/paper_trading_tracker.py,
datastore/api/routers/paper_trading.py — all of which remain untouched,
"wrap don't refactor") to run against any of the four channels' adapters.

Directory layout, one level more scoped than the ML-only original
(paper_trading/pending/{date}.json, paper_trading/executions/) since a
run is now (channel, strategy_id)-scoped rather than implicitly "the one
ML signal engine":

  paper_trading/
    pending/{channel}/{strategy_id}/{date}.json     — proposed actions awaiting approval
    executions/{channel}/{strategy_id}/{date}.json  — accepted/rejected actions, Gate-7-style day counter
    state/{channel}/{strategy_id}.json               — persisted StrategyPortfolio cash/positions/equity

See live_runner.py (propose/accept/reject) and approval_queue.py (the
JSON-file queue itself) for the mechanics.
"""
