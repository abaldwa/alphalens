"""
backtest/adapters/

Phase: Unified Backtest & Paper Trading Umbrella, Phase 1-2
(BacktestUmbrellaPlan.md at the repo root)

Per-channel StrategyAdapter implementations (or, for ML, a result-schema
translator — see ml_adapter.py's module docstring for why ML doesn't
plug into backtest/core/engine.py's BacktestOrchestrator loop the way
the other three channels will in Phase 2).
"""
