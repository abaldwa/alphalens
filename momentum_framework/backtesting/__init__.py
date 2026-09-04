"""
Backtesting Framework

Provides the common orchestration layer for running momentum strategy
backtests, wrapping the existing backtest/core/engine.py machinery behind
a stable interface that new strategy files can target.
"""

from momentum_framework.backtesting.adapter import StrategyAdapter
from momentum_framework.backtesting.orchestrator import BacktestOrchestrator, BacktestConfig
from momentum_framework.backtesting.result import BacktestResult

__all__ = [
    "StrategyAdapter",
    "BacktestOrchestrator",
    "BacktestConfig",
    "BacktestResult",
]
