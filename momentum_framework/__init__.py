"""
Momentum Framework v1.0

A robust, modular framework for momentum-based trading strategies.
"""

__version__ = "1.0.0"
__author__ = "AlphaLens Platform"

from momentum_framework.common import MomentumSignal, UniverseDefinition, StrategyRegistry
from momentum_framework.backtesting import BacktestOrchestrator, StrategyAdapter, BacktestResult
from momentum_framework.queues import QueueGenerator, QueueValidator
from momentum_framework.metrics import MetricsCalculator, StandardMetrics
from momentum_framework.strategies import StrategyBase
from momentum_framework.results import ResultsNomenclature, ResultsReader, ResultsWriter

__all__ = [
    "MomentumSignal",
    "UniverseDefinition",
    "StrategyRegistry",
    "BacktestOrchestrator",
    "StrategyAdapter",
    "BacktestResult",
    "QueueGenerator",
    "QueueValidator",
    "MetricsCalculator",
    "StandardMetrics",
    "StrategyBase",
    "ResultsNomenclature",
    "ResultsReader",
    "ResultsWriter",
]
