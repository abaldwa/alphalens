"""
Queue Generation Framework

Common base for generating backtest job queues. Every strategy's queue
generator subclasses QueueGenerator and only needs to supply its own
parameter grid — strategy_id construction, validation, and JSON output
are handled once, here.
"""

from momentum_framework.queues.generator import QueueGenerator
from momentum_framework.queues.validator import QueueValidator

__all__ = ["QueueGenerator", "QueueValidator"]
