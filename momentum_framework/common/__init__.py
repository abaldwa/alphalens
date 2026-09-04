"""
Common modules shared across all momentum strategies.

Exports:
  - MomentumSignal: Base class for momentum signal computation
  - UniverseDefinition: M-band universe definitions
  - StrategyRegistry: Point-in-time strategy versioning
"""

from momentum_framework.common.signals import MomentumSignal
from momentum_framework.common.universe import UniverseDefinition, MBANDS, TOP_N_BY_BAND
from momentum_framework.common.registry import StrategyRegistry

__all__ = ["MomentumSignal", "UniverseDefinition", "MBANDS", "TOP_N_BY_BAND", "StrategyRegistry"]
