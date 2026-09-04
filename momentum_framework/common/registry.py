"""
Strategy Registry - Point-in-time versioned strategy definitions.

Wraps strategies/registry.py to provide momentum-specific registration.
Append-only: no updates or deletes, only new versions.
"""

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional


@dataclass
class StrategyDefinition:
    """A versioned strategy definition."""
    strategy_key: str          # e.g., "momentum:R01"
    version: int
    rank_method: str           # e.g., "jt_momentum", "trailing_return"
    parameters: Dict[str, Any]
    valid_from: date
    valid_to: Optional[date] = None
    status: str = "active"     # draft | active | retired
    description: str = ""


class StrategyRegistry:
    """
    In-memory registry for momentum strategy definitions during framework
    development. Production use should delegate to strategies/registry.py
    for DuckDB-backed point-in-time versioning.
    """

    def __init__(self) -> None:
        self._definitions: Dict[str, List[StrategyDefinition]] = {}

    def register(
        self,
        strategy_key: str,
        rank_method: str,
        parameters: Dict[str, Any],
        description: str = "",
        valid_from: Optional[date] = None,
    ) -> StrategyDefinition:
        """Register version 1 of a new strategy, or version N+1 of existing."""
        existing = self._definitions.get(strategy_key, [])
        version = len(existing) + 1

        # Close out the previous version
        if existing:
            existing[-1].valid_to = valid_from or date.today()

        definition = StrategyDefinition(
            strategy_key=strategy_key,
            version=version,
            rank_method=rank_method,
            parameters=parameters,
            valid_from=valid_from or date.today(),
            description=description,
        )
        self._definitions.setdefault(strategy_key, []).append(definition)
        return definition

    def get(self, strategy_key: str, as_of: Optional[date] = None) -> StrategyDefinition:
        """Get the strategy definition in force as of a given date (default: today)."""
        as_of = as_of or date.today()
        versions = self._definitions.get(strategy_key, [])
        if not versions:
            raise KeyError(f"No definition registered for {strategy_key}")

        for v in reversed(versions):
            if v.valid_from <= as_of and (v.valid_to is None or as_of < v.valid_to):
                return v
        raise KeyError(f"No definition for {strategy_key} valid as_of {as_of}")

    def list_strategies(self) -> List[str]:
        """List all registered strategy keys."""
        return list(self._definitions.keys())

    def history(self, strategy_key: str) -> List[StrategyDefinition]:
        """Get full version history for a strategy."""
        return self._definitions.get(strategy_key, [])
