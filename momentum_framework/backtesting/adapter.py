"""
StrategyAdapter Protocol

Common interface every momentum strategy must implement to plug into the
BacktestOrchestrator. Mirrors backtest/core/engine.py's StrategyAdapter
protocol but scoped to what momentum strategies actually need, so a new
strategy file only has to implement rebalance() + describe().
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, List, Optional, cast
import logging

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class Signal:
    """A single buy/sell/hold decision emitted at a rebalance date."""
    ticker: str
    action: str  # "buy" | "sell" | "hold" | "forced_close"
    conviction: Optional[float] = None
    size_multiplier: float = 1.0
    rank: Optional[int] = None
    context: Dict[str, Any] = field(default_factory=dict)


class StrategyAdapter(ABC):
    """
    Base class every momentum strategy (R01, R03, R07-R17, ...) implements.
    R0 was retired 2026-09-04 — its 4 weight_method variants were split
    into standalone strategies R14-R17, each with its own strategy_code
    (see strategies/r14_inverse_volatility.py etc.) rather than one
    strategy parameterized by weight_method — see project_r0_split_r14_r17
    memory for the rationale.

    The orchestrator calls rebalance() at every rebalance_cadence_days
    boundary and expects a list of Signal objects for the tickers that
    should change position. It never mutates strategy state directly.
    """

    #: Set by subclasses — used to build the canonical strategy_id and to
    #: route to the correct cache/rankings table (see metrics/nomenclature.py)
    strategy_code: str = "UNSET"
    rank_method: str = "unset"

    def __init__(self, band_id: int, top_n: int, lookback_months: int,
                 rebalance_cadence_days: int, **kwargs: Any):
        self.band_id = band_id
        self.top_n = top_n
        self.lookback_months = lookback_months
        self.rebalance_cadence_days = rebalance_cadence_days
        self.extra_params: Dict[str, Any] = kwargs

    @abstractmethod
    def rebalance(
        self, as_of_date: str, universe: List[str], conn: Any,
        held: FrozenSet[str], equity_curve: pd.Series,
    ) -> List[Signal]:
        """
        Compute the target basket at as_of_date and return Signal objects
        for every ticker entering, leaving, or resized in the portfolio.

        `universe` MUST already be scoped to this strategy's band — pass
        the result of self.resolve_universe(as_of_date, conn), never the
        full cross-band ticker list. Ranking within the wrong universe
        (e.g. ranking against all 800 stocks for a band_id=2 strategy)
        silently produces a different top_n than the band promises.

        PURE-FUNCTION CONTRACT (explicit user instruction, 2026-09-04:
        "the purpose of Strategy is to generate trades and nothing more
        ... everything should be pure-computed or available for the
        strategy to generate the trades"): a strategy must never hold its
        own mutable state that duplicates ground truth the orchestrator
        already has — `held` and `equity_curve` exist so it doesn't have
        to. Before this, R07 tracked its own `self._held` set (assigned
        from its OWN intended target at the end of rebalance(), BEFORE
        Portfolio actually executed the trade) — if a buy silently failed
        (no price data that day, see Portfolio.rebalance_to_target()'s
        `if price is None: continue`), R07's guess would silently diverge
        from what Portfolio really held. `held` is instead always
        `set(portfolio.positions.keys())` — real, post-execution ground
        truth, passed in fresh every call, impossible to drift.

        `held`: tickers actually held by the Portfolio as of the START of
        this rebalance (before today's trades) — real positions, not a
        strategy's own memory of what it last requested.

        `equity_curve`: the Portfolio's realized daily equity so far this
        backtest (date-indexed, Timestamp index), for strategies whose
        exposure depends on their OWN recent volatility (R08's Barroso-
        Santa-Clara vol-target, R09's Moreira-Muir vol-scaling). Replaces
        the old update_portfolio_equity() mutation hook — the orchestrator
        already computes this series in its simulation loop; strategies
        needing it now receive a read-only view instead of independently
        re-accumulating their own copy via a stateful callback.
        """
        raise NotImplementedError

    def resolve_universe(self, as_of_date: str, conn: Any) -> List[str]:
        """
        This strategy's band-scoped ticker list on as_of_date — the
        correct input to rebalance()'s ranking. See common/band_universe.py
        for why band membership (market-cap rank within the ADTV-liquid
        universe) is a different ranking axis from the momentum SIGNAL
        this strategy applies inside that resolved set.

        PREFERS the pre-built cache (common/universe_cache.py — explicit
        user instruction, 2026-09-04: "pre-build the same and all
        strategies to refer to these tables") over live computation. Every
        strategy calls this same method, so the cache is shared across
        all 13 automatically — no per-strategy wiring needed. Falls back
        to a live query (logged, not silent) in THREE cases, none of them
        fatal: the cache file doesn't exist yet, the requested date isn't
        in the pre-built grid, or (DuckDB single-writer constraint) the
        cache file exists but is mid-build and locked by
        scripts/build_universe_cache.py's writer connection — a
        concurrent read during a build must degrade gracefully, not
        crash every caller running at the same time.
        """
        from momentum_framework.common.universe_cache import get_cached_universe, CACHE_DB_PATH

        if CACHE_DB_PATH.exists():
            try:
                cached = get_cached_universe(self.band_id, as_of_date)
            except Exception as e:
                logger.debug(
                    f"Universe cache unreadable for band_id={self.band_id}, date={as_of_date} "
                    f"({type(e).__name__}: {e}) — falling back to live query. "
                    f"Likely a build in progress (DuckDB single-writer lock)."
                )
                cached = None
            if cached is not None:
                return cast(List[str], cached)
            logger.debug(
                f"Universe cache miss for band_id={self.band_id}, date={as_of_date} "
                f"— falling back to live query. Run scripts/build_universe_cache.py "
                f"to cover this date."
            )

        from momentum_framework.common.band_universe import resolve_band_universe
        return cast(List[str], resolve_band_universe(self.band_id, as_of_date, conn))

    def describe(self) -> Dict[str, Any]:
        """Full parameter dict — used for strategy_id generation and reports."""
        return {
            "strategy_code": self.strategy_code,
            "rank_method": self.rank_method,
            "band_id": self.band_id,
            "top_n": self.top_n,
            "lookback_months": self.lookback_months,
            "rebalance_cadence_days": self.rebalance_cadence_days,
            **self.extra_params,
        }

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.describe()})"
