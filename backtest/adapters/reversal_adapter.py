"""
backtest/adapters/reversal_adapter.py

Phase: Phase 12 (R11/R12 reversal strategies)
Owner: Platform / Backtest
Consumers: backtest/run_orchestrator_backtest.py, backtest/run_strategy_queue.py

Specialized adapter for mean-reversion / reversal strategies that buy losers
(stocks with lowest trailing returns) rather than winners. Wraps MomentumAdapter
to override ranking direction for reversal signals.

Key difference from momentum_adapter.py:
- Momentum: rank_universe() → sort_values(ascending=False) → TOP winners
- Reversal: rank_universe() → sort_values(ascending=True) → TOP losers

This adapter does NOT modify momentum_adapter itself — it provides a reversal-
specific wrapper that can be used in place of MomentumAdapter for reversal
strategies like R11 (pure reversal) and R12 (reversal vs momentum comparison).
"""

import logging
from datetime import date as date_type
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from backtest.adapters.momentum_adapter import MomentumAdapter
from backtest.core.engine import Signal
from backtest.core.horizon import HorizonBucket

logger = logging.getLogger(__name__)


class ReversalAdapter(MomentumAdapter):
    """
    Mean-reversion adapter: buys losers (lowest recent returns) instead of winners.

    Inherits all MomentumAdapter behavior but overrides:
    - generate_signals(): flips sort order to select LOW performers
    - feature_vector(): labels reversal-specific metrics

    All MomentumAdapter parameters apply, with rank_method expected to be
    "trailing_reversal_1mo" (or any reversal-based rank method).
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize as MomentumAdapter. Detects reversal rank_method automatically."""
        super().__init__(*args, **kwargs)
        self._is_reversal = self.rank_method == "trailing_reversal_1mo"
        if self._is_reversal:
            logger.info(f"ReversalAdapter initialized: rank_method={self.rank_method}, will select LOSERS")

    def generate_signals(
        self, universe: List[str], as_of_date: date_type, horizon_bucket: HorizonBucket
    ) -> List[Signal]:
        """
        Generate reversal signals by selecting LOSERS (lowest returns) instead of winners.

        Process:
        1. Compute trailing returns (same as momentum)
        2. Filter buy pool (same as momentum)
        3. Select TOP LOSERS (ascending=True) instead of TOP WINNERS (ascending=False)
        4. Execute sells for holdings that left top-loser list

        Returns: List[Signal] with buy/sell actions for loser rotation.
        """
        # Sticky-promotion (Phase 3): extend the ranking pool BEFORE
        # momentum is computed, so a promoted holding's score is real and
        # it genuinely competes for a top_n slot rather than being scored
        # after the cut. No-op unless rank_start + yearly_rank_lookup were
        # both supplied.
        sticky = self._sticky_promoted_holdings(universe, as_of_date)
        if sticky:
            universe = list(universe) + sorted(sticky)

        # [ML40] One ranking implementation, shared with MomentumAdapter.
        # [Phase 0] Support custom rank functions (rank_fn) for R-family strategies.
        rank_fn = self.rank_fn or (
            lambda price_panel, universe, date, lookback_days: (
                pd.Series(0.0, index=universe) if not universe else
                price_panel.loc[:date, universe].pct_change().iloc[-1]
            )
        )
        momentum = rank_universe(self.price_panel, universe, as_of_date, self.lookback_days, rank_fn=rank_fn)  # type: ignore[operator]
        self._last_momentum = momentum

        if momentum.empty:
            return []

        # [Phase 4] Sector momentum (if applicable, same as momentum)
        if self.rank_method == "industry_momentum" and self._sector_lookup:
            from features.momentum_strategy import rank_sectors, rank_constituents_within_sectors
            sector_scores = rank_sectors(momentum, self._sector_lookup, self.top_sectors)
            if not sector_scores.empty:
                top_sectors_list = sector_scores.head(self.top_sectors).index.tolist()
                momentum = rank_constituents_within_sectors(momentum, self._sector_lookup, top_sectors_list)
            if momentum.empty:
                return []

        # BUY side: the filtered pool
        pool = self._selection_pool(momentum, as_of_date)

        # *** REVERSAL DIFFERENCE: Sort ASCENDING to select LOSERS ***
        target = (
            set(pool.sort_values(ascending=True).head(self.top_n).index)
            if not pool.empty else set()
        )
        logger.debug(f"ReversalAdapter: {as_of_date} selected {len(target)} losers (ascending=True)")

        # HOLD side: the same cut on RAW momentum, before any filter
        # *** REVERSAL DIFFERENCE: Sort ASCENDING for losers ***
        keep = (
            set(momentum.sort_values(ascending=True).head(self.top_n).index)
            if not momentum.empty else set()
        )

        # [2026-08-18, user decision] The rotation is a plain list swap
        buys_disabled = self._is_buys_disabled(as_of_date)
        in_crash_regime = self._is_crash_regime_today(as_of_date)
        if in_crash_regime and self.crash_disable_buys:
            buys_disabled = True

        signals: List[Signal] = []
        new_held: Set[str] = set()

        # Sell signals: holdings that fell out of top-loser list
        for ticker in sorted(self._held):
            if ticker in keep:
                new_held.add(ticker)
                continue
            if self._is_circuit_locked(as_of_date, ticker):
                new_held.add(ticker)
                continue
            signals.append(Signal(
                ticker=ticker, action="sell", sector=self._sector_lookup.get(ticker, "Unknown"),
                conviction=0.0, adtv_cr=self._adtv_cr(ticker, as_of_date),
            ))

        # Buy signals: new entrants into top-loser list
        new_entrants = [] if buys_disabled else sorted(target - self._held)

        # [Phase R0] Per-ticker volatility weighting
        weight_mults: Optional[pd.Series] = None
        if new_entrants and self.weight_method is not None:
            from features.volatility_scaling import WEIGHT_DISPATCH
            weight_fn = WEIGHT_DISPATCH[self.weight_method]
            kwargs: Dict[str, Any] = {"lookback_days": self.weight_lookback_days}
            if self.weight_method == "target_volatility":
                kwargs["target_vol"] = self.vol_target_pct
            weight_mults = weight_fn(self.price_panel, sorted(target), as_of_date, **kwargs)

        for ticker in new_entrants:
            conviction = momentum.get(ticker, 0.0)
            # Size multiplier: weighted or uniform
            size_mult_for_signal = 1.0 / self.top_n
            if weight_mults is not None and ticker in weight_mults.index:
                size_mult_for_signal = float(weight_mults[ticker])

            # Exposure multiplier: crash regime or vol scaling (same as momentum)
            exposure_mult = self._compute_exposure_multiplier_today(as_of_date)
            total_mult = size_mult_for_signal * exposure_mult

            signals.append(Signal(
                ticker=ticker, action="buy", sector=self._sector_lookup.get(ticker, "Unknown"),
                conviction=conviction, adtv_cr=self._adtv_cr(ticker, as_of_date),
                size_multiplier=total_mult,
            ))
            new_held.add(ticker)

        self._held = new_held
        return signals

    def feature_vector(self, ticker: str, as_of_date: date_type) -> Dict[str, Any]:  # type: ignore[override]
        """Return reversal-specific feature vector (lowest recent return = best reversal candidate)."""
        return {
            "trailing_return": float(self._last_momentum.get(ticker)) if ticker in self._last_momentum.index else None,
            "lookback_days": self.lookback_days,
            "in_top_n_losers": ticker in self._held,
            "reversal_score": -float(self._last_momentum.get(ticker)) if ticker in self._last_momentum.index else None,
        }


def rank_universe(
    price_panel: pd.DataFrame,
    universe: List[str],
    as_of_date: date_type,
    lookback_days: int,
    rank_fn=None,
) -> pd.Series:
    """
    Compute trailing returns for universe (same as momentum_adapter, imported here for clarity).
    This is a wrapper to features.momentum_signal.rank_universe.
    """
    from features.momentum_strategy import rank_universe as momentum_rank_universe
    return momentum_rank_universe(price_panel, universe, as_of_date, lookback_days, rank_fn=rank_fn)
