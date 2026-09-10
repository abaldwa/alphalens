"""
R12: 1-Month Reversal + Liquidity

Reuses the SAME TrailingMomentumSignal every trailing_return strategy
uses (see common/signals.py's module note) — confirmed against
strategies/migrations/r12_momentum_reversal_liquidity.py's own
docstring: "ranks the band's universe by 1-month reversal (low returns =
strong reversal signal) and buys the top N." That is this class with
lookback_months=1, selecting the LOWEST scores (losers) instead of
TrailingMomentumSignal's default highest-wins convention — not a
different signal, only a reversed selection direction on identical
output (rank_method="trailing_reversal_1mo" in the legacy schema).

The "+ Liquidity" half of R12's name (spec 7.12: "tests interaction of
reversal signal with liquidity quintiles") IS NOW MODELED, 2026-09-04 —
common/liquidity.py::liquidity_quintile_universe() restricts the band's
universe to ONE ADTV quintile (1=least liquid, 5=most liquid) BEFORE
reversal ranking runs, so a job's results are directly comparable across
quintiles (does the reversal effect hold in illiquid names, or only
liquid ones?). `liquidity_quintile=None` (the default) skips the filter
entirely — ranks the full band, matching the plain reversal signal with
no liquidity interaction.
"""

from typing import Any, Dict, FrozenSet, List, Optional

import pandas as pd

from momentum_framework.backtesting.adapter import Signal
from momentum_framework.common.crash_regime import (
    CrashRegimeGuardMixin,
    DEFAULT_CRASH_DRAWDOWN_THRESHOLD,
    DEFAULT_CRASH_VOL_LOOKBACK_DAYS,
    DEFAULT_CRASH_VOL_PERCENTILE_THRESHOLD,
)
from momentum_framework.common.signals import TrailingMomentumSignal
from momentum_framework.queues.generator import QueueGenerator
from momentum_framework.strategies.base import StrategyBase

STRATEGY_CODE = "R12"
RANK_METHOD = "trailing_reversal_1mo"
REVERSAL_LOOKBACK_MONTHS = 1  # fixed — this IS the "1-month" in "1-Month Reversal"


class R12Reversal1Mo(CrashRegimeGuardMixin, StrategyBase):
    """1-month trailing-return reversal within an optional liquidity quintile.

    Optional crash-regime guard (Category B6 Option B, 2026-09-06,
    disabled by default) — see common/crash_regime.py::
    CrashRegimeGuardMixin and R11's class docstring for the mechanism.
    """

    strategy_code = STRATEGY_CODE
    rank_method = RANK_METHOD

    def __init__(self, band_id: int, top_n: int, rebalance_cadence_days: int,
                 filter_preset: str = "all_risk",
                 liquidity_quintile: Optional[int] = None,
                 crash_regime_enabled: bool = False,
                 crash_drawdown_threshold: float = DEFAULT_CRASH_DRAWDOWN_THRESHOLD,
                 crash_vol_percentile_threshold: float = DEFAULT_CRASH_VOL_PERCENTILE_THRESHOLD,
                 crash_vol_lookback_days: int = DEFAULT_CRASH_VOL_LOOKBACK_DAYS,
                 **kwargs: Any):
        super().__init__(band_id, top_n, REVERSAL_LOOKBACK_MONTHS, rebalance_cadence_days,
                          filter_preset=filter_preset, liquidity_quintile=liquidity_quintile,
                          crash_regime_enabled=crash_regime_enabled,
                          crash_drawdown_threshold=crash_drawdown_threshold,
                          crash_vol_percentile_threshold=crash_vol_percentile_threshold,
                          crash_vol_lookback_days=crash_vol_lookback_days,
                          **kwargs)
        self.signal = TrailingMomentumSignal(lookback_months=REVERSAL_LOOKBACK_MONTHS)
        self.liquidity_quintile = liquidity_quintile
        self.crash_regime_enabled = crash_regime_enabled
        self.crash_drawdown_threshold = crash_drawdown_threshold
        self.crash_vol_percentile_threshold = crash_vol_percentile_threshold
        self.crash_vol_lookback_days = crash_vol_lookback_days
        self._benchmark_equity = None

    def rebalance(self, as_of_date: str, universe: List[str], conn: Any,
                  held: FrozenSet[str], equity_curve: pd.Series) -> List[Signal]:
        if self.liquidity_quintile is not None:
            from momentum_framework.common.liquidity import liquidity_quintile_universe
            universe = liquidity_quintile_universe(conn, universe, as_of_date, self.liquidity_quintile)
            if not universe:
                return []

        scores = self.signal.compute(conn, universe, as_of_date, self.signal.lookback_days)
        # ascending=True: LOWEST 1-month return first (strongest reversal signal)
        losers = scores.sort_values(ascending=True).head(self.top_n)
        target = set(losers.index)

        if self._in_crash_regime(as_of_date, conn):
            allowed = set(held) & target
        else:
            allowed = target

        signals: List[Signal] = []
        rank = 0
        for ticker, score in losers.items():
            if ticker not in allowed:
                continue
            rank += 1
            signals.append(Signal(ticker=str(ticker), action="buy", conviction=-score, rank=rank))
        return signals


class R12QueueGenerator(QueueGenerator):
    """
    Standard grid — same shape as R01/R03/R07-R11/R14-R17, M13 included,
    EXCEPT lookback_months is fixed at 1 (that's the strategy's defining
    parameter, not a sweep dimension — see REVERSAL_LOOKBACK_MONTHS).

    Sweeps liquidity_quintile over [None, 1, 2, 3, 4, 5] as a genuine grid
    dimension — None = no liquidity filter (plain reversal), 1-5 = the
    reversal signal tested within just that ADTV quintile.
    """

    strategy_family = STRATEGY_CODE

    BANDS = [2, 4, 7, 9, 10, 12, 13]
    REBALANCE_CADENCES = [5, 10, 21]
    FILTER_PRESETS = ["all_risk"]
    LIQUIDITY_QUINTILES: List[Optional[int]] = [None, 1, 2, 3, 4, 5]

    def __init__(self, start_date: str = "2009-01-01", end_date: str = "2026-06-30"):
        super().__init__()
        self.start_date = start_date
        self.end_date = end_date

    def build_jobs(self) -> List[Dict[str, Any]]:
        jobs = []
        for quintile in self.LIQUIDITY_QUINTILES:
            jobs.extend(self.simple_momentum_grid(
                strategy_code=STRATEGY_CODE,
                rank_method=RANK_METHOD,
                bands=self.BANDS,
                lookback_months=[REVERSAL_LOOKBACK_MONTHS],
                rebalance_cadences=self.REBALANCE_CADENCES,
                start_date=self.start_date,
                end_date=self.end_date,
                filter_presets=self.FILTER_PRESETS,
                extra_fields={"liquidity_quintile": quintile},
                crash_regime_enabled=True,  # user decision 2026-09-06 (B6): current default is guard ON
            ))
        return jobs
