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

from typing import Any, Dict, List, Optional

from momentum_framework.backtesting.adapter import Signal
from momentum_framework.common.signals import TrailingMomentumSignal
from momentum_framework.queues.generator import QueueGenerator
from momentum_framework.strategies.base import StrategyBase

STRATEGY_CODE = "R12"
RANK_METHOD = "trailing_reversal_1mo"
REVERSAL_LOOKBACK_MONTHS = 1  # fixed — this IS the "1-month" in "1-Month Reversal"


class R12Reversal1Mo(StrategyBase):
    """1-month trailing-return reversal within an optional liquidity quintile."""

    strategy_code = STRATEGY_CODE
    rank_method = RANK_METHOD

    def __init__(self, band_id: int, top_n: int, rebalance_cadence_days: int,
                 filter_preset: str = "all_risk",
                 liquidity_quintile: Optional[int] = None,
                 **kwargs: Any):
        super().__init__(band_id, top_n, REVERSAL_LOOKBACK_MONTHS, rebalance_cadence_days,
                          filter_preset=filter_preset, liquidity_quintile=liquidity_quintile, **kwargs)
        self.signal = TrailingMomentumSignal(lookback_months=REVERSAL_LOOKBACK_MONTHS)
        self.liquidity_quintile = liquidity_quintile

    def rebalance(self, as_of_date: str, universe: List[str], conn: Any) -> List[Signal]:
        if self.liquidity_quintile is not None:
            from momentum_framework.common.liquidity import liquidity_quintile_universe
            universe = liquidity_quintile_universe(conn, universe, as_of_date, self.liquidity_quintile)
            if not universe:
                return []

        scores = self.signal.compute(conn, universe, as_of_date, self.signal.lookback_days)
        # ascending=True: LOWEST 1-month return first (strongest reversal signal)
        losers = scores.sort_values(ascending=True).head(self.top_n)
        return [
            Signal(ticker=str(ticker), action="buy", conviction=-score, rank=rank + 1)
            for rank, (ticker, score) in enumerate(losers.items())
        ]


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
            ))
        return jobs
