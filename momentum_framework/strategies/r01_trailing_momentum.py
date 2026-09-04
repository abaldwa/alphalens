"""
R01: Trailing-Return Momentum (the original strategy)

This IS the strategy the project started with. Before the R-numbering
scheme existed, it was the only momentum strategy in the codebase and
was executed across all M-bands with four cumulative filter presets
(all_risk / balanced / risk_managed / max_defensive — see
common/filter_presets.py). It was later assigned the number "R1" (now
zero-padded to "R01" — see project_r_number_zero_padding memory, 2026-09-04)
once J&T's skip-month variant (see r03_jt_skipmonth.py) was added as a
distinct strategy.

Buys the top_n stocks ranked by trailing return over lookback_months,
with NO skip-month (skip_months=0 is what distinguishes this from R03 —
see that file's docstring for why the two must never share one file).

Citation: informally "momentum rotation" — this predates any specific
academic citation in the codebase; R03 is the one that explicitly
implements Jegadeesh & Titman (1993)'s skip-month methodology.
"""

from typing import Any, Dict, List

from momentum_framework.backtesting.adapter import Signal
from momentum_framework.common.signals import TrailingMomentumSignal
from momentum_framework.queues.generator import QueueGenerator
from momentum_framework.strategies.base import StrategyBase

STRATEGY_CODE = "R01"
RANK_METHOD = "trailing_return"
SKIP_MONTHS = 0  # the defining difference from R03 — never set this > 0 here


class R01TrailingMomentum(StrategyBase):
    """Original momentum strategy: trailing-return ranking, no skip-month."""

    strategy_code = STRATEGY_CODE
    rank_method = RANK_METHOD

    def __init__(self, band_id: int, top_n: int, lookback_months: int,
                 rebalance_cadence_days: int, filter_preset: str = "all_risk",
                 **kwargs: Any):
        super().__init__(band_id, top_n, lookback_months, rebalance_cadence_days,
                          filter_preset=filter_preset, skip_months=SKIP_MONTHS, **kwargs)
        self.signal = TrailingMomentumSignal(lookback_months=lookback_months)

    def rebalance(self, as_of_date: str, universe: List[str], conn: Any) -> List[Signal]:
        scores = self.signal.compute(conn, universe, as_of_date, self.signal.lookback_days)
        winners = scores.sort_values(ascending=False).head(self.top_n)
        return [
            Signal(ticker=str(ticker), action="buy", conviction=score, rank=rank + 1)
            for rank, (ticker, score) in enumerate(winners.items())
        ]


class R01QueueGenerator(QueueGenerator):
    """
    Standard R01 grid. skip_months is ALWAYS 0 here — a job needing
    skip_months > 0 belongs in r03_jt_skipmonth.py's generator, not this one
    (see that file's docstring for why merging them caused the Sept 2026
    strategy-identity bug).
    """

    strategy_family = STRATEGY_CODE

    # M13 (band_id=13) = full 800-stock ADTV universe, added 2026-09-04.
    # Its top_n set (10/20/30/40) comes from common/universe.py::TOP_N_BY_BAND
    # via band_top_n_pairs() below — never hardcode a single TOP_NS list
    # across all bands, M13 deliberately tests wider baskets than the
    # partitioned bands (M2/M4/M7/M9/M10/M12, top 5/10/15).
    BANDS = [2, 4, 7, 9, 10, 12, 13]
    LOOKBACK_MONTHS = [3, 6, 9, 12]
    REBALANCE_CADENCES = [5, 10, 21]
    FILTER_PRESETS = ["all_risk"]  # extend to run the balanced/risk_managed/max_defensive sweep

    def __init__(self, start_date: str = "2009-01-01", end_date: str = "2026-06-30"):
        super().__init__()
        self.start_date = start_date
        self.end_date = end_date

    def build_jobs(self) -> List[Dict[str, Any]]:
        jobs = []
        for band_id, top_n in self.band_top_n_pairs(self.BANDS):
            for lookback in self.LOOKBACK_MONTHS:
                for cadence in self.REBALANCE_CADENCES:
                    for filter_preset in self.FILTER_PRESETS:
                        jobs.append({
                            "kind": "orchestrator",
                            "channel": "momentum",
                            "start_date": self.start_date,
                            "end_date": self.end_date,
                            "rank_band_id": band_id,
                            "top_n": top_n,
                            "lookback_months": lookback,
                            "rebalance_cadence_days": cadence,
                            "rank_method": RANK_METHOD,          # explicit, always
                            "crash_regime_enabled": False,       # explicit, always
                            "skip_months": SKIP_MONTHS,          # explicit, always 0
                            "filter_preset": filter_preset,
                            "strategy_family": STRATEGY_CODE,
                            "capital_mode": "lump",
                            "initial_capital": 1_000_000,
                            "max_tickers": 800,
                            "min_history_days": 60,
                            "exit_variant": "unconstrained",
                        })
        return jobs
