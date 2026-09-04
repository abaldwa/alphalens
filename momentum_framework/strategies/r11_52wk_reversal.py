"""
R11: 52-Week-High Reversal

Uses common/signals.py::PctOf52WeekHighSignal (close / trailing 52-week
high, values in [0,1]) — a DIFFERENT signal from the shared
TrailingMomentumSignal family (R01/R03/R07-R09/R14-R17/R10/R12), since
proximity-to-high is a genuinely different computation from a trailing
return. Selects the LOWEST scores (furthest from the 52-week high —
oversold), the opposite convention from every trailing_return strategy.

Ported from strategies/migrations/r11_52wk_high_momentum.py +
backtest/adapters/momentum_adapter.py's select_lowest=True branch.

Distinguished from the REJECTED R05 (same rank_method="pct_of_52wk_high",
but select_lowest=False — buys stocks NEAR their highs, not away from
them) by this file's hardcoded SELECT_LOWEST=True. R05 is deliberately
not ported — see docs/CODE_TRACEABILITY.md's R05 row and
project_strategy_identity_bug_r_vs_m memory for why (rejected at the
Phase 3 gate, historical reference only).
"""

from typing import Any, Dict, FrozenSet, List, cast

import pandas as pd

from momentum_framework.backtesting.adapter import Signal
from momentum_framework.common.signals import PctOf52WeekHighSignal
from momentum_framework.queues.generator import QueueGenerator
from momentum_framework.strategies.base import StrategyBase

STRATEGY_CODE = "R11"
RANK_METHOD = "pct_of_52wk_high"
SELECT_LOWEST = True  # the defining difference from the rejected R05 — never set False here
LOOKBACK_DAYS_52WK = 252


class R11FiftyTwoWeekReversal(StrategyBase):
    """52-week-high reversal: buy stocks furthest from their 52-week high."""

    strategy_code = STRATEGY_CODE
    rank_method = RANK_METHOD
    citation = "George & Hwang (2004), Journal of Finance — 52-week high reversal"

    def __init__(self, band_id: int, top_n: int, lookback_months: int,
                 rebalance_cadence_days: int, filter_preset: str = "all_risk",
                 **kwargs: Any):
        super().__init__(band_id, top_n, lookback_months, rebalance_cadence_days,
                          filter_preset=filter_preset, select_lowest=SELECT_LOWEST, **kwargs)
        self.signal = PctOf52WeekHighSignal()

    def rebalance(self, as_of_date: str, universe: List[str], conn: Any,
                  held: FrozenSet[str], equity_curve: pd.Series) -> List[Signal]:
        scores = self.signal.compute(conn, universe, as_of_date, LOOKBACK_DAYS_52WK)
        # ascending=True: LOWEST pct-of-52wk-high first (furthest from high = most oversold)
        losers = scores.sort_values(ascending=True).head(self.top_n)
        return [
            Signal(ticker=str(ticker), action="buy", conviction=1.0 - score, rank=rank + 1)
            for rank, (ticker, score) in enumerate(losers.items())
        ]


class R11QueueGenerator(QueueGenerator):
    """Standard grid — same shape as R01/R03/R07-R10/R14-R17, M13 included via band_top_n_pairs().

    lookback_months is accepted for grid-shape consistency with every
    other strategy's generator but is NOT used by the 52-week-high signal
    itself (that window is fixed at LOOKBACK_DAYS_52WK) — included so
    R11's strategy_id still varies by lookback like every other strategy's
    does, keeping cross-strategy comparison tables uniformly shaped.
    """

    strategy_family = STRATEGY_CODE

    BANDS = [2, 4, 7, 9, 10, 12, 13]
    LOOKBACK_MONTHS = [12]  # single value — the 52wk window itself is fixed, see class docstring
    REBALANCE_CADENCES = [5, 10, 21]
    FILTER_PRESETS = ["all_risk"]

    def __init__(self, start_date: str = "2009-01-01", end_date: str = "2026-06-30"):
        super().__init__()
        self.start_date = start_date
        self.end_date = end_date

    def build_jobs(self) -> List[Dict[str, Any]]:
        return cast(List[Dict[str, Any]], self.simple_momentum_grid(
            strategy_code=STRATEGY_CODE,
            rank_method=RANK_METHOD,
            bands=self.BANDS,
            lookback_months=self.LOOKBACK_MONTHS,
            rebalance_cadences=self.REBALANCE_CADENCES,
            start_date=self.start_date,
            end_date=self.end_date,
            filter_presets=self.FILTER_PRESETS,
            extra_fields={"select_lowest": SELECT_LOWEST},
        ))
