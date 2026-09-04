"""
R14: Trailing Momentum, Inverse-Volatility Weighted

Same ranking as R01 (trailing_return, no skip) — the ONLY difference from
R01 is position sizing: instead of equal-weighting the top_n basket, each
ticker's weight is proportional to 1/volatility (see
common/position_weighting.py::InverseVolatilityWeighting).

This is one of R0's 4 former weight_method variants, split into its own
standalone strategy 2026-09-04 — see project_r0_split_r14_r17 memory for
why (R08/R09 precedent: a distinct vol-scaling methodology gets its own
R-number rather than being a parameter of one strategy).
"""

from typing import Any, Dict, List

from momentum_framework.common.position_weighting import InverseVolatilityWeighting
from momentum_framework.queues.generator import QueueGenerator
from momentum_framework.strategies.base import WeightedMomentumStrategy

STRATEGY_CODE = "R14"
RANK_METHOD = "trailing_return"


class R14InverseVolatility(WeightedMomentumStrategy):
    strategy_code = STRATEGY_CODE
    rank_method = RANK_METHOD
    weighting_scheme_class = InverseVolatilityWeighting


class R14QueueGenerator(QueueGenerator):
    """Standard grid — same shape as R01/R03 (see those files), M13 included via band_top_n_pairs()."""

    strategy_family = STRATEGY_CODE

    BANDS = [2, 4, 7, 9, 10, 12, 13]
    LOOKBACK_MONTHS = [3, 6, 9, 12]
    REBALANCE_CADENCES = [5, 10, 21]
    FILTER_PRESETS = ["all_risk"]

    def __init__(self, start_date: str = "2009-01-01", end_date: str = "2026-06-30"):
        super().__init__()
        self.start_date = start_date
        self.end_date = end_date

    def build_jobs(self) -> List[Dict[str, Any]]:
        return self.simple_momentum_grid(
            strategy_code=STRATEGY_CODE,
            rank_method=RANK_METHOD,
            bands=self.BANDS,
            lookback_months=self.LOOKBACK_MONTHS,
            rebalance_cadences=self.REBALANCE_CADENCES,
            start_date=self.start_date,
            end_date=self.end_date,
            filter_presets=self.FILTER_PRESETS,
            weight_method="inverse_volatility",
        )
