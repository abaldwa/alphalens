"""
R15: Trailing Momentum, Inverse-Variance Weighted

Same shared ranking as R01/R03/R07/R08/R09/R14/R16/R17 (see
common/signals.py::TrailingMomentumSignal's module note) — the only
difference from R14 is the weighting formula: 1/vol² instead of 1/vol,
a Barroso-Santa-Clara-style squared penalty on volatility (see
common/position_weighting.py::InverseVarianceWeighting).

One of R0's 4 former weight_method variants, split into its own
standalone strategy 2026-09-04 — see project_r0_split_r14_r17 memory.
"""

from typing import Any, Dict, List

from momentum_framework.common.position_weighting import InverseVarianceWeighting
from momentum_framework.queues.generator import QueueGenerator
from momentum_framework.strategies.base import WeightedMomentumStrategy

STRATEGY_CODE = "R15"
RANK_METHOD = "trailing_return"


class R15InverseVariance(WeightedMomentumStrategy):
    strategy_code = STRATEGY_CODE
    rank_method = RANK_METHOD
    weighting_scheme_class = InverseVarianceWeighting


class R15QueueGenerator(QueueGenerator):
    """Standard grid — same shape as R01/R03/R14 (see those files), M13 included via band_top_n_pairs()."""

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
            weight_method="inverse_variance",
        )
