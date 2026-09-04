"""
R10: Sector Momentum (Nigam-Pandey style)

Two-stage ranking built on the SAME shared trailing-return computation as
R01/R03/R07/R08/R09/R14-R17 (see common/signals.py::IndustryMomentumSignal
and its module note) — rank every ticker in the band by trailing return,
average by sector to rank sectors, keep only the top_sectors sectors,
then select top_n from what's left.

Ported from strategies/migrations/r10_nigam_pandey_momentum.py's
declared grid + backtest/adapters/momentum_adapter.py's
rank_method="industry_momentum" branch (calls
features/momentum_strategy.py::rank_sectors() /
rank_constituents_within_sectors(), same formulas, ported unchanged into
common/sector_ranking.py).

`sector_lookup` (ticker -> sector name) is AUTO-RESOLVED from
`stock_master.sector` (common/sector_data.py, verified 100% coverage
2026-09-04) if not explicitly supplied — see
common/signals.py::IndustryMomentumSignal for where. Pass it explicitly
only to override with a different mapping (e.g. a future point-in-time
sector table, should one exist — stock_master is a static current
snapshot, not point-in-time, see common/sector_data.py's docstring).
"""

from typing import Any, Dict, FrozenSet, List, Optional, cast

import pandas as pd

from momentum_framework.backtesting.adapter import Signal
from momentum_framework.common.signals import IndustryMomentumSignal
from momentum_framework.queues.generator import QueueGenerator
from momentum_framework.strategies.base import StrategyBase

STRATEGY_CODE = "R10"
RANK_METHOD = "industry_momentum"
DEFAULT_TOP_SECTORS = 5


class R10SectorMomentum(StrategyBase):
    """Sector-filtered trailing-return momentum (Nigam-Pandey style)."""

    strategy_code = STRATEGY_CODE
    rank_method = RANK_METHOD
    citation = "Nigam & Pandey — sector-level momentum rotation"

    def __init__(
        self,
        band_id: int,
        top_n: int,
        lookback_months: int,
        rebalance_cadence_days: int,
        sector_lookup: Optional[Dict[str, str]] = None,
        filter_preset: str = "all_risk",
        top_sectors: int = DEFAULT_TOP_SECTORS,
        **kwargs: Any,
    ):
        super().__init__(
            band_id, top_n, lookback_months, rebalance_cadence_days,
            filter_preset=filter_preset, top_sectors=top_sectors, **kwargs,
        )
        self.top_sectors = top_sectors
        self.signal = IndustryMomentumSignal(
            lookback_months=lookback_months, sector_lookup=sector_lookup, top_sectors=top_sectors,
        )

    def rebalance(self, as_of_date: str, universe: List[str], conn: Any,
                  held: FrozenSet[str], equity_curve: pd.Series) -> List[Signal]:
        scores = self.signal.compute(conn, universe, as_of_date)
        winners = scores.sort_values(ascending=False).head(self.top_n)
        return [
            Signal(ticker=str(ticker), action="buy", conviction=score, rank=rank + 1)
            for rank, (ticker, score) in enumerate(winners.items())
        ]


class R10QueueGenerator(QueueGenerator):
    """Standard grid — same shape as R01/R03/R07-R09/R14-R17, M13 included via band_top_n_pairs()."""

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
        return cast(List[Dict[str, Any]], self.simple_momentum_grid(
            strategy_code=STRATEGY_CODE,
            rank_method=RANK_METHOD,
            bands=self.BANDS,
            lookback_months=self.LOOKBACK_MONTHS,
            rebalance_cadences=self.REBALANCE_CADENCES,
            start_date=self.start_date,
            end_date=self.end_date,
            filter_presets=self.FILTER_PRESETS,
            extra_fields={"top_sectors": DEFAULT_TOP_SECTORS},
        ))
