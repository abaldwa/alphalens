"""
R13: Bollinger Band Mean-Reversion

Uses common/bollinger_signal.py::BollingerBandSignal — a signal genuinely
distinct from the shared TrailingMomentumSignal family and from
PctOf52WeekHighSignal (R11). Buys stocks near their LOWER Bollinger Band
(oversold, %B close to 0); the opposite convention from every
trailing_return strategy.

Ported from strategies/migrations/r13_bollinger_mean_reversion.py's
declared grid + backtest/adapters/momentum_adapter.py's
rank_method="bollinger_mean_reversion" branch (calls
features/momentum_signal.py::bollinger_mean_reversion(), same formula,
reimplemented pandas-only in common/bollinger_signal.py — see that
file's docstring for why).
"""

from typing import Any, Dict, FrozenSet, List, cast

import pandas as pd

from momentum_framework.backtesting.adapter import Signal
from momentum_framework.common.bollinger_signal import BollingerBandSignal
from momentum_framework.queues.generator import QueueGenerator
from momentum_framework.strategies.base import StrategyBase

STRATEGY_CODE = "R13"
RANK_METHOD = "bollinger_mean_reversion"
DEFAULT_WINDOW = 20  # standard Bollinger Band window
DEFAULT_NUM_STD = 2.0


class R13BollingerReversal(StrategyBase):
    """Bollinger Band %B mean-reversion: buy stocks nearest their lower band."""

    strategy_code = STRATEGY_CODE
    rank_method = RANK_METHOD
    citation = "George & Hwang (2004)-style mean reversion, Bollinger Band variant"

    def __init__(self, band_id: int, top_n: int, rebalance_cadence_days: int,
                 filter_preset: str = "all_risk", bollinger_window: int = DEFAULT_WINDOW,
                 **kwargs: Any):
        # Bollinger has no "lookback_months" concept — the window is in
        # trading days (bollinger_window), not months. lookback_months=1 is
        # a deliberate placeholder (must be >0 to satisfy QueueValidator's
        # positivity check) to fit StrategyAdapter's shared constructor
        # signature; it plays no role in this strategy's ranking.
        super().__init__(band_id, top_n, 1, rebalance_cadence_days,
                          filter_preset=filter_preset, bollinger_window=bollinger_window, **kwargs)
        self.bollinger_window = bollinger_window
        self.signal = BollingerBandSignal(window=bollinger_window, num_std=DEFAULT_NUM_STD)

    def rebalance(self, as_of_date: str, universe: List[str], conn: Any,
                  held: FrozenSet[str], equity_curve: pd.Series) -> List[Signal]:
        scores = self.signal.compute(conn, universe, as_of_date, self.bollinger_window)
        # ascending=True: LOWEST %B first (closest to lower band = most oversold)
        oversold = scores.sort_values(ascending=True).head(self.top_n)
        return [
            Signal(ticker=str(ticker), action="buy", conviction=1.0 - score, rank=rank + 1)
            for rank, (ticker, score) in enumerate(oversold.items())
        ]


class R13QueueGenerator(QueueGenerator):
    """
    Standard grid — same shape as R01/R03/R07-R12/R14-R17, M13 included,
    EXCEPT lookback_months is fixed at [1] (a placeholder — Bollinger has no lookback_months
    concept, see R13BollingerReversal's constructor comment) — bollinger_window
    (trading days) is the real parameter, held at DEFAULT_WINDOW here.
    """

    strategy_family = STRATEGY_CODE

    BANDS = [2, 4, 7, 9, 10, 12, 13]
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
            lookback_months=[1],  # placeholder — see R13BollingerReversal constructor comment
            rebalance_cadences=self.REBALANCE_CADENCES,
            start_date=self.start_date,
            end_date=self.end_date,
            filter_presets=self.FILTER_PRESETS,
            extra_fields={"bollinger_window": DEFAULT_WINDOW},
        ))
