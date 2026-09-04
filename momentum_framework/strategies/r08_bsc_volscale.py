"""
R08: Barroso-Santa-Clara Volatility-Target Overlay

Same shared ranking as R01/R03/R07/R14-R17 (see common/signals.py::
TrailingMomentumSignal's module note) — the only difference is a
PORTFOLIO-LEVEL exposure multiplier applied after selection: scale the
whole book's exposure to target a constant annualized volatility
(target_vol / realized_vol, capped), rather than a per-ticker weighting
scheme (that's R14-R17, an orthogonal, different mechanism — see
common/portfolio_vol_scaling.py's module docstring for the distinction).

Ported from backtest/adapters/momentum_adapter.py's vol_target_enabled
branch + features/momentum_signal.py::realized_vol_target_multiplier().

ARCHITECTURE NOTE: this strategy's exposure multiplier depends on its
OWN realized portfolio value history — received as `equity_curve`, a
rebalance() parameter (see StrategyAdapter.rebalance()'s PURE-FUNCTION
CONTRACT docstring). Previously this was self-tracked via a mutating
update_portfolio_equity() hook and a self._equity_history attribute;
removed 2026-09-04 (explicit user instruction — strategies must not own
mutable state duplicating what BacktestOrchestrator.run_native() already
computes in its simulation loop).
"""

from typing import Any, Dict, FrozenSet, List, cast

import pandas as pd

from momentum_framework.backtesting.adapter import Signal
from momentum_framework.common.portfolio_vol_scaling import vol_target_multiplier
from momentum_framework.common.signals import TrailingMomentumSignal
from momentum_framework.queues.generator import QueueGenerator
from momentum_framework.strategies.base import StrategyBase

STRATEGY_CODE = "R08"
RANK_METHOD = "trailing_return"

DEFAULT_VOL_TARGET_PCT = 0.15
DEFAULT_VOL_TARGET_LOOKBACK_DAYS = 63  # matches legacy generate_r8_queue.py default
DEFAULT_LEVERAGE_CAP = 1.0


class R08BSCVolScale(StrategyBase):
    """Trailing-return momentum with Barroso-Santa-Clara portfolio vol-targeting."""

    strategy_code = STRATEGY_CODE
    rank_method = RANK_METHOD
    citation = "Barroso & Santa-Clara (2015), Journal of Financial and Quantitative Analysis"

    def __init__(
        self,
        band_id: int,
        top_n: int,
        lookback_months: int,
        rebalance_cadence_days: int,
        filter_preset: str = "all_risk",
        vol_target_pct: float = DEFAULT_VOL_TARGET_PCT,
        vol_target_lookback_days: int = DEFAULT_VOL_TARGET_LOOKBACK_DAYS,
        vol_target_leverage_cap: float = DEFAULT_LEVERAGE_CAP,
        **kwargs: Any,
    ):
        super().__init__(
            band_id, top_n, lookback_months, rebalance_cadence_days,
            filter_preset=filter_preset, vol_target_enabled=True,
            vol_target_pct=vol_target_pct,
            vol_target_lookback_days=vol_target_lookback_days,
            vol_target_leverage_cap=vol_target_leverage_cap,
            **kwargs,
        )
        self.signal = TrailingMomentumSignal(lookback_months=lookback_months)
        self.vol_target_pct = vol_target_pct
        self.vol_target_lookback_days = vol_target_lookback_days
        self.vol_target_leverage_cap = vol_target_leverage_cap

    def _exposure_multiplier(self, as_of_date: str, equity_curve: pd.Series) -> float:
        if equity_curve.empty:
            return 1.0
        try:
            mult_series = vol_target_multiplier(
                equity_curve,
                target_vol=self.vol_target_pct,
                lookback_days=self.vol_target_lookback_days,
                leverage_cap=self.vol_target_leverage_cap,
            )
        except (ValueError, KeyError):
            return 1.0
        ts = pd.Timestamp(as_of_date)
        # Mirrors legacy off-by-one comment: rebalance runs before today's
        # equity is recorded, so use the most recent available multiplier.
        if ts in mult_series.index:
            return float(mult_series.loc[ts])
        if len(mult_series) > 0:
            return float(mult_series.iloc[-1])
        return 1.0

    def rebalance(self, as_of_date: str, universe: List[str], conn: Any,
                  held: FrozenSet[str], equity_curve: pd.Series) -> List[Signal]:
        scores = self.signal.compute(conn, universe, as_of_date, self.signal.lookback_days)
        winners = scores.sort_values(ascending=False).head(self.top_n)
        if winners.empty:
            return []

        exposure = self._exposure_multiplier(as_of_date, equity_curve)
        return [
            Signal(ticker=str(ticker), action="buy", conviction=score,
                   rank=rank + 1, size_multiplier=exposure)
            for rank, (ticker, score) in enumerate(winners.items())
        ]


class R08QueueGenerator(QueueGenerator):
    """Standard grid — same shape as R01/R03/R07/R14-R17, M13 included via band_top_n_pairs()."""

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
            vol_target_enabled=True,
            extra_fields={
                "vol_target_pct": DEFAULT_VOL_TARGET_PCT,
                "vol_target_lookback_days": DEFAULT_VOL_TARGET_LOOKBACK_DAYS,
                "vol_target_leverage_cap": DEFAULT_LEVERAGE_CAP,
            },
        ))
