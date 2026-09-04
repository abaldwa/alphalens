"""
Position Weighting Schemes — R14-R17's ONLY point of difference.

R14, R15, R16, R17 all rank their band's universe by the exact same
signal (TrailingMomentumSignal — see common/signals.py) and select the
exact same top_n basket; the only thing that differs between them is how
much capital each selected ticker gets. That is deliberately factored
into these four interchangeable WeightingScheme classes rather than
duplicated per strategy file, so a strategy file is nothing but "which
scheme" plus the shared ranking (see strategies/r14_inverse_volatility.py
for the pattern).

This is the direct replacement for what was R0's `weight_method`
parameter (see project_r0_split_r14_r17 memory for why splitting into 4
strategy_codes was chosen over one parameterized strategy).
"""

from abc import ABC, abstractmethod
from typing import Any, List
import pandas as pd

from momentum_framework.common.volatility import (
    daily_returns,
    downside_volatility,
    realized_variance,
    realized_volatility,
)

DEFAULT_WEIGHT_LOOKBACK_DAYS = 126  # 6 months, matches legacy R0's weight_lookback_days


class WeightingScheme(ABC):
    """Turns a basket of tickers into normalized portfolio weights."""

    #: Matches the legacy weight_method string this scheme replaces —
    #: used only for cross-referencing the results traceability baseline
    #: (legacy_runs_baseline.csv's weight_method column), never for identity.
    legacy_weight_method: str = "unset"

    def __init__(self, lookback_days: int = DEFAULT_WEIGHT_LOOKBACK_DAYS):
        self.lookback_days = lookback_days

    @abstractmethod
    def compute_weights(self, tickers: List[str], as_of_date: str, conn: Any) -> pd.Series:
        """
        Returns a pd.Series indexed by ticker, weights summing to 1.0.
        A ticker with unmeasurable volatility (insufficient history) is
        DROPPED, not given a fabricated default weight — the caller
        should treat a dropped ticker as excluded from that rebalance,
        same convention as common/signals.py's momentum computation.
        """
        raise NotImplementedError

    @staticmethod
    def _normalize(raw_weights: pd.Series) -> pd.Series:
        raw_weights = raw_weights.dropna()
        raw_weights = raw_weights[raw_weights > 0]
        if raw_weights.empty:
            return raw_weights
        return raw_weights / raw_weights.sum()


class InverseVolatilityWeighting(WeightingScheme):
    """weight_i ∝ 1 / vol_i — R14. Simple risk parity: less volatile names get more capital."""

    legacy_weight_method = "inverse_volatility"

    def compute_weights(self, tickers: List[str], as_of_date: str, conn: Any) -> pd.Series:
        returns = daily_returns(conn, tickers, as_of_date, self.lookback_days)
        vol = realized_volatility(returns)
        vol = vol[vol > 0]
        return self._normalize(1.0 / vol)


class InverseVarianceWeighting(WeightingScheme):
    """weight_i ∝ 1 / vol_i² — R15. Barroso-Santa-Clara-style risk parity (squared penalty on vol)."""

    legacy_weight_method = "inverse_variance"

    def compute_weights(self, tickers: List[str], as_of_date: str, conn: Any) -> pd.Series:
        returns = daily_returns(conn, tickers, as_of_date, self.lookback_days)
        var = realized_variance(returns)
        var = var[var > 0]
        return self._normalize(1.0 / var)


class TargetVolatilityWeighting(WeightingScheme):
    """
    weight_i ∝ target_vol / vol_i, capped — R16. Scales each position so
    its OWN contribution approaches a target annualized vol, rather than
    just ranking names by relative volatility.
    """

    legacy_weight_method = "target_volatility"

    def __init__(self, target_vol: float = 0.15, leverage_cap: float = 1.0,
                 lookback_days: int = DEFAULT_WEIGHT_LOOKBACK_DAYS):
        super().__init__(lookback_days)
        self.target_vol = target_vol
        self.leverage_cap = leverage_cap

    def compute_weights(self, tickers: List[str], as_of_date: str, conn: Any) -> pd.Series:
        returns = daily_returns(conn, tickers, as_of_date, self.lookback_days)
        vol = realized_volatility(returns)
        vol = vol[vol > 0]
        raw = (self.target_vol / vol).clip(upper=self.leverage_cap)
        return self._normalize(raw)


class DownsideVolatilityWeighting(WeightingScheme):
    """weight_i ∝ 1 / downside_vol_i — R17. Sortino-style: only penalizes downside variance, not upside swings."""

    legacy_weight_method = "downside_volatility"

    def compute_weights(self, tickers: List[str], as_of_date: str, conn: Any) -> pd.Series:
        returns = daily_returns(conn, tickers, as_of_date, self.lookback_days)
        dvol = downside_volatility(returns)
        dvol = dvol[dvol > 0]
        return self._normalize(1.0 / dvol)
