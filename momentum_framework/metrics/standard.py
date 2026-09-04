"""
Standard Metrics - the canonical set of metrics every backtest result must
report, and their calculation formulas.

Source: backtest/core/metrics.py (formulas adapted; this module defines
WHICH metrics are mandatory and their exact keys, so every strategy's
report.json has the same schema regardless of which strategy produced it).
"""

from dataclasses import dataclass, fields
from typing import Dict, List, Optional
import numpy as np
import pandas as pd

TRADING_DAYS_PER_YEAR = 252
DEFAULT_RISK_FREE_RATE = 0.05


@dataclass
class StandardMetrics:
    """
    The mandatory metric set. A BacktestResult.metrics dict should contain
    exactly these keys (as floats) — no more, no fewer, so cross-strategy
    comparison tables never hit a missing column.
    """
    sharpe_ratio: Optional[float] = None
    sortino_ratio: Optional[float] = None
    cagr: Optional[float] = None
    max_drawdown: Optional[float] = None
    calmar_ratio: Optional[float] = None
    win_rate: Optional[float] = None
    total_return: Optional[float] = None
    volatility_annualized: Optional[float] = None
    trade_count: Optional[int] = None
    avg_holding_days: Optional[float] = None

    @classmethod
    def field_names(cls) -> List[str]:
        return [f.name for f in fields(cls)]

    def to_dict(self) -> Dict[str, Optional[float]]:
        return {name: getattr(self, name) for name in self.field_names()}

    def validate_complete(self) -> None:
        """Raise if any mandatory metric is missing — catches silent gaps
        before a result gets written to the results/ table."""
        missing = [name for name in self.field_names() if getattr(self, name) is None]
        if missing:
            raise ValueError(f"StandardMetrics incomplete, missing: {missing}")


class MetricsCalculator:
    """Computes StandardMetrics from an equity curve + trade log."""

    def __init__(self, risk_free_rate: float = DEFAULT_RISK_FREE_RATE):
        self.risk_free_rate = risk_free_rate

    def compute(
        self,
        equity_curve: pd.Series,
        trade_count: int = 0,
        avg_holding_days: Optional[float] = None,
    ) -> StandardMetrics:
        """
        equity_curve: pd.Series indexed by date, values = portfolio value.
        Must be sorted ascending by date and have at least 2 points.
        """
        if len(equity_curve) < 2:
            raise ValueError("equity_curve needs at least 2 points to compute metrics")

        returns = equity_curve.pct_change().dropna()
        years = (equity_curve.index[-1] - equity_curve.index[0]).days / 365.25

        return StandardMetrics(
            sharpe_ratio=self._sharpe(returns),
            sortino_ratio=self._sortino(returns),
            cagr=self._cagr(equity_curve.iloc[0], equity_curve.iloc[-1], years),
            max_drawdown=self._max_drawdown(equity_curve),
            calmar_ratio=self._calmar(equity_curve, years),
            win_rate=self._win_rate(returns),
            total_return=(equity_curve.iloc[-1] / equity_curve.iloc[0]) - 1.0,
            volatility_annualized=returns.std() * np.sqrt(TRADING_DAYS_PER_YEAR),
            trade_count=trade_count,
            avg_holding_days=avg_holding_days,
        )

    def _sharpe(self, returns: pd.Series) -> float:
        excess = returns - self.risk_free_rate / TRADING_DAYS_PER_YEAR
        std = excess.std()
        if std == 0 or np.isnan(std):
            return 0.0
        return float(excess.mean() / std * np.sqrt(TRADING_DAYS_PER_YEAR))

    def _sortino(self, returns: pd.Series) -> float:
        excess = returns - self.risk_free_rate / TRADING_DAYS_PER_YEAR
        downside = excess[excess < 0]
        downside_std = downside.std()
        if downside_std == 0 or np.isnan(downside_std):
            return 0.0
        return float(excess.mean() / downside_std * np.sqrt(TRADING_DAYS_PER_YEAR))

    def _cagr(self, start_value: float, end_value: float, years: float) -> float:
        if years <= 0 or start_value <= 0:
            return 0.0
        return float((end_value / start_value) ** (1 / years) - 1)

    def _max_drawdown(self, equity_curve: pd.Series) -> float:
        running_max = equity_curve.expanding().max()
        dd = (equity_curve - running_max) / running_max
        return float(dd.min())

    def _calmar(self, equity_curve: pd.Series, years: float) -> float:
        cagr = self._cagr(equity_curve.iloc[0], equity_curve.iloc[-1], years)
        max_dd = abs(self._max_drawdown(equity_curve))
        if max_dd == 0:
            return 0.0
        return float(cagr / max_dd)

    def _win_rate(self, returns: pd.Series) -> float:
        if len(returns) == 0:
            return 0.0
        return float((returns > 0).sum() / len(returns))
