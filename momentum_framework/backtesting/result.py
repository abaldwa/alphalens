"""
BacktestResult - standardized container for a single backtest run's output.

This is the object every strategy run returns, regardless of which
strategy or band it came from — the metrics/ and results/ modules only
ever operate on this shape, never on the raw engine.py report dict.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import pandas as pd


@dataclass
class BacktestResult:
    """Standardized backtest output — the framework's common currency."""

    run_id: str
    strategy_id: str
    config: Dict[str, Any]

    # Core metrics (see metrics/standard.py for the canonical field list).
    # Optional[float] because some ratios are legitimately undefined (e.g.
    # Sortino with zero downside deviation) — StandardMetrics.to_dict()'s
    # real return type, not loosened here for convenience.
    metrics: Dict[str, Optional[float]] = field(default_factory=dict)

    # Time series
    equity_curve: Optional[pd.DataFrame] = None       # columns: date, portfolio_value
    benchmark_curve: Optional[pd.DataFrame] = None    # columns: date, benchmark_value
    annual_returns: Optional[pd.Series] = None        # indexed by year

    # Trade-level detail. `trades` holds the actual rows (from
    # backtesting/portfolio.py::Portfolio.trade_log) when the result came
    # from run_native() — needed for trade-by-trade parity comparison
    # against the legacy engine (see scripts/parity_check.py). A result
    # built from _normalize_report() (a legacy report.json) leaves this
    # None and relies on trade_log_path instead — the legacy engine
    # writes its trade book to a CSV, not an in-memory list.
    trade_log_path: Optional[str] = None
    trades: Optional[List[Dict[str, Any]]] = None
    trade_count: int = 0

    # Integrity
    integrity_passed: bool = False
    integrity_detail: Dict[str, Any] = field(default_factory=dict)
    data_gaps: List[Dict[str, Any]] = field(default_factory=list)

    # Provenance — always populate so a result can be traced back to code
    source_commit: Optional[str] = None
    framework_version: str = "1.0.0"

    def sharpe(self) -> Optional[float]:
        return self.metrics.get("sharpe_ratio")

    def cagr(self) -> Optional[float]:
        return self.metrics.get("cagr")

    def max_drawdown(self) -> Optional[float]:
        return self.metrics.get("max_drawdown")

    @staticmethod
    def _fmt(value: Optional[float]) -> str:
        return f"{value:.3f}" if value is not None else "N/A"

    def summary(self) -> str:
        return (
            f"{self.strategy_id}: "
            f"Sharpe={self._fmt(self.sharpe())} "
            f"CAGR={self._fmt(self.cagr())} "
            f"MaxDD={self._fmt(self.max_drawdown())} "
            f"Integrity={'PASS' if self.integrity_passed else 'FAIL'}"
        )
