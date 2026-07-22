"""
tests/unit/test_regime_breakdown.py

Unit tests for backtest/core/regime_breakdown.py's compute_regime_breakdown()
— slicing a run's equity curve/trades by market_regimes segments.
"""

from datetime import date, timedelta

import pandas as pd

from backtest.core.regime_breakdown import compute_regime_breakdown
from backtest.portfolio import Trade


def _trade(exit_date: date, pnl_inr: float) -> Trade:
    return Trade(
        ticker="RELIANCE", entry_date=exit_date - timedelta(days=5), exit_date=exit_date,
        entry_price=100.0, exit_price=100.0 + pnl_inr, quantity=1, pnl_inr=pnl_inr, pnl_pct=0.0,
        cost_inr=0.0, exit_reason="target",
    )


def _equity_curve(start: date, n_days: int, start_value: float = 1_000_000.0, daily_growth: float = 0.001) -> pd.Series:
    dates = [start + timedelta(days=i) for i in range(n_days)]
    values = [start_value * (1 + daily_growth) ** i for i in range(n_days)]
    return pd.Series(values, index=pd.DatetimeIndex(dates))


class TestComputeRegimeBreakdown:
    def test_empty_equity_curve_returns_no_rows(self):
        rows = compute_regime_breakdown(pd.Series(dtype=float), [], date(2020, 1, 1), date(2020, 12, 31), [{"regime": "bull", "start_date": date(2020, 1, 1), "end_date": date(2020, 12, 31)}])
        assert rows == []

    def test_no_segments_returns_no_rows(self):
        curve = _equity_curve(date(2020, 1, 1), 30)
        rows = compute_regime_breakdown(curve, [], date(2020, 1, 1), date(2020, 1, 30), [])
        assert rows == []

    def test_single_segment_covering_full_run(self):
        curve = _equity_curve(date(2020, 1, 1), 60)
        segments = [{"regime": "bull", "start_date": date(2020, 1, 1), "end_date": date(2020, 3, 1)}]
        rows = compute_regime_breakdown(curve, [], date(2020, 1, 1), date(2020, 2, 29), segments)
        assert len(rows) == 1
        assert rows[0].regime == "bull"
        # segment end is clipped to the run's own end_date
        assert rows[0].end_date == date(2020, 2, 29)
        assert rows[0].cagr is not None and rows[0].cagr > 0

    def test_segments_outside_run_window_are_excluded(self):
        curve = _equity_curve(date(2020, 6, 1), 30)
        segments = [{"regime": "bear", "start_date": date(2019, 1, 1), "end_date": date(2019, 12, 31)}]
        rows = compute_regime_breakdown(curve, [], date(2020, 6, 1), date(2020, 6, 30), segments)
        assert rows == []

    def test_trades_attributed_by_exit_date(self):
        curve = _equity_curve(date(2020, 1, 1), 200)
        segments = [
            {"regime": "bull", "start_date": date(2020, 1, 1), "end_date": date(2020, 3, 31)},
            {"regime": "bear", "start_date": date(2020, 4, 1), "end_date": date(2020, 6, 30)},
        ]
        trades = [
            _trade(date(2020, 2, 1), 1000.0),  # bull segment, win
            _trade(date(2020, 2, 15), -500.0),  # bull segment, loss
            _trade(date(2020, 5, 1), -2000.0),  # bear segment, loss
        ]
        rows = compute_regime_breakdown(curve, trades, date(2020, 1, 1), date(2020, 6, 30), segments)
        by_regime = {r.regime: r for r in rows}
        assert by_regime["bull"].n_trades == 2
        assert by_regime["bull"].win_rate == 0.5
        assert by_regime["bear"].n_trades == 1
        assert by_regime["bear"].win_rate == 0.0

    def test_multiple_segments_produce_multiple_rows(self):
        curve = _equity_curve(date(2020, 1, 1), 400)
        segments = [
            {"regime": "bull", "start_date": date(2020, 1, 1), "end_date": date(2020, 4, 30)},
            {"regime": "bear", "start_date": date(2020, 5, 1), "end_date": date(2020, 7, 31)},
            {"regime": "sideways", "start_date": date(2020, 8, 1), "end_date": date(2021, 2, 1)},
        ]
        rows = compute_regime_breakdown(curve, [], date(2020, 1, 1), date(2021, 2, 1), segments)
        assert [r.regime for r in rows] == ["bull", "bear", "sideways"]
        assert all(r.n_days > 0 for r in rows)
