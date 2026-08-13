"""
tests/unit/test_benchmark_regime_split.py

A98: the index a strategy is COMPARED against must be a different parameter
from the index its regime is detected on.

Why this needs a test rather than being self-evident: the two were one
parameter, so choosing a fairer benchmark for a midcap strategy also changed
which days counted as "bear" and therefore which days the strategy was allowed
to buy. The comparison and the result moved together, which means the run was
no longer the same experiment — and nothing in the output said so.

These tests use an in-memory DuckDB. Per project policy, no test row is ever
written to a real database file.
"""

from __future__ import annotations

import datetime as dt

import duckdb
import pandas as pd
import pytest

from backtest.core.engine import BacktestOrchestrator


@pytest.fixture()
def index_conn():
    """Two indices with deliberately different trajectories, so a curve built
    from the wrong one is unmistakable rather than merely slightly off."""
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE index_ohlcv (index_name VARCHAR, date DATE, close DOUBLE)"
    )
    rows = []
    for i in range(11):
        d = dt.date(2020, 1, 1) + dt.timedelta(days=i)
        rows.append(("Nifty 500", d, 100.0 + i))       # +10% over the window
        rows.append(("Nifty Midcap 150", d, 100.0 + 5 * i))  # +50%
    conn.executemany("INSERT INTO index_ohlcv VALUES (?, ?, ?)", rows)
    return conn


def _curve(orch: BacktestOrchestrator) -> pd.Series:
    return orch._build_benchmark_curve(
        dt.date(2020, 1, 1), dt.date(2020, 1, 11), 100_000.0
    )


def test_benchmark_defaults_to_the_regime_index(index_conn):
    """Backward compatibility: a caller that names only the regime index gets
    exactly the behaviour it had before the split."""
    orch = BacktestOrchestrator(regime_conn=index_conn, regime_index_name="Nifty 500")
    assert orch._benchmark_index_name == "Nifty 500"
    curve = _curve(orch)
    assert curve is not None
    assert curve.iloc[-1] == pytest.approx(110_000.0, rel=1e-6)


def test_benchmark_can_differ_from_the_regime_index(index_conn):
    """The whole point: compare against a midcap index while still detecting
    regime on the broad one."""
    orch = BacktestOrchestrator(
        regime_conn=index_conn,
        regime_index_name="Nifty 500",
        benchmark_index_name="Nifty Midcap 150",
    )
    assert orch._regime_index_name == "Nifty 500"
    assert orch._benchmark_index_name == "Nifty Midcap 150"
    curve = _curve(orch)
    assert curve.iloc[-1] == pytest.approx(150_000.0, rel=1e-6)


def test_changing_the_benchmark_leaves_the_regime_index_untouched(index_conn):
    """The regression this guards: before the split, passing a different
    benchmark silently repointed regime detection too, so the strategy traded
    on different days and the comparison was no longer like-for-like."""
    a = BacktestOrchestrator(regime_conn=index_conn, regime_index_name="Nifty 500")
    b = BacktestOrchestrator(
        regime_conn=index_conn,
        regime_index_name="Nifty 500",
        benchmark_index_name="Nifty Midcap 150",
    )
    assert a._regime_index_name == b._regime_index_name
    assert a._benchmark_index_name != b._benchmark_index_name


def test_missing_benchmark_series_returns_none_not_a_stand_in(index_conn):
    """An index with no rows must yield no curve. Falling back to another
    index would report an excess return against something the user did not
    choose and was never told about."""
    orch = BacktestOrchestrator(
        regime_conn=index_conn,
        regime_index_name="Nifty 500",
        benchmark_index_name="Nifty Microcap 250",  # not in the fixture
    )
    assert _curve(orch) is None


def test_metrics_record_which_index_was_used():
    """A run that does not record its benchmark cannot be honestly compared
    with another run later -- two excess returns look comparable when they may
    be measured against different yardsticks."""
    from backtest.core.metrics import compute_metrics

    idx = pd.date_range("2020-01-01", periods=10, freq="D")
    equity = pd.Series([100_000.0 + 1000 * i for i in range(10)], index=idx)
    bench = pd.Series([100_000.0 + 500 * i for i in range(10)], index=idx)

    m = compute_metrics(
        equity_curve=equity,
        cash_flows=[("2020-01-01", -100_000.0)],
        trade_pnls=[100.0],
        trade_values=[1000.0],
        distinct_tickers=["AAA"],
        start_date=dt.date(2020, 1, 1),
        end_date=dt.date(2020, 1, 10),
        total_contributed=100_000.0,
        benchmark_equity_curve=bench,
        benchmark_index_name="Nifty Midcap 150",
    )
    assert m.benchmark_index_name == "Nifty Midcap 150"
