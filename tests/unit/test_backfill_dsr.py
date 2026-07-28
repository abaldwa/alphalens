"""
tests/unit/test_backfill_dsr.py

[BUG FIX, 4th fundamental-strategies review, item 1] backfill_dsr.py reads
metrics_json's "sharpe" (ANNUALIZED, from backtest/core/metrics.py::
sharpe_ratio) and previously fed it straight into deflated_sharpe_ratio,
which expects a per-period Sharpe. This test confirms the wired value is
de-annualized (divided by sqrt(TRADING_DAYS_PER_YEAR)) before being passed.
"""

import datetime
import json

import backtest.backfill_dsr as backfill_dsr_mod
from backtest.core.metrics import TRADING_DAYS_PER_YEAR


class _FakeConn:
    def __init__(self, rows, n_trials):
        self._rows = rows
        self._n_trials = n_trials
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if "SELECT run_id" in sql:
            return _FakeResult(self._rows)
        if "COUNT(*)" in sql:
            return _FakeResult([(self._n_trials,)])
        return _FakeResult([])

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


def test_backfill_dsr_wires_per_period_not_annualized_sharpe(monkeypatch):
    annualized_sharpe = 3.0  # deliberately "impossible looking" annualized value
    raw_sharpe_expected = annualized_sharpe / (TRADING_DAYS_PER_YEAR ** 0.5)

    metrics_json = json.dumps({"sharpe": annualized_sharpe})
    rows = [("run-1", metrics_json, datetime.date(2025, 1, 1), datetime.date(2026, 1, 1))]
    fake_conn = _FakeConn(rows, n_trials=10)

    monkeypatch.setattr(backfill_dsr_mod, "get_duckdb_connection", lambda *a, **k: fake_conn)

    captured = {}

    def _fake_dsr(sharpe, n_trials, n_obs):
        captured["sharpe"] = sharpe
        return 0.5

    monkeypatch.setattr(
        "backtest.overfit_checks.deflated_sharpe_ratio", _fake_dsr,
    )
    monkeypatch.setattr("backtest.core.run_store.update_dsr", lambda *a, **k: None)

    result = backfill_dsr_mod.backfill_dsr_for_queue("queue-1")

    assert result["updated"] == 1
    assert captured["sharpe"] == raw_sharpe_expected
    assert captured["sharpe"] != annualized_sharpe
