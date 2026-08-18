"""
Unit tests for the one-set-per-strategy-per-window contract
(strategies/signals.supersede_backtest_signals + SignalRecorder.finalize).

tmp_path DuckDB only -- never the real database.

The contract exists because run_id is part of the primary key, so
regenerating a backtest over dates it had already covered collided with
nothing and simply doubled the rows, leaving "what did this strategy say on
this date" with two answers. The load-bearing tests here are the ones that
assert the ASYMMETRY -- backtest rows are superseded, live and paper rows
never are -- and that a regeneration which changes the decisions is reported
rather than absorbed silently.
"""

from datetime import date

import pytest

from datastore.schema.create_strategy_registry import create_strategy_registry_schema
from strategies.signals import (
    read_signals,
    supersede_backtest_signals,
    SignalError,
    write_signals,
)

KEY = "momentum:all_risk_b1_1-50_lb3mo_weekly_top10"


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "signals.duckdb"
    create_strategy_registry_schema(db_path=path)
    return path


def _sig(day, ticker, action="buy", **extra):
    return {"signal_date": date(2026, 1, day), "ticker": ticker, "action": action, **extra}


def _write(db, signals, run_id, *, source="backtest", version=1):
    return write_signals(
        signals,
        strategy_key=KEY,
        strategy_version=version,
        source=source,
        run_id=run_id,
        db_path=db,
    )


def _supersede(db, run_id, first=1, last=31):
    return supersede_backtest_signals(
        strategy_key=KEY,
        run_id=run_id,
        start_date=date(2026, 1, first),
        end_date=date(2026, 1, last),
        db_path=db,
    )


def test_identical_regeneration_keeps_one_set(db):
    """The normal case: the same strategy re-run over the same window
    reproduces the same decisions, and the ledger ends with one copy."""
    _write(db, [_sig(5, "TCS"), _sig(5, "INFY")], "run-a")
    _write(db, [_sig(5, "TCS"), _sig(5, "INFY")], "run-b")

    report = _supersede(db, "run-b")

    assert report.identical is True
    assert report.prior_runs == ["run-a"]
    assert report.deleted_rows == 2
    assert report.kept_rows == 2

    rows = read_signals(strategy_key=KEY, db_path=db)
    assert len(rows) == 2
    assert {r["run_id"] for r in rows} == {"run-b"}


def test_changed_decisions_are_reported_not_absorbed(db):
    """A regeneration that selects differently is legitimate -- a revised
    definition, or corrected data -- but it must be visible. Silently
    replacing one set with a different one is how a changed chart becomes a
    mystery weeks later."""
    _write(db, [_sig(5, "TCS"), _sig(5, "INFY")], "run-a")
    _write(db, [_sig(5, "TCS"), _sig(5, "WIPRO")], "run-b")

    report = _supersede(db, "run-b")

    assert report.identical is False
    assert report.added == 1  # WIPRO
    assert report.removed == 1  # INFY
    assert "DIFFERS" in report.summary()


def test_same_picks_different_sizing_is_a_detail_change(db):
    """Same stocks on the same days, sized differently, is a different
    finding from different stocks -- the report must distinguish them or
    neither can be acted on."""
    _write(db, [_sig(5, "TCS", rank=1, size_multiplier=1.0)], "run-a")
    _write(db, [_sig(5, "TCS", rank=1, size_multiplier=0.5)], "run-b")

    report = _supersede(db, "run-b")

    assert report.identical is False
    assert report.added == 0 and report.removed == 0
    assert report.detail_changed == 1


def test_live_and_paper_signals_are_never_superseded(db):
    """The asymmetry that makes this a supersede rather than a narrower
    primary key: live/paper rows record what was actually acted on, on a day
    that already happened. Deleting them would rewrite history."""
    _write(db, [_sig(5, "TCS")], "", source="live")
    _write(db, [_sig(5, "TCS")], "", source="paper")
    _write(db, [_sig(5, "TCS")], "run-a")
    _write(db, [_sig(5, "TCS")], "run-b")

    _supersede(db, "run-b")

    rows = read_signals(strategy_key=KEY, db_path=db)
    by_source = {}
    for r in rows:
        by_source[r["source"]] = by_source.get(r["source"], 0) + 1
    assert by_source == {"live": 1, "paper": 1, "backtest": 1}


def test_supersede_is_bounded_by_the_window(db):
    """A run that covered January must not delete February's signals from an
    earlier run that legitimately covered a different period."""
    _write(db, [_sig(5, "TCS")], "run-a")
    _write(db, [{"signal_date": date(2026, 2, 9), "ticker": "TCS", "action": "buy"}], "run-a")
    _write(db, [_sig(5, "TCS")], "run-b")

    _supersede(db, "run-b", first=1, last=31)

    remaining = read_signals(strategy_key=KEY, db_path=db)
    dates = sorted({r["signal_date"] for r in remaining})
    assert dates == [date(2026, 1, 5), date(2026, 2, 9)]
    assert len(remaining) == 2


def test_supersede_crosses_versions(db):
    """A revised definition's older backtest rows for the same dates are
    stale output of a definition no longer in force. Keeping them is exactly
    the duplication this closes -- but the change is still reported."""
    _write(db, [_sig(5, "TCS"), _sig(5, "INFY")], "run-a", version=1)
    _write(db, [_sig(5, "TCS")], "run-b", version=2)

    report = _supersede(db, "run-b")

    assert report.deleted_rows == 2
    assert report.identical is False
    assert report.removed == 1
    rows = read_signals(strategy_key=KEY, db_path=db)
    assert {r["strategy_version"] for r in rows} == {2}


def test_first_run_reports_no_prior_set(db):
    """Nothing to compare against must read as 'no prior set', not as
    'identical' -- the difference matters to anyone reading the log."""
    _write(db, [_sig(5, "TCS")], "run-a")
    report = _supersede(db, "run-a")
    assert report.identical is None
    assert report.prior_runs == []
    assert report.deleted_rows == 0


def test_supersede_demands_a_real_run_id(db):
    with pytest.raises(SignalError):
        _supersede(db, "")


def test_recorder_finalize_supersedes_and_live_recorder_does_not(db):
    """The wiring: the orchestrator calls finalize(), and the recorder must
    apply the contract for backtest runs and skip it for paper ones."""
    from backtest.core.signal_ledger import SignalLedgerRecorder

    class _Sig:
        def __init__(self, ticker, action="buy"):
            self.ticker = ticker
            self.action = action
            self.conviction = 0.5

    def _record(run_id, source, tickers):
        rec = SignalLedgerRecorder(
            strategy_key=KEY, source=source, run_id=run_id, strategy_version=1, db_path=db
        )
        rec.record(date(2026, 1, 5), [_Sig(t) for t in tickers])
        return rec.finalize()

    assert _record("run-a", "backtest", ["TCS", "INFY"]) is not None
    report = _record("run-b", "backtest", ["TCS", "INFY"])
    assert report is not None and report.identical is True

    rows = read_signals(strategy_key=KEY, source="backtest", db_path=db)
    assert len(rows) == 2 and {r["run_id"] for r in rows} == {"run-b"}

    assert _record("", "paper", ["TCS"]) is None
