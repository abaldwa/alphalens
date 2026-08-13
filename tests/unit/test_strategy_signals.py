"""
Unit tests for the strategy_signals ledger (A94).

tmp_path DuckDB only -- never the real database.

The idempotent-rewrite and hold-rejection tests are the load-bearing ones:
the first is what lets a resumed backtest job re-emit an interrupted day
without colliding on the primary key, and the second is the guard that keeps
the table in the millions of rows rather than the hundreds of millions.
"""

from datetime import date, datetime

import pytest

from datastore.schema.create_strategy_registry import create_strategy_registry_schema
from strategies.signals import (
    NO_RUN,
    SignalError,
    delete_run_signals,
    read_signals,
    signal_counts,
    write_signals,
    UNVERSIONED,
)

KEY = "technical:A1_pullback"


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "signals.duckdb"
    create_strategy_registry_schema(db_path=path)
    return path


def _sig(day, ticker, action="buy", **extra):
    return {"signal_date": date(2026, 1, day), "ticker": ticker, "action": action, **extra}


def _write(db, signals, **overrides):
    kwargs = dict(
        strategy_key=KEY,
        strategy_version=1,
        source="backtest",
        run_id="run_abc",
        db_path=db,
    )
    kwargs.update(overrides)
    return write_signals(signals, **kwargs)


class TestWrite:
    def test_write_and_read_back(self, db):
        assert _write(db, [_sig(5, "INFY"), _sig(5, "TCS", "sell")]) == 2
        rows = read_signals(strategy_key=KEY, db_path=db)
        assert [r["ticker"] for r in rows] == ["INFY", "TCS"]
        assert rows[1]["action"] == "sell"

    def test_context_round_trips_as_a_dict(self, db):
        """context_json is what lets a report explain WHY a strategy acted."""
        _write(db, [_sig(5, "INFY", context={"sector": "IT", "adtv_cr": 412.0})])
        assert read_signals(strategy_key=KEY, db_path=db)[0]["context"] == {
            "sector": "IT",
            "adtv_cr": 412.0,
        }

    def test_missing_context_reads_back_as_none(self, db):
        _write(db, [_sig(5, "INFY")])
        assert read_signals(strategy_key=KEY, db_path=db)[0]["context"] is None

    def test_empty_batch_is_a_no_op(self, db):
        assert _write(db, []) == 0

    def test_optional_fields_persist(self, db):
        _write(db, [_sig(5, "INFY", conviction=0.82, rank=3, size_multiplier=1.5)])
        row = read_signals(strategy_key=KEY, db_path=db)[0]
        assert row["conviction"] == pytest.approx(0.82)
        assert row["rank"] == 3
        assert row["size_multiplier"] == pytest.approx(1.5)

    def test_string_and_datetime_dates_accepted(self, db):
        _write(
            db,
            [
                {"signal_date": "2026-01-05", "ticker": "INFY", "action": "buy"},
                {"signal_date": datetime(2026, 1, 6, 15, 30), "ticker": "TCS", "action": "buy"},
            ],
        )
        assert [r["signal_date"] for r in read_signals(strategy_key=KEY, db_path=db)] == [
            date(2026, 1, 5),
            date(2026, 1, 6),
        ]


class TestRejections:
    def test_hold_rejected_by_default(self, db):
        """Universe-wide holds are what turn 1e6 rows into 1e8."""
        with pytest.raises(SignalError, match="hold"):
            _write(db, [_sig(5, "INFY", "hold")])

    def test_hold_allowed_when_explicitly_opted_in(self, db):
        assert _write(db, [_sig(5, "INFY", "hold")], allow_hold=True) == 1

    def test_unknown_action_rejected(self, db):
        with pytest.raises(SignalError, match="unknown action"):
            _write(db, [_sig(5, "INFY", "accumulate")])

    def test_unknown_source_rejected(self, db):
        with pytest.raises(SignalError, match="unknown source"):
            _write(db, [_sig(5, "INFY")], source="simulation")

    def test_backtest_signals_require_a_run_id(self, db):
        """Without it, a backtest's signals cannot be attributed to the run
        that produced them, which is the whole point of the ledger."""
        with pytest.raises(SignalError, match="run_id"):
            _write(db, [_sig(5, "INFY")], run_id=NO_RUN)

    def test_live_signals_do_not_need_a_run_id(self, db):
        assert _write(db, [_sig(5, "INFY")], source="live", run_id=NO_RUN) == 1

    def test_missing_ticker_rejected(self, db):
        with pytest.raises(SignalError, match="ticker"):
            _write(db, [{"signal_date": date(2026, 1, 5), "action": "buy"}])

    def test_missing_date_rejected(self, db):
        with pytest.raises(SignalError, match="signal_date"):
            _write(db, [{"ticker": "INFY", "action": "buy"}])


class TestIdempotence:
    def test_rewriting_the_same_key_replaces_rather_than_duplicating(self, db):
        """A resumed job re-emits the day it was interrupted on. That must not
        collide on the primary key, or every resume would fail."""
        _write(db, [_sig(5, "INFY", conviction=0.5)])
        _write(db, [_sig(5, "INFY", conviction=0.9)])

        rows = read_signals(strategy_key=KEY, db_path=db)
        assert len(rows) == 1
        assert rows[0]["conviction"] == pytest.approx(0.9)

    def test_same_ticker_different_source_coexists(self, db):
        """A backtest signal and a live signal for the same day are different
        facts, not a duplicate."""
        _write(db, [_sig(5, "INFY")])
        _write(db, [_sig(5, "INFY")], source="live", run_id=NO_RUN)
        assert len(read_signals(strategy_key=KEY, db_path=db)) == 2

    def test_same_ticker_different_version_coexists(self, db):
        _write(db, [_sig(5, "INFY")])
        _write(db, [_sig(5, "INFY")], strategy_version=2)
        assert len(read_signals(strategy_key=KEY, db_path=db)) == 2
        assert len(read_signals(strategy_key=KEY, strategy_version=2, db_path=db)) == 1


class TestRead:
    @pytest.fixture(autouse=True)
    def _seed(self, db):
        _write(
            db,
            [
                _sig(5, "INFY"),
                _sig(6, "TCS", "sell"),
                _sig(7, "WIPRO"),
                _sig(8, "HCLTECH", "forced_close"),
            ],
        )

    def test_date_range_filter(self, db):
        rows = read_signals(
            strategy_key=KEY,
            start_date=date(2026, 1, 6),
            end_date=date(2026, 1, 7),
            db_path=db,
        )
        assert [r["ticker"] for r in rows] == ["TCS", "WIPRO"]

    def test_action_filter(self, db):
        rows = read_signals(strategy_key=KEY, actions=["sell", "forced_close"], db_path=db)
        assert {r["ticker"] for r in rows} == {"TCS", "HCLTECH"}

    def test_run_filter(self, db):
        assert read_signals(strategy_key=KEY, run_id="other_run", db_path=db) == []

    def test_unknown_strategy_returns_empty(self, db):
        assert read_signals(strategy_key="momentum:nope", db_path=db) == []

    def test_ordered_by_date_then_ticker(self, db):
        rows = read_signals(strategy_key=KEY, db_path=db)
        assert [r["signal_date"] for r in rows] == sorted(r["signal_date"] for r in rows)


class TestMaintenance:
    def test_delete_run_signals_removes_only_that_run(self, db):
        _write(db, [_sig(5, "INFY")])
        _write(db, [_sig(5, "TCS")], run_id="run_xyz")
        _write(db, [_sig(5, "WIPRO")], source="live", run_id=NO_RUN)

        assert delete_run_signals("run_abc", db_path=db) == 1
        remaining = {r["ticker"] for r in read_signals(strategy_key=KEY, db_path=db)}
        assert remaining == {"TCS", "WIPRO"}

    def test_delete_refuses_the_live_sentinel(self, db):
        """Live signals are the audit record and must not be bulk-deletable
        by passing an empty run_id."""
        with pytest.raises(SignalError, match="real run_id"):
            delete_run_signals(NO_RUN, db_path=db)

    def test_signal_counts_groups_by_source_and_action(self, db):
        _write(db, [_sig(5, "INFY"), _sig(6, "TCS", "sell")])
        counts = {(c["source"], c["action"]): c["n"] for c in signal_counts(db_path=db)}
        assert counts == {("backtest", "buy"): 1, ("backtest", "sell"): 1}


class TestVersionSentinel:
    """A run that cannot resolve its registry version still has to write a
    row: strategy_version is NOT NULL and part of the primary key.

    The sentinel matters because the alternative that was in place -- default
    to 1 -- stamps rows with a definition the run may never have executed.
    That reads as authoritative while being wrong, and it is the same hole
    that let the deploy hand-off pin the CURRENT registry version instead of
    the backtested one (AGENTS.md invariant 6).
    """

    def test_none_version_is_stored_as_the_unversioned_sentinel(self, db):
        _write(db, [_sig(5, "INFY")], strategy_version=None)
        rows = read_signals(strategy_key=KEY, db_path=db)
        assert rows[0]["strategy_version"] == UNVERSIONED

    def test_the_sentinel_cannot_collide_with_a_real_version(self):
        """Registry versions are append-only from 1, so 0 is unreachable."""
        assert UNVERSIONED == 0

    def test_unversioned_and_versioned_rows_coexist_for_one_strategy(self, db):
        """They are distinct rows, not an overwrite: a pre-registry run and a
        registered run are different evidence and must both survive."""
        _write(db, [_sig(5, "INFY")], strategy_version=None)
        _write(db, [_sig(5, "INFY")], strategy_version=3)
        versions = sorted(
            r["strategy_version"] for r in read_signals(strategy_key=KEY, db_path=db)
        )
        assert versions == [UNVERSIONED, 3]
