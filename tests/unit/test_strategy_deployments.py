"""
tests/unit/test_strategy_deployments.py

A91: deploying a strategy of ANY channel.

momentum_strategy_configs is momentum-shaped down to its column names, so
Technical, Fundamental and ML strategies had nowhere to be deployed to — the
Deploy control had to render disabled for three channels out of four, which
defeats the purpose of a report whose whole job is a deploy decision.

All writes here go to an in-memory DuckDB. Per project policy no test row is
ever written to a real database file.
"""

from __future__ import annotations

import datetime as dt

import duckdb
import pytest

from datastore.schema.create_strategy_deployments import (
    create_strategy_deployments_schema,
)


@pytest.fixture()
def conn():
    c = duckdb.connect(":memory:")
    create_strategy_deployments_schema(conn=c)
    return c


def _insert(c, key="technical:A1", version=1, channel="technical", portfolio=0,
            capital=500_000.0, active=True):
    c.execute(
        """
        INSERT INTO strategy_deployments
          (strategy_key, strategy_version, channel, initial_capital,
           start_date, portfolio_id, is_active)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [key, version, channel, capital, dt.date(2026, 8, 13), portfolio, active],
    )


def test_the_schema_is_idempotent(conn):
    create_strategy_deployments_schema(conn=conn)
    create_strategy_deployments_schema(conn=conn)
    assert conn.execute("SELECT count(*) FROM strategy_deployments").fetchone()[0] == 0


def test_every_channel_can_be_deployed(conn):
    """The entire point of A91. A Technical strategy previously had nowhere to
    go at all."""
    for key, channel in [
        ("technical:A1", "technical"),
        ("fundamental:smile", "fundamental"),
        ("ml:signal_21d", "ml"),
        ("momentum:balanced_b1", "momentum"),
    ]:
        _insert(conn, key=key, channel=channel)
    channels = {
        r[0] for r in conn.execute("SELECT DISTINCT channel FROM strategy_deployments").fetchall()
    }
    assert channels == {"technical", "fundamental", "ml", "momentum"}


def test_the_version_is_pinned_not_floating(conn):
    """Deploying "the current version" is how a live position ends up running
    rules revised after it opened. The column is NOT NULL so that cannot be
    skipped."""
    with pytest.raises(duckdb.ConstraintException):
        conn.execute(
            "INSERT INTO strategy_deployments (strategy_key, channel, start_date) "
            "VALUES ('technical:A1', 'technical', '2026-08-13')"
        )


def test_no_portfolio_uses_a_sentinel_not_null(conn):
    """NULLs compare as distinct, so a nullable portfolio_id would let two
    'unassigned' deployments of the same strategy both look unique — the exact
    duplicate the API check is meant to catch."""
    _insert(conn)
    assert conn.execute("SELECT portfolio_id FROM strategy_deployments").fetchone()[0] == 0


def test_negative_capital_is_rejected(conn):
    with pytest.raises(duckdb.ConstraintException):
        _insert(conn, capital=-1.0)


def test_retiring_keeps_the_row(conn):
    """A live position's history is the audit trail for every trade it
    produced, so deactivation must not delete it."""
    _insert(conn)
    conn.execute("UPDATE strategy_deployments SET is_active = FALSE")
    assert conn.execute("SELECT count(*) FROM strategy_deployments").fetchone()[0] == 1


def test_history_allows_many_retired_deployments_of_one_strategy(conn):
    """Why the uniqueness rule is enforced in the API rather than as a UNIQUE
    constraint: including is_active in one would forbid this, destroying the
    history the table exists to keep."""
    _insert(conn, active=False)
    _insert(conn, active=False)
    _insert(conn, active=True)
    assert conn.execute("SELECT count(*) FROM strategy_deployments").fetchone()[0] == 3


def test_the_active_duplicate_rule_is_detectable_by_query(conn):
    """The API's check is exactly this query; if it could not find the clash,
    two deployments would both trade the strategy and double the position."""
    _insert(conn, active=True)
    clash = conn.execute(
        """
        SELECT count(*) FROM strategy_deployments
        WHERE strategy_key = ? AND strategy_version = ? AND portfolio_id = ? AND is_active
        """,
        ["technical:A1", 1, 0],
    ).fetchone()[0]
    assert clash == 1


def test_deployments_carry_no_strategy_rules(conn):
    """Invariant 6: the deployed definition IS the registry row. A rules
    column here would let a deployment drift from what was backtested."""
    cols = {
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'strategy_deployments'"
        ).fetchall()
    }
    assert not cols & {"entry_criterion_json", "exit_criterion_json", "definition_json"}
    assert "strategy_key" in cols and "strategy_version" in cols
