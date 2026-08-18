"""
tests/unit/test_momentum_live_signal_ledger.py

Phase: Signal-generator consolidation (UnifiedGeneratorRefactorPlan.md, B1)
Owner: Platform / Features
Consumers: CI / `pytest tests/unit/`

Covers the live dual-write into `strategy_signals`. Before B1 the ledger's
`source` column was 100% 'backtest': a live pick could not be traced back to
the registry revision that produced it, which is the audit gap A94 exists to
close.

Every write here goes to an ISOLATED temp DuckDB. The real
BACKTEST_DUCKDB_PATH is never touched -- a test that writes a fixture ticker
into the production ledger is indistinguishable from a real signal
afterwards.

PIT Assumptions
---------------
Signals are recorded as-of the date passed in; no forward data is read.
"""

from __future__ import annotations

import pandas as pd
import pytest

from datastore.schema.create_strategy_registry import (
    create_strategy_registry_schema,
)
from features import momentum_live


@pytest.fixture
def ledger_db(tmp_path, monkeypatch):
    """An empty, isolated ledger. Schema created by the real DDL, the same
    way tests/unit/test_signal_supersede.py builds one."""
    db = tmp_path / "ledger.duckdb"
    create_strategy_registry_schema(db_path=db)
    return db


def _ranking(rows):
    return pd.DataFrame(
        rows, columns=["ticker", "momentum_return", "momentum_rank", "in_top_n"]
    )


def test_only_selected_names_are_recorded(ledger_db):
    """The full ranking already lives in momentum_rankings. Writing a row per
    scored ticker per band per day is what turns this table from millions of
    rows into hundreds of millions, so only the actual selection is
    recorded."""
    ranking = _ranking([
        ("AAA", 0.5, 1, True),
        ("BBB", 0.4, 2, True),
        ("CCC", 0.1, 3, False),
    ])
    written = momentum_live.record_live_signals(
        ranking, "2026-08-14", momentum_live.DEFAULT_STRATEGY_ID, db_path=ledger_db,
    )
    assert written == 2

    import duckdb

    conn = duckdb.connect(str(ledger_db), read_only=True)
    rows = conn.execute(
        "SELECT ticker, action, source FROM strategy_signals ORDER BY ticker"
    ).fetchall()
    conn.close()
    assert rows == [("AAA", "buy", "live"), ("BBB", "buy", "live")]


def test_recorded_signal_names_the_registry_revision(ledger_db):
    """The whole point of the dual-write. momentum_rankings records a bare
    strategy_id with no version, so a pick there can never be tied to the
    declaration it came from."""
    ranking = _ranking([("AAA", 0.5, 1, True)])
    momentum_live.record_live_signals(
        ranking, "2026-08-14", momentum_live.DEFAULT_STRATEGY_ID, db_path=ledger_db,
    )

    import duckdb

    conn = duckdb.connect(str(ledger_db), read_only=True)
    key, version = conn.execute(
        "SELECT strategy_key, strategy_version FROM strategy_signals"
    ).fetchone()
    conn.close()

    expected_key = momentum_live.get_strategy(
        momentum_live.DEFAULT_STRATEGY_ID
    )["registry_key"]
    assert key == expected_key
    assert isinstance(version, int) and version >= 1


def test_empty_ranking_writes_nothing(ledger_db):
    assert momentum_live.record_live_signals(
        _ranking([]), "2026-08-14", momentum_live.DEFAULT_STRATEGY_ID, db_path=ledger_db,
    ) == 0


def test_ranking_with_no_selection_writes_nothing(ledger_db):
    """A band where every name failed its filters selects nothing. That is a
    real outcome, not an error, and it must not write a phantom row."""
    ranking = _ranking([("AAA", 0.5, 1, False)])
    assert momentum_live.record_live_signals(
        ranking, "2026-08-14", momentum_live.DEFAULT_STRATEGY_ID, db_path=ledger_db,
    ) == 0


def test_rewriting_the_same_day_is_idempotent(ledger_db):
    """The scheduler can re-run a date after a failure. A second run must
    replace the day's rows, not duplicate or collide on the primary key."""
    ranking = _ranking([("AAA", 0.5, 1, True)])
    for _ in range(2):
        momentum_live.record_live_signals(
            ranking, "2026-08-14", momentum_live.DEFAULT_STRATEGY_ID, db_path=ledger_db,
        )

    import duckdb

    conn = duckdb.connect(str(ledger_db), read_only=True)
    (count,) = conn.execute("SELECT count(*) FROM strategy_signals").fetchone()
    conn.close()
    assert count == 1
