"""
Superseded runs must not appear in any listing a human reads as current.

[2026-08-20] The static-ADTV universe defect invalidated 1,634 runs. They stay
in backtest_runs because their trade logs are the evidence for what the defect
did -- so the only thing standing between a retired run and the dashboard is
this filter. `ta_e6_21d_20260820` reached the run table alongside its own
replacement before it existed.
"""

import duckdb
import pytest

from backtest.core.run_store import count_runs, list_experiments, list_runs


def _db():
    """A backtest_runs table with two runs, one of them superseded."""
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE backtest_runs ("
        " run_id VARCHAR, parent_run_id VARCHAR, channel VARCHAR, strategy_id VARCHAR,"
        " horizon_bucket VARCHAR, mode VARCHAR, universe_spec VARCHAR, start_date DATE,"
        " end_date DATE, capital_mode VARCHAR, initial_capital DOUBLE, sip_amount DOUBLE,"
        " sip_cadence_days INTEGER, random_seed INTEGER, config_hash VARCHAR,"
        " config_json VARCHAR, created_at TIMESTAMP, metrics_json VARCHAR,"
        " data_gaps_json VARCHAR, integrity_passed BOOLEAN, integrity_detail_json VARCHAR,"
        " live_eligible BOOLEAN, regime_breakdown_json VARCHAR, exit_policy_variant VARCHAR,"
        " regime_label VARCHAR, trade_log_path VARCHAR, queue_id VARCHAR, dsr DOUBLE,"
        " dsr_n_trials INTEGER, dsr_computed_post_hoc BOOLEAN)"
    )
    for run_id, sid, created in (
        ("old", "ta_e6_21d_20260820", "2026-08-20 02:22:10"),
        ("new", "E6_unconstrained_21d", "2026-08-20 12:28:11"),
    ):
        conn.execute(
            "INSERT INTO backtest_runs (run_id, channel, strategy_id, mode, created_at,"
            " config_json, metrics_json, data_gaps_json, integrity_detail_json,"
            " regime_breakdown_json) "
            "VALUES (?, 'technical', ?, 'backtest', ?, '{}', '{}', '[]', '{}', '[]')",
            [run_id, sid, created],
        )
    return conn


def _with_supersessions(conn):
    conn.execute(
        "CREATE TABLE run_supersessions (run_id VARCHAR, channel VARCHAR, reason VARCHAR,"
        " superseded_at TIMESTAMP, export_path VARCHAR)"
    )
    conn.execute(
        "INSERT INTO run_supersessions VALUES ('old', 'technical', 'universe defect', now(), NULL)"
    )
    return conn


def test_superseded_run_is_hidden_from_listings():
    conn = _with_supersessions(_db())
    assert [r["strategy_id"] for r in list_runs(conn)] == ["E6_unconstrained_21d"]
    assert [r["strategy_id"] for r in list_experiments(conn)] == ["E6_unconstrained_21d"]


def test_count_agrees_with_the_rows_it_describes():
    """The count and the list are rendered side by side; filtering one and not
    the other reports 'N runs' above a table showing fewer."""
    conn = _with_supersessions(_db())
    assert count_runs(conn) == len(list_runs(conn)) == 1


def test_superseded_runs_are_retained_and_reachable():
    # Hidden by default, never deleted -- the before/after comparison depends
    # on them still being queryable.
    conn = _with_supersessions(_db())
    assert count_runs(conn, include_superseded=True) == 2
    assert len(list_runs(conn, include_superseded=True)) == 2


def test_database_without_the_table_is_not_an_error():
    """Fresh databases and per-shard side stores have no run_supersessions;
    the filter must degrade to a no-op rather than failing to bind."""
    conn = _db()
    assert count_runs(conn) == 2
    assert len(list_runs(conn)) == 2


@pytest.mark.parametrize("channel", ["technical", "momentum"])
def test_filter_composes_with_other_predicates(channel):
    conn = _with_supersessions(_db())
    expected = 1 if channel == "technical" else 0
    assert count_runs(conn, channel=channel) == expected
