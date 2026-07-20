"""tests/unit/test_schema_backtest.py — datastore/schema/create_backtest.py."""


from datastore.api.db import get_duckdb_connection
from datastore.schema import create_backtest


class TestCreateBacktestSchema:
    def test_tables_created(self):
        create_backtest.create_backtest_schema(in_memory=True)
        with get_duckdb_connection(None) as conn:
            existing = {
                r[0] for r in conn.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
                ).fetchall()
            }
        assert "backtest_runs" in existing
        assert "backtest_feature_log" in existing

    def test_idempotent_create(self):
        create_backtest.create_backtest_schema(in_memory=True)
        create_backtest.create_backtest_schema(in_memory=True)  # should not raise

    def test_backtest_runs_live_eligible_defaults_false(self):
        create_backtest.create_backtest_schema(in_memory=True)
        with get_duckdb_connection(None) as conn:
            conn.execute(
                """
                INSERT INTO backtest_runs
                    (run_id, channel, strategy_id, horizon_bucket, mode, universe_spec,
                     start_date, end_date, capital_mode, initial_capital, random_seed,
                     config_hash, config_json, created_at)
                VALUES ('r1', 'technical', 's1', '21_day', 'backtest', 'nifty500',
                        '2015-01-01', '2020-01-01', 'lump', 1000000.0, 0, 'abc', '{}', now())
                """
            )
            live_eligible = conn.execute("SELECT live_eligible FROM backtest_runs WHERE run_id = 'r1'").fetchone()[0]
        assert live_eligible is False

    def test_list_tables(self):
        tables = create_backtest.list_tables()
        assert set(tables["duckdb"]) == {"backtest_runs", "backtest_feature_log"}
