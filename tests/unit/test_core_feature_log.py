"""tests/unit/test_core_feature_log.py — backtest/core/feature_log.py."""

from datetime import date

from datastore.api.db import get_duckdb_connection
from datastore.schema import create_backtest
from backtest.core.feature_log import FeatureLogWriter, query_feature_log
from backtest.core.horizon import HorizonBucket


class TestFeatureLogWriter:
    def _fresh_conn(self):
        create_backtest.create_backtest_schema(in_memory=True)
        with get_duckdb_connection(None) as conn:
            conn.execute("DELETE FROM backtest_feature_log")
        return get_duckdb_connection(None)

    def test_record_buffers_without_writing_until_flush(self):
        with self._fresh_conn() as conn:
            writer = FeatureLogWriter(conn, flush_batch_size=100)
            writer.record(
                run_id="r1", ticker="RELIANCE", as_of_date=date(2020, 1, 1),
                horizon_bucket=HorizonBucket.D21, feature_vector={"rsi_14": 55.2},
                decision_taken="bought", signal_output="buy",
            )
            assert len(writer) == 1
            rows = conn.execute("SELECT COUNT(*) FROM backtest_feature_log WHERE run_id = 'r1'").fetchone()[0]
            assert rows == 0

    def test_flush_writes_buffered_rows_and_clears_buffer(self):
        with self._fresh_conn() as conn:
            writer = FeatureLogWriter(conn, flush_batch_size=100)
            writer.record(
                run_id="r2", ticker="TCS", as_of_date=date(2020, 1, 1),
                horizon_bucket=HorizonBucket.D5, feature_vector={"momentum_63d": 0.12},
                decision_taken="skipped_no_cash",
            )
            n = writer.flush()
            assert n == 1
            assert len(writer) == 0
            rows = conn.execute("SELECT COUNT(*) FROM backtest_feature_log WHERE run_id = 'r2'").fetchone()[0]
            assert rows == 1

    def test_flush_on_empty_buffer_is_a_noop(self):
        with self._fresh_conn() as conn:
            writer = FeatureLogWriter(conn)
            assert writer.flush() == 0

    def test_auto_flushes_when_batch_size_reached(self):
        with self._fresh_conn() as conn:
            writer = FeatureLogWriter(conn, flush_batch_size=2)
            for i in range(3):
                writer.record(
                    run_id="r3", ticker=f"T{i}", as_of_date=date(2020, 1, 1),
                    horizon_bucket=HorizonBucket.D21, feature_vector={"x": i},
                    decision_taken="skipped_sector_cap",
                )
            # after 2 records the buffer auto-flushed (batch_size=2); 1 remains buffered
            assert len(writer) == 1
            rows = conn.execute("SELECT COUNT(*) FROM backtest_feature_log WHERE run_id = 'r3'").fetchone()[0]
            assert rows == 2

    def test_query_feature_log_round_trips_feature_vector(self):
        with self._fresh_conn() as conn:
            writer = FeatureLogWriter(conn, flush_batch_size=100)
            writer.record(
                run_id="r4", ticker="INFY", as_of_date=date(2020, 3, 15),
                horizon_bucket=HorizonBucket.Y1, feature_vector={"roe": 0.22, "sector": "IT"},
                decision_taken="bought", signal_output="buy",
            )
            writer.flush()
            results = query_feature_log(conn, "r4")
        assert len(results) == 1
        assert results[0]["ticker"] == "INFY"
        assert results[0]["feature_vector"] == {"roe": 0.22, "sector": "IT"}
        assert results[0]["decision_taken"] == "bought"

    def test_query_feature_log_scoped_to_run_id(self):
        with self._fresh_conn() as conn:
            writer = FeatureLogWriter(conn, flush_batch_size=100)
            writer.record(
                run_id="r5a", ticker="A", as_of_date=date(2020, 1, 1),
                horizon_bucket=HorizonBucket.D5, feature_vector={}, decision_taken="held",
            )
            writer.record(
                run_id="r5b", ticker="B", as_of_date=date(2020, 1, 1),
                horizon_bucket=HorizonBucket.D5, feature_vector={}, decision_taken="held",
            )
            writer.flush()
            results = query_feature_log(conn, "r5a")
        assert len(results) == 1
        assert results[0]["ticker"] == "A"

    def test_record_upserts_on_same_primary_key(self):
        with self._fresh_conn() as conn:
            writer = FeatureLogWriter(conn, flush_batch_size=100)
            writer.record(
                run_id="r6", ticker="X", as_of_date=date(2020, 1, 1),
                horizon_bucket=HorizonBucket.D5, feature_vector={"v": 1}, decision_taken="held",
            )
            writer.flush()
            writer.record(
                run_id="r6", ticker="X", as_of_date=date(2020, 1, 1),
                horizon_bucket=HorizonBucket.D5, feature_vector={"v": 2}, decision_taken="bought",
            )
            writer.flush()
            results = query_feature_log(conn, "r6")
        assert len(results) == 1
        assert results[0]["feature_vector"] == {"v": 2}
        assert results[0]["decision_taken"] == "bought"
