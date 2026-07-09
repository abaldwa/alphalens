"""
tests/unit/test_staging_rollout.py

Phase: A25 (Write-Audit-Publish Architecture) — full rollout
Owner: Platform / QA
Consumers: CI, pytest

Integration tests for the staged-mode write paths added to
ingestion/scrapers/amfi_holdings.py::sync_duckdb_table and
ingestion/scrapers/corporate_actions.py::upsert_corporate_actions_staged,
against the real DDL (datastore/schema/create_normalised.py) on a private
in-memory DuckDB connection — never the real alphalens.duckdb.
"""

from datetime import date

import duckdb
import pandas as pd

from datastore.schema.create_normalised import _CREATE_CORPORATE_ACTIONS, _CREATE_MF_HOLDINGS
from ingestion.scrapers.amfi_holdings import sync_duckdb_table
from ingestion.scrapers.corporate_actions import upsert_corporate_actions_staged


def _mf_holdings_conn():
    conn = duckdb.connect(":memory:")
    conn.execute(_CREATE_MF_HOLDINGS)
    return conn


def _corporate_actions_conn():
    conn = duckdb.connect(":memory:")
    conn.execute(_CREATE_CORPORATE_ACTIONS)
    return conn


class TestSyncDuckdbTableStaged:
    def test_staged_matches_direct_for_a_fresh_month(self, tmp_path):
        df = pd.DataFrame({
            "ticker": ["RELIANCE", "TCS"],
            "scheme_name": ["Fund A", "Fund B"],
            "isin": ["INE1", "INE2"],
            "quantity": [100, 200],
            "value_inr": [1000.0, 2000.0],
            "availability_date": [date(2026, 6, 5), date(2026, 6, 5)],
        })
        df.to_parquet(tmp_path / "2026-06.parquet")

        direct_conn = _mf_holdings_conn()
        sync_duckdb_table(direct_conn, 2026, 6, output_dir=tmp_path, publish_mode="direct")
        direct_rows = direct_conn.execute("SELECT * FROM mf_holdings ORDER BY ticker").df()

        staged_conn = _mf_holdings_conn()
        sync_duckdb_table(staged_conn, 2026, 6, output_dir=tmp_path, publish_mode="staged")
        staged_rows = staged_conn.execute("SELECT * FROM mf_holdings ORDER BY ticker").df()

        assert list(direct_rows["ticker"]) == list(staged_rows["ticker"]) == ["RELIANCE", "TCS"]

    def test_staged_replace_of_one_month_leaves_other_months_untouched(self, tmp_path):
        conn = _mf_holdings_conn()
        conn.execute(
            "INSERT INTO mf_holdings VALUES ('OLDCO', '2026-05-01', 'Fund X', 'INE9', 50, 500.0, '2026-05-05')"
        )

        df = pd.DataFrame({
            "ticker": ["RELIANCE"], "scheme_name": ["Fund A"], "isin": ["INE1"],
            "quantity": [100], "value_inr": [1000.0], "availability_date": [date(2026, 6, 5)],
        })
        df.to_parquet(tmp_path / "2026-06.parquet")

        sync_duckdb_table(conn, 2026, 6, output_dir=tmp_path, publish_mode="staged")

        rows = conn.execute("SELECT ticker, month FROM mf_holdings ORDER BY ticker").df()
        assert set(rows["ticker"]) == {"OLDCO", "RELIANCE"}

    def test_staged_rerun_is_idempotent(self, tmp_path):
        df = pd.DataFrame({
            "ticker": ["RELIANCE"], "scheme_name": ["Fund A"], "isin": ["INE1"],
            "quantity": [100], "value_inr": [1000.0], "availability_date": [date(2026, 6, 5)],
        })
        df.to_parquet(tmp_path / "2026-06.parquet")

        conn = _mf_holdings_conn()
        sync_duckdb_table(conn, 2026, 6, output_dir=tmp_path, publish_mode="staged")
        sync_duckdb_table(conn, 2026, 6, output_dir=tmp_path, publish_mode="staged")

        count = conn.execute("SELECT COUNT(*) FROM mf_holdings").fetchone()[0]
        assert count == 1


class TestUpsertCorporateActionsStaged:
    def test_new_rows_are_added(self):
        conn = _corporate_actions_conn()
        df = pd.DataFrame({
            "ticker": ["RELIANCE"], "ex_date": [date(2026, 1, 1)], "action_type": ["DIVIDEND"],
            "ratio": [1.0], "announcement_date": [date(2025, 12, 1)],
            "record_date": [date(2025, 12, 15)], "details": ["Rs 10/share"],
        })
        n = upsert_corporate_actions_staged(conn, df)
        assert n == 1
        rows = conn.execute("SELECT * FROM corporate_actions").fetchall()
        assert len(rows) == 1

    def test_existing_row_is_never_overwritten(self):
        conn = _corporate_actions_conn()
        conn.execute(
            "INSERT INTO corporate_actions VALUES "
            "('RELIANCE', '2026-01-01', 'DIVIDEND', 1.0, '2025-12-01', '2025-12-15', 'ORIGINAL')"
        )
        df = pd.DataFrame({
            "ticker": ["RELIANCE"], "ex_date": [date(2026, 1, 1)], "action_type": ["DIVIDEND"],
            "ratio": [999.0], "announcement_date": [date(2025, 12, 1)],
            "record_date": [date(2025, 12, 15)], "details": ["CHANGED"],
        })
        n = upsert_corporate_actions_staged(conn, df)
        assert n == 0
        row = conn.execute("SELECT ratio, details FROM corporate_actions").fetchone()
        assert row == (1.0, "ORIGINAL")

    def test_empty_dataframe_is_a_noop(self):
        conn = _corporate_actions_conn()
        n = upsert_corporate_actions_staged(conn, pd.DataFrame())
        assert n == 0
