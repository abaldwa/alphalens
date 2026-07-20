"""tests/unit/test_migrate_momentum_paper_trading_dry_run.py —
scripts/migrate_momentum_paper_trading_dry_run.py.

Verifies the dry-run report against a real (in-memory) momentum_trades
table with real-shaped rows — and, critically, that it never writes
anything (no paper_trading/ files, no DB writes beyond the read-only
connection).
"""

import pytest

from datastore.api.db import get_duckdb_connection
from datastore.schema.create_normalised import _CREATE_MOMENTUM_TRADES
from scripts import migrate_momentum_paper_trading_dry_run as dry_run


@pytest.fixture
def seeded_conn(monkeypatch):
    monkeypatch.setattr(dry_run, "DUCKDB_PATH", None)
    with get_duckdb_connection(None) as conn:
        conn.execute(_CREATE_MOMENTUM_TRADES)
        conn.execute("DELETE FROM momentum_trades")
        conn.execute(
            """
            INSERT INTO momentum_trades
                (strategy_id, ticker, purchase_date, qty, purchase_price, sale_date, sell_price)
            VALUES
                ('band_1_15', 'RELIANCE', '2024-01-05', 10, 2500.0, '2024-02-05', 2650.0),
                ('band_1_15', 'TCS', '2024-01-05', 5, 3800.0, NULL, NULL),
                ('band_1_15', 'INFY', '2024-01-05', 8, NULL, NULL, NULL)
            """
        )
        yield conn


class TestFetchRealMomentumTrades:
    def test_no_table_returns_empty(self, monkeypatch):
        monkeypatch.setattr(dry_run, "DUCKDB_PATH", None)
        with get_duckdb_connection(None) as conn:
            conn.execute("DROP TABLE IF EXISTS momentum_trades")
        assert dry_run._fetch_real_momentum_trades() == []


class TestRunDryRun:
    def test_reports_open_and_closed_trades_separately(self, seeded_conn):
        report = dry_run.run_dry_run()
        s = report["strategies"]["band_1_15"]
        assert s["n_trades"] == 3
        assert s["n_open"] == 2  # TCS (no sale_date) + INFY (unmappable, still counted as open)
        assert s["n_closed"] == 1  # RELIANCE

    def test_unmappable_row_flagged_not_silently_dropped(self, seeded_conn):
        report = dry_run.run_dry_run()
        s = report["strategies"]["band_1_15"]
        assert s["n_unmappable_rows"] == 1
        assert "purchase_price is NULL" in s["unmappable_trade_ids_and_reasons"][0]["problems"][0]

    def test_gate7_day_count_only_reflects_mappable_actions(self, seeded_conn):
        report = dry_run.run_dry_run()
        s = report["strategies"]["band_1_15"]
        # RELIANCE contributes buy(2024-01-05) + sell(2024-02-05); TCS contributes buy(2024-01-05,
        # same day as RELIANCE's buy -> dedup); INFY contributes nothing (unmappable)
        assert s["distinct_trading_days_that_would_count_toward_gate7"] == 2

    def test_total_capital_deployed_ignores_unmappable_rows(self, seeded_conn):
        report = dry_run.run_dry_run()
        s = report["strategies"]["band_1_15"]
        assert s["total_capital_deployed_across_all_buys_inr"] == pytest.approx(10 * 2500.0 + 5 * 3800.0)

    def test_required_human_decision_flag_always_present(self, seeded_conn):
        report = dry_run.run_dry_run()
        s = report["strategies"]["band_1_15"]
        assert "initial_capital" in s["REQUIRED_HUMAN_DECISION"]
        assert "horizon_bucket" in s["REQUIRED_HUMAN_DECISION"]

    def test_no_rows_at_all_returns_empty_strategies(self, monkeypatch):
        monkeypatch.setattr(dry_run, "DUCKDB_PATH", None)
        with get_duckdb_connection(None) as conn:
            conn.execute(_CREATE_MOMENTUM_TRADES)
            conn.execute("DELETE FROM momentum_trades")
        report = dry_run.run_dry_run()
        assert report == {"strategies": {}, "n_trades_total": 0}

    def test_never_writes_anything(self, seeded_conn, monkeypatch, tmp_path):
        """The dry run must never touch backtest/paper_trading/'s file tree."""
        written_files = []
        original_write_text = __import__("pathlib").Path.write_text

        def _tracking_write_text(self, *args, **kwargs):
            written_files.append(str(self))
            return original_write_text(self, *args, **kwargs)

        monkeypatch.setattr("pathlib.Path.write_text", _tracking_write_text)
        dry_run.run_dry_run()
        assert written_files == []
