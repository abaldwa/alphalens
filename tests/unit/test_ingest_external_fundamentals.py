"""
tests/unit/test_ingest_external_fundamentals.py

F5 (2026-07-10): scripts/ingest_external_fundamentals.py previously only
logged what it "would write" — this tests the real pivot logic
(_infer_quarter_end/_fiscal_year_quarter/_pivot_to_fundamentals_rows) that
turns the CSV source's long/EAV rows into FundamentalsWrite-shaped wide
rows, and the actual DB write path.
"""

from datetime import date

from datastore.api.db import get_duckdb_connection
from datastore.schema import create_normalised
from scripts.ingest_external_fundamentals import (
    _fiscal_year_quarter,
    _infer_quarter_end,
    _pivot_to_fundamentals_rows,
    main as ingest_main,
)


class TestInferQuarterEnd:
    def test_mid_quarter_date_rounds_back_to_prior_quarter_end(self):
        assert _infer_quarter_end(date(2026, 5, 15)) == date(2026, 3, 31)

    def test_exact_quarter_end_date_rounds_back_to_the_PRIOR_quarter(self):
        """SPEC-PIPE-003: announcement_date must be STRICTLY after
        quarter_end_date — if as_of_date itself lands exactly on a quarter
        boundary, quarter_end_date must be the one before it, not itself."""
        assert _infer_quarter_end(date(2026, 3, 31)) == date(2025, 12, 31)

    def test_early_january_rolls_back_into_prior_calendar_year(self):
        assert _infer_quarter_end(date(2026, 1, 5)) == date(2025, 12, 31)


class TestFiscalYearQuarter:
    def test_march_quarter_end_is_q4_of_same_fiscal_year(self):
        assert _fiscal_year_quarter(date(2026, 3, 31)) == (2026, 4)

    def test_june_quarter_end_is_q1_of_next_fiscal_year(self):
        assert _fiscal_year_quarter(date(2026, 6, 30)) == (2027, 1)


class TestPivotToFundamentalsRows:
    def test_groups_multiple_metrics_for_same_ticker_quarter_into_one_row(self):
        rows = [
            {"ticker": "AAA", "metric": "revenue", "as_of_date": "2026-05-15", "value": 100.0},
            {"ticker": "AAA", "metric": "pat", "as_of_date": "2026-05-20", "value": 10.0},
        ]
        pivoted = _pivot_to_fundamentals_rows(rows)
        assert len(pivoted) == 1
        assert pivoted[0]["ticker"] == "AAA"
        assert pivoted[0]["revenue"] == 100.0
        assert pivoted[0]["pat"] == 10.0
        assert pivoted[0]["quarter_end_date"] == date(2026, 3, 31)

    def test_unrecognized_metric_is_dropped_not_fabricated_into_some_column(self):
        rows = [{"ticker": "AAA", "metric": "totally_made_up", "as_of_date": "2026-05-15", "value": 1.0}]
        pivoted = _pivot_to_fundamentals_rows(rows)
        assert pivoted == []

    def test_latest_as_of_date_wins_as_announcement_date(self):
        rows = [
            {"ticker": "AAA", "metric": "revenue", "as_of_date": "2026-05-01", "value": 100.0},
            {"ticker": "AAA", "metric": "pat", "as_of_date": "2026-05-20", "value": 10.0},
        ]
        pivoted = _pivot_to_fundamentals_rows(rows)
        assert pivoted[0]["announcement_date"] == date(2026, 5, 20)

    def test_different_tickers_produce_separate_rows(self):
        rows = [
            {"ticker": "AAA", "metric": "revenue", "as_of_date": "2026-05-15", "value": 100.0},
            {"ticker": "BBB", "metric": "revenue", "as_of_date": "2026-05-15", "value": 200.0},
        ]
        pivoted = _pivot_to_fundamentals_rows(rows)
        assert {r["ticker"] for r in pivoted} == {"AAA", "BBB"}

    def test_unparseable_as_of_date_is_skipped_not_raised(self):
        rows = [{"ticker": "AAA", "metric": "revenue", "as_of_date": "not-a-date", "value": 100.0}]
        pivoted = _pivot_to_fundamentals_rows(rows)
        assert pivoted == []


class TestMainWritePath:
    def test_dry_run_writes_nothing(self, tmp_path, monkeypatch, capsys):
        csv_path = tmp_path / "external.csv"
        csv_path.write_text(
            "ticker,metric,as_of_date,value,source,confidence\n"
            "AAA,revenue,2026-05-15,100.0,vendor_x,0.9\n"
        )
        create_normalised.create_schema(in_memory=True)

        monkeypatch.setattr(
            "sys.argv",
            ["ingest_external_fundamentals.py", "--csv", str(csv_path), "--dry-run"],
        )
        ingest_main()

        with get_duckdb_connection(None) as conn:
            count = conn.execute("SELECT COUNT(*) FROM fundamentals").fetchone()[0]
        assert count == 0

    def test_real_run_writes_rows_tagged_external_csv(self, tmp_path, monkeypatch):
        csv_path = tmp_path / "external.csv"
        csv_path.write_text(
            "ticker,metric,as_of_date,value,source,confidence\n"
            "AAA,revenue,2026-05-15,100.0,vendor_x,0.9\n"
            "AAA,pat,2026-05-20,10.0,vendor_x,0.9\n"
        )
        create_normalised.create_schema(in_memory=True)

        with get_duckdb_connection(None) as conn:
            monkeypatch.setattr(
                "scripts.ingest_external_fundamentals.get_duckdb_connection",
                lambda path, persist=True: _FixedConn(conn),
            )
            monkeypatch.setattr(
                "sys.argv",
                ["ingest_external_fundamentals.py", "--csv", str(csv_path)],
            )
            ingest_main()

            row = conn.execute(
                "SELECT ticker, revenue, pat, fundamentals_source, fundamentals_source_priority "
                "FROM fundamentals WHERE ticker = 'AAA'"
            ).fetchone()
        assert row == ("AAA", 100.0, 10.0, "external_csv", 1)

    def test_real_run_never_overwrites_a_higher_priority_source(self, tmp_path, monkeypatch):
        csv_path = tmp_path / "external.csv"
        csv_path.write_text(
            "ticker,metric,as_of_date,value,source,confidence\n"
            "AAA,revenue,2026-05-15,999.0,vendor_x,0.9\n"
        )
        create_normalised.create_schema(in_memory=True)

        with get_duckdb_connection(None) as conn:
            conn.execute(
                "INSERT INTO fundamentals (ticker, fiscal_year, quarter, quarter_end_date, "
                "announcement_date, revenue, fundamentals_source, fundamentals_source_priority) "
                "VALUES ('AAA', 2026, 4, '2026-03-31', '2026-05-01', 500.0, 'nse_xbrl', 4)"
            )
            monkeypatch.setattr(
                "scripts.ingest_external_fundamentals.get_duckdb_connection",
                lambda path, persist=True: _FixedConn(conn),
            )
            monkeypatch.setattr(
                "sys.argv",
                ["ingest_external_fundamentals.py", "--csv", str(csv_path)],
            )
            ingest_main()

            row = conn.execute(
                "SELECT revenue, fundamentals_source FROM fundamentals WHERE ticker = 'AAA'"
            ).fetchone()
        assert row == (500.0, "nse_xbrl")  # untouched — external_csv (1) < nse_xbrl (4)


class _FixedConn:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, *exc_info):
        return False
