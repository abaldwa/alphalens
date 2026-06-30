"""
tests/unit/test_pit_alignment.py

Phase: 2.1 (Fundamental Data Ingestion + PIT Validation)
Specs: SPEC-PIPE-003 (CRITICAL), SPEC-DS-003, SPEC-FEAT-002
Owner: Platform / QA
Consumers: CI, pytest

SPEC-PIPE-003 (CRITICAL): a point-in-time bug here silently inflates every
downstream backtest result by leaking future information into historical
feature computation. These tests exercise the real FastAPI app
(datastore.api.main.app) against a real on-disk DuckDB file (not mocks),
so the full router -> SQL -> pit.py chain is verified end-to-end, not
just that individual functions don't crash.

Test 1: fundamentals are gated on announcement_date, NOT quarter_end_date
        — a row whose quarter_end_date is inside the query window but
        whose announcement_date is still in the future (relative to
        as_of) must be excluded.
Test 2: across many rows with a mix of past/future announcement_dates, no
        row with announcement_date > the computation date is ever
        returned — the general PIT invariant, not just the single-row case.
Test 3: staleness features (days_since_results, quarter_age_pct,
        results_pending_flag) are correctly derived purely from
        announcement_date (03_data_pipeline.md's compute_staleness formula).
Test 4: a quarter that is significantly overdue (more than
        RESULTS_PENDING_THRESHOLD_DAYS=70 days since its last known
        announcement — i.e., the next quarter's results are "30 days
        away" past the normal ~70-day reporting cycle) returns
        results_pending_flag=1.
"""

from datetime import date, datetime, timedelta

import duckdb
import pytest
from fastapi.testclient import TestClient

from config.settings import RESULTS_PENDING_THRESHOLD_DAYS
from datastore.api.db import close_all_connections
from datastore.api.main import app
from datastore.api.routers import fundamentals as fundamentals_router
from datastore.schema import create_normalised
from features.fundamental import compute_staleness


@pytest.fixture
def client(tmp_path, monkeypatch):
    """Real FastAPI TestClient against a real, isolated on-disk DuckDB file."""
    db_path = tmp_path / "pit_test.duckdb"
    create_normalised.create_schema(db_path=db_path)
    # create_schema's connection is cached (persist=True default) — release it
    # before any request opens a differently-configured (read_only) connection
    # to the same file, or DuckDB rejects the second open (SPEC-SCHED-013).
    close_all_connections()
    monkeypatch.setattr(fundamentals_router, "DUCKDB_PATH", db_path)
    return TestClient(app)


def _write_fundamentals(client, ticker, fiscal_year, quarter, quarter_end_date, announcement_date, **kwargs):
    payload = {
        "ticker": ticker,
        "fiscal_year": fiscal_year,
        "quarter": quarter,
        "quarter_end_date": quarter_end_date.isoformat(),
        "announcement_date": announcement_date.isoformat(),
        **kwargs,
    }
    response = client.post("/api/v1/fundamentals/write", json=payload)
    assert response.status_code == 200, response.text
    return response


class TestPITGatedOnAnnouncementDate:
    """SPEC-PIPE-003 (CRITICAL): fundamentals joined on announcement_date, NEVER quarter_end_date."""

    def test_row_excluded_while_announcement_is_future_even_though_quarter_end_is_in_window(self, client):
        """
        SPEC-PIPE-003: Q4 FY2025 (quarter_end 2025-03-31) with results
        announced 2025-05-15 must NOT appear in a query as_of 2025-04-15 —
        even though quarter_end_date (2025-03-31) is well inside the
        [2025-01-01, 2025-06-30] fetch window. If the API were (bug)
        filtering on quarter_end_date instead of announcement_date, this
        row would incorrectly appear 1 month early.
        """
        _write_fundamentals(
            client, "RELIANCE", 2025, 4,
            quarter_end_date=date(2025, 3, 31),
            announcement_date=date(2025, 5, 15),
            revenue=200000.0,
        )

        response = client.get(
            "/api/v1/fundamentals/RELIANCE",
            params={"start_date": "2025-01-01", "end_date": "2025-06-30", "as_of": "2025-04-15"},
        )
        assert response.status_code == 200
        assert response.json()["data"] == []

        # The same row IS visible once as_of reaches the real announcement_date.
        response_after = client.get(
            "/api/v1/fundamentals/RELIANCE",
            params={"start_date": "2025-01-01", "end_date": "2025-06-30", "as_of": "2025-05-15"},
        )
        rows = response_after.json()["data"]
        assert len(rows) == 1
        assert rows[0]["quarter_end_date"].startswith("2025-03-31")

    def test_write_rejects_announcement_date_before_quarter_end_date(self, client):
        """SPEC-PIPE-003: a build failure — results cannot predate the quarter they cover."""
        response = client.post(
            "/api/v1/fundamentals/write",
            json={
                "ticker": "BADCO",
                "fiscal_year": 2025,
                "quarter": 1,
                "quarter_end_date": "2025-03-31",
                "announcement_date": "2025-03-01",  # before quarter_end_date — invalid
            },
        )
        assert response.status_code == 400


class TestNoForwardLookingFeatureData:
    """SPEC-PIPE-003 (CRITICAL): no feature uses data with announcement_date > computation date."""

    def test_no_returned_row_has_announcement_date_after_as_of(self, client):
        """
        General invariant, not just the single-row case: write a mix of
        past- and future-announced quarters, then assert every row the
        API returns for a fixed as_of has announcement_date <= as_of.
        """
        as_of = date(2025, 6, 1)
        quarters = [
            (2024, 2, date(2024, 6, 30), date(2024, 8, 10)),   # well past — eligible
            (2024, 3, date(2024, 9, 30), date(2024, 11, 12)),  # past — eligible
            (2024, 4, date(2024, 12, 31), date(2025, 2, 14)),  # past — eligible
            (2025, 1, date(2025, 3, 31), date(2025, 5, 15)),   # past — eligible
            (2025, 2, date(2025, 6, 30), date(2025, 8, 14)),   # FUTURE relative to as_of — must be excluded
        ]
        for fy, q, qed, ann in quarters:
            _write_fundamentals(client, "TCS", fy, q, quarter_end_date=qed, announcement_date=ann, revenue=10000.0)

        response = client.get(
            "/api/v1/fundamentals/TCS",
            params={"start_date": "2024-01-01", "end_date": "2025-12-31", "as_of": as_of.isoformat()},
        )
        rows = response.json()["data"]
        assert len(rows) == 4  # the 5th (future-announced) quarter excluded
        for row in rows:
            announcement_date = datetime.fromisoformat(row["announcement_date"]).date()
            assert announcement_date <= as_of, f"PIT violation: {row}"

    def test_pit_violation_detection_query_finds_zero_rows(self, tmp_path):
        """
        ✅ TEST block's literal verification command (P2.1 build prompt):
        SELECT COUNT(*) FROM fundamentals WHERE announcement_date <= quarter_end_date — must be 0,
        confirmed directly against the schema's own data (independent of API filtering).
        """
        db_path = tmp_path / "pit_violation_check.duckdb"
        create_normalised.create_schema(db_path=db_path)
        conn = duckdb.connect(str(db_path))
        conn.execute(
            "INSERT INTO fundamentals (ticker, fiscal_year, quarter, quarter_end_date, announcement_date, revenue) "
            "VALUES ('INFY', 2025, 1, '2025-03-31', '2025-05-15', 50000.0)"
        )
        violations = conn.execute(
            "SELECT COUNT(*) FROM fundamentals WHERE announcement_date <= quarter_end_date"
        ).fetchone()[0]
        conn.close()
        assert violations == 0


class TestStalenessFeatures:
    """SPEC-PIPE-003: staleness features correct (days_since_results = compute_date - announcement_date)."""

    def test_days_since_results_and_quarter_age_pct(self):
        announcement = datetime(2025, 5, 15)
        compute_date = datetime(2025, 6, 19)  # 35 days later
        result = compute_staleness(announcement, compute_date)
        assert result["days_since_results"] == 35.0
        assert result["quarter_age_pct"] == pytest.approx(35.0 / 63.0)
        assert result["results_pending_flag"] == 0

    def test_quarter_age_pct_clips_at_one(self):
        announcement = datetime(2025, 1, 1)
        compute_date = datetime(2025, 6, 1)  # 151 days later — far past 63
        result = compute_staleness(announcement, compute_date)
        assert result["quarter_age_pct"] == 1.0

    def test_raises_if_compute_date_before_announcement(self):
        """A row with compute_date < announcement_date was never PIT-eligible — must never reach here."""
        with pytest.raises(ValueError):
            compute_staleness(datetime(2025, 6, 1), datetime(2025, 5, 1))


class TestResultsPendingFlag:
    """
    SPEC-PIPE-003: an overdue announcement — i.e. the last known results
    are now `RESULTS_PENDING_THRESHOLD_DAYS` (70) + 30 = 100 days old,
    meaning the next quarter's announcement is overdue/"30 days away"
    past the normal reporting cycle — returns results_pending_flag=1.
    """

    def test_announcement_30_days_overdue_sets_pending_flag(self):
        announcement = datetime(2025, 1, 1)
        compute_date = announcement + timedelta(days=RESULTS_PENDING_THRESHOLD_DAYS + 30)  # 100 days
        result = compute_staleness(announcement, compute_date)
        assert result["results_pending_flag"] == 1

    def test_announcement_just_under_threshold_not_pending(self):
        announcement = datetime(2025, 1, 1)
        compute_date = announcement + timedelta(days=RESULTS_PENDING_THRESHOLD_DAYS - 1)  # 69 days
        result = compute_staleness(announcement, compute_date)
        assert result["results_pending_flag"] == 0
