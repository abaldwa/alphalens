"""
tests/unit/test_backfill_bhavcopy_raw.py

Phase: A25 (Write-Audit-Publish Architecture)
Owner: Platform / QA
Consumers: CI, pytest

Tests scripts/backfill_bhavcopy_raw.py's date-selection logic — resumable/
idempotent (skips dates whose raw CSV already exists), trading-calendar-
correct (skips weekends/holidays). No network calls, no real DuckDB.
"""

from datetime import date

from scripts.backfill_bhavcopy_raw import _pending_dates


class TestPendingDates:
    def test_skips_dates_with_existing_raw_csv(self, tmp_path, monkeypatch):
        import scripts.backfill_bhavcopy_raw as mod

        raw_dir = tmp_path / "bhavcopy"
        raw_dir.mkdir()
        (raw_dir / "2026-07-01.csv").write_text("already,here")
        monkeypatch.setattr(mod, "_RAW_BHAVCOPY_DIR", raw_dir)
        monkeypatch.setattr(mod, "is_trading_day", lambda d: True)

        pending = _pending_dates(date(2026, 7, 1), date(2026, 7, 2))
        assert pending == [date(2026, 7, 2)]

    def test_skips_non_trading_days(self, tmp_path, monkeypatch):
        import scripts.backfill_bhavcopy_raw as mod

        monkeypatch.setattr(mod, "_RAW_BHAVCOPY_DIR", tmp_path / "bhavcopy")
        weekend = {date(2026, 7, 4), date(2026, 7, 5)}  # Sat/Sun
        monkeypatch.setattr(mod, "is_trading_day", lambda d: d not in weekend)

        pending = _pending_dates(date(2026, 7, 3), date(2026, 7, 6))
        assert weekend.isdisjoint(pending)
        assert date(2026, 7, 3) in pending
        assert date(2026, 7, 6) in pending

    def test_empty_range_when_all_dates_present(self, tmp_path, monkeypatch):
        import scripts.backfill_bhavcopy_raw as mod

        raw_dir = tmp_path / "bhavcopy"
        raw_dir.mkdir()
        (raw_dir / "2026-07-01.csv").write_text("x")
        monkeypatch.setattr(mod, "_RAW_BHAVCOPY_DIR", raw_dir)
        monkeypatch.setattr(mod, "is_trading_day", lambda d: True)

        pending = _pending_dates(date(2026, 7, 1), date(2026, 7, 1))
        assert pending == []

    def test_correct_date_range_from_trading_calendar(self, tmp_path, monkeypatch):
        import scripts.backfill_bhavcopy_raw as mod

        monkeypatch.setattr(mod, "_RAW_BHAVCOPY_DIR", tmp_path / "bhavcopy")
        monkeypatch.setattr(mod, "is_trading_day", lambda d: True)

        pending = _pending_dates(date(2026, 7, 1), date(2026, 7, 5))
        assert pending == [date(2026, 7, 1), date(2026, 7, 2), date(2026, 7, 3),
                            date(2026, 7, 4), date(2026, 7, 5)]
