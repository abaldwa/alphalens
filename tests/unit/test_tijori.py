"""
tests/unit/test_tijori.py

Phase: 2.6 (Phase 2 Data Source Integration)
Specs: SPEC-PIPE-001, SPEC-PIPE-003 (CRITICAL), SPEC-SEC-001
Owner: Platform / QA
Consumers: CI, pytest

Tests ingestion/scrapers/tijori.py's sector-metric lookup, HTML parsing,
PIT-default logic, and fiscal-year/quarter derivation entirely offline —
same "no accidental live call" discipline as test_screener.py/test_trendlyne.py.
"""

from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from config.settings import FUNDAMENTALS_ANNOUNCEMENT_DELAY_DAYS
from ingestion.scrapers.tijori import (
    _SECTOR_METRICS,
    TijoriAuthError,
    TijoriScraper,
    _current_quarter_end,
    _fiscal_year_quarter,
    _parse_operating_metrics,
)

_SAMPLE_HTML = """
<html><body>
<table>
<tr><td>ARPU</td><td>185.5</td></tr>
<tr><td>Subscriber Churn Rate %</td><td>2.1</td></tr>
<tr><td>Some Unrelated Row</td><td>999</td></tr>
</table>
</body></html>
"""

_UNIVERSE_DF = pd.DataFrame(
    [
        {"ticker": "BHARTIARTL", "company_name": "Bharti Airtel Ltd", "sector": "Telecommunication"},
        {"ticker": "RANDOMCO", "company_name": "Random Co Ltd", "sector": "Diversified"},
    ]
)


class TestSectorMetrics:
    def test_telecom_matches_build_prompt_example(self):
        assert "ARPU" in _SECTOR_METRICS["Telecommunication"]

    def test_banking_matches_build_prompt_example(self):
        assert "Gross NPA %" in _SECTOR_METRICS["Financial Services"]

    def test_pharma_matches_build_prompt_example(self):
        assert "ANDA Approvals Cumulative" in _SECTOR_METRICS["Healthcare"]

    def test_every_mapped_sector_has_at_most_6_metrics(self):
        for sector, metrics in _SECTOR_METRICS.items():
            assert 1 <= len(metrics) <= 6, f"{sector} has {len(metrics)} metrics"

    def test_diversified_intentionally_unmapped(self):
        """A true conglomerate has no single coherent operational metric set —
        documented gap, not an oversight (see module docstring)."""
        assert "Diversified" not in _SECTOR_METRICS


class TestParseOperatingMetrics:
    def test_parses_values_in_metric_order(self):
        values = _parse_operating_metrics(_SAMPLE_HTML, ["ARPU", "Subscriber Churn Rate %", "Tower Tenancy Ratio"])
        assert values == [185.5, 2.1, None]

    def test_no_matching_rows_returns_all_none(self):
        values = _parse_operating_metrics("<html><body></body></html>", ["ARPU"])
        assert values == [None]


class TestCurrentQuarterEnd:
    def test_returns_most_recent_completed_quarter(self):
        assert _current_quarter_end(date(2025, 5, 20)) == date(2025, 3, 31)
        assert _current_quarter_end(date(2025, 1, 5)) == date(2024, 12, 31)


class TestFiscalYearQuarter:
    def test_maps_calendar_quarter_ends_to_indian_fiscal_year_quarter(self):
        assert _fiscal_year_quarter(date(2025, 6, 30)) == (2026, 1)
        assert _fiscal_year_quarter(date(2025, 9, 30)) == (2026, 2)
        assert _fiscal_year_quarter(date(2025, 12, 31)) == (2026, 3)
        assert _fiscal_year_quarter(date(2026, 3, 31)) == (2026, 4)


class TestLoginAndAuthErrors:
    def test_missing_credentials_raises_auth_error_without_any_network_call(self, monkeypatch):
        monkeypatch.setattr("ingestion.scrapers.tijori.TIJORI_USERNAME", None)
        monkeypatch.setattr("ingestion.scrapers.tijori.TIJORI_PASSWORD", None)
        scraper = TijoriScraper(username=None, password=None)
        with pytest.raises(TijoriAuthError, match="not set"):
            scraper.login()


class TestExportCompanyMetrics:
    def test_unmapped_sector_returns_none_without_fetching(self, monkeypatch):
        scraper = TijoriScraper(username="u", password="p", client=MagicMock())
        fetch_called = []
        monkeypatch.setattr(scraper, "_fetch_company_page", lambda t: fetch_called.append(t) or "<html></html>")

        result = scraper.export_company_metrics("RANDOMCO", "Diversified")

        assert result is None
        assert fetch_called == []

    def test_known_sector_builds_row_in_metric_order(self, monkeypatch):
        scraper = TijoriScraper(username="u", password="p", client=MagicMock())
        monkeypatch.setattr(scraper, "_fetch_company_page", lambda t: _SAMPLE_HTML)

        row = scraper.export_company_metrics("BHARTIARTL", "Telecommunication")

        assert row is not None
        assert row["ticker"] == "BHARTIARTL"
        assert row["sector_specific_metric_1"] == 185.5  # ARPU, position 0
        assert row["sector_specific_metric_2"] == 2.1  # Subscriber Churn Rate %, position 1
        assert row["sector_specific_metric_3"] is None  # Data Usage per Subscriber GB, not found
        assert row["announcement_date"] > row["quarter_end_date"]
        expected = row["quarter_end_date"] + timedelta(days=FUNDAMENTALS_ANNOUNCEMENT_DELAY_DAYS)
        assert row["announcement_date"] == expected

    def test_no_matching_rows_at_all_returns_none(self, monkeypatch):
        scraper = TijoriScraper(username="u", password="p", client=MagicMock())
        monkeypatch.setattr(scraper, "_fetch_company_page", lambda t: "<html><body></body></html>")

        result = scraper.export_company_metrics("BHARTIARTL", "Telecommunication")

        assert result is None


class TestBatchExport:
    def test_sector_detected_from_stock_master(self, monkeypatch):
        """Build prompt deliverable: 'Sector detection from stock_master.sector column'."""
        scraper = TijoriScraper(username="u", password="p", client=MagicMock())
        sectors_seen = []

        def fake_export(ticker, sector):
            sectors_seen.append((ticker, sector))
            return {"ticker": ticker, "fiscal_year": 2026, "quarter": 1,
                    "quarter_end_date": date(2025, 6, 30), "announcement_date": date(2025, 8, 14)}

        monkeypatch.setattr(scraper, "export_company_metrics", fake_export)
        monkeypatch.setattr("ingestion.scrapers.tijori.time.sleep", lambda s: None)
        with patch("ingestion.scrapers.tijori.load_universe_raw", return_value=_UNIVERSE_DF):
            scraper.batch_export()

        assert ("BHARTIARTL", "Telecommunication") in sectors_seen
        assert ("RANDOMCO", "Diversified") in sectors_seen

    def test_unmapped_sector_counts_as_success_not_failure(self, monkeypatch):
        scraper = TijoriScraper(username="u", password="p", client=MagicMock())
        monkeypatch.setattr(scraper, "export_company_metrics", lambda t, s: None)
        monkeypatch.setattr("ingestion.scrapers.tijori.time.sleep", lambda s: None)
        with patch("ingestion.scrapers.tijori.load_universe_raw", return_value=_UNIVERSE_DF):
            results = scraper.batch_export()

        assert results == {"BHARTIARTL": True, "RANDOMCO": True}
        scraper.client.write_fundamentals.assert_not_called()

    def test_one_bad_ticker_does_not_abort_the_batch(self, monkeypatch):
        scraper = TijoriScraper(username="u", password="p", client=MagicMock())

        def fake_export(ticker, sector):
            if ticker == "BHARTIARTL":
                raise ConnectionError("boom")
            return None

        monkeypatch.setattr(scraper, "export_company_metrics", fake_export)
        monkeypatch.setattr("ingestion.scrapers.tijori.time.sleep", lambda s: None)
        with patch("ingestion.scrapers.tijori.load_universe_raw", return_value=_UNIVERSE_DF):
            results = scraper.batch_export()

        assert results == {"BHARTIARTL": False, "RANDOMCO": True}

    def test_write_false_skips_api_calls(self, monkeypatch):
        scraper = TijoriScraper(username="u", password="p", client=MagicMock())
        fake_row = {
            "ticker": "x", "fiscal_year": 2026, "quarter": 1,
            "quarter_end_date": date(2025, 6, 30), "announcement_date": date(2025, 8, 14),
        }
        monkeypatch.setattr(scraper, "export_company_metrics", lambda t, s: {**fake_row, "ticker": t})
        monkeypatch.setattr("ingestion.scrapers.tijori.time.sleep", lambda s: None)
        with patch("ingestion.scrapers.tijori.load_universe_raw", return_value=_UNIVERSE_DF):
            scraper.batch_export(write=False)

        scraper.client.write_fundamentals.assert_not_called()
