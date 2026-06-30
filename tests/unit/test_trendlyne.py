"""
tests/unit/test_trendlyne.py

Phase: 2.6 (Phase 2 Data Source Integration)
Specs: SPEC-PIPE-001, SPEC-PIPE-003 (CRITICAL), SPEC-SEC-001
Owner: Platform / QA
Consumers: CI, pytest

Tests ingestion/scrapers/trendlyne.py's HTML parsing, company-name-to-
ticker mapping, aggregation rule, and PIT-default logic entirely offline —
no real network call to Trendlyne is made or mocked-and-silently-skipped,
same "no accidental live call" discipline as tests/unit/test_screener.py.
"""

from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from config.settings import SHAREHOLDING_FILING_DELAY_DAYS
from ingestion.scrapers.trendlyne import (
    SUPERSTAR_INVESTORS,
    TrendlyneAuthError,
    TrendlyneScraper,
    _current_quarter_end,
    _normalize_company_name,
    _parse_holdings_table,
)

_SAMPLE_HTML = """
<html><body>
<table>
<tr><th>Company</th><th>Holding %</th><th>Change</th></tr>
<tr><td>HDFC Bank Ltd</td><td>2.5</td><td>+0.3</td></tr>
<tr><td>Tata Motors Limited</td><td>1.8</td><td>-0.2</td></tr>
</table>
</body></html>
"""

_UNIVERSE_DF = pd.DataFrame(
    [
        {"ticker": "HDFCBANK", "company_name": "HDFC Bank Ltd", "sector": "Financial Services"},
        {"ticker": "TATAMOTORS", "company_name": "Tata Motors Ltd", "sector": "Automobile and Auto Components"},
    ]
)


class TestNormalizeCompanyName:
    def test_strips_ltd_limited_and_casing(self):
        assert _normalize_company_name("HDFC Bank Ltd") == "hdfcbank"
        assert _normalize_company_name("Tata Motors Limited") == "tatamotors"

    def test_none_returns_empty_string(self):
        assert _normalize_company_name(None) == ""


class TestParseHoldingsTable:
    def test_parses_company_stake_and_change(self):
        rows = _parse_holdings_table(_SAMPLE_HTML)
        assert len(rows) == 2
        assert rows[0] == {"company_name": "HDFC Bank Ltd", "stake_pct": 2.5, "qoq_change_pct": 0.3}
        assert rows[1] == {"company_name": "Tata Motors Limited", "stake_pct": 1.8, "qoq_change_pct": -0.2}

    def test_no_table_returns_empty_list(self):
        assert _parse_holdings_table("<html><body></body></html>") == []


class TestCurrentQuarterEnd:
    def test_returns_most_recent_completed_quarter(self):
        assert _current_quarter_end(date(2025, 5, 20)) == date(2025, 3, 31)
        assert _current_quarter_end(date(2025, 1, 5)) == date(2024, 12, 31)


class TestLoginAndAuthErrors:
    def test_missing_credentials_raises_auth_error_without_any_network_call(self, monkeypatch):
        """.env ships placeholder strings — explicitly None out the module-level
        constants so this test can never make a live network call regardless of
        what's in .env, same requirement as test_screener.py's identical test."""
        monkeypatch.setattr("ingestion.scrapers.trendlyne.TRENDLYNE_USERNAME", None)
        monkeypatch.setattr("ingestion.scrapers.trendlyne.TRENDLYNE_PASSWORD", None)
        scraper = TrendlyneScraper(username=None, password=None)
        with pytest.raises(TrendlyneAuthError, match="not set"):
            scraper.login()

    def test_unknown_investor_raises_value_error(self):
        scraper = TrendlyneScraper(username="u", password="p", client=MagicMock())
        with pytest.raises(ValueError, match="Unknown superstar investor"):
            scraper.fetch_investor_holdings("Not A Real Investor")


class TestExportSuperstarHoldings:
    def test_aggregates_across_investors_and_maps_to_tickers(self, monkeypatch):
        """A ticker held by 2 of the 5 investors gets superstar_flag=True and
        superstar_change = sum of both investors' QoQ changes (module docstring's
        documented aggregation rule)."""
        scraper = TrendlyneScraper(username="u", password="p", client=MagicMock())

        def fake_fetch(investor_name):
            if investor_name == "Dolly Khanna":
                return [{"company_name": "HDFC Bank Ltd", "stake_pct": 2.5, "qoq_change_pct": 0.3}]
            if investor_name == "Vijay Kedia":
                return [{"company_name": "HDFC Bank Ltd", "stake_pct": 1.0, "qoq_change_pct": 0.1}]
            return []

        monkeypatch.setattr(scraper, "fetch_investor_holdings", fake_fetch)
        monkeypatch.setattr("ingestion.scrapers.trendlyne.time.sleep", lambda s: None)
        with patch("ingestion.scrapers.trendlyne.load_universe_raw", return_value=_UNIVERSE_DF):
            result = scraper.export_superstar_holdings()

        assert result == {"HDFCBANK": {"superstar_flag": True, "superstar_change": pytest.approx(0.4)}}

    def test_unmatched_company_name_is_dropped_not_fabricated(self, monkeypatch):
        scraper = TrendlyneScraper(username="u", password="p", client=MagicMock())

        def fake_fetch(investor_name):
            if investor_name == "Dolly Khanna":
                return [{"company_name": "Totally Unknown Co", "stake_pct": 1.0, "qoq_change_pct": 0.5}]
            return []

        monkeypatch.setattr(scraper, "fetch_investor_holdings", fake_fetch)
        monkeypatch.setattr("ingestion.scrapers.trendlyne.time.sleep", lambda s: None)
        with patch("ingestion.scrapers.trendlyne.load_universe_raw", return_value=_UNIVERSE_DF):
            result = scraper.export_superstar_holdings()

        assert result == {}

    def test_one_failed_investor_does_not_abort_export(self, monkeypatch):
        scraper = TrendlyneScraper(username="u", password="p", client=MagicMock())

        def fake_fetch(investor_name):
            if investor_name == "Dolly Khanna":
                raise ConnectionError("boom")
            if investor_name == "Vijay Kedia":
                return [{"company_name": "HDFC Bank Ltd", "stake_pct": 1.0, "qoq_change_pct": 0.1}]
            return []

        monkeypatch.setattr(scraper, "fetch_investor_holdings", fake_fetch)
        monkeypatch.setattr("ingestion.scrapers.trendlyne.time.sleep", lambda s: None)
        with patch("ingestion.scrapers.trendlyne.load_universe_raw", return_value=_UNIVERSE_DF):
            result = scraper.export_superstar_holdings()

        assert result == {"HDFCBANK": {"superstar_flag": True, "superstar_change": pytest.approx(0.1)}}


class TestBatchExport:
    def test_writes_through_client_with_pit_correct_dates(self, monkeypatch):
        scraper = TrendlyneScraper(username="u", password="p", client=MagicMock())
        monkeypatch.setattr(
            scraper, "export_superstar_holdings",
            lambda: {"HDFCBANK": {"superstar_flag": True, "superstar_change": 0.4}},
        )

        results = scraper.batch_export()

        assert results == {"HDFCBANK": True}
        scraper.client.write_shareholding.assert_called_once()
        written = scraper.client.write_shareholding.call_args[0][0]
        assert written["superstar_flag"] is True
        assert written["superstar_change"] == 0.4
        assert written["filing_date"] == written["quarter_end_date"] + timedelta(days=SHAREHOLDING_FILING_DELAY_DAYS)

    def test_write_false_skips_api_calls(self, monkeypatch):
        scraper = TrendlyneScraper(username="u", password="p", client=MagicMock())
        monkeypatch.setattr(
            scraper, "export_superstar_holdings",
            lambda: {"HDFCBANK": {"superstar_flag": True, "superstar_change": 0.4}},
        )

        results = scraper.batch_export(write=False)

        assert results == {"HDFCBANK": True}
        scraper.client.write_shareholding.assert_not_called()

    def test_one_bad_ticker_does_not_abort_the_batch(self, monkeypatch):
        scraper = TrendlyneScraper(username="u", password="p", client=MagicMock())
        monkeypatch.setattr(
            scraper, "export_superstar_holdings",
            lambda: {
                "GOODCO": {"superstar_flag": True, "superstar_change": 0.4},
                "BADCO": {"superstar_flag": True, "superstar_change": 0.1},
            },
        )
        scraper.client.write_shareholding.side_effect = lambda rec: (
            (_ for _ in ()).throw(ConnectionError("boom")) if rec["ticker"] == "BADCO" else {"written": True}
        )

        results = scraper.batch_export()

        assert results == {"GOODCO": True, "BADCO": False}


def test_superstar_investors_has_exactly_the_5_named_investors():
    """Build prompt deliverable: Dolly Khanna, Vijay Kedia, Ashish Kacholia,
    Sunil Singhania, Porinju Veliyath."""
    assert set(SUPERSTAR_INVESTORS) == {
        "Dolly Khanna", "Vijay Kedia", "Ashish Kacholia", "Sunil Singhania", "Porinju Veliyath",
    }
