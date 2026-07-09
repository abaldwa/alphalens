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
    _verify_page_matches_investor,
    discover_superstar_investors,
)

# [AS BUILT, 2026-07-05] Real Trendlyne markup (confirmed via a live
# authenticated fetch that day): table class is "superstar-shareholding",
# not a bare <table>; header <th> tags are UNCLOSED (the real page nests
# every header after the first inside it — see _flatten_header_cells);
# column headers are quarter-dependent ("Jun 2026  Holding %", "Jun 2026
# Change %"), not the fixed "Holding %"/"Change"/"Company" this module
# originally (wrongly) assumed. This fixture reproduces that real
# structure at a minimal scale, not the old wrong assumption.
_SAMPLE_HTML = """
<html><body>
<table class="superstar-shareholding">
<thead><tr>
<th>Stock</th>
<th>Holding Value<th>Qty Held</th><th>Jun 2026 Change %</th><th>Jun 2026  Holding %</th>
</tr></thead>
<tbody>
<tr><td>HDFC Bank Ltd</td><td>1,200 Cr</td><td>1,000,000</td><td>+0.3</td><td>2.5</td></tr>
<tr><td>Tata Motors Limited</td><td>800 Cr</td><td>500,000</td><td>-0.2</td><td>1.8</td></tr>
</tbody>
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

    def test_nan_float_returns_empty_string_not_crash(self):
        """Regression: a real run crashed with TypeError on stock_master's
        ~691 still-unresolved blank company_name rows (NaN, not None) —
        `bool(float("nan"))` is True so the old `if not name` guard let it
        through to re.sub, which requires a str. See FutureDevelopment.md #31."""
        assert _normalize_company_name(float("nan")) == ""


class TestParseHoldingsTable:
    def test_parses_company_quantity_stake_and_change(self):
        """Real markup (see _SAMPLE_HTML's comment): unclosed <th> tags,
        quarter-dependent header text, table selected by
        class="superstar-shareholding" rather than "the first table"."""
        rows = _parse_holdings_table(_SAMPLE_HTML)
        assert len(rows) == 2
        assert rows[0] == {
            "company_name": "HDFC Bank Ltd", "quantity": 1000000.0,
            "qoq_change_pct": 0.3, "stake_pct": 2.5,
        }
        assert rows[1] == {
            "company_name": "Tata Motors Limited", "quantity": 500000.0,
            "qoq_change_pct": -0.2, "stake_pct": 1.8,
        }

    def test_no_table_returns_empty_list(self):
        assert _parse_holdings_table("<html><body></body></html>") == []

    def test_wrong_table_class_returns_empty_list(self):
        """A page with only unrelated tables (e.g. the 34 other tables a
        real Trendlyne page has) must not be mistaken for the holdings table."""
        html = '<html><body><table class="some-other-table"><tr><td>x</td></tr></table></body></html>'
        assert _parse_holdings_table(html) == []

    def test_filing_awaited_and_dash_cells_become_none(self):
        html = """
        <html><body>
        <table class="superstar-shareholding">
        <thead><tr>
        <th>Stock</th>
        <th>Qty Held<th>Jun 2026 Change %</th><th>Jun 2026  Holding %</th>
        </tr></thead>
        <tbody>
        <tr><td>20 Microns</td><td>-</td><td>Filing Awaited</td><td>-</td></tr>
        </tbody>
        </table>
        </body></html>
        """
        rows = _parse_holdings_table(html)
        assert rows == [{"company_name": "20 Microns", "quantity": None, "qoq_change_pct": None, "stake_pct": None}]


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
            if investor_name == "Vijay Kishanlal Kedia":
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
            if investor_name == "Vijay Kishanlal Kedia":
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


class TestVerifyPageMatchesInvestor:
    """Phase D safety net for _UNVERIFIED_SLUG_INVESTORS' guessed slugs."""

    def test_matches_when_title_mentions_investor(self):
        html = "<html><head><title>Vijay Kedia Portfolio - Trendlyne</title></head><body></body></html>"
        assert _verify_page_matches_investor(html, "Vijay Kishanlal Kedia") is True

    def test_matches_ignoring_middle_name(self):
        html = "<html><head><title>Anil Goel Superstar Portfolio</title></head></html>"
        assert _verify_page_matches_investor(html, "Anil Kumar Goel and Associates") is True

    def test_rejects_wrong_investor_page(self):
        html = "<html><head><title>Dolly Khanna Portfolio - Trendlyne</title></head></html>"
        assert _verify_page_matches_investor(html, "Vijay Kishanlal Kedia") is False

    def test_rejects_empty_page(self):
        assert _verify_page_matches_investor("<html><body></body></html>", "Vijay Kishanlal Kedia") is False


class TestDiscoverSuperstarInvestors:
    """Phase D: real scrape of the public index page, replacing an earlier
    guessed-slug approach that was wrong once checked against real data."""

    _INDEX_HTML = """
    <html><body>
    <a href="/portfolio/superstar-shareholders/53757/latest/dolly-khanna-portfolio/">Dolly Khanna</a>
    <a href="/portfolio/superstar-shareholders/53805/latest/vijay-kishanlal-kedia-portfolio/">Vijay Kishanlal Kedia</a>
    <a href="/portfolio/superstar-shareholders/53743/latest/anil-kumar-goel-and-associates-portfolio/">Anil Kumar Goel and Associates</a>
    </body></html>
    """

    def test_parses_investor_name_and_path_from_real_link_pattern(self, monkeypatch):
        mock_session = MagicMock()
        mock_session.get.return_value = MagicMock(status_code=200, text=self._INDEX_HTML)

        result = discover_superstar_investors(session=mock_session)

        assert result == {
            "Dolly Khanna": "/portfolio/superstar-shareholders/53757/latest/dolly-khanna-portfolio/",
            "Vijay Kishanlal Kedia": "/portfolio/superstar-shareholders/53805/latest/vijay-kishanlal-kedia-portfolio/",
            "Anil Kumar Goel and Associates": "/portfolio/superstar-shareholders/53743/latest/anil-kumar-goel-and-associates-portfolio/",
        }

    def test_raises_connection_error_on_non_200(self):
        mock_session = MagicMock()
        mock_session.get.return_value = MagicMock(status_code=503, text="")
        with pytest.raises(ConnectionError, match="503"):
            discover_superstar_investors(session=mock_session)

    def test_never_produces_title_cased_and(self):
        """Regression: a live diff against discover_superstar_investors()'s
        real output (2026-07-05) found 10 SUPERSTAR_INVESTORS keys with
        "And" where the live-discovered casing is "and" (e.g. "Anil Kumar
        Goel And Associates" vs the real "...and Associates") — a
        hand-transcription mistake that would have silently produced
        duplicate/mismatched entries the next time someone merged a fresh
        discover_superstar_investors() call into this dict. Guards against
        it recurring for any future manually-added entry."""
        for name in SUPERSTAR_INVESTORS:
            assert " And " not in name, f"{name!r} should use lowercase 'and' (see discover_superstar_investors)"


def test_superstar_investors_includes_the_5_originally_confirmed_investors():
    """Build prompt deliverable: Dolly Khanna, Vijay Kedia, Ashish Kacholia,
    Sunil Singhania, Porinju Veliyath — these 5 must always remain present.
    Paths are the real ones scraped from Trendlyne's public index page
    2026-07-05 (Phase D expanded SUPERSTAR_INVESTORS from these 5 to all
    62 listed investors, using real scraped paths throughout — see
    discover_superstar_investors())."""
    confirmed_names = {
        "Dolly Khanna", "Vijay Kishanlal Kedia", "Ashish Kacholia",
        "Sunil Singhania", "Porinju V Veliyath",
    }
    for name in confirmed_names:
        assert name in SUPERSTAR_INVESTORS
        assert SUPERSTAR_INVESTORS[name].startswith("/portfolio/superstar-shareholders/")
    assert len(SUPERSTAR_INVESTORS) > len(confirmed_names)
