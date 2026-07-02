"""
tests/unit/test_screener.py

Phase: 2.1 (Fundamental Data Ingestion + PIT Validation)
Specs: SPEC-PIPE-001, SPEC-PIPE-003 (CRITICAL), SPEC-SEC-001
Owner: Platform / QA
Consumers: CI, pytest

Tests ingestion/scrapers/screener.py's HTML parsing, PIT-default logic,
and unit-conversion correctness (₹ Crore vs raw rupees — see
screener.py's module docstring for the real bug this test suite guards
against) entirely offline — no real network call to screener.in is made
or mocked-and-silently-skipped; every test exercises real parsing code
against a synthetic fixture matching the real, live-verified page
structure (BuildLog.md "P2.1" records the live structure audit).
"""

from datetime import date, timedelta
from unittest.mock import MagicMock

import pytest
from bs4 import BeautifulSoup

from config.settings import FUNDAMENTALS_ANNOUNCEMENT_DELAY_DAYS, SHAREHOLDING_FILING_DELAY_DAYS
from ingestion.scrapers.screener import (
    ScreenerAuthError,
    ScreenerScraper,
    _build_fundamentals_row,
    _build_shareholding_row,
    _current_quarter_end,
    _indian_fiscal_year_quarter,
    _parse_balance_sheet_history,
    _parse_number,
    _parse_section_table,
)

# Synthetic fixture matching the real, live-verified screener.in page
# structure (#quarters, #balance-sheet, #shareholding, header ratio stats).
_SAMPLE_HTML = """
<html><body>
<div class="company-ratios">
<ul>
<li><span class="name">Market Cap</span><span class="number">17,95,296</span></li>
<li><span class="name">Current Price</span><span class="number">1,326</span></li>
<li><span class="name">Book Value</span><span class="number">668</span></li>
<li><span class="name">ROCE</span><span class="number">10.3%</span></li>
<li><span class="name">ROE</span><span class="number">8.91%</span></li>
</ul>
</div>
<section id="quarters">
<table><tbody>
<tr><td class="text">Sales</td><td>200000</td><td>210000</td></tr>
<tr><td class="text">Operating Profit</td><td>30000</td><td>32000</td></tr>
<tr><td class="text">Depreciation</td><td>5000</td><td>5200</td></tr>
<tr><td class="text">Interest</td><td>2000</td><td>2100</td></tr>
<tr><td class="text">Net Profit</td><td>18000</td><td>19000</td></tr>
<tr><td class="text">EPS in Rs</td><td>13.3</td><td>14.0</td></tr>
</tbody></table>
</section>
<section id="balance-sheet">
<table><tbody>
<tr><td class="text">Borrowings</td><td>120000</td><td>125000</td></tr>
</tbody></table>
</section>
<section id="shareholding">
<table><tbody>
<tr><td class="text">Promoters</td><td>50.3</td><td>50.3</td></tr>
<tr><td class="text">FIIs</td><td>23.1</td><td>23.5</td></tr>
<tr><td class="text">DIIs</td><td>16.2</td><td>16.0</td></tr>
<tr><td class="text">Public</td><td>10.4</td><td>10.2</td></tr>
</tbody></table>
</section>
</body></html>
"""


class TestParseNumber:
    def test_strips_currency_commas_and_percent(self):
        assert _parse_number("1,234.5") == 1234.5
        assert _parse_number("12.3%") == 12.3
        assert _parse_number("₹ 1,326") == 1326.0

    def test_dash_and_none_return_none(self):
        assert _parse_number("-") is None
        assert _parse_number(None) is None


class TestParseSectionTable:
    def test_quarters_section_picks_most_recent_column(self):
        from ingestion.scrapers.screener import _QUARTERS_FIELDS

        soup = BeautifulSoup(_SAMPLE_HTML, "html.parser")
        quarters = _parse_section_table(soup, "quarters", _QUARTERS_FIELDS)
        assert quarters["revenue"] == 210000.0  # rightmost column, not the first
        assert quarters["pat"] == 19000.0
        assert quarters["eps"] == 14.0

    def test_missing_section_returns_all_none_not_an_exception(self):
        soup = BeautifulSoup("<html><body></body></html>", "html.parser")
        result = _parse_section_table(soup, "quarters", {"Sales": "revenue"})
        assert result == {"revenue": None}


class TestBuildFundamentalsRow:
    def test_unit_conversion_debt_to_equity_is_sane(self):
        """
        Regression test for the real unit-mismatch bug caught while
        building this module (see screener.py's module docstring):
        total_debt is in Crore, book_value_per_share x shares_outstanding
        is raw rupees — debt_to_equity must divide by 1e7 to compare them
        in the same unit. A buggy version of this code produced
        debt_to_equity ~= 1.4e-8 instead of a sane ~0.1-0.2 ratio.
        """
        soup = BeautifulSoup(_SAMPLE_HTML, "html.parser")
        from ingestion.scrapers.screener import _BALANCE_SHEET_FIELDS, _HEADER_FIELDS, _QUARTERS_FIELDS

        header = _parse_section_table(soup, None, _HEADER_FIELDS, header_stats=True)
        quarters = _parse_section_table(soup, "quarters", _QUARTERS_FIELDS)
        balance_sheet = _parse_section_table(soup, "balance-sheet", _BALANCE_SHEET_FIELDS)

        row = _build_fundamentals_row("RELIANCE", quarters, balance_sheet, header)

        assert row is not None
        assert 0.01 < row["debt_to_equity"] < 1.0, f"debt_to_equity not sane: {row['debt_to_equity']}"
        assert row["shares_outstanding"] > 0

    def test_pit_default_announcement_date_is_after_quarter_end(self):
        soup = BeautifulSoup(_SAMPLE_HTML, "html.parser")
        from ingestion.scrapers.screener import _BALANCE_SHEET_FIELDS, _HEADER_FIELDS, _QUARTERS_FIELDS

        header = _parse_section_table(soup, None, _HEADER_FIELDS, header_stats=True)
        quarters = _parse_section_table(soup, "quarters", _QUARTERS_FIELDS)
        balance_sheet = _parse_section_table(soup, "balance-sheet", _BALANCE_SHEET_FIELDS)
        row = _build_fundamentals_row("RELIANCE", quarters, balance_sheet, header)

        assert row["announcement_date"] > row["quarter_end_date"]
        expected = row["quarter_end_date"] + timedelta(days=FUNDAMENTALS_ANNOUNCEMENT_DELAY_DAYS)
        assert row["announcement_date"] == expected

    def test_no_revenue_row_returns_none(self):
        row = _build_fundamentals_row("EMPTYCO", {"revenue": None}, {}, {})
        assert row is None

    def test_ebitda_adds_back_depreciation_to_operating_profit(self):
        soup = BeautifulSoup(_SAMPLE_HTML, "html.parser")
        from ingestion.scrapers.screener import _QUARTERS_FIELDS

        quarters = _parse_section_table(soup, "quarters", _QUARTERS_FIELDS)
        row = _build_fundamentals_row("RELIANCE", quarters, {}, {})
        assert row["ebitda"] == 32000.0 + 5200.0


class TestBuildShareholdingRow:
    def test_pit_default_filing_date_is_after_quarter_end(self):
        soup = BeautifulSoup(_SAMPLE_HTML, "html.parser")
        from ingestion.scrapers.screener import _SHAREHOLDING_FIELDS

        shareholding = _parse_section_table(soup, "shareholding", _SHAREHOLDING_FIELDS)
        row = _build_shareholding_row("RELIANCE", shareholding)

        assert row is not None
        assert row["filing_date"] > row["quarter_end_date"]
        assert row["filing_date"] == row["quarter_end_date"] + timedelta(days=SHAREHOLDING_FILING_DELAY_DAYS)
        assert row["promoter_pct"] == 50.3

    def test_no_promoter_row_returns_none(self):
        assert _build_shareholding_row("EMPTYCO", {"promoter_pct": None}) is None


# Real screener.in #balance-sheet markup (verified against a live cached
# page, datastore/raw/screener/IIFL.html): one column per fiscal year,
# header label "Mar YYYY", "Borrowing" without a trailing "s"/"+" on some
# pages (banks/NBFCs), "+" expander suffix on Equity-adjacent rows elsewhere.
_SAMPLE_HTML_WITH_HISTORY = """
<html><body>
<section id="balance-sheet">
<table><tbody>
<tr><th></th><th>Mar 2023</th><th>Mar 2024</th><th>Mar 2025</th></tr>
<tr><td class="text">Equity Capital</td><td>76</td><td>76</td><td>85</td></tr>
<tr><td class="text">Reserves</td><td>8,916</td><td>10,561</td><td>12,327</td></tr>
<tr><td class="text">Borrowing</td><td>50,000</td><td>52,000</td><td>54,000</td></tr>
<tr><td class="text">Total Liabilities</td><td>90,000</td><td>95,000</td><td>100,000</td></tr>
</tbody></table>
</section>
</body></html>
"""


class TestParseBalanceSheetHistory:
    def test_extracts_equity_per_fiscal_year_across_all_columns(self):
        soup = BeautifulSoup(_SAMPLE_HTML_WITH_HISTORY, "html.parser")
        history = _parse_balance_sheet_history(soup)
        assert history == {2023: 8992.0, 2024: 10637.0, 2025: 12412.0}

    def test_missing_section_returns_empty_dict(self):
        soup = BeautifulSoup("<html><body></body></html>", "html.parser")
        assert _parse_balance_sheet_history(soup) == {}

    def test_year_with_missing_reserves_is_skipped_not_guessed(self):
        html = """
        <section id="balance-sheet"><table><tbody>
        <tr><th></th><th>Mar 2023</th><th>Mar 2024</th></tr>
        <tr><td class="text">Equity Capital</td><td>76</td><td>76</td></tr>
        <tr><td class="text">Reserves</td><td>8,916</td><td>-</td></tr>
        </tbody></table></section>
        """
        soup = BeautifulSoup(html, "html.parser")
        history = _parse_balance_sheet_history(soup)
        assert history == {2023: 8992.0}  # 2024 dropped, not fabricated as 76

    def test_borrowing_singular_label_matches_total_debt_field(self):
        soup = BeautifulSoup(_SAMPLE_HTML_WITH_HISTORY, "html.parser")
        from ingestion.scrapers.screener import _BALANCE_SHEET_FIELDS

        balance_sheet = _parse_section_table(soup, "balance-sheet", _BALANCE_SHEET_FIELDS)
        assert balance_sheet["total_debt"] == 54000.0  # rightmost column


class TestExportEquityHistory:
    def test_offline_with_pre_fetched_html_makes_no_network_call(self):
        scraper = ScreenerScraper(username="u", password="p", client=MagicMock())
        history = scraper.export_equity_history("IIFL", html=_SAMPLE_HTML_WITH_HISTORY)
        assert history == {2023: 8992.0, 2024: 10637.0, 2025: 12412.0}


class TestIndianFiscalYearQuarter:
    def test_march_quarter_end_is_q4_of_its_own_year(self):
        # Regression test: the real fundamentals table's convention
        # (verified against IIFL's live rows) is fiscal_year = the
        # calendar year March falls in, quarter=4 — a prior version of
        # this code computed (year - 1, calendar-quarter-1) instead,
        # producing a wrong-keyed row.
        assert _indian_fiscal_year_quarter(date(2026, 3, 31)) == (2026, 4)

    def test_june_quarter_end_is_q1_of_next_fiscal_year(self):
        assert _indian_fiscal_year_quarter(date(2021, 6, 30)) == (2022, 1)

    def test_september_and_december(self):
        assert _indian_fiscal_year_quarter(date(2021, 9, 30)) == (2022, 2)
        assert _indian_fiscal_year_quarter(date(2021, 12, 31)) == (2022, 3)


class TestCurrentQuarterEnd:
    def test_returns_most_recent_completed_quarter(self):
        assert _current_quarter_end(date(2025, 5, 20)) == date(2025, 3, 31)
        assert _current_quarter_end(date(2025, 3, 31)) == date(2025, 3, 31)
        assert _current_quarter_end(date(2025, 1, 5)) == date(2024, 12, 31)


class TestLoginAndAuthErrors:
    def test_missing_credentials_raises_auth_error_without_any_network_call(self, monkeypatch):
        """
        .env ships SCREENER_USERNAME/PASSWORD as non-empty placeholder
        strings (e.g. "your_screener_in_username_here") — `username=None`
        alone would fall through to those placeholders via the
        `username or SCREENER_USERNAME` default and actually attempt a
        real login() POST against screener.in (caught running this test
        for the first time: it returned a real HTTP 200 from the live
        site). Explicitly blank out the module-level constants too so
        this test can never make a live network call, regardless of
        what's in .env — same "no accidental live call" requirement as
        ingestion/scrapers/bhavcopy.py's and fyers_backfill.py's tests.
        """
        monkeypatch.setattr("ingestion.scrapers.screener.SCREENER_USERNAME", None)
        monkeypatch.setattr("ingestion.scrapers.screener.SCREENER_PASSWORD", None)
        scraper = ScreenerScraper(username=None, password=None)
        with pytest.raises(ScreenerAuthError, match="not set"):
            scraper.login()


class TestBatchExport:
    def test_one_bad_ticker_does_not_abort_the_batch(self, monkeypatch):
        """Same per-ticker isolation as ingestion/scrapers/fyers_backfill.py's batch_download."""
        scraper = ScreenerScraper(username="u", password="p", client=MagicMock())
        monkeypatch.setattr(
            scraper, "export_company_data",
            lambda t: (_ for _ in ()).throw(ConnectionError("boom")) if t == "BADCO" else
            {"fundamentals": {"ticker": t}, "shareholding": {"ticker": t}},
        )
        monkeypatch.setattr("ingestion.scrapers.screener.time.sleep", lambda s: None)

        results = scraper.batch_export(["GOODCO", "BADCO", "GOODCO2"])

        assert results == {"GOODCO": True, "BADCO": False, "GOODCO2": True}
        assert scraper.client.write_fundamentals.call_count == 2
        assert scraper.client.write_shareholding.call_count == 2

    def test_write_false_skips_api_calls(self, monkeypatch):
        scraper = ScreenerScraper(username="u", password="p", client=MagicMock())
        monkeypatch.setattr(
            scraper, "export_company_data",
            lambda t: {"fundamentals": {"ticker": t}, "shareholding": {"ticker": t}},
        )
        monkeypatch.setattr("ingestion.scrapers.screener.time.sleep", lambda s: None)

        results = scraper.batch_export(["GOODCO"], write=False)

        assert results == {"GOODCO": True}
        scraper.client.write_fundamentals.assert_not_called()
        scraper.client.write_shareholding.assert_not_called()
