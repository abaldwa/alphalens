"""
ingestion/scrapers/screener.py

Phase: 2.1 (Fundamental Data Ingestion + PIT Validation)
Specs: SPEC-PIPE-001, SPEC-PIPE-003 (CRITICAL), SPEC-SEC-001
Owner: Platform / Ingestion
Consumers: features/fundamental.py, features/governance.py

ScreenerScraper: logs into screener.in Premium and exports quarterly P&L,
balance sheet, and shareholding pattern for the universe, writing both
through the DataStore API (datastore/api/routers/fundamentals.py,
datastore/api/routers/shareholding.py) — this build prompt's explicit
instruction ("Saves to fundamentals table in DuckDB via DataStore API
write endpoint"), unlike ingestion/backfill_runner.py's direct DuckDB
writes (that earlier P0.5 ambiguity was resolved the other way because
its prompt's wording was genuinely ambiguous — see that module's
docstring; this prompt's wording is explicit, so it is followed literally).

[AS BUILT] HTML page structure (section ids, row labels, header stats)
verified live against a real screener.in company page before writing this
parser (BuildLog.md "P2.1" records the exact verified structure):
  - #quarters: Sales, Expenses, Operating Profit, OPM %, Other Income,
    Interest, Depreciation, Profit before tax, Tax %, Net Profit, EPS in Rs
  - #balance-sheet: Equity Capital, Reserves, Borrowings, Other
    Liabilities, Total Liabilities, Fixed Assets, CWIP, Investments,
    Other Assets, Total Assets
  - #shareholding: Promoters, FIIs, DIIs, Government, Public,
    No. of Shareholders (quarterly sub-table)
  - Header stats: Market Cap, Current Price, Book Value, ROCE, ROE,
    Stock P/E, Face Value
This was verified via a read-only fetch of a public (non-premium) company
page — the *table structure* is identical for free and Premium accounts;
only the login flow and rate limits differ. The login POST itself
(field names `username`/`password`/`csrfmiddlewaretoken`, Django's
standard AuthenticationForm + CSRF convention — screener.in is a known
Django site) could NOT be verified the same way (WebFetch renders pages
to markdown, stripping raw <form> markup) — `login()` raises a clear
ScreenerAuthError with the raw response status on failure rather than
silently proceeding, so a field-name mismatch fails loud on the
operator's first real run with real credentials, the same
verify-against-the-real-thing pattern P0.5's FYERS OAuth flow needed
(see BuildLog.md "P0.5", "Post-handoff bug" entries).

[AS BUILT] Unit convention: every monetary `fundamentals` column
(revenue, ebitda, pat, fcf, total_debt, cash_and_equivalents,
gross_profit, capex, current_assets, current_liabilities) is in
**₹ Crore**, matching Screener.in's own reporting convention for these
figures verbatim. `book_value_per_share`/`eps`/header `current_price` are
per-share, in raw ₹ (also Screener's own convention). `shares_outstanding`
is a raw share count. Any equity/market-cap figure derived from
price x shares must be divided by 1e7 before comparing against a
fundamentals-table monetary column — a real bug caught in this module's
own debt_to_equity computation (see `equity_cr` below) and independently
in features/fundamental.py's market_cap (ev_to_ebitda) before either was
ever exercised against real data; both fixed in this same session, see
BuildLog.md "P2.1".

[AS BUILT] Screener's free-tier balance sheet table does NOT expose
current_assets, current_liabilities, cash_and_equivalents, gross_profit,
or capex as distinct labeled rows (it is a 10-row aggregate, not full
line-item detail) — these 5 raw fields (added to the `fundamentals`
schema this phase for features/fundamental.py's gross_margin,
capex_intensity, current_ratio, net_debt_to_ebitda, roic) are written as
None/NULL by this scraper. P2.6's Tijori Finance Pro integration
("operational metrics, segment data" per CLAUDE.md's data source table)
is the natural source for these — not fabricated here. The downstream
feature functions already treat them as NaN-tolerant.

[AS BUILT, deep-forensic 20-field gap fix, 2026-07-07] Two rows of that
same 10-row aggregate table WERE always present but never captured:
"Total Assets" and "CWIP" (Capital Work in Progress) — verified live
against TCS's real consolidated page. These are now parsed into
`total_assets`/`cwip` (see datastore/schema/create_normalised.py). The
remaining balance-sheet-quality fields this scraper still cannot supply —
goodwill, intangibles, contingent liabilities, subsidiary_count,
loans_to_related_parties — were specifically grepped for on the same live
page and genuinely do not appear anywhere in the free-tier HTML (no
labeled row, no embedded schedule data). Screener does expose a
"Related Party Transactions" modal (`/results/rpt/{id}/consolidated/`)
but even with a real authenticated login it (a) is labeled "Experimental
new feature" with disclosed extraction errors by Screener itself, (b) has
no fixed row schema — party names and transaction-type rows vary per
company — and (c) frequently leaves the most recent 1-2 fiscal years
blank pending annual report publication. Automated aggregation into a
single `related_party_transactions` figure was judged too unreliable to
ship without risking silently-wrong numbers; left undone rather than
built fragile. See features/deep_forensic.py's module docstring for the
full list of governance fields (audit qualification, auditor change, CFO
tenure, board independence, director resignations, whistleblower policy)
that have no realistic free structured source at all (annual-report/
XBRL-only data, not present on Screener, Tijori, or NSE's scrapeable
corporate-actions endpoints).

PIT Assumptions
----------------
SPEC-PIPE-003 (CRITICAL): announcement_date is never directly visible in
this page's scraped content (verified — no "Result" timestamp in the
quarters table). Conservative default: quarter_end_date +
config.settings.FUNDAMENTALS_ANNOUNCEMENT_DELAY_DAYS (45 days, the NSE
regulatory deadline) — this can only ever be LATER than the true
announcement date, never earlier, so it can never introduce look-ahead
bias (it can only make data look available slightly later than it
really was, the safe direction). Same logic for shareholding's
filing_date (quarter_end_date + SHAREHOLDING_FILING_DELAY_DAYS, 21 days).
"""

import logging
import re
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from config.settings import (
    DEFAULT_RETRY_COUNT,
    FUNDAMENTALS_ANNOUNCEMENT_DELAY_DAYS,
    SCREENER_PASSWORD,
    SCREENER_RATE_LIMIT_SLEEP_SECONDS,
    SCREENER_RAW_DIR,
    SCREENER_USERNAME,
    SHAREHOLDING_FILING_DELAY_DAYS,
)
from datastore.client import DataStoreClient

logger = logging.getLogger(__name__)

BASE_URL = "https://www.screener.in"
LOGIN_URL = f"{BASE_URL}/login/"
_HEADERS = {"User-Agent": "Mozilla/5.0 (AlphaLens research scraper; contact via account owner)"}

# #quarters row label -> internal field name
_QUARTERS_FIELDS = {
    "Sales": "revenue",
    "Operating Profit": "operating_profit",
    "Depreciation": "depreciation",
    "Interest": "interest",
    "Net Profit": "pat",
    "EPS in Rs": "eps",
    # [AS BUILT] Banks/NBFCs/HFCs use a different P&L vocabulary on
    # screener.in (verified live against AXISBANK's real page) — "Revenue"
    # instead of "Sales", "Financing Profit"/"Financing Margin %" instead
    # of "Operating Profit"/"OPM %". "Net Profit"/"EPS in Rs"/"Interest"
    # are unchanged. Caught after the first full 502-ticker run: 51/90
    # tickers with no fundamentals row turned out to be exactly this
    # vocabulary gap (every major bank — HDFCBANK, ICICIBANK, AXISBANK,
    # SBI-equivalents — plus NBFCs/HFCs), not a parsing failure. The
    # generic interest_coverage formula (operating_profit/interest) is
    # less meaningful for banks (interest IS their core cost of funds,
    # not a debt-servicing-risk signal the way it is for industrials) —
    # documented limitation, not fixed here (a bank-specific ratio model
    # is out of scope for this single mapping fix).
    "Revenue": "revenue",
    "Financing Profit": "operating_profit",
}
# #balance-sheet row label -> internal field name (only the reliably-present ones — see module docstring)
# [AS BUILT] Some pages (verified against real cached pages, e.g. banks/
# NBFCs) label this row "Borrowing" (no trailing "s", no "+" expander)
# instead of "Borrowings+" — both map to the same field.
_BALANCE_SHEET_FIELDS = {
    "Borrowings": "total_debt",
    "Borrowing": "total_debt",
    # [AS BUILT, deep-forensic 20-field gap fix] Total Assets and CWIP are
    # real distinct rows in the free-tier #balance-sheet table (verified
    # live against TCS's real consolidated page 2026-07-07 — see
    # datastore/schema/create_normalised.py's total_assets/cwip column
    # comment for the exact verified values). Feeds features/deep_forensic.py's
    # cwip_ratio and asset_inflation_flag, which were previously always NaN
    # because these columns didn't exist in the schema at all.
    "Total Assets": "total_assets",
    "CWIP": "cwip",
}
# #balance-sheet row label -> internal field name, for the FULL multi-year
# history parse (_parse_balance_sheet_history) used to derive total_equity
# per fiscal year — see that function's docstring.
_BALANCE_SHEET_HISTORY_FIELDS = {
    "Equity Capital": "equity_capital",
    "Reserves": "reserves",
}
# #shareholding row label -> internal field name
_SHAREHOLDING_FIELDS = {
    "Promoters": "promoter_pct",
    "FIIs": "fii_pct",
    "DIIs": "dii_pct",
    "Public": "retail_pct",
}
# Header stat label -> internal field name
_HEADER_FIELDS = {
    "Market Cap": "market_cap_cr",
    "Current Price": "current_price",
    "Book Value": "book_value_per_share",
    "ROCE": "roce",
    "ROE": "roe",
}


class ScreenerAuthError(RuntimeError):
    """Raised when login() cannot establish an authenticated session."""


class ScreenerScraper:
    """
    Logs into screener.in Premium and exports quarterly fundamentals +
    shareholding pattern, written via the DataStore API.

    Parameters
    ----------
    username : str, optional
        Defaults to config.settings.SCREENER_USERNAME (.env).
    password : str, optional
        Defaults to config.settings.SCREENER_PASSWORD (.env).
    raw_dir : Path, optional
        Where raw HTML is saved (SPEC-PIPE-001 raw retention).
        Defaults to config.settings.SCREENER_RAW_DIR.
    client : DataStoreClient, optional
        Injectable for testability (SPEC-SOLID-005).

    Spec References
    ----------------
    SPEC-PIPE-001, SPEC-PIPE-003 (CRITICAL), SPEC-SEC-001.

    Raises
    ------
    None on construction — credentials are only used (and validated) on
    the first login() call.
    """

    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        raw_dir: Path = SCREENER_RAW_DIR,
        client: Optional[DataStoreClient] = None,
    ) -> None:
        self.username = username or SCREENER_USERNAME
        self.password = password or SCREENER_PASSWORD
        self.raw_dir = raw_dir
        self.client = client or DataStoreClient()
        self._session: Optional[requests.Session] = None

    def login(self) -> requests.Session:
        """
        Authenticate against screener.in and cache the session.

        Returns
        -------
        requests.Session
            Authenticated session, reused by subsequent calls.

        Raises
        ------
        ScreenerAuthError
            If SCREENER_USERNAME/SCREENER_PASSWORD are not set, the login
            page's CSRF token can't be found, or the login POST does not
            land on an authenticated page (response still shows a login
            form, or a non-2xx status).
        """
        if not self.username or not self.password:
            raise ScreenerAuthError(
                "SCREENER_USERNAME/SCREENER_PASSWORD not set — add real screener.in "
                "Premium credentials to .env before calling login()."
            )

        session = requests.Session()
        session.headers.update(_HEADERS)

        login_page = _retry(lambda: session.get(LOGIN_URL, timeout=30))
        csrf_match = re.search(r'name=["\']csrfmiddlewaretoken["\']\s+value=["\']([^"\']+)["\']', login_page.text)
        if not csrf_match:
            raise ScreenerAuthError(
                "Could not find csrfmiddlewaretoken on the login page — screener.in's "
                "login form markup may have changed; inspect a live page before retrying."
            )
        csrf_token = csrf_match.group(1)

        response = _retry(
            lambda: session.post(
                LOGIN_URL,
                data={"csrfmiddlewaretoken": csrf_token, "username": self.username, "password": self.password},
                headers={"Referer": LOGIN_URL},
                timeout=30,
            )
        )
        if response.status_code >= 400 or "id_password" in response.text:
            raise ScreenerAuthError(
                f"screener.in login failed (status={response.status_code}). "
                "If field names changed (username/password/csrfmiddlewaretoken), "
                "update login()'s POST payload — see module docstring."
            )

        self._session = session
        logger.info("screener.in login successful")
        return session

    def _fetch_company_page(self, ticker: str) -> str:
        """Fetch and raw-save a company's consolidated financials page (SPEC-PIPE-001)."""
        if self._session is None:
            self.login()

        url = f"{BASE_URL}/company/{ticker}/consolidated/"
        response = _retry(lambda: self._session.get(url, timeout=30))
        if response.status_code != 200:
            raise ConnectionError(f"screener.in fetch failed for {ticker}: HTTP {response.status_code}")

        self.raw_dir.mkdir(parents=True, exist_ok=True)
        (self.raw_dir / f"{ticker}.html").write_text(response.text, encoding="utf-8")
        return response.text

    def export_company_data(self, ticker: str) -> Dict[str, Dict[str, Any]]:
        """
        Export one ticker's quarterly fundamentals + shareholding pattern.

        Parameters
        ----------
        ticker : str

        Returns
        -------
        dict
            {'fundamentals': {...} or None, 'shareholding': {...} or None}
            — matches datastore.api.schemas.FundamentalsWrite /
            ShareholdingWrite field shapes. None for a section if its
            table wasn't found on the page (e.g. a newly-listed company
            with no shareholding history yet).

        Spec References
        ----------------
        SPEC-PIPE-003 (CRITICAL): announcement_date/filing_date are
        conservative quarter_end_date + regulatory-deadline defaults
        (module docstring) — never the quarter_end_date itself.

        Raises
        ------
        ScreenerAuthError
            If not yet logged in and login() fails.
        ConnectionError
            If the company page can't be fetched after retries.
        """
        html = self._fetch_company_page(ticker)
        soup = BeautifulSoup(html, "html.parser")

        header = _parse_section_table(soup, section_id=None, field_map=_HEADER_FIELDS, header_stats=True)
        quarters = _parse_section_table(soup, "quarters", _QUARTERS_FIELDS)
        balance_sheet = _parse_section_table(soup, "balance-sheet", _BALANCE_SHEET_FIELDS)
        shareholding = _parse_section_table(soup, "shareholding", _SHAREHOLDING_FIELDS)

        fundamentals_row = _build_fundamentals_row(ticker, quarters, balance_sheet, header)
        shareholding_row = _build_shareholding_row(ticker, shareholding)
        return {"fundamentals": fundamentals_row, "shareholding": shareholding_row}

    def export_equity_history(self, ticker: str, html: Optional[str] = None) -> Dict[int, Dict[str, float]]:
        """
        fiscal_year -> {"total_equity": float, "retained_earnings": float}
        (both INR Cr) for every year Screener's #balance-sheet table shows
        for this ticker.

        Parameters
        ----------
        ticker : str
        html : str, optional
            Pre-fetched page HTML (e.g. from a previously cached
            `SCREENER_RAW_DIR/{ticker}.html`) — when given, NO network
            call or login is made, letting callers replay history from
            already-downloaded pages. When omitted, fetches a fresh page
            (requires login()), same as export_company_data().

        Returns
        -------
        dict
            See `_parse_balance_sheet_history`. Empty if the section
            wasn't found or no year had both Equity Capital and Reserves.
        """
        if html is None:
            html = self._fetch_company_page(ticker)
        soup = BeautifulSoup(html, "html.parser")
        return _parse_balance_sheet_history(soup)

    def batch_export(self, tickers: List[str], write: bool = True) -> Dict[str, bool]:
        """
        Export and write fundamentals + shareholding for many tickers, rate-limited.

        Parameters
        ----------
        tickers : list of str
        write : bool
            If True (default), upserts via DataStoreClient.write_fundamentals/
            write_shareholding. If False, exports only (used by tests).

        Returns
        -------
        dict
            ticker -> True if both writes succeeded (or write=False and
            export succeeded), False if export or any write failed. One
            bad ticker never aborts the batch — same per-ticker isolation
            as ingestion/scrapers/fyers_backfill.py's batch_download.

        Spec References
        ----------------
        SPEC-PIPE-001: rate-limited batch export.
        SPEC-PIPE-003 (CRITICAL): writes go through the DataStore API only.
        """
        results: Dict[str, bool] = {}
        for ticker in tickers:
            try:
                data = self.export_company_data(ticker)
                ok = True
                if write:
                    if data["fundamentals"] is not None:
                        self.client.write_fundamentals(data["fundamentals"])
                    if data["shareholding"] is not None:
                        self.client.write_shareholding(data["shareholding"])
                results[ticker] = ok
            except Exception as exc:
                logger.warning(f"screener export failed for {ticker}: {exc}")
                results[ticker] = False
            time.sleep(SCREENER_RATE_LIMIT_SLEEP_SECONDS)
        return results


def _retry(fn, retries: int = DEFAULT_RETRY_COUNT):
    """Retry a zero-arg callable up to `retries` times, same pattern as ingestion/scrapers/bhavcopy.py."""
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except requests.RequestException as exc:
            last_exc = exc
            logger.warning(f"screener.in request failed (attempt {attempt}/{retries}): {exc}")
    raise ConnectionError(f"screener.in request failed after {retries} attempts: {last_exc}")


def _parse_number(text: str) -> Optional[float]:
    """Parse a Screener-formatted number cell ('1,234', '12.3%', '₹ 1,326') to float, or None."""
    if text is None:
        return None
    cleaned = re.sub(r"[₹,%\s]", "", text.strip())
    if cleaned in ("", "-"):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_section_table(
    soup: BeautifulSoup, section_id: Optional[str], field_map: Dict[str, str], header_stats: bool = False
) -> Dict[str, float]:
    """
    Find the most recent (rightmost) column's value for each labeled row
    inside a `<section id="...">`, or the page header's labeled ratio
    stats if header_stats=True.

    Returns
    -------
    dict
        internal_field_name -> latest value (None if the row/section was not found).
    """
    result: Dict[str, float] = {field: None for field in field_map.values()}

    if header_stats:
        # Header stats render as label/value pairs (commonly <li> "name"/"value" spans)
        # outside any single <section> — search the whole page.
        text = soup.get_text("\n")
        for label, field in field_map.items():
            match = re.search(rf"{re.escape(label)}\s*\n?\s*([₹\d,.\s%]+)", text)
            if match:
                result[field] = _parse_number(match.group(1))
        return result

    section = soup.find(id=section_id)
    if section is None:
        logger.warning(f"screener.in section #{section_id} not found on page")
        return result

    table = section.find("table")
    if table is None:
        return result

    for row in table.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        # [AS BUILT] Real screener.in markup wraps many (not all) row labels
        # in a "show schedule breakdown" <button> with a trailing "+" icon
        # (e.g. "Sales+", "Borrowings+", "Promoters+") — verified live
        # against a real saved page (datastore/raw/screener/*.html) after
        # an exact-match login-test run returned every field as None
        # despite a successful login; see BuildLog.md "P2.1" follow-up.
        # Strip a trailing "+" (and any whitespace before it) so the label
        # lookup matches regardless of whether the row happens to be
        # schedule-expandable.
        label = cells[0].get_text(strip=True).rstrip("+").strip()
        if label not in field_map:
            continue
        # Last cell is the most recent period (Screener tables are chronological left-to-right)
        result[field_map[label]] = _parse_number(cells[-1].get_text(strip=True))

    return result


def _parse_balance_sheet_history(soup: BeautifulSoup) -> Dict[int, Dict[str, float]]:
    """
    Parse EVERY column of the #balance-sheet table (Screener renders one
    column per fiscal year, e.g. 'Mar 2015'..'Mar 2026' on one page) into
    fiscal_year -> {"total_equity": Equity Capital + Reserves, "retained_earnings":
    Reserves alone}, both in INR Cr.

    Unlike `_parse_section_table` (used for the live current-quarter export,
    which only reads the rightmost/most-recent column), this reads every
    column so a single page fetch yields up to ~11 years of equity history
    — Screener's balance sheet is annual-only, there is no quarterly
    breakdown, so each fiscal year gets exactly one value pair, to be
    patched onto every quarter row of that FY (same pattern as Trendlyne's
    ROE_A/DEBT_CE_A annual fields).

    [AS BUILT, deep-forensic altman_z fix 2026-07-07] "Reserves" is kept as
    its own value (not just summed into total_equity) because it is the
    standard accounting analog of Altman Z's "retained earnings" term
    (accumulated profits not distributed as paid-up equity capital) — a
    real, separately-labeled row on the same page, not a fabricated split.

    Returns
    -------
    dict
        fiscal_year (int, e.g. 2023 for the 'Mar 2023' column) ->
        {"total_equity": float, "retained_earnings": float}, both INR Cr.
        Empty dict if the section/table isn't found, or a given year is
        skipped if either Equity Capital or Reserves is missing/unparseable
        for that column (no partial-equity guessing).
    """
    section = soup.find(id="balance-sheet")
    if section is None:
        return {}
    table = section.find("table")
    if table is None:
        return {}

    rows = table.find_all("tr")
    if not rows:
        return {}

    header_cells = rows[0].find_all(["td", "th"])
    fiscal_years: List[Optional[int]] = []
    for cell in header_cells[1:]:
        match = re.search(r"(\d{4})", cell.get_text(strip=True))
        fiscal_years.append(int(match.group(1)) if match else None)

    per_field: Dict[str, List[Optional[float]]] = {}
    for row in rows[1:]:
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        label = cells[0].get_text(strip=True).rstrip("+").strip()
        if label not in _BALANCE_SHEET_HISTORY_FIELDS:
            continue
        field = _BALANCE_SHEET_HISTORY_FIELDS[label]
        per_field[field] = [_parse_number(c.get_text(strip=True)) for c in cells[1:]]

    equity_capital = per_field.get("equity_capital", [])
    reserves = per_field.get("reserves", [])

    result: Dict[int, Dict[str, float]] = {}
    for i, fy in enumerate(fiscal_years):
        if fy is None:
            continue
        ec = equity_capital[i] if i < len(equity_capital) else None
        rs = reserves[i] if i < len(reserves) else None
        if ec is None or rs is None:
            continue
        result[fy] = {"total_equity": ec + rs, "retained_earnings": rs}
    return result


def _current_quarter_end(today: Optional[date] = None) -> date:
    """Most recently completed Indian fiscal quarter-end on or before `today`."""
    today = today or date.today()
    quarter_ends = [date(today.year, 3, 31), date(today.year, 6, 30), date(today.year, 9, 30), date(today.year, 12, 31)]
    past = [d for d in quarter_ends if d <= today]
    if past:
        return max(past)
    return date(today.year - 1, 12, 31)


# Indian FY: Apr-Jun=Q1, Jul-Sep=Q2, Oct-Dec=Q3, Jan-Mar=Q4. FY label = the
# calendar year in which March falls (FY-end) — same convention documented
# and used by scripts/backfill_fundamentals_trendlyne.py's
# _parse_quarter_label, and the convention already live in the `fundamentals`
# table's real rows (verified: IIFL's 2021-09-30 row is fiscal_year=2022,
# quarter=2). The previous `year if month != 3 else year - 1` / calendar-
# quarter-number formula here disagreed with that — a real bug, caught while
# adding the equity-history backfill (it produced a wrong-keyed row for
# IIFL's most recent quarter: (fiscal_year=2025, quarter=1, 2026-03-31)
# instead of the correct (2026, 4)). Fixed here so Screener's own writes
# land on the same (ticker, fiscal_year, quarter) key Trendlyne would use
# for the same quarter, instead of silently creating a duplicate row.
_FY_QUARTER_MAP = {3: 4, 6: 1, 9: 2, 12: 3}


def _indian_fiscal_year_quarter(quarter_end: date) -> "tuple[int, int]":
    """('Mar 2026'-style quarter_end) -> (fiscal_year=2026, quarter=4)."""
    quarter = _FY_QUARTER_MAP[quarter_end.month]
    fiscal_year = quarter_end.year if quarter_end.month == 3 else quarter_end.year + 1
    return fiscal_year, quarter


def _build_fundamentals_row(
    ticker: str, quarters: Dict[str, float], balance_sheet: Dict[str, float], header: Dict[str, float]
) -> Optional[Dict[str, Any]]:
    """Assemble one FundamentalsWrite-shaped dict from parsed page sections, or None if no quarterly data found."""
    if quarters.get("revenue") is None:
        return None

    quarter_end = _current_quarter_end()
    announcement_date = quarter_end + timedelta(days=FUNDAMENTALS_ANNOUNCEMENT_DELAY_DAYS)
    fiscal_year, quarter = _indian_fiscal_year_quarter(quarter_end)

    revenue = quarters.get("revenue")
    operating_profit = quarters.get("operating_profit")
    depreciation = quarters.get("depreciation")
    interest = quarters.get("interest")
    pat = quarters.get("pat")

    ebitda = (operating_profit + depreciation) if operating_profit is not None and depreciation is not None else None
    operating_margin = (operating_profit / revenue) if operating_profit is not None and revenue else None
    ebitda_margin = (ebitda / revenue) if ebitda is not None and revenue else None
    net_margin = (pat / revenue) if pat is not None and revenue else None
    interest_coverage = (operating_profit / interest) if operating_profit is not None and interest else None

    current_price = header.get("current_price")
    book_value_per_share = header.get("book_value_per_share")
    market_cap_cr = header.get("market_cap_cr")
    # shares_outstanding is a raw share COUNT (not crore) — market_cap_cr * 1e7
    # converts crore to raw rupees before dividing by the raw per-share price.
    shares_outstanding = (
        int((market_cap_cr * 1e7) / current_price) if market_cap_cr and current_price else None
    )
    # book_value_per_share is in raw rupees-per-share; multiplying by a raw
    # share count gives raw-rupee equity, which must be converted back to
    # crore (/ 1e7) to be comparable to total_debt (already in crore, same
    # unit Screener reports every balance-sheet/quarterly figure in).
    equity_cr = (
        (book_value_per_share * shares_outstanding) / 1e7
        if book_value_per_share and shares_outstanding else None
    )

    return {
        "ticker": ticker,
        "fiscal_year": fiscal_year,
        "quarter": quarter,
        "quarter_end_date": quarter_end,
        "announcement_date": announcement_date,
        "revenue": revenue,
        "ebitda": ebitda,
        "pat": pat,
        "eps": quarters.get("eps"),
        "operating_margin": operating_margin,
        "ebitda_margin": ebitda_margin,
        "net_margin": net_margin,
        "roe": header.get("roe"),
        "roce": header.get("roce"),
        "debt_to_equity": (
            balance_sheet.get("total_debt") / equity_cr
            if balance_sheet.get("total_debt") is not None and equity_cr else None
        ),
        "interest_coverage": interest_coverage,
        "fcf": None,  # not reliably parseable from the free-tier cash-flow table — see module docstring
        "asset_turnover": None,
        "inventory_days": None,
        "receivable_days": None,
        "payable_days": None,
        "book_value_per_share": book_value_per_share,
        "shares_outstanding": shares_outstanding,
        "gross_profit": None,
        "capex": None,
        "current_assets": None,
        "current_liabilities": None,
        "total_debt": balance_sheet.get("total_debt"),
        "cash_and_equivalents": None,
        "depreciation": depreciation,
        "total_assets": balance_sheet.get("total_assets"),
        "cwip": balance_sheet.get("cwip"),
    }


def _build_shareholding_row(ticker: str, shareholding: Dict[str, float]) -> Optional[Dict[str, Any]]:
    """Assemble one ShareholdingWrite-shaped dict from the parsed #shareholding section, or None if absent."""
    if shareholding.get("promoter_pct") is None:
        return None

    quarter_end = _current_quarter_end()
    filing_date = quarter_end + timedelta(days=SHAREHOLDING_FILING_DELAY_DAYS)

    return {
        "ticker": ticker,
        "quarter_end_date": quarter_end,
        "filing_date": filing_date,
        "promoter_pct": shareholding.get("promoter_pct"),
        # [AS BUILT] Screener's free-tier #shareholding table has no
        # "Pledged %" row when pledge is 0% / not disclosed (verified live —
        # see module docstring); left None here rather than fabricated as
        # 0, since "not shown" and "confirmed zero" are not the same claim.
        # [AS BUILT, deep-forensic cluster A follow-up 2026-07-07] NSE does
        # publish a public "Corporate Filings > Pledged Data" page
        # (nseindia.com/companies-listing/corporate-filings-pledged-data,
        # SEBI SAST Reg 31(4) disclosure) but it is a client-rendered SPA
        # with no discoverable public JSON API: live-tested against
        # nseindia.com/api/CorpInfo (the endpoint backing NSE's adjacent
        # SAST Reg 29(2) acquisition/sale disclosures, confirmed working —
        # returns real 2020 sale-disclosure rows for VERTOZ under
        # corpType=sast) with every plausible corpType guess for pledge
        # itself (pledge, encumbrance, reg31, sast_regulation_31,
        # pledgedata, corp_pledge, ...) against VERTOZ, a company with a
        # real, currently-disclosed 51.82% promoter-holding encumbrance as
        # of 2026-03-31 (per FY26 scanx.trade disclosure coverage) — every
        # guess returned HTTP 200 with an empty `{"data":[],"msg":"no data
        # found"}` body, indicating the API silently no-ops on an
        # unrecognized corpType rather than 404ing, i.e. none of these
        # guesses is the real backing endpoint. The page itself ships no
        # inline API path in its server-rendered HTML (fully client-side
        # data fetch via a hashed JS bundle not resolvable without
        # executing it in a real browser). Trendlyne.py (this repo's other
        # authenticated source) has no pledge/encumbrance field anywhere in
        # its scraped pages either. Genuinely blocked on the free tier
        # without a headless-browser NSE session — not fabricated.
        "promoter_pledge": None,
        "fii_pct": shareholding.get("fii_pct"),
        "dii_pct": shareholding.get("dii_pct"),
        # [AS BUILT, deep-forensic cluster A2 follow-up 2026-07-07]
        # Verified live against all 3,309 cached real Screener raw HTML
        # pages in datastore/raw/screener/: zero have a distinct "Mutual
        # Funds" row inside the #shareholding table (a text search for
        # "Mutual Fund" inside that div matched 0/3309 — the only false
        # positives found by an earlier looser whole-page search were
        # incidental mentions in business-description prose, e.g. CDSL's
        # "units of mutual funds"). Screener's free-tier shareholding
        # breakup is only {Promoters, FIIs, DIIs, Public}; MF is not
        # broken out from DIIs/Public on this tier. Confirms the existing
        # conclusion below rather than changing it.
        "mf_pct": None,  # not a distinct row in #shareholding (Public aggregates non-institutional + MF)
        "retail_pct": shareholding.get("retail_pct"),
    }


def _cli() -> None:
    """
    CLI entry point: `python3 -m ingestion.scrapers.screener export TICKER`
    or `python3 -m ingestion.scrapers.screener batch --tickers A,B,C` /
    `--universe` (config.universe.get_tickers()). Same
    `python3 -m ...` invocation convention as every other operator-run
    script in this project (backfill_runner.py, fyers_backfill.py) —
    running this file directly (`python3 screener.py`) breaks every
    package-relative import.
    """
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="screener.in fundamentals + shareholding export")
    subparsers = parser.add_subparsers(dest="command", required=True)

    export_parser = subparsers.add_parser("export", help="Export one ticker, print only (no write)")
    export_parser.add_argument("ticker")

    batch_parser = subparsers.add_parser("batch", help="Export + write many tickers, rate-limited")
    batch_group = batch_parser.add_mutually_exclusive_group(required=True)
    batch_group.add_argument("--tickers", help="Comma-separated ticker list")
    batch_group.add_argument("--universe", action="store_true", help="Use config.universe.get_tickers()")
    batch_parser.add_argument("--no-write", action="store_true", help="Export only, skip the API writes")

    args = parser.parse_args()
    scraper = ScreenerScraper()

    if args.command == "export":
        import json

        result = scraper.export_company_data(args.ticker)
        print(json.dumps(result, default=str, indent=2), flush=True)
        return

    if args.universe:
        from config.universe import get_tickers

        tickers = get_tickers()
    else:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

    print(f"Exporting {len(tickers)} tickers (write={not args.no_write})...", flush=True)
    results = scraper.batch_export(tickers, write=not args.no_write)
    n_ok = sum(1 for ok in results.values() if ok)
    print(f"Done: {n_ok}/{len(tickers)} succeeded.", flush=True)
    failed = [t for t, ok in results.items() if not ok]
    if failed:
        print(f"Failed: {failed}", flush=True)


if __name__ == "__main__":
    _cli()
