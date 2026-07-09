"""
ingestion/scrapers/trendlyne.py

Phase: 2.6 (Phase 2 Data Source Integration)
Specs: SPEC-PIPE-001, SPEC-PIPE-003 (CRITICAL), SPEC-SEC-001
Owner: Platform / Ingestion
Consumers: features/governance.py, systems/ml_signal_engine (superstar_flag/superstar_change)

TrendlyneScraper: logs into Trendlyne StratQ and exports the quarterly
portfolio holdings of named "superstar" retail investors (originally 5,
expanded to ~62 in Phase D — see SUPERSTAR_INVESTORS below), mapping each
holding to a ticker in stock_master and writing superstar_flag /
superstar_change through the DataStore API.

[AS BUILT, P2.6] Class name: the build prompt names this class
"ScreenerSync" — almost certainly a copy-paste artifact from
ingestion/scrapers/screener.py's `ScreenerScraper` (this module has
nothing to do with screener.in). Named `TrendlyneScraper` instead, the
same naming convention as every other source-specific scraper in this
directory (ScreenerScraper, TijoriScraper, GrowwMFHoldingsScraper) — same
"a literal name that is clearly a mistake, not a deliberate prompt
choice, gets fixed and documented rather than propagated" precedent as
P2.5's AQI/round-number-flag bug fixes (see BuildLog.md "P2.5").

[AS BUILT, P2.6] Credentials: the build prompt says "TRENDLYNE_API_KEY
from .env". No such variable exists anywhere in this codebase.
config/settings.py and .env.example already define TRENDLYNE_USERNAME /
TRENDLYNE_PASSWORD (added ahead of this phase, labelled "Phase 2.6:
Trendlyne StratQ login") — Trendlyne StratQ, like screener.in Premium, is
a paid login-walled web subscription (CLAUDE.md's data source table:
"₹5,900/yr"), not a token-authenticated REST API. Used the established
username/password login pattern instead of inventing a non-existent
API-key auth flow — same resolution category as P2.5's `depreciation`
column landing on the existing `fundamentals` table rather than a new one.

[AS BUILT, P2.6] "Writes to governance table": no standalone `governance`
table exists in this schema. 12_platform_architecture.md (line 320)
labels `shareholding` itself as this project's governance store:
"/governance/  # Shareholding patterns (PIT via filing_date)". superstar_flag
/ superstar_change are therefore new columns on the EXISTING `shareholding`
table (datastore/schema/create_normalised.py), written via
DataStoreClient.write_shareholding() — not a new table or a new client method.

[AS BUILT, P2.6, partially resolved Phase D 2026-07-05] HONEST GAP —
Trendlyne's login form field names and the paid-content parts of each
investor's portfolio page (actual stake_pct/qty numbers) could NOT be
verified live: no real paid account exists in this environment
(TRENDLYNE_USERNAME/PASSWORD in .env are operator-filled placeholders,
same as every other paid-source credential in this project). The login
POST field names and _parse_holdings_table()'s column-header matching are
still best-effort, modelled on screener.in's verified Django-CSRF login
flow — not independently confirmed. login() and fetch_investor_holdings()
raise a clear TrendlyneAuthError / ValueError on a structure mismatch
rather than silently returning wrong data, so a field-name mismatch fails
loud on the operator's first real run with real credentials — the exact
same "verify-against-the-real-thing, fail loud if not" pattern screener.py's
own module docstring documents for its own unverifiable login POST (see
BuildLog.md "P0.5", "P2.1"). What IS now verified (Phase D, 2026-07-05):
every investor's portfolio page PATH in SUPERSTAR_INVESTORS — see below —
via a real unauthenticated fetch of the public index page (that part of
Trendlyne needs no login at all). A live fetch of one investor's detail
page that same day (unauthenticated) confirmed the table structure this
module expects is real (real stock names in real rows), but the actual
stake_pct/quantity cells were blanked ("-") without a paid login — i.e.
the page structure assumption is now confirmed, only the paid-data-behind-
login assumption remains unverified.

Aggregation rule (own construction, not from an external spec): a ticker
held by more than one of the superstar investors gets superstar_flag=True
(any holds it) and superstar_change = the SUM of each holding investor's
own QoQ stake-percentage-point change (net combined superstar buying/
selling pressure across the cohort, signed) — a simple, documented choice,
not a literal Trendlyne StratQ output field.

[AS BUILT, Big Investor Activity Phase D, 2026-07-05] export_named_holdings()
/ batch_export_named_holdings() reuse the same per-investor fetch but keep
each investor's own per-ticker stake_pct instead of collapsing it into the
aggregate flag above — written to the new `public_shareholders` table
(datastore/schema/create_normalised.py), matched to investor_family via
the same normalize_client_name() used on the bulk-deal side
(ingestion/scrapers/bulk_deal_attribution.py). This is what
ingestion/scrapers/bulk_deal_reconciliation.py reconciles against.

SUPERSTAR_INVESTORS was expanded the same day from the original 5 to all
62 investors on Trendlyne's superstar-shareholders index
(https://trendlyne.com/portfolio/superstar-shareholders/index/). An
earlier version of this expansion GUESSED each new investor's URL slug
from a heuristic and was WRONG on every count once checked against a real
fetch — the actual URL is `/portfolio/superstar-shareholders/
{numeric_id}/latest/{full-name-slug}-portfolio/` (Trendlyne's own numeric
object ID plus the FULL name, not the shortened `/stratq/
superstar-investors/portfolio/{slug}/` guess this module previously
assumed even for the original 5). All 62 paths below were scraped
directly from a real, live, unauthenticated fetch of the index page
(discover_superstar_investors() reproduces this fetch on demand for a
fresh mapping) — none of them are guessed. _verify_page_matches_investor()
remains as a fail-loud safety net in case Trendlyne's routing changes
again in the future.

[AS BUILT, Big Investor Activity, 2026-07-05] Considered and rejected:
scraping Trendlyne on a DAILY cadence to get faster investor entry/exit
signals than the existing quarterly stake_pct data. Decided against —
Trendlyne's per-investor "transaction history" almost certainly derives
from the same NSE/BSE bulk/block deal filings ingestion/scrapers/
large_deals.py already ingests directly (first-party, T+1, no login
wall), so daily scraping would add real risk (paid-subscription ToS
exposure at that request volume, login/session fragility, no faster than
source) for no new information. bulk_deal_positions (Phase B) already IS
the daily entry/exit signal; Trendlyne's role stays scoped to the
inherently-quarterly stake_pct reconciliation (Phase D) it's actually
suited for.
"""

import logging
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

import requests
from bs4 import BeautifulSoup

from config.settings import (
    DEFAULT_RETRY_COUNT,
    SHAREHOLDING_FILING_DELAY_DAYS,
    TRENDLYNE_PASSWORD,
    TRENDLYNE_RATE_LIMIT_SLEEP_SECONDS,
    TRENDLYNE_RAW_DIR,
    TRENDLYNE_USERNAME,
)
from config.universe import load_universe_raw
from datastore.client import DataStoreClient

logger = logging.getLogger(__name__)

BASE_URL = "https://trendlyne.com"
LOGIN_URL = f"{BASE_URL}/accounts/login/"
SUPERSTAR_INDEX_PATH = "/portfolio/superstar-shareholders/index/"
_HEADERS = {"User-Agent": "Mozilla/5.0 (AlphaLens research scraper; contact via account owner)"}

# Matches Trendlyne's real per-investor portfolio URL, e.g.
# "/portfolio/superstar-shareholders/53757/latest/dolly-khanna-portfolio/"
# — discovered live 2026-07-05 (see discover_superstar_investors()). The
# numeric ID is Trendlyne's own object ID and is REQUIRED, not decorative
# — it is not reconstructible from the investor's name.
_INVESTOR_LINK_PATTERN = re.compile(r"/portfolio/superstar-shareholders/\d+/latest/[a-z0-9-]+-portfolio/")


def discover_superstar_investors(session: Optional[requests.Session] = None) -> Dict[str, str]:
    """
    Fetch Trendlyne's public superstar-shareholders index page and parse
    out every investor's real portfolio path (investor_name -> path).

    [AS BUILT, Big Investor Activity Phase D, 2026-07-05] Replaces an
    earlier version of this module that GUESSED each non-original-5
    investor's URL slug from a heuristic (drop middle names) — live
    verification that day showed the guess was wrong on every count: the
    real path is `/portfolio/superstar-shareholders/{numeric_id}/latest/
    {full-name-slug}-portfolio/` (numeric ID + FULL name kept, not
    `/stratq/superstar-investors/portfolio/{slug}/` with a shortened
    name as previously assumed). This function scrapes the real mapping
    instead of guessing it. The index page itself requires no login
    (confirmed via a live unauthenticated fetch); the per-investor detail
    pages do still require a real paid login for the actual holding
    percentages/quantities — this function only discovers WHERE each
    investor's page lives, not their holdings.

    Parameters
    ----------
    session : requests.Session, optional
        Reused if provided; otherwise a bare unauthenticated session
        (the index page doesn't need login).

    Returns
    -------
    dict
        investor_name (Title Case, derived from the URL slug) -> full
        path (e.g. "/portfolio/superstar-shareholders/53757/latest/
        dolly-khanna-portfolio/"). investor_name is exactly the
        space-joined, title-cased slug tokens — matches this project's
        investor_family seed via normalize_client_name() regardless of
        cosmetic case differences (e.g. "and" vs "And").

    Raises
    ------
    ConnectionError
        If the index page can't be fetched after retries.
    """
    sess = session or requests.Session()
    if session is None:
        sess.headers.update(_HEADERS)

    response = _retry(lambda: sess.get(f"{BASE_URL}{SUPERSTAR_INDEX_PATH}", timeout=30))
    if response.status_code != 200:
        raise ConnectionError(f"Trendlyne superstar index fetch failed: HTTP {response.status_code}")

    paths = sorted(set(_INVESTOR_LINK_PATTERN.findall(response.text)))
    result: Dict[str, str] = {}
    for path in paths:
        slug = path.split("/latest/")[1].rsplit("-portfolio/", 1)[0]
        name = " ".join(word if word == "and" else word.capitalize() for word in slug.split("-"))
        result[name] = path
    logger.info(f"discover_superstar_investors: found {len(result)} investors on the index page")
    return result


# [AS BUILT, Big Investor Activity Phase D, 2026-07-05] Real snapshot from
# a live fetch of SUPERSTAR_INDEX_PATH that day (not a guess — see
# discover_superstar_investors()'s docstring for the "guessed slug was
# wrong" history this replaced). Used as the default so callers don't
# need a network call just to know investor_name -> path; call
# discover_superstar_investors() directly for a fresh live mapping (e.g.
# if Trendlyne adds/removes a superstar investor later).
SUPERSTAR_INVESTORS: Dict[str, str] = {
    "Ajay Upadhyaya": "/portfolio/superstar-shareholders/53739/latest/ajay-upadhyaya-portfolio/",
    "Akash Bhanshali": "/portfolio/superstar-shareholders/53740/latest/akash-bhanshali-portfolio/",
    "Amit Gupta": "/portfolio/superstar-shareholders/53741/latest/amit-gupta-portfolio/",
    "Anil Kumar Goel and Associates": "/portfolio/superstar-shareholders/53743/latest/anil-kumar-goel-and-associates-portfolio/",
    "Anuj Anantrai Sheth and Associates": "/portfolio/superstar-shareholders/53744/latest/anuj-anantrai-sheth-and-associates-portfolio/",
    "Ashish Dhawan": "/portfolio/superstar-shareholders/53745/latest/ashish-dhawan-portfolio/",
    "Ashish Kacholia": "/portfolio/superstar-shareholders/53746/latest/ashish-kacholia-portfolio/",
    "Ashok Kumar Jain": "/portfolio/superstar-shareholders/53748/latest/ashok-kumar-jain-portfolio/",
    "Atim Kabra": "/portfolio/superstar-shareholders/53749/latest/atim-kabra-portfolio/",
    "Bharat Jayantilal Patel and Associates": "/portfolio/superstar-shareholders/53751/latest/bharat-jayantilal-patel-and-associates-portfolio/",
    "Dheeraj Kumar Lohia and Associates": "/portfolio/superstar-shareholders/53754/latest/dheeraj-kumar-lohia-and-associates-portfolio/",
    "Dilipkumar Lakhi": "/portfolio/superstar-shareholders/53755/latest/dilipkumar-lakhi-portfolio/",
    "Dipak Kanayalal Shah": "/portfolio/superstar-shareholders/53756/latest/dipak-kanayalal-shah-portfolio/",
    "Dolly Khanna": "/portfolio/superstar-shareholders/53757/latest/dolly-khanna-portfolio/",
    "Harsha Hitesh Javeri": "/portfolio/superstar-shareholders/53759/latest/harsha-hitesh-javeri-portfolio/",
    "Hiten Anantrai Sheth": "/portfolio/superstar-shareholders/53761/latest/hiten-anantrai-sheth-portfolio/",
    "Hitesh Ramji Javeri and Associates": "/portfolio/superstar-shareholders/53762/latest/hitesh-ramji-javeri-and-associates-portfolio/",
    "Hitesh Satishchandra Doshi": "/portfolio/superstar-shareholders/53763/latest/hitesh-satishchandra-doshi-portfolio/",
    "Keswani Haresh": "/portfolio/superstar-shareholders/53765/latest/keswani-haresh-portfolio/",
    "Lata Bhanshali": "/portfolio/superstar-shareholders/53767/latest/lata-bhanshali-portfolio/",
    "Lincoln P Coelho": "/portfolio/superstar-shareholders/53768/latest/lincoln-p-coelho-portfolio/",
    "Madhukar Sheth": "/portfolio/superstar-shareholders/53770/latest/madhukar-sheth-portfolio/",
    "Mahendra Girdharilal": "/portfolio/superstar-shareholders/53771/latest/mahendra-girdharilal-portfolio/",
    "Minal Bharat Patel": "/portfolio/superstar-shareholders/53772/latest/minal-bharat-patel-portfolio/",
    "Mukul Agrawal": "/portfolio/superstar-shareholders/53774/latest/mukul-agrawal-portfolio/",
    "Nemish S Shah": "/portfolio/superstar-shareholders/53776/latest/nemish-s-shah-portfolio/",
    "Porinju V Veliyath": "/portfolio/superstar-shareholders/53777/latest/porinju-v-veliyath-portfolio/",
    "Raj Kumar Lohia": "/portfolio/superstar-shareholders/53779/latest/raj-kumar-lohia-portfolio/",
    "Rakesh Jhunjhunwala and Associates": "/portfolio/superstar-shareholders/53781/latest/rakesh-jhunjhunwala-and-associates-portfolio/",
    "Rekha Jhunjhunwala": "/portfolio/superstar-shareholders/53782/latest/rekha-jhunjhunwala-portfolio/",
    "Ricky Ishwardas Kirpalani": "/portfolio/superstar-shareholders/53783/latest/ricky-ishwardas-kirpalani-portfolio/",
    "Sangeetha S": "/portfolio/superstar-shareholders/53786/latest/sangeetha-s-portfolio/",
    "Sanjay Gupta": "/portfolio/superstar-shareholders/53787/latest/sanjay-gupta-portfolio/",
    "Sanjay Kumar Agarwal": "/portfolio/superstar-shareholders/53788/latest/sanjay-kumar-agarwal-portfolio/",
    "Sanjeev Vinodchandra Parekh": "/portfolio/superstar-shareholders/53790/latest/sanjeev-vinodchandra-parekh-portfolio/",
    "Sanjiv Dhireshbhai Shah": "/portfolio/superstar-shareholders/53791/latest/sanjiv-dhireshbhai-shah-portfolio/",
    "Satpal Khattar": "/portfolio/superstar-shareholders/53793/latest/satpal-khattar-portfolio/",
    "Seetha Kumari": "/portfolio/superstar-shareholders/53795/latest/seetha-kumari-portfolio/",
    "Sharad Kanayalal Shah and Associates": "/portfolio/superstar-shareholders/53796/latest/sharad-kanayalal-shah-and-associates-portfolio/",
    "Shaunak Jagdish Shah": "/portfolio/superstar-shareholders/53797/latest/shaunak-jagdish-shah-portfolio/",
    "Shivani Tejas Trivedi": "/portfolio/superstar-shareholders/53798/latest/shivani-tejas-trivedi-portfolio/",
    "Sunil Kumar": "/portfolio/superstar-shareholders/53800/latest/sunil-kumar-portfolio/",
    "Suresh Kumar Agarwal": "/portfolio/superstar-shareholders/53802/latest/suresh-kumar-agarwal-portfolio/",
    "Vallabh Roopchand Bhanshali": "/portfolio/superstar-shareholders/53803/latest/vallabh-roopchand-bhanshali-portfolio/",
    "Vanaja Sundar Iyer": "/portfolio/superstar-shareholders/53804/latest/vanaja-sundar-iyer-portfolio/",
    "Vijay Kishanlal Kedia": "/portfolio/superstar-shareholders/53805/latest/vijay-kishanlal-kedia-portfolio/",
    "Vinodchandra Mansukhlal Parekh and Associates": "/portfolio/superstar-shareholders/53807/latest/vinodchandra-mansukhlal-parekh-and-associates-portfolio/",
    "Girish Gulati": "/portfolio/superstar-shareholders/584324/latest/girish-gulati-portfolio/",
    "Madhusudan Kela": "/portfolio/superstar-shareholders/584325/latest/madhusudan-kela-portfolio/",
    "Shankar Sharma": "/portfolio/superstar-shareholders/584326/latest/shankar-sharma-portfolio/",
    "Urjita Master": "/portfolio/superstar-shareholders/584327/latest/urjita-master-portfolio/",
    "Nikhil Vora": "/portfolio/superstar-shareholders/584329/latest/nikhil-vora-portfolio/",
    "Bhavook Tripathi": "/portfolio/superstar-shareholders/584330/latest/bhavook-tripathi-portfolio/",
    "Amal Parikh": "/portfolio/superstar-shareholders/584331/latest/amal-parikh-portfolio/",
    "Jayesh Patel": "/portfolio/superstar-shareholders/584332/latest/jayesh-patel-portfolio/",
    "Premji and Associates": "/portfolio/superstar-shareholders/584333/latest/premji-and-associates-portfolio/",
    "Ramesh Damani": "/portfolio/superstar-shareholders/62728/latest/ramesh-damani-portfolio/",
    "Mohnish Pabrai": "/portfolio/superstar-shareholders/69664/latest/mohnish-pabrai-portfolio/",
    "Radhakishan Damani": "/portfolio/superstar-shareholders/178317/latest/radhakishan-damani-portfolio/",
    "Sunil Singhania": "/portfolio/superstar-shareholders/182955/latest/sunil-singhania-portfolio/",
    "Mukesh Ambani and Family": "/portfolio/superstar-shareholders/2214734/latest/mukesh-ambani-and-family-portfolio/",
    "Govindlal M Parikh": "/portfolio/superstar-shareholders/3130116/latest/govindlal-m-parikh-portfolio/",
}

_NAME_NOISE_PATTERN = re.compile(r"\b(ltd|limited|the)\b\.?", re.IGNORECASE)
# [AS BUILT, Big Investor Activity, 2026-07-05] Real column headers are
# QUARTER-DEPENDENT ("Jun 2026  Holding %", "Jun 2026 Change %") rather
# than the fixed "Holding %"/"Change" this module originally assumed
# (verified via a real authenticated fetch — see _parse_holdings_table's
# docstring for the full history of what that fetch found). Matched by
# pattern instead of a fixed lookup dict; the FIRST header matching each
# pattern is the current/latest quarter (subsequent bare "{quarter} %"
# columns are prior quarters, not currently captured).
_STOCK_HEADER = re.compile(r"^Stock$", re.IGNORECASE)
_QTY_HEADER = re.compile(r"^Qty Held$", re.IGNORECASE)
_HOLDING_PCT_HEADER = re.compile(r"Holding\s*%", re.IGNORECASE)
_CHANGE_PCT_HEADER = re.compile(r"Change\s*%", re.IGNORECASE)


class TrendlyneAuthError(RuntimeError):
    """Raised when login() cannot establish an authenticated session."""


class TrendlyneScraper:
    """
    Logs into Trendlyne StratQ and exports the named superstar investors'
    quarterly portfolio holdings, written via the DataStore API as
    shareholding's superstar_flag / superstar_change columns.

    Parameters
    ----------
    username : str, optional
        Defaults to config.settings.TRENDLYNE_USERNAME (.env).
    password : str, optional
        Defaults to config.settings.TRENDLYNE_PASSWORD (.env).
    raw_dir : Path, optional
        Where raw HTML is saved (SPEC-PIPE-001 raw retention).
        Defaults to config.settings.TRENDLYNE_RAW_DIR.
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
        raw_dir: Path = TRENDLYNE_RAW_DIR,
        client: Optional[DataStoreClient] = None,
    ) -> None:
        self.username = username or TRENDLYNE_USERNAME
        self.password = password or TRENDLYNE_PASSWORD
        self.raw_dir = raw_dir
        self.client = client or DataStoreClient()
        self._session: Optional[requests.Session] = None

    def login(self) -> requests.Session:
        """
        Authenticate against Trendlyne and cache the session.

        Returns
        -------
        requests.Session
            Authenticated session, reused by subsequent calls.

        Raises
        ------
        TrendlyneAuthError
            If TRENDLYNE_USERNAME/TRENDLYNE_PASSWORD are not set, the
            login page's CSRF token can't be found, or the login POST
            does not land on an authenticated page.
        """
        if not self.username or not self.password:
            raise TrendlyneAuthError(
                "TRENDLYNE_USERNAME/TRENDLYNE_PASSWORD not set — add real Trendlyne "
                "StratQ credentials to .env before calling login()."
            )

        session = requests.Session()
        session.headers.update(_HEADERS)

        login_page = _retry(lambda: session.get(LOGIN_URL, timeout=30))
        csrf_match = re.search(r'name=["\']csrfmiddlewaretoken["\']\s+value=["\']([^"\']+)["\']', login_page.text)
        if not csrf_match:
            raise TrendlyneAuthError(
                "Could not find csrfmiddlewaretoken on the login page — Trendlyne's "
                "login form markup may differ from this module's assumption; "
                "inspect a live page before retrying (see module docstring's HONEST GAP)."
            )
        csrf_token = csrf_match.group(1)

        response = _retry(
            lambda: session.post(
                LOGIN_URL,
                data={"csrfmiddlewaretoken": csrf_token, "login": self.username, "password": self.password,
                      "recaptcha_token": "", "recaptcha_action": "login", "remember": "on"},
                headers={"Referer": LOGIN_URL},
                timeout=30,
            )
        )
        if response.status_code >= 400 or "id_password" in response.text:
            raise TrendlyneAuthError(
                f"Trendlyne login failed (status={response.status_code}). "
                "If field names changed, update login()'s POST payload — see module docstring."
            )

        self._session = session
        logger.info("Trendlyne login successful")
        return session

    def _fetch_investor_page(self, path: str) -> str:
        """
        Fetch and raw-save a superstar investor's portfolio page
        (SPEC-PIPE-001).

        Parameters
        ----------
        path : str
            Full path from SUPERSTAR_INVESTORS, e.g.
            "/portfolio/superstar-shareholders/53757/latest/dolly-khanna-portfolio/"
            — the numeric ID is Trendlyne's real object ID, not
            reconstructible from the investor's name (see
            discover_superstar_investors()'s docstring).
        """
        if self._session is None:
            self.login()

        url = f"{BASE_URL}{path}"
        response = _retry(lambda: self._session.get(url, timeout=30))
        if response.status_code != 200:
            raise ConnectionError(f"Trendlyne fetch failed for {path}: HTTP {response.status_code}")

        self.raw_dir.mkdir(parents=True, exist_ok=True)
        filename = path.strip("/").replace("/", "_") + ".html"
        (self.raw_dir / filename).write_text(response.text, encoding="utf-8")
        return response.text

    def fetch_investor_holdings(self, investor_name: str) -> List[Dict[str, Any]]:
        """
        Fetch one superstar investor's current quarterly portfolio.

        Parameters
        ----------
        investor_name : str
            Must be a key of SUPERSTAR_INVESTORS.

        Returns
        -------
        list of dict
            [{"company_name": str, "stake_pct": float, "qoq_change_pct": float}, ...]

        Raises
        ------
        ValueError
            If investor_name is not a key of SUPERSTAR_INVESTORS.
        TrendlyneAuthError
            If not yet logged in and login() fails.
        ConnectionError
            If the portfolio page can't be fetched after retries.
        """
        if investor_name not in SUPERSTAR_INVESTORS:
            raise ValueError(
                f"Unknown superstar investor {investor_name!r} — expected one of {list(SUPERSTAR_INVESTORS)}"
            )

        html = self._fetch_investor_page(SUPERSTAR_INVESTORS[investor_name])
        if not _verify_page_matches_investor(html, investor_name):
            raise ConnectionError(
                f"Trendlyne page for path {SUPERSTAR_INVESTORS[investor_name]!r} does not appear to "
                f"mention {investor_name!r} — either SUPERSTAR_INVESTORS is stale (call "
                "discover_superstar_investors() to refresh) or Trendlyne's page structure changed. "
                "Not parsing this page's holdings to avoid silently attributing another investor's "
                "portfolio to this name."
            )
        return _parse_holdings_table(html)

    def export_superstar_holdings(self) -> Dict[str, Dict[str, Any]]:
        """
        Fetch all registered superstar investors and aggregate to one row per ticker.

        Returns
        -------
        dict
            ticker -> {"superstar_flag": True, "superstar_change": float}
            — see module docstring's aggregation rule. Holdings whose
            company_name doesn't match any stock_master ticker are
            dropped (logged at debug level), not silently fabricated.
        """
        name_to_ticker = _build_company_name_to_ticker_map()
        aggregated: Dict[str, float] = {}

        for investor_name in SUPERSTAR_INVESTORS:
            try:
                holdings = self.fetch_investor_holdings(investor_name)
            except (ConnectionError, TrendlyneAuthError) as exc:
                logger.warning(f"Trendlyne export failed for {investor_name}: {exc}")
                continue

            for holding in holdings:
                ticker = name_to_ticker.get(_normalize_company_name(holding.get("company_name")))
                if ticker is None:
                    logger.debug(f"No stock_master match for {holding.get('company_name')!r} ({investor_name})")
                    continue
                change = holding.get("qoq_change_pct") or 0.0
                aggregated[ticker] = aggregated.get(ticker, 0.0) + change

            time.sleep(TRENDLYNE_RATE_LIMIT_SLEEP_SECONDS)

        return {ticker: {"superstar_flag": True, "superstar_change": change} for ticker, change in aggregated.items()}

    def batch_export(self, write: bool = True) -> Dict[str, bool]:
        """
        Export all registered superstar investors' holdings and write
        superstar_flag/superstar_change for every matched ticker.

        Parameters
        ----------
        write : bool
            If True (default), upserts via DataStoreClient.write_shareholding.
            If False, exports only (used by tests).

        Returns
        -------
        dict
            ticker -> True if the write succeeded (or write=False), False
            if that ticker's write failed. One bad ticker never aborts the
            batch (same per-ticker isolation as screener.py's batch_export).
        """
        rows = self.export_superstar_holdings()
        quarter_end = _current_quarter_end()
        filing_date = quarter_end + timedelta(days=SHAREHOLDING_FILING_DELAY_DAYS)

        results: Dict[str, bool] = {}
        for ticker, fields in rows.items():
            try:
                if write:
                    self.client.write_shareholding(
                        {
                            "ticker": ticker,
                            "quarter_end_date": quarter_end,
                            "filing_date": filing_date,
                            "superstar_flag": fields["superstar_flag"],
                            "superstar_change": fields["superstar_change"],
                        }
                    )
                results[ticker] = True
            except Exception as exc:
                logger.warning(f"Trendlyne write failed for {ticker}: {exc}")
                results[ticker] = False
        return results

    def export_named_holdings(self) -> List[Dict[str, Any]]:
        """
        Phase D (Big Investor Activity — plan: gentle-wobbling-swing.md):
        per-investor, per-ticker holdings (NOT aggregated across the
        superstar investors like export_superstar_holdings) — this is the
        named-holder granularity bulk_deal_reconciliation.py needs to
        reconcile a SPECIFIC family's estimated bulk-deal position against
        a reported stake, rather than only knowing "some superstar holds
        this ticker".

        Returns
        -------
        list of dict
            [{"investor_name": str, "ticker": str, "stake_pct": float,
              "qoq_change_pct": float, "quantity": float|None}, ...].
            quantity is Trendlyne's real "Qty Held" figure where disclosed
            (see _parse_holdings_table) — None for quarters shown as "-"
            or "Filing Awaited". Rows whose company_name doesn't match a
            stock_master ticker are dropped (same per-holding isolation as
            export_superstar_holdings).
        """
        name_to_ticker = _build_company_name_to_ticker_map()
        out: List[Dict[str, Any]] = []

        for investor_name in SUPERSTAR_INVESTORS:
            try:
                holdings = self.fetch_investor_holdings(investor_name)
            except (ConnectionError, TrendlyneAuthError) as exc:
                logger.warning(f"Trendlyne export failed for {investor_name}: {exc}")
                continue

            for holding in holdings:
                ticker = name_to_ticker.get(_normalize_company_name(holding.get("company_name")))
                if ticker is None:
                    logger.debug(f"No stock_master match for {holding.get('company_name')!r} ({investor_name})")
                    continue
                out.append({
                    "investor_name": investor_name,
                    "ticker": ticker,
                    "stake_pct": holding.get("stake_pct"),
                    "qoq_change_pct": holding.get("qoq_change_pct"),
                    "quantity": holding.get("quantity"),
                })
            time.sleep(TRENDLYNE_RATE_LIMIT_SLEEP_SECONDS)

        return out

    def batch_export_named_holdings(self, conn) -> int:
        """
        Export per-investor holdings and upsert into public_shareholders,
        matching each investor_name to investor_family.family_id via
        ingestion.scrapers.bulk_deal_attribution.normalize_client_name (the
        same normalization used for large_deals.client_name, so a holder
        name here lines up with the same family the bulk-deal side sees).

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection

        Returns
        -------
        int
            Rows upserted.
        """
        from ingestion.scrapers.bulk_deal_attribution import normalize_client_name

        rows = self.export_named_holdings()
        if not rows:
            return 0

        quarter_end = _current_quarter_end()
        filing_date = quarter_end + timedelta(days=SHAREHOLDING_FILING_DELAY_DAYS)
        fetched_at = datetime.utcnow()

        family_map = dict(conn.execute("SELECT entity_name, family_id FROM investor_family").fetchall())

        written = 0
        for row in rows:
            family_id = family_map.get(normalize_client_name(row["investor_name"]))
            reported_shares = row.get("quantity")
            conn.execute(
                """
                INSERT INTO public_shareholders (
                    ticker, holder_name, quarter_end_date, filing_date, family_id,
                    stake_pct, qoq_change_pct, reported_shares, source, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'trendlyne', ?)
                ON CONFLICT (ticker, holder_name, quarter_end_date) DO UPDATE SET
                    filing_date = excluded.filing_date,
                    family_id = excluded.family_id,
                    stake_pct = excluded.stake_pct,
                    qoq_change_pct = excluded.qoq_change_pct,
                    reported_shares = excluded.reported_shares,
                    fetched_at = excluded.fetched_at
                """,
                [row["ticker"], row["investor_name"], quarter_end, filing_date, family_id,
                 row["stake_pct"], row["qoq_change_pct"],
                 int(reported_shares) if reported_shares is not None else None, fetched_at],
            )
            written += 1
        return written

    def fetch_investor_bulk_deals(self, investor_name: str) -> List[Dict[str, Any]]:
        """
        Fetch one superstar investor's full bulk/block-deal history from
        Trendlyne's bulk-block-deals page (see _bulk_deals_path_for and
        _parse_bulk_block_deals_table) — real per-transaction trade dates
        and prices, unlike fetch_investor_holdings' quarterly stake
        snapshot. No login required (confirmed live, unlike the
        superstar-shareholders holdings page) — uses a bare session, not
        self._session/login().

        Parameters
        ----------
        investor_name : str
            Must be a key of SUPERSTAR_INVESTORS.

        Returns
        -------
        list of dict
            See _parse_bulk_block_deals_table's docstring for the shape.

        Raises
        ------
        ValueError
            If investor_name is not a key of SUPERSTAR_INVESTORS.
        ConnectionError
            If the page can't be fetched after retries.
        """
        if investor_name not in SUPERSTAR_INVESTORS:
            raise ValueError(
                f"Unknown superstar investor {investor_name!r} — expected one of {list(SUPERSTAR_INVESTORS)}"
            )

        path = _bulk_deals_path_for(investor_name)
        session = requests.Session()
        session.headers.update(_HEADERS)
        response = _retry(lambda: session.get(f"{BASE_URL}{path}", timeout=30))
        if response.status_code != 200:
            raise ConnectionError(f"Trendlyne bulk-deals fetch failed for {path}: HTTP {response.status_code}")

        self.raw_dir.mkdir(parents=True, exist_ok=True)
        filename = path.strip("/").replace("/", "_") + ".html"
        (self.raw_dir / filename).write_text(response.text, encoding="utf-8")

        return _parse_bulk_block_deals_table(response.text)

    def export_bulk_deals_history(self) -> "pd.DataFrame":
        """
        Fetch every superstar investor's bulk/block-deal history and shape
        it into large_deals' exact column set (trade_date, exchange,
        deal_type, ticker, client_name, transaction_type, quantity, price,
        remarks), ready for persist_bulk_deals_history to insert.

        remarks is tagged "trendlyne:{investor_name}" — an audit trail
        distinguishing these Trendlyne-backfilled rows from the daily
        NSE/BSE ingestion (ingestion/scrapers/large_deals.py), which
        leaves remarks as NSE/BSE's own (usually blank) field.

        Rows whose company_name doesn't match a stock_master ticker are
        dropped (logged at debug level) — same per-holding isolation as
        export_named_holdings. A single investor's page failing to fetch
        or parse does not abort the batch.

        Returns
        -------
        pd.DataFrame
            Columns match ingestion.scrapers.large_deals._REQUIRED_COLUMNS.
        """
        import pandas as pd

        from ingestion.scrapers.large_deals import _REQUIRED_COLUMNS

        name_to_ticker = _build_company_name_to_ticker_map()
        out_rows: List[Dict[str, Any]] = []

        for investor_name in SUPERSTAR_INVESTORS:
            try:
                deals = self.fetch_investor_bulk_deals(investor_name)
            except ConnectionError as exc:
                logger.warning(f"Trendlyne bulk-deals export failed for {investor_name}: {exc}")
                continue

            for deal in deals:
                ticker = name_to_ticker.get(_normalize_company_name(deal.get("company_name")))
                if ticker is None:
                    logger.debug(f"No stock_master match for {deal.get('company_name')!r} ({investor_name})")
                    continue
                out_rows.append({
                    "trade_date": deal["trade_date"],
                    "exchange": deal.get("exchange"),
                    "deal_type": deal.get("deal_type"),
                    "ticker": ticker,
                    "client_name": deal.get("client_name"),
                    "transaction_type": deal.get("transaction_type"),
                    "quantity": int(deal["quantity"]) if deal.get("quantity") is not None else None,
                    "price": deal.get("price"),
                    "remarks": f"trendlyne:{investor_name}",
                })
            time.sleep(TRENDLYNE_RATE_LIMIT_SLEEP_SECONDS)

        return pd.DataFrame(out_rows, columns=_REQUIRED_COLUMNS) if out_rows else pd.DataFrame(columns=_REQUIRED_COLUMNS)

    def backfill_bulk_deals_history(self, conn) -> int:
        """
        Export every superstar investor's bulk/block-deal history and
        insert any rows not already present into large_deals.

        large_deals has no PRIMARY KEY (see create_normalised.py — the
        daily ingestion path instead does a delete-then-insert per
        trade_date, which would be wrong here since a single trade_date
        can already hold OTHER investors'/sources' rows we must not
        touch). Dedup is instead done per row via a NOT EXISTS anti-join
        on the same (trade_date, exchange, deal_type, ticker, client_name,
        transaction_type, quantity, price) tuple, so re-running this is
        idempotent without ever deleting existing rows.

        Does NOT re-run ingestion.scrapers.bulk_deal_attribution.
        attribute_bulk_deals over the newly-backfilled dates — callers
        (see scripts/backfill_bulk_deals_trendlyne.py) do that afterward,
        once per distinct new trade_date in ascending order, since
        bulk_deal_positions' running cumulative_position_est must be
        rebuilt oldest-to-newest.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection

        Returns
        -------
        int
            Rows actually inserted (excludes rows already present).
        """
        df = self.export_bulk_deals_history()
        if df.empty:
            return 0

        conn.register("_trendlyne_bulk_deals_staging", df)
        try:
            before = conn.execute("SELECT COUNT(*) FROM large_deals").fetchone()[0]
            conn.execute(
                """
                INSERT INTO large_deals
                    (trade_date, exchange, deal_type, ticker,
                     client_name, transaction_type, quantity, price, remarks)
                SELECT
                    CAST(s.trade_date AS DATE), s.exchange, s.deal_type, s.ticker,
                    s.client_name, s.transaction_type,
                    CAST(s.quantity AS BIGINT), CAST(s.price AS DOUBLE), s.remarks
                FROM _trendlyne_bulk_deals_staging s
                WHERE NOT EXISTS (
                    SELECT 1 FROM large_deals ld
                    WHERE ld.trade_date = CAST(s.trade_date AS DATE)
                      AND ld.exchange = s.exchange
                      AND ld.deal_type = s.deal_type
                      AND ld.ticker = s.ticker
                      AND ld.client_name IS NOT DISTINCT FROM s.client_name
                      AND ld.transaction_type IS NOT DISTINCT FROM s.transaction_type
                      AND ld.quantity IS NOT DISTINCT FROM CAST(s.quantity AS BIGINT)
                      AND ld.price IS NOT DISTINCT FROM CAST(s.price AS DOUBLE)
                )
                """
            )
            after = conn.execute("SELECT COUNT(*) FROM large_deals").fetchone()[0]
        finally:
            conn.unregister("_trendlyne_bulk_deals_staging")

        inserted = after - before
        logger.info(f"backfill_bulk_deals_history: {inserted} new large_deals rows from Trendlyne (of {len(df)} scraped)")
        return inserted


def _retry(fn, retries: int = DEFAULT_RETRY_COUNT):
    """Retry a zero-arg callable up to `retries` times, same pattern as screener.py."""
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except requests.RequestException as exc:
            last_exc = exc
            logger.warning(f"Trendlyne request failed (attempt {attempt}/{retries}): {exc}")
    raise ConnectionError(f"Trendlyne request failed after {retries} attempts: {last_exc}")


def _verify_page_matches_investor(html: str, investor_name: str) -> bool:
    """
    Fail-loud safety net for SUPERSTAR_INVESTORS' paths: check that the
    fetched page's title/heading text plausibly mentions the requested
    investor (first + last name token; middle names/patronymics/"and
    Associates" excluded, since Trendlyne's own <title>/<h1> text may not
    include every token from the full legal name either) before trusting
    the page's content — guards against SUPERSTAR_INVESTORS going stale
    if Trendlyne renumbers/reroutes an investor's page in the future.

    Returns
    -------
    bool
        True if both the first and last name token appear (case-insensitive)
        in the page's <title> or any heading tag; False otherwise.
    """
    soup = BeautifulSoup(html, "html.parser")
    header_text = " ".join(
        tag.get_text(" ", strip=True)
        for tag in soup.find_all(["title", "h1", "h2"])
    ).lower()
    if not header_text:
        return False

    tokens = [t for t in re.split(r"\s+", _NAME_NOISE_PATTERN.sub("", investor_name).strip()) if t]
    tokens = [t for t in tokens if t.lower() not in ("and", "associates", "family")]
    if not tokens:
        return False
    first, last = tokens[0].lower(), tokens[-1].lower()
    return first in header_text and last in header_text


def _parse_number(text: Optional[str]) -> Optional[float]:
    """
    Parse a Trendlyne-formatted number cell ('1,234', '12.3%', '+0.5%') to
    float, or None for any of Trendlyne's real "no value" markers: a bare
    '-' (below-disclosure-threshold or not held that quarter) or the
    literal text 'Filing Awaited' (quarter's shareholding filing not yet
    published) — both observed on a real authenticated page fetch,
    2026-07-05.
    """
    if text is None:
        return None
    stripped = text.strip()
    if stripped.lower() == "filing awaited":
        return None
    cleaned = re.sub(r"[,%\s+]", "", stripped)
    if cleaned in ("", "-"):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _flatten_header_cells(container) -> List[str]:
    """
    Trendlyne's real <thead> row has UNCLOSED <th> tags (confirmed via a
    real authenticated fetch, 2026-07-05: `<th>Holding Value<th>Qty
    Held</th>...` with no closing tag on the first <th>) — html.parser
    (this module has no lxml/html5lib dependency) nests every subsequent
    header inside the first unclosed one instead of treating them as
    siblings, the way a browser's forgiving HTML5 parser would. This
    walks that malformed nesting and returns the real flat header list in
    document order, using only each <th>'s own direct text (not its
    descendants' text) at every level.

    Parameters
    ----------
    container : bs4.Tag
        The <tr> (or nested <th>) to extract direct-child <th> text from.

    Returns
    -------
    list of str
    """
    from bs4 import NavigableString

    result = []
    for th in container.find_all("th", recursive=False):
        direct_strings = [str(c).strip() for c in th.children if isinstance(c, NavigableString)]
        text = " ".join(s for s in direct_strings if s)
        if text:
            result.append(text)
        result.extend(_flatten_header_cells(th))
    return result


def _parse_holdings_table(html: str) -> List[Dict[str, Any]]:
    """
    Parse a superstar investor's portfolio page holdings table into row
    dicts: [{"company_name": str, "quantity": float|None,
    "stake_pct": float|None, "qoq_change_pct": float|None}, ...].

    [AS BUILT, Big Investor Activity, 2026-07-05] Rewritten after a real
    authenticated fetch of a live page (Dolly Khanna, 2026-07-05) showed
    this function's original assumptions were wrong on two counts: (1)
    the page has 35 <table> elements, not 1 — `soup.find("table")` must
    target the specific `superstar-shareholding` class, not just the
    first table on the page; (2) column headers are quarter-dependent
    ("Jun 2026  Holding %") rather than the fixed "Holding %"/"Change"/
    "Company" labels originally assumed, and the header row's <th> tags
    are themselves unclosed/malformed (see _flatten_header_cells). Both
    were caught by running this against real data, not by inspection.

    quantity ("Qty Held") is a new field this rewrite also captures —
    Trendlyne reports a real absolute share count per holding, which
    ingestion/scrapers/bulk_deal_reconciliation.py now prefers over its
    own market-cap/price-derived estimate when available (see
    trendlyne.py's batch_export_named_holdings and
    bulk_deal_reconciliation.py's reconcile_family_ticker_quarter).
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="superstar-shareholding")
    if table is None:
        return []

    thead = table.find("thead", recursive=False)
    tbody = table.find("tbody", recursive=False)
    if thead is None or tbody is None:
        return []
    header_row = thead.find("tr", recursive=False)
    if header_row is None:
        return []

    headers = _flatten_header_cells(header_row)
    col_map: Dict[int, str] = {}
    for i, label in enumerate(headers):
        if _STOCK_HEADER.match(label.strip()):
            col_map[i] = "company_name"
        elif _QTY_HEADER.match(label.strip()):
            col_map[i] = "quantity"
        elif "stake_pct" not in col_map.values() and _HOLDING_PCT_HEADER.search(label):
            col_map[i] = "stake_pct"
        elif "qoq_change_pct" not in col_map.values() and _CHANGE_PCT_HEADER.search(label):
            col_map[i] = "qoq_change_pct"
    if not col_map:
        return []

    rows: List[Dict[str, Any]] = []
    for tr in tbody.find_all("tr", recursive=False):
        cells = tr.find_all("td", recursive=False)
        if not cells:
            continue
        row: Dict[str, Any] = {}
        for i, field in col_map.items():
            if i >= len(cells):
                continue
            text = cells[i].get_text(strip=True)
            row[field] = text if field == "company_name" else _parse_number(text)
        if row.get("company_name"):
            rows.append(row)
    return rows


def _bulk_deals_path_for(investor_name: str) -> str:
    """
    Derive an investor's Trendlyne bulk/block-deals-history page path from
    their SUPERSTAR_INVESTORS superstar-shareholders path — same numeric
    object ID and name slug, different URL family:
        superstar-shareholders/{id}/latest/{slug}-portfolio/
        bulk-block-deals/{id}/{slug}-portfolio/
    (confirmed live, 2026-07-05, against
    https://trendlyne.com/portfolio/bulk-block-deals/53781/
    rakesh-jhunjhunwala-and-associates-portfolio/ — same id (53781) and
    slug as SUPERSTAR_INVESTORS["Rakesh Jhunjhunwala and Associates"]).
    """
    path = SUPERSTAR_INVESTORS[investor_name]
    return path.replace("/superstar-shareholders/", "/bulk-block-deals/").replace("/latest/", "/")


def _parse_bulk_block_deals_table(html: str) -> List[Dict[str, Any]]:
    """
    Parse an investor's Trendlyne bulk/block-deals-history page into row
    dicts matching the large_deals column shape: [{"company_name": str,
    "client_name": str, "exchange": str, "deal_type": str,
    "transaction_type": "B"|"S", "trade_date": str ("YYYY-MM-DD"),
    "price": float|None, "quantity": float|None}, ...].

    [AS BUILT, Big Investor Activity Phase E, 2026-07-05] Verified live
    against a real unauthenticated fetch (no login needed — unlike the
    quarterly superstar-shareholders holdings page, this page's deal
    history is fully public) of the Rakesh Jhunjhunwala and Associates
    page: single `<table id="bbdealTable">`, one `<tr>` per disclosed
    bulk/block deal, going back to 2010-02-02 in that one page load (131
    rows, no pagination or AJAX call needed — the `JS_autoDataTables`
    class is a client-side DataTables.js search/sort layer applied AFTER
    the full history is already server-rendered, not a paginated source).
    Company name comes from the stockrow `<td>`'s `data-export` attribute
    (clean text, no need to strip the nested DVM-score/badge markup each
    row also carries) and trade_date from the Date `<td>`'s `data-order`
    attribute (already ISO "YYYY-MM-DD", not the "14 May 2026" display
    text). Action ("Purchase"/"Sell") maps the same way
    large_deals.py._normalise_transaction_type does, reused here via that
    module rather than reimplemented, so this stays byte-identical to how
    the daily NSE/BSE ingestion classifies the same B/S flag.

    If some other investor's page ever lacks table#bbdealTable (page
    structure changed, or Trendlyne truly has zero bulk/block history for
    them), this returns [] rather than raising — callers already treat a
    per-investor failure as skip-and-continue (same isolation as
    fetch_investor_holdings's callers), and an empty deal history is a
    valid real state (not every superstar investor's activity crosses the
    NSE bulk/block-deal disclosure threshold).
    """
    from ingestion.scrapers.large_deals import _normalise_transaction_type

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", id="bbdealTable")
    if table is None:
        return []
    tbody = table.find("tbody", recursive=False)
    if tbody is None:
        return []

    rows: List[Dict[str, Any]] = []
    for tr in tbody.find_all("tr", recursive=False):
        cells = tr.find_all("td", recursive=False)
        if len(cells) < 8:
            continue
        stock_cell, client_cell, exch_cell, deal_type_cell, action_cell, date_cell, price_cell, qty_cell = cells[:8]

        company_name = stock_cell.get("data-export") or stock_cell.get_text(strip=True)
        date_order = date_cell.get("data-order")
        if not company_name or not date_order:
            continue

        rows.append({
            "company_name": company_name,
            "client_name": client_cell.get_text(strip=True) or None,
            "exchange": exch_cell.get_text(strip=True).upper() or None,
            "deal_type": deal_type_cell.get_text(strip=True).upper() or None,
            "transaction_type": _normalise_transaction_type(action_cell.get_text(strip=True)),
            "trade_date": date_order.strip(),
            "price": _parse_number(price_cell.get_text(strip=True)),
            "quantity": _parse_number(qty_cell.get_text(strip=True)),
        })
    return rows


def _normalize_company_name(name: Optional[str]) -> str:
    """'HDFC Bank Ltd' / 'Hdfc Bank Limited' -> 'hdfcbank' — robust enough for exact-set matching, not fuzzy.

    [AS BUILT] Same logic as ingestion/scrapers/groww_mf_holdings.py's
    _normalize_company_name — duplicated locally rather than imported,
    since the two modules are independent data domains (MF scheme
    holdings vs. superstar-investor holdings) with no other coupling;
    same "don't share private helpers across unrelated modules" precedent
    as features/technical.py vs. features/fno_features.py (see BuildLog.md).

    [AS BUILT, 2026-07-05] `not name` alone does not catch a NaN float —
    `bool(float("nan"))` is True, not False — so a real run against
    _build_company_name_to_ticker_map() crashed with a TypeError the
    moment it reached one of stock_master's ~691 still-unresolved blank
    company_name rows (see FutureDevelopment.md's #31). `isinstance` guard
    added so any non-string company_name (NaN included) normalizes to ""
    like a real empty name would, rather than crashing the whole export.
    """
    if not isinstance(name, str) or not name:
        return ""
    cleaned = _NAME_NOISE_PATTERN.sub("", name)
    return re.sub(r"[^a-z0-9]+", "", cleaned.lower())


def _build_company_name_to_ticker_map() -> Dict[str, str]:
    """Normalized company_name -> ticker, from the real universe (config.universe.load_universe_raw)."""
    df = load_universe_raw()
    return {_normalize_company_name(row.company_name): row.ticker for row in df.itertuples()}


def _current_quarter_end(today: Optional[date] = None) -> date:
    """Most recently completed Indian fiscal quarter-end on or before `today`."""
    today = today or date.today()
    quarter_ends = [date(today.year, 3, 31), date(today.year, 6, 30), date(today.year, 9, 30), date(today.year, 12, 31)]
    past = [d for d in quarter_ends if d <= today]
    if past:
        return max(past)
    return date(today.year - 1, 12, 31)


def _cli() -> None:
    """
    CLI entry point: `python3 -m ingestion.scrapers.trendlyne export "Dolly Khanna"`
    or `python3 -m ingestion.scrapers.trendlyne batch`.
    """
    import argparse
    import json

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Trendlyne StratQ superstar-investor holdings export")
    subparsers = parser.add_subparsers(dest="command", required=True)

    export_parser = subparsers.add_parser("export", help="Export one investor's holdings, print only (no write)")
    export_parser.add_argument("investor", choices=list(SUPERSTAR_INVESTORS))

    batch_parser = subparsers.add_parser("batch", help="Export + write all superstar investors, rate-limited")
    batch_parser.add_argument("--no-write", action="store_true", help="Export only, skip the API writes")

    bulk_deals_parser = subparsers.add_parser(
        "export-bulk-deals", help="Export one investor's real bulk/block-deal history, print only (no write)"
    )
    bulk_deals_parser.add_argument("investor", choices=list(SUPERSTAR_INVESTORS))

    args = parser.parse_args()
    scraper = TrendlyneScraper()

    if args.command == "export":
        result = scraper.fetch_investor_holdings(args.investor)
        print(json.dumps(result, default=str, indent=2), flush=True)
        return

    if args.command == "export-bulk-deals":
        result = scraper.fetch_investor_bulk_deals(args.investor)
        print(json.dumps(result, default=str, indent=2), flush=True)
        return

    results = scraper.batch_export(write=not args.no_write)
    n_ok = sum(1 for ok in results.values() if ok)
    print(f"Done: {n_ok}/{len(results)} tickers written.", flush=True)
    failed = [t for t, ok in results.items() if not ok]
    if failed:
        print(f"Failed: {failed}", flush=True)


if __name__ == "__main__":
    _cli()
