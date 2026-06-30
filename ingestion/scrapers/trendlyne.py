"""
ingestion/scrapers/trendlyne.py

Phase: 2.6 (Phase 2 Data Source Integration)
Specs: SPEC-PIPE-001, SPEC-PIPE-003 (CRITICAL), SPEC-SEC-001
Owner: Platform / Ingestion
Consumers: features/governance.py, systems/ml_signal_engine (superstar_flag/superstar_change)

TrendlyneScraper: logs into Trendlyne StratQ and exports the quarterly
portfolio holdings of 5 named "superstar" retail investors, mapping each
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

[AS BUILT, P2.6] HONEST GAP — Trendlyne StratQ's real page structure and
login form field names could NOT be verified live: no real account exists
in this environment (TRENDLYNE_USERNAME/PASSWORD in .env are operator-filled
placeholders, same as every other paid-source credential in this project).
The investor portfolio page slugs in SUPERSTAR_INVESTORS, the login POST
field names, and _parse_holdings_table()'s column-header matching are all
best-effort, modelled directly on screener.in's verified Django-CSRF login
flow (the same login mechanism family — both are Django-templated sites) —
not independently confirmed. login() and fetch_investor_holdings() raise a
clear TrendlyneAuthError / ValueError on a structure mismatch rather than
silently returning wrong data, so a field-name or slug mismatch fails loud
on the operator's first real run with real credentials — the exact same
"verify-against-the-real-thing, fail loud if not" pattern screener.py's own
module docstring documents for its own unverifiable login POST (see
BuildLog.md "P0.5", "P2.1").

Aggregation rule (own construction, not from an external spec): a ticker
held by more than one of the 5 superstar investors gets superstar_flag=True
(any holds it) and superstar_change = the SUM of each holding investor's
own QoQ stake-percentage-point change (net combined superstar buying/
selling pressure across the cohort, signed) — a simple, documented choice,
not a literal Trendlyne StratQ output field.
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
_HEADERS = {"User-Agent": "Mozilla/5.0 (AlphaLens research scraper; contact via account owner)"}

# Build prompt's literal 5 superstar investors -> best-effort StratQ portfolio
# page slug (UNVERIFIED — see module docstring's "HONEST GAP").
SUPERSTAR_INVESTORS: Dict[str, str] = {
    "Dolly Khanna": "dolly-khanna",
    "Vijay Kedia": "vijay-kedia",
    "Ashish Kacholia": "ashish-kacholia",
    "Sunil Singhania": "sunil-singhania",
    "Porinju Veliyath": "porinju-veliyath",
}

_NAME_NOISE_PATTERN = re.compile(r"\b(ltd|limited|the)\b\.?", re.IGNORECASE)
# Holdings-table column header -> internal field name (best-effort, see module docstring)
_HOLDINGS_FIELDS = {
    "Company": "company_name",
    "Holding %": "stake_pct",
    "Change": "qoq_change_pct",
}


class TrendlyneAuthError(RuntimeError):
    """Raised when login() cannot establish an authenticated session."""


class TrendlyneScraper:
    """
    Logs into Trendlyne StratQ and exports the 5 named superstar investors'
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

    def _fetch_investor_page(self, slug: str) -> str:
        """Fetch and raw-save a superstar investor's StratQ portfolio page (SPEC-PIPE-001)."""
        if self._session is None:
            self.login()

        url = f"{BASE_URL}/stratq/superstar-investors/portfolio/{slug}/"
        response = _retry(lambda: self._session.get(url, timeout=30))
        if response.status_code != 200:
            raise ConnectionError(f"Trendlyne fetch failed for {slug}: HTTP {response.status_code}")

        self.raw_dir.mkdir(parents=True, exist_ok=True)
        (self.raw_dir / f"{slug}.html").write_text(response.text, encoding="utf-8")
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
            If investor_name is not one of the 5 named superstar investors.
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
        return _parse_holdings_table(html)

    def export_superstar_holdings(self) -> Dict[str, Dict[str, Any]]:
        """
        Fetch all 5 superstar investors and aggregate to one row per ticker.

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
        Export all 5 superstar investors' holdings and write
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


def _parse_number(text: Optional[str]) -> Optional[float]:
    """Parse a Trendlyne-formatted number cell ('1,234', '12.3%', '+0.5%') to float, or None."""
    if text is None:
        return None
    cleaned = re.sub(r"[,%\s+]", "", text.strip())
    if cleaned in ("", "-"):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_holdings_table(html: str) -> List[Dict[str, Any]]:
    """Parse a StratQ portfolio page's holdings table into row dicts (best-effort, see module docstring)."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if table is None:
        return []

    header_cells = [th.get_text(strip=True) for th in table.find_all("th")]
    col_map = {i: _HOLDINGS_FIELDS[label] for i, label in enumerate(header_cells) if label in _HOLDINGS_FIELDS}
    if not col_map:
        return []

    rows: List[Dict[str, Any]] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all("td")
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


def _normalize_company_name(name: Optional[str]) -> str:
    """'HDFC Bank Ltd' / 'Hdfc Bank Limited' -> 'hdfcbank' — robust enough for exact-set matching, not fuzzy.

    [AS BUILT] Same logic as ingestion/scrapers/groww_mf_holdings.py's
    _normalize_company_name — duplicated locally rather than imported,
    since the two modules are independent data domains (MF scheme
    holdings vs. superstar-investor holdings) with no other coupling;
    same "don't share private helpers across unrelated modules" precedent
    as features/technical.py vs. features/fno_features.py (see BuildLog.md).
    """
    if not name:
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

    batch_parser = subparsers.add_parser("batch", help="Export + write all 5 superstar investors, rate-limited")
    batch_parser.add_argument("--no-write", action="store_true", help="Export only, skip the API writes")

    args = parser.parse_args()
    scraper = TrendlyneScraper()

    if args.command == "export":
        result = scraper.fetch_investor_holdings(args.investor)
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
