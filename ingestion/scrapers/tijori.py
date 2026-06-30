"""
ingestion/scrapers/tijori.py

Phase: 2.6 (Phase 2 Data Source Integration)
Specs: SPEC-PIPE-001, SPEC-PIPE-003 (CRITICAL), SPEC-SEC-001
Owner: Platform / Ingestion
Consumers: features/fundamental.py, systems/ml_signal_engine (sector_specific_metric_1..6)

TijoriScraper: logs into Tijori Finance Pro and exports sector-specific
operational metrics (ARPU for telecom, NPA for banking, ANDA approvals
for pharma, etc.) per company, written through the DataStore API as
fundamentals' sector_specific_metric_1 through sector_specific_metric_6.
Sector is read from stock_master.sector (config.universe.load_universe_raw),
per the build prompt's explicit instruction.

[AS BUILT, P2.6] Credentials: the build prompt says "TIJORI_API_KEY from
.env". No such variable exists anywhere in this codebase.
config/settings.py and .env.example already define TIJORI_USERNAME /
TIJORI_PASSWORD ("Phase 2.6: Tijori Finance Pro login") — Tijori Finance
Pro, like screener.in Premium and Trendlyne StratQ, is a paid login-walled
web subscription (CLAUDE.md's data source table: "₹3,500/yr"), not a
token-authenticated REST API. Same resolution as
ingestion/scrapers/trendlyne.py's identical TRENDLYNE_API_KEY mismatch —
see that module's docstring.

[AS BUILT, P2.6] HONEST GAP — the alphalens_docs/Fundamental_Data_Sourcing_
Guide.md referenced by this build prompt does not exist in this repository
(confirmed: no match anywhere in the filesystem or git history). The build
prompt itself only names 3 of an implied larger set ("ARPU for telecom,
NPA for banking, ANDA for pharma, etc."). _SECTOR_METRICS below is this
module's OWN construction — a reasonable, India-equity-research-standard
operational metric per real sector string found in
config/nifty500_universe.csv (Financial Services, Information Technology,
Telecommunication, Healthcare, Automobile and Auto Components, Oil Gas &
Consumable Fuels, Construction Materials, Power, Metals & Mining,
Chemicals, Capital Goods, Fast Moving Consumer Goods, Consumer Durables,
Realty, Textiles, Consumer Services, Services, Media Entertainment &
Publication, Construction) — NOT lifted from a verified real sourcing
document. "Diversified" (conglomerates) is intentionally left unmapped:
a true multi-business conglomerate has no single coherent operational
metric set, so fabricating one would be worse than an honest NaN.

Tijori Finance Pro's real page structure and login form field names also
could NOT be verified live (no real account in this environment) — same
"fails loud on the operator's first real run, not silently wrong"
discipline as trendlyne.py and screener.py's own documented gaps.
sector_specific_metric_1 through _6 are written in the SAME order as
_SECTOR_METRICS[sector]'s metric-name list, for a given row — so the
metric label for a specific value is always recoverable by cross-
referencing that list with the column index, even though the schema
itself only carries generic numbered columns (see
datastore/schema/create_normalised.py's fundamentals DDL comment).
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
    TIJORI_PASSWORD,
    TIJORI_RATE_LIMIT_SLEEP_SECONDS,
    TIJORI_RAW_DIR,
    TIJORI_USERNAME,
)
from config.universe import load_universe_raw
from datastore.client import DataStoreClient

logger = logging.getLogger(__name__)

BASE_URL = "https://www.tijorifinance.com"
LOGIN_URL = f"{BASE_URL}/accounts/login/"
_HEADERS = {"User-Agent": "Mozilla/5.0 (AlphaLens research scraper; contact via account owner)"}

# sector (stock_master.sector / config/nifty500_universe.csv real values) ->
# up to 6 sector-specific operational metric NAMES, in the order they are
# written to sector_specific_metric_1.._6 — see module docstring's HONEST GAP.
_SECTOR_METRICS: Dict[str, List[str]] = {
    "Financial Services": [
        "Gross NPA %", "CASA Ratio %", "Net Interest Margin %",
        "Credit to Deposit Ratio %", "Provision Coverage Ratio %", "Cost to Income Ratio %",
    ],
    "Information Technology": [
        "Revenue per Employee", "Attrition Rate %", "Utilization Rate %",
        "Deal TCV", "Offshore Revenue Mix %", "Digital Revenue Mix %",
    ],
    "Telecommunication": [
        "ARPU", "Subscriber Churn Rate %", "Data Usage per Subscriber GB",
        "Tower Tenancy Ratio", "Network Capex Intensity %", "4G/5G Subscriber Mix %",
    ],
    "Healthcare": [
        "ANDA Approvals Cumulative", "R&D Spend % of Revenue", "US Generics Revenue Mix %",
        "Plant Utilization Rate %", "FDA Inspection Flags", "Domestic Formulation Growth %",
    ],
    "Automobile and Auto Components": [
        "Volume Growth %", "Capacity Utilization %", "Realization per Unit",
        "EV Mix %", "Channel Inventory Days", "Export Mix %",
    ],
    "Oil Gas & Consumable Fuels": [
        "Gross Refining Margin", "Crude Throughput", "Reserve Replacement Ratio",
        "Marketing Margin", "Petrochemical Spread", "Upstream Realization",
    ],
    "Construction Materials": [
        "Capacity Utilization %", "Realization per Tonne", "EBITDA per Tonne",
        "Volume Growth %", "Fuel Cost per Tonne", "Clinker Factor",
    ],
    "Power": [
        "Plant Load Factor %", "Tariff Realization", "AT&C Losses %",
        "Capacity Addition MW", "PPA Coverage %", "Fuel Cost per Unit",
    ],
    "Metals & Mining": [
        "Capacity Utilization %", "Realization per Tonne", "Cost per Tonne",
        "Volume Growth %", "Raw Material Linkage %", "Export Mix %",
    ],
    "Chemicals": [
        "Capacity Utilization %", "Price-Feedstock Spread", "Export Mix %",
        "Specialty Product Mix %", "Volume Growth %", "Contract vs Spot Mix %",
    ],
    "Capital Goods": [
        "Order Book to Revenue Ratio", "Order Inflow Growth %", "Execution Cycle Months",
        "Export Order Mix %", "Working Capital Days", "Margin on Order Book %",
    ],
    "Fast Moving Consumer Goods": [
        "Volume Growth %", "Rural Mix %", "Distribution Reach Outlets",
        "Ad Spend % of Revenue", "Gross Margin %", "New Product Revenue Mix %",
    ],
    "Consumer Durables": [
        "Volume Growth %", "Dealer Inventory Days", "Premiumization Mix %",
        "Online Sales Mix %", "Service Revenue Mix %", "Working Capital Days",
    ],
    "Realty": [
        "Pre-sales Growth %", "Collection Efficiency %", "Net Debt to Equity",
        "Unsold Inventory Months", "Launch Pipeline Sq Ft", "Realization per Sq Ft",
    ],
    "Textiles": [
        "Capacity Utilization %", "Export Mix %", "Raw Material Cost Mix %",
        "Volume Growth %", "Value-Added Product Mix %", "Realization per Unit",
    ],
    "Consumer Services": [
        "Same-Store Sales Growth %", "Store Count Growth", "Footfall Conversion %",
        "Average Ticket Size", "Online Mix %", "EBITDA Margin per Store %",
    ],
    "Services": [
        "Volume Growth %", "Utilization/Load Factor %", "Yield/Realization",
        "Cost per Unit", "Network Expansion", "On-time Performance %",
    ],
    "Media Entertainment & Publication": [
        "Subscriber/Viewership Growth %", "Ad Revenue Mix %", "Content Cost Ratio %",
        "Digital Revenue Mix %", "ARPU", "Churn Rate %",
    ],
    "Construction": [
        "Order Book to Revenue Ratio", "Order Inflow Growth %", "Execution Pace %",
        "Working Capital Days", "Net Debt to Equity", "Margin on Order Book %",
    ],
    # "Diversified" intentionally absent — see module docstring.
}


class TijoriAuthError(RuntimeError):
    """Raised when login() cannot establish an authenticated session."""


class TijoriScraper:
    """
    Logs into Tijori Finance Pro and exports sector-specific operational
    metrics per company, written via the DataStore API as fundamentals'
    sector_specific_metric_1 through sector_specific_metric_6.

    Parameters
    ----------
    username : str, optional
        Defaults to config.settings.TIJORI_USERNAME (.env).
    password : str, optional
        Defaults to config.settings.TIJORI_PASSWORD (.env).
    raw_dir : Path, optional
        Where raw HTML is saved (SPEC-PIPE-001 raw retention).
        Defaults to config.settings.TIJORI_RAW_DIR.
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
        raw_dir: Path = TIJORI_RAW_DIR,
        client: Optional[DataStoreClient] = None,
    ) -> None:
        self.username = username or TIJORI_USERNAME
        self.password = password or TIJORI_PASSWORD
        self.raw_dir = raw_dir
        self.client = client or DataStoreClient()
        self._session: Optional[requests.Session] = None

    def login(self) -> requests.Session:
        """
        Authenticate against Tijori Finance Pro and cache the session.

        Returns
        -------
        requests.Session

        Raises
        ------
        TijoriAuthError
            If TIJORI_USERNAME/TIJORI_PASSWORD are not set, the login
            page's CSRF token can't be found, or the login POST does not
            land on an authenticated page.
        """
        if not self.username or not self.password:
            raise TijoriAuthError(
                "TIJORI_USERNAME/TIJORI_PASSWORD not set — add real Tijori "
                "Finance Pro credentials to .env before calling login()."
            )

        session = requests.Session()
        session.headers.update(_HEADERS)

        login_page = _retry(lambda: session.get(LOGIN_URL, timeout=30))
        csrf_match = re.search(r'name=["\']csrfmiddlewaretoken["\']\s+value=["\']([^"\']+)["\']', login_page.text)
        if not csrf_match:
            raise TijoriAuthError(
                "Could not find csrfmiddlewaretoken on the login page — Tijori's "
                "login form markup may differ from this module's assumption; "
                "inspect a live page before retrying (see module docstring's HONEST GAP)."
            )
        csrf_token = csrf_match.group(1)

        response = _retry(
            lambda: session.post(
                LOGIN_URL,
                data={"csrfmiddlewaretoken": csrf_token, "email": self.username, "password": self.password},
                headers={"Referer": LOGIN_URL},
                timeout=30,
            )
        )
        if response.status_code >= 400 or "id_password" in response.text:
            raise TijoriAuthError(
                f"Tijori login failed (status={response.status_code}). "
                "If field names changed, update login()'s POST payload — see module docstring."
            )

        self._session = session
        logger.info("Tijori login successful")
        return session

    def _fetch_company_page(self, ticker: str) -> str:
        """Fetch and raw-save a company's Tijori operational-metrics page (SPEC-PIPE-001)."""
        if self._session is None:
            self.login()

        url = f"{BASE_URL}/company/{ticker}/operating-metrics/"
        response = _retry(lambda: self._session.get(url, timeout=30))
        if response.status_code != 200:
            raise ConnectionError(f"Tijori fetch failed for {ticker}: HTTP {response.status_code}")

        self.raw_dir.mkdir(parents=True, exist_ok=True)
        (self.raw_dir / f"{ticker}.html").write_text(response.text, encoding="utf-8")
        return response.text

    def export_company_metrics(self, ticker: str, sector: Optional[str]) -> Optional[Dict[str, Any]]:
        """
        Export one ticker's sector-specific operational metrics.

        Parameters
        ----------
        ticker : str
        sector : str, optional
            stock_master.sector value for this ticker (the build prompt's
            "Sector detection from stock_master.sector column"). If None,
            or not a key of _SECTOR_METRICS (e.g. "Diversified", or a
            sector string not in config/nifty500_universe.csv's real set),
            returns None — no metric labels exist to parse against.

        Returns
        -------
        dict or None
            FundamentalsWrite-shaped partial dict (ticker, fiscal_year,
            quarter, quarter_end_date, announcement_date,
            sector_specific_metric_1.._6), or None if sector has no
            mapping or the page has no matching rows.

        Raises
        ------
        TijoriAuthError
            If not yet logged in and login() fails.
        ConnectionError
            If the company page can't be fetched after retries.
        """
        metric_names = _SECTOR_METRICS.get(sector or "")
        if metric_names is None:
            logger.debug(f"No sector-metric mapping for {ticker} (sector={sector!r}) — skipping")
            return None

        html = self._fetch_company_page(ticker)
        values = _parse_operating_metrics(html, metric_names)
        if all(v is None for v in values):
            return None

        quarter_end = _current_quarter_end()
        announcement_date = quarter_end + timedelta(days=FUNDAMENTALS_ANNOUNCEMENT_DELAY_DAYS)
        fiscal_year, quarter = _fiscal_year_quarter(quarter_end)

        row: Dict[str, Any] = {
            "ticker": ticker,
            "fiscal_year": fiscal_year,
            "quarter": quarter,
            "quarter_end_date": quarter_end,
            "announcement_date": announcement_date,
        }
        for i, value in enumerate(values, start=1):
            row[f"sector_specific_metric_{i}"] = value
        return row

    def batch_export(self, tickers: Optional[List[str]] = None, write: bool = True) -> Dict[str, bool]:
        """
        Export and write sector-specific metrics for many tickers, rate-limited.

        Parameters
        ----------
        tickers : list of str, optional
            Defaults to every ticker in config.universe.load_universe_raw()
            (the build prompt's "For each sector" — driven by the real
            universe, not a hardcoded list).
        write : bool
            If True (default), upserts via DataStoreClient.write_fundamentals.
            If False, exports only (used by tests).

        Returns
        -------
        dict
            ticker -> True if export+write succeeded (or write=False and
            export succeeded/skipped), False if export or write failed.
            A ticker with no sector mapping is recorded as True (correctly
            skipped, not a failure) — same per-ticker isolation as
            screener.py's batch_export.
        """
        universe = load_universe_raw()
        sector_map = dict(zip(universe["ticker"], universe["sector"]))
        if tickers is None:
            tickers = list(universe["ticker"])

        results: Dict[str, bool] = {}
        for ticker in tickers:
            try:
                row = self.export_company_metrics(ticker, sector_map.get(ticker))
                if row is not None and write:
                    self.client.write_fundamentals(row)
                results[ticker] = True
            except Exception as exc:
                logger.warning(f"Tijori export failed for {ticker}: {exc}")
                results[ticker] = False
            time.sleep(TIJORI_RATE_LIMIT_SLEEP_SECONDS)
        return results


def _retry(fn, retries: int = DEFAULT_RETRY_COUNT):
    """Retry a zero-arg callable up to `retries` times, same pattern as screener.py."""
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except requests.RequestException as exc:
            last_exc = exc
            logger.warning(f"Tijori request failed (attempt {attempt}/{retries}): {exc}")
    raise ConnectionError(f"Tijori request failed after {retries} attempts: {last_exc}")


def _parse_number(text: Optional[str]) -> Optional[float]:
    """Parse a Tijori-formatted number cell ('1,234', '12.3%') to float, or None."""
    if text is None:
        return None
    cleaned = re.sub(r"[,%\s]", "", text.strip())
    if cleaned in ("", "-"):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_operating_metrics(html: str, metric_names: List[str]) -> List[Optional[float]]:
    """
    Find each of `metric_names` as a row label in the page and return its
    value, in the same order — best-effort, see module docstring's HONEST
    GAP. Always returns exactly len(metric_names) entries (None where not found).
    """
    soup = BeautifulSoup(html, "html.parser")
    label_to_value: Dict[str, Optional[float]] = {}
    for row in soup.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        label = cells[0].get_text(strip=True)
        label_to_value[label] = _parse_number(cells[1].get_text(strip=True))

    return [label_to_value.get(name) for name in metric_names]


def _current_quarter_end(today: Optional[date] = None) -> date:
    """Most recently completed Indian fiscal quarter-end on or before `today`."""
    today = today or date.today()
    quarter_ends = [date(today.year, 3, 31), date(today.year, 6, 30), date(today.year, 9, 30), date(today.year, 12, 31)]
    past = [d for d in quarter_ends if d <= today]
    if past:
        return max(past)
    return date(today.year - 1, 12, 31)


def _fiscal_year_quarter(quarter_end: date) -> tuple:
    """Indian fiscal year (Apr-Mar) and quarter number (1=Apr-Jun) for a quarter_end_date."""
    if quarter_end.month == 3:
        return quarter_end.year, 4
    if quarter_end.month == 6:
        return quarter_end.year + 1, 1
    if quarter_end.month == 9:
        return quarter_end.year + 1, 2
    return quarter_end.year + 1, 3


def _cli() -> None:
    """
    CLI entry point: `python3 -m ingestion.scrapers.tijori export TICKER`
    or `python3 -m ingestion.scrapers.tijori batch [--tickers A,B,C]`.
    """
    import argparse
    import json

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Tijori Finance Pro sector-specific metrics export")
    subparsers = parser.add_subparsers(dest="command", required=True)

    export_parser = subparsers.add_parser("export", help="Export one ticker, print only (no write)")
    export_parser.add_argument("ticker")
    export_parser.add_argument("--sector", required=True, help="stock_master.sector value for this ticker")

    batch_parser = subparsers.add_parser("batch", help="Export + write the real universe, rate-limited")
    batch_parser.add_argument("--tickers", help="Comma-separated ticker list (default: full universe)")
    batch_parser.add_argument("--no-write", action="store_true", help="Export only, skip the API writes")

    args = parser.parse_args()
    scraper = TijoriScraper()

    if args.command == "export":
        result = scraper.export_company_metrics(args.ticker, args.sector)
        print(json.dumps(result, default=str, indent=2), flush=True)
        return

    tickers = [t.strip().upper() for t in args.tickers.split(",")] if args.tickers else None
    results = scraper.batch_export(tickers=tickers, write=not args.no_write)
    n_ok = sum(1 for ok in results.values() if ok)
    print(f"Done: {n_ok}/{len(results)} succeeded.", flush=True)
    failed = [t for t, ok in results.items() if not ok]
    if failed:
        print(f"Failed: {failed}", flush=True)


if __name__ == "__main__":
    _cli()
