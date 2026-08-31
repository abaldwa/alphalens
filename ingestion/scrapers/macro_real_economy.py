"""
ingestion/scrapers/macro_real_economy.py

Phase: 3.1 (Real Economy Macro Features)
Specs: SPEC-FEAT-001, SPEC-PIPE-003 (CRITICAL), SPEC-PIPE-006
Owner: Platform / Ingestion
Consumers: features/real_economy_macro.py

STATUS (2026-07-07, follow-up session): 2 of 10 series now have a real,
free, structured source — cement_dispatches_growth and
power_consumption_growth, both from DPIIT's Office of the Economic
Adviser (eaindustry.nic.in), which publishes the "Index of Eight Core
Industries" (ICI) as a downloadable .xlsx (not a PDF) updated monthly.
Its "Growth (%)" sheet already carries pre-computed YoY growth for Cement
and Electricity (used here as the power-consumption proxy — ICI's
Electricity index tracks power generation, the closest free monthly
series to "power consumption"), alongside Coal/Crude Oil/Natural Gas/
Refinery/Fertilizers/Steel growth (not currently consumed by any feature,
left unused rather than repurposed). Live-verified 2026-07-07: real
monthly rows through 2026-05-01 (May 2026), e.g. cement growth=5.0%,
electricity growth=8.4% for that month — see download_core_industries_index()
and fetch_cement_and_power_growth() below.

The other 8 series remain genuinely blocked; the original research below
(from the 2026-07-07 same-day investigation, before this ICI source was
found) still applies to them unchanged.

Per-series findings (all live-tested 2026-07-07, not guessed)
---------------------------------------------------------------
- pmi_manufacturing / pmi_services:
    S&P Global (formerly IHS Markit/Markit Economics) owns and licenses the
    India PMI series commercially. There is no free official release of
    the numeric series anywhere (only the headline number appears in news
    coverage, not a fetchable feed). Not pursued further — this is a
    licensing wall, not a technical one.

- iip_growth (Index of Industrial Production):
    FRED carries an India IIP series, INDPROINDMISMEI (OECD-sourced,
    ultimately MOSPI). Live-tested:
      curl "https://fred.stlouisfed.org/graph/fredgraph.csv?id=INDPROINDMISMEI"
    returns real historical data, BUT the series is discontinued/stale —
    the last observation is 2023-01-01 (verified via `tail` on the live
    CSV response), over 3 years stale as of this writing (2026-07-07).
    Wiring this in would produce a permanently-frozen 2023 value forward-
    filled forever, which is worse than an honest NaN. MOSPI's own site
    (mospi.gov.in) publishes IIP only as PDF press releases, not a
    scrapeable JSON/CSV endpoint. data.gov.in has an IIP dataset resource
    (catalog id 6176ee09-3d56-4a3b-8115-21841576b2f6) behind api.data.gov.in,
    but every request requires a personal registered API key
    (`api-key=test` live-tested and rejected with
    `{"error": "Key not authorised"}`); no key is available in this
    environment/session, and even a valid key would still be MOSPI IIP
    with the same ~45-day lag already modeled in
    features/real_economy_macro.py's _RELEASE_LAG_DAYS.

- bank_credit_growth:
    RBI's own Database on Indian Economy (DBIE, dbie.rbi.org.in) does not
    expose a public JSON/CSV API — it's a query-builder web UI with
    session-based exports, not a link fetchable outside a browser. FRED
    was checked for an India bank-credit series under several plausible
    series IDs (QINBAM770A, DDOI02INA066NWDB, DDSI02INA066NWDB,
    CBANKS01INQ657S) — all returned FRED's HTML 404 page, not real data;
    none of these IDs exist. The World Bank API does have India domestic
    credit to private sector (% of GDP), live-tested and working:
      curl "https://api.worldbank.org/v2/country/IND/indicator/FS.AST.PRVT.GD.ZS?format=json&per_page=5&date=2020:2024"
    returned real values (e.g. 41.6% for 2024), but this series is ANNUAL
    with a >1 year publication lag and is a stock ratio (% of GDP), not
    the monthly YoY growth rate the feature name/semantics call for — too
    coarse to be a faithful "bank_credit_growth" and would misrepresent a
    monthly indicator using stale annual data. Not wired in for that
    reason; documented rather than approximated.

- gst_collection_growth:
    Published monthly via PIB (pib.gov.in) press releases as HTML/prose
    ("GST collection for the month of ... stood at Rs. ... crore"), not a
    structured feed. Scraping this reliably would mean regex-parsing
    freeform press-release text with no stable schema/versioning
    guarantee — fragile in a way the rest of this module's sources (NSE
    JSON, Yahoo chart JSON, FRED CSV) are not. GST Council's own gst.gov.in
    has no public statistics API. Not pursued: no structured endpoint
    found.

- auto_monthly_sales_growth:
    SIAM (siam.in) publishes monthly domestic sales figures. Live-tested
    (`curl -I https://www.siam.in/statistics.aspx?...`) returns HTTP 200
    but it is a server-rendered ASPX statistics dashboard (HTML tables,
    not JSON/CSV), and SIAM's terms of use do not grant a scraping
    license. No structured/licensed endpoint found.

- cement_dispatches_growth / power_consumption_growth: RESOLVED (2026-07-07
  follow-up). DPIIT's Office of the Economic Adviser publishes the "Index
  of Eight Core Industries" as a real downloadable .xlsx (not a PDF) at
  https://eaindustry.nic.in/eight_core_infra/Core_Industries_2011_12_<date>.xlsx
  (the exact filename's trailing date changes on each monthly refresh — the
  current link is discovered by scraping the eaindustry.nic.in homepage's
  "Eight Core Industries (ICI)" dropdown for its "Download Data (2011-12)"
  href, rather than hardcoding a filename that will 404 next month). Its
  "Growth (%)" sheet has a `Growth of Cement (%)` and `Growth of
  Electricity (%)` column per calendar month — live-verified real values
  through 2026-05-01. Used as: cement_dispatches_growth = Cement growth
  (ICI tracks cement production/dispatches as one series, standard proxy);
  power_consumption_growth = Electricity growth (ICI's Electricity index
  tracks power generation — the closest free monthly series to
  "consumption"; no separate consumption-only series is freely published).
  See download_core_industries_index()/fetch_cement_and_power_growth()
  below.

- rail_freight_growth:
    Indian Railways / Ministry of Railways publish freight loading figures
    via PIB press releases (same HTML/prose problem as GST) and annual
    reports; no monthly structured feed found.

- upi_transaction_growth:
    NPCI (npci.org.in) publishes UPI statistics on a web dashboard.
    Live-tested (`curl -I https://www.npci.org.in/statistics/upi-transaction-statistics`)
    returned HTTP 403 to a plain scripted client — the endpoint actively
    blocks non-browser requests. No accessible API found.

What would unblock each gap
----------------------------
- PMI: a paid S&P Global Market Intelligence subscription/license.
- IIP: a registered data.gov.in API key (free to obtain but requires
  account signup this environment cannot perform), still subject to the
  same ~45-day MOSPI release lag already modeled in
  features/real_economy_macro.py.
- Bank credit: RBI DBIE has no public API; would need either a
  Selenium/Playwright-driven export from the DBIE UI (fragile, likely
  against RBI's terms) or a commercial data vendor (e.g. Refinitiv,
  Bloomberg) that redistributes RBI credit data with an API.
- GST / rail freight: would need PIB press-release HTML scraping with a
  maintained regex/parser per release template, accepting the fragility
  documented above — a deliberate choice was made NOT to build this given
  the schema-stability bar the rest of this module holds to.
- Auto sales (SIAM), UPI (NPCI): each would need either a licensed data
  feed from the respective body, or a scraping approach that survives
  that site's bot-mitigation (NPCI actively blocks) and HTML-template
  changes (SIAM). Cement/power are resolved — see above.

Graceful degradation
---------------------
features/real_economy_macro.py already tolerates a completely absent
datastore/normalised/macro_real_economy.parquet (see its module docstring
and `load_real_economy_macro`): with no Parquet present, all 10 features
return NaN via `_MACRO_REAL_ECONOMY_PATH.exists()` returning False, and
`compute_real_economy_macro_panel` broadcasts those NaNs correctly per
ticker. No change was needed there — this module simply has nothing to
write yet, and that NaN behavior is the correct, honest state rather than
a bug. This module will be filled in as sources become genuinely
available (e.g. a data.gov.in key is obtained for IIP).
"""

import io
import logging
import re
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import requests

from config.settings import NORMALISED_DIR

logger = logging.getLogger(__name__)

_EAINDUSTRY_HOME = "https://eaindustry.nic.in/"
_EAINDUSTRY_BASE = "https://eaindustry.nic.in/"
_MAX_RETRIES = 3
_TIMEOUT_S = 20

_MACRO_REAL_ECONOMY_PATH = Path(NORMALISED_DIR) / "macro_real_economy.parquet"

# SPEC-PIPE-003: release-lag, mirrors features/real_economy_macro.py's
# _RELEASE_LAG_DAYS for these two series (kept here too so this module has
# no import-time dependency on the features layer — SPEC-SOLID-005).
_CEMENT_RELEASE_LAG_DAYS = 15
_POWER_RELEASE_LAG_DAYS = 7


def _find_ici_xlsx_url() -> str:
    """
    Scrape eaindustry.nic.in's homepage for the current "Index of Eight Core
    Industries" download link. The filename's trailing date changes on every
    monthly refresh (e.g. Core_Industries_2011_12_20260622.xlsx), so this
    must be discovered live each run rather than hardcoded — a hardcoded
    filename would silently 404 the month after being written.

    Raises
    ------
    ConnectionError
        If the homepage is unreachable or no matching link is found.
    """
    resp = requests.get(_EAINDUSTRY_HOME, timeout=_TIMEOUT_S)
    resp.raise_for_status()
    match = re.search(r'href="(eight_core_infra/Core_Industries_2011_12_[^"]+\.xlsx)"', resp.text)
    if not match:
        raise ConnectionError(
            "eaindustry.nic.in: could not find an ICI 'Download Data (2011-12)' "
            "link on the homepage — page structure may have changed"
        )
    return _EAINDUSTRY_BASE + match.group(1)


def download_core_industries_index(date: str) -> pd.DataFrame:
    """
    Download DPIIT/Office of the Economic Adviser's Index of Eight Core
    Industries (ICI) "Growth (%)" sheet — real, monthly YoY growth for
    Coal/Crude Oil/Natural Gas/Refinery Products/Fertilizers/Steel/Cement/
    Electricity.

    Parameters
    ----------
    date : str
        "YYYY-MM-DD" — only used for the retry/error-message context;
        the source has no date-range query parameter, it always serves
        its full history in one file.

    Returns
    -------
    pd.DataFrame
        Columns: month_end (Timestamp, first-of-month), cement_growth_pct,
        electricity_growth_pct. Only genuine single-calendar-month rows are
        kept — the workbook also carries annual ("2024-25(Apr-Mar)") and
        year-to-date ("2026-27(Apr-May)") aggregate rows under the same
        column, which are NOT growth-rate-comparable to a single month and
        are filtered out here (their first cell is a string label, not a
        real Timestamp, so `pd.to_datetime(..., errors="coerce")` naturally
        drops them).

    Spec References
    ----------------
    SPEC-PIPE-006: retry up to _MAX_RETRIES times; SPEC-FEAT-001: source
    has no fabricated fallback — either real data or an exception.

    Raises
    ------
    ConnectionError
        If the homepage/file fetch fails after retries, or the expected
        sheet/columns aren't present (source format changed).
    """
    last_exc: Optional[Exception] = None
    for attempt in range(_MAX_RETRIES):
        try:
            url = _find_ici_xlsx_url()
            resp = requests.get(url, timeout=_TIMEOUT_S)
            resp.raise_for_status()
            wb = pd.ExcelFile(io.BytesIO(resp.content))
            if "Growth (%)" not in wb.sheet_names:
                raise ConnectionError(
                    f"eaindustry.nic.in ICI workbook: 'Growth (%)' sheet not found "
                    f"(sheets present: {wb.sheet_names}) — source format may have changed"
                )
            raw = wb.parse("Growth (%)")
            raw = raw.rename(columns=lambda c: str(c).strip())
            cement_col = next((c for c in raw.columns if "Cement" in c), None)
            electricity_col = next((c for c in raw.columns if "Electricity" in c), None)
            month_col = raw.columns[0]
            if cement_col is None or electricity_col is None:
                raise ConnectionError(
                    "eaindustry.nic.in ICI workbook: Cement/Electricity growth "
                    f"columns not found (columns present: {list(raw.columns)})"
                )

            out = raw[[month_col, cement_col, electricity_col]].copy()
            out.columns = ["month_end", "cement_growth_pct", "electricity_growth_pct"]
            # Real single-month rows have a real Timestamp in month_col; the
            # annual/YTD aggregate rows ("2024-25(Apr-Mar)") are strings and
            # become NaT here — dropped, not a real month.
            out["month_end"] = pd.to_datetime(out["month_end"], errors="coerce")
            out = out.dropna(subset=["month_end", "cement_growth_pct", "electricity_growth_pct"])
            if out.empty:
                raise ConnectionError("eaindustry.nic.in ICI workbook: no valid monthly rows parsed")
            return out.reset_index(drop=True)
        except (requests.RequestException, ConnectionError) as exc:
            last_exc = exc
            logger.warning(f"download_core_industries_index attempt {attempt + 1}/{_MAX_RETRIES} failed: {exc}")
    raise ConnectionError(f"Failed to download ICI index after {_MAX_RETRIES} attempts: {last_exc}")


def fetch_cement_and_power_growth(date: str) -> Dict[str, Optional[Dict]]:
    """
    Fetch the latest real cement/electricity(power-proxy) YoY growth
    reading as of `date` and shape it for macro_real_economy.parquet's
    long-format schema (SPEC-PIPE-003 PIT: reference_month_end +
    availability_date, consumed by features/real_economy_macro.py's
    load_real_economy_macro via `availability_date <= as_of`).

    Parameters
    ----------
    date : str
        "YYYY-MM-DD" — the as-of date; only months whose
        month_end + release_lag <= date are eligible.

    Returns
    -------
    dict
        {'cement_dispatches_growth': {'reference_month_end': Timestamp,
         'value': float, 'availability_date': Timestamp} or None,
         'power_consumption_growth': {...} or None} — None for a series
        if no eligible month exists yet as of `date` (e.g. this month's
        release hasn't happened), not fabricated.

    Raises
    ------
    ConnectionError
        Propagated from download_core_industries_index if the source is
        unreachable — this function does not silently swallow that, the
        caller (scheduler step) decides fallback behavior.
    """
    as_of = pd.Timestamp(date)
    idx = download_core_industries_index(date)
    idx = idx.sort_values("month_end")

    result: Dict[str, Optional[Dict]] = {"cement_dispatches_growth": None, "power_consumption_growth": None}
    for feature_name, value_col, lag_days in (
        ("cement_dispatches_growth", "cement_growth_pct", _CEMENT_RELEASE_LAG_DAYS),
        ("power_consumption_growth", "electricity_growth_pct", _POWER_RELEASE_LAG_DAYS),
    ):
        idx["availability_date"] = idx["month_end"] + pd.offsets.MonthEnd(0) + pd.Timedelta(days=lag_days)
        eligible = idx[idx["availability_date"] <= as_of]
        if eligible.empty:
            continue
        latest = eligible.iloc[-1]
        result[feature_name] = {
            "reference_month_end": latest["month_end"] + pd.offsets.MonthEnd(0),
            "value": float(latest[value_col]),
            "availability_date": latest["availability_date"],
        }
    return result


def upsert_macro_real_economy_parquet(date: str) -> int:
    """
    Fetch cement/power growth for `date` and upsert into
    macro_real_economy.parquet (append-and-dedupe on (feature_name,
    reference_month_end), keeping the file idempotent across repeated runs
    for the same month — matches this codebase's checkpoint-resume
    philosophy of "safe to re-run".

    Returns
    -------
    int
        Number of (feature_name, reference_month_end) rows newly written
        or updated (0 if nothing new/eligible this call).

    Raises
    ------
    ConnectionError
        Propagated from fetch_cement_and_power_growth.
    """
    fetched = fetch_cement_and_power_growth(date)
    new_rows = [
        {"feature_name": name, **payload} for name, payload in fetched.items() if payload is not None
    ]
    if not new_rows:
        return 0
    new_df = pd.DataFrame(new_rows)

    _MACRO_REAL_ECONOMY_PATH.parent.mkdir(parents=True, exist_ok=True)
    if _MACRO_REAL_ECONOMY_PATH.exists():
        existing = pd.read_parquet(_MACRO_REAL_ECONOMY_PATH)
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["feature_name", "reference_month_end"], keep="last")
    else:
        combined = new_df
    combined.to_parquet(_MACRO_REAL_ECONOMY_PATH, index=False)
    return len(new_rows)
