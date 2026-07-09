"""
features/real_economy_macro.py

Phase: 3.1 (Real Economy Macro Features)
Specs: SPEC-FEAT-001, SPEC-PIPE-003 (CRITICAL), SPEC-PIPE-006
Owner: Platform / Features
Consumers: features/matrix_builder, systems/ml_signal_engine

Computes 10 real-economy macro indicators, all at monthly frequency
forward-filled to daily:
  gst_collection_growth, pmi_manufacturing, pmi_services, iip_growth,
  auto_monthly_sales_growth, cement_dispatches_growth,
  power_consumption_growth, rail_freight_growth, upi_transaction_growth,
  bank_credit_growth

PIT Assumptions (SPEC-PIPE-003 CRITICAL)
-----------------------------------------
Each monthly indicator has a data-release lag before it is publicly available:
  - GST: released ~25th of following month
  - PMI: released 1st business day of following month
  - IIP: released ~12th of 2nd following month (i.e., 6-week lag)
  - Auto sales: released 1st day of following month by SIAM
  - Cement dispatches: released ~15th of following month
  - Power consumption: released ~5th of following month (CEA)
  - Rail freight: released ~7th of following month
  - UPI transactions: released ~3rd of following month (NPCI)
  - Bank credit: released biweekly by RBI (approx 15-day lag from reference fortnight)

No future monthly release is ever consumed on a date before its `availability_date`.
`availability_date` is stored alongside each row in the macro indicator store.
Features are NaN — NOT forward-filled — if no month's data is yet available
for the current feature_date.

Storage
-------
Real-economy macro data is read from
`datastore/normalised/macro_real_economy.parquet` (or
`datastore/normalised/macro_indicators.db`). This module ships with a
lightweight stub-loader; the daily pipeline writes real data via
ingestion/scrapers/macro_real_economy.py (a Phase 3 deliverable).
When the Parquet is absent or empty, all features return NaN.

Source research (2026-07-07) — no scraper built yet
-----------------------------------------------------
All 10 features above are currently 100% NaN because
`ingestion/scrapers/macro_real_economy.py` does not exist. Live research
was done into a free, programmatically-fetchable source for each of the
10 series before concluding this. Findings, so this isn't re-litigated
every session:

- FRED (`fredgraph.csv`, same pattern already used by
  `ingestion/scrapers/macro.py` for yield_10yr/yield_3m) was checked
  first, since it already has working plumbing in this repo. Its India
  industrial-production series (`INDPROINDMISMEI`, OECD MEI via FRED)
  is real but **dead** — last observation 2023-01-01, over 3 years
  stale as of 2026-07 — not usable for iip_growth; nothing else in
  FRED's India tag set covers GST, auto sales, cement, power, rail
  freight, UPI, or bank credit growth at a useful frequency.
- `data.gov.in` (Open Government Data Platform India) hosts an RBI
  bank-credit dataset (API id `e26d8bcd-4f3a-4bea-aa24-fe95a1012440`)
  behind `api.data.gov.in/resource/<id>?api-key=...`. This requires a
  user-specific registered API key (free, but a manual signup step);
  the commonly-referenced public tutorial demo key
  (`579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571`) returned
  `{"error": "Key not authorised"}` when tried live. No
  `DATA_GOV_IN_API_KEY` exists in this repo's config/.env. This is a
  legitimate free path for bank_credit_growth (and possibly others)
  once a project owner registers for a key — tracked as a blocked
  prerequisite, not a "no source exists" gap.
- RBI DBIE (`dbie.rbi.org.in` / `data.rbi.org.in/DBIE/`) is the
  authoritative bank-credit/sectoral-deployment source but is a
  session-based BI portal (Excel/CSV export via UI), not a stable
  unauthenticated REST/CSV endpoint — same class of blocker
  `ingestion/scrapers/macro.py`'s docstring already documents for the
  2yr G-Sec yield.
- GST collections are published monthly only as PIB press releases and
  as GST-Council PDF tables (e.g.
  `tutorial.gst.gov.in/downloads/news/*.pdf`) — no JSON/CSV endpoint
  found.
- PMI (manufacturing/services) is commercially licensed by S&P
  Global/HSBC; there is no free official series. Confirmed no
  free alternative exists.
- Auto sales: SIAM (siam.in) publishes a monthly Flash Report but its
  publications page states prior written permission is required to
  share/publish its statistics; no CSV/API endpoint found.
- Cement dispatches: no free official series found (industry data is
  sold by CMA India / commercial research firms).
- Power consumption: CEA (cea.nic.in) advertises "API for Central
  Electricity Authority Data" but the page did not return a stable,
  documented endpoint on live fetch (connection reset / no parseable
  links); POSOCO/Grid-India's daily reports are PDF. Worth revisiting
  with more time, but not verified live as of this writing.
- Rail freight: Indian Railways publishes freight-loading figures only
  via PIB press releases, no API/CSV found.
- UPI transactions: NPCI's statistics page
  (`npci.org.in/what-we-do/upi/product-statistics`) returned HTTP 403
  to a non-browser client; no alternative JSON/CSV endpoint found.

Net result: none of the 10 series has a verified, currently-working,
free, unauthenticated JSON/CSV endpoint. `ingestion/scrapers/macro_real_
economy.py` was therefore NOT created — building it would mean either
fabricating values or shipping dead code with no reachable data source,
both of which are excluded by this project's no-synthetic-data policy.
The most promising unblocked path is `data.gov.in`'s bank-credit
dataset once a project owner obtains a free registered API key; PMI is
the one series with no free path even in principle (commercial
licensing).
"""

import logging
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

from config.settings import NORMALISED_DIR as NORMALISED_DATASTORE_DIR

logger = logging.getLogger(__name__)

REAL_ECONOMY_MACRO_FEATURES: List[str] = [
    "gst_collection_growth",
    "pmi_manufacturing",
    "pmi_services",
    "iip_growth",
    "auto_monthly_sales_growth",
    "cement_dispatches_growth",
    "power_consumption_growth",
    "rail_freight_growth",
    "upi_transaction_growth",
    "bank_credit_growth",
]

# Release-lag in calendar days from the END of the reference month.
# Feature is only available on `month_end + lag` or later.
_RELEASE_LAG_DAYS: Dict[str, int] = {
    "gst_collection_growth": 25,
    "pmi_manufacturing": 3,       # 1st business day ≈ day 3
    "pmi_services": 5,
    "iip_growth": 45,             # 12th of 2nd following month ≈ ~45 days
    "auto_monthly_sales_growth": 2,
    "cement_dispatches_growth": 15,
    "power_consumption_growth": 7,
    "rail_freight_growth": 8,
    "upi_transaction_growth": 5,
    "bank_credit_growth": 20,     # biweekly; conservative 20-day lag
}

# Parquet path for real-economy macro data
_MACRO_REAL_ECONOMY_PATH = Path(NORMALISED_DATASTORE_DIR) / "macro_real_economy.parquet"


def _compute_availability_date(reference_month_end: pd.Timestamp, feature_name: str) -> pd.Timestamp:
    """Return the calendar date on which `feature_name` for that month becomes available."""
    lag = _RELEASE_LAG_DAYS.get(feature_name, 30)
    return reference_month_end + pd.Timedelta(days=lag)


def load_real_economy_macro(as_of: pd.Timestamp) -> pd.Series:
    """
    Load the latest available reading for each real-economy macro feature as of `as_of`.

    Parameters
    ----------
    as_of : pd.Timestamp
        The feature date. Only data whose availability_date <= as_of is used
        (SPEC-PIPE-003).

    Returns
    -------
    pd.Series
        Index = REAL_ECONOMY_MACRO_FEATURES, values = latest PIT-valid reading
        or np.nan if none available.

    Spec References
    ---------------
    SPEC-PIPE-003 (CRITICAL): availability_date used, never month_end_date.
    SPEC-PIPE-006: NaN returned (not fallback fabrication) if data unavailable.
    """
    result = pd.Series(np.nan, index=REAL_ECONOMY_MACRO_FEATURES)

    if not _MACRO_REAL_ECONOMY_PATH.exists():
        logger.debug(
            "macro_real_economy.parquet not found — real-economy features will be NaN; "
            "run ingestion/scrapers/macro_real_economy.py to populate"
        )
        return result

    try:
        df = pd.read_parquet(_MACRO_REAL_ECONOMY_PATH)
    except Exception as exc:
        logger.warning(f"Failed to read macro_real_economy.parquet: {exc}")
        return result

    if df.empty:
        return result

    required_cols = {"reference_month_end", "feature_name", "value", "availability_date"}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        logger.warning(f"macro_real_economy.parquet missing columns: {missing}")
        return result

    df["reference_month_end"] = pd.to_datetime(df["reference_month_end"])
    df["availability_date"] = pd.to_datetime(df["availability_date"])

    # SPEC-PIPE-003: never use rows not yet available on as_of
    available = df[df["availability_date"] <= as_of]

    for feature in REAL_ECONOMY_MACRO_FEATURES:
        rows = available[available["feature_name"] == feature]
        if rows.empty:
            continue
        # Take the most recent available reading (latest reference_month_end)
        latest = rows.sort_values("reference_month_end").iloc[-1]
        result[feature] = latest["value"]

    return result


def compute_real_economy_macro_row(as_of: pd.Timestamp) -> pd.DataFrame:
    """
    Return a one-row DataFrame with all real-economy macro features for `as_of`.

    Parameters
    ----------
    as_of : pd.Timestamp

    Returns
    -------
    pd.DataFrame
        One row; columns: date + REAL_ECONOMY_MACRO_FEATURES.

    Spec References
    ---------------
    SPEC-PIPE-003: PIT enforcement via availability_date (see load_real_economy_macro).
    SPEC-FEAT-001: NaN returned — never imputed or fabricated — when no data available.
    """
    values = load_real_economy_macro(as_of)
    row = {"date": as_of}
    row.update(values.to_dict())
    return pd.DataFrame([row])


def compute_real_economy_macro_panel(as_of: pd.Timestamp, tickers: List[str]) -> pd.DataFrame:
    """
    Broadcast real-economy macro features to all tickers on `as_of`.

    Macro features are the same for every stock on a given date (they are
    economy-wide indicators). This function expands the single-row result
    into one row per ticker so matrix_builder can merge on `ticker`.

    Parameters
    ----------
    as_of : pd.Timestamp
    tickers : list of str

    Returns
    -------
    pd.DataFrame
        Columns: ticker + REAL_ECONOMY_MACRO_FEATURES; one row per ticker.
    """
    values = load_real_economy_macro(as_of)
    rows = []
    for ticker in tickers:
        row = {"ticker": ticker}
        row.update(values.to_dict())
        rows.append(row)
    return pd.DataFrame(rows)
