"""
features/deep_forensic.py

Phase: 3.1 (Deep Forensic ML Features — Groups D–I)
Specs: SPEC-MODEL-010, SPEC-PIPE-003 (CRITICAL), SPEC-PIPE-004, SPEC-SOLID-005
Owner: Platform / Features
Consumers: features/matrix_builder, systems/ml_signal_engine/models/forensic/forensic_ml

Computes 28 deep forensic features across Groups D–I from the build prompt:

  Group D — Balance Sheet Quality (12):
    goodwill_ratio, cwip_ratio, contingent_liability_ratio, subsidiary_count,
    loans_to_related, capex_to_assets, intangibles_growth, off_balance_sheet_proxy,
    noncash_assets_ratio, asset_quality_score, balance_sheet_manipulation_score,
    asset_inflation_flag

  Group E — Governance & Promoter Risk (8):
    salary_to_pat, rpt_intensity, audit_qualification_flag, auditor_change_flag,
    cfo_tenure_months, board_independence, director_resignation_count_4q,
    whistle_blower_policy

  Groups F–I — Cross-Validation (8):
    benford_mad, altman_z, interest_coverage_trend, pledge_spiral_risk,
    gst_revenue_divergence, peer_outlier_score, tax_rate_anomaly, insider_selling_flag

Total: 28 features. Groups A–C (Phase 2.5) live in features/forensic_classical.py.
These groups extend the forensic ML ensemble's feature set for SPEC-MODEL-010.

PIT Assumptions (SPEC-PIPE-003 CRITICAL)
-----------------------------------------
All fundamental data accessed via DataStoreClient.get_fundamentals_history()
which filters on announcement_date <= as_of. Shareholding data uses
filing_date <= as_of. No quarter_end_date used as a join key.

Real-data availability audit (2026-07-07)
------------------------------------------
Of Group D's 12 columns and Group E's 8 columns, the underlying
`fundamentals` raw fields were confirmed 100%-NaN because the schema never
had the columns at all (not a parsing bug). Investigated against a real
live screener.in fetch (ingestion/scrapers/screener.py) plus Tijori and
NSE corporate-actions sourcing patterns already used elsewhere in this repo:

- NOW REAL (schema + scraper added this session): `total_assets`, `cwip`
  are genuine labeled rows in Screener's free-tier #balance-sheet table
  (verified live against TCS). This makes `cwip_ratio` and
  `asset_inflation_flag` real, non-fabricated values wherever Screener has
  data. `asset_quality_score`/`balance_sheet_manipulation_score` partially
  improve (their goodwill_ratio/noncash_assets_ratio/current_assets
  components remain NaN — see below).
- A54 correction (2026-07-10): the paragraph below was accurate against
  screener.py alone but predates ingestion/scrapers/nse_xbrl_financials.py's
  real "Integrated Filing — IndAS" parser, which populates several of these
  columns from structured NSE data. Now REAL, non-fabricated, partial
  coverage: `goodwill_ratio` (`goodwill`), `capex_to_assets` (`capex`, ~65%
  coverage), `noncash_assets_ratio` (`current_assets`/`cash_and_equivalents`),
  `intangibles_growth` (`intangible_assets`, 5,760/36,346 rows — this
  function previously read the wrong key, "intangibles" instead of
  "intangible_assets", and always returned NaN regardless of data
  availability; fixed same session), and Group E's `audit_qualification_flag`
  (`audit_qualified_flag`, populated by `_parse_audit_qualification` from the
  real structured "Details of Impact of Audit Qualification" section,
  5,013/36,346 rows). These five should NOT be treated as structurally
  sparse/allowlisted — their null rate is expected to keep improving as more
  filings are scraped, same as any other partial-coverage NSE XBRL field.
- STILL GENUINELY UNAVAILABLE (confirmed via direct inspection of the raw
  NSE XBRL filing cache, not just a live Screener grep): `contingent_liabilities`
  is only mentioned in unstructured prose (1.2% of filings, no consistent
  regex-extractable phrasing) — no schema column exists. `subsidiary_count`,
  `loans_to_related_parties` — also no schema column, no structured source
  found. So `contingent_liability_ratio`, `subsidiary_count`,
  `loans_to_related`, `off_balance_sheet_proxy` stay NaN until real
  NLP-extraction work is scoped (tracked separately, out of scope here) —
  not fabricated.
- Group E's remaining 7 columns (all but `audit_qualification_flag`, see
  above) are ENTIRELY unavailable from any free structured source
  investigated: `salary_to_pat`/`rpt_intensity` need
  director_remuneration/related_party_transactions, which Screener only
  exposes via an "Experimental", Premium-gated, per-company-variable-schema
  RPT modal that still blanks the most recent 1-2 FYs (see screener.py's
  docstring) — too unreliable to auto-aggregate. `auditor_change_flag`,
  `cfo_tenure_months`, `board_independence`, `director_resignation_count_4q`,
  `whistle_blower_policy` are corporate-governance-report/annual-report/
  MCA21 data — grepped for on a live Screener page (zero matches: no
  "auditor", "whistle", "independent director", or "remuneration" text
  anywhere) and not published as structured (non-PDF) data on any NSE/BSE
  endpoint this repo's ingestion/scrapers/corporate_actions.py-style
  authenticated session can reach. These 7 Group E columns remain NaN —
  real gap, not fabricated.

Cluster E.2 follow-up (2026-07-07)
------------------------------------
- `total_assets`/`cwip` (added to the schema by an earlier session this
  same day) had NEVER actually been backfilled into the real DB — a fresh
  `SELECT COUNT(*) FROM fundamentals WHERE total_assets IS NOT NULL` was 0
  despite ~3,300 cached raw Screener pages already having the real data on
  them (the parsing only fires on a fresh live scrape going forward, and
  none had run since). Added scripts/backfill_balance_sheet_from_screener.py
  and ran it from the existing cache (no network call): total_assets/cwip
  are now real, populated for 2,308/27,176 rows (8.5% — one current-quarter
  snapshot per ticker, not full history). This makes `cwip_ratio` and
  `asset_inflation_flag` produce real non-NaN values for the first time
  (live-verified: TCS's 2026-03-31 row now gives cwip_ratio = 2665/181167
  = 0.0147, matching the real Screener page).
- `altman_z` was WIRED but had real bugs that made it always NaN regardless
  of data availability: it looked up `market_cap`/`book_equity`, neither a
  real fundamentals column, and had no derivation for
  `working_capital`/`total_liabilities`/`retained_earnings`. Fixed:
  `total_liabilities` derives from `total_assets - total_equity` (both real
  columns, confirmed via `book_equity` never matching the real
  `total_equity` column name); `retained_earnings` is now a real column
  (Screener's "Reserves" row, kept separate from `total_equity` instead of
  only summed into it — see datastore/schema/create_normalised.py and
  scripts/backfill_equity_from_screener.py, backfilled for real: 6,519
  (ticker, fiscal_year) groups patched from the existing cache).
  `altman_z` as a WHOLE still resolves to NaN in every real case today: (1)
  `working_capital` needs `current_assets`/`current_liabilities`, real
  schema columns but ALWAYS NULL from Screener's free tier (confirmed —
  see screener.py's module docstring); (2) no real, PIT-correct market-cap
  column exists anywhere `fundamentals` — `market_cap_cr` lives only on
  `stock_master` (current snapshot, not historical PIT data) and this
  function's inputs (fundamentals + shareholding history only) have no
  price series to derive one, unlike features/fundamental.py's
  ev_to_ebitda which is handed a `close` price series. Left NaN rather than
  fabricated from a non-PIT current price. Not a regression — previously
  silently-wrong-always-NaN from field-name typos, now correctly-NaN for a
  documented, real data gap; the fix is real even though the net output is
  unchanged today.
- `peer_outlier_score`, `tax_rate_anomaly` were already correctly wired
  against real columns (`roe`, `tax_expense`, `pbt`) — verified by
  re-reading and a live DB query, no bug found.
- `insider_selling_flag` had a real bug: it read `fund_df["promoter_pct"]`
  (the `fundamentals` table), but `promoter_pct` only exists on
  `shareholding` — so `"promoter_pct" in fund_df.columns` was always False
  and this feature was always NaN regardless of real data availability.
  Fixed to read from the same shareholding rows already fetched for
  pledge_spiral_risk. `pledge_spiral_risk` separately looked up
  "promoter_pledge_pct" instead of the real "promoter_pledge" column name
  — fixed too, though `promoter_pledge` is itself always None (see below),
  so this fix alone doesn't unblock it.
- `pledge_spiral_risk` depends on `shareholding.promoter_pledge`, which is
  always None — see ingestion/scrapers/screener.py's
  `_build_shareholding_row` docstring for the live investigation into NSE's
  pledge-disclosure page (no discoverable public JSON API despite testing
  the adjacent, working `api/CorpInfo?corpType=sast` endpoint pattern
  against a company with a real, currently-disclosed 51.82% pledge).
  Genuinely blocked, not fabricated.
"""

import logging
import math
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from datastore.client import DataStoreClient
from features.fundamental import _latest_close_on_or_before
from systems.damodaran_valuation.lifecycle.classifier import _FINANCIAL_SERVICES_SECTORS

logger = logging.getLogger(__name__)

# ── Feature catalog ──────────────────────────────────────────────────────────

GROUP_D_FEATURES: List[str] = [
    "goodwill_ratio",
    "cwip_ratio",
    "contingent_liability_ratio",
    "subsidiary_count",
    "loans_to_related",
    "capex_to_assets",
    "intangibles_growth",
    "off_balance_sheet_proxy",
    "noncash_assets_ratio",
    "asset_quality_score",
    "balance_sheet_manipulation_score",
    "asset_inflation_flag",
]

GROUP_E_FEATURES: List[str] = [
    "salary_to_pat",
    "rpt_intensity",
    "audit_qualification_flag",
    "auditor_change_flag",
    "cfo_tenure_months",
    "board_independence",
    "director_resignation_count_4q",
    "whistle_blower_policy",
]

GROUP_FI_FEATURES: List[str] = [
    "benford_mad",
    "altman_z",
    "interest_coverage_trend",
    "pledge_spiral_risk",
    "gst_revenue_divergence",
    "peer_outlier_score",
    "tax_rate_anomaly",
    "insider_selling_flag",
]

DEEP_FORENSIC_FEATURES: List[str] = GROUP_D_FEATURES + GROUP_E_FEATURES + GROUP_FI_FEATURES


# ── Benford's Law helper ──────────────────────────────────────────────────────


def _benford_expected() -> np.ndarray:
    """Expected first-digit frequencies per Benford's Law (digits 1–9)."""
    return np.array([math.log10(1 + 1 / d) for d in range(1, 10)])


def _benford_mad(values: np.ndarray) -> float:
    """
    Mean Absolute Deviation from Benford's Law for leading digit distribution.

    Lower MAD = more Benford-compliant (natural-looking data).
    Higher MAD (> 0.015) suggests potential manipulation.
    """
    valid = values[~np.isnan(values)]
    valid = np.abs(valid[valid != 0])
    if len(valid) < 10:
        return np.nan
    try:
        leading_digits = []
        for v in valid:
            s = f"{v:.6e}"
            for ch in s:
                if ch.isdigit() and ch != "0":
                    leading_digits.append(int(ch))
                    break
        if not leading_digits:
            return np.nan
        observed = np.zeros(9)
        for d in leading_digits:
            if 1 <= d <= 9:
                observed[d - 1] += 1
        observed /= observed.sum() + 1e-10
        expected = _benford_expected()
        return float(np.mean(np.abs(observed - expected)))
    except Exception:
        return np.nan


# ── Altman Z-Score ────────────────────────────────────────────────────────────


def _altman_z(
    working_capital: float,
    retained_earnings: float,
    ebit: float,
    total_assets: float,
    total_liabilities: float,
    revenue: float,
    market_cap: float,
) -> float:
    """
    Altman Z-Score (public company modified version).

    Z = 1.2*X1 + 1.4*X2 + 3.3*X3 + 0.6*X4 + 1.0*X5
    X1 = working_capital / total_assets
    X2 = retained_earnings / total_assets
    X3 = ebit / total_assets
    X4 = market_cap / total_liabilities
    X5 = revenue / total_assets

    Interpretation: Z < 1.81 = distress, 1.81–2.99 = grey zone, > 2.99 = safe
    (SPEC-MODEL-009)
    """
    if any(v is None or (isinstance(v, float) and math.isnan(v)) for v in
           [working_capital, retained_earnings, ebit, total_assets, total_liabilities, revenue, market_cap]):
        return np.nan
    if abs(total_assets) < 1e-6:
        return np.nan
    # total_liabilities <= 0 is either a data error or (when derived as
    # total_assets - total_equity, see caller) a negative-equity company —
    # X4 = market_cap / total_liabilities is undefined/meaningless in
    # either case. Previously `abs(total_liabilities)` silently flipped
    # the sign, producing a plausible-looking but wrong Z-score instead of
    # flagging the input as unusable (2026-07-19 full-codebase-review).
    if total_liabilities <= 0:
        return np.nan
    try:
        x1 = working_capital / total_assets
        x2 = retained_earnings / total_assets
        x3 = ebit / total_assets
        x4 = market_cap / total_liabilities
        x5 = revenue / total_assets
        z = 1.2 * x1 + 1.4 * x2 + 3.3 * x3 + 0.6 * x4 + 1.0 * x5
        return float(z)
    except Exception:
        return np.nan


# ── Peer outlier score ────────────────────────────────────────────────────────


def _peer_outlier_z(value: float, peer_values: np.ndarray) -> float:
    """Z-score of a value within its peer group. Returns NaN if < 3 peers."""
    valid = peer_values[~np.isnan(peer_values)]
    if len(valid) < 3:
        return np.nan
    mean = valid.mean()
    std = valid.std(ddof=0) + 1e-10
    return float((value - mean) / std)


# ── Per-ticker computation ────────────────────────────────────────────────────


def _nan_dict() -> Dict[str, Any]:
    return {f: np.nan for f in DEEP_FORENSIC_FEATURES}


def compute_deep_forensic_features(
    client: DataStoreClient,
    ticker: str,
    as_of: datetime,
    lookback_years: int = 3,
    sector_fundamentals: Optional[pd.DataFrame] = None,
    pre_loaded_fundamentals=None,
    pre_loaded_shareholding=None,
    ticker_ohlcv: "Optional[pd.DataFrame]" = None,
    sector: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Compute all 28 deep forensic features for one ticker.

    Parameters
    ----------
    client : DataStoreClient
        SPEC-SOLID-005: all data access via DataStore API.
    ticker : str
    as_of : datetime
        PIT reference date (SPEC-PIPE-003).
    lookback_years : int
        History window in years for trend and Benford calculations.
    sector_fundamentals : pd.DataFrame, optional
        Pre-fetched sector-wide fundamental rows for peer_outlier_score.
        If None, peer_outlier_score returns NaN.
    sector : str, optional
        Real sector taxonomy string (e.g. from config.universe/hybrid_compute's
        ticker->sector map). When it matches
        systems.damodaran_valuation.lifecycle.classifier._FINANCIAL_SERVICES_SECTORS,
        altman_z is forced to NaN rather than computed — Altman Z's
        liabilities/working-capital ratios are structurally invalid for
        banks/NBFCs/insurers (deposits/borrowings are core operating
        liabilities, not distress leverage), not just a division-by-zero
        edge case (2026-07-19 full-codebase-review). None (default)
        preserves prior behavior — always compute altman_z.

    Returns
    -------
    dict
        Keys = DEEP_FORENSIC_FEATURES, values = float or np.nan.

    PIT Assumptions
    ---------------
    All rows from get_fundamentals_history() and get_shareholding_history()
    are already filtered server-side with announcement_date/filing_date <= as_of.
    """
    result = _nan_dict()

    try:
        fund_rows = (
            pre_loaded_fundamentals if pre_loaded_fundamentals is not None
            else client.get_fundamentals_history(ticker, as_of, lookback_years=lookback_years)
        )
    except Exception as exc:
        logger.debug(f"fundamentals fetch failed for {ticker}: {exc}")
        return result

    if not fund_rows:
        return result

    fund_df = pd.DataFrame(fund_rows)
    fund_df = fund_df.sort_values("quarter_end_date", ascending=True)

    # Latest quarter
    latest = fund_df.iloc[-1] if not fund_df.empty else None
    if latest is None:
        return result

    def _get(row, *keys, default=np.nan):
        for k in keys:
            v = row.get(k) if isinstance(row, dict) else getattr(row, k, None)
            if v is not None and not (isinstance(v, float) and math.isnan(v)):
                return float(v)
        return default

    # ── Group D — Balance Sheet Quality ──────────────────────────────────────

    total_assets = _get(latest, "total_assets")
    goodwill = _get(latest, "goodwill")
    cwip = _get(latest, "cwip")  # Capital Work in Progress
    contingent_liabilities = _get(latest, "contingent_liabilities")
    subsidiaries = _get(latest, "subsidiary_count")
    loans_related = _get(latest, "loans_to_related_parties")
    capex = _get(latest, "capex")
    # A54: schema/NSE-XBRL column is "intangible_assets", not "intangibles" —
    # this key was wrong, so intangibles_growth was always NaN even though
    # 5,760/36,346 rows have real data. Fixed the lookup key only, no schema change.
    intangibles = _get(latest, "intangible_assets")
    current_assets = _get(latest, "current_assets")
    cash_equivalents = _get(latest, "cash_and_equivalents")

    if not np.isnan(total_assets) and total_assets > 0:
        if not np.isnan(goodwill):
            result["goodwill_ratio"] = goodwill / total_assets
        if not np.isnan(cwip):
            result["cwip_ratio"] = cwip / total_assets
        if not np.isnan(contingent_liabilities):
            result["contingent_liability_ratio"] = contingent_liabilities / total_assets
        if not np.isnan(loans_related):
            result["loans_to_related"] = loans_related / total_assets
        if not np.isnan(capex):
            result["capex_to_assets"] = capex / total_assets
        if not np.isnan(current_assets) and not np.isnan(cash_equivalents):
            noncash = current_assets - cash_equivalents
            result["noncash_assets_ratio"] = noncash / total_assets

    result["subsidiary_count"] = subsidiaries

    # Intangibles growth YoY
    if len(fund_df) >= 5:
        yr_ago = fund_df.iloc[-5]  # approximately 1 year ago (4 quarters)
        intangibles_prior = _get(yr_ago, "intangible_assets")
        if not np.isnan(intangibles) and not np.isnan(intangibles_prior) and intangibles_prior > 0:
            result["intangibles_growth"] = (intangibles - intangibles_prior) / intangibles_prior

    # Off-balance-sheet proxy: contingent liabilities / (total_assets + contingent_liabilities)
    if not np.isnan(contingent_liabilities) and not np.isnan(total_assets):
        denom = total_assets + contingent_liabilities + 1e-6
        result["off_balance_sheet_proxy"] = contingent_liabilities / denom

    # Asset quality score: composite of multiple balance sheet flags (0 = poor, 1 = clean)
    aqs_components = []
    if not np.isnan(result.get("goodwill_ratio", np.nan)):
        aqs_components.append(max(0.0, 1.0 - result["goodwill_ratio"] * 5))
    if not np.isnan(result.get("cwip_ratio", np.nan)):
        aqs_components.append(max(0.0, 1.0 - result["cwip_ratio"] * 3))
    if not np.isnan(result.get("noncash_assets_ratio", np.nan)):
        aqs_components.append(max(0.0, 1.0 - result["noncash_assets_ratio"] * 2))
    result["asset_quality_score"] = float(np.mean(aqs_components)) if aqs_components else np.nan

    # Balance sheet manipulation score: Beneish AQI proxy from consecutive quarters
    if len(fund_df) >= 5:
        yr_ago = fund_df.iloc[-5]
        ta_now = total_assets
        ta_prior = _get(yr_ago, "total_assets")
        revenue_now = _get(latest, "revenue")
        revenue_prior = _get(yr_ago, "revenue")
        ca_now = _get(latest, "current_assets")
        ca_prior = _get(yr_ago, "current_assets")
        if all(not np.isnan(v) and v > 0 for v in [ta_now, ta_prior, revenue_now, revenue_prior, ca_now, ca_prior]):
            # AQI = (1 - (current_assets/total_assets)_t) / (1 - (current_assets/total_assets)_{t-1})
            aqr_now = 1 - ca_now / ta_now
            aqr_prior = 1 - ca_prior / ta_prior
            aqi = aqr_now / (aqr_prior + 1e-10)
            result["balance_sheet_manipulation_score"] = float(np.clip(aqi - 1.0, -1.0, 3.0))

    # Asset inflation flag: total assets grew significantly faster than revenue (CAGR comparison)
    if len(fund_df) >= 5:
        yr_ago = fund_df.iloc[-5]
        ta_prior = _get(yr_ago, "total_assets")
        rev_now = _get(latest, "revenue")
        rev_prior = _get(yr_ago, "revenue")
        ta_now = total_assets
        if all(not np.isnan(v) and v > 0 for v in [ta_prior, ta_now, rev_now, rev_prior]):
            asset_growth = ta_now / ta_prior - 1
            rev_growth = rev_now / rev_prior - 1
            result["asset_inflation_flag"] = float(1 if asset_growth > rev_growth + 0.15 else 0)

    # ── Group E — Governance & Promoter Risk ──────────────────────────────────

    pat = _get(latest, "pat")
    director_remuneration = _get(latest, "director_remuneration")
    related_party_transactions = _get(latest, "related_party_transactions")
    audit_qualified = _get(latest, "audit_qualified_flag")
    auditor_changed = _get(latest, "auditor_changed_flag")
    cfo_tenure = _get(latest, "cfo_tenure_months")
    board_independence = _get(latest, "board_independence_ratio")
    director_resignations = _get(latest, "director_resignations_4q")
    whistle_blower = _get(latest, "whistle_blower_policy_flag")
    revenue = _get(latest, "revenue")

    if not np.isnan(director_remuneration) and not np.isnan(pat) and abs(pat) > 0:
        result["salary_to_pat"] = abs(director_remuneration / pat)

    if not np.isnan(related_party_transactions) and not np.isnan(revenue) and revenue > 0:
        result["rpt_intensity"] = abs(related_party_transactions) / revenue

    result["audit_qualification_flag"] = float(audit_qualified) if not np.isnan(audit_qualified) else np.nan
    result["auditor_change_flag"] = float(auditor_changed) if not np.isnan(auditor_changed) else np.nan
    result["cfo_tenure_months"] = cfo_tenure
    result["board_independence"] = board_independence
    result["director_resignation_count_4q"] = director_resignations
    result["whistle_blower_policy"] = float(whistle_blower) if not np.isnan(whistle_blower) else np.nan

    # ── Groups F–I — Cross-Validation Features ────────────────────────────────

    # benford_mad: MAD of revenue series vs Benford's Law
    if len(fund_df) >= 10:
        revenue_series = fund_df["revenue"].dropna().to_numpy() if "revenue" in fund_df else np.array([])
        if len(revenue_series) >= 10:
            result["benford_mad"] = _benford_mad(revenue_series)

    # altman_z (requires fields that may not all be populated)
    # [AS BUILT, deep-forensic altman_z fix 2026-07-07] This block previously
    # always produced NaN: no `working_capital`/`total_liabilities` columns
    # exist in the fundamentals schema at all, and the two field names used
    # to look up market cap / equity ("market_cap", "book_equity") don't
    # match any real fundamentals column — so even the derivation fallback
    # silently never fired. Fixed:
    #  - total_liabilities derives from total_assets - total_equity (both
    #    real columns).
    #  - retained_earnings is now a real column (Screener's "Reserves" row,
    #    kept separate from total_equity in the schema this session — see
    #    datastore/schema/create_normalised.py's docstring).
    #  - working_capital derives from current_assets - current_liabilities,
    #    which ARE real schema columns but are ALWAYS NULL from Screener's
    #    free tier (see ingestion/scrapers/screener.py's module docstring —
    #    not a distinct labeled row on the free-tier balance sheet). So
    #    working_capital, and therefore altman_z's X1 term, stays genuinely
    #    blocked today, not a wiring bug.
    #  - market cap (X4's numerator) — [FIXED 2026-07-07, same-day
    #    follow-up] now derived the same way features/fundamental.py's
    #    ev_to_ebitda does: real PIT-safe close price (via
    #    _latest_close_on_or_before, same helper, imported directly rather
    #    than duplicated) x shares_outstanding (a real, already-existing
    #    fundamentals column). Requires ticker_ohlcv to be passed in by the
    #    caller (compute_deep_forensic_features_panel now accepts and
    #    forwards ohlcv_panel, same as fundamental/governance/corp-action
    #    panels) — if the caller doesn't provide it, market_cap stays NaN
    #    (no fabricated fallback), same honest-degradation behavior as
    #    every other field here.
    wc = _get(latest, "working_capital")
    if np.isnan(wc):
        ca = _get(latest, "current_assets")
        cl = _get(latest, "current_liabilities")
        if not np.isnan(ca) and not np.isnan(cl):
            wc = ca - cl
    retained = _get(latest, "retained_earnings")
    ebit_v = _get(latest, "ebit")
    # Derive ebit if not directly available: operating_margin * revenue
    if np.isnan(ebit_v):
        op_margin = _get(latest, "operating_margin")
        rev = _get(latest, "revenue")
        if not np.isnan(op_margin) and not np.isnan(rev):
            ebit_v = op_margin * rev
    total_liab = _get(latest, "total_liabilities")
    if np.isnan(total_liab):
        # Derive: total_assets - total_equity (both real columns)
        equity = _get(latest, "total_equity")
        if not np.isnan(total_assets) and not np.isnan(equity):
            total_liab = total_assets - equity
    mktcap = _get(latest, "market_cap")
    if np.isnan(mktcap):
        shares_out = _get(latest, "shares_outstanding")
        try:
            close = _latest_close_on_or_before(client, ticker, as_of, ticker_ohlcv=ticker_ohlcv)
        except Exception:
            # Never let a price-lookup failure (real API error, or a test
            # double that doesn't stub get_ohlcv) crash the other 27
            # forensic features this function computes — same
            # never-fabricate-but-never-crash pattern used throughout this
            # module (e.g. every other _get(...) NaN-on-missing path).
            close = None
        if not np.isnan(shares_out) and close is not None:
            mktcap = (shares_out * close) / 1e7  # raw rupees -> Crore, same unit as total_liabilities
    rev_latest = _get(latest, "revenue")
    if sector in _FINANCIAL_SERVICES_SECTORS:
        result["altman_z"] = np.nan
    else:
        result["altman_z"] = _altman_z(wc, retained, ebit_v, total_assets, total_liab, rev_latest, mktcap)

    # interest_coverage_trend: slope of interest_coverage over 4 quarters
    if len(fund_df) >= 4 and "interest_coverage" in fund_df.columns:
        ic_series = fund_df["interest_coverage"].dropna()
        if len(ic_series) >= 3:
            x = np.arange(len(ic_series))
            slope = np.polyfit(x, ic_series.to_numpy(dtype=float), 1)[0]
            result["interest_coverage_trend"] = float(slope)

    # pledge_spiral_risk: promoter pledge × |price_decline_proxy|
    # insider_selling_flag: promoter holding QoQ decline > threshold
    # [AS BUILT, deep-forensic cluster E.2 fix 2026-07-07] Both features need
    # `shareholding` table rows (promoter_pct/promoter_pledge live there, NOT
    # on `fundamentals` — see datastore/schema/create_normalised.py's
    # _CREATE_SHAREHOLDING). This block previously had two real bugs:
    #  - pledge_spiral_risk looked up "promoter_pledge_pct", but the real
    #    shareholding column is "promoter_pledge" (still always None today —
    #    see ingestion/scrapers/screener.py's _build_shareholding_row
    #    docstring for the live NSE pledge-source investigation — so this
    #    field-name fix alone doesn't unblock it, just removes a second,
    #    independent bug stacked on top of the real data gap).
    #  - insider_selling_flag read `fund_df["promoter_pct"]`, a column that
    #    only exists on `shareholding`, never on `fundamentals` — so
    #    `"promoter_pct" in fund_df.columns` was always False and this
    #    feature was always NaN regardless of real data availability. Fixed
    #    to use the same shareholding rows fetched for pledge_spiral_risk.
    #    Live-verified this now produces real 0/1 flags wherever
    #    shareholding history has >= 2 quarters (promoter_pct IS populated
    #    from Screener — see screener.py's #shareholding parsing).
    try:
        share_rows = (
            pre_loaded_shareholding if pre_loaded_shareholding is not None
            else client.get_shareholding_history(ticker, as_of, lookback_years=1)
        )
        if share_rows:
            sh_df = pd.DataFrame(share_rows).sort_values("quarter_end_date")
            if not sh_df.empty:
                latest_sh = sh_df.iloc[-1]
                pledge_pct = _get(latest_sh, "promoter_pledge")
                # Promoter pledge change (risk increases as pledge grows)
                if len(sh_df) >= 2:
                    prior_sh = sh_df.iloc[-2]
                    pledge_prior = _get(prior_sh, "promoter_pledge")
                    if not np.isnan(pledge_pct) and not np.isnan(pledge_prior):
                        pledge_delta = pledge_pct - pledge_prior
                        result["pledge_spiral_risk"] = float(pledge_pct * max(0.0, pledge_delta) / 100.0)
                    elif not np.isnan(pledge_pct):
                        result["pledge_spiral_risk"] = float(pledge_pct / 100.0)
                elif not np.isnan(pledge_pct):
                    result["pledge_spiral_risk"] = float(pledge_pct / 100.0)

                if "promoter_pct" in sh_df.columns and len(sh_df) >= 2:
                    recent_promoter = sh_df["promoter_pct"].dropna()
                    if len(recent_promoter) >= 2:
                        change = recent_promoter.iloc[-1] - recent_promoter.iloc[-2]
                        result["insider_selling_flag"] = float(1 if change < -2.0 else 0)
    except Exception as exc:
        logger.debug(f"shareholding fetch failed for {ticker}: {exc}")

    # gst_revenue_divergence: proxy using revenue vs IIP growth (from macro store)
    # Only computable if macro data is available; returns NaN otherwise
    # (Full computation requires ingestion/scrapers/macro_real_economy.py)
    result["gst_revenue_divergence"] = np.nan  # populated by matrix_builder via cross-join with macro

    # peer_outlier_score: z-score of roe vs sector peers
    if sector_fundamentals is not None and not sector_fundamentals.empty and "roe" in sector_fundamentals.columns:
        roe_self = _get(latest, "roe")
        sector_roes = sector_fundamentals["roe"].dropna().to_numpy()
        if not np.isnan(roe_self) and len(sector_roes) >= 3:
            result["peer_outlier_score"] = _peer_outlier_z(roe_self, sector_roes)

    # tax_rate_anomaly: effective tax rate vs statutory 25.17%
    STATUTORY_TAX = 0.2517
    tax_provision = _get(latest, "tax_expense")
    pbt = _get(latest, "pbt")
    if not np.isnan(tax_provision) and not np.isnan(pbt) and abs(pbt) > 0:
        effective_rate = tax_provision / pbt
        result["tax_rate_anomaly"] = float(abs(effective_rate - STATUTORY_TAX))

    return result


# ── Panel wrapper ─────────────────────────────────────────────────────────────


def compute_deep_forensic_features_panel(
    client: DataStoreClient,
    tickers: List[str],
    as_of: datetime,
    lookback_years: int = 3,
    data_cache=None,
    ohlcv_panel: "Optional[pd.DataFrame]" = None,
    sector_map: "Optional[Dict[str, str]]" = None,
) -> pd.DataFrame:
    """
    Compute deep forensic features for all tickers.

    Parameters
    ----------
    client : DataStoreClient
    tickers : list of str
    as_of : datetime
    lookback_years : int
    ohlcv_panel : pd.DataFrame, optional
        Pre-fetched bulk OHLCV panel (same one build_feature_matrix already
        fetches for every other panel) — 2026-07-07: forwarded to
        compute_deep_forensic_features so altman_z's market_cap term (close
        price x shares_outstanding) can be computed PIT-safely without an
        extra per-ticker API call; without it, market_cap (and therefore
        altman_z) stays NaN, same honest-degradation behavior as before.
    sector_map : dict, optional
        ticker -> real sector taxonomy string (e.g. hybrid_compute.py's
        existing sector_map). Forwarded to compute_deep_forensic_features
        so altman_z is skipped (NaN) for Financial Services tickers,
        where its liabilities/working-capital ratios don't apply
        (2026-07-19 full-codebase-review). None (default) preserves prior
        behavior - always compute altman_z.

    Returns
    -------
    pd.DataFrame
        Columns: ticker + DEEP_FORENSIC_FEATURES; one row per ticker.

    Spec References
    ---------------
    SPEC-PIPE-004: per-ticker calls are I/O orchestration, not vectorized math;
      the "no Python loops over stocks" rule governs the pandas rolling
      operations inside feature computation, not the fetch-and-compute loop here.
    SPEC-SOLID-005: data only through DataStoreClient.
    """
    rows = []
    for ticker in tickers:
        pre_fund = data_cache.get_fundamentals(ticker, as_of) if data_cache is not None else None
        pre_sh = data_cache.get_shareholding(ticker, as_of) if data_cache is not None else None
        t_ohlcv = ohlcv_panel[ohlcv_panel["ticker"] == ticker] if ohlcv_panel is not None else None
        feat = compute_deep_forensic_features(
            client, ticker, as_of, lookback_years,
            pre_loaded_fundamentals=pre_fund,
            pre_loaded_shareholding=pre_sh,
            ticker_ohlcv=t_ohlcv,
            sector=sector_map.get(ticker) if sector_map is not None else None,
        )
        feat["ticker"] = ticker
        rows.append(feat)

    if not rows:
        df = pd.DataFrame(columns=["ticker"] + DEEP_FORENSIC_FEATURES)
        return df

    df = pd.DataFrame(rows)
    for col in DEEP_FORENSIC_FEATURES:
        if col not in df.columns:
            df[col] = np.nan
    return df[["ticker"] + DEEP_FORENSIC_FEATURES]
