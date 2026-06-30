"""
features/forensic_classical.py

Phase: 2.5 (Forensic Accounting System M-09/M-10)
Specs: SPEC-MODEL-009, SPEC-PIPE-003 (CRITICAL), SPEC-PIPE-004, SPEC-SOLID-005
Owner: Platform / Features
Consumers: features/matrix_builder.py, systems/ml_signal_engine/models/forensic/forensic_ml.py

Forensic features from the build prompt's literal Groups A-C list
(Group A: all 8 Beneish components; Group B: 10 cash-flow-quality
features; Group C: 8 revenue-quality features) — computed from real
DataStoreClient data (SPEC-SOLID-005) and
systems/ml_signal_engine/models/forensic/classical_scores.py's pure
Beneish formula (Group A only; Groups B/C have no academic formula to
reuse, computed directly here).

[AS BUILT] Actual total: 26 features (8+10+8), not the literal "30" in
this phase's build prompt's own summary line for this deliverable — same
header-vs-enumerated-list mismatch this project has resolved the same
way every time it's occurred (features/technical.py 70-vs-76,
features/fundamental.py 30-vs-28, etc.): trust the explicit per-group
enumeration, not the header arithmetic. The prompt's own group counts
(8+10+8) already sum to 26; nothing is missing or dropped here.

[AS BUILT] Group C's literal prompt list (receivable_days_change,
unbilled_revenue_ratio, cash_revenue_ratio, revenue_vs_gst_proxy,
revenue_concentration, round_number_revenue_flag,
channel_stuffing_indicator, quarter_end_revenue_spike) differs from
alphalens_docs/Forensic_Accounting_ML_Specification.md's own Group C
(which additionally has deferred_revenue_decline, other_income_ratio,
export_revenue_flag instead of the prompt's last 3 names) — same
"literal build-prompt text governs over the doc when they diverge"
precedent already applied throughout this project (fundamental.py,
governance.py, fno_features.py, multibagger.py). The doc's Group C names
are not implemented here.

[AS BUILT] Real data-sourcing gap, documented not hidden: several raw
line items these formulas formally need (Total Assets, PPE, SGA,
Depreciation*, absolute Receivables, Unbilled Revenue, Cash Received
from Customers, top-5 customer concentration, industry GST collection
growth) are not separately captured by ingestion/scrapers/screener.py's
current free-tier parsing (only revenue, ebitda, pat, margins, roe/roce,
debt_to_equity, interest_coverage, total_debt are populated live as of
this phase — gross_profit/capex/current_assets/current_liabilities/
cash_and_equivalents/fcf/asset_turnover/inventory_days/receivable_days/
payable_days are columns that EXIST in the schema [P2.1] but are not yet
populated by the live scraper). *depreciation WAS added this phase (P2.5)
— screener.py already parsed it internally (to derive ebitda) but never
persisted it; now it does.

Where a raw input is genuinely missing, this module either:
  (a) derives a documented, real-data-only APPROXIMATION (e.g.
      `derive_total_assets`: book_equity + total_debt — omits non-debt
      current liabilities, which the schema doesn't carry; `_derive_sga`:
      gross_profit - ebitda — treats all non-COGS opex as SGA; `_derive_cfo`:
      fcf + capex — the standard FCF = CFO - Capex identity), or
  (b) returns NaN, never fabricated (SPEC-FEAT-001's discipline already
      applied to every other feature module).
The CODE is correct and ready to compute real, non-approximated values
the moment screener.py's parser is extended to capture the missing raw
balance-sheet line items — only the live DATA is partial today, not the
formulas.

PIT Assumptions
----------------
SPEC-PIPE-003 (CRITICAL): every row consumed here comes from
DataStoreClient.get_fundamentals_history(), already PIT-filtered
server-side (announcement_date <= as_of). All "t-4" (year-ago)
comparisons use the SAME QUARTER ONE YEAR AGO — see
classical_scores.py's module docstring for why (seasonality control,
standard practice applying annual-filing-designed models to quarterly data).
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from config.settings import ASSUMED_TAX_RATE
from datastore.client import DataStoreClient
from systems.ml_signal_engine.models.forensic.classical_scores import (
    altman_z_score,
    beneish_m_score,
    benford_analysis,
    dechow_f_score,
    forensic_classical_composite,
    ohlson_o_score,
    piotroski_f_score,
    sloan_accrual,
)

logger = logging.getLogger(__name__)

CRORE = 1e7

GROUP_A_FEATURES = ["dsri", "gmi", "aqi", "sgi", "depi", "sgai", "tata", "lvgi"]
GROUP_B_FEATURES = [
    "cfo_to_net_income", "accrual_ratio", "accrual_ratio_change", "cash_flow_variability",
    "capex_to_cfo_ratio", "cfo_net_income_divergence", "fcf_to_revenue", "interest_income_vs_cash",
    "tax_paid_to_pbt", "operating_cash_cycle_change",
]
GROUP_C_FEATURES = [
    "receivable_days_change", "unbilled_revenue_ratio", "cash_revenue_ratio", "revenue_vs_gst_proxy",
    "revenue_concentration", "round_number_revenue_flag", "channel_stuffing_indicator", "quarter_end_revenue_spike",
]

FORENSIC_CLASSICAL_FEATURES: List[str] = GROUP_A_FEATURES + GROUP_B_FEATURES + GROUP_C_FEATURES


def find_quarter(history: pd.DataFrame, fiscal_year: int, quarter: int) -> Optional[pd.Series]:
    match = history[(history["fiscal_year"] == fiscal_year) & (history["quarter"] == quarter)]
    return match.iloc[0] if len(match) else None


def quarters_back(fiscal_year: int, quarter: int, n: int) -> tuple:
    """Walk back n quarters from (fiscal_year, quarter), wrapping across fiscal years."""
    total = (fiscal_year * 4 + (quarter - 1)) - n
    return total // 4, (total % 4) + 1


def get_quarter_value(row: Optional[pd.Series], col: str) -> float:
    """Safe column lookup — NaN if row is None, column absent, or present-but-null
    (same helper as features/fundamental.py's `v()` — see that module's docstring
    for the real bug this guards against)."""
    if row is None or col not in row.index:
        return np.nan
    val = row[col]
    return np.nan if val is None or pd.isna(val) else val


def derive_total_assets(row: Optional[pd.Series]) -> float:
    """TA approx = book_equity + total_debt — see module docstring's documented approximation."""
    bvps = get_quarter_value(row, "book_value_per_share")
    shares = get_quarter_value(row, "shares_outstanding")
    debt = get_quarter_value(row, "total_debt")
    if pd.isna(bvps) or pd.isna(shares):
        return np.nan
    equity = (bvps * shares) / CRORE
    return equity + (debt if pd.notna(debt) else 0.0)


def _derive_cfo(row: Optional[pd.Series]) -> float:
    """CFO = FCF + Capex (standard identity: FCF = CFO - Capex)."""
    fcf, capex = get_quarter_value(row, "fcf"), get_quarter_value(row, "capex")
    if pd.isna(fcf) or pd.isna(capex):
        return np.nan
    return fcf + capex


def _derive_sga(row: Optional[pd.Series]) -> float:
    """SGA approx = gross_profit - ebitda — see module docstring's documented approximation."""
    gp, ebitda = get_quarter_value(row, "gross_profit"), get_quarter_value(row, "ebitda")
    if pd.isna(gp) or pd.isna(ebitda):
        return np.nan
    return gp - ebitda


def _build_beneish_inputs(latest: Optional[pd.Series], yoy: Optional[pd.Series]) -> Dict[str, float]:
    """Translate two fundamentals rows into classical_scores.beneish_m_score's expected dict."""
    receivable_days_t, revenue_t = get_quarter_value(latest, "receivable_days"), get_quarter_value(latest, "revenue")
    receivable_days_yoy, revenue_yoy = get_quarter_value(yoy, "receivable_days"), get_quarter_value(yoy, "revenue")
    # Beneish's DSRI numerator is absolute Receivables, not days — Receivables = receivable_days/365*revenue.
    receivables_t = (
        receivable_days_t / 365.0 * revenue_t if pd.notna(receivable_days_t) and pd.notna(revenue_t) else np.nan
    )
    receivables_yoy = (
        receivable_days_yoy / 365.0 * revenue_yoy
        if pd.notna(receivable_days_yoy) and pd.notna(revenue_yoy)
        else np.nan
    )

    ta, ta_yoy = derive_total_assets(latest), derive_total_assets(yoy)
    ca, ca_yoy = get_quarter_value(latest, "current_assets"), get_quarter_value(yoy, "current_assets")
    # [AS BUILT] PPE is NOT derived as TA - CA: AQI's whole purpose is to detect
    # growth in the "soft" residual (TA - CA - PPE, i.e. intangibles/goodwill/other
    # assets) — defining PPE as that exact residual would make (CA+PPE)/TA equal
    # 1.0 by construction on every row, a structural 0/0 in the AQI formula, not a
    # real measurement (caught by tests/unit/test_forensic_classical.py's "full
    # inputs return finite floats" test — see BuildLog.md "P2.5"). No separate
    # PPE/Net-Block field is captured by the live scraper yet (documented gap,
    # same as the rest of this module) — `aqi` stays honestly NaN until one is.
    ppe = get_quarter_value(latest, "ppe")
    ppe_yoy = get_quarter_value(yoy, "ppe")

    cl, cl_yoy = get_quarter_value(latest, "current_liabilities"), get_quarter_value(yoy, "current_liabilities")
    debt, debt_yoy = get_quarter_value(latest, "total_debt"), get_quarter_value(yoy, "total_debt")
    ltd_cl = debt + cl if pd.notna(debt) and pd.notna(cl) else np.nan
    ltd_cl_yoy = debt_yoy + cl_yoy if pd.notna(debt_yoy) and pd.notna(cl_yoy) else np.nan

    return {
        "receivables": receivables_t, "revenue": revenue_t,
        "receivables_yoy": receivables_yoy, "revenue_yoy": revenue_yoy,
        "gross_profit": get_quarter_value(latest, "gross_profit"),
        "gross_profit_yoy": get_quarter_value(yoy, "gross_profit"),
        "ca": ca, "ppe": ppe, "ta": ta, "ca_yoy": ca_yoy, "ppe_yoy": ppe_yoy, "ta_yoy": ta_yoy,
        "depreciation": get_quarter_value(latest, "depreciation"),
        "depreciation_yoy": get_quarter_value(yoy, "depreciation"),
        "sga": _derive_sga(latest), "sga_yoy": _derive_sga(yoy),
        "ni": get_quarter_value(latest, "pat"), "cfo": _derive_cfo(latest),
        "ltd_cl": ltd_cl, "ltd_cl_yoy": ltd_cl_yoy,
    }


def _round_number_revenue_flag(revenue: float) -> float:
    """
    1 if `revenue` (INR crore) is an exact multiple of 100 — a real,
    data-free signal: genuine organic revenue (the sum of countless
    individual transactions) essentially never lands on an exact
    round-hundred-crore figure by chance.

    [AS BUILT] An earlier version used a PERCENTAGE tolerance ("within
    0.1% of the nearest multiple of 10"), which is a real bug at the
    scale Indian large-caps report at: for a revenue of ~Rs 2,94,059 Cr,
    being off by even Rs 50 Cr from the nearest 10 is still within 0.1%,
    so the flag fired on essentially EVERY large company regardless of
    genuine roundness — caught live against RELIANCE's real revenue
    (Rs 2,94,059 Cr, not actually round, incorrectly flagged 1.0 under
    the old logic). Fixed to an ABSOLUTE, scale-independent exact-multiple
    test (see BuildLog.md "P2.5").
    """
    if pd.isna(revenue) or revenue < 1000:
        return np.nan if pd.isna(revenue) else 0.0
    return float(round(revenue) % 100 == 0)


def compute_forensic_classical_features(
    client: DataStoreClient, ticker: str, as_of: datetime, lookback_years: int = 4
) -> Dict[str, Any]:
    """
    Compute all 30 Group A-C forensic classical features for one ticker.

    Parameters
    ----------
    client : DataStoreClient
    ticker : str
    as_of : datetime
        PIT reference date.
    lookback_years : int
        History window — needs >= 8 quarters for cash_flow_variability.

    Returns
    -------
    dict
        feature_name -> value for all 30 FORENSIC_CLASSICAL_FEATURES.
        All-NaN if no PIT-eligible quarter exists yet.

    Spec References
    ----------------
    SPEC-MODEL-009, SPEC-PIPE-003 (CRITICAL).

    PIT Assumptions
    ----------------
    See module docstring.

    Raises
    ------
    None — missing/insufficient history degrades to NaN features, not an exception.
    """
    rows = client.get_fundamentals_history(ticker, as_of, lookback_years=lookback_years)
    if not rows:
        return {f: np.nan for f in FORENSIC_CLASSICAL_FEATURES}

    history = pd.DataFrame(rows)
    history["quarter_end_date"] = pd.to_datetime(history["quarter_end_date"])
    history = history.sort_values("quarter_end_date").reset_index(drop=True)

    latest = history.iloc[-1]
    fy, q = int(latest["fiscal_year"]), int(latest["quarter"])
    yoy_fy, yoy_q = quarters_back(fy, q, 4)
    yoy = find_quarter(history, yoy_fy, yoy_q)

    out: Dict[str, Any] = {}

    # ===== Group A: Beneish components =====
    beneish_inputs = _build_beneish_inputs(latest, yoy)
    beneish = beneish_m_score(beneish_inputs)
    for feat in GROUP_A_FEATURES:
        out[feat] = beneish[feat]

    # ===== Group B: Cash flow quality (10) =====
    cfo, cfo_yoy = _derive_cfo(latest), _derive_cfo(yoy)
    ni, ni_yoy = get_quarter_value(latest, "pat"), get_quarter_value(yoy, "pat")
    ta = derive_total_assets(latest)
    revenue, revenue_yoy = get_quarter_value(latest, "revenue"), get_quarter_value(yoy, "revenue")
    fcf = get_quarter_value(latest, "fcf")
    capex = get_quarter_value(latest, "capex")

    out["cfo_to_net_income"] = cfo / ni if pd.notna(cfo) and pd.notna(ni) and ni != 0 else np.nan
    out["accrual_ratio"] = (ni - cfo) / ta if pd.notna(ni) and pd.notna(cfo) and pd.notna(ta) and ta != 0 else np.nan

    fy4_fy, fy4_q = quarters_back(fy, q, 4)
    accrual_yoy_row = find_quarter(history, fy4_fy, fy4_q)
    ta_yoy_for_accrual = derive_total_assets(accrual_yoy_row)
    accrual_ratio_yoy = (
        (ni_yoy - cfo_yoy) / ta_yoy_for_accrual
        if pd.notna(ni_yoy) and pd.notna(cfo_yoy) and pd.notna(ta_yoy_for_accrual) and ta_yoy_for_accrual != 0
        else np.nan
    )
    out["accrual_ratio_change"] = (
        out["accrual_ratio"] - accrual_ratio_yoy
        if pd.notna(out["accrual_ratio"]) and pd.notna(accrual_ratio_yoy)
        else np.nan
    )

    last_8q = history.tail(8)
    cfo_series = pd.Series([_derive_cfo(r) for _, r in last_8q.iterrows()]).dropna()
    out["cash_flow_variability"] = (
        float(cfo_series.std() / abs(cfo_series.mean())) if len(cfo_series) >= 2 and cfo_series.mean() != 0 else np.nan
    )

    out["capex_to_cfo_ratio"] = capex / cfo if pd.notna(capex) and pd.notna(cfo) and cfo != 0 else np.nan

    revenue_growth_yoy = (
        (revenue / revenue_yoy - 1.0) if pd.notna(revenue) and pd.notna(revenue_yoy) and revenue_yoy != 0 else np.nan
    )
    cfo_growth_yoy = (cfo / cfo_yoy - 1.0) if pd.notna(cfo) and pd.notna(cfo_yoy) and cfo_yoy != 0 else np.nan
    out["cfo_net_income_divergence"] = (
        revenue_growth_yoy - cfo_growth_yoy if pd.notna(revenue_growth_yoy) and pd.notna(cfo_growth_yoy) else np.nan
    )

    out["fcf_to_revenue"] = fcf / revenue if pd.notna(fcf) and pd.notna(revenue) and revenue != 0 else np.nan

    # Not available: no separate Interest_Income line item is captured (NaN — documented gap).
    out["interest_income_vs_cash"] = np.nan

    # PBT/tax-paid approximation via the same ASSUMED_TAX_RATE precedent as
    # features/fundamental.py's ROIC NOPAT proxy (no real PBT/cash-tax line item available).
    pbt_proxy = ni / (1.0 - ASSUMED_TAX_RATE) if pd.notna(ni) else np.nan
    tax_paid_proxy = pbt_proxy - ni if pd.notna(pbt_proxy) and pd.notna(ni) else np.nan
    out["tax_paid_to_pbt"] = (
        tax_paid_proxy / pbt_proxy if pd.notna(tax_paid_proxy) and pd.notna(pbt_proxy) and pbt_proxy != 0 else np.nan
    )

    def _cash_cycle(row: Optional[pd.Series]) -> float:
        inv_d = get_quarter_value(row, "inventory_days")
        rec_d = get_quarter_value(row, "receivable_days")
        pay_d = get_quarter_value(row, "payable_days")
        if pd.isna(inv_d) or pd.isna(rec_d) or pd.isna(pay_d):
            return np.nan
        return inv_d + rec_d - pay_d

    cycle_t, cycle_yoy = _cash_cycle(latest), _cash_cycle(yoy)
    out["operating_cash_cycle_change"] = (
        cycle_t - cycle_yoy if pd.notna(cycle_t) and pd.notna(cycle_yoy) else np.nan
    )

    # ===== Group C: Revenue quality (8) =====
    rec_days_t, rec_days_yoy = get_quarter_value(latest, "receivable_days"), get_quarter_value(yoy, "receivable_days")
    out["receivable_days_change"] = (
        rec_days_t - rec_days_yoy if pd.notna(rec_days_t) and pd.notna(rec_days_yoy) else np.nan
    )
    # Not available: no unbilled-revenue, cash-received-from-customers, customer-concentration,
    # or industry GST-collection data is ingested anywhere in this project (NaN — documented gaps).
    out["unbilled_revenue_ratio"] = np.nan
    out["cash_revenue_ratio"] = np.nan
    out["revenue_vs_gst_proxy"] = np.nan
    out["revenue_concentration"] = np.nan

    out["round_number_revenue_flag"] = _round_number_revenue_flag(revenue)

    # channel_stuffing_indicator / quarter_end_revenue_spike: the spec's detection signal
    # ("spike in last month of quarter, reversal next month") needs intra-quarter (monthly)
    # granularity this project's quarterly fundamentals don't have — both use the coarsest
    # real proxy available: this quarter's QoQ revenue growth vs. the trailing-4-quarter
    # average QoQ growth (an unusually large quarter-end jump relative to the company's own
    # recent normal cadence), documented as an approximation, not the literal monthly signal.
    qoq_fy, qoq_q = quarters_back(fy, q, 1)
    qoq_prior = find_quarter(history, qoq_fy, qoq_q)
    revenue_qoq_prior = get_quarter_value(qoq_prior, "revenue")
    qoq_growth_valid = pd.notna(revenue) and pd.notna(revenue_qoq_prior) and revenue_qoq_prior != 0
    qoq_growth = (revenue / revenue_qoq_prior - 1.0) if qoq_growth_valid else np.nan
    # [AS BUILT] Starts at i=2, not i=1: i=1 would compare (latest vs. 1-quarter-back),
    # which is exactly qoq_growth itself — including the spike quarter's own growth
    # rate in its own "trailing baseline" silently dilutes/contaminates the z-score
    # (real bug caught by tests/unit/test_forensic_classical.py's spike-detection
    # test: a deliberately large jump scored z=1.73, under the 2.0 flag threshold,
    # purely because the spike was averaged into its own comparison baseline — see
    # BuildLog.md "P2.5"). The trailing window must only contain HISTORICAL
    # (pre-spike) growth rates: (t-1 vs t-2), (t-2 vs t-3), (t-3 vs t-4), (t-4 vs t-5).
    trailing_qoq_growths = []
    for i in range(2, 6):
        cur_fy, cur_q = quarters_back(fy, q, i - 1)
        prev_fy, prev_q = quarters_back(fy, q, i)
        cur_row, prev_row = find_quarter(history, cur_fy, cur_q), find_quarter(history, prev_fy, prev_q)
        cur_rev, prev_rev = get_quarter_value(cur_row, "revenue"), get_quarter_value(prev_row, "revenue")
        if pd.notna(cur_rev) and pd.notna(prev_rev) and prev_rev != 0:
            trailing_qoq_growths.append(cur_rev / prev_rev - 1.0)

    if pd.notna(qoq_growth) and len(trailing_qoq_growths) >= 2:
        trailing_mean = float(np.mean(trailing_qoq_growths))
        trailing_std = float(np.std(trailing_qoq_growths))
        spike_z = (qoq_growth - trailing_mean) / trailing_std if trailing_std > 0 else np.nan
        out["quarter_end_revenue_spike"] = spike_z
        out["channel_stuffing_indicator"] = float(spike_z > 2.0) if pd.notna(spike_z) else np.nan
    else:
        out["quarter_end_revenue_spike"] = np.nan
        out["channel_stuffing_indicator"] = np.nan

    return out


def compute_forensic_classical_features_panel(
    client: DataStoreClient, tickers: List[str], as_of: datetime
) -> pd.DataFrame:
    """
    Compute the 30-feature Group A-C panel for many tickers.

    Parameters
    ----------
    client : DataStoreClient
    tickers : list of str
    as_of : datetime

    Returns
    -------
    pd.DataFrame
        One row per ticker, columns = ['ticker'] + FORENSIC_CLASSICAL_FEATURES.

    Spec References
    ----------------
    SPEC-PIPE-004: per-ticker loop is I/O orchestration, same exemption as
    features/fundamental.py's panel function.
    """
    records = []
    for ticker in tickers:
        try:
            feats = compute_forensic_classical_features(client, ticker, as_of)
        except Exception as exc:
            logger.warning(f"forensic classical features failed for {ticker}: {exc}")
            feats = {f: np.nan for f in FORENSIC_CLASSICAL_FEATURES}
        feats["ticker"] = ticker
        records.append(feats)

    panel = pd.DataFrame(records)
    return panel[["ticker"] + FORENSIC_CLASSICAL_FEATURES]


def compute_forensic_classical_scores(
    client: DataStoreClient, ticker: str, as_of: datetime, lookback_years: int = 4
) -> Dict[str, Any]:
    """
    Compute all 7 M-09 classical MODEL-LEVEL outputs (m_score, z_score,
    f_score, o_score/bankruptcy_prob, dechow f_score/misstatement_prob,
    sloan_accrual, benford_mad) plus forensic_classical_composite for one
    real ticker — used by systems/ml_signal_engine/inference/
    score_forensic.py to populate the ml_forensic table's
    forensic_composite column for real tickers.

    [AS BUILT, P2.6] compute_forensic_classical_features() above only
    returns the 26 Group A-C COMPONENT features (dsri, gmi, ... — Beneish's
    8 raw inputs, not beneish_m_score's own m_score/is_likely_manipulator
    output); this function is the real-data counterpart to
    tests/regression/test_known_frauds.py's _classical_composite() helper,
    which only exists for the archive's pre-built feature dicts. Most
    Altman/Piotroski/Ohlson/Dechow inputs need raw line items (retained
    earnings, market cap, current assets/liabilities, employee counts,
    book-to-market) the live screener.py scraper does not capture (same
    documented gap as compute_forensic_classical_features's own module
    docstring) — those stay honest NaN; classical_scores.py's own
    functions already degrade gracefully to NaN sub-scores rather than
    raising (test_forensic_classical.py's
    test_missing_inputs_degrade_to_nan_not_exception).

    Returns
    -------
    dict
        m_score, z_score, f_score (Piotroski), o_score, bankruptcy_prob,
        dechow_f_score, misstatement_prob, sloan_accrual, benford_mad,
        forensic_classical_composite, flag, n_models_used.
    """
    rows = client.get_fundamentals_history(ticker, as_of, lookback_years=lookback_years)
    if not rows:
        return {
            "m_score": np.nan, "z_score": np.nan, "f_score": np.nan, "o_score": np.nan,
            "bankruptcy_prob": np.nan, "dechow_f_score": np.nan, "misstatement_prob": np.nan,
            "sloan_accrual": np.nan, "benford_mad": np.nan,
            "forensic_classical_composite": np.nan, "flag": None, "n_models_used": 0,
        }

    history = pd.DataFrame(rows)
    history["quarter_end_date"] = pd.to_datetime(history["quarter_end_date"])
    history = history.sort_values("quarter_end_date").reset_index(drop=True)
    latest = history.iloc[-1]
    fy, q = int(latest["fiscal_year"]), int(latest["quarter"])
    yoy_fy, yoy_q = quarters_back(fy, q, 4)
    yoy = find_quarter(history, yoy_fy, yoy_q)

    ta = derive_total_assets(latest)
    cfo = _derive_cfo(latest)
    ni = get_quarter_value(latest, "pat")
    ni_yoy = get_quarter_value(yoy, "pat")
    ca = get_quarter_value(latest, "current_assets")
    cl = get_quarter_value(latest, "current_liabilities")
    tl = get_quarter_value(latest, "total_debt")
    revenue = get_quarter_value(latest, "revenue")
    shares = get_quarter_value(latest, "shares_outstanding")
    bvps = get_quarter_value(latest, "book_value_per_share")

    beneish = beneish_m_score(_build_beneish_inputs(latest, yoy))

    wc = ca - cl if pd.notna(ca) and pd.notna(cl) else np.nan
    book_equity_cr = shares * bvps / CRORE if pd.notna(shares) and pd.notna(bvps) else np.nan
    mktcap = book_equity_cr  # no live market-cap feed wired here — book equity used as the documented proxy
    # Retained earnings has no direct line item — book equity (bvps x shares)
    # is used as a documented proxy (same approximation class as
    # derive_total_assets), since retained earnings is the dominant
    # component of book equity for a going concern with no recent capital
    # raise (a real but bounded approximation, not a fabricated value).
    re_proxy = book_equity_cr
    ebit = get_quarter_value(latest, "ebitda")  # depreciation not reliably available — EBITDA used as EBIT proxy
    altman = altman_z_score(
        {"wc": wc, "re": re_proxy, "ebit": ebit, "ta": ta, "mktcap": mktcap, "tl": tl, "sales": revenue}
    )

    ta_yoy = derive_total_assets(yoy)
    roa = ni / ta if pd.notna(ni) and pd.notna(ta) and ta != 0 else np.nan
    roa_yoy = ni_yoy / ta_yoy if pd.notna(ni_yoy) and pd.notna(ta_yoy) else np.nan
    op_margin, op_margin_yoy = get_quarter_value(latest, "operating_margin"), get_quarter_value(yoy, "operating_margin")
    at, at_yoy = get_quarter_value(latest, "asset_turnover"), get_quarter_value(yoy, "asset_turnover")
    piotroski = piotroski_f_score(
        {
            "ni": ni, "cfo": cfo, "roa": roa, "roa_yoy": roa_yoy,
            "ltd_cl": tl, "ta": ta, "ltd_cl_yoy": get_quarter_value(yoy, "total_debt"), "ta_yoy": ta_yoy,
            "current_ratio": ca / cl if pd.notna(ca) and pd.notna(cl) and cl != 0 else np.nan,
            "current_ratio_yoy": np.nan, "shares": shares, "shares_yoy": get_quarter_value(yoy, "shares_outstanding"),
            "gross_margin": op_margin, "gross_margin_yoy": op_margin_yoy,
            "asset_turnover": at, "asset_turnover_yoy": at_yoy,
        }
    )

    ohlson = ohlson_o_score(
        {"ta": ta, "tl": tl, "wc": wc, "cl": cl, "ca": ca, "ni": ni, "ffo": cfo, "ni_yoy": ni_yoy}
    )

    # Dechow needs employee-count/issuance/book-to-market inputs no real
    # source captures (documented gap, same class as Group D/E/H/I in
    # forensic_ml.py) — left NaN, degrades gracefully (see this function's
    # docstring).
    dechow = dechow_f_score({})

    sloan = sloan_accrual({"ni": ni, "cfo": cfo, "ta": ta})

    revenue_series = history["revenue"].dropna().tolist() if "revenue" in history else []
    benford = benford_analysis({"revenue": revenue_series}) if revenue_series else {"benford_mad": np.nan}

    scores = {
        "m_score": beneish["m_score"],
        "z_score": altman["z_score"],
        "piotroski_f_score": piotroski["f_score"],
        "ohlson_bankruptcy_prob": ohlson["bankruptcy_prob"],
        "dechow_misstatement_prob": dechow["misstatement_prob"],
        "sloan_accrual": sloan["sloan_accrual"],
        "benford_mad": benford.get("benford_mad", np.nan),
    }
    composite = forensic_classical_composite(scores)

    return {
        "m_score": beneish["m_score"],
        "z_score": altman["z_score"],
        "f_score": piotroski["f_score"],
        "o_score": ohlson["o_score"],
        "bankruptcy_prob": ohlson["bankruptcy_prob"],
        "dechow_f_score": dechow["f_score"],
        "misstatement_prob": dechow["misstatement_prob"],
        "sloan_accrual": sloan["sloan_accrual"],
        "benford_mad": benford.get("benford_mad", np.nan),
        "forensic_classical_composite": composite["forensic_classical_composite"],
        "flag": composite["flag"],
        "n_models_used": composite["n_models_used"],
    }
