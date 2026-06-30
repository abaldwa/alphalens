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
"""

import logging
import math
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from datastore.client import DataStoreClient

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
    try:
        x1 = working_capital / total_assets
        x2 = retained_earnings / total_assets
        x3 = ebit / total_assets
        x4 = market_cap / max(abs(total_liabilities), 1e-6)
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
    intangibles = _get(latest, "intangibles")
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
        intangibles_prior = _get(yr_ago, "intangibles")
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
    wc = _get(latest, "working_capital")
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
        # Derive: total_assets - book_equity (total_equity)
        equity = _get(latest, "book_equity")
        if not np.isnan(total_assets) and not np.isnan(equity):
            total_liab = total_assets - equity
    mktcap = _get(latest, "market_cap")
    rev_latest = _get(latest, "revenue")
    result["altman_z"] = _altman_z(wc, retained, ebit_v, total_assets, total_liab, rev_latest, mktcap)

    # interest_coverage_trend: slope of interest_coverage over 4 quarters
    if len(fund_df) >= 4 and "interest_coverage" in fund_df.columns:
        ic_series = fund_df["interest_coverage"].dropna()
        if len(ic_series) >= 3:
            x = np.arange(len(ic_series))
            slope = np.polyfit(x, ic_series.to_numpy(dtype=float), 1)[0]
            result["interest_coverage_trend"] = float(slope)

    # pledge_spiral_risk: promoter pledge × |price_decline_proxy|
    try:
        share_rows = (
            pre_loaded_shareholding if pre_loaded_shareholding is not None
            else client.get_shareholding_history(ticker, as_of, lookback_years=1)
        )
        if share_rows:
            sh_df = pd.DataFrame(share_rows).sort_values("quarter_end_date")
            if not sh_df.empty:
                latest_sh = sh_df.iloc[-1]
                pledge_pct = _get(latest_sh, "promoter_pledge_pct")
                # Promoter pledge change (risk increases as pledge grows)
                if len(sh_df) >= 2:
                    prior_sh = sh_df.iloc[-2]
                    pledge_prior = _get(prior_sh, "promoter_pledge_pct")
                    if not np.isnan(pledge_pct) and not np.isnan(pledge_prior):
                        pledge_delta = pledge_pct - pledge_prior
                        result["pledge_spiral_risk"] = float(pledge_pct * max(0.0, pledge_delta) / 100.0)
                    elif not np.isnan(pledge_pct):
                        result["pledge_spiral_risk"] = float(pledge_pct / 100.0)
                elif not np.isnan(pledge_pct):
                    result["pledge_spiral_risk"] = float(pledge_pct / 100.0)
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

    # insider_selling_flag: from shareholding — promoter holding decline > threshold
    if "promoter_pct" in fund_df.columns and len(fund_df) >= 2:
        recent_promoter = fund_df["promoter_pct"].dropna()
        if len(recent_promoter) >= 2:
            change = recent_promoter.iloc[-1] - recent_promoter.iloc[-2]
            result["insider_selling_flag"] = float(1 if change < -2.0 else 0)

    return result


# ── Panel wrapper ─────────────────────────────────────────────────────────────


def compute_deep_forensic_features_panel(
    client: DataStoreClient,
    tickers: List[str],
    as_of: datetime,
    lookback_years: int = 3,
    data_cache=None,
) -> pd.DataFrame:
    """
    Compute deep forensic features for all tickers.

    Parameters
    ----------
    client : DataStoreClient
    tickers : list of str
    as_of : datetime
    lookback_years : int

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
        feat = compute_deep_forensic_features(
            client, ticker, as_of, lookback_years,
            pre_loaded_fundamentals=pre_fund,
            pre_loaded_shareholding=pre_sh,
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
