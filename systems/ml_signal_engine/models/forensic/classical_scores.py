"""
systems/ml_signal_engine/models/forensic/classical_scores.py

Phase: 2.5 (Forensic Accounting System M-09/M-10)
Specs: SPEC-MODEL-009
Owner: ml_signal_engine / forensic
Consumers: features/forensic_classical.py, systems/ml_signal_engine/models/forensic/forensic_ml.py

M-09: Forensic Classical Scores. Pure formula computation, no ML, no
training (SPEC-MODEL-009: "No training required — pure formula
computation from quarterly fundamentals") — 7 academic forensic-
accounting models: Beneish M-Score, Altman Z-Score, Piotroski F-Score,
Ohlson O-Score, Dechow F-Score, Sloan Accrual, Benford's Law. Each is a
small, independently-testable pure function operating on explicit named
inputs (not a DataStoreClient call) — features/forensic_classical.py is
the consumer that fetches real data and derives these inputs.

[AS BUILT] The build prompt's deliverable list names 6 models explicitly
(Beneish, Altman, Piotroski, Ohlson, Sloan, Benford) but says "All 7
classical scores combined" — Dechow F-Score is the 7th, named in
alphalens_docs/Forensic_Accounting_ML_Specification.md's "Classical
scoring models | 7 (Beneish, Altman, Ohlson, Piotroski, Dechow, Sloan,
Benford)" and given a full formula there (the prompt's own bullets just
didn't restate it). Implemented here using that doc's formula.

[AS BUILT] All Beneish/Piotroski/Ohlson/Dechow "t-1" comparisons use the
SAME QUARTER ONE YEAR AGO (t-4 in this project's quarterly cadence), not
the immediately preceding quarter — controls for seasonality, the
standard practice for applying these (originally annual-filing) models
to quarterly data, consistent with how this project already computes
YoY deltas elsewhere (features/fundamental.py's revenue_growth_yoy).

[AS BUILT] Ohlson's `log(TA/GNP_Deflator)` term: no Indian GNP deflator
series is ingested in this project. Uses `log(TA)` directly (the
deflator term exists in the original 1980 US-calibrated paper purely to
normalize firm size for inflation across decades of data — for this
project's same-currency, same-period comparisons, omitting it changes
the absolute O-Score scale but not its relative ordering/trend, which is
what `ohlson_prob_change_4q` actually uses). Documented, not silently
approximated.

PIT Assumptions
----------------
This module is PIT-agnostic by design — it has no knowledge of
announcement_date/filing_date. The caller (features/forensic_classical.py)
is responsible for supplying only already-PIT-eligible inputs (SPEC-PIPE-003).
"""

import logging
from typing import Any, Dict, List, Optional

import numpy as np
from scipy.stats import chisquare

logger = logging.getLogger(__name__)

BENEISH_MANIPULATOR_THRESHOLD = -1.78
ALTMAN_DISTRESS_THRESHOLD = 1.81
ALTMAN_SAFE_THRESHOLD = 2.99
PIOTROSKI_WEAK_THRESHOLD = 2
PIOTROSKI_STRONG_THRESHOLD = 7
BENFORD_MAD_NONCONFORMING = 0.015
BENFORD_MAD_SIGNIFICANT = 0.030

# Benford's Law expected first-digit frequencies (digits 1-9).
_BENFORD_EXPECTED = np.array([np.log10(1 + 1.0 / d) for d in range(1, 10)])

CLASSICAL_SCORE_NAMES = [
    "beneish_m_score", "altman_z_score", "piotroski_f_score",
    "ohlson_o_score", "dechow_f_score", "sloan_accrual", "benford_mad",
]

# forensic_classical_composite weights — equal-weighted across the 7
# classical models (the build prompt's "weighted average" doesn't specify
# unequal weights; equal weighting is the documented default).
_COMPOSITE_WEIGHTS = {name: 1.0 / len(CLASSICAL_SCORE_NAMES) for name in CLASSICAL_SCORE_NAMES}


def _safe_div(numerator: Optional[float], denominator: Optional[float]) -> float:
    """NaN (not inf/ZeroDivisionError) on 0/0, x/0, or a missing operand."""
    if numerator is None or denominator is None:
        return np.nan
    try:
        if denominator == 0 or np.isnan(denominator) or np.isnan(numerator):
            return np.nan
        return float(numerator) / float(denominator)
    except (TypeError, ValueError):
        return np.nan


def _safe_ratio_of_ratios(num_t: float, den_t: float, num_prev: float, den_prev: float) -> float:
    """(num_t/den_t) / (num_prev/den_prev) — the shape of every Beneish index."""
    ratio_t = _safe_div(num_t, den_t)
    ratio_prev = _safe_div(num_prev, den_prev)
    return _safe_div(ratio_t, ratio_prev)


# ===== Group A: Beneish M-Score (8 components + composite) =====
def beneish_m_score(financials: Dict[str, float]) -> Dict[str, float]:
    """
    Compute all 8 Beneish M-Score components plus the composite.

    Parameters
    ----------
    financials : dict
        Expected keys (current period, no suffix; year-ago period,
        `_yoy` suffix): revenue, receivables, gross_profit, ca (current
        assets), ppe, ta (total assets), depreciation, sga, ni, cfo,
        ltd_cl (long-term debt + current liabilities).
        Missing keys degrade that component to NaN, not an error.

    Returns
    -------
    dict
        dsri, gmi, aqi, sgi, depi, sgai, tata, lvgi, m_score,
        is_likely_manipulator (bool, m_score > -1.78).

    Spec References
    ----------------
    SPEC-MODEL-009.

    Raises
    ------
    None
    """
    f = financials

    dsri = _safe_ratio_of_ratios(
        f.get("receivables"), f.get("revenue"), f.get("receivables_yoy"), f.get("revenue_yoy")
    )

    gm_t = _safe_div(f.get("gross_profit"), f.get("revenue"))
    gm_prev = _safe_div(f.get("gross_profit_yoy"), f.get("revenue_yoy"))
    gmi = _safe_div(gm_prev, gm_t)

    aq_t = 1.0 - _safe_div((f.get("ca", np.nan) or 0) + (f.get("ppe", np.nan) or 0), f.get("ta"))
    aq_prev = 1.0 - _safe_div(
        (f.get("ca_yoy", np.nan) or 0) + (f.get("ppe_yoy", np.nan) or 0), f.get("ta_yoy")
    )
    aqi = _safe_div(aq_t, aq_prev) if not (np.isnan(aq_t) or np.isnan(aq_prev)) else np.nan

    sgi = _safe_div(f.get("revenue"), f.get("revenue_yoy"))

    deprec_rate_t = _safe_div(f.get("depreciation"), (f.get("depreciation", np.nan) or 0) + (f.get("ppe", np.nan) or 0))
    deprec_rate_prev = _safe_div(
        f.get("depreciation_yoy"), (f.get("depreciation_yoy", np.nan) or 0) + (f.get("ppe_yoy", np.nan) or 0)
    )
    depi = _safe_div(deprec_rate_prev, deprec_rate_t)

    sgai = _safe_ratio_of_ratios(f.get("sga"), f.get("revenue"), f.get("sga_yoy"), f.get("revenue_yoy"))

    tata = _safe_div((f.get("ni", np.nan) or np.nan) - (f.get("cfo", np.nan) or np.nan), f.get("ta"))

    lvgi = _safe_ratio_of_ratios(f.get("ltd_cl"), f.get("ta"), f.get("ltd_cl_yoy"), f.get("ta_yoy"))

    components = [dsri, gmi, aqi, sgi, depi, sgai, tata, lvgi]
    if any(np.isnan(c) for c in components):
        m_score = np.nan
    else:
        m_score = (
            -4.84 + 0.920 * dsri + 0.528 * gmi + 0.404 * aqi + 0.892 * sgi
            + 0.115 * depi - 0.172 * sgai + 4.679 * tata - 0.327 * lvgi
        )

    return {
        "dsri": dsri, "gmi": gmi, "aqi": aqi, "sgi": sgi, "depi": depi,
        "sgai": sgai, "tata": tata, "lvgi": lvgi, "m_score": m_score,
        "is_likely_manipulator": (
            bool(m_score > BENEISH_MANIPULATOR_THRESHOLD) if not np.isnan(m_score) else None
        ),
    }


# ===== Altman Z-Score =====
def altman_z_score(financials: Dict[str, float]) -> Dict[str, float]:
    """
    Z = 1.2*(WC/TA) + 1.4*(RE/TA) + 3.3*(EBIT/TA) + 0.6*(MktCap/TL) + 1.0*(Sales/TA)

    Parameters
    ----------
    financials : dict
        Keys: wc, re, ebit, ta, mktcap, tl, sales (= revenue).

    Returns
    -------
    dict
        z_score, distress_zone (Z < 1.81), safe_zone (Z > 2.99).

    Raises
    ------
    None
    """
    f = financials
    ta = f.get("ta")
    terms = [
        1.2 * _safe_div(f.get("wc"), ta),
        1.4 * _safe_div(f.get("re"), ta),
        3.3 * _safe_div(f.get("ebit"), ta),
        0.6 * _safe_div(f.get("mktcap"), f.get("tl")),
        1.0 * _safe_div(f.get("sales"), ta),
    ]
    z = np.nan if any(np.isnan(t) for t in terms) else float(sum(terms))
    return {
        "z_score": z,
        "distress_zone": bool(z < ALTMAN_DISTRESS_THRESHOLD) if not np.isnan(z) else None,
        "safe_zone": bool(z > ALTMAN_SAFE_THRESHOLD) if not np.isnan(z) else None,
    }


# ===== Piotroski F-Score =====
def piotroski_f_score(financials: Dict[str, float]) -> Dict[str, Any]:
    """
    9 binary components, summed 0-9.

    Parameters
    ----------
    financials : dict
        Keys (current + `_yoy`): ni, ta, cfo, roa, ltd_cl, current_ratio,
        shares, gross_margin, asset_turnover.

    Returns
    -------
    dict
        f_roa, f_cfo, f_delta_roa, f_accrual, f_delta_leverage,
        f_delta_liquidity, f_no_dilution, f_delta_margin,
        f_delta_turnover (each 0/1/NaN), f_score (sum, NaN if any
        component is NaN), is_weak (<=2), is_strong (>=7).

    Raises
    ------
    None
    """
    f = financials

    def _gt(a, b):
        if a is None or b is None or (isinstance(a, float) and np.isnan(a)) or (isinstance(b, float) and np.isnan(b)):
            return np.nan
        return float(a > b)

    def _gt_zero(a):
        if a is None or (isinstance(a, float) and np.isnan(a)):
            return np.nan
        return float(a > 0)

    components = {
        "f_roa": _gt_zero(f.get("ni")),
        "f_cfo": _gt_zero(f.get("cfo")),
        "f_delta_roa": _gt(f.get("roa"), f.get("roa_yoy")),
        "f_accrual": _gt(f.get("cfo"), f.get("ni")),
        "f_delta_leverage": _gt(
            _safe_div(f.get("ltd_cl_yoy"), f.get("ta_yoy")), _safe_div(f.get("ltd_cl"), f.get("ta"))
        ),
        "f_delta_liquidity": _gt(f.get("current_ratio"), f.get("current_ratio_yoy")),
        "f_no_dilution": _gt(f.get("shares_yoy"), f.get("shares")) if f.get("shares") is not None else np.nan,
        "f_delta_margin": _gt(f.get("gross_margin"), f.get("gross_margin_yoy")),
        "f_delta_turnover": _gt(f.get("asset_turnover"), f.get("asset_turnover_yoy")),
    }
    # f_no_dilution: Score=1 if shares_t <= shares_{t-1} (i.e. not(shares_t > shares_prev))
    if f.get("shares") is not None and f.get("shares_yoy") is not None:
        shares, shares_yoy = f["shares"], f["shares_yoy"]
        components["f_no_dilution"] = (
            np.nan if (np.isnan(shares) or np.isnan(shares_yoy)) else float(shares <= shares_yoy)
        )

    values = list(components.values())
    f_score = np.nan if any(np.isnan(v) for v in values) else float(sum(values))

    return {
        **components,
        "f_score": f_score,
        "is_weak": bool(f_score <= PIOTROSKI_WEAK_THRESHOLD) if not np.isnan(f_score) else None,
        "is_strong": bool(f_score >= PIOTROSKI_STRONG_THRESHOLD) if not np.isnan(f_score) else None,
    }


# ===== Ohlson O-Score =====
def ohlson_o_score(financials: Dict[str, float]) -> Dict[str, float]:
    """
    O = -1.32 - 0.407*log(TA) + 6.03*(TL/TA) - 1.43*(WC/TA) + 0.0757*(CL/CA)
        - 1.72*X - 2.37*(NI/TA) - 1.83*(FFO/TL) + 0.285*Y - 0.521*Z

    See module docstring for the documented GNP-deflator omission.

    Parameters
    ----------
    financials : dict
        Keys: ta, tl, wc, cl, ca, ni, ffo (= ni + depreciation),
        ni_yoy, net_loss_2yr (bool/0/1, optional — if omitted, Y=0).

    Returns
    -------
    dict
        o_score, bankruptcy_prob (sigmoid of o_score).

    Raises
    ------
    None
    """
    f = financials
    ta, tl, wc, cl, ca, ni, ffo, ni_yoy = (
        f.get("ta"), f.get("tl"), f.get("wc"), f.get("cl"), f.get("ca"), f.get("ni"), f.get("ffo"), f.get("ni_yoy"),
    )

    if ta is None or np.isnan(ta) or ta <= 0:
        return {"o_score": np.nan, "bankruptcy_prob": np.nan}

    x = float(tl > ta) if (tl is not None and not np.isnan(tl)) else np.nan
    y = float(bool(f.get("net_loss_2yr", 0)))
    z = (
        _safe_div((ni - ni_yoy), (abs(ni) + abs(ni_yoy)))
        if (ni is not None and ni_yoy is not None and not np.isnan(ni) and not np.isnan(ni_yoy))
        else np.nan
    )

    terms = [
        -0.407 * np.log(ta),
        6.03 * _safe_div(tl, ta),
        -1.43 * _safe_div(wc, ta),
        0.0757 * _safe_div(cl, ca),
        -1.72 * x,
        -2.37 * _safe_div(ni, ta),
        -1.83 * _safe_div(ffo, tl),
        0.285 * y,
        -0.521 * z,
    ]
    if any(np.isnan(t) for t in terms):
        return {"o_score": np.nan, "bankruptcy_prob": np.nan}

    o_score = -1.32 + float(sum(terms))
    bankruptcy_prob = float(np.exp(o_score) / (1 + np.exp(o_score)))
    return {"o_score": o_score, "bankruptcy_prob": bankruptcy_prob}


# ===== Dechow F-Score =====
def dechow_f_score(financials: Dict[str, float]) -> Dict[str, float]:
    """
    F = -7.893 + 0.790*rsst_accruals + 2.518*change_receivables
        + 1.191*change_inventory + 1.979*pct_soft_assets
        + 0.171*change_cash_sales - 0.932*change_roa
        + 1.029*issuance + 0.255*book_to_market
        - 0.189*abnormal_change_employees

    Parameters
    ----------
    financials : dict
        Keys: rsst_accruals, change_receivables, change_inventory,
        pct_soft_assets (= (ta-cash-ppe)/ta), change_cash_sales,
        change_roa, issuance (0/1), book_to_market,
        abnormal_change_employees (optional — NaN if employee data
        unavailable, same documented gap as features/forensic_classical.py).

    Returns
    -------
    dict
        f_score (raw), misstatement_prob (sigmoid).

    Raises
    ------
    None
    """
    f = financials
    keys = [
        "rsst_accruals", "change_receivables", "change_inventory", "pct_soft_assets",
        "change_cash_sales", "change_roa", "issuance", "book_to_market", "abnormal_change_employees",
    ]
    vals = {k: f.get(k) for k in keys}
    if any(v is None or (isinstance(v, float) and np.isnan(v)) for v in vals.values()):
        return {"f_score": np.nan, "misstatement_prob": np.nan}

    f_score = (
        -7.893 + 0.790 * vals["rsst_accruals"] + 2.518 * vals["change_receivables"]
        + 1.191 * vals["change_inventory"] + 1.979 * vals["pct_soft_assets"]
        + 0.171 * vals["change_cash_sales"] - 0.932 * vals["change_roa"]
        + 1.029 * vals["issuance"] + 0.255 * vals["book_to_market"]
        - 0.189 * vals["abnormal_change_employees"]
    )
    misstatement_prob = float(np.exp(f_score) / (1 + np.exp(f_score)))
    return {"f_score": float(f_score), "misstatement_prob": misstatement_prob}


# ===== Sloan Accrual =====
def sloan_accrual(financials: Dict[str, float]) -> Dict[str, float]:
    """
    sloan_accrual = (NI - CFO) / Total_Assets.
    balance_sheet_accrual = (dCA - dCash - dCL + dSTD + dTP - Depreciation) / Avg_TA.

    Parameters
    ----------
    financials : dict
        Keys: ni, cfo, ta, ca, ca_yoy, cash, cash_yoy, cl, cl_yoy, std,
        std_yoy, tp (trade payables — falls back to 0 if unavailable),
        tp_yoy, depreciation, ta_yoy.

    Returns
    -------
    dict
        sloan_accrual, balance_sheet_accrual, is_high_accrual (>0.10).

    Raises
    ------
    None
    """
    f = financials
    core = _safe_div((f.get("ni", np.nan) or np.nan) - (f.get("cfo", np.nan) or np.nan), f.get("ta"))

    d_ca = _delta(f, "ca")
    d_cash = _delta(f, "cash")
    d_cl = _delta(f, "cl")
    d_std = _delta(f, "std")
    d_tp = _delta(f, "tp")
    depreciation = f.get("depreciation")
    avg_ta = _avg(f.get("ta"), f.get("ta_yoy"))

    bs_terms = [d_ca, d_cash, d_cl, d_std, d_tp, depreciation, avg_ta]
    if any(t is None or (isinstance(t, float) and np.isnan(t)) for t in bs_terms):
        bs_accrual = np.nan
    else:
        bs_accrual = (d_ca - d_cash - d_cl + d_std + d_tp - depreciation) / avg_ta

    return {
        "sloan_accrual": core,
        "balance_sheet_accrual": bs_accrual,
        "is_high_accrual": bool(core > 0.10) if not np.isnan(core) else None,
    }


def _delta(f: Dict[str, float], key: str) -> float:
    cur, prev = f.get(key), f.get(f"{key}_yoy")
    if cur is None or prev is None or np.isnan(cur) or np.isnan(prev):
        return np.nan
    return float(cur - prev)


def _avg(a: Optional[float], b: Optional[float]) -> float:
    if a is None or b is None or np.isnan(a) or np.isnan(b):
        return np.nan
    return float((a + b) / 2.0)


# ===== Benford's Law =====
def benford_analysis(series_dict: Dict[str, List[float]]) -> Dict[str, float]:
    """
    Chi-squared test + Mean Absolute Deviation of first-digit distribution
    against Benford's Law expected frequencies, per financial line item,
    plus an overall composite.

    Parameters
    ----------
    series_dict : dict of str -> list of float
        e.g. {'revenue': [...20 quarterly figures...], 'expense': [...],
        'receivables': [...]}. Each series should have >= 20 positive
        values for a meaningful chi-squared test (fewer values are still
        computed but are statistically weaker — not rejected, since
        SPEC-FEAT-001's "NaN until enough history" is about WARMUP, not a
        hard minimum-sample gate here).

    Returns
    -------
    dict
        `benford_{name}_chi2`/`benford_{name}_p_value`/`benford_{name}_mad`/
        `benford_{name}_digit_distribution` (observed first-digit 1-9
        frequencies)/`benford_{name}_n_obs` per series,
        `benford_expected_distribution` (the theoretical Benford 1-9
        frequencies, same for every series), `benford_overall_deviation`
        (mean chi2 across series), `benford_mad` (mean absolute
        deviation, averaged across series), `is_nonconforming` (MAD >
        0.015), `is_significant_deviation` (MAD > 0.030).

    Spec References
    ----------------
    SPEC-MODEL-009. FO5 (2026-07-11): extended to surface the full
    chi-square p-value + per-digit distribution that was already computed
    internally but previously discarded before reaching the API/UI.

    Raises
    ------
    None — a series with no usable positive values returns NaN for that
    series, not an error.
    """
    chi2_results: Dict[str, float] = {}
    mad_results: Dict[str, float] = {}
    p_value_results: Dict[str, float] = {}
    digit_dist_results: Dict[str, List[float]] = {}
    n_obs_results: Dict[str, int] = {}

    for name, series in series_dict.items():
        positive = [abs(v) for v in series if v is not None and not np.isnan(v) and v != 0]
        if len(positive) < 5:
            chi2_results[name] = np.nan
            mad_results[name] = np.nan
            p_value_results[name] = np.nan
            digit_dist_results[name] = [np.nan] * 9
            n_obs_results[name] = len(positive)
            continue

        first_digits = [int(str(v).lstrip("0.").replace(".", "")[0]) for v in positive]
        observed_counts = np.array([first_digits.count(d) for d in range(1, 10)], dtype=float)
        observed_freq = observed_counts / observed_counts.sum()

        expected_counts = _BENFORD_EXPECTED * observed_counts.sum()
        chi2_stat, p_value = chisquare(observed_counts, f_exp=expected_counts)
        chi2_results[name] = float(chi2_stat)
        mad_results[name] = float(np.mean(np.abs(observed_freq - _BENFORD_EXPECTED)))
        p_value_results[name] = float(p_value)
        digit_dist_results[name] = [float(f) for f in observed_freq]
        n_obs_results[name] = len(positive)

    out: Dict[str, Any] = {}
    for name in series_dict:
        out[f"benford_{name}_chi2"] = chi2_results[name]
        out[f"benford_{name}_p_value"] = p_value_results[name]
        out[f"benford_{name}_mad"] = mad_results[name]
        out[f"benford_{name}_digit_distribution"] = digit_dist_results[name]
        out[f"benford_{name}_n_obs"] = n_obs_results[name]
    out["benford_expected_distribution"] = [float(f) for f in _BENFORD_EXPECTED]
    valid_chi2 = [v for v in chi2_results.values() if not np.isnan(v)]
    valid_mad = [v for v in mad_results.values() if not np.isnan(v)]
    out["benford_overall_deviation"] = float(np.mean(valid_chi2)) if valid_chi2 else np.nan
    out["benford_mad"] = float(np.mean(valid_mad)) if valid_mad else np.nan
    out["is_nonconforming"] = (
        bool(out["benford_mad"] > BENFORD_MAD_NONCONFORMING) if not np.isnan(out["benford_mad"]) else None
    )
    out["is_significant_deviation"] = (
        bool(out["benford_mad"] > BENFORD_MAD_SIGNIFICANT) if not np.isnan(out["benford_mad"]) else None
    )
    return out


# ===== Composite =====
def forensic_classical_composite(scores: Dict[str, float]) -> Dict[str, Any]:
    """
    Weighted average of all 7 classical scores, each independently
    normalized to a 0-100 "risk" scale (higher = more manipulation-like)
    before blending — the 7 raw scores live on entirely different native
    scales (M-Score ~[-5,2], Z-Score ~[-5,10], F-Score [0,9], etc.) and
    cannot be averaged directly.

    Parameters
    ----------
    scores : dict
        m_score, z_score, f_score (Piotroski), o_score (or
        bankruptcy_prob), dechow_f_score (or misstatement_prob),
        sloan_accrual, benford_mad. Missing/NaN entries are excluded from
        the average (renormalized over whatever's available), not
        treated as 0 risk.

    Returns
    -------
    dict
        forensic_classical_composite (0-100), flag ('green'/'yellow'/
        'orange'/'red'/'black'), n_models_used.

    Spec References
    ----------------
    SPEC-MODEL-009.

    Raises
    ------
    None — an empty/all-NaN `scores` returns composite=NaN, flag=None.
    """
    normalized: Dict[str, float] = {}

    if "m_score" in scores and not _isnan(scores["m_score"]):
        # M-Score: -1.78 is the manipulator threshold. Map [-5, 1] -> [0, 100],
        # clipped, so -1.78 lands at roughly the 50-60 range (moderate risk).
        normalized["beneish_m_score"] = float(np.clip((scores["m_score"] + 5.0) / 6.0 * 100.0, 0, 100))

    if "z_score" in scores and not _isnan(scores["z_score"]):
        # Altman: lower Z = higher risk. Map [0, 4] -> [100, 0], clipped.
        normalized["altman_z_score"] = float(np.clip((4.0 - scores["z_score"]) / 4.0 * 100.0, 0, 100))

    if "piotroski_f_score" in scores and not _isnan(scores["piotroski_f_score"]):
        # F-Score: 0-9, lower = higher risk. Map [0,9] -> [100,0].
        normalized["piotroski_f_score"] = float(np.clip((9.0 - scores["piotroski_f_score"]) / 9.0 * 100.0, 0, 100))

    if "ohlson_bankruptcy_prob" in scores and not _isnan(scores["ohlson_bankruptcy_prob"]):
        normalized["ohlson_o_score"] = float(np.clip(scores["ohlson_bankruptcy_prob"] * 100.0, 0, 100))

    if "dechow_misstatement_prob" in scores and not _isnan(scores["dechow_misstatement_prob"]):
        normalized["dechow_f_score"] = float(np.clip(scores["dechow_misstatement_prob"] * 100.0, 0, 100))

    if "sloan_accrual" in scores and not _isnan(scores["sloan_accrual"]):
        # Sloan accrual: >0.10 = high risk. Map [0, 0.25] -> [0, 100], clipped.
        normalized["sloan_accrual"] = float(np.clip(scores["sloan_accrual"] / 0.25 * 100.0, 0, 100))

    if "benford_mad" in scores and not _isnan(scores["benford_mad"]):
        # Benford MAD: >0.015 non-conforming, >0.03 significant. Map [0, 0.05] -> [0, 100].
        normalized["benford_mad"] = float(np.clip(scores["benford_mad"] / 0.05 * 100.0, 0, 100))

    if not normalized:
        return {"forensic_classical_composite": np.nan, "flag": None, "n_models_used": 0}

    composite = float(np.mean(list(normalized.values())))
    return {
        "forensic_classical_composite": composite,
        "flag": _flag_for_score(composite),
        "n_models_used": len(normalized),
    }


def _isnan(v: Any) -> bool:
    return v is None or (isinstance(v, float) and np.isnan(v))


def _flag_for_score(score: float) -> str:
    if score <= 20:
        return "green"
    if score <= 40:
        return "yellow"
    if score <= 60:
        return "orange"
    if score <= 80:
        return "red"
    return "black"
