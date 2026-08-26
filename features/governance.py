"""
features/governance.py

Phase: 2.1 (Fundamental Data Ingestion + PIT Validation)
Specs: SPEC-FEAT-002, SPEC-PIPE-003 (CRITICAL), SPEC-PIPE-004, SPEC-SOLID-005
Owner: Platform / Features
Consumers: features/matrix_builder (wired in P2.3), systems/ml_signal_engine

Computes the 12 governance features named in this phase's build prompt
(CLAUDE_CODE_PROMPTS.md P2.1): promoter_pct, promoter_change_qoq,
promoter_pledge, promoter_pledge_change_qoq, fii_pct, fii_change_qoq,
dii_pct, dii_change_qoq, mf_pct, mf_change_qoq,
promoter_pledge_spiral_flag, institutional_conviction_flag.

[AS BUILT] 01_features.md's older "Governance Features (12)" list uses a
different shape (promoter_holding_change_4q, institutional_total_change,
no composite flags) — same prompt-vs-doc divergence already documented in
features/fundamental.py's module docstring; the P2.1 build prompt's
literal list is implemented here.

A 13th feature, institutional_ownership_pct (= fii_pct + dii_pct + mf_pct),
was added later for the Under-followed Growth Improvers and
Governance-Aware Quality Growth strategies — a rollup of fields already
computed above, not new raw data.

`mf_pct` here is the BSE shareholding-pattern aggregate (one number per
quarter, `shareholding.mf_pct`) — distinct from the scheme-level monthly
AMFI holdings detail (`mf_scheme_count`, `mf_new_entry_count`, etc.)
that P2.2's features/mf_holdings.py computes from a different source and
a different PIT rule (5th of next month, not filing_date).

SPEC-PIPE-003 (CRITICAL): every row consumed here comes from
DataStoreClient.get_shareholding_history(), already PIT-filtered
server-side on filing_date (never quarter_end_date). Sequencing among
already-eligible rows uses quarter_end_date purely as a chronological
sort key — see features/fundamental.py's module docstring for why that is
not a PIT violation.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, cast

import numpy as np
import pandas as pd

from datastore.client import DataStoreClient

logger = logging.getLogger(__name__)

GOVERNANCE_FEATURES: List[str] = [
    "promoter_pct", "promoter_change_qoq", "promoter_pledge", "promoter_pledge_change_qoq",
    "fii_pct", "fii_change_qoq", "dii_pct", "dii_change_qoq", "mf_pct", "mf_change_qoq",
    "promoter_pledge_spiral_flag", "institutional_conviction_flag",
    # Added for Under-followed Growth Improvers (percentile_rank_asc target)
    # and Governance-Aware Quality Growth — simple rollup of the 3 existing
    # institutional-ownership fields above, not a new data source.
    "institutional_ownership_pct",
]

# promoter_pledge_spiral_flag: pledge > this AND price falling over the lookback window
PLEDGE_SPIRAL_THRESHOLD_PCT = 20.0
PRICE_FALL_LOOKBACK_DAYS = 63  # ~1 quarter, consistent with quarter_age_pct's 63-day convention


def _safe_change(current: Any, prior: Any) -> float:
    if current is None or prior is None or pd.isna(current) or pd.isna(prior):
        return float(np.nan)
    return float(current - prior)


def _sum_if_any_present(*values: Any) -> float:
    """Sum of the non-NaN values; NaN only if every value is missing (treats a
    missing FII/DII/MF row as 0% of that category, not as unknown-total)."""
    present = [v for v in values if v is not None and pd.notna(v)]
    return float(sum(present)) if present else float(np.nan)


def compute_governance_features(
    client: DataStoreClient,
    ticker: str,
    as_of: datetime,
    lookback_years: int = 2,
    pre_loaded_rows: Any = None,
    ticker_ohlcv: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """
    Compute all 13 governance features for one ticker.

    Parameters
    ----------
    client : DataStoreClient
        SPEC-DS-002: all shareholding/OHLCV access goes through the API.
    ticker : str
    as_of : datetime
        PIT reference date.
    lookback_years : int
        History window requested from the API — 2 years comfortably
        covers the single quarter-over-quarter comparison these features need.

    Returns
    -------
    dict
        feature_name -> value for all 13 GOVERNANCE_FEATURES. All-NaN
        (flags 0) if no PIT-eligible shareholding row exists yet.

    Spec References
    ----------------
    SPEC-PIPE-003 (CRITICAL), SPEC-FEAT-002.

    PIT Assumptions
    ----------------
    Trusts DataStoreClient.get_shareholding_history()'s server-side PIT
    filter (filing_date <= as_of) entirely.

    Raises
    ------
    None — missing/insufficient history degrades to NaN/0, not an exception.
    """
    rows = pre_loaded_rows if pre_loaded_rows is not None else client.get_shareholding_history(
        ticker, as_of, lookback_years=lookback_years
    )
    if not rows:
        result = {f: np.nan for f in GOVERNANCE_FEATURES}
        result["promoter_pledge_spiral_flag"] = 0
        result["institutional_conviction_flag"] = 0
        return result

    history = pd.DataFrame(rows)
    history["quarter_end_date"] = pd.to_datetime(history["quarter_end_date"])
    history = history.sort_values("quarter_end_date").reset_index(drop=True)

    latest = history.iloc[-1]
    qoq_prior = history.iloc[-2] if len(history) >= 2 else None

    def prior(col: str) -> Any:
        return qoq_prior[col] if qoq_prior is not None else None

    promoter_pledge_change_qoq = _safe_change(latest.get("promoter_pledge"), prior("promoter_pledge"))

    # promoter_pledge_spiral_flag: pledge above threshold AND price has fallen
    # over the lookback window — a classic distress signal (forced-selling risk).
    spiral_flag = 0
    if pd.notna(latest.get("promoter_pledge")) and latest["promoter_pledge"] > PLEDGE_SPIRAL_THRESHOLD_PCT:
        if ticker_ohlcv is not None and not ticker_ohlcv.empty:
            cutoff = pd.Timestamp(as_of) - pd.Timedelta(days=PRICE_FALL_LOOKBACK_DAYS)
            window = ticker_ohlcv[
                (ticker_ohlcv["date"] >= cutoff) & (ticker_ohlcv["date"] <= pd.Timestamp(as_of))
            ].sort_values("date")
            if len(window) >= 2:
                price_falling = float(window.iloc[-1]["close"]) < float(window.iloc[0]["close"])
                spiral_flag = int(price_falling)
        else:
            price_rows = client.get_ohlcv(
                ticker, from_date=as_of - timedelta(days=PRICE_FALL_LOOKBACK_DAYS), to_date=as_of
            )
            if len(price_rows) >= 2:
                ordered = sorted(price_rows, key=lambda r: r["date"])
                price_falling = ordered[-1]["close"] < ordered[0]["close"]
                spiral_flag = int(price_falling)

    # institutional_conviction_flag: FII + DII + MF holding all increased QoQ
    conviction_flag = 0
    if qoq_prior is not None:
        fii_up = pd.notna(latest.get("fii_pct")) and pd.notna(prior("fii_pct")) and latest["fii_pct"] > prior("fii_pct")
        dii_up = pd.notna(latest.get("dii_pct")) and pd.notna(prior("dii_pct")) and latest["dii_pct"] > prior("dii_pct")
        mf_up = pd.notna(latest.get("mf_pct")) and pd.notna(prior("mf_pct")) and latest["mf_pct"] > prior("mf_pct")
        conviction_flag = int(fii_up and dii_up and mf_up)

    return {
        "promoter_pct": latest.get("promoter_pct", np.nan),
        "promoter_change_qoq": _safe_change(latest.get("promoter_pct"), prior("promoter_pct")),
        "promoter_pledge": latest.get("promoter_pledge", np.nan),
        "promoter_pledge_change_qoq": promoter_pledge_change_qoq,
        "fii_pct": latest.get("fii_pct", np.nan),
        "fii_change_qoq": _safe_change(latest.get("fii_pct"), prior("fii_pct")),
        "dii_pct": latest.get("dii_pct", np.nan),
        "dii_change_qoq": _safe_change(latest.get("dii_pct"), prior("dii_pct")),
        "mf_pct": latest.get("mf_pct", np.nan),
        "mf_change_qoq": _safe_change(latest.get("mf_pct"), prior("mf_pct")),
        "promoter_pledge_spiral_flag": spiral_flag,
        "institutional_conviction_flag": conviction_flag,
        "institutional_ownership_pct": _sum_if_any_present(
            latest.get("fii_pct"), latest.get("dii_pct"), latest.get("mf_pct")
        ),
    }


def compute_governance_features_panel(
    client: DataStoreClient,
    tickers: List[str],
    as_of: datetime,
    data_cache: Any = None,
    ohlcv_panel: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Compute the 12-feature governance panel for many tickers.

    Unlike features/fundamental.py's panel function, governance features
    are NOT sector-z-scored — promoter/FII/DII/MF holding percentages and
    the two composite flags are already bounded/categorical, not the kind
    of unbounded ratio SPEC-FEAT-002's z-score normalization targets
    (the spec's normalization rule is scoped to "Fundamental features",
    01_features.md's separate "Governance Features (12)" section does not
    repeat that instruction).

    Parameters
    ----------
    client : DataStoreClient
    tickers : list of str
    as_of : datetime
        PIT reference date, shared across the whole panel.

    Returns
    -------
    pd.DataFrame
        One row per ticker, columns = ['ticker'] + GOVERNANCE_FEATURES.

    Spec References
    ----------------
    SPEC-PIPE-004: the per-ticker loop is I/O orchestration (one API call
    per ticker), same exemption as features/fundamental.py's panel function.
    """
    records = []
    for ticker in tickers:
        try:
            pre_rows = data_cache.get_shareholding(ticker, as_of) if data_cache is not None else None
            t_ohlcv = (
                ohlcv_panel[ohlcv_panel["ticker"] == ticker] if ohlcv_panel is not None else None
            )
            feats = compute_governance_features(
                client, ticker, as_of, pre_loaded_rows=pre_rows, ticker_ohlcv=t_ohlcv
            )
        except Exception as exc:
            logger.warning(f"governance features failed for {ticker}: {exc}")
            feats = {f: np.nan for f in GOVERNANCE_FEATURES}
            feats["promoter_pledge_spiral_flag"] = 0
            feats["institutional_conviction_flag"] = 0
        feats["ticker"] = ticker
        records.append(feats)

    panel = pd.DataFrame(records)
    return panel[["ticker"] + GOVERNANCE_FEATURES]


def compute_governance_features_panel_vectorized(
    client: DataStoreClient,
    tickers: List[str],
    as_of: datetime,
    data_cache: Any = None,
    ohlcv_panel: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Vectorized alternative to `compute_governance_features_panel`: gathers
    every ticker's already-PIT-eligible shareholding rows into ONE
    DataFrame and computes latest/prior/QoQ/flags with pandas
    groupby/shift, instead of a per-ticker Python function call in a loop.

    Kept alongside (not replacing) `compute_governance_features_panel` /
    `compute_governance_features_panel_chunked` (see
    features/matrix_builder.py, commit 07d0122) as an alternative path —
    the sequential function remains the production baseline this is
    diffed against (tests/unit/test_governance_features.py's parity
    tests).

    Correctness notes (2026-07-29 model-review, mandatory fixes)
    --------------------------------------------------------------
    1. Ordering key: PIT eligibility is still `filing_date <= as_of`
       (unchanged — trusted entirely from the caller's already-filtered
       rows, same as the sequential function). Once eligible rows are
       gathered, "latest"/"prior" (QoQ) selection sorts by
       `quarter_end_date` — never by filing_date or merge order — exactly
       matching `compute_governance_features`'s own
       `history.sort_values("quarter_end_date")` before `iloc[-1]`/`iloc[-2]`.
    2. Dedup: checked directly against the live DuckDB `shareholding`
       table (2026-07-29) — zero duplicate (ticker, quarter_end_date) rows
       exist today. `drop_duplicates(subset=["ticker","quarter_end_date"],
       keep="last")` after a stable (mergesort) sort is applied
       defensively anyway, so a future duplicate resolves the same way a
       stable per-ticker sort + `iloc[-1]` would (last row in original
       insertion/API-return order for a tie).
    3. `shift(1)` (the "prior" row) is computed on the deduped,
       quarter_end_date-sorted, per-ticker history BEFORE any lookup —
       there is no second independent as-of lookup for "prior"; it is
       always the row immediately preceding "latest" in the same
       groupby-shift operation.
    4. `promoter_pledge_spiral_flag` preserves both the pre-sliced
       `ohlcv_panel` window path and the live `client.get_ohlcv` fallback
       (used only for tickers with zero rows in `ohlcv_panel`, or when
       `ohlcv_panel` itself is None — mirroring the sequential function's
       `ticker_ohlcv is not None and not ticker_ohlcv.empty` branch
       exactly), and the `len(window) >= 2` guard (fewer than 2 OHLCV rows
       -> flag stays 0, never NaN-propagates).

    Spec References
    ----------------
    SPEC-PIPE-003 (CRITICAL), SPEC-FEAT-002, SPEC-PIPE-004.
    """
    rows: List[Dict[str, Any]] = []
    no_data_tickers: List[str] = []
    for ticker in tickers:
        try:
            t_rows = (
                data_cache.get_shareholding(ticker, as_of) if data_cache is not None
                else client.get_shareholding_history(ticker, as_of)
            )
        except Exception as exc:
            logger.warning(f"governance vectorized: shareholding fetch failed for {ticker}: {exc}")
            t_rows = []
        if not t_rows:
            no_data_tickers.append(ticker)
            continue
        for r in t_rows:
            r2 = dict(r)
            r2["ticker"] = ticker
            rows.append(r2)

    def _no_data_frame(tks: List[str]) -> pd.DataFrame:
        frame = pd.DataFrame({"ticker": tks})
        for f in GOVERNANCE_FEATURES:
            frame[f] = np.nan
        if not frame.empty:
            frame["promoter_pledge_spiral_flag"] = 0
            frame["institutional_conviction_flag"] = 0
        return frame

    if not rows:
        return _no_data_frame(list(tickers))[["ticker"] + GOVERNANCE_FEATURES]

    df = pd.DataFrame(rows)
    df["quarter_end_date"] = pd.to_datetime(df["quarter_end_date"])

    # Points 1+2: stable sort by (ticker, quarter_end_date), then explicit
    # dedup tiebreak (see docstring).
    df = df.sort_values(["ticker", "quarter_end_date"], kind="mergesort")
    df = df.drop_duplicates(subset=["ticker", "quarter_end_date"], keep="last")

    # Point 3: shift(1) on the deduped/sorted per-ticker history BEFORE
    # any downstream lookup.
    change_cols = ["promoter_pct", "promoter_pledge", "fii_pct", "dii_pct", "mf_pct"]
    grouped = df.groupby("ticker", sort=False)
    for col in change_cols:
        if col not in df.columns:
            df[col] = np.nan
        df[f"_prior_{col}"] = grouped[col].shift(1)
    df["_has_prior"] = grouped.cumcount() > 0

    latest = df.groupby("ticker", sort=False).tail(1).set_index("ticker")

    def _chg(col: str) -> np.ndarray[Any, Any]:
        # Coerce for the same reason as fii/dii/mf below: these raw/shifted
        # columns can be object-dtype with real `None`, and `cur - prior`
        # is evaluated over the whole array before `both_present` masks it.
        cur = pd.to_numeric(latest[col], errors="coerce")
        prior = pd.to_numeric(latest[f"_prior_{col}"], errors="coerce")
        both_present = cur.notna() & prior.notna()
        return cast(np.ndarray[Any, Any], np.where(both_present, cur - prior, np.nan))

    result = pd.DataFrame(index=latest.index)
    for col in ["promoter_pct", "promoter_pledge", "fii_pct", "dii_pct", "mf_pct"]:
        result[col] = latest[col] if col in latest.columns else np.nan
    result["promoter_change_qoq"] = _chg("promoter_pct")
    result["promoter_pledge_change_qoq"] = _chg("promoter_pledge")
    result["fii_change_qoq"] = _chg("fii_pct")
    result["dii_change_qoq"] = _chg("dii_pct")
    result["mf_change_qoq"] = _chg("mf_pct")

    fii = pd.to_numeric(latest["fii_pct"], errors="coerce")
    dii = pd.to_numeric(latest["dii_pct"], errors="coerce")
    mf = pd.to_numeric(latest["mf_pct"], errors="coerce")
    any_present = fii.notna() | dii.notna() | mf.notna()
    result["institutional_ownership_pct"] = np.where(
        any_present, fii.fillna(0.0) + dii.fillna(0.0) + mf.fillna(0.0), np.nan
    )

    # The raw `_prior_*` columns (shift(1) of API-sourced dicts) can be
    # object-dtype with real Python `None` rather than NaN — comparing that
    # against the already-coerced `fii`/`dii`/`mf` float64 Series raises
    # TypeError: '>' not supported between 'float' and 'NoneType' (found
    # live 2026-07-30: every backfill date failed on this exact line).
    # `.notna()` alone doesn't prevent it since `&` still evaluates the `>`
    # over the whole array before masking — coerce first so NaN-vs-NaN
    # comparisons are safe no-ops instead.
    prior_fii = pd.to_numeric(latest["_prior_fii_pct"], errors="coerce")
    prior_dii = pd.to_numeric(latest["_prior_dii_pct"], errors="coerce")
    prior_mf = pd.to_numeric(latest["_prior_mf_pct"], errors="coerce")

    has_prior = latest["_has_prior"]
    fii_up = fii.notna() & prior_fii.notna() & (fii > prior_fii)
    dii_up = dii.notna() & prior_dii.notna() & (dii > prior_dii)
    mf_up = mf.notna() & prior_mf.notna() & (mf > prior_mf)
    result["institutional_conviction_flag"] = np.where(
        has_prior, (fii_up & dii_up & mf_up).astype(int), 0
    ).astype(int)

    # Point 4: promoter_pledge_spiral_flag.
    pledge = pd.to_numeric(latest["promoter_pledge"], errors="coerce")
    high_pledge = pledge.notna() & (pledge > PLEDGE_SPIRAL_THRESHOLD_PCT)
    spiral_flag = pd.Series(0, index=latest.index, dtype=int)
    candidates = list(latest.index[high_pledge])

    if candidates:
        cutoff = pd.Timestamp(as_of) - pd.Timedelta(days=PRICE_FALL_LOOKBACK_DAYS)
        as_of_ts = pd.Timestamp(as_of)

        covered_tickers = set()
        if ohlcv_panel is not None and not ohlcv_panel.empty:
            covered_tickers = set(ohlcv_panel[ohlcv_panel["ticker"].isin(candidates)]["ticker"].unique())
            window_all = ohlcv_panel[
                ohlcv_panel["ticker"].isin(candidates)
                & (ohlcv_panel["date"] >= cutoff)
                & (ohlcv_panel["date"] <= as_of_ts)
            ].sort_values(["ticker", "date"], kind="mergesort")
            if not window_all.empty:
                g = window_all.groupby("ticker", sort=False)
                counts = g.size()
                first_close = g["close"].first().astype(float)
                last_close = g["close"].last().astype(float)
                falling = (last_close < first_close) & (counts >= 2)
                for t, val in falling.items():
                    spiral_flag.loc[t] = int(bool(val))

        # Live fallback: only for candidates with zero rows in ohlcv_panel
        # (or ohlcv_panel is None entirely) — mirrors the sequential
        # function's `else: client.get_ohlcv(...)` branch exactly.
        needs_fallback = [t for t in candidates if t not in covered_tickers]
        for t in needs_fallback:
            price_rows = client.get_ohlcv(
                t, from_date=as_of - timedelta(days=PRICE_FALL_LOOKBACK_DAYS), to_date=as_of
            )
            if len(price_rows) >= 2:
                ordered = sorted(price_rows, key=lambda r: r["date"])
                spiral_flag.loc[t] = int(ordered[-1]["close"] < ordered[0]["close"])

    result["promoter_pledge_spiral_flag"] = spiral_flag
    result = result.reset_index()

    no_data_frame = _no_data_frame(no_data_tickers)
    full = pd.concat([result, no_data_frame], ignore_index=True) if not no_data_frame.empty else result
    full["promoter_pledge_spiral_flag"] = full["promoter_pledge_spiral_flag"].fillna(0).astype(int)
    full["institutional_conviction_flag"] = full["institutional_conviction_flag"].fillna(0).astype(int)
    full = full.set_index("ticker").loc[list(tickers)].reset_index()
    return full[["ticker"] + GOVERNANCE_FEATURES]
