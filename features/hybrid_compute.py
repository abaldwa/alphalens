"""
features/hybrid_compute.py

Stage 1 (per-ticker) and Stage 2 (date-assembly) compute functions for
the hybrid feature backfill engine (scripts/feature_backfill_hybrid.py).

Two-stage approach:

  Stage 1 — Ticker-first (compute_per_ticker):
    For each ticker: load full OHLCV history ONCE from DuckDB, compute all
    per-ticker features for ALL dates in-memory using pre-loaded caches.

    I/O reduction vs. date-first:
      OHLCV:         4785 bulk calls × 380k rows → 500 queries × 5k rows (720× ↓)
      Fundamentals:  2.39M API calls → 500 cache loads (already BackfillDataCache)
      F&O:           2.39M API calls → 500 DuckDB queries
      MF holdings:   2.39M parquet reads → 1 directory scan
      Computation:   rolling windows run once per ticker over 5k rows (not 4785×
                     repeated over 760-row slices)

  Stage 2 — Date assembly (assemble_date):
    For each date: slice staging dict (no I/O), apply cross-ticker features
    (sector z-scores, mf_crowdedness_rank, multibagger, macro), write parquet.

Cross-ticker features that MUST stay in Stage 2 (require all tickers together):
  - Sector z-scores of RATIO_FEATURES (fundamental)
  - mf_crowdedness_rank (percentile of mf_scheme_count within tier)
  - compute_multibagger_features (uses full universe OHLCV panel for RS ranks)
  - advance_decline_ratio / market_breadth_21d (needs all tickers' closes)

Everything else is per-ticker and computed in Stage 1.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from config.settings import FNO_ELIGIBILITY_LOOKBACK_DAYS
from features.advanced_technical import ADVANCED_TECHNICAL_FEATURES, compute_advanced_technical_features
from features.calendar import CALENDAR_FEATURES, compute_calendar_features
from features.corporate_action_features import CORPORATE_ACTION_FEATURES, compute_corporate_action_features
from features.deep_forensic import DEEP_FORENSIC_FEATURES, compute_deep_forensic_features
from features.fno_features import FNO_FEATURES, compute_fno_features
from features.fundamental import (
    FUNDAMENTAL_FEATURES,
    RATIO_FEATURES,
    _sector_relative_zscore,
    compute_fundamental_features,
)
from features.governance import GOVERNANCE_FEATURES, compute_governance_features
from features.intraday import INTRADAY_FEATURES, compute_intraday_features
from features.macro_features import MACRO_FEATURES, compute_macro_features
from features.mf_holdings import MF_HOLDINGS_FEATURES, compute_mf_holdings_features
from features.multibagger import MULTIBAGGER_FEATURES, compute_multibagger_features
from features.pattern_scores import PATTERN_FEATURES, compute_pattern_scores
from features.pnd_features import PND_FEATURES, compute_pnd_features
from features.real_economy_macro import REAL_ECONOMY_MACRO_FEATURES, compute_real_economy_macro_panel
from features.technical import BENCHMARK_TICKERS, CORE_TECHNICAL_FEATURES, compute_technical_features
from systems.ml_signal_engine.models.hmm.regime_detector import HMM_REGIME_FEATURES, compute_hmm_regime_features

logger = logging.getLogger(__name__)

# OHLCV columns carried through staging so Stage 2 can reconstruct the
# universe panel for cross-ticker computations (multibagger, macro breadth).
_OHLCV_PASS = ["open", "high", "low", "close", "volume", "delivery_pct"]

# All feature columns produced in Stage 1 (no cross-ticker dependency).
# MULTIBAGGER_FEATURES, MACRO_FEATURES, CALENDAR_FEATURES, and
# REAL_ECONOMY_MACRO_FEATURES are computed in Stage 2.
_STAGE1_FEATURE_COLS = (
    CORE_TECHNICAL_FEATURES
    + INTRADAY_FEATURES
    + HMM_REGIME_FEATURES
    + PND_FEATURES
    + ADVANCED_TECHNICAL_FEATURES
    + PATTERN_FEATURES
    + FUNDAMENTAL_FEATURES   # raw ratios; z-scores applied in Stage 2
    + GOVERNANCE_FEATURES
    + MF_HOLDINGS_FEATURES   # mf_crowdedness_rank=NaN; cross-sectional rank in Stage 2
    + CORPORATE_ACTION_FEATURES
    + FNO_FEATURES
    + DEEP_FORENSIC_FEATURES
)


# ── Stage 1 helpers ───────────────────────────────────────────────────────────

def _empty_staging(ticker: str, all_dates: List[pd.Timestamp]) -> pd.DataFrame:
    """Return an all-NaN staging DataFrame (used when a ticker has no OHLCV)."""
    df = pd.DataFrame({"date": all_dates})
    df["ticker"] = ticker
    for col in _STAGE1_FEATURE_COLS + _OHLCV_PASS:
        df[col] = np.nan
    return df


def _merge_ohlcv_features(
    all_dates: List[pd.Timestamp],
    ticker: str,
    ohlcv: pd.DataFrame,
    benchmark_wide: Optional[pd.DataFrame],
    compute_hmm: bool,
) -> pd.DataFrame:
    """
    Compute all OHLCV-derived features (rolling windows, HMM) for one ticker.

    Returns a DataFrame with one row per date in all_dates; dates that have
    no OHLCV (e.g., before listing) get NaN for all OHLCV-based features.
    """
    spine = pd.DataFrame({"date": all_dates, "ticker": ticker})

    def _safe(fn, cols, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            logger.warning("%s failed for %s: %s", fn.__name__, ticker, exc)
            return pd.DataFrame(columns=["date", "ticker"] + cols)

    technical = _safe(compute_technical_features, CORE_TECHNICAL_FEATURES, ohlcv, benchmark_wide)
    intraday = _safe(compute_intraday_features, INTRADAY_FEATURES, ohlcv)
    pnd = _safe(compute_pnd_features, PND_FEATURES, ohlcv)
    adv_tech = _safe(compute_advanced_technical_features, ADVANCED_TECHNICAL_FEATURES, ohlcv)
    patterns = _safe(compute_pattern_scores, PATTERN_FEATURES, ohlcv)
    if compute_hmm:
        # n_restarts=1, n_iter=50: ~10× faster than defaults (5×200) for backfill.
        # Regime labels are stable with 1 restart on long history (4785 days);
        # use the full 5×200 defaults only for offline research fits.
        hmm = _safe(compute_hmm_regime_features, HMM_REGIME_FEATURES, ohlcv,
                    n_restarts=1, n_iter=50)
    else:
        hmm = pd.DataFrame(columns=["date", "ticker"] + HMM_REGIME_FEATURES)

    # Left-join each feature set onto the date spine so every date appears
    # (dates without OHLCV get NaN for OHLCV-based features).
    for fdf in [technical, intraday, pnd, adv_tech, patterns, hmm]:
        if fdf.empty:
            continue
        fdf = fdf.drop(columns=["ticker"], errors="ignore")
        spine = spine.merge(fdf, on="date", how="left")

    # Carry raw OHLCV columns for Stage 2 cross-ticker computations.
    ohlcv_pass = ohlcv[["date"] + _OHLCV_PASS].copy()
    spine = spine.merge(ohlcv_pass, on="date", how="left")

    # Fill any feature column gaps introduced by partial merges.
    for col in (
        CORE_TECHNICAL_FEATURES
        + INTRADAY_FEATURES
        + HMM_REGIME_FEATURES
        + PND_FEATURES
        + ADVANCED_TECHNICAL_FEATURES
        + PATTERN_FEATURES
    ):
        if col not in spine.columns:
            spine[col] = np.nan
    for col in _OHLCV_PASS:
        if col not in spine.columns:
            spine[col] = np.nan

    return spine


# ── Stage 1 main function ─────────────────────────────────────────────────────

def compute_per_ticker(
    ticker: str,
    ohlcv: pd.DataFrame,
    fno_df: pd.DataFrame,
    benchmark_wide: Optional[pd.DataFrame],
    all_dates: List[pd.Timestamp],
    cache,
    mf_for_ticker: pd.DataFrame,
    listing_date: Optional[datetime],
    compute_hmm: bool = True,
) -> pd.DataFrame:
    """
    Stage 1: compute all per-ticker features for all dates.

    Parameters
    ----------
    ticker : str
    ohlcv : pd.DataFrame
        Full OHLCV history for this ticker (all available dates, not just
        the backfill window) — needed so rolling windows are warm at the
        start of the backfill period.
    fno_df : pd.DataFrame
        Full F&O history for this ticker loaded from DuckDB directly. May
        be empty (non-F&O-eligible tickers). Columns must include trade_date.
    benchmark_wide : pd.DataFrame or None
        Wide-format benchmark OHLCV (date, nifty50_close, ...) pre-built
        from benchmark ETF OHLCV.  Passed to compute_technical_features for
        the RS-vs-benchmark categories.
    all_dates : list of pd.Timestamp
        Sorted ascending list of trading dates to compute features for.
    cache : BackfillDataCache
        Pre-loaded fundamentals, shareholding, corp_actions for all tickers.
    mf_for_ticker : pd.DataFrame
        All MF holdings rows for this ticker (pre-loaded, unfiltered by
        availability_date). PIT filtering is applied per-date below.
    listing_date : datetime or None
        Used for ipo_lockin_expiry_proximity / ipo_listing_age_months.
    compute_hmm : bool
        If False, HMM regime features are NaN (same as --no-hmm flag).

    Returns
    -------
    pd.DataFrame
        Columns: date, ticker, all _STAGE1_FEATURE_COLS, _OHLCV_PASS.
        One row per date in all_dates (NaN for dates without OHLCV).
    """
    if ohlcv.empty:
        logger.debug("No OHLCV for %s — returning all-NaN staging", ticker)
        return _empty_staging(ticker, all_dates)

    ohlcv = ohlcv.copy()
    ohlcv["date"] = pd.to_datetime(ohlcv["date"])
    ohlcv["ticker"] = ticker
    if "delivery_pct" not in ohlcv.columns:
        ohlcv["delivery_pct"] = np.nan
    ohlcv = ohlcv.sort_values("date").reset_index(drop=True)

    # ── OHLCV-based features (rolling windows computed ONCE for all dates) ──
    spine = _merge_ohlcv_features(all_dates, ticker, ohlcv, benchmark_wide, compute_hmm)

    # ── Pre-sort cache data; build numpy arrays for O(log n) searchsorted slicing ──
    fundamentals_raw = cache._fundamentals.get(ticker, [])
    shareholding_raw = cache._shareholding.get(ticker, [])
    corp_actions_raw = cache._corp_actions.get(ticker, [])

    if fundamentals_raw:
        fund_df = pd.DataFrame(fundamentals_raw)
        fund_df["announcement_date"] = pd.to_datetime(fund_df["announcement_date"])
        fund_df = fund_df.sort_values("announcement_date").reset_index(drop=True)
        _fund_date_arr = fund_df["announcement_date"].values
        _fund_records: List[Dict] = fund_df.to_dict("records")
    else:
        fund_df = pd.DataFrame()
        _fund_date_arr = np.array([], dtype="datetime64[ns]")
        _fund_records = []

    if shareholding_raw:
        share_df = pd.DataFrame(shareholding_raw)
        share_df["filing_date"] = pd.to_datetime(share_df["filing_date"])
        share_df = share_df.sort_values("filing_date").reset_index(drop=True)
        _share_date_arr = share_df["filing_date"].values
        _share_records: List[Dict] = share_df.to_dict("records")
    else:
        share_df = pd.DataFrame()
        _share_date_arr = np.array([], dtype="datetime64[ns]")
        _share_records = []

    if not fno_df.empty:
        fno_df = fno_df.copy()
        fno_df["trade_date"] = pd.to_datetime(fno_df["trade_date"])
        fno_df = fno_df.sort_values("trade_date").reset_index(drop=True)
        _fno_date_arr = fno_df["trade_date"].values
    else:
        _fno_date_arr = np.array([], dtype="datetime64[ns]")

    if not mf_for_ticker.empty:
        mf_for_ticker = mf_for_ticker.copy()
        mf_for_ticker["availability_date"] = pd.to_datetime(mf_for_ticker["availability_date"])
        mf_for_ticker = mf_for_ticker.sort_values("availability_date").reset_index(drop=True)
        _mf_date_arr = mf_for_ticker["availability_date"].values
    else:
        _mf_date_arr = np.array([], dtype="datetime64[ns]")

    # ── Per-date loop for PIT-sensitive features ──
    # Uses np.searchsorted on pre-sorted date arrays — O(log n) per iteration
    # instead of O(n) boolean masks. Key speedup for tickers with large F&O
    # histories (e.g. 679k rows × 4785 dates = ~3B comparisons avoided).
    fund_rows: List[Dict] = []
    gov_rows: List[Dict] = []
    corp_rows: List[Dict] = []
    forensic_rows: List[Dict] = []
    fno_rows: List[Dict] = []
    mf_rows: List[Dict] = []

    for date_ts in all_dates:
        date_np = date_ts.to_datetime64()
        as_of_dt = date_ts.to_pydatetime()

        # Binary-search PIT slices — O(log n) per date
        pit_fund: List[Dict] = _fund_records[:int(np.searchsorted(_fund_date_arr, date_np, side="right"))]
        pit_share: List[Dict] = _share_records[:int(np.searchsorted(_share_date_arr, date_np, side="right"))]
        hi_mf = int(np.searchsorted(_mf_date_arr, date_np, side="right"))
        pit_mf = mf_for_ticker.iloc[:hi_mf] if hi_mf > 0 else pd.DataFrame()

        # Fundamental (raw ratios; z-scores applied cross-ticker in Stage 2)
        try:
            f: Dict[str, Any] = compute_fundamental_features(
                None, ticker, as_of_dt,
                pre_loaded_rows=pit_fund,
                ticker_ohlcv=ohlcv,
            )
        except Exception as exc:
            logger.debug("fundamental failed for %s on %s: %s", ticker, date_ts.date(), exc)
            f = {col: np.nan for col in FUNDAMENTAL_FEATURES}
        f["date"] = date_ts
        fund_rows.append(f)

        # Governance
        try:
            g: Dict[str, Any] = compute_governance_features(
                None, ticker, as_of_dt,
                pre_loaded_rows=pit_share,
                ticker_ohlcv=ohlcv,
            )
        except Exception as exc:
            logger.debug("governance failed for %s on %s: %s", ticker, date_ts.date(), exc)
            g = {col: np.nan for col in GOVERNANCE_FEATURES}
        g["date"] = date_ts
        gov_rows.append(g)

        # Corporate actions
        try:
            c: Dict[str, Any] = compute_corporate_action_features(
                None, ticker, as_of_dt,
                listing_date=listing_date,
                pre_loaded_actions=corp_actions_raw,
                pre_loaded_fundamentals=pit_fund,
                ticker_ohlcv=ohlcv,
            )
        except Exception as exc:
            logger.debug("corp_action failed for %s on %s: %s", ticker, date_ts.date(), exc)
            c = {col: np.nan for col in CORPORATE_ACTION_FEATURES}
        c["date"] = date_ts
        corp_rows.append(c)

        # Deep forensic
        try:
            df_f: Dict[str, Any] = compute_deep_forensic_features(
                None, ticker, as_of_dt,
                pre_loaded_fundamentals=pit_fund,
                pre_loaded_shareholding=pit_share,
            )
        except Exception as exc:
            logger.debug("deep_forensic failed for %s on %s: %s", ticker, date_ts.date(), exc)
            df_f = {col: np.nan for col in DEEP_FORENSIC_FEATURES}
        df_f["date"] = date_ts
        forensic_rows.append(df_f)

        # F&O: searchsorted window + DataFrame slice avoids dict roundtrip for large sets
        fno_start_np = (date_ts - pd.Timedelta(days=FNO_ELIGIBILITY_LOOKBACK_DAYS)).to_datetime64()
        lo_fno = int(np.searchsorted(_fno_date_arr, fno_start_np, side="left"))
        hi_fno = int(np.searchsorted(_fno_date_arr, date_np, side="right"))
        fno_slice = fno_df.iloc[lo_fno:hi_fno] if (not fno_df.empty and lo_fno < hi_fno) else pd.DataFrame()
        try:
            fn: Dict[str, Any] = compute_fno_features(
                None, ticker, as_of_dt,
                pre_loaded_df=fno_slice if not fno_slice.empty else None,
            )
        except Exception as exc:
            logger.debug("fno failed for %s on %s: %s", ticker, date_ts.date(), exc)
            fn = {col: np.nan for col in FNO_FEATURES}
        fn["date"] = date_ts
        fno_rows.append(fn)

        # MF holdings
        try:
            mf: Dict[str, Any] = compute_mf_holdings_features(ticker, as_of_dt, pit_mf)
        except Exception as exc:
            logger.debug("mf_holdings failed for %s on %s: %s", ticker, date_ts.date(), exc)
            mf = {col: np.nan for col in MF_HOLDINGS_FEATURES}
        mf["date"] = date_ts
        mf_rows.append(mf)

    # Build per-date DataFrames and merge onto spine
    def _to_df(rows: List[Dict], cols: List[str]) -> pd.DataFrame:
        df = pd.DataFrame(rows)
        for col in cols:
            if col not in df.columns:
                df[col] = np.nan
        return df[["date"] + cols]

    pit_frames = [
        _to_df(fund_rows, FUNDAMENTAL_FEATURES),
        _to_df(gov_rows, GOVERNANCE_FEATURES),
        _to_df(corp_rows, CORPORATE_ACTION_FEATURES),
        _to_df(forensic_rows, DEEP_FORENSIC_FEATURES),
        _to_df(fno_rows, FNO_FEATURES),
        _to_df(mf_rows, MF_HOLDINGS_FEATURES),
    ]
    for extra in pit_frames:
        spine = spine.merge(extra, on="date", how="left")

    spine["ticker"] = ticker
    return spine


# ── Stage 2: date assembly ────────────────────────────────────────────────────

def build_benchmark_wide(benchmark_ohlcv: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Pivot long-format benchmark OHLCV into the wide shape compute_technical_features
    expects (date, nifty50_close, nifty100_close, nifty500_close).

    Same logic as matrix_builder._build_benchmark_wide — reproduced here to
    avoid importing a private function from matrix_builder.
    """
    if benchmark_ohlcv.empty:
        return None
    wide = benchmark_ohlcv.pivot_table(index="date", columns="ticker", values="close").reset_index()
    rename = {ticker_sym: f"{name}_close" for name, ticker_sym in BENCHMARK_TICKERS.items()}
    wide = wide.rename(columns=rename)
    for name in BENCHMARK_TICKERS:
        col = f"{name}_close"
        if col not in wide.columns:
            wide[col] = np.nan
    return wide


def assemble_date(
    date: pd.Timestamp,
    staging: Dict[str, pd.DataFrame],
    benchmark_ohlcv: pd.DataFrame,
    sector_map: Dict[str, str],
    tier_map: Dict[str, str],
    macro_all: pd.DataFrame,
    tickers: List[str],
    universe_ohlcv_panel: Optional[pd.DataFrame] = None,
    mb_precomputed: Optional[Dict] = None,
    real_eco_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Stage 2: assemble one date's feature matrix from per-ticker staging data.

    Parameters
    ----------
    date : pd.Timestamp
        The date whose matrix to assemble.
    staging : dict
        {ticker: DataFrame} from Stage 1. Each DataFrame has columns
        date, ticker, _STAGE1_FEATURE_COLS, _OHLCV_PASS.
    benchmark_ohlcv : pd.DataFrame
        Full benchmark ETF OHLCV (all dates). Used to rebuild benchmark_wide
        for the 760-day window needed by compute_multibagger_features, and
        to supply nifty50_ohlcv for compute_macro_features.
    sector_map : dict
        ticker -> sector string.
    tier_map : dict
        ticker -> tier string (used for mf_crowdedness_rank).
    macro_all : pd.DataFrame
        Pre-loaded macro_indicators rows for the full date range.
    tickers : list of str
    universe_ohlcv_panel : pd.DataFrame, optional
        Pre-loaded full-history OHLCV panel (date, ticker, open, high, low,
        close, volume, delivery_pct) for ALL tickers and ALL dates. When
        provided, step 4 uses this instead of reconstructing from the staging
        dict — required for correctness in chunked Stage 2 runs where staging
        only covers a subset of dates, and also much faster (single DataFrame
        filter vs 2492 per-ticker dict lookups + concat).
        Universe ticker list (determines row order in output matrix).

    Returns
    -------
    pd.DataFrame
        One row per ticker, columns: date, ticker, ALL_FEATURE_COLUMNS.
        Returns an empty DataFrame if no staging data exists for `date`.
    """
    # 1. Collect per-ticker rows for this date from staging
    rows = []
    for ticker in tickers:
        df = staging.get(ticker)
        if df is None:
            continue
        row = df[df["date"] == date]
        if not row.empty:
            rows.append(row)
    if not rows:
        return pd.DataFrame()

    panel = pd.concat(rows, ignore_index=True)

    # 2. Sector z-scores (cross-ticker): apply to raw RATIO_FEATURES
    panel["sector"] = panel["ticker"].map(sector_map).fillna("UNKNOWN")
    panel = _sector_relative_zscore(panel, RATIO_FEATURES, sector_col="sector")

    # 3. mf_crowdedness_rank: percentile of mf_scheme_count within tier group
    panel["tier"] = panel["ticker"].map(tier_map).fillna("UNKNOWN")
    if "mf_scheme_count" in panel.columns:
        panel["mf_crowdedness_rank"] = panel.groupby("tier")["mf_scheme_count"].rank(pct=True)

    # 4. Reconstruct universe OHLCV window for multibagger + macro breadth
    window_start = date - pd.Timedelta(days=760)
    ohlcv_cols = ["date", "ticker"] + _OHLCV_PASS
    if universe_ohlcv_panel is not None:
        # Fast path: filter the pre-loaded full-history panel (single boolean mask,
        # no per-ticker loop). Required for chunked Stage 2 where staging only
        # covers a date subset — reconstructing from staging would give an
        # incomplete 760-day window and incorrect multibagger features.
        universe_panel = universe_ohlcv_panel[
            (universe_ohlcv_panel["date"] >= window_start)
            & (universe_ohlcv_panel["date"] <= date)
        ][ohlcv_cols].copy()
    else:
        frames = []
        for ticker in tickers:
            df = staging.get(ticker)
            if df is None:
                continue
            window = df[(df["date"] >= window_start) & (df["date"] <= date)][ohlcv_cols]
            if not window.empty:
                frames.append(window)
        universe_panel = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=ohlcv_cols)

    # 5. Multibagger features
    if mb_precomputed is not None and date in mb_precomputed:
        # Fast path: use precomputed result from run_stage2 (computed once per chunk).
        # mf/governance snapshot-derived features are already merged at precompute time.
        try:
            today_mb = mb_precomputed[date]
            panel = panel.merge(today_mb, on="ticker", how="left")
        except Exception as exc:
            logger.warning("multibagger merge failed for %s: %s", date.date(), exc)
            for col in MULTIBAGGER_FEATURES:
                if col not in panel.columns:
                    panel[col] = np.nan
    else:
        # Slow path: compute on the fly (used when mb_precomputed is not available)
        bm_window = benchmark_ohlcv[
            (benchmark_ohlcv["date"] >= window_start) & (benchmark_ohlcv["date"] <= date)
        ]
        bm_wide = build_benchmark_wide(bm_window)

        if not universe_panel.empty:
            try:
                mf_snap = panel[["ticker"] + MF_HOLDINGS_FEATURES].copy()
                gov_snap = panel[["ticker"] + GOVERNANCE_FEATURES].copy()
                mb = compute_multibagger_features(
                    universe_panel, bm_wide, sector_map,
                    mf_snapshot=mf_snap, governance_snapshot=gov_snap,
                )
                today_mb = mb[mb["date"] == date].drop(columns=["date"])
                panel = panel.merge(today_mb, on="ticker", how="left")
            except Exception as exc:
                logger.warning("multibagger failed for %s: %s", date.date(), exc)
                for col in MULTIBAGGER_FEATURES:
                    panel[col] = np.nan
        else:
            for col in MULTIBAGGER_FEATURES:
                panel[col] = np.nan

    # 6. Macro features (needs pre-loaded macro_all + benchmark close + universe close)
    macro_window = macro_all[macro_all["date"] <= date].copy()
    nifty50_sym = BENCHMARK_TICKERS["nifty50"]
    nifty50_hist = benchmark_ohlcv[benchmark_ohlcv["ticker"] == nifty50_sym][["date", "close"]].copy()
    universe_close = (
        universe_panel[["date", "ticker", "close"]].copy()
        if not universe_panel.empty else None
    )
    try:
        macro_row = compute_macro_features(date, macro_window, nifty50_hist, universe_close)
        panel = panel.merge(macro_row, on="date", how="left")
    except Exception as exc:
        logger.warning("macro features failed for %s: %s", date.date(), exc)
        for col in MACRO_FEATURES:
            panel[col] = np.nan

    # 7. Calendar features (pure computation — no I/O)
    try:
        cal_row = compute_calendar_features(date)
        panel = panel.merge(cal_row, on="date", how="left")
    except Exception as exc:
        logger.warning("calendar features failed for %s: %s", date.date(), exc)
        for col in CALENDAR_FEATURES:
            panel[col] = np.nan

    # 8. Real-economy macro
    if real_eco_df is not None:
        # Fast path: filter pre-loaded DataFrame (no disk I/O per date)
        try:
            from features.real_economy_macro import REAL_ECONOMY_MACRO_FEATURES as _RE_FEATS
            avail = real_eco_df[real_eco_df["availability_date"] <= date]
            values: Dict = {}
            for feat in _RE_FEATS:
                rows_f = avail[avail["feature_name"] == feat]
                if rows_f.empty:
                    values[feat] = np.nan
                else:
                    values[feat] = float(rows_f.sort_values("reference_month_end").iloc[-1]["value"])
            real_eco = pd.DataFrame([{"ticker": t, **values} for t in panel["ticker"]])
            panel = panel.merge(real_eco, on="ticker", how="left")
        except Exception as exc:
            logger.warning("real_economy_macro (cached) failed for %s: %s", date.date(), exc)
            for col in REAL_ECONOMY_MACRO_FEATURES:
                if col not in panel.columns:
                    panel[col] = np.nan
    else:
        # Slow path: reads parquet from disk on every call
        try:
            real_eco = compute_real_economy_macro_panel(date, tickers)
            panel = panel.merge(real_eco, on="ticker", how="left")
        except Exception as exc:
            logger.warning("real_economy_macro failed for %s: %s", date.date(), exc)
            for col in REAL_ECONOMY_MACRO_FEATURES:
                panel[col] = np.nan

    # Drop temporary metadata columns used only for cross-ticker computations
    panel = panel.drop(columns=["sector", "tier"], errors="ignore")

    return panel
