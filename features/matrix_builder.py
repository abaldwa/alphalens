"""
features/matrix_builder.py

Phase: 1.1 (Core Feature Computation); Phase 2 set in 2.3/2.4; Phase 3 in 3.1
Specs: SPEC-SOLID-005, SPEC-DS-005, SPEC-DS-007, SPEC-PIPE-004, SPEC-PIPE-005, SPEC-FEAT-001
Owner: Platform / Features
Consumers: ingestion/scheduler/daily_pipeline (compute_features step), systems/ml_signal_engine

Assembles the daily feature matrix by orchestrating every Phase 1 + Phase 2
+ Phase 3 feature category module. Phase 3 adds:
  advanced_technical.py (18): wavelet, hurst, entropy, fracdiff, complexity
  pattern_scores.py (6): chart pattern recognition scores
  real_economy_macro.py (10): GST, PMI, IIP, auto sales, cement, power,
    rail freight, UPI, bank credit
  deep_forensic.py (28): Groups D–I forensic features

Phase 1+2 total: 235 columns (documented gap vs. "268" prompt header — see
previous docstring versions and BuildLog.md for per-category reconciliation).
Phase 3 adds 62 additional features → target 330 (the project's 330-feature
catalog from 01_features.md).

This module assembles whatever the category modules produce (SOLID-002:
open/closed). Adding future categories requires only extending
ALL_FEATURE_COLUMNS here; the compute core is untouched.

SPEC-SOLID-005 (Dependency Inversion): all OHLCV/fundamentals/
shareholding/corporate-actions/F&O access goes through DataStoreClient
(an HTTP client over the DataStore API), never direct DuckDB — `client`
is constructor-injectable so tests can substitute a fake. Two documented
SPEC-DS-002 exceptions (direct reads permitted "within ingestion and
feature layers" when no API endpoint exists yet for that store):
macro_indicators (features/macro_features.py's load_macro_indicators())
and MF holdings (features/mf_holdings.py reads
datastore/normalised/mf_holdings/*.parquet directly).
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

from config.settings import DELIVERY_PCT_RANGE, FEATURES_DAILY_DIR, NULL_RATE_ALERT_THRESHOLD, RATIO_FEATURE_RANGE
from config.universe import load_universe
from datastore.client import DataStoreClient
from features.calendar import CALENDAR_FEATURES, compute_calendar_features
from features.corporate_action_features import CORPORATE_ACTION_FEATURES, compute_corporate_action_features_panel
from features.fno_features import FNO_FEATURES, compute_fno_features_panel
from features.fundamental import FUNDAMENTAL_FEATURES, compute_fundamental_features_panel
from features.governance import GOVERNANCE_FEATURES, compute_governance_features_panel
from features.intraday import INTRADAY_FEATURES, compute_intraday_features
from features.macro_features import MACRO_FEATURES, compute_macro_features, load_macro_indicators
from features.mf_holdings import MF_HOLDINGS_FEATURES, compute_mf_holdings_features_panel
from features.multibagger import MULTIBAGGER_FEATURES, compute_multibagger_features
from features.pnd_features import PND_FEATURES, compute_pnd_features
from features.technical import BENCHMARK_TICKERS, CORE_TECHNICAL_FEATURES, compute_technical_features
from features.advanced_technical import ADVANCED_TECHNICAL_FEATURES, compute_advanced_technical_features
from features.pattern_scores import PATTERN_FEATURES, compute_pattern_scores
from features.real_economy_macro import REAL_ECONOMY_MACRO_FEATURES, compute_real_economy_macro_panel
from features.deep_forensic import DEEP_FORENSIC_FEATURES, compute_deep_forensic_features_panel
from systems.ml_signal_engine.models.hmm.regime_detector import HMM_REGIME_FEATURES, compute_hmm_regime_features

logger = logging.getLogger(__name__)

ALL_FEATURE_COLUMNS: List[str] = (
    CORE_TECHNICAL_FEATURES
    + INTRADAY_FEATURES
    + CALENDAR_FEATURES
    + HMM_REGIME_FEATURES
    + MACRO_FEATURES
    + PND_FEATURES
    + FUNDAMENTAL_FEATURES
    + GOVERNANCE_FEATURES
    + MF_HOLDINGS_FEATURES
    + CORPORATE_ACTION_FEATURES
    + FNO_FEATURES
    + MULTIBAGGER_FEATURES
    # Phase 3 additions (+62 features → target 330 total)
    + ADVANCED_TECHNICAL_FEATURES
    + PATTERN_FEATURES
    + REAL_ECONOMY_MACRO_FEATURES
    + DEEP_FORENSIC_FEATURES
)

# ~2 calendar years: comfortably over 252 *trading* days even allowing for
# the occasional real-world ingestion gap (a missed scraper run, an
# unlisted NSE holiday) that would otherwise silently starve the 252-row
# rolling windows (dist_from_52w_high/low, sma_200_ratio, etc.) of data
# they'd technically have under a tighter, gap-free assumption
# (SPEC-FEAT-001). Cost is still one bounded API call per ticker.
LOOKBACK_CALENDAR_DAYS = 760

_EMPTY_OHLCV_COLUMNS = ["date", "ticker", "open", "high", "low", "close", "volume", "delivery_pct"]


def _fetch_ohlcv_panel(
    client: DataStoreClient, tickers: List[str], from_date: datetime, to_date: datetime,
    _bulk_panel: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Fetch OHLCV history for many tickers.

    If a pre-fetched bulk panel is provided (from client.get_ohlcv_bulk called
    once for the whole universe), filters it to the requested tickers — one bulk
    HTTP call instead of one per ticker (build_feature_matrix does this).
    Falls back to per-ticker calls when called standalone (tests, single tickers).
    """
    if _bulk_panel is not None:
        panel = _bulk_panel[_bulk_panel["ticker"].isin(set(tickers))].copy()
        if panel.empty:
            return pd.DataFrame(columns=_EMPTY_OHLCV_COLUMNS)
        if "delivery_pct" not in panel.columns:
            panel["delivery_pct"] = np.nan
        return panel

    # Fallback: per-ticker (for tests or callers that pass no bulk panel)
    frames = []
    for ticker in tickers:
        try:
            rows = client.get_ohlcv(ticker, from_date, to_date)
        except Exception as exc:
            logger.warning(f"OHLCV fetch failed for {ticker}: {exc}")
            continue
        if rows:
            frames.append(pd.DataFrame(rows))

    if not frames:
        return pd.DataFrame(columns=_EMPTY_OHLCV_COLUMNS)

    panel = pd.concat(frames, ignore_index=True)
    panel["date"] = pd.to_datetime(panel["date"])
    if "delivery_pct" not in panel.columns:
        panel["delivery_pct"] = np.nan
    return panel


def _build_benchmark_wide(benchmark_panel: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Pivot the long-format benchmark OHLCV panel into the wide shape technical.py expects."""
    if benchmark_panel.empty:
        return None

    wide = benchmark_panel.pivot_table(index="date", columns="ticker", values="close").reset_index()
    rename = {ticker: f"{name}_close" for name, ticker in BENCHMARK_TICKERS.items()}
    wide = wide.rename(columns=rename)
    for name in BENCHMARK_TICKERS:
        col = f"{name}_close"
        if col not in wide.columns:
            wide[col] = np.nan
    return wide


def _extract_target_date_panel(
    panel: pd.DataFrame, target_date: pd.Timestamp, feature_columns: List[str]
) -> pd.DataFrame:
    """Return the rows for target_date, tolerating feature panels that have no date column."""
    if panel.empty:
        return pd.DataFrame(columns=["ticker"] + feature_columns)

    if "date" not in panel.columns:
        out = panel.copy()
        out["date"] = target_date
        return out[["ticker"] + feature_columns]

    filtered = panel[panel["date"] == target_date]
    if filtered.empty:
        return pd.DataFrame(columns=["ticker"] + feature_columns)
    return filtered.drop(columns=["date"])


def _validate_feature_matrix(matrix: pd.DataFrame) -> None:
    """
    SPEC-PIPE-005 quality gates: null rate, delivery_pct range, ratio range.

    Logged as warnings (a soft flag for the drift/quality monitor to act
    on), not raised — the hard completeness gate is SPEC-SYS-003's
    >= 450/500-stock check, which lives in the pipeline orchestrator, not
    here.
    """
    if matrix.empty:
        logger.warning("Feature matrix is empty — nothing to validate")
        return

    null_rates = matrix[ALL_FEATURE_COLUMNS].isna().mean()
    flagged = null_rates[null_rates > NULL_RATE_ALERT_THRESHOLD]
    if not flagged.empty:
        logger.warning(
            f"{len(flagged)}/{len(ALL_FEATURE_COLUMNS)} features exceed "
            f"{NULL_RATE_ALERT_THRESHOLD:.0%} null rate: {flagged.round(3).to_dict()}"
        )

    if "delivery_pct" in matrix.columns:
        lo, hi = DELIVERY_PCT_RANGE
        dp = matrix["delivery_pct"].dropna()
        bad = dp[(dp < lo) | (dp > hi)]
        if not bad.empty:
            logger.warning(f"{len(bad)} delivery_pct values outside [{lo}, {hi}]")

    # SPEC-PIPE-005's [0.1, 10.0] range check is meant for *price* ratios
    # (close/SMA, close/EMA, etc.) — scoped to CORE_TECHNICAL_FEATURES so it
    # doesn't false-positive on differently-shaped "_ratio"-suffixed
    # features elsewhere (e.g. macro's advance_decline_ratio, which can
    # legitimately be > 10 or < 0.1 in a lopsided breadth day).
    lo, hi = RATIO_FEATURE_RANGE
    _FRACTION_RATIO_COLS = {"body_to_range_ratio"}  # bounded [0,1]; not a price ratio
    for col in [c for c in CORE_TECHNICAL_FEATURES if c.endswith("_ratio") and c not in _FRACTION_RATIO_COLS]:
        vals = matrix[col].dropna()
        bad = vals[(vals < lo) | (vals > hi)]
        if not bad.empty:
            logger.warning(f"{len(bad)}/{len(vals)} '{col}' values outside [{lo}, {hi}]")


def _save_feature_matrix(matrix: pd.DataFrame, target_date: pd.Timestamp) -> Path:
    """SPEC-DS-005/007: feature Parquets live at datastore/features/daily/YYYY-MM-DD.parquet."""
    FEATURES_DAILY_DIR.mkdir(parents=True, exist_ok=True)
    out_path = FEATURES_DAILY_DIR / f"{target_date.date().isoformat()}.parquet"
    matrix.to_parquet(out_path, index=False)
    logger.info(f"Wrote feature matrix to {out_path} ({len(matrix)} rows x {len(ALL_FEATURE_COLUMNS)} features)")
    return out_path


def build_feature_matrix(
    date: str,
    tickers: List[str],
    client: Optional[DataStoreClient] = None,
    save: bool = True,
    compute_hmm: bool = True,
    data_cache=None,
    hmm_workers: int = 1,
) -> pd.DataFrame:
    """
    Build the full daily feature matrix for `tickers` on `date`.

    Parameters
    ----------
    date : str
        Target date, "YYYY-MM-DD".
    tickers : list of str
        Universe to build features for (e.g. config.universe.get_tickers()).
    client : DataStoreClient, optional
        Injected for testability (SPEC-SOLID-005); defaults to a real
        DataStoreClient hitting config.settings.DATASTORE_API_BASE_URL.
    save : bool
        If True (default), writes the result to
        datastore/features/daily/YYYY-MM-DD.parquet (SPEC-DS-005).
    compute_hmm : bool
        If True (default), fits one HMMRegimeDetector per ticker (M-01) to
        populate HMM_REGIME_FEATURES. This is the most expensive step here
        by far (a model fit per ticker, not vectorized arithmetic — see
        systems/ml_signal_engine/models/hmm/regime_detector.py's module
        docstring) — set False to skip it (HMM_REGIME_FEATURES columns
        come back all-NaN) for fast iteration/tests that don't need regime
        features.
    hmm_workers : int
        Forwarded to compute_hmm_regime_features's n_workers (default 1 =
        original single-process behavior). See that function's docstring
        for the OOM history behind not defaulting this higher.

    Returns
    -------
    pd.DataFrame
        One row per ticker (even if its OHLCV fetch failed — all-NaN
        feature row in that case), columns: date, ticker +
        ALL_FEATURE_COLUMNS (235 cols — see module docstring for the
        documented gap vs. this phase's build prompt's literal "268").

    Spec References
    ----------------
    SPEC-SOLID-005: OHLCV reached exclusively through DataStoreClient/API.
    SPEC-DS-005: output path convention for Store 3 (Features).
    SPEC-PIPE-005: null-rate / range validation (see _validate_feature_matrix).
    SPEC-FEAT-001: stocks with insufficient history get NaN, not an error.
    02_models.md M-01: HMM regime features, "today only, not backfill" in
    production (CLAUDE.md STEP 10) — this function still computes the full
    historical panel and extracts `date`'s row, the same shape as every
    other category here, since the daily pipeline only ever calls this for
    "today" in practice.

    PIT Assumptions
    ----------------
    OHLCV is PITRule.NONE (always same-day knowable). This function does
    not touch fundamentals/shareholding, so no announcement_date/
    filing_date PIT logic is needed here. Each ticker's HMM fit uses only
    that ticker's own OHLCV through `date` (see regime_detector.py).

    Raises
    ------
    ValueError
        If `tickers` is empty.
    """
    if not tickers:
        raise ValueError("tickers must be non-empty")

    target_date = pd.Timestamp(date)
    from_date = (target_date - pd.Timedelta(days=LOOKBACK_CALENDAR_DAYS)).to_pydatetime()
    to_date = target_date.to_pydatetime()

    client = client or DataStoreClient()

    # One bulk HTTP call for all tickers (universe + benchmarks) when the
    # client supports it; otherwise fall back to the existing per-ticker path.
    bulk_panel = None
    bulk_loader = getattr(client, "get_ohlcv_bulk", None)
    if callable(bulk_loader):
        try:
            bulk_panel = bulk_loader(from_date, to_date)
        except Exception as exc:
            logger.warning("Bulk OHLCV fetch failed, falling back to per-ticker fetch: %s", exc)

    universe_panel = _fetch_ohlcv_panel(client, tickers, from_date, to_date, _bulk_panel=bulk_panel)
    benchmark_panel = _fetch_ohlcv_panel(
        client, list(BENCHMARK_TICKERS.values()), from_date, to_date, _bulk_panel=bulk_panel
    )
    benchmark_wide = _build_benchmark_wide(benchmark_panel)

    if universe_panel.empty:
        # 2026-07-07 incident: this branch used to degrade to an all-NaN
        # matrix (still checkpointed 'success') whenever the DataStore API
        # was unreachable — SPEC-FEAT-001's "stocks with insufficient
        # history get NaN, not an error" is meant for a handful of tickers
        # with genuinely no data, not the entire universe returning zero
        # rows. Zero-of-N is never a legitimate market outcome; it always
        # means the OHLCV source itself is broken (API down, DB
        # unreachable), so this must hard-fail rather than silently write
        # a garbage feature matrix that every downstream model then trains/
        # scores on. See step_sanity_check's 100%-NaN-column check for the
        # equivalent guard on the write side of this same failure mode.
        raise RuntimeError(
            f"No OHLCV data returned for any of {len(tickers)} tickers on {date} — "
            "the DataStore API is very likely unreachable. Refusing to write an "
            "all-NaN feature matrix."
        )
    else:
        technical = compute_technical_features(universe_panel, benchmark_wide)
        intraday = compute_intraday_features(universe_panel)
        hmm = (
            compute_hmm_regime_features(universe_panel, n_workers=hmm_workers)
            if compute_hmm
            else pd.DataFrame(columns=["date", "ticker"] + HMM_REGIME_FEATURES)
        )
        pnd = compute_pnd_features(universe_panel)
    today_technical = _extract_target_date_panel(technical, target_date, CORE_TECHNICAL_FEATURES)
    today_intraday = _extract_target_date_panel(intraday, target_date, INTRADAY_FEATURES)
    today_hmm = _extract_target_date_panel(hmm, target_date, HMM_REGIME_FEATURES)
    today_pnd = _extract_target_date_panel(pnd, target_date, PND_FEATURES)

    calendar_row = compute_calendar_features(target_date)

    macro_indicators = load_macro_indicators(target_date)
    nifty50_ticker = BENCHMARK_TICKERS["nifty50"]
    nifty50_hist = benchmark_panel.loc[benchmark_panel["ticker"] == nifty50_ticker, ["date", "close"]]
    universe_close = universe_panel[["date", "ticker", "close"]] if not universe_panel.empty else None
    macro_row = compute_macro_features(target_date, macro_indicators, nifty50_hist, universe_close)

    # SPEC-PIPE-003 (CRITICAL): fundamental/governance/MF-holdings/
    # corp-action panels each enforce their own PIT rule internally
    # (announcement_date/filing_date/availability_date <= as_of) — this
    # function passes `target_date` through as `as_of` and does no PIT
    # filtering of its own, same discipline as every other PIT-sensitive
    # category here.
    universe_meta = load_universe()
    sector_map = dict(zip(universe_meta["ticker"], universe_meta["sector"]))
    tier_map = dict(zip(universe_meta["ticker"], universe_meta["tier"]))

    # Pass universe_panel so per-ticker OHLCV price lookups (valuation close,
    # pledge spiral check, corp-action windows, post-earnings drift) all hit
    # memory instead of making per-ticker API calls — the data is already in
    # the bulk panel fetched above.
    fundamental = compute_fundamental_features_panel(
        client, tickers, target_date, sector_map,
        data_cache=data_cache, ohlcv_panel=universe_panel if not universe_panel.empty else None,
    )
    governance = compute_governance_features_panel(
        client, tickers, target_date,
        data_cache=data_cache, ohlcv_panel=universe_panel if not universe_panel.empty else None,
    )
    mf_holdings = compute_mf_holdings_features_panel(tickers, target_date, tier_map=tier_map)
    # 2026-07-07 (follow-up): listing_dates was never passed here, so
    # ipo_lockin_expiry_proximity/ipo_listing_age_months were always NaN
    # regardless of stock_master coverage — see
    # scripts/backfill_listing_dates_nse.py for the real NSE-sourced backfill
    # that populated stock_master.listing_date for the first time (402/1626
    # tickers, NSE's history only covers IPOs from ~2012 on).
    try:
        listing_dates = client.get_listing_dates()
    except Exception as exc:
        logger.warning(f"Could not fetch listing_dates for corp-action panel: {exc}")
        listing_dates = {}
    corp_action = compute_corporate_action_features_panel(
        client, tickers, target_date, listing_dates=listing_dates,
        data_cache=data_cache, ohlcv_panel=universe_panel if not universe_panel.empty else None,
    )
    fno = compute_fno_features_panel(client, tickers, target_date, data_cache=data_cache)

    # mf_holdings/governance are already ticker-keyed single-snapshot panels
    # "as of" target_date — exactly the shape compute_multibagger_features'
    # PIT-safe institutional merge expects (see that module's docstring).
    # No fno_iv_panel: matrix_builder only has TODAY's fno panel, not a
    # rolling IV history, so iv_compression_flag stays NaN here — a
    # documented gap, not a silent omission.
    if not universe_panel.empty:
        multibagger = compute_multibagger_features(
            universe_panel, benchmark_wide, sector_map, mf_snapshot=mf_holdings, governance_snapshot=governance
        )
        today_multibagger = multibagger[multibagger["date"] == target_date].drop(columns=["date"])
    else:
        today_multibagger = pd.DataFrame(columns=["ticker"] + MULTIBAGGER_FEATURES)

    # ── Phase 3: advanced technical + pattern + real-economy macro + deep forensic ──
    if not universe_panel.empty:
        adv_tech = compute_advanced_technical_features(universe_panel)
        today_adv_tech = adv_tech[adv_tech["date"] == target_date].drop(columns=["date"])
        pat_scores = compute_pattern_scores(universe_panel)
        today_patterns = pat_scores[pat_scores["date"] == target_date].drop(columns=["date"])
    else:
        today_adv_tech = pd.DataFrame(columns=["ticker"] + ADVANCED_TECHNICAL_FEATURES)
        today_patterns = pd.DataFrame(columns=["ticker"] + PATTERN_FEATURES)

    real_economy = compute_real_economy_macro_panel(target_date, tickers)
    deep_forensic = compute_deep_forensic_features_panel(
        client, tickers, to_date, data_cache=data_cache,
        ohlcv_panel=universe_panel if not universe_panel.empty else None,
    )

    matrix = pd.DataFrame({"ticker": tickers})
    matrix = matrix.merge(today_technical, on="ticker", how="left")
    matrix = matrix.merge(today_intraday, on="ticker", how="left")
    if not today_hmm.empty:
        matrix = matrix.merge(today_hmm, on="ticker", how="left")
    else:
        for col in HMM_REGIME_FEATURES:
            matrix[col] = np.nan
    if not today_pnd.empty:
        matrix = matrix.merge(today_pnd, on="ticker", how="left")
    else:
        for col in PND_FEATURES:
            matrix[col] = np.nan
    matrix = matrix.merge(fundamental, on="ticker", how="left")
    matrix = matrix.merge(governance, on="ticker", how="left")
    matrix = matrix.merge(mf_holdings, on="ticker", how="left")
    matrix = matrix.merge(corp_action, on="ticker", how="left")
    matrix = matrix.merge(fno, on="ticker", how="left")
    matrix = matrix.merge(today_multibagger, on="ticker", how="left")
    matrix = matrix.merge(today_adv_tech, on="ticker", how="left")
    matrix = matrix.merge(today_patterns, on="ticker", how="left")
    matrix = matrix.merge(real_economy, on="ticker", how="left")
    matrix = matrix.merge(deep_forensic, on="ticker", how="left")
    matrix["date"] = target_date
    matrix = matrix.merge(calendar_row, on="date", how="left")
    matrix = matrix.merge(macro_row, on="date", how="left")
    matrix = matrix[["date", "ticker"] + ALL_FEATURE_COLUMNS]

    _validate_feature_matrix(matrix)
    if save:
        _save_feature_matrix(matrix, target_date)

    return matrix
