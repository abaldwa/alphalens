"""
features/matrix_builder.py

Phase: 1.1 (Core Feature Computation)
Specs: SPEC-SOLID-005, SPEC-DS-005, SPEC-DS-007, SPEC-PIPE-004, SPEC-PIPE-005, SPEC-FEAT-001
Owner: Platform / Features
Consumers: ingestion/scheduler/daily_pipeline (compute_features step), systems/ml_signal_engine

Assembles the daily feature matrix by orchestrating features/technical.py
(70 cols), features/intraday.py (3 net-new cols), features/calendar.py
(7 cols), the M-01 HMM regime detector (6 cols), and features/
macro_features.py (14 cols) — 100 feature columns total, not the literal
111 in 02_models.md's "76 core + 8 intraday + 7 calendar + 6 HMM + 14
macro" formula for the Signal 5d/21d models. The gap is the same 70-vs-76
technical-feature accounting flagged in features/technical.py, plus a
5-feature overlap between technical.py's Category 11 and the canonical
8-feature intraday category (see features/intraday.py's module docstring)
— both documented rather than silently padded. This module assembles
whatever the category modules produce — adding a future category
(SOLID-002: open/closed) only requires extending ALL_FEATURE_COLUMNS here,
not rewriting this file.

SPEC-SOLID-005 (Dependency Inversion): all OHLCV access goes through
DataStoreClient (an HTTP client over the DataStore API), never direct
DuckDB — `client` is constructor-injectable so tests can substitute a
fake. macro_indicators is the one exception: SPEC-DS-002 explicitly
permits direct DuckDB reads "within ingestion and feature layers", and no
DataStore API endpoint for it exists yet, so features/macro_features.py's
load_macro_indicators() reads DuckDB directly.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

from config.settings import DELIVERY_PCT_RANGE, FEATURES_DAILY_DIR, NULL_RATE_ALERT_THRESHOLD, RATIO_FEATURE_RANGE
from datastore.client import DataStoreClient
from features.calendar import CALENDAR_FEATURES, compute_calendar_features
from features.intraday import INTRADAY_FEATURES, compute_intraday_features
from features.macro_features import MACRO_FEATURES, compute_macro_features, load_macro_indicators
from features.technical import BENCHMARK_TICKERS, CORE_TECHNICAL_FEATURES, compute_technical_features
from systems.ml_signal_engine.models.hmm.regime_detector import HMM_REGIME_FEATURES, compute_hmm_regime_features

logger = logging.getLogger(__name__)

ALL_FEATURE_COLUMNS: List[str] = (
    CORE_TECHNICAL_FEATURES + INTRADAY_FEATURES + CALENDAR_FEATURES + HMM_REGIME_FEATURES + MACRO_FEATURES
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
    client: DataStoreClient, tickers: List[str], from_date: datetime, to_date: datetime
) -> pd.DataFrame:
    """
    Fetch OHLCV history for many tickers via the DataStore API.

    One HTTP call per ticker — this loop is I/O orchestration (fetching),
    not feature computation, so SPEC-PIPE-004's "no loop over stocks" rule
    does not apply here; that rule governs the vectorized math inside
    features/technical.py, which receives the panel this function builds.
    """
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
    for col in [c for c in CORE_TECHNICAL_FEATURES if c.endswith("_ratio")]:
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

    Returns
    -------
    pd.DataFrame
        One row per ticker (even if its OHLCV fetch failed — all-NaN
        feature row in that case), columns: date, ticker +
        ALL_FEATURE_COLUMNS (100 cols).

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

    universe_panel = _fetch_ohlcv_panel(client, tickers, from_date, to_date)
    benchmark_panel = _fetch_ohlcv_panel(client, list(BENCHMARK_TICKERS.values()), from_date, to_date)
    benchmark_wide = _build_benchmark_wide(benchmark_panel)

    if universe_panel.empty:
        logger.warning(f"No OHLCV data returned for any of {len(tickers)} tickers on {date}")
        technical = pd.DataFrame(columns=["date", "ticker"] + CORE_TECHNICAL_FEATURES)
        intraday = pd.DataFrame(columns=["date", "ticker"] + INTRADAY_FEATURES)
        hmm = pd.DataFrame(columns=["date", "ticker"] + HMM_REGIME_FEATURES)
    else:
        technical = compute_technical_features(universe_panel, benchmark_wide)
        intraday = compute_intraday_features(universe_panel)
        hmm = (
            compute_hmm_regime_features(universe_panel)
            if compute_hmm
            else pd.DataFrame(columns=["date", "ticker"] + HMM_REGIME_FEATURES)
        )
    today_technical = technical[technical["date"] == target_date].drop(columns=["date"])
    today_intraday = intraday[intraday["date"] == target_date].drop(columns=["date"])
    today_hmm = hmm[hmm["date"] == target_date].drop(columns=["date"]) if not hmm.empty else hmm.drop(columns=["date"])

    calendar_row = compute_calendar_features(target_date)

    macro_indicators = load_macro_indicators(target_date)
    nifty50_ticker = BENCHMARK_TICKERS["nifty50"]
    nifty50_hist = benchmark_panel.loc[benchmark_panel["ticker"] == nifty50_ticker, ["date", "close"]]
    universe_close = universe_panel[["date", "ticker", "close"]] if not universe_panel.empty else None
    macro_row = compute_macro_features(target_date, macro_indicators, nifty50_hist, universe_close)

    matrix = pd.DataFrame({"ticker": tickers})
    matrix = matrix.merge(today_technical, on="ticker", how="left")
    matrix = matrix.merge(today_intraday, on="ticker", how="left")
    if not today_hmm.empty:
        matrix = matrix.merge(today_hmm, on="ticker", how="left")
    else:
        for col in HMM_REGIME_FEATURES:
            matrix[col] = np.nan
    matrix["date"] = target_date
    matrix = matrix.merge(calendar_row, on="date", how="left")
    matrix = matrix.merge(macro_row, on="date", how="left")
    matrix = matrix[["date", "ticker"] + ALL_FEATURE_COLUMNS]

    _validate_feature_matrix(matrix)
    if save:
        _save_feature_matrix(matrix, target_date)

    return matrix
