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
from features.fno_features import FNO_FEATURES, compute_fno_features_panel, load_ever_fno_eligible_tickers
from features.fundamental import FUNDAMENTAL_FEATURES, compute_fundamental_features_panel
from features.fundamental_cache import load_fundamental_raw_cache, save_fundamental_raw_cache_entries
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

# [2026-07-28] Module-level, lazily-loaded singleton for features/
# fundamental_cache.py's event-driven raw-fundamental cache. Loaded once
# per process (a full scan of the persistent DuckDB cache table, not
# something to repeat per date) and kept warm across every subsequent
# build_feature_matrix call in that process — the whole point for a
# multi-day backfill loop, and equally correct for the live daily
# pipeline's one-call-per-cron-run pattern since the DuckDB file persists
# across process restarts too.
_fundamental_raw_cache: Optional[dict] = None


def _get_fundamental_raw_cache() -> dict:
    global _fundamental_raw_cache
    if _fundamental_raw_cache is None:
        _fundamental_raw_cache = load_fundamental_raw_cache()
    return _fundamental_raw_cache


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

    # Fallback: per-ticker (for tests or callers that pass no bulk panel).
    # 2026-07-10 incident: when the DataStore API is down, this loop used to
    # plough through all ~2,300 tickers one exception at a time, and on a
    # laptop restart (API not up yet) that ran the process's RSS up to 5+ GB
    # before it finally gave up. A connection error (as opposed to a per-
    # ticker data problem) means the whole API is unreachable, not that this
    # one ticker lacks data — so stop after the first one instead of
    # burning through the rest of the universe the same way.
    import httpx

    frames = []
    for ticker in tickers:
        try:
            rows = client.get_ohlcv(ticker, from_date, to_date)
        except httpx.RequestError as exc:
            logger.error(
                f"OHLCV fetch failed for {ticker} with a connection error ({exc}) — "
                "the DataStore API is very likely unreachable; aborting the per-ticker "
                "fallback instead of retrying it for every remaining ticker"
            )
            break
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


def _compute_chunked_ticker_independent_panels(
    universe_panel: pd.DataFrame,
    benchmark_wide: Optional[pd.DataFrame],
    tickers: List[str],
    target_date: pd.Timestamp,
    compute_hmm: bool,
    hmm_workers: int,
) -> "tuple":
    """
    A47 (2026-07-10): computes technical/intraday/hmm/pnd/adv_tech/patterns
    in ticker chunks instead of one full-universe pass, bounding peak
    memory to one chunk's derived DataFrames at a time instead of holding
    6 full-universe-sized derived DataFrames simultaneously alongside
    `universe_panel` itself.

    Deliberately does NOT include fundamental/mf_holdings/multibagger —
    those do real cross-ticker aggregation (sector-relative z-score,
    tier-percentile rank, universe/sector-relative rank respectively) that
    would be silently corrupted by seeing only one chunk's sub-cohort
    instead of the true full-universe cohort. This function only chunks
    categories confirmed per-ticker-independent (no groupby/rank/zscore
    across the ticker dimension) — see FeatureBacklog.md A47 for the full
    per-category audit.

    `universe_panel` itself is NOT chunked/discarded per-chunk — it's
    already one full-universe DataFrame from a single bulk OHLCV fetch,
    and `compute_multibagger_features` (called by the caller, after this
    function returns) needs the full-universe panel for its own
    cross-sectional ranks regardless. Only the DERIVED per-chunk
    DataFrames (technical/intraday/hmm/pnd/adv_tech/patterns) are
    bounded and freed between chunks — this is still a real reduction in
    peak memory vs. holding all 6 full-universe-sized derived frames at
    once, without touching the raw OHLCV panel's own footprint.

    Returns
    -------
    tuple of 6 pd.DataFrame
        (today_technical, today_intraday, today_hmm, today_pnd,
        today_adv_tech, today_patterns), each ['ticker'] + that
        category's feature columns, concatenated across all chunks.
    """
    import gc

    from config.settings import PIPELINE_MEMORY_CEILING_MB, SCREENER_BATCH_EXPORT_CHUNK_SIZE
    from ingestion.scheduler.resource_guard import adaptive_chunk_size

    technical_chunks, intraday_chunks, hmm_chunks, pnd_chunks = [], [], [], []
    adv_tech_chunks, patterns_chunks = [], []

    i = 0
    while i < len(tickers):
        chunk_size = adaptive_chunk_size(SCREENER_BATCH_EXPORT_CHUNK_SIZE, ceiling_mb=PIPELINE_MEMORY_CEILING_MB)
        chunk_tickers = tickers[i : i + chunk_size]
        i += chunk_size

        chunk_panel = universe_panel[universe_panel["ticker"].isin(set(chunk_tickers))]
        if chunk_panel.empty:
            continue

        technical = compute_technical_features(chunk_panel, benchmark_wide)
        intraday = compute_intraday_features(chunk_panel)
        hmm = (
            compute_hmm_regime_features(chunk_panel, n_workers=hmm_workers)
            if compute_hmm
            else pd.DataFrame(columns=["date", "ticker"] + HMM_REGIME_FEATURES)
        )
        pnd = compute_pnd_features(chunk_panel)
        adv_tech = compute_advanced_technical_features(chunk_panel)
        pat_scores = compute_pattern_scores(chunk_panel)

        technical_chunks.append(_extract_target_date_panel(technical, target_date, CORE_TECHNICAL_FEATURES))
        intraday_chunks.append(_extract_target_date_panel(intraday, target_date, INTRADAY_FEATURES))
        hmm_chunks.append(_extract_target_date_panel(hmm, target_date, HMM_REGIME_FEATURES))
        pnd_chunks.append(_extract_target_date_panel(pnd, target_date, PND_FEATURES))
        adv_tech_chunks.append(adv_tech[adv_tech["date"] == target_date].drop(columns=["date"]))
        patterns_chunks.append(pat_scores[pat_scores["date"] == target_date].drop(columns=["date"]))

        del chunk_panel, technical, intraday, hmm, pnd, adv_tech, pat_scores
        gc.collect()

    def _concat(frames: List[pd.DataFrame], cols: List[str]) -> pd.DataFrame:
        non_empty = [f for f in frames if not f.empty]
        if not non_empty:
            return pd.DataFrame(columns=["ticker"] + cols)
        return pd.concat(non_empty, ignore_index=True)

    return (
        _concat(technical_chunks, CORE_TECHNICAL_FEATURES),
        _concat(intraday_chunks, INTRADAY_FEATURES),
        _concat(hmm_chunks, HMM_REGIME_FEATURES),
        _concat(pnd_chunks, PND_FEATURES),
        _concat(adv_tech_chunks, ADVANCED_TECHNICAL_FEATURES),
        _concat(patterns_chunks, PATTERN_FEATURES),
    )


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

    # [2026-07-28 perf fix] Fetched here (moved earlier than the corp-action/
    # fundamental use sites below) so active_tickers can be resolved before
    # ANY per-ticker panel work starts. Profiling a 2022 backfill date found
    # 37% of a 150-ticker sample missing from the bulk OHLCV panel — not a
    # bug, confirmed directly (e.g. ADANIENSOL listed 2023, AKUMS listed
    # 2024): these are real tickers from TODAY's universe that simply
    # hadn't listed yet on the historical `as_of` date. Every panel that do
    # a per-ticker fallback when its bulk/cached slice comes up empty
    # (features/fundamental.py's _latest_close_on_or_before,
    # governance.py/corporate_action_features.py/deep_forensic.py's
    # equivalents) was paying a live API round-trip just to confirm "no
    # data, as expected" for these tickers, every single day — the
    # dominant cost for early-history backfill dates where a large
    # fraction of today's ~2,317-ticker universe didn't exist yet.
    try:
        listing_dates = client.get_listing_dates()
    except Exception as exc:
        # [BUG FIX, 2026-07-28 model-review] This fail-open path (listing_dates
        # = {}) silently REINTRODUCES the not-yet-listed-ticker bug this whole
        # active_tickers filter exists to fix — every not-yet-listed ticker in
        # the universe becomes indistinguishable from "listing_date unknown"
        # and is included again. Non-fatal (a single API hiccup must not crash
        # the whole feature build), but this must be LOUD: ERROR level (not
        # WARNING) so it's visible in log-level-filtered monitoring, plus an
        # explicit marker string a log-based alert/metric can grep for.
        logger.error(
            "FEATURE_BUILD_DEGRADED: could not fetch listing_dates for active-ticker filtering "
            f"({exc}) — falling back to an EMPTY listing_dates map, which silently disables the "
            "not-yet-listed-ticker filter for this entire feature build (every not-yet-listed "
            "ticker will be treated as active). This is the exact bug that filter exists to fix."
        )
        listing_dates = {}

    # Conservative: only exclude a ticker when its listing_date is KNOWN
    # and confirms it hadn't listed yet — an unknown listing_date (missing
    # from stock_master, e.g. pre-2012 IPOs per that scraper's coverage
    # gap) is never treated as "not yet listed," same missing-data
    # convention as everywhere else in this module.
    active_tickers = [
        t for t in tickers
        if not (listing_dates.get(t) is not None and pd.Timestamp(listing_dates[t]) > target_date)
    ]

    # One bulk HTTP call for all tickers (universe + benchmarks) when the
    # client supports it; otherwise fall back to the existing per-ticker path.
    bulk_panel = None
    bulk_loader = getattr(client, "get_ohlcv_bulk", None)
    if callable(bulk_loader):
        try:
            bulk_panel = bulk_loader(from_date, to_date)
        except Exception as exc:
            logger.warning("Bulk OHLCV fetch failed, falling back to per-ticker fetch: %s", exc)

    universe_panel = _fetch_ohlcv_panel(client, active_tickers, from_date, to_date, _bulk_panel=bulk_panel)
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
        today_technical, today_intraday, today_hmm, today_pnd, today_adv_tech, today_patterns = (
            _compute_chunked_ticker_independent_panels(
                universe_panel, benchmark_wide, active_tickers, target_date, compute_hmm, hmm_workers,
            )
        )

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

    # listing_dates (needed for ipo_lockin_expiry_proximity/
    # ipo_listing_age_months/company_age_years, and for active_tickers
    # above) was already fetched at the top of this function — see the
    # 2026-07-28 perf-fix comment there for why it moved earlier.

    # Pass universe_panel so per-ticker OHLCV price lookups (valuation close,
    # pledge spiral check, corp-action windows, post-earnings drift) all hit
    # memory instead of making per-ticker API calls — the data is already in
    # the bulk panel fetched above.
    raw_cache = _get_fundamental_raw_cache()
    cache_misses: dict = {}
    fundamental = compute_fundamental_features_panel(
        client, active_tickers, target_date, sector_map,
        data_cache=data_cache, ohlcv_panel=universe_panel if not universe_panel.empty else None,
        listing_date_map=listing_dates,
        raw_cache=raw_cache, cache_misses_out=cache_misses,
    )
    # Persist only this date's new entries (typically a small fraction of
    # the universe once warm), not the whole cache — see
    # features/fundamental_cache.py's docstring for why.
    save_fundamental_raw_cache_entries(cache_misses)
    governance = compute_governance_features_panel(
        client, active_tickers, target_date,
        data_cache=data_cache, ohlcv_panel=universe_panel if not universe_panel.empty else None,
    )
    mf_holdings = compute_mf_holdings_features_panel(active_tickers, target_date, tier_map=tier_map)
    corp_action = compute_corporate_action_features_panel(
        client, active_tickers, target_date, listing_dates=listing_dates,
        data_cache=data_cache, ohlcv_panel=universe_panel if not universe_panel.empty else None,
    )
    # [2026-07-26 perf fix] ~2,100 of ~2,300 universe tickers have never had
    # F&O activity at all — compute_fno_features_panel used to call the F&O
    # API for every one of them anyway, the dominant cost of a full-universe
    # backfill (measured: 2,317 of ~2,423 HTTP calls for one day were this
    # call). fno_eligible_tickers pre-filters those out with zero behavior
    # change (see load_ever_fno_eligible_tickers's docstring).
    fno = compute_fno_features_panel(
        client, active_tickers, target_date, data_cache=data_cache,
        fno_eligible_tickers=load_ever_fno_eligible_tickers(),
    )

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

    # ── Phase 3: real-economy macro + deep forensic ──
    # (today_adv_tech/today_patterns already computed above, chunked
    # alongside technical/intraday/hmm/pnd — A47.)

    real_economy = compute_real_economy_macro_panel(target_date, active_tickers)
    deep_forensic = compute_deep_forensic_features_panel(
        client, active_tickers, to_date, data_cache=data_cache,
        ohlcv_panel=universe_panel if not universe_panel.empty else None,
        sector_map=sector_map,
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
