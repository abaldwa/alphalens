"""
scripts/feature_backfill_hybrid.py

Hybrid two-stage feature backfill — replaces the date-first
feature_backfill.py for historical runs.

Stage 1 (ticker-first, ~7–17 min single-threaded):
  For each of 500 tickers, load its full OHLCV history from DuckDB ONCE,
  compute ALL per-ticker features for ALL dates in-memory, hold in a dict.

Stage 2 (date assembly, ~25 min):
  For each date, slice the in-memory staging dict (no I/O), apply
  cross-ticker features (sector z-scores, mf_crowdedness_rank, multibagger,
  macro), write the final daily parquet.

Estimated total: ~35–45 minutes (vs ~11 days with the optimised date-first
approach, or ~15 days with the original).

I/O savings vs. date-first:
  OHLCV reads:    4785 × 380k rows → 500 × 5k rows  (720× reduction)
  F&O API calls:  2.39M → 500 DuckDB queries
  MF holdings:    2.39M parquet reads → 1 directory scan
  Rolling compute: 4785 × 760 rows/ticker → 1 × 5k rows/ticker (same maths,
                   720× fewer total iterations)
  HMM fitting:    skipped with --no-hmm → fitted ONCE per ticker (enabled!)

Usage
-----
    # Default: 2007-01-03 → today, newest-first
    .venv/bin/python3 scripts/feature_backfill_hybrid.py

    # Oldest-first
    .venv/bin/python3 scripts/feature_backfill_hybrid.py --chronological

    # Skip HMM (saves ~200 ms per ticker in Stage 1 — already much cheaper
    # than the date-first mode where it cost ~14 min PER DATE)
    .venv/bin/python3 scripts/feature_backfill_hybrid.py --no-hmm

    # Background
    nohup .venv/bin/python3 scripts/feature_backfill_hybrid.py \\
        > logs/feature_backfill_hybrid.log 2>&1 &

Prerequisites
-------------
DataStore API must be running (for BackfillDataCache pre-load):
    .venv/bin/uvicorn datastore.api.main:app --host 127.0.0.1 --port 8000

DuckDB is accessed directly for OHLCV and F&O in both stages (SPEC-DS-002
exception: direct reads permitted in the feature layer when no API endpoint
covers bulk-historical access — same precedent as macro_features.py's
load_macro_indicators and mf_holdings.py's direct parquet reads).

SPEC-PIPE-003 (PIT safety): all per-ticker PIT filtering (announcement_date
<= as_of, filing_date <= as_of, availability_date <= as_of) is applied
inside features/hybrid_compute.py's per-date loop, not here.
"""

import argparse
import logging
import sys
import time
from datetime import date as date_type
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── Module-level globals for fork-based multiprocessing ───────────────────────
# Only SMALL objects are stored here — large per-ticker data (OHLCV, cache,
# MF holdings) is passed as pickle args so Python's reference-counting
# never dirties forked copy-on-write pages for the heavy structures.
_G_BENCHMARK_WIDE: Optional[pd.DataFrame] = None  # ~50 KB
_G_ALL_DATES: List[pd.Timestamp] = []             # ~300 KB


def _worker_init(benchmark_wide: "pd.DataFrame", all_dates: list) -> None:
    """Pool initializer — sets globals in each spawned worker process."""
    global _G_BENCHMARK_WIDE, _G_ALL_DATES
    _G_BENCHMARK_WIDE = benchmark_wide
    _G_ALL_DATES = all_dates


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Hybrid two-stage feature backfill (ticker-first → date assembly)"
    )
    p.add_argument(
        "--from-date", default="2007-01-03", metavar="YYYY-MM-DD",
        help="Earliest date to compute (default: 2007-01-03, after 252-day warm-up)",
    )
    p.add_argument(
        "--to-date", default=None, metavar="YYYY-MM-DD",
        help="Latest date to compute (default: today)",
    )
    p.add_argument(
        "--chronological", action="store_true",
        help="Process oldest dates first (default: newest-first so training data is ready sooner)",
    )
    p.add_argument(
        "--force", action="store_true",
        help="Recompute dates that already have a parquet",
    )
    p.add_argument(
        "--no-hmm", action="store_true",
        help=(
            "Skip HMM regime-feature fitting in Stage 1 (HMM columns become NaN). "
            "In hybrid mode HMM is already ~200 ms per ticker (not 14 min per date) "
            "so this flag is mainly useful for very fast smoke tests."
        ),
    )
    p.add_argument(
        "--staging-dir", default=None, metavar="PATH",
        help=(
            "Directory to write per-ticker staging parquets (default: a tmp dir "
            "under datastore/features/staging/). If the dir already contains "
            "complete staging parquets, Stage 1 is skipped and Stage 2 reads "
            "them directly — useful for resuming an interrupted run."
        ),
    )
    p.add_argument(
        "--workers", type=int, default=1, metavar="N",
        help=(
            "Number of parallel worker processes for Stage 1 (default: 1). "
            "Workers load OHLCV+F&O from DuckDB directly and receive only "
            "per-ticker cache data (~150 KB) as pickle args — peak memory is "
            "~150–250 MB per worker regardless of universe size. "
            "On a 14-core machine, --workers 10 is recommended."
        ),
    )
    p.add_argument(
        "--all-db-tickers", action="store_true",
        help=(
            "Run Stage 1 for ALL tickers present in ohlcv_adjusted (not just "
            "config/nifty500_universe.csv). Tickers not in the universe CSV get "
            "sector='UNKNOWN', tier='UNKNOWN'. Stage 2 is automatically skipped "
            "because loading 4000+ staging parquets into RAM would OOM on a laptop. "
            "Tickers that already have a staging parquet are skipped automatically."
        ),
    )
    p.add_argument(
        "--active-only", action="store_true",
        help=(
            "When used with --all-db-tickers, restrict to tickers that have traded "
            "within 30 calendar days of the last date in ohlcv_adjusted. This removes "
            "delisted/suspended stocks (~2500 tickers vs ~4100 total in the DB)."
        ),
    )
    p.add_argument(
        "--skip-stage2", action="store_true",
        help="Skip Stage 2 date assembly after Stage 1 completes (per-ticker staging only).",
    )
    p.add_argument(
        "--ticker-batch-size", type=int, default=None, metavar="N",
        help=(
            "Restrict Stage 1 to the Nth slice of --ticker-batch-size tickers "
            "(use with --ticker-batch-index). Added 2026-07-05 so a large "
            "--all-db-tickers run can be split across several fresh processes "
            "instead of one long-lived process holding the full-universe "
            "BackfillDataCache preload in memory at once (that preload alone "
            "was enough to trigger an OOM kill on a 14 GB machine at ~2500 "
            "tickers). Each batch still gets its own resumable staging-parquet "
            "cache, same as the unbatched path."
        ),
    )
    p.add_argument(
        "--ticker-batch-index", type=int, default=0, metavar="I",
        help="Which batch (0-indexed) to run when --ticker-batch-size is set.",
    )
    p.add_argument(
        "--rebuild-daily", action="store_true",
        help=(
            "Skip Stage 1 entirely and rebuild daily parquets (Stage 2 only) from "
            "existing staging parquets on disk. Loads staging in date-range chunks to "
            "stay within RAM (see --stage2-chunk-size). Use with --all-db-tickers "
            "--active-only --force to rebuild all daily parquets for the full 2492-ticker "
            "universe without loading all staging into memory at once."
        ),
    )
    p.add_argument(
        "--stage2-chunk-size", type=int, default=400, metavar="N",
        help=(
            "Number of dates per chunk when using --rebuild-daily (default: 400). "
            "Each chunk loads ~N/4790 of the staging data — 400 dates ≈ 1 GB RAM "
            "(safe on the 14.9 GB dev machine; reduce to 200 on <8 GB systems)."
        ),
    )
    return p.parse_args()


# ── Pre-load helpers ──────────────────────────────────────────────────────────

def _get_trading_dates(from_dt: date_type, to_dt: date_type, duckdb_path: Path) -> List[date_type]:
    """Return sorted trading dates in [from_dt, to_dt] from ohlcv_adjusted."""
    from datastore.api.db import get_duckdb_connection
    with get_duckdb_connection(duckdb_path, read_only=True, persist=False) as conn:
        rows = conn.execute(
            "SELECT DISTINCT CAST(date AS VARCHAR) FROM ohlcv_adjusted "
            "WHERE date >= ? AND date <= ? ORDER BY date",
            [from_dt.isoformat(), to_dt.isoformat()],
        ).fetchall()
    return [date_type.fromisoformat(r[0]) for r in rows]


def _load_ohlcv_per_ticker(
    tickers: List[str],
    duckdb_path: Path,
) -> Dict[str, pd.DataFrame]:
    """
    Load full OHLCV history for every ticker from DuckDB in one connection.

    Each ticker's DataFrame has columns: date, open, high, low, close,
    volume, delivery_pct (NaN if absent).

    SPEC-DS-002: direct DuckDB read permitted in the feature layer for
    bulk-historical access where no API endpoint exists (same exception as
    macro_features.py's load_macro_indicators).
    """
    from datastore.api.db import get_duckdb_connection

    result: Dict[str, pd.DataFrame] = {}
    logger.info("Stage 1 pre-load: loading OHLCV for %d tickers from DuckDB …", len(tickers))
    t0 = time.monotonic()

    with get_duckdb_connection(duckdb_path, read_only=True, persist=False) as conn:
        for i, ticker in enumerate(tickers):
            if i % 100 == 0:
                logger.info("  OHLCV pre-load %d/%d …", i, len(tickers))
            try:
                df = conn.execute(
                    """
                    SELECT date, open, high, low, close, volume,
                           COALESCE(delivery_pct, NULL) AS delivery_pct
                    FROM ohlcv_adjusted
                    WHERE ticker = ?
                    ORDER BY date
                    """,
                    [ticker],
                ).fetchdf()
                df["date"] = pd.to_datetime(df["date"])
                result[ticker] = df
            except Exception as exc:
                logger.warning("OHLCV load failed for %s: %s", ticker, exc)
                result[ticker] = pd.DataFrame()

    logger.info(
        "OHLCV pre-load complete for %d tickers in %.1f s",
        len(tickers), time.monotonic() - t0,
    )
    return result


def _load_benchmark_ohlcv(duckdb_path: Path) -> pd.DataFrame:
    """Load full OHLCV history for benchmark ETF tickers (NIFTYBEES etc.)."""
    from datastore.api.db import get_duckdb_connection
    from features.technical import BENCHMARK_TICKERS

    syms = list(BENCHMARK_TICKERS.values())
    with get_duckdb_connection(duckdb_path, read_only=True, persist=False) as conn:
        placeholders = ", ".join(["?"] * len(syms))
        df = conn.execute(
            f"SELECT date, ticker, close FROM ohlcv_adjusted "
            f"WHERE ticker IN ({placeholders}) ORDER BY date",
            syms,
        ).fetchdf()
    df["date"] = pd.to_datetime(df["date"])
    return df


def _get_fno_eligible_tickers(duckdb_path: Path) -> frozenset:
    """Return the set of tickers that actually have rows in fno_data."""
    from datastore.api.db import get_duckdb_connection
    with get_duckdb_connection(duckdb_path, read_only=True, persist=False) as conn:
        rows = conn.execute("SELECT DISTINCT ticker FROM fno_data").fetchall()
    return frozenset(r[0] for r in rows)


def _load_fno_for_ticker(
    ticker: str, conn, fno_eligible: Optional[frozenset] = None
) -> pd.DataFrame:
    """Load full F&O bhavcopy rows for one ticker from an open DuckDB connection.

    If fno_eligible is provided and ticker is not in it, returns an empty
    DataFrame immediately without issuing any DuckDB query — avoids 2000+
    wasted queries for non-F&O tickers in the 2492-ticker active universe.
    """
    if fno_eligible is not None and ticker not in fno_eligible:
        return pd.DataFrame()
    try:
        df = conn.execute(
            "SELECT * FROM fno_data WHERE ticker = ? ORDER BY trade_date",
            [ticker],
        ).fetchdf()
        if "trade_date" in df.columns:
            df["trade_date"] = pd.to_datetime(df["trade_date"])
        return df
    except Exception as exc:
        logger.debug("F&O load failed for %s: %s", ticker, exc)
        return pd.DataFrame()


def _load_ohlcv_for_ticker(ticker: str, conn) -> pd.DataFrame:
    """Load full OHLCV history for one ticker from an open DuckDB connection."""
    try:
        df = conn.execute(
            """
            SELECT date, open, high, low, close, volume,
                   COALESCE(delivery_pct, NULL) AS delivery_pct
            FROM ohlcv_adjusted
            WHERE ticker = ?
            ORDER BY date
            """,
            [ticker],
        ).fetchdf()
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        return df
    except Exception as exc:
        logger.debug("OHLCV load failed for %s: %s", ticker, exc)
        return pd.DataFrame()


def _load_all_macro(from_dt: date_type, to_dt: date_type, duckdb_path: Path) -> pd.DataFrame:
    """
    Load macro_indicators for the full backfill date range in one DuckDB query.

    In Stage 2, each date's compute_macro_features call slices this DataFrame
    instead of opening a new DuckDB connection (saves 4785 connections).
    """
    from datastore.api.db import get_duckdb_connection

    # Include 30-day pre-history so the earliest backfill date has a full
    # 21-trading-day window for VIX changes, Nifty returns, etc.
    start = pd.Timestamp(from_dt) - pd.Timedelta(days=35)
    try:
        with get_duckdb_connection(duckdb_path, read_only=True, persist=False) as conn:
            df = conn.execute(
                "SELECT date, indicator, value FROM macro_indicators "
                "WHERE date >= ? AND date <= ? ORDER BY date",
                [start.date().isoformat(), to_dt.isoformat()],
            ).fetchdf()
        df["date"] = pd.to_datetime(df["date"])
        logger.info("Macro pre-load: %d rows for %d indicators", len(df), df["indicator"].nunique())
        return df
    except Exception as exc:
        logger.warning("Could not pre-load macro_indicators: %s — macro features will be NaN", exc)
        return pd.DataFrame(columns=["date", "indicator", "value"])


def _load_all_mf_holdings() -> Dict[str, pd.DataFrame]:
    """
    Load every MF holdings parquet file and return a per-ticker dict.

    {ticker: DataFrame} where each DataFrame has all historical rows for
    that ticker across all months (unfiltered by availability_date).
    PIT filtering is applied per-date inside hybrid_compute.compute_per_ticker.
    """
    from config.settings import MF_HOLDINGS_DIR

    if not MF_HOLDINGS_DIR.exists():
        logger.info("MF holdings directory absent — mf_* features will be NaN")
        return {}

    frames = []
    for path in sorted(MF_HOLDINGS_DIR.glob("*.parquet")):
        try:
            df = pd.read_parquet(path)
            df["availability_date"] = pd.to_datetime(df["availability_date"])
            frames.append(df)
        except Exception as exc:
            logger.warning("Could not read MF holdings file %s: %s", path.name, exc)

    if not frames:
        return {}

    all_mf = pd.concat(frames, ignore_index=True)
    logger.info("MF holdings pre-load: %d rows across %d months", len(all_mf), len(frames))

    by_ticker: Dict[str, pd.DataFrame] = {}
    for ticker, grp in all_mf.groupby("ticker", sort=False):
        by_ticker[str(ticker)] = grp.reset_index(drop=True)
    return by_ticker


def _load_staging(staging_dir: Path, ticker: str) -> Optional[pd.DataFrame]:
    """Read a ticker's staging parquet if it exists (resume support)."""
    path = staging_dir / f"{ticker}.parquet"
    if path.exists():
        return pd.read_parquet(path)
    return None


def _save_staging(staging_dir: Path, ticker: str, df: pd.DataFrame) -> None:
    """Persist a ticker's staging DataFrame for resume support."""
    staging_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(staging_dir / f"{ticker}.parquet", index=False)


def _stage1_ticker(args: Tuple) -> Tuple[str, str]:
    """
    Stage 1 worker — called per-ticker via multiprocessing.Pool.imap_unordered.

    Memory design: per-ticker cache data (fundamentals, shareholding, corp
    actions, MF holdings) arrives as pickle args (~150 KB). Workers load OHLCV
    and F&O directly from DuckDB. Only _G_BENCHMARK_WIDE and _G_ALL_DATES are
    inherited via fork — both are tiny (~350 KB total), so copy-on-write
    page-dirtying from reference counting is negligible.

    Per-worker peak memory: ~150–250 MB (vs ~750 MB with the old globals approach).
    Returns (ticker, status) where status is "cached", "done", or "error: …".
    """
    (
        ticker, fund_raw, share_raw, corp_raw, mf_df,
        listing_dt, compute_hmm, staging_dir, duckdb_path, fno_eligible,
    ) = args
    staging_dir = Path(staging_dir)
    duckdb_path = Path(duckdb_path)

    if (staging_dir / f"{ticker}.parquet").exists():
        return ticker, "cached"

    try:
        from datastore.api.db import get_duckdb_connection
        from features.hybrid_compute import compute_per_ticker

        # Load OHLCV + F&O in one connection (DuckDB read-only allows concurrency).
        # fno_eligible is a frozenset of tickers with F&O data — skips the query
        # for the ~88% of tickers that have no F&O rows.
        with get_duckdb_connection(duckdb_path, read_only=True, persist=False) as conn:
            ohlcv = _load_ohlcv_for_ticker(ticker, conn)
            fno_df = _load_fno_for_ticker(ticker, conn, fno_eligible=fno_eligible)

        class _TickerCache:
            """Minimal cache stub — avoids passing the full BackfillDataCache object."""
            def __init__(self):
                self._fundamentals = {ticker: fund_raw}
                self._shareholding = {ticker: share_raw}
                self._corp_actions = {ticker: corp_raw}

        df = compute_per_ticker(
            ticker=ticker,
            ohlcv=ohlcv,
            fno_df=fno_df,
            benchmark_wide=_G_BENCHMARK_WIDE,
            all_dates=_G_ALL_DATES,
            cache=_TickerCache(),
            mf_for_ticker=mf_df,
            listing_date=listing_dt,
            compute_hmm=compute_hmm,
        )
        _save_staging(staging_dir, ticker, df)
        return ticker, "done"
    except Exception as exc:
        return ticker, f"error: {exc}"


# ── Stage 1 orchestration ─────────────────────────────────────────────────────

def run_stage1(
    tickers: List[str],
    all_dates: List[pd.Timestamp],
    ohlcv_by_ticker: Dict[str, pd.DataFrame],
    benchmark_ohlcv: pd.DataFrame,
    cache,
    mf_by_ticker: Dict[str, pd.DataFrame],
    listing_dates: Dict[str, Optional[object]],
    compute_hmm: bool,
    staging_dir: Path,
    duckdb_path: Path,
    n_workers: int = 1,
    load_for_stage2: bool = True,
) -> Dict[str, pd.DataFrame]:
    """
    Stage 1: per-ticker feature computation.

    Returns {ticker: staging_DataFrame}. If a staging parquet already
    exists in staging_dir, that ticker is skipped (resume support).

    With n_workers > 1, forks subprocesses. Workers receive per-ticker cache
    data as pickle args and load OHLCV/F&O from DuckDB directly — parent's
    large structures (OHLCV dict, BackfillDataCache, MF holdings) are never
    touched by workers, so copy-on-write holds and per-worker peak memory
    stays at ~150–250 MB regardless of universe size.
    """
    from features.hybrid_compute import build_benchmark_wide, _empty_staging

    benchmark_wide = build_benchmark_wide(benchmark_ohlcv)
    staging: Dict[str, pd.DataFrame] = {}

    # Pre-fetch the set of tickers that have any F&O data in the DB.
    # This lets workers skip the DuckDB query for the ~88% of tickers with no
    # F&O rows, avoiding ~2000 wasted queries per Stage 1 run.
    fno_eligible = _get_fno_eligible_tickers(duckdb_path)
    fno_in_universe = sum(1 for t in tickers if t in fno_eligible)
    logger.info(
        "F&O eligibility: %d/%d tickers in universe have F&O data (%d will skip query)",
        fno_in_universe, len(tickers), len(tickers) - fno_in_universe,
    )

    logger.info(
        "Stage 1: computing per-ticker features for %d tickers (workers=%d) …",
        len(tickers), n_workers,
    )
    t0_stage1 = time.monotonic()

    if n_workers > 1:
        # ── Parallel path: spawn-based pool to avoid memory inheritance ────
        # 'fork' copies the parent's ~1.5 GB RSS into every worker (BackfillDataCache
        # + pre-loaded OHLCV + benchmark_wide + all_dates). Python reference counting
        # dirties CoW pages immediately, so 10 workers × 1.5 GB = OOM.
        # 'spawn' starts each worker fresh; _worker_init sets the tiny globals
        # (~350 KB) and each task receives only its per-ticker slice (~150 KB).
        # Total peak memory ≈ parent (1.5 GB) + workers × ~200 MB each.

        # OHLCV dict is no longer needed — workers load from DuckDB directly.
        ohlcv_by_ticker.clear()

        logger.info("  Building per-ticker worker args …")
        worker_args = [
            (
                ticker,
                cache._fundamentals.get(ticker, []),
                cache._shareholding.get(ticker, []),
                cache._corp_actions.get(ticker, []),
                mf_by_ticker.get(ticker, pd.DataFrame()),
                listing_dates.get(ticker),
                compute_hmm,
                str(staging_dir),
                str(duckdb_path),
                fno_eligible,
            )
            for ticker in tickers
        ]

        import multiprocessing
        _spawn_ctx = multiprocessing.get_context("spawn")
        done_count = cached_count = error_count = 0
        with _spawn_ctx.Pool(
            processes=n_workers,
            initializer=_worker_init,
            initargs=(benchmark_wide, all_dates),
        ) as pool:
            for i, (ticker, status) in enumerate(
                pool.imap_unordered(_stage1_ticker, worker_args), start=1
            ):
                if status == "cached":
                    cached_count += 1
                elif status == "done":
                    done_count += 1
                    if done_count % 10 == 0 or done_count == 1:
                        elapsed = time.monotonic() - t0_stage1
                        rate = done_count / elapsed * 60
                        logger.info(
                            "  Stage 1 progress: %d/%d done, %d cached, %d errors "
                            "(%.1f tickers/min)",
                            done_count, len(tickers), cached_count, error_count, rate,
                        )
                else:
                    error_count += 1
                    logger.error("  %s: %s", ticker, status)

        logger.info(
            "  Stage 1 workers complete: %d done, %d cached, %d errors",
            done_count, cached_count, error_count,
        )

        # Load all staging files into the in-memory dict for Stage 2.
        # Skipped when load_for_stage2=False (--all-db-tickers mode) to
        # avoid loading thousands of parquets that would OOM on a laptop.
        if load_for_stage2:
            for ticker in tickers:
                df = _load_staging(staging_dir, ticker)
                if df is not None:
                    staging[ticker] = df
                else:
                    logger.warning("  %s: staging file missing after parallel run", ticker)
                    staging[ticker] = _empty_staging(ticker, all_dates)

    else:
        # ── Sequential path (original) ─────────────────────────────────────
        from datastore.api.db import get_duckdb_connection
        from features.hybrid_compute import compute_per_ticker

        with get_duckdb_connection(duckdb_path, read_only=True, persist=False) as fno_conn:
            for i, ticker in enumerate(tickers, start=1):
                cached = _load_staging(staging_dir, ticker)
                if cached is not None:
                    staging[ticker] = cached
                    if i % 50 == 0:
                        logger.info("  [%d/%d] %s — loaded from staging cache", i, len(tickers), ticker)
                    continue

                t0 = time.monotonic()
                try:
                    # ohlcv_by_ticker is intentionally {} whenever --all-db-tickers
                    # is set (see caller) so the >1-worker path can load OHLCV
                    # itself per-ticker from DuckDB without a giant upfront
                    # preload. But the sequential path (n_workers==1, the
                    # default) has no other way to get OHLCV — falling back to
                    # ohlcv_by_ticker.get(ticker, pd.DataFrame()) silently NaNs
                    # every price-derived feature for every ticker (found
                    # 2026-07-05: rsi_14 etc. 100% null after a full sequential
                    # --all-db-tickers run "succeeded"). Load directly here
                    # instead of trusting the empty dict.
                    ohlcv = ohlcv_by_ticker.get(ticker)
                    if ohlcv is None:
                        ohlcv = _load_ohlcv_for_ticker(ticker, fno_conn)
                    fno_df = _load_fno_for_ticker(ticker, fno_conn, fno_eligible=fno_eligible)
                    mf_tk = mf_by_ticker.get(ticker, pd.DataFrame())
                    listing_dt = listing_dates.get(ticker)

                    df = compute_per_ticker(
                        ticker=ticker,
                        ohlcv=ohlcv,
                        fno_df=fno_df,
                        benchmark_wide=benchmark_wide,
                        all_dates=all_dates,
                        cache=cache,
                        mf_for_ticker=mf_tk,
                        listing_date=listing_dt,
                        compute_hmm=compute_hmm,
                    )
                    staging[ticker] = df
                    _save_staging(staging_dir, ticker, df)

                    elapsed = time.monotonic() - t0
                    if i % 50 == 0 or elapsed > 60:
                        logger.info(
                            "  [%d/%d] %s done in %.1f s (F&O rows: %d)",
                            i, len(tickers), ticker, elapsed, len(fno_df),
                        )
                except Exception as exc:
                    logger.error("  [%d/%d] %s FAILED in Stage 1: %s", i, len(tickers), ticker, exc)
                    staging[ticker] = _empty_staging(ticker, all_dates)

    logger.info(
        "Stage 1 complete: %d tickers in %.1f s",
        len(tickers), time.monotonic() - t0_stage1,
    )
    return staging


# ── Stage 2 orchestration ─────────────────────────────────────────────────────

def run_stage2(
    dates: List[date_type],
    staging: Dict[str, pd.DataFrame],
    benchmark_ohlcv: pd.DataFrame,
    sector_map: Dict[str, str],
    tier_map: Dict[str, str],
    macro_all: pd.DataFrame,
    tickers: List[str],
    features_dir: Path,
    force: bool,
    universe_ohlcv_panel: Optional[pd.DataFrame] = None,
    real_eco_df: Optional[pd.DataFrame] = None,
) -> None:
    """
    Stage 2: date assembly + write daily parquets.

    Precomputes multibagger features ONCE for the full date range before the
    per-date loop (vs the previous approach of calling compute_multibagger_features
    once per date with a 760-day window). This gives a ~15-20× speedup per chunk.

    Also accepts a pre-loaded real_eco_df to avoid reading the parquet file per date.
    """
    from features.hybrid_compute import assemble_date, build_benchmark_wide
    from features.matrix_builder import ALL_FEATURE_COLUMNS, _validate_feature_matrix
    from features.multibagger import compute_multibagger_features

    features_dir.mkdir(parents=True, exist_ok=True)

    # Check how many dates can be skipped before doing expensive precomputation
    pending_dates = [d for d in dates if force or not (features_dir / f"{d.isoformat()}.parquet").exists()]
    if not pending_dates:
        logger.info("Stage 2: all %d dates already exist — skipping precomputation", len(dates))
        return

    # Precompute multibagger features ONCE for all dates in this chunk.
    # Uses universe_ohlcv_panel (full history already in RAM) filtered to
    # [first_date - 760 days, last_date] — gives the rolling window for all dates.
    mb_by_date: Dict[pd.Timestamp, pd.DataFrame] = {}
    if universe_ohlcv_panel is not None and pending_dates:
        # min()/max() rather than pending_dates[0]/[-1] — Stage 2's default date
        # order is newest-first, which inverted this range (see the identical
        # class of bug fixed 2026-07-05 in run_stage2_chunked's chunk-date
        # filter). With [0]/[-1], first_ts was the newest date and last_ts the
        # oldest, so mb_panel got filtered to `date <= last_ts` (the OLDEST
        # date in the chunk) — computing multibagger features only up through
        # the start of the window instead of through today. The subsequent
        # `ts >= first_ts_norm` (comparing against the newest date) then never
        # matched, so mb_by_date came out EMPTY every chunk ("Multibagger
        # precomputed: 0 dates"), silently forcing every one of the 150 dates
        # per chunk down assemble_date's ~15-25s/date uncached fallback path
        # (~40-60 min/chunk) instead of the ~15s-for-the-whole-chunk fast path.
        first_ts = pd.Timestamp(min(pending_dates))
        last_ts = pd.Timestamp(max(pending_dates))
        window_cutoff = first_ts - pd.Timedelta(days=760)
        mb_panel = universe_ohlcv_panel[
            (universe_ohlcv_panel["date"] >= window_cutoff)
            & (universe_ohlcv_panel["date"] <= last_ts)
        ]
        bm_wide_full = build_benchmark_wide(benchmark_ohlcv)
        logger.info(
            "Precomputing multibagger for %d dates [%s → %s]: %.0f K panel rows …",
            len(pending_dates), first_ts.date(), last_ts.date(), len(mb_panel) / 1000,
        )
        t0_mb = time.monotonic()
        try:
            mb_all = compute_multibagger_features(mb_panel, bm_wide_full, sector_map)
            mb_all["date"] = pd.to_datetime(mb_all["date"])
            for ts, grp in mb_all.groupby("date"):
                if ts >= first_ts:
                    mb_by_date[ts] = grp.drop(columns="date").reset_index(drop=True)
            del mb_all, mb_panel
            logger.info(
                "Multibagger precomputed: %d dates in %.1f s",
                len(mb_by_date), time.monotonic() - t0_mb,
            )
        except Exception as exc:
            logger.warning("Multibagger precomputation failed (%s) — will fall back to per-date compute", exc)
            mb_by_date = {}

    ok = err = skip = 0
    elapsed_times: list = []

    logger.info("Stage 2: assembling %d date matrices …", len(dates))
    t0_stage2 = time.monotonic()

    for i, d in enumerate(dates, start=1):
        date_ts = pd.Timestamp(d)
        out_path = features_dir / f"{d.isoformat()}.parquet"

        if not force and out_path.exists():
            skip += 1
            continue

        t0 = time.monotonic()
        try:
            matrix = assemble_date(
                date=date_ts,
                staging=staging,
                benchmark_ohlcv=benchmark_ohlcv,
                sector_map=sector_map,
                tier_map=tier_map,
                macro_all=macro_all,
                tickers=tickers,
                universe_ohlcv_panel=universe_ohlcv_panel,
                mb_precomputed=mb_by_date if mb_by_date else None,
                real_eco_df=real_eco_df,
            )

            if matrix.empty:
                logger.warning("[%d/%d] %s — empty matrix, skipping write", i, len(dates), d)
                err += 1
                continue

            # Align to ALL_FEATURE_COLUMNS (add missing as NaN, drop extras)
            for col in ALL_FEATURE_COLUMNS:
                if col not in matrix.columns:
                    matrix[col] = np.nan
            matrix["date"] = date_ts
            matrix = matrix[["date", "ticker"] + ALL_FEATURE_COLUMNS]

            _validate_feature_matrix(matrix)
            matrix.to_parquet(out_path, index=False)

            elapsed = time.monotonic() - t0
            elapsed_times.append(elapsed)
            remaining = len(dates) - i
            avg = sum(elapsed_times[-50:]) / len(elapsed_times[-50:])
            eta_min = remaining * avg / 60

            if i % 100 == 0 or elapsed > 30:
                logger.info(
                    "[%d/%d] %s done in %.1f s — %d remaining, ETA ~%.0f min",
                    i, len(dates), d, elapsed, remaining, eta_min,
                )
            ok += 1

        except Exception as exc:
            err += 1
            logger.error("[%d/%d] %s FAILED in Stage 2: %s", i, len(dates), d, exc)

    total_s = time.monotonic() - t0_stage2
    logger.info(
        "Stage 2 complete: %d ok, %d skipped, %d failed — total %.1f min",
        ok, skip, err, total_s / 60,
    )


def run_stage2_chunked(
    staging_dir: Path,
    tickers: List[str],
    dates: List[date_type],
    benchmark_ohlcv: pd.DataFrame,
    sector_map: Dict[str, str],
    tier_map: Dict[str, str],
    macro_all: pd.DataFrame,
    features_dir: Path,
    force: bool,
    chunk_size: int = 200,
) -> None:
    """
    Stage 2 with date-range chunked staging loads for large universes.

    Instead of loading all staging parquets into RAM at once (which would OOM
    for 2000+ tickers), this loads staging filtered to `chunk_size` dates at a
    time using parquet predicate pushdown (~500 MB per chunk for 2492 tickers).

    Parameters
    ----------
    staging_dir : Path
        Directory containing per-ticker staging parquets (one file per ticker).
    tickers : list of str
        Universe to assemble — cross-sectional features are computed over all
        tickers present in staging_dir for each date.
    dates : list of date
        Dates to assemble in order.
    chunk_size : int
        Number of dates per chunk (default 200). Lower = less RAM, more I/O.
    """
    import gc

    from datastore.api.db import get_duckdb_connection
    from config.settings import DUCKDB_PATH

    features_dir.mkdir(parents=True, exist_ok=True)
    staging_files = {p.stem: p for p in staging_dir.glob("*.parquet")}
    available_tickers = [t for t in tickers if t in staging_files]

    logger.info(
        "Stage 2 (chunked): %d dates, chunk_size=%d, %d/%d tickers have staging",
        len(dates), chunk_size, len(available_tickers), len(tickers),
    )

    # Pre-load full-history OHLCV for all tickers (date, ticker + 6 OHLCV cols).
    # ~670 MB RAM for 2492 tickers × 4785 dates — stays resident across all chunks
    # so assemble_date can always build the full 760-day multibagger window regardless
    # of which chunk's staging is currently loaded.
    logger.info("Pre-loading full-history universe OHLCV panel from DuckDB …")
    ticker_list_sql = ", ".join(f"'{t}'" for t in available_tickers)
    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        universe_ohlcv_panel = conn.execute(f"""
            SELECT
                CAST(date AS TIMESTAMP) AS date,
                ticker,
                open, high, low, close, volume,
                COALESCE(delivery_pct, NULL) AS delivery_pct
            FROM ohlcv_adjusted
            WHERE ticker IN ({ticker_list_sql})
            ORDER BY date, ticker
        """).df()
    universe_ohlcv_panel["date"] = pd.to_datetime(universe_ohlcv_panel["date"])
    logger.info(
        "Universe OHLCV panel: %d rows, %d tickers, %.0f MB RAM",
        len(universe_ohlcv_panel),
        universe_ohlcv_panel["ticker"].nunique(),
        universe_ohlcv_panel.memory_usage(deep=True).sum() / 1e6,
    )

    # Pre-load real-economy macro parquet ONCE — avoids 200 pd.read_parquet calls per chunk.
    real_eco_df: Optional[pd.DataFrame] = None
    from pathlib import Path as _Path
    _re_path = _Path("datastore/normalised/macro_real_economy.parquet")
    if _re_path.exists():
        try:
            real_eco_df = pd.read_parquet(_re_path)
            real_eco_df["availability_date"] = pd.to_datetime(real_eco_df["availability_date"])
            real_eco_df["reference_month_end"] = pd.to_datetime(real_eco_df["reference_month_end"])
            logger.info("Real-economy macro pre-loaded: %d rows", len(real_eco_df))
        except Exception as exc:
            logger.warning("Failed to pre-load real_economy_macro parquet: %s", exc)

    t0_total = time.monotonic()

    for chunk_idx, chunk_start in enumerate(range(0, len(dates), chunk_size), start=1):
        chunk_dates = dates[chunk_start:chunk_start + chunk_size]
        # `dates` may be newest-first (the default Stage 2 ordering) or
        # chronological (--chronological) — min/max rather than [0]/[-1]
        # so the parquet predicate-pushdown filter below is never an
        # inverted (always-empty) range regardless of ordering. Found
        # 2026-07-05: every chunk was silently loading 0 tickers because
        # chunk_dates[0] > chunk_dates[-1] under the default newest-first
        # order, making "date >= d_start AND date <= d_end" impossible.
        d_start = pd.Timestamp(min(chunk_dates))
        d_end = pd.Timestamp(max(chunk_dates))
        n_chunks = (len(dates) + chunk_size - 1) // chunk_size

        logger.info(
            "Chunk %d/%d: loading staging for %s → %s (%d dates) …",
            chunk_idx, n_chunks, d_start.date(), d_end.date(), len(chunk_dates),
        )

        staging_chunk: Dict[str, pd.DataFrame] = {}
        load_errors = 0
        for ticker in available_tickers:
            try:
                df = pd.read_parquet(
                    staging_files[ticker],
                    filters=[
                        ("date", ">=", d_start),
                        ("date", "<=", d_end),
                    ],
                )
                if not df.empty:
                    staging_chunk[ticker] = df
            except Exception as exc:
                load_errors += 1
                if load_errors <= 5:
                    logger.warning("Failed to load staging for %s: %s", ticker, exc)

        mem_mb = sum(df.memory_usage(deep=True).sum() for df in staging_chunk.values()) / 1e6
        logger.info(
            "Chunk %d/%d loaded: %d tickers, %.0f MB RAM, %d load errors",
            chunk_idx, n_chunks, len(staging_chunk), mem_mb, load_errors,
        )

        run_stage2(
            dates=chunk_dates,
            staging=staging_chunk,
            benchmark_ohlcv=benchmark_ohlcv,
            sector_map=sector_map,
            tier_map=tier_map,
            macro_all=macro_all,
            tickers=tickers,
            features_dir=features_dir,
            force=force,
            universe_ohlcv_panel=universe_ohlcv_panel,
            real_eco_df=real_eco_df,
        )

        del staging_chunk
        gc.collect()

        elapsed_min = (time.monotonic() - t0_total) / 60
        pct_done = min(chunk_start + chunk_size, len(dates)) / len(dates)
        eta_min = elapsed_min / pct_done * (1 - pct_done) if pct_done > 0 else 0
        logger.info(
            "Chunk %d/%d done — %.0f min elapsed, ETA ~%.0f min",
            chunk_idx, n_chunks, elapsed_min, eta_min,
        )

    logger.info(
        "Stage 2 chunked complete in %.1f min",
        (time.monotonic() - t0_total) / 60,
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    args = _parse_args()

    from config.settings import DUCKDB_PATH, FEATURES_DAILY_DIR
    from config.universe import get_tickers_for_feature_engineering, load_universe
    from datastore.client import DataStoreClient
    from features.backfill_cache import BackfillDataCache
    from datetime import datetime

    from_dt = date_type.fromisoformat(args.from_date)
    to_dt = date_type.fromisoformat(args.to_date) if args.to_date else date_type.today()
    compute_hmm = not args.no_hmm

    staging_dir = (
        Path(args.staging_dir)
        if args.staging_dir
        else FEATURES_DAILY_DIR.parent / "staging"
    )

    logger.info("=" * 60)
    logger.info("Hybrid feature backfill: %s → %s", from_dt, to_dt)
    logger.info(
        "HMM: %s | workers: %d | staging_dir: %s",
        "enabled" if compute_hmm else "disabled", args.workers, staging_dir,
    )
    logger.info("=" * 60)

    # ── Get trading dates ──────────────────────────────────────────────────
    all_trading_dates = _get_trading_dates(from_dt, to_dt, DUCKDB_PATH)
    logger.info("Found %d trading dates", len(all_trading_dates))
    if not all_trading_dates:
        logger.info("No trading dates found — nothing to do.")
        return

    all_dates_ts = [pd.Timestamp(d) for d in all_trading_dates]

    # Stage 2 ordering (newest-first by default, same as feature_backfill.py)
    stage2_dates = list(reversed(all_trading_dates)) if not args.chronological else all_trading_dates

    # Skip dates already written (Stage 2 also does its own skip check).
    # When Stage 2 is being skipped entirely (--all-db-tickers / --skip-stage2),
    # don't early-exit here — Stage 1 may still have tickers to process.
    skip_stage2_early = args.all_db_tickers or args.skip_stage2 or args.rebuild_daily
    if not args.force and not skip_stage2_early:
        existing = {p.stem for p in FEATURES_DAILY_DIR.glob("*.parquet")}
        pending = [d for d in stage2_dates if d.isoformat() not in existing]
        logger.info(
            "%d already have parquets, %d pending",
            len(stage2_dates) - len(pending), len(pending),
        )
        if not pending:
            logger.info("All dates complete. Use --force to recompute.")
            return
        stage2_dates = pending

    # ── Universe metadata ──────────────────────────────────────────────────
    from datastore.api.db import get_duckdb_connection

    universe_meta = load_universe()
    sector_map = dict(zip(universe_meta["ticker"], universe_meta["sector"]))
    tier_map = dict(zip(universe_meta["ticker"], universe_meta["tier"].fillna("UNKNOWN")))

    if args.all_db_tickers:
        # Expand to every ticker in ohlcv_adjusted; use UNKNOWN for sector/tier
        # if the ticker is not in the universe CSV.
        with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
            if args.active_only:
                # Keep only tickers that have traded within 30 calendar days of the
                # last date in the DB — removes delisted/suspended stocks.
                db_rows = conn.execute("""
                    SELECT DISTINCT ticker FROM ohlcv_adjusted
                    WHERE date >= (
                        SELECT CAST(MAX(date) - INTERVAL 30 DAYS AS DATE)
                        FROM ohlcv_adjusted
                    )
                    ORDER BY ticker
                """).fetchall()
            else:
                db_rows = conn.execute(
                    "SELECT DISTINCT ticker FROM ohlcv_adjusted ORDER BY ticker"
                ).fetchall()
        tickers = [r[0] for r in db_rows]
        for t in tickers:
            sector_map.setdefault(t, "UNKNOWN")
            tier_map.setdefault(t, "UNKNOWN")
        # Derive listing dates from first OHLCV date per ticker
        with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
            ld_rows = conn.execute(
                "SELECT ticker, CAST(MIN(date) AS VARCHAR) FROM ohlcv_adjusted GROUP BY ticker"
            ).fetchall()
        listing_dates: Dict[str, Optional[object]] = {
            r[0]: pd.Timestamp(r[1]).to_pydatetime() for r in ld_rows
        }
        already_done = sum(1 for t in tickers if (staging_dir / f"{t}.parquet").exists())
        logger.info(
            "Universe (--all-db-tickers%s): %d tickers from ohlcv_adjusted "
            "(%d already have staging parquets, %d to compute)",
            " --active-only" if args.active_only else "",
            len(tickers),
            already_done,
            len(tickers) - already_done,
        )
    else:
        tickers = get_tickers_for_feature_engineering()
        listing_dates: Dict[str, Optional[object]] = {}
        if "listing_date" in universe_meta.columns:
            listing_dates = {
                row["ticker"]: pd.Timestamp(row["listing_date"]).to_pydatetime()
                if pd.notna(row.get("listing_date")) else None
                for _, row in universe_meta.iterrows()
            }
        logger.info("Universe: %d tickers", len(tickers))

    if args.ticker_batch_size:
        tickers = sorted(tickers)
        start = args.ticker_batch_index * args.ticker_batch_size
        end = start + args.ticker_batch_size
        batch = tickers[start:end]
        n_batches = (len(tickers) + args.ticker_batch_size - 1) // args.ticker_batch_size
        logger.info(
            "Ticker batch %d/%d: %d tickers (of %d total) — indices [%d:%d]",
            args.ticker_batch_index + 1, n_batches, len(batch), len(tickers), start, end,
        )
        tickers = batch
        listing_dates = {t: listing_dates[t] for t in tickers if t in listing_dates}

    # Stage 2 is always skipped in --all-db-tickers mode (loading 4000+ staging
    # parquets into RAM at once would OOM; Stage 2 is re-run separately on the
    # Nifty 500 universe once all per-ticker staging is ready).
    skip_stage2 = args.all_db_tickers or args.skip_stage2

    # ── Shortcut: --rebuild-daily skips Stage 1 and goes straight to chunked Stage 2 ──
    if args.rebuild_daily:
        logger.info("--rebuild-daily: skipping Stage 1, loading staging from disk in chunks.")
        benchmark_ohlcv = _load_benchmark_ohlcv(DUCKDB_PATH)
        macro_all = _load_all_macro(from_dt, to_dt, DUCKDB_PATH)
        run_stage2_chunked(
            staging_dir=staging_dir,
            tickers=tickers,
            dates=stage2_dates,
            benchmark_ohlcv=benchmark_ohlcv,
            sector_map=sector_map,
            tier_map=tier_map,
            macro_all=macro_all,
            features_dir=FEATURES_DAILY_DIR,
            force=args.force,
            chunk_size=args.stage2_chunk_size,
        )
        logger.info("Hybrid backfill complete.")
        return

    # ── Pre-load phase ─────────────────────────────────────────────────────
    t0_preload = time.monotonic()

    # (a) BackfillDataCache: fundamentals, shareholding, corp_actions via API
    client = DataStoreClient()
    to_datetime = datetime.combine(to_dt, datetime.min.time())
    logger.info("Pre-loading BackfillDataCache (fundamentals / shareholding / corp_actions) …")
    cache = BackfillDataCache(client, tickers, to_date=to_datetime)

    # (b) OHLCV per ticker — only needed for the single-process path.
    # In parallel mode workers load from DuckDB directly, so skip the pre-load
    # when using --workers > 1 or --all-db-tickers (which can have 4000+ tickers).
    if args.workers == 1 and not args.all_db_tickers:
        ohlcv_by_ticker = _load_ohlcv_per_ticker(tickers, DUCKDB_PATH)
    else:
        ohlcv_by_ticker = {}

    # (c) Benchmark OHLCV (Nifty50, Nifty100, Nifty500 ETF proxies)
    benchmark_ohlcv = _load_benchmark_ohlcv(DUCKDB_PATH)
    logger.info("Benchmark OHLCV: %d rows for %d tickers", len(benchmark_ohlcv), benchmark_ohlcv["ticker"].nunique())

    # (d) Macro indicators (one DuckDB query for full date range)
    macro_all = _load_all_macro(from_dt, to_dt, DUCKDB_PATH)

    # (e) MF holdings (one parquet directory scan)
    mf_by_ticker = _load_all_mf_holdings()

    preload_s = time.monotonic() - t0_preload
    logger.info("Pre-load phase complete in %.1f s", preload_s)

    # ── Stage 1: per-ticker computation ───────────────────────────────────
    staging = run_stage1(
        tickers=tickers,
        all_dates=all_dates_ts,
        ohlcv_by_ticker=ohlcv_by_ticker,
        benchmark_ohlcv=benchmark_ohlcv,
        cache=cache,
        mf_by_ticker=mf_by_ticker,
        listing_dates=listing_dates,
        compute_hmm=compute_hmm,
        staging_dir=staging_dir,
        duckdb_path=DUCKDB_PATH,
        n_workers=args.workers,
        load_for_stage2=not skip_stage2,
    )

    # Release per-ticker OHLCV cache — no longer needed after Stage 1
    del ohlcv_by_ticker

    # ── Stage 2: date assembly ─────────────────────────────────────────────
    if skip_stage2:
        logger.info(
            "Stage 2 skipped (%s). Re-run without --all-db-tickers / --skip-stage2 "
            "to rebuild daily parquets.",
            "--all-db-tickers" if args.all_db_tickers else "--skip-stage2",
        )
    else:
        run_stage2(
            dates=stage2_dates,
            staging=staging,
            benchmark_ohlcv=benchmark_ohlcv,
            sector_map=sector_map,
            tier_map=tier_map,
            macro_all=macro_all,
            tickers=tickers,
            features_dir=FEATURES_DAILY_DIR,
            force=args.force,
        )

    logger.info("Hybrid backfill complete.")


if __name__ == "__main__":
    main()
