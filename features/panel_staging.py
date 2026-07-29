"""
features/panel_staging.py

Phase: 3 (feature-backfill performance)
Owner: Platform / Features
Consumers: scripts/feature_backfill.py

Batch pre-computation + DuckDB staging for the 5 ticker-independent,
rolling-window feature categories (technical/intraday/pnd/
advanced_technical/pattern_scores — see features/matrix_builder.py's
`_compute_chunked_ticker_independent_panels` docstring for why exactly
these 5 and not fundamental/governance/mf_holdings/corp_action/fno/
deep_forensic/real_economy_macro/multibagger/hmm).

Root cause this fixes
----------------------
scripts/feature_backfill.py's per-date loop calls
features/matrix_builder.py::build_feature_matrix once PER DATE. For each
date it fetches a 760-calendar-day trailing OHLCV window and computes
these 5 categories over that whole window, then keeps only the target
date's row and discards the rest. Each category function's own contract
is "one row per (ticker, date) in the input panel" — i.e. calling it on a
WIDE multi-date panel already returns every date's row in a single pass.
Calling it once per date over an overlapping window therefore redoes the
same (ticker, date) rolling-window math up to ~760 times across a full
historical backfill.

This module computes each ticker-chunk's 5 categories ONCE across the
union of every requested backfill date (plus the 760-day lookback buffer
before the EARLIEST requested date), then persists the result to a
DuckDB staging table as each chunk finishes — not held in one large
in-memory structure spanning the whole run. This keeps peak memory
bounded to one chunk's derived DataFrames at a time (mirrors
matrix_builder.py's existing per-chunk memory-bounding rationale) and
makes a crash between "compute" and "the per-date write pass" recoverable
in principle (the already-staged chunks survive; only IN-PROGRESS/
un-staged work is lost) — full resume-from-partial-staging logic is not
implemented here, but nothing about this schema/design precludes it.

Storage: a SEPARATE DuckDB file
(config.settings.FEATURE_PANEL_STAGING_DB_PATH), not the main
alphalens.duckdb — a multi-hour backfill holding this file's writer lock
must never contend with the live scheduler/API process's own writes to
alphalens.duckdb (this project has hit real single-writer DuckDB
contention/timeouts before). Rows are keyed by (run_id, ticker, date) so
multiple backfill runs (e.g. a retry) never collide; call
`drop_staging_run` once a run's per-date write pass has fully consumed
its staged rows.

NOT used by the live daily pipeline
------------------------------------
ingestion/scheduler/daily_pipeline.py::step_compute_features never calls
into this module — it only ever computes ONE date, for which the
original per-date computation in matrix_builder.py is already efficient
and remains completely unchanged (features/matrix_builder.py::
build_feature_matrix's `staged_panel=None` default preserves the original
behavior byte-for-byte).
"""

import gc
import logging
from typing import List, Optional

import pandas as pd

from config.settings import FEATURE_PANEL_STAGING_DB_PATH, PIPELINE_MEMORY_CEILING_MB, SCREENER_BATCH_EXPORT_CHUNK_SIZE
from datastore.api.db import get_duckdb_connection
from datastore.client import DataStoreClient
from features.advanced_technical import ADVANCED_TECHNICAL_FEATURES
from features.intraday import INTRADAY_FEATURES
from features.matrix_builder import (
    LOOKBACK_CALENDAR_DAYS,
    BENCHMARK_TICKERS,
    _build_benchmark_wide,
    _fetch_ohlcv_panel,
    compute_full_range_chunk_panels,
)
from features.pattern_scores import PATTERN_FEATURES
from features.pnd_features import PND_FEATURES
from features.technical import CORE_TECHNICAL_FEATURES
from ingestion.scheduler.resource_guard import adaptive_chunk_size

logger = logging.getLogger(__name__)

_TABLE_NAME = "feature_panel_staging"

# Union of the 5 batched categories' feature columns — verified column-name
# disjoint across all 5 (no category shares a column name with another).
_STAGED_FEATURE_COLUMNS: List[str] = (
    CORE_TECHNICAL_FEATURES + INTRADAY_FEATURES + PND_FEATURES + ADVANCED_TECHNICAL_FEATURES + PATTERN_FEATURES
)

# [2026-07-29] A batch chunk's panel now spans the FULL requested backfill
# date range (potentially years) instead of one 760-day window — one row
# per ticker per date, not one row per ticker. The existing
# SCREENER_BATCH_EXPORT_CHUNK_SIZE (tickers per chunk for a single-date
# panel) would make each chunk's derived DataFrames roughly
# (n_requested_dates) times larger than the single-date case. Divide the
# adaptive per-date chunk size down so a chunk's total row count
# (tickers_per_chunk * dates) stays in a comparable ballpark — a
# conservative fixed divisor rather than a dynamic one keyed off the
# actual date count, since the memory cost scales linearly with dates and
# a fixed floor(1) still guarantees forward progress on a huge range.
_BATCH_CHUNK_SIZE_DIVISOR = 10
_BATCH_CHUNK_SIZE_FLOOR = 1


def _ensure_staging_table(conn) -> None:
    cols_ddl = ",\n            ".join(f'"{c}" DOUBLE' for c in _STAGED_FEATURE_COLUMNS)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_TABLE_NAME} (
            run_id VARCHAR NOT NULL,
            ticker VARCHAR NOT NULL,
            date VARCHAR NOT NULL,
            {cols_ddl},
            PRIMARY KEY (run_id, ticker, date)
        )
        """
    )
    conn.execute(
        f'CREATE INDEX IF NOT EXISTS idx_{_TABLE_NAME}_run_date ON {_TABLE_NAME} (run_id, date)'
    )


def _stage_chunk(conn, run_id: str, chunk_wide: pd.DataFrame) -> None:
    """Bulk-insert one chunk's wide (ticker, date) + feature-column rows.

    Mirrors features/fundamental_cache.py's established
    conn.register()+INSERT-SELECT bulk-write convention (that module's
    docstring is this codebase's precedent for a DuckDB-backed feature
    cache/staging table).
    """
    if chunk_wide.empty:
        return
    to_insert = chunk_wide.copy()
    to_insert["run_id"] = run_id
    to_insert["date"] = to_insert["date"].astype(str)
    ordered_cols = ["run_id", "ticker", "date"] + _STAGED_FEATURE_COLUMNS
    for c in _STAGED_FEATURE_COLUMNS:
        if c not in to_insert.columns:
            to_insert[c] = float("nan")
    to_insert = to_insert[ordered_cols]

    conn.register("_panel_staging_chunk", to_insert)
    try:
        col_list = ", ".join(f'"{c}"' for c in ordered_cols)
        conn.execute(
            f"""
            INSERT INTO {_TABLE_NAME} ({col_list})
            SELECT {col_list} FROM _panel_staging_chunk
            ON CONFLICT (run_id, ticker, date) DO UPDATE SET
                {", ".join(f'"{c}" = excluded."{c}"' for c in _STAGED_FEATURE_COLUMNS)}
            """
        )
    finally:
        conn.unregister("_panel_staging_chunk")


def stage_batch_panels(
    client: DataStoreClient,
    tickers: List[str],
    dates: List[pd.Timestamp],
    run_id: str,
    db_path=None,
) -> int:
    """
    Compute technical/intraday/pnd/adv_tech/patterns ONCE per ticker chunk
    across the full [min(dates) - LOOKBACK_CALENDAR_DAYS, max(dates)]
    range, and stage each chunk's rows for `dates` only (the lookback
    buffer's own rows are discarded after each chunk's compute — they only
    exist to warm up rolling windows, never written to the staging table)
    into config.settings.FEATURE_PANEL_STAGING_DB_PATH, one chunk at a
    time, freeing that chunk's DataFrames before starting the next.

    Parameters
    ----------
    client : DataStoreClient
    tickers : list of str
        Full universe to stage (the per-date write pass later still
        applies its own per-date not-yet-listed-ticker filtering the same
        way the original per-date path does — this function does not need
        to replicate that, missing OHLCV rows for a not-yet-listed ticker
        already yield NaN features the same as the per-date path).
    dates : list of pd.Timestamp
        Every date the backfill run needs staged rows for.
    run_id : str
        Keys this run's staged rows so concurrent/retried backfill runs
        never collide; pass the same value to `load_staged_panel_for_date`
        and `drop_staging_run`.
    db_path : Path, optional
        Defaults to FEATURE_PANEL_STAGING_DB_PATH.

    Returns
    -------
    int
        Total (ticker, date) rows staged.
    """
    if not dates:
        return 0

    dates = sorted(pd.Timestamp(d) for d in dates)
    earliest, latest = dates[0], dates[-1]
    requested_dates = set(dates)

    from_date = (earliest - pd.Timedelta(days=LOOKBACK_CALENDAR_DAYS)).to_pydatetime()
    to_date = latest.to_pydatetime()

    logger.info(
        "panel_staging: fetching bulk OHLCV for %d tickers, %s -> %s (%d requested dates)",
        len(tickers), from_date.date(), to_date.date(), len(dates),
    )

    bulk_panel = None
    bulk_loader = getattr(client, "get_ohlcv_bulk", None)
    if callable(bulk_loader):
        try:
            bulk_panel = bulk_loader(from_date, to_date)
        except Exception as exc:
            logger.warning("panel_staging: bulk OHLCV fetch failed, falling back to per-ticker fetch: %s", exc)

    universe_panel = _fetch_ohlcv_panel(client, tickers, from_date, to_date, _bulk_panel=bulk_panel)
    benchmark_panel = _fetch_ohlcv_panel(
        client, list(BENCHMARK_TICKERS.values()), from_date, to_date, _bulk_panel=bulk_panel
    )
    benchmark_wide = _build_benchmark_wide(benchmark_panel)

    if universe_panel.empty:
        raise RuntimeError(
            f"panel_staging: no OHLCV data returned for any of {len(tickers)} tickers over "
            f"{from_date.date()}..{to_date.date()} — the DataStore API is very likely unreachable."
        )

    path = db_path or FEATURE_PANEL_STAGING_DB_PATH
    with get_duckdb_connection(path, read_only=False, persist=False) as conn:
        _ensure_staging_table(conn)
        conn.execute(f"DELETE FROM {_TABLE_NAME} WHERE run_id = ?", [run_id])

        total_staged = 0
        i = 0
        n_chunks = 0
        while i < len(tickers):
            base_chunk_size = adaptive_chunk_size(SCREENER_BATCH_EXPORT_CHUNK_SIZE, ceiling_mb=PIPELINE_MEMORY_CEILING_MB)
            chunk_size = max(_BATCH_CHUNK_SIZE_FLOOR, base_chunk_size // _BATCH_CHUNK_SIZE_DIVISOR)
            chunk_tickers = tickers[i : i + chunk_size]
            i += chunk_size

            chunk_panel = universe_panel[universe_panel["ticker"].isin(set(chunk_tickers))]
            if chunk_panel.empty:
                continue
            n_chunks += 1

            technical, intraday, pnd, adv_tech, patterns = compute_full_range_chunk_panels(
                chunk_panel, benchmark_wide
            )

            # Outer-merge the 5 category outputs into one wide (ticker, date)
            # row set, restricted to the requested dates only (the lookback
            # buffer's own rows are dropped here — never staged).
            wide = None
            for df, cols in (
                (technical, CORE_TECHNICAL_FEATURES),
                (intraday, INTRADAY_FEATURES),
                (pnd, PND_FEATURES),
                (adv_tech, ADVANCED_TECHNICAL_FEATURES),
                (patterns, PATTERN_FEATURES),
            ):
                sub = df[df["date"].isin(requested_dates)][["date", "ticker"] + cols] if not df.empty else pd.DataFrame(columns=["date", "ticker"] + cols)
                wide = sub if wide is None else wide.merge(sub, on=["date", "ticker"], how="outer")

            if wide is not None and not wide.empty:
                _stage_chunk(conn, run_id, wide)
                total_staged += len(wide)

            logger.info(
                "panel_staging: chunk %d staged (%d tickers, %d rows) — %d/%d tickers done",
                n_chunks, len(chunk_tickers), 0 if wide is None else len(wide), min(i, len(tickers)), len(tickers),
            )

            del chunk_panel, technical, intraday, pnd, adv_tech, patterns, wide
            gc.collect()

    logger.info("panel_staging: run_id=%s staged %d total (ticker, date) rows across %d chunks", run_id, total_staged, n_chunks)
    return total_staged


def load_staged_panel_for_date(run_id: str, target_date: pd.Timestamp, db_path=None) -> Optional[pd.DataFrame]:
    """
    Fast indexed lookup of one date's already-staged rows, for the per-date
    write pass to hand to build_feature_matrix(..., staged_panel=...).

    Returns
    -------
    pd.DataFrame or None
        Columns: ['ticker'] + the 5 categories' feature columns. None if
        nothing is staged for (run_id, target_date) — caller should treat
        this as "batch staging wasn't run for this date" and fall back to
        the original per-date computation path.
    """
    path = db_path or FEATURE_PANEL_STAGING_DB_PATH
    date_str = pd.Timestamp(target_date).date().isoformat()
    col_list = ", ".join(f'"{c}"' for c in _STAGED_FEATURE_COLUMNS)
    with get_duckdb_connection(path, read_only=False, persist=False) as conn:
        _ensure_staging_table(conn)
        df = conn.execute(
            f'SELECT ticker, {col_list} FROM {_TABLE_NAME} WHERE run_id = ? AND date = ?',
            [run_id, date_str],
        ).fetchdf()
    if df.empty:
        return None
    return df


def drop_staging_run(run_id: str, db_path=None) -> None:
    """Delete all staged rows for `run_id` — call once its per-date write
    pass has fully consumed them (backfill completed, or is being retried
    from scratch)."""
    path = db_path or FEATURE_PANEL_STAGING_DB_PATH
    try:
        with get_duckdb_connection(path, read_only=False, persist=False) as conn:
            _ensure_staging_table(conn)
            conn.execute(f"DELETE FROM {_TABLE_NAME} WHERE run_id = ?", [run_id])
        logger.info("panel_staging: dropped staged rows for run_id=%s", run_id)
    except Exception as exc:
        logger.warning("panel_staging: could not drop staged rows for run_id=%s (%s)", run_id, exc)
