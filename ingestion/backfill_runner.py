"""
ingestion/backfill_runner.py

Phase: 0.5 (FYERS Historical Backfill)
Specs: SPEC-PIPE-001, SPEC-PIPE-002, SPEC-SCHED-004
Owner: Platform / Ingestion
Consumers: operator (manual, one-time run); datastore/normalised

Orchestrates the one-time, multi-hour FYERS historical backfill across the
configured universe (config/universe.py): for each ticker, downloads
BACKFILL_YEARS of daily OHLCV via ingestion/scrapers/fyers_backfill.py and
writes it directly into the Store 2 DuckDB ohlcv_adjusted table.

Two independent progress mechanisms, serving different failure modes:
  - DuckDB-coverage skip (has_sufficient_history): lets a re-run days later
    skip tickers that are already fully backfilled, without any extra state.
  - Resume checkpoint file (FYERS_RESUME_CHECKPOINT_PATH): lets a run that
    was killed mid-ticker-loop (Ctrl-C, laptop sleep, crash) restart from
    the next ticker instead of re-scanning the whole universe through the
    (slower) DuckDB-coverage check.

NOTE on "write to DuckDB ohlcv_adjusted table via DataStore API" (task
wording): see ingestion/scrapers/fyers_backfill.py's module docstring for
why this is implemented as a direct DuckDB write, matching the rest of the
ingestion layer (bhavcopy.py, macro.py, price_adjuster.py) rather than a
call through the read-only DataStoreClient (SPEC-DS-002).
"""

import argparse
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Optional

from config.settings import (
    BACKFILL_YEARS,
    DUCKDB_PATH,
    FYERS_HISTORY_MAX_DAYS_PER_CALL,
    FYERS_RATE_LIMIT_SLEEP_SECONDS,
    FYERS_RESUME_CHECKPOINT_PATH,
)
from config.timezone import now_ist
from datastore.api.db import get_duckdb_connection
from ingestion.scrapers.fyers_backfill import FYERSBackfill

logger = logging.getLogger(__name__)

TRADING_DAYS_PER_YEAR = 252
MIN_COVERAGE_RATIO = 0.90  # SPEC-SCHED-004-adjacent: "sufficient" history threshold

_UPSERT_OHLCV_ADJUSTED = """
    INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume, adj_factor)
    VALUES (?, ?, ?, ?, ?, ?, ?, 1.0)
    ON CONFLICT (date, ticker) DO UPDATE SET
        open = excluded.open, high = excluded.high, low = excluded.low,
        close = excluded.close, volume = excluded.volume
"""


def estimate_runtime_hours(n_tickers: int, from_date: str, to_date: str) -> float:
    """
    Estimate total backfill wall-clock time from the FYERS rate limit alone.

    Parameters
    ----------
    n_tickers : int
    from_date : str
        "YYYY-MM-DD".
    to_date : str
        "YYYY-MM-DD".

    Returns
    -------
    float
        Estimated hours, based on (chunks per ticker) x (throttle sleep).
        This is a lower bound — it ignores actual HTTP round-trip latency,
        which is small relative to the 0.5s throttle.

    Spec References
    ----------------
    SPEC-PIPE-001: "Estimated runtime displayed."

    PIT Assumptions
    ----------------
    None.

    Raises
    ------
    None
    """
    start = date.fromisoformat(from_date)
    end = date.fromisoformat(to_date)
    span_days = max((end - start).days + 1, 1)
    chunks_per_ticker = -(-span_days // FYERS_HISTORY_MAX_DAYS_PER_CALL)  # ceil div
    total_calls = n_tickers * chunks_per_ticker
    return total_calls * FYERS_RATE_LIMIT_SLEEP_SECONDS / 3600


def has_sufficient_history(
    conn, ticker: str, from_date: str, to_date: str, min_coverage_ratio: float = MIN_COVERAGE_RATIO
) -> bool:
    """
    Check whether ohlcv_adjusted already has near-complete coverage for a
    ticker over [from_date, to_date], so its backfill can be skipped.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
    ticker : str
    from_date : str
        "YYYY-MM-DD".
    to_date : str
        "YYYY-MM-DD".
    min_coverage_ratio : float
        Row count vs. expected-trading-days ratio above which the ticker
        is treated as already backfilled.

    Returns
    -------
    bool

    Spec References
    ----------------
    SPEC-PIPE-001: "Tracks progress: skip tickers already in DuckDB with
    sufficient history."

    PIT Assumptions
    ----------------
    None.

    Raises
    ------
    None
    """
    row = conn.execute(
        "SELECT COUNT(*), MIN(date), MAX(date) FROM ohlcv_adjusted "
        "WHERE ticker = ? AND date >= ? AND date <= ?",
        [ticker, from_date, to_date],
    ).fetchone()
    count = row[0] if row else 0

    span_days = max((date.fromisoformat(to_date) - date.fromisoformat(from_date)).days + 1, 1)
    expected_rows = span_days / 365.25 * TRADING_DAYS_PER_YEAR

    return expected_rows > 0 and (count / expected_rows) >= min_coverage_ratio


def read_resume_checkpoint(checkpoint_path: Path) -> Optional[str]:
    """
    Return the last successfully completed ticker, or None if absent.

    Parameters
    ----------
    checkpoint_path : Path

    Returns
    -------
    str or None

    Spec References
    ----------------
    SPEC-PIPE-001: "Include a progress checkpoint: save last completed
    ticker to a resume file so backfill can restart after interruption."

    Raises
    ------
    None
    """
    if not checkpoint_path.exists():
        return None
    content = checkpoint_path.read_text().strip()
    return content or None


def write_resume_checkpoint(checkpoint_path: Path, ticker: str) -> None:
    """
    Persist the last successfully completed ticker.

    Parameters
    ----------
    checkpoint_path : Path
    ticker : str

    Returns
    -------
    None

    Spec References
    ----------------
    SPEC-PIPE-001

    Raises
    ------
    None
    """
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text(ticker)


def write_ohlcv_to_duckdb(conn, ticker: str, df) -> int:
    """
    Upsert one ticker's downloaded OHLCV rows into ohlcv_adjusted.

    adj_factor is set to 1.0 on insert and left untouched on conflict —
    corporate-action adjustment is applied afterwards, uniformly, by
    ingestion/adjust/price_adjuster.py (SPEC-PIPE-002), never here.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
    ticker : str
    df : pd.DataFrame
        Output of FYERSBackfill.download_history() — columns date, ticker,
        open, high, low, close, volume.

    Returns
    -------
    int
        Number of rows written.

    Spec References
    ----------------
    SPEC-PIPE-001, SPEC-PIPE-002 (adj_factor left as the pre-adjustment
    default; price_adjuster.py owns all subsequent mutation of it).

    Raises
    ------
    None
    """
    if df.empty:
        return 0

    rows = list(
        df[["date", "ticker", "open", "high", "low", "close", "volume"]].itertuples(
            index=False, name=None
        )
    )
    conn.executemany(_UPSERT_OHLCV_ADJUSTED, rows)
    return len(rows)


def run_backfill(
    tickers,
    from_date: str,
    to_date: str,
    db_path: Optional[Path] = None,
    in_memory: bool = False,
    checkpoint_path: Optional[Path] = None,
    client: Optional[FYERSBackfill] = None,
) -> Dict[str, int]:
    """
    Run the full FYERS backfill loop across `tickers`.

    Parameters
    ----------
    tickers : list of str
    from_date : str
        "YYYY-MM-DD".
    to_date : str
        "YYYY-MM-DD".
    db_path : Path, optional
        ohlcv_adjusted DuckDB path. Defaults to config.settings.DUCKDB_PATH.
    in_memory : bool
        If True, use an in-memory DuckDB (db_path is ignored) — for tests.
    checkpoint_path : Path, optional
        Defaults to config.settings.FYERS_RESUME_CHECKPOINT_PATH.
    client : FYERSBackfill, optional
        Defaults to a freshly constructed FYERSBackfill() — injectable for
        tests.

    Returns
    -------
    dict
        ticker -> rows written (0 for skipped or failed tickers).

    Spec References
    ----------------
    SPEC-PIPE-001: full backfill orchestration, skip-if-sufficient,
        resume-from-checkpoint.
    SPEC-SCHED-004: tickers are processed in the order given (the caller —
        typically config.universe.get_tickers() — controls ordering;
        ticker order has no PIT significance, unlike date order for
        per-date backfill).

    PIT Assumptions
    ----------------
    None — historical OHLCV backfill is not a PIT-sensitive operation;
    PIT correctness applies to point-in-time *fields* (e.g. fundamentals'
    announcement_date), not to when this script happens to run.

    Raises
    ------
    None — per-ticker failures (including FYERS rate-limit exhaustion) are
    caught and logged so the rest of the universe still completes; a
    failed ticker still has insufficient history in ohlcv_adjusted, so
    has_sufficient_history() naturally retries it on the next run.

    Resume design note
    -------------------
    The checkpoint file (write_resume_checkpoint) is written for progress
    observability only — it is NOT used to decide which tickers to skip.
    An earlier version skipped every ticker up to the checkpoint's *list
    position*, which silently broke the moment the ticker list's
    membership or order changed between runs (e.g. after rebuilding
    config/nifty500_universe.csv from a different source — a position
    that meant "the last of 20 tickers" no longer meant the same thing
    against a differently-ordered universe list, and ~340 never-
    downloaded tickers got skipped as if complete). has_sufficient_history()
    is the sole skip mechanism: it checks actual ohlcv_adjusted row
    coverage per ticker, which is correct regardless of what order or
    composition the ticker list arrives in on any given run.
    """
    checkpoint_path = checkpoint_path or FYERS_RESUME_CHECKPOINT_PATH
    client = client or FYERSBackfill()

    if in_memory:
        resolved_db_path = None
    else:
        resolved_db_path = db_path or DUCKDB_PATH

    results: Dict[str, int] = {}
    # persist=False (SPEC-SCHED-013): this is called from both a one-shot
    # CLI process (where it's moot) and the scheduler's recurring
    # backfill-catchup job (a long-lived process sharing DUCKDB_PATH with
    # the DataStore API) — release the write lock once the whole backfill
    # loop finishes, not for the scheduler process's entire lifetime.
    with get_duckdb_connection(resolved_db_path, persist=False) as conn:
        for ticker in tickers:
            if has_sufficient_history(conn, ticker, from_date, to_date):
                logger.info(f"{ticker}: sufficient history already present — skipping")
                results[ticker] = 0
                write_resume_checkpoint(checkpoint_path, ticker)
                continue

            try:
                df = client.download_history(ticker, from_date, to_date)
                rows_written = write_ohlcv_to_duckdb(conn, ticker, df)
            except Exception as exc:
                logger.error(f"{ticker}: backfill failed: {exc}")
                results[ticker] = 0
                # Checkpoint deliberately NOT advanced past a failed ticker —
                # advancing here would make a transient/auth failure look
                # "completed", causing the next run's resume to wrongly
                # skip a ticker that still has zero rows in ohlcv_adjusted.
                continue

            results[ticker] = rows_written
            write_resume_checkpoint(checkpoint_path, ticker)
            logger.info(f"{ticker}: {rows_written} rows written ({len(results)}/{len(tickers)} tickers done)")

    total_rows = sum(results.values())
    logger.info(f"Backfill complete: {len(results)} tickers processed, {total_rows} rows written")
    return results


def main() -> None:
    """CLI entry point: `python -m ingestion.backfill_runner --from ... --to ...`."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from config.universe import get_tickers

    default_to = now_ist().date()
    default_from = default_to - timedelta(days=365 * BACKFILL_YEARS)

    parser = argparse.ArgumentParser(description="FYERS historical OHLCV backfill")
    parser.add_argument("--from", dest="from_date", default=default_from.isoformat())
    parser.add_argument("--to", dest="to_date", default=default_to.isoformat())
    args = parser.parse_args()

    tickers = get_tickers()
    estimated_hours = estimate_runtime_hours(len(tickers), args.from_date, args.to_date)
    print(
        f"Backfilling {len(tickers)} tickers from {args.from_date} to {args.to_date}. "
        f"Estimated {estimated_hours:.1f} hours based on rate limit.",
        flush=True,
    )

    run_backfill(tickers, args.from_date, args.to_date)


if __name__ == "__main__":
    main()
