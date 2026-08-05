"""
backtest/core/ohlcv_prewarm.py

Phase: 3.x (Batch/Queue performance — Technical channel OHLCV reuse)
Owner: Platform / Backtest
Consumers: backtest/run_strategy_queue.py, backtest/run_orchestrator_backtest.py

Closes FeatureBacklog A73's remaining gap: run_orchestrator_backtest.py's
_fetch_real_ohlcv() already does ONE bulk GET /ohlcv/_bulk call per run
(2026-07-26 fix, replacing a per-ticker loop) — but a technical batch sweep
launches each job as its own subprocess (backtest/run_strategy_queue.py,
deliberate OOM-safety isolation, see batch_common.py), so a 42-template
sweep still made 42+ independent bulk calls for the exact same
[start_date, end_date] window. GET /ohlcv/_bulk takes no universe filter
(datastore/client.py::get_ohlcv_bulk) — it always returns the full
universe for the date range — so the fetch is a pure function of
(start_date, end_date) alone, making it safe to cache and share verbatim
across every job in a queue run, regardless of that job's own
max_tickers/universe_spec (each job still applies its own client-side
ticker filter after reading the cached DataFrame, unchanged).

Design mirrors backtest/core/screener_cache.py's already-reviewed pattern:
  - Snapshot written to a single Parquet file + a sibling manifest JSON
    (row/ticker counts, generated_at) so a reader can tell "cached" from
    "not yet cached" without a partial/corrupt read masquerading as a hit.
  - A miss NEVER silently resolves to empty data — get_or_fetch_ohlcv_bulk
    always falls through to a live DataStoreClient.get_ohlcv_bulk() call on
    any miss (missing file, missing/corrupt manifest), which then populates
    the cache for the next reader. Same "population happens lazily, from
    the same live call path every job already uses" rationale as
    screener_cache.py — no separate precompute script with its own idea of
    which range to cover.
  - Keyed only by (start_date, end_date) — never by universe_spec/
    max_tickers — matching get_ohlcv_bulk's actual dependency exactly.
"""

import json
import logging
from datetime import date as date_type, datetime
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_OHLCV_SNAPSHOT_DIR = Path(__file__).resolve().parent.parent / "cache" / "ohlcv_snapshots"


def _snapshot_paths(start_date: date_type, end_date: date_type, snapshot_dir: Path) -> tuple:
    stem = f"{start_date.isoformat()}_{end_date.isoformat()}"
    return snapshot_dir / f"{stem}.parquet", snapshot_dir / f"{stem}.manifest.json"


def read_snapshot(start_date: date_type, end_date: date_type, snapshot_dir: Path) -> Optional[pd.DataFrame]:
    """None means "not cached" (or unreadable — treated as a miss, never
    raised, matching screener_cache.py's read-through contract) — callers
    must fall through to a live fetch, never treat None as "empty result."""
    parquet_path, manifest_path = _snapshot_paths(start_date, end_date, snapshot_dir)
    if not parquet_path.exists() or not manifest_path.exists():
        return None
    try:
        json.loads(manifest_path.read_text())  # presence + well-formedness check only
        return pd.read_parquet(parquet_path)
    except (OSError, ValueError, json.JSONDecodeError):
        logger.warning(f"ohlcv_prewarm: snapshot at {parquet_path} unreadable — treating as a cache miss", exc_info=True)
        return None


def write_snapshot(df: pd.DataFrame, start_date: date_type, end_date: date_type, snapshot_dir: Path) -> None:
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    parquet_path, manifest_path = _snapshot_paths(start_date, end_date, snapshot_dir)
    tmp_parquet = parquet_path.with_suffix(".parquet.tmp")
    df.to_parquet(tmp_parquet, index=False)
    tmp_parquet.replace(parquet_path)
    manifest = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "rows": len(df),
        "tickers": int(df["ticker"].nunique()) if "ticker" in df.columns else None,
        "generated_at": datetime.now().isoformat(),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2))


def get_or_fetch_ohlcv_bulk(client, start_date: date_type, end_date: date_type, snapshot_dir: Path) -> pd.DataFrame:
    """The one entry point _fetch_real_ohlcv() uses instead of calling
    client.get_ohlcv_bulk() directly when a snapshot dir is configured.
    Same return shape as get_ohlcv_bulk() (raw bulk frame, pre client-side
    ticker/history filtering) — callers that already filter downstream are
    unaffected by switching to this function."""
    cached = read_snapshot(start_date, end_date, snapshot_dir)
    if cached is not None:
        logger.info(f"ohlcv_prewarm: snapshot hit for [{start_date}, {end_date}] ({len(cached)} rows) — skipping live bulk fetch")
        return cached
    from_dt = pd.Timestamp(start_date)
    to_dt = pd.Timestamp(end_date)
    bulk = client.get_ohlcv_bulk(from_dt, to_dt)
    write_snapshot(bulk, start_date, end_date, snapshot_dir)
    logger.info(f"ohlcv_prewarm: populated snapshot for [{start_date}, {end_date}] ({len(bulk)} rows)")
    return bulk


def prewarm_ohlcv_snapshot(start_date: date_type, end_date: date_type, snapshot_dir: Optional[Path] = None) -> Path:
    """Called ONCE by a batch/queue driver before launching per-job
    subprocesses (never by an individual job itself) — populates the
    shared snapshot up front so every subsequent job's read is a cache hit
    instead of a race to populate it independently. Idempotent: a second
    call for the same (start_date, end_date) is a cache hit and does no
    network work."""
    from datastore.client import DataStoreClient

    snapshot_dir = snapshot_dir or DEFAULT_OHLCV_SNAPSHOT_DIR
    client = DataStoreClient()
    get_or_fetch_ohlcv_bulk(client, start_date, end_date, snapshot_dir)
    return snapshot_dir
