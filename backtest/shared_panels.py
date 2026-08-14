"""
backtest/shared_panels.py

Phase: Backtest sweep performance (A87 Stage 1)
Owner: Platform / Backtest
Consumers: backtest/run_orchestrator_backtest.py, backtest/run_sweep_inprocess.py

A process-lifetime memo for the artifacts every strategy in a sweep rebuilds
identically.

WHY THIS EXISTS
---------------
Measured 2026-08-14 on the 186-job sweep. Phase instrumentation inside
BacktestOrchestrator.run() accounts for a small minority of wall clock:

    technical   247.8s total, 17.7s measured, 138.6s unattributed
    momentum     82.8s total,  3.8s measured,  10.3s unattributed

Profiling the unattributed region directly:

    _fetch_real_ohlcv                            39.5s
    _build_config (ticker_dates + PIT ADTV)      12.7s
    pivot close panel                             0.7s
    pivot volume panel                            0.7s

None of it varies with the strategy. It is a pure function of (window,
universe size, snapshot) -- and it was being recomputed in each of 186
subprocesses, against a 4,275-day simulation that costs about 11 seconds.
That is roughly seven hours of duplicated setup around 35 minutes of work.

WHAT THIS IS NOT
----------------
Not a correctness mechanism, and deliberately not a persistent cache. It is
an in-process memo: run many strategies in ONE process and they share the
panels; run one strategy and nothing changes. Subprocess isolation still
works exactly as before, which matters because the parallel queue relies on
it.

The key includes every input that can change the DATA -- window, universe
size, minimum history, snapshot directory, and for the config artifacts the
ADTV top-N and circuit-fill flag. Two strategies whose data differs in any of
those get different entries rather than silently sharing one. Getting that
wrong would be a correctness bug of the worst kind (a strategy simulated
against another strategy's universe), so the key errs toward being too
specific: a spurious miss costs 40 seconds, a spurious hit costs a wrong
backtest.

MEMORY
------
One OHLCV frame is ~7.1M rows. Holding it once and sharing it across
strategies uses strictly LESS memory than two subprocesses each holding their
own copy, which is what the queue does today. But the cache is unbounded by
design (a sweep uses one window), so a caller that walks many windows in one
process must call clear() between them -- see the max-entries guard below,
which refuses to grow without the caller saying so.
"""

from __future__ import annotations

import logging
from datetime import date as date_type
from typing import Any, Dict, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

#: More distinct windows than this in one process is a caller mistake, not a
#: workload -- a sweep shares one window. Raising it is a deliberate act.
MAX_CACHED_WINDOWS = 4

_ohlcv_cache: Dict[Tuple, pd.DataFrame] = {}
_artifact_cache: Dict[Tuple, Dict[str, Any]] = {}

_stats = {"ohlcv_hits": 0, "ohlcv_misses": 0, "artifact_hits": 0, "artifact_misses": 0}


def ohlcv_key(
    max_tickers: Optional[int], min_history_days: int,
    start_date: date_type, end_date: date_type, ohlcv_snapshot_dir: Optional[str],
) -> Tuple:
    """Everything that can change which rows come back. `sector_map` is
    deliberately absent -- it is derived downstream and does not affect the
    OHLCV itself."""
    return (max_tickers, min_history_days, str(start_date), str(end_date), ohlcv_snapshot_dir)


def get_ohlcv(key: Tuple, build) -> pd.DataFrame:
    """Memoised OHLCV. `build` is called only on a miss.

    Returns the SAME object on every hit, not a copy: copying a 7.1M-row
    frame per strategy would give back much of what the cache saves. Callers
    must therefore treat it as read-only. Every caller today does (the
    orchestrator pivots and groups it, never mutates), and a caller that
    needs to mutate should copy explicitly at its own call site where the
    cost is visible.
    """
    if key in _ohlcv_cache:
        _stats["ohlcv_hits"] += 1
        return _ohlcv_cache[key]
    if len(_ohlcv_cache) >= MAX_CACHED_WINDOWS:
        raise RuntimeError(
            f"shared_panels: refusing to cache a {len(_ohlcv_cache) + 1}th distinct OHLCV window "
            f"in one process (limit {MAX_CACHED_WINDOWS}). A sweep shares one window; walking many "
            f"windows in-process needs an explicit clear() between them, or this cache will hold "
            f"every one of them in memory at once."
        )
    _stats["ohlcv_misses"] += 1
    frame = build()
    _ohlcv_cache[key] = frame
    logger.info(
        "shared_panels: cached OHLCV %s (%s rows); hits=%d misses=%d",
        key, f"{len(frame):,}", _stats["ohlcv_hits"], _stats["ohlcv_misses"],
    )
    return frame


def artifact_key(ohlcv_cache_key: Tuple, top_n_by_adtv: Optional[int], block_circuit_fills: bool) -> Tuple:
    """The derived artifacts depend on the OHLCV *and* on the two options
    that change what is derived from it."""
    return (ohlcv_cache_key, top_n_by_adtv, bool(block_circuit_fills))


def get_artifacts(key: Tuple, build) -> Dict[str, Any]:
    """Memoised {ticker_dates, adtv_panel, locked_bars, price_panel,
    volume_panel} -- the per-run derivations of one OHLCV frame."""
    if key in _artifact_cache:
        _stats["artifact_hits"] += 1
        return _artifact_cache[key]
    _stats["artifact_misses"] += 1
    built = build()
    _artifact_cache[key] = built
    return built


def stats() -> Dict[str, int]:
    return dict(_stats)


def clear() -> None:
    """Drop everything. Call between windows, or to release memory."""
    _ohlcv_cache.clear()
    _artifact_cache.clear()
    logger.info("shared_panels: cleared")
