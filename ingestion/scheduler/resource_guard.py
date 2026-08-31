"""
ingestion/scheduler/resource_guard.py

Phase: Pipeline & Monitoring Remediation, Phase 2
Owner: Platform / Scheduler
Consumers: ingestion/scrapers/screener.py (batch_export chunk flush),
    any future chunked/batched writer, datastore/api/routers/ops.py
    (live resource panel)

Self-healing memory pressure guard: rather than letting a long-running
chunked job (screener fundamentals export, feature-matrix backfills)
plow ahead at a fixed chunk size until the OS OOM-kills it, callers check
current process RSS against config.settings.PIPELINE_MEMORY_CEILING_MB
before each chunk and shrink the next chunk size if pressure is high.
This trades throughput for survival — a slower run that finishes beats a
fast one that gets killed mid-write.

Uses psutil when available (accurate, cross-platform) and falls back to
parsing /proc/self/status directly otherwise (same rationale
scripts/monitor_scheduler_resources.py's docstring gives for avoiding a
hard psutil dependency, kept here as a safety net rather than a
philosophy — Phase 4 of this remediation formally adopts psutil).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

try:
    import psutil

    _HAS_PSUTIL = True
except ImportError:  # pragma: no cover - exercised only in psutil-less envs
    _HAS_PSUTIL = False


def current_rss_mb() -> float:
    """
    Return this process's current resident set size in MB.

    Returns
    -------
    float
        RSS in MB. Falls back to /proc/self/status's VmRSS line if
        psutil isn't installed; returns 0.0 (never raises) if neither
        source is available (e.g. non-Linux without psutil), so a guard
        failure never blocks the caller's actual work.

    Raises
    ------
    None
    """
    if _HAS_PSUTIL:
        try:
            return float(psutil.Process().memory_info().rss / (1024 * 1024))
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning(f"resource_guard: psutil RSS read failed ({exc}), falling back to /proc")

    try:
        status_path = Path("/proc/self/status")
        for line in status_path.read_text().splitlines():
            if line.startswith("VmRSS:"):
                kb = int(line.split()[1])
                return kb / 1024.0
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning(f"resource_guard: /proc/self/status RSS read failed ({exc})")

    return 0.0


def memory_pressure_high(ceiling_mb: Optional[float] = None, high_water_fraction: float = 0.8) -> bool:
    """
    Return whether current RSS exceeds `high_water_fraction` of the ceiling.

    Parameters
    ----------
    ceiling_mb : float, optional
        Defaults to config.settings.PIPELINE_MEMORY_CEILING_MB.
    high_water_fraction : float
        Fraction of the ceiling that counts as "high pressure" (default
        0.8 — trigger before actually hitting the ceiling, so there's
        headroom left to finish the in-flight chunk).

    Returns
    -------
    bool

    Raises
    ------
    None
    """
    if ceiling_mb is None:
        from config.settings import PIPELINE_MEMORY_CEILING_MB

        ceiling_mb = PIPELINE_MEMORY_CEILING_MB

    return current_rss_mb() >= ceiling_mb * high_water_fraction


def adaptive_chunk_size(
    configured_size: int,
    floor: int = 5,
    ceiling_mb: Optional[float] = None,
) -> int:
    """
    Return the chunk size to use for the NEXT chunk, shrinking under
    memory pressure.

    Parameters
    ----------
    configured_size : int
        The caller's normal/default chunk size (e.g.
        config.settings.SCREENER_BATCH_EXPORT_CHUNK_SIZE).
    floor : int
        Never shrink below this — a chunk size of 0 would stop all
        progress rather than merely slow it down.
    ceiling_mb : float, optional
        Forwarded to memory_pressure_high.

    Returns
    -------
    int
        `configured_size` under normal conditions; half of it (down to
        `floor`) when current RSS is at or above the high-water mark.

    Raises
    ------
    None
    """
    if configured_size <= floor:
        return configured_size

    if memory_pressure_high(ceiling_mb=ceiling_mb):
        shrunk = max(floor, configured_size // 2)
        logger.warning(
            f"resource_guard: memory pressure high (RSS={current_rss_mb():.0f}MB) — "
            f"shrinking next chunk size {configured_size} -> {shrunk}"
        )
        return shrunk

    return configured_size


def poll_process_resources(pid: int) -> dict[str, Any]:
    """
    Read a single live snapshot of an arbitrary PID's RSS/CPU via psutil.

    A48: unlike current_rss_mb() (this process only) or
    monitor_scheduler_resources.py's 30-min timer-driven log, this is a
    direct, uncached psutil.Process(pid) read intended to be called
    on-demand by datastore/api/routers/ops.py's GET /live-resources
    endpoint, which the Ops dashboard polls every 10-30s while a pipeline
    run is active — giving near-real-time visibility instead of waiting
    for the next 30-min monitor tick.

    Parameters
    ----------
    pid : int
        Process ID to poll (e.g. alphalens-scheduler.service's MainPID).

    Returns
    -------
    dict
        {"rss_mb": float, "cpu_percent": float} on success, or
        {"error": str} if the PID doesn't exist, has exited, or psutil
        isn't installed. Never raises.

    Raises
    ------
    None
    """
    if not _HAS_PSUTIL:
        return {"error": "psutil not installed"}
    try:
        proc = psutil.Process(pid)
        # cpu_percent() needs a non-zero interval on the first call per
        # process handle to report anything other than 0.0 — a short
        # blocking interval is acceptable here since this runs inside a
        # single on-demand API request, not a hot loop.
        cpu = proc.cpu_percent(interval=0.1)
        rss_mb = proc.memory_info().rss / (1024 * 1024)
        return {"rss_mb": rss_mb, "cpu_percent": cpu}
    except psutil.NoSuchProcess:
        return {"error": f"pid {pid} not found (process exited)"}
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning(f"resource_guard: poll_process_resources({pid}) failed: {exc}")
        return {"error": str(exc)}
