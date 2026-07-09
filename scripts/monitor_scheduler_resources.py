"""
Resource monitor for the alphalens-scheduler systemd --user service.

Phase: Ops/Infra
Specs: SPEC-SCHED-009 (laptop-only scheduler), SPEC-SCHED-010
Owner: Ops
Consumers: alphalens-scheduler-monitor.timer (runs this every 30 min)

Reads current system memory pressure from /proc/meminfo (no psutil dependency
in the venv), logs a snapshot, and — if available memory is low — writes an
env override file dialing HMM_FEATURE_WORKERS / FEATURE_CACHE_PRELOAD_WORKERS
down for the next scheduler run, then restarts the systemd service so the
new values take effect (config/settings.py reads these via os.environ.get,
2026-07 change). Escalates back up once memory pressure clears so the
pipeline doesn't stay throttled forever off one bad reading.

Never restarts the service while any pipeline_checkpoints row for today is
status='running' (checkpoint.py's STEPS, in particular the non-backfillable
run_models/write_signals/sanity_check/paper_trade training-and-inference
chain — see checkpoint.STEPS) — a restart mid-step is a SIGTERM to the
in-progress step, and although get_resume_step() treats 'running' as
non-terminal and retries it on the next start (so it is never silently
marked 'skipped'), repeatedly killing a slow step every 30 minutes could
mean it never gets a long enough uninterrupted window to finish. User
directive (2026-07-05): training must never be skipped the way
run_models/write_signals silently were 2026-06-23 to 2026-07-02 (AF-2) —
so when a step is genuinely in flight, this monitor logs a WARNING and
defers the restart (and the env-file write) to the next 30-min tick rather
than touching the running process, even under CRITICAL_MEM_PCT pressure.
The systemd MemoryMax/MemoryHigh cgroup limits (alphalens-scheduler.service)
remain the only backstop against a true OOM in that window.

PIT Assumptions: none (infra script, no market data).
Raises: nothing — best-effort monitoring, must never crash the timer unit.
"""
import logging
import sqlite3
import subprocess
from pathlib import Path

LOG_PATH = Path("/home/amit/projects/AlphaLens/datastore/logs/scheduler_resource_monitor.log")
ENV_OVERRIDE_PATH = Path.home() / ".config/alphalens/scheduler.env"
PIPELINE_LOG_DB_PATH = Path("/home/amit/projects/AlphaLens/datastore/normalised/pipeline_log.db")

# Available-memory thresholds (percent of total) that trigger throttling.
LOW_MEM_PCT = 20
CRITICAL_MEM_PCT = 10

DEFAULT_HMM_WORKERS = 3
DEFAULT_PRELOAD_WORKERS = 16

logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s %(message)s",
)
log = logging.getLogger("scheduler_resource_monitor")


def read_meminfo() -> dict:
    info = {}
    with open("/proc/meminfo") as f:
        for line in f:
            key, val = line.split(":", 1)
            info[key] = int(val.strip().split()[0])  # kB
    return info


def current_worker_settings() -> tuple[int, int]:
    if not ENV_OVERRIDE_PATH.exists():
        return DEFAULT_HMM_WORKERS, DEFAULT_PRELOAD_WORKERS
    values = {}
    for line in ENV_OVERRIDE_PATH.read_text().splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            values[k.strip()] = int(v.strip())
    return (
        values.get("HMM_FEATURE_WORKERS", DEFAULT_HMM_WORKERS),
        values.get("FEATURE_CACHE_PRELOAD_WORKERS", DEFAULT_PRELOAD_WORKERS),
    )


def write_worker_settings(hmm_workers: int, preload_workers: int) -> None:
    ENV_OVERRIDE_PATH.parent.mkdir(parents=True, exist_ok=True)
    ENV_OVERRIDE_PATH.write_text(
        f"HMM_FEATURE_WORKERS={hmm_workers}\n"
        f"FEATURE_CACHE_PRELOAD_WORKERS={preload_workers}\n"
    )


def step_in_progress() -> str | None:
    """Return the step_name of any pipeline_checkpoints row still 'running'.

    Checked against every row regardless of date, not just today — a
    backfill run can be mid-step for a past date at the same time a live
    run is mid-step for today.
    """
    if not PIPELINE_LOG_DB_PATH.exists():
        return None
    try:
        with sqlite3.connect(str(PIPELINE_LOG_DB_PATH)) as conn:
            row = conn.execute(
                "SELECT step_name FROM pipeline_checkpoints "
                "WHERE status = 'running' LIMIT 1"
            ).fetchone()
        return row[0] if row else None
    except sqlite3.Error as e:
        log.warning("Could not read pipeline_checkpoints (%s) — assuming no step in progress", e)
        return None


def main() -> None:
    info = read_meminfo()
    total_kb = info["MemTotal"]
    avail_kb = info["MemAvailable"]
    avail_pct = 100 * avail_kb / total_kb
    load1, load5, load15 = __import__("os").getloadavg()

    hmm_workers, preload_workers = current_worker_settings()

    log.info(
        "mem_available=%.1f%% (%.0fMB/%.0fMB) load1=%.2f load5=%.2f "
        "hmm_workers=%d preload_workers=%d",
        avail_pct, avail_kb / 1024, total_kb / 1024, load1, load5,
        hmm_workers, preload_workers,
    )

    new_hmm, new_preload = hmm_workers, preload_workers
    if avail_pct < CRITICAL_MEM_PCT:
        new_hmm, new_preload = 1, 4
    elif avail_pct < LOW_MEM_PCT:
        new_hmm, new_preload = 2, 8
    elif avail_pct > 40:
        # Pressure has cleared — restore defaults so we don't stay throttled.
        new_hmm, new_preload = DEFAULT_HMM_WORKERS, DEFAULT_PRELOAD_WORKERS

    if (new_hmm, new_preload) == (hmm_workers, preload_workers):
        return

    running_step = step_in_progress()
    if running_step is not None:
        log.warning(
            "mem_available=%.1f%% wants hmm %d->%d, preload %d->%d, but "
            "step '%s' is in progress — deferring restart to next tick so "
            "training/inference is never interrupted or skipped",
            avail_pct, hmm_workers, new_hmm, preload_workers, new_preload,
            running_step,
        )
        return

    log.warning(
        "Adjusting worker counts: hmm %d->%d, preload %d->%d (mem_available=%.1f%%)",
        hmm_workers, new_hmm, preload_workers, new_preload, avail_pct,
    )
    write_worker_settings(new_hmm, new_preload)
    subprocess.run(
        ["systemctl", "--user", "restart", "alphalens-scheduler.service"],
        check=False,
    )


if __name__ == "__main__":
    main()
