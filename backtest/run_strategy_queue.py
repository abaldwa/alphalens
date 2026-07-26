"""
backtest/run_strategy_queue.py

Owner: Platform / Backtest
Consumers: operator CLI (`python3 -m backtest.run_strategy_queue`),
datastore/api/routers/backtest_runs.py (trigger endpoint, background)

Schedules multiple strategies to be backtested (and optionally the
MetaLabeler retrained) SEQUENTIALLY, in one invocation — user request:
"I do not want to do a manual backtesting and retraining of the modules."
Point this at a queue of strategy definitions once; it runs every job
one after another and hands back one consolidated report, instead of an
operator invoking run_orchestrator_backtest.py / run_iterative_backtest.py
by hand, one at a time, and stitching the results together themselves.

Design
------
Same subprocess-isolation + sequential-execution + memory-gate pattern
as run_batch_backtest.py (backtest/batch_common.py — shared, not
duplicated): each job is `python -m backtest.run_orchestrator_backtest`
or `python -m backtest.run_iterative_backtest` as its own subprocess,
never run concurrently, gated on free system memory before each launch.

Unlike run_batch_backtest.py's fixed light-to-heavy ordering (which only
has to sequence 3 KNOWN phase scripts), a strategy queue is arbitrary
operator-defined content — there's no static memory ranking to apply, so
jobs run in the order given. Put anything you know is heavier later in
the queue yourself.

Queue definition
-----------------
A JSON file: {"jobs": [ {...}, {...}, ... ]}. Each job is either:

    {"kind": "orchestrator", "channel": "technical", "template_name": "E2",
     "top_n": 10, "start_date": "2023-01-01", "end_date": "2026-07-01",
     ...any other run_orchestrator_backtest.py flag, "-"-free key names...}

    {"kind": "iterative_retrain", "horizon_days": 5, "folds": 4, ...}

`strategy_id` / `horizon_bucket` are OPTIONAL per job — omitted, each
orchestrator job gets the same codified strategy_id + Explainer-default
horizon_bucket run_orchestrator_backtest.py itself would default to
(backtest/strategy_id.py) — the queue does not re-implement that
defaulting, it just doesn't force the caller to specify it.
"""

import argparse
import json
import logging
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from backtest.batch_common import wait_for_headroom

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPORTS_DIR = Path(__file__).resolve().parent / "reports"

_ORCHESTRATOR_FLAGS = {
    "channel", "strategy_id", "horizon_bucket", "start_date", "end_date", "capital_mode", "initial_capital",
    "sip_amount", "universe_spec", "max_tickers", "min_history_days", "template_name", "preset", "top_n",
    "lookback_months", "exit_variant", "regime_method",
}
_ITERATIVE_RETRAIN_FLAGS = {
    "horizon_days", "seed", "max_real_tickers", "min_history_days", "max_iterations", "plateau_patience",
    "min_dsr_threshold", "max_random_feature_accuracy", "folds",
}
_KIND_MODULE = {
    "orchestrator": "backtest.run_orchestrator_backtest",
    "iterative_retrain": "backtest.run_iterative_backtest",
}
_KIND_ALLOWED_FLAGS = {
    "orchestrator": _ORCHESTRATOR_FLAGS,
    "iterative_retrain": _ITERATIVE_RETRAIN_FLAGS,
}


def _job_to_cmd(job: Dict[str, Any], job_index: int, report_suffix: str) -> List[str]:
    kind = job.get("kind")
    if kind not in _KIND_MODULE:
        raise ValueError(f"job[{job_index}]: unknown kind {kind!r} — must be one of {list(_KIND_MODULE)}")
    allowed = _KIND_ALLOWED_FLAGS[kind]
    unknown = set(job) - allowed - {"kind"}
    if unknown:
        raise ValueError(f"job[{job_index}] (kind={kind}): unknown field(s) {sorted(unknown)} — allowed: {sorted(allowed)}")

    cmd = [sys.executable, "-m", _KIND_MODULE[kind]]
    for key, value in job.items():
        if key == "kind" or value is None:
            continue
        cmd += [f"--{key.replace('_', '-')}", str(value)]
    cmd += ["--report-suffix", f"{report_suffix}_job{job_index}"]
    return cmd


def _run_job(job: Dict[str, Any], job_index: int, report_suffix: str) -> Dict[str, Any]:
    cmd = _job_to_cmd(job, job_index, report_suffix)
    logger.info(f"run_strategy_queue: launching job[{job_index}] (kind={job.get('kind')}) — {' '.join(cmd)}")
    started = time.monotonic()
    # Isolated subprocess per job — same OOM rationale as run_batch_backtest.py:
    # whatever an orchestrator/retrain run allocated is returned to the OS the
    # instant this subprocess exits, not left to Python's own allocator.
    proc = subprocess.run(cmd, capture_output=False)
    elapsed_s = time.monotonic() - started
    logger.info(f"run_strategy_queue: job[{job_index}] exited {proc.returncode} in {elapsed_s:.0f}s")
    return {"job_index": job_index, "kind": job.get("kind"), "job": job, "returncode": proc.returncode, "elapsed_s": elapsed_s}


def _job_label(job: Dict[str, Any]) -> str:
    """Human-readable label for the Queued/In Progress/Completed board —
    mirrors what OrchestratorTriggerPanel derives client-side for a
    UI-triggered job, so a queue job reads the same way regardless of how
    it was launched."""
    if job.get("kind") == "iterative_retrain":
        return "Iterative Retrain (MetaLabeler)"
    channel = job.get("channel", "")
    descriptor = job.get("template_name") or job.get("preset")
    if not descriptor and channel == "momentum":
        descriptor = f"top{job.get('top_n', '?')}_{job.get('lookback_months', '?')}m"
    label = f"{channel} · {descriptor}" if descriptor else (channel or "job")
    exit_variant = job.get("exit_variant")
    if exit_variant:
        label += f" [{exit_variant}]"
    regime_method = job.get("regime_method")
    if regime_method:
        label += f" ({regime_method})"
    return label


def _write_progress(path: Path, jobs: List[Dict[str, Any]], statuses: List[str]) -> None:
    """Per-job Queued/Running/Completed/Failed/Skipped snapshot, rewritten
    after every state change — lets the API's /queue/status/{queue_id}
    (and the Backtest page's status board) show which specific strategies
    are still queued vs. actively running vs. done, instead of only
    knowing "the whole queue is running somewhere." """
    payload = {
        "generated_at": datetime.now().isoformat(),
        "jobs": [
            {"job_index": i, "kind": j.get("kind"), "label": _job_label(j), "status": s}
            for i, (j, s) in enumerate(zip(jobs, statuses))
        ],
    }
    path.write_text(json.dumps(payload, indent=2, default=str))


# Statuses that mean "don't run this job again on resume" — "completed" is
# a real prior success; "excluded" is an operator manually pulling one job
# out of the run (e.g. it's spiking memory and needs investigation) without
# discarding the rest of the queue. Deliberately distinct from "skipped"
# (which _write_progress/run_queue already use internally to mean "the
# queue stopped on an earlier failure before reaching this job" — that one
# SHOULD be retried on resume, "excluded" should not).
_RESUME_SKIP_STATUSES = {"completed", "excluded"}


def _load_prior_resolved(progress_path: Path, n_jobs: int) -> Dict[int, str]:
    """Best-effort read of a progress file from an earlier (crashed/killed)
    invocation with the same report_suffix — used to skip jobs that already
    reached 'completed' (or were manually 'excluded') rather than re-running
    the whole queue from job 0 after a reboot, OOM kill, or other unclean
    process death. Returns {job_index: prior_status}, preserving the actual
    status string (not collapsing 'excluded' into 'completed') so a
    re-written progress file doesn't misreport why a job was skipped."""
    resolved: Dict[int, str] = {}
    if not progress_path.exists():
        return resolved
    try:
        prior = json.loads(progress_path.read_text())
    except (json.JSONDecodeError, OSError):
        return resolved
    for entry in prior.get("jobs", []):
        i = entry.get("job_index")
        status = entry.get("status")
        if isinstance(i, int) and 0 <= i < n_jobs and status in _RESUME_SKIP_STATUSES:
            resolved[i] = status
    return resolved


def run_queue(
    jobs: List[Dict[str, Any]], min_free_mb: float = 3072.0, wait_timeout_s: float = 600.0,
    stop_on_failure: bool = True, report_suffix: Optional[str] = None, resume: bool = True,
) -> Dict[str, Any]:
    if not jobs:
        raise ValueError("jobs is empty — nothing to schedule")

    run_started = time.monotonic()
    suffix = report_suffix or datetime.now().strftime("%Y%m%d_%H%M%S")
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    progress_path = REPORTS_DIR / f"strategy_queue_progress_{suffix}.json"

    prior_resolved = _load_prior_resolved(progress_path, len(jobs)) if resume else {}
    statuses = [prior_resolved.get(i, "queued") for i in range(len(jobs))]
    if prior_resolved:
        n_completed = sum(1 for s in prior_resolved.values() if s == "completed")
        n_excluded = sum(1 for s in prior_resolved.values() if s == "excluded")
        logger.info(
            f"run_strategy_queue: resuming {suffix} — skipping {n_completed} already-completed "
            f"and {n_excluded} manually-excluded job(s) of {len(jobs)}"
        )
    _write_progress(progress_path, jobs, statuses)

    results = []
    for i, job in enumerate(jobs):
        if i in prior_resolved:
            continue
        wait_for_headroom(min_free_mb, wait_timeout_s, label="run_strategy_queue")
        statuses[i] = "running"
        _write_progress(progress_path, jobs, statuses)
        result = _run_job(job, i, suffix)
        statuses[i] = "completed" if result["returncode"] == 0 else "failed"
        _write_progress(progress_path, jobs, statuses)
        results.append(result)
        if result["returncode"] != 0:
            logger.error(f"run_strategy_queue: job[{i}] failed (exit {result['returncode']})")
            if stop_on_failure:
                logger.error("run_strategy_queue: stopping the remainder of the queue")
                for j in range(i + 1, len(jobs)):
                    statuses[j] = "skipped"
                _write_progress(progress_path, jobs, statuses)
                break

    n_skipped_prior = len(prior_resolved)
    summary = {
        "generated_at": datetime.now().isoformat(),
        "total_jobs": len(jobs),
        "jobs_run": len(results),
        "jobs_skipped_already_completed": n_skipped_prior,
        "results": results,
        "all_passed": (
            all(r["returncode"] == 0 for r in results) and len(results) + n_skipped_prior == len(jobs)
        ),
        "runtime_seconds": time.monotonic() - run_started,
    }
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    summary_path = REPORTS_DIR / f"strategy_queue_{suffix}.json"
    with open(summary_path, "w") as fh:
        json.dump(summary, fh, indent=2, default=str)
    logger.info(f"run_strategy_queue: summary written to {summary_path}")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Schedule multiple strategies (backtests, and optionally an iterative MetaLabeler retrain) "
            "to run sequentially from one queue definition — no manual one-at-a-time triggering."
        )
    )
    parser.add_argument("--queue-file", required=True, help="Path to a JSON file: {\"jobs\": [ {...}, ... ]}")
    parser.add_argument("--min-free-mb", type=float, default=3072.0)
    parser.add_argument("--wait-timeout-s", type=float, default=600.0)
    parser.add_argument(
        "--continue-on-failure", action="store_true",
        help="Keep running later jobs even after one fails (default: stop the queue on the first failure)",
    )
    parser.add_argument("--report-suffix", default=None)
    parser.add_argument(
        "--no-resume", action="store_true",
        help=(
            "Ignore any existing progress file for this --report-suffix and re-run every job from "
            "scratch (default: skip jobs a prior run with the same suffix already marked 'completed' "
            "— safe to re-invoke after a crash, OOM kill, or reboot)"
        ),
    )
    args = parser.parse_args()

    with open(args.queue_file) as fh:
        queue_def = json.load(fh)
    jobs = queue_def["jobs"]

    summary = run_queue(
        jobs=jobs, min_free_mb=args.min_free_mb, wait_timeout_s=args.wait_timeout_s,
        stop_on_failure=not args.continue_on_failure, report_suffix=args.report_suffix,
        resume=not args.no_resume,
    )
    print(json.dumps(summary, indent=2, default=str))
    sys.exit(0 if summary["all_passed"] else 1)


if __name__ == "__main__":
    main()
