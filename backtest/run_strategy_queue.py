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
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date as date_type, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from backtest.batch_common import wait_for_headroom

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPORTS_DIR = Path(__file__).resolve().parent / "reports"

# "min_dsr_threshold" is queue-only bookkeeping — run_orchestrator_backtest.py
# has no such CLI flag (only run_iterative_backtest.py computes/gates its own
# DSR internally), so it's accepted here as an orchestrator-job field but
# stripped before building the subprocess command (see _QUEUE_ONLY_ORCHESTRATOR_FIELDS
# below) and instead consumed by run_queue() itself after _compute_and_write_dsr
# runs, to gate the job's status. Default None (unset) = no gate, matching the
# pre-existing behavior for every caller that doesn't pass it.
_QUEUE_ONLY_ORCHESTRATOR_FIELDS = {"min_dsr_threshold"}
_ORCHESTRATOR_FLAGS = {
    "channel", "strategy_id", "horizon_bucket", "start_date", "end_date", "capital_mode", "initial_capital",
    "sip_amount", "universe_spec", "max_tickers", "min_history_days", "template_name", "preset", "top_n",
    "lookback_months", "rank_method", "skip_months", "strategy_family", "exit_variant", "regime_method",
    # [2026-08-18] grace_cycles and exit_rank are gone: momentum's rotation is
    # a plain list swap, so neither lever exists to sweep.
    # 2026-08-01 Momentum-parity Technical filters (backtest/run_orchestrator_backtest.py
    # --max-hold-days/--min-adtv-cr/etc.) — see that script's argparse block.
    "max_hold_days", "min_adtv_cr", "quality_gate_min_f_score", "quality_gate_max_m_score",
    "downtrend_filter_pct", "circuit_band_pct", "disable_buys_in_regime", "combo_templates",
    # 2026-08-09 PIT bear gate (run_orchestrator_backtest.py --bear-drawdown-pct)
    "bear_drawdown_pct",
    "defer_db_writes", "precomputed_matches_dir", "prefetch_feature_parquets", "ohlcv_snapshot_dir",
    # 2026-08-05 Momentum engine consolidation Phase 2: market-cap rank-band
    # universe selection (features/momentum_universe.py RANK_BANDS ids 1-5).
    "rank_band_id",
    # 2026-08-19: needed to reach the momentum registry's biweekly (10) and
    # bimonthly (42) cadences, which the horizon table cannot express.
    "rebalance_cadence_days",
    # 2026-08-12 capital_mode="annual_reset" (the user's third measure). The
    # LTCG regime must be per-job: it changes the FY withdrawal, hence next
    # year's capital, hence which trades execute, so the two regimes are
    # separate runs and cannot be derived post-hoc from one trade book the way
    # the lump run's regimes are.
    "annual_reset_ltcg_rate", "annual_reset_ltcg_exemption", "annual_reset_regime_label",
    # 2026-08-13: withhold the top-up after a losing FY. Per-job for the same
    # reason as the LTCG regime — a book left smaller sizes smaller positions
    # and can fail can_buy outright, so the two variants take different trades
    # and neither is derivable from the other's trade book.
    #
    # Named to match the CLI flag (--annual-reset-no-top-up), NOT the config
    # field (top_up_after_loss), because the loop below maps field names to
    # flags mechanically and omits False booleans entirely. A field named
    # annual_reset_top_up_after_loss set to false would therefore emit no flag
    # at all and silently run the TOPPED-UP variant — the exact inversion the
    # author was trying to avoid. Spelling it the other way round makes
    # true -> flag emitted -> no top-up, and the allowed-field check rejects
    # the tempting wrong name with an explicit error rather than misreading it.
    "annual_reset_no_top_up",
    # 2026-08-23: regime_type for EMA-RSI and future regime overlays (engine feature)
    "regime_type",
} | _QUEUE_ONLY_ORCHESTRATOR_FIELDS
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
    queue_only = _QUEUE_ONLY_ORCHESTRATOR_FIELDS if kind == "orchestrator" else set()
    for key, value in job.items():
        if key == "kind" or key in queue_only or value is None:
            continue
        flag = f"--{key.replace('_', '-')}"
        if isinstance(value, bool):
            # 2026-08-02 (defer_db_writes): a plain store_true CLI flag
            # takes no value — emitting "--flag True" would be rejected by
            # argparse as an unrecognized positional. False is simply
            # omitted (the flag's absence already means "off").
            if value:
                cmd.append(flag)
        elif isinstance(value, (list, tuple)):
            # 2026-08-14: str(["bear"]) is "['bear']", and the orchestrator
            # then splits it on "," into ["['bear']"] -- a regime name that
            # matches nothing, so disable_buys_in_regime silently became a
            # no-op and the job reported itself as bear-gated while running
            # ungated. The same applied to combo_templates. Both CLI flags
            # take a comma-separated string, so build one.
            cmd += [flag, ",".join(str(v) for v in value)]
        else:
            cmd += [flag, str(value)]
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


def _compute_and_write_dsr(job: Dict[str, Any], job_index: int, suffix: str, n_trials_so_far: int) -> Optional[float]:
    """2026-07-26 (REV6 wiring, model-review-corrected design): called
    right after ONE job is marked 'completed' — computes deflated Sharpe
    using n_trials=n_trials_so_far (the count of jobs completed in THIS
    queue up to and including this one, matching backtest/iterative_
    retrain.py::RetrainLoop's live n_trials_so_far convention) and writes
    it back immediately. Deliberately NOT a batch pass run once after the
    whole queue finishes — reviewers confirmed a full-queue batch pass
    can't function as a gate (nothing can be rejected once every row is
    already published) and risks becoming another "exists but nobody
    calls it" utility if left as a manual/on-demand step. Only orchestrator
    jobs (technical/fundamental/momentum) are handled here — iterative_
    retrain jobs already compute/gate their own DSR internally.

    Failures here are logged, never raised — a bug in this NEW wiring
    must never abort an otherwise-successful queue.

    Returns the computed DSR (float) on success, or None if it couldn't be
    computed (non-orchestrator job, missing report, degenerate/empty run, or
    an unexpected error) — callers that want to gate on min_dsr_threshold
    must treat None as "couldn't verify," not "passed."""
    if job.get("kind") != "orchestrator":
        return None
    try:
        report_path = REPORTS_DIR / f"orchestrator_{suffix}_job{job_index}.json"
        report = json.loads(report_path.read_text())
        run_id = report["run"]["run_id"]
        sharpe = report.get("metrics", {}).get("sharpe")
        if sharpe is None:
            logger.info(f"run_strategy_queue: job[{job_index}] has no sharpe (degenerate/empty run) — skipping DSR")
            return None
        start_date = date_type.fromisoformat(str(report["run"]["start_date"])[:10])
        end_date = date_type.fromisoformat(str(report["run"]["end_date"])[:10])
        # Trading-day observation count approximated from the calendar span
        # (~252/365.25 trading days per calendar day) — the report doesn't
        # carry a literal daily-return count; deflated_sharpe_ratio's own
        # docstring accepts an approximate n_obs (skew/kurtosis correction,
        # which needs the real return series, is the precise part — that's
        # only applied when `returns` is passed, which this approximation
        # deliberately omits rather than fabricate a return series).
        n_obs = max(int((end_date - start_date).days * (252 / 365.25)), 1)

        from backtest.overfit_checks import deflated_sharpe_ratio
        from backtest.core.metrics import TRADING_DAYS_PER_YEAR
        from config.settings import BACKTEST_DUCKDB_PATH, DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS, DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S, DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S
        from backtest.core.run_store import update_dsr
        from datastore.api.db import get_duckdb_connection

        # [BUG FIX, 4th fundamental-strategies review] `sharpe` here comes from
        # backtest/core/metrics.py::sharpe_ratio(), which is ANNUALIZED
        # (returns.mean()/std * sqrt(TRADING_DAYS_PER_YEAR)) — deflated_sharpe_
        # ratio's Bailey/Lopez de Prado formula expects a per-period (daily)
        # Sharpe. Feeding it the annualized value inflates the DSR statistic by
        # ~sqrt(252), saturating it near 1.0 and defeating the gate. The report
        # only carries the scalar annualized Sharpe (no raw daily-return
        # series), but de-annualizing is exact — sharpe_annualized = sharpe_
        # daily * sqrt(TRADING_DAYS_PER_YEAR) by construction — so dividing
        # back out recovers the true per-period value with no approximation.
        raw_sharpe = sharpe / (TRADING_DAYS_PER_YEAR ** 0.5)
        dsr = deflated_sharpe_ratio(sharpe=raw_sharpe, n_trials=n_trials_so_far, n_obs=n_obs)
        with get_duckdb_connection(
            BACKTEST_DUCKDB_PATH, read_only=False, persist=False,
            retry_attempts=DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS,
            retry_base_delay_s=DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S,
            retry_max_delay_s=DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S,
        ) as conn:
            update_dsr(conn, run_id, dsr, n_trials_so_far, post_hoc=False)
        logger.info(f"run_strategy_queue: job[{job_index}] dsr={dsr:.3f} (n_trials={n_trials_so_far}, n_obs={n_obs})")
        return float(dsr)
    except Exception:
        logger.warning(f"run_strategy_queue: DSR computation failed for job[{job_index}] — leaving unset", exc_info=True)
        return None


def _check_integrity_passed(job: Dict[str, Any], job_index: int, suffix: str) -> Optional[bool]:
    """[BUG FIX, 4th fundamental-strategies review, item 3] A job could show
    status='completed'/all_passed=True even when the persisted run's
    integrity_passed is False (a CRITICAL SPEC-BT-001 check failed) — the
    queue only ever read the subprocess returncode plus the opt-in
    min_dsr_threshold, never integrity_passed from the saved report. This
    reads it back from backtest_runs (via the same orchestrator report
    file/run_id lookup _compute_and_write_dsr already uses) so a critical
    integrity failure is always visible, regardless of whether
    min_dsr_threshold was ever set (unlike the DSR gate, this isn't opt-in).

    Returns True/False when determinable, or None (not orchestrator kind,
    missing report, or an unexpected error — logged, never raised) when it
    can't be — callers must treat None as "couldn't verify," not "passed."
    """
    if job.get("kind") != "orchestrator":
        return None
    try:
        report_path = REPORTS_DIR / f"orchestrator_{suffix}_job{job_index}.json"
        report = json.loads(report_path.read_text())
        run_id = report["run"]["run_id"]

        from backtest.core.run_store import get_run
        from config.settings import BACKTEST_DUCKDB_PATH, DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS, DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S, DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S
        from datastore.api.db import get_duckdb_connection

        with get_duckdb_connection(
            BACKTEST_DUCKDB_PATH, read_only=True, persist=False,
            retry_attempts=DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS,
            retry_base_delay_s=DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S,
            retry_max_delay_s=DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S,
        ) as conn:
            run_record = get_run(conn, run_id)
        if run_record is None:
            logger.warning(f"run_strategy_queue: job[{job_index}] run_id={run_id!r} not found in backtest_runs")
            return None
        return bool(run_record.get("integrity_passed"))
    except Exception:
        logger.warning(f"run_strategy_queue: integrity_passed lookup failed for job[{job_index}]", exc_info=True)
        return None


def _maybe_prewarm_ohlcv(jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """2026-08-05 (FeatureBacklog A73 remaining gap): the common technical-
    sweep shape is N orchestrator jobs all covering the exact same
    [start_date, end_date] window (42 templates x up to 9 exit variants,
    same backtest period) — each launched as its own subprocess (deliberate
    OOM-safety isolation, see batch_common.py), so each independently
    issued the same GET /ohlcv/_bulk call. When every orchestrator job in
    this queue shares one (start_date, end_date) and none has already set
    its own ohlcv_snapshot_dir, prewarm the shared snapshot ONCE here (main
    process, before any subprocess launches) and inject it into every such
    job — turning N live bulk fetches into 1. A queue mixing date ranges,
    or where a caller already set ohlcv_snapshot_dir explicitly, is left
    untouched (falls back to each job's own current behavior) — this is a
    pure optimization, never a correctness change."""
    orchestrator_jobs = [j for j in jobs if j.get("kind") == "orchestrator"]
    candidates = [j for j in orchestrator_jobs if not j.get("ohlcv_snapshot_dir")]
    if not candidates:
        return jobs
    date_pairs = {(j.get("start_date"), j.get("end_date")) for j in candidates}
    if len(date_pairs) != 1 or None in next(iter(date_pairs)):
        return jobs
    start_date_str, end_date_str = next(iter(date_pairs))
    try:
        from backtest.core.ohlcv_prewarm import prewarm_ohlcv_snapshot
        start_date = date_type.fromisoformat(str(start_date_str)[:10])
        end_date = date_type.fromisoformat(str(end_date_str)[:10])
        snapshot_dir = prewarm_ohlcv_snapshot(start_date, end_date)
    except Exception:
        logger.warning("run_strategy_queue: OHLCV prewarm failed — jobs fall back to their own live fetch", exc_info=True)
        return jobs
    logger.info(f"run_strategy_queue: OHLCV prewarmed for [{start_date}, {end_date}] at {snapshot_dir} — {len(candidates)} job(s) will reuse it")
    candidate_ids = {id(j) for j in candidates}
    return [
        {**j, "ohlcv_snapshot_dir": str(snapshot_dir)} if id(j) in candidate_ids else j
        for j in jobs
    ]


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


def _process_job_completion(
    job: Dict[str, Any], i: int, suffix: str, result: Dict[str, Any],
    statuses: List[str], progress_path: Path, jobs: List[Dict[str, Any]], lock: threading.Lock,
) -> Dict[str, Any]:
    """Same per-job post-processing the sequential run_queue() loop does
    right after a job's subprocess exits (status update, DSR gate,
    integrity check, progress write) — extracted so _run_queue_concurrent
    can call it under a lock from whichever worker thread's job finished,
    in COMPLETION order rather than launch order. n_trials_so_far (for
    DSR) is a total-completed-count snapshot at the moment this job
    finished, which is what a Deflated Sharpe Ratio correction actually
    needs — it doesn't require strict launch-order completion, only an
    honest count of how many trials have concluded so far."""
    with lock:
        statuses[i] = "completed" if result["returncode"] == 0 else "failed"
        _write_progress(progress_path, jobs, statuses)
        job_dsr_gate_failed = False
        job_integrity_failed = False
        if statuses[i] == "completed":
            n_trials_so_far = sum(1 for s in statuses if s == "completed")
            dsr = _compute_and_write_dsr(job, i, suffix, n_trials_so_far)
            min_dsr_threshold = job.get("min_dsr_threshold")
            if min_dsr_threshold is not None and (dsr is None or dsr < min_dsr_threshold):
                job_dsr_gate_failed = True
                statuses[i] = "dsr_gate_failed"
                result["dsr"] = dsr
                result["min_dsr_threshold"] = min_dsr_threshold
                logger.error(
                    f"run_strategy_queue: job[{i}] failed the DSR gate "
                    f"(dsr={dsr!r} < min_dsr_threshold={min_dsr_threshold!r})"
                )
                _write_progress(progress_path, jobs, statuses)

            if not job_dsr_gate_failed:
                integrity_passed = _check_integrity_passed(job, i, suffix)
                if integrity_passed is False:
                    job_integrity_failed = True
                    statuses[i] = "integrity_check_failed"
                    result["integrity_passed"] = integrity_passed
                    logger.error(
                        f"run_strategy_queue: job[{i}] failed post-run integrity checks "
                        "(integrity_passed=False)"
                    )
                    _write_progress(progress_path, jobs, statuses)
        result["_dsr_gate_failed"] = job_dsr_gate_failed
        result["_integrity_failed"] = job_integrity_failed
        if result["returncode"] != 0 or job_dsr_gate_failed or job_integrity_failed:
            if not job_dsr_gate_failed and not job_integrity_failed:
                logger.error(f"run_strategy_queue: job[{i}] failed (exit {result['returncode']})")
        return result


def _run_queue_concurrent(
    jobs: List[Dict[str, Any]], min_free_mb: float, wait_timeout_s: float,
    stop_on_failure: bool, report_suffix: Optional[str], resume: bool, max_workers: int,
) -> Dict[str, Any]:
    """max_workers>1 path — see run_queue()'s docstring. Jobs are submitted
    to a bounded ThreadPoolExecutor (each job is its own OS subprocess —
    the thread only blocks waiting on it, so this is real process-level
    concurrency); shared mutable state (statuses, progress file) is
    guarded by a lock, same invariants the sequential loop maintains one
    job at a time. stop_on_failure here means "stop SUBMITTING new jobs
    once a failure is observed" — already-in-flight jobs are allowed to
    finish rather than killed."""
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
            f"and {n_excluded} manually-excluded job(s) of {len(jobs)} (concurrent, max_workers={max_workers})"
        )
    _write_progress(progress_path, jobs, statuses)

    lock = threading.Lock()
    stop_submitting = threading.Event()
    results: List[Dict[str, Any]] = []
    pending_indices = [i for i in range(len(jobs)) if i not in prior_resolved]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        next_idx = 0

        def _submit_next() -> bool:
            nonlocal next_idx
            if next_idx >= len(pending_indices) or stop_submitting.is_set():
                return False
            i = pending_indices[next_idx]
            next_idx += 1
            wait_for_headroom(min_free_mb, wait_timeout_s, label="run_strategy_queue")
            with lock:
                statuses[i] = "running"
                _write_progress(progress_path, jobs, statuses)
            futures[executor.submit(_run_job, jobs[i], i, suffix)] = i
            return True

        for _ in range(min(max_workers, len(pending_indices))):
            _submit_next()

        while futures:
            for future in as_completed(list(futures)):
                i = futures.pop(future)
                job_result = future.result()
                job_result = _process_job_completion(jobs[i], i, suffix, job_result, statuses, progress_path, jobs, lock)
                results.append(job_result)
                if job_result["returncode"] != 0 or job_result["_dsr_gate_failed"] or job_result["_integrity_failed"]:
                    if stop_on_failure:
                        stop_submitting.set()
                if not stop_submitting.is_set():
                    _submit_next()
                break  # re-enter as_completed() with the current (mutated) futures dict

    with lock:
        if stop_submitting.is_set():
            unresolved = [i for i in pending_indices if statuses[i] in ("queued", "running")]
            for j in unresolved:
                statuses[j] = "skipped"
            if unresolved:
                logger.error("run_strategy_queue: stopping — skipping remaining unsubmitted job(s) after a failure")
            _write_progress(progress_path, jobs, statuses)

    n_skipped_prior = len(prior_resolved)
    results.sort(key=lambda r: r["job_index"])
    summary = {
        "generated_at": datetime.now().isoformat(),
        "total_jobs": len(jobs),
        "jobs_run": len(results),
        "jobs_skipped_already_completed": n_skipped_prior,
        "results": results,
        "all_passed": (
            all(r["returncode"] == 0 for r in results)
            and not any(statuses[r["job_index"]] == "dsr_gate_failed" for r in results)
            and not any(statuses[r["job_index"]] == "integrity_check_failed" for r in results)
            and len(results) + n_skipped_prior == len(jobs)
        ),
        "runtime_seconds": time.monotonic() - run_started,
    }
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    summary_path = REPORTS_DIR / f"strategy_queue_{suffix}.json"
    with open(summary_path, "w") as fh:
        json.dump(summary, fh, indent=2, default=str)
    logger.info(f"run_strategy_queue: summary written to {summary_path}")
    return summary


def run_queue(
    jobs: List[Dict[str, Any]], min_free_mb: float = 3072.0, wait_timeout_s: float = 600.0,
    stop_on_failure: bool = True, report_suffix: Optional[str] = None, resume: bool = True,
    max_workers: int = 1,
) -> Dict[str, Any]:
    """
    max_workers : (2026-08-02, Technical sweep parallelization) — default 1
        preserves today's behavior EXACTLY: jobs run one at a time, in
        order, in this same thread. Every existing caller omitting this
        param is completely unaffected. >1 dispatches jobs to a bounded
        ThreadPoolExecutor instead (each job is still its own OS
        subprocess — the thread just waits on it — so this is real
        process-level parallelism, not GIL-limited). Each job should pass
        {"defer_db_writes": true} (backtest/run_orchestrator_backtest.py)
        so concurrent jobs don't fight over BACKTEST_DUCKDB_PATH's single
        writer connection — max_workers>1 without defer_db_writes will
        still "work" but jobs will mostly serialize on
        exclusive_backtest_lock anyway, gaining nothing.
        wait_for_headroom() is still checked before each new job is
        admitted into the pool (not just at the very start), so a memory
        squeeze mid-sweep still throttles new admissions the same way the
        serial path always has.
    """
    if not jobs:
        raise ValueError("jobs is empty — nothing to schedule")
    jobs = _maybe_prewarm_ohlcv(jobs)
    if max_workers > 1:
        return _run_queue_concurrent(
            jobs, min_free_mb=min_free_mb, wait_timeout_s=wait_timeout_s,
            stop_on_failure=stop_on_failure, report_suffix=report_suffix, resume=resume,
            max_workers=max_workers,
        )

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
        job_dsr_gate_failed = False
        job_integrity_failed = False
        if statuses[i] == "completed":
            n_trials_so_far = sum(1 for s in statuses if s == "completed")
            dsr = _compute_and_write_dsr(job, i, suffix, n_trials_so_far)
            min_dsr_threshold = job.get("min_dsr_threshold")
            # Opt-in gate: only enforced when the job explicitly sets
            # min_dsr_threshold (default None = no gate, matching the
            # pre-existing behavior for every caller that doesn't pass it).
            if min_dsr_threshold is not None and (dsr is None or dsr < min_dsr_threshold):
                job_dsr_gate_failed = True
                statuses[i] = "dsr_gate_failed"
                result["dsr"] = dsr
                result["min_dsr_threshold"] = min_dsr_threshold
                logger.error(
                    f"run_strategy_queue: job[{i}] failed the DSR gate "
                    f"(dsr={dsr!r} < min_dsr_threshold={min_dsr_threshold!r})"
                )
                _write_progress(progress_path, jobs, statuses)

            # [BUG FIX, 4th fundamental-strategies review, item 3] NOT opt-in
            # (unlike min_dsr_threshold above) — a run whose persisted
            # integrity_passed is False (CRITICAL SPEC-BT-001 checks failed)
            # must never be indistinguishable from a genuinely clean
            # 'completed' run, regardless of whether this job set a DSR gate.
            if not job_dsr_gate_failed:
                integrity_passed = _check_integrity_passed(job, i, suffix)
                if integrity_passed is False:
                    job_integrity_failed = True
                    statuses[i] = "integrity_check_failed"
                    result["integrity_passed"] = integrity_passed
                    logger.error(
                        f"run_strategy_queue: job[{i}] failed post-run integrity checks "
                        "(integrity_passed=False)"
                    )
                    _write_progress(progress_path, jobs, statuses)
        results.append(result)
        if result["returncode"] != 0 or job_dsr_gate_failed or job_integrity_failed:
            if not job_dsr_gate_failed and not job_integrity_failed:
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
            all(r["returncode"] == 0 for r in results)
            and not any(statuses[r["job_index"]] == "dsr_gate_failed" for r in results)
            and not any(statuses[r["job_index"]] == "integrity_check_failed" for r in results)
            and len(results) + n_skipped_prior == len(jobs)
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
        "--max-workers", type=int, default=1,
        help=(
            "Run this many jobs concurrently (default 1 = the long-standing serial "
            "behaviour). Each job is its own OS subprocess, so this is real "
            "process-level parallelism. Jobs MUST set defer_db_writes, otherwise they "
            "serialize on BACKTEST_DUCKDB_PATH's single writer and nothing is gained. "
            "Pin BLAS to 1 thread per worker (OMP_NUM_THREADS=1 etc.) or the workers "
            "will oversubscribe the CPU and run slower than serial."
        ),
    )
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

    # Guard the footgun the run_queue docstring warns about: >1 worker without
    # defer_db_writes doesn't fail, it just quietly serializes on the write
    # lock and buys nothing — the kind of "it ran, so it must have worked"
    # outcome that is easy to miss in a multi-hour sweep.
    if args.max_workers > 1:
        n_deferred = sum(1 for j in jobs if j.get("defer_db_writes"))
        if n_deferred < len(jobs):
            logger.warning(
                "run_strategy_queue: --max-workers=%d but only %d/%d jobs set "
                "defer_db_writes — the rest will serialize on the DuckDB writer",
                args.max_workers, n_deferred, len(jobs),
            )

    summary = run_queue(
        jobs=jobs, min_free_mb=args.min_free_mb, wait_timeout_s=args.wait_timeout_s,
        stop_on_failure=not args.continue_on_failure, report_suffix=args.report_suffix,
        resume=not args.no_resume, max_workers=args.max_workers,
    )
    print(json.dumps(summary, indent=2, default=str))
    sys.exit(0 if summary["all_passed"] else 1)


if __name__ == "__main__":
    main()
