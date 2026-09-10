"""
momentum_framework/scripts/run_pass_queue.py

Executes one of the phased queue files built by
build_pass_queues_2026_09_06.py (results/queues/campaign_2026_09_06_pass{1,2,3}.json)
directly against the native engine — no legacy parity gate (this queue's
jobs were built AFTER the 2026-09-06 fixes; parity against the pre-fix
legacy engine is not the question being asked here).

Same concurrency pattern as run_campaign.py's Pass 2 (see that file's
comments for why): BacktestOrchestrator.run_native() calls run in
parallel via a thread pool (pure read-only OHLCV queries + independent
Portfolio state per thread — no shared mutable state), but
FrameworkResultsDBWriter.write() calls happen SEQUENTIALLY in the main
thread as futures complete, never concurrently — DuckDB is single-writer,
and serializing writes in the thread that's already receiving completed
results one at a time is the simplest way to guarantee that.

Run: PYTHONPATH=. python3 momentum_framework/scripts/run_pass_queue.py <pass_file.json>
"""

import concurrent.futures
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict

import duckdb

from momentum_framework.backtesting.orchestrator import BacktestConfig, BacktestOrchestrator
from momentum_framework.results.db_writer import FrameworkResultsDBWriter
from momentum_framework.scripts.job_dispatch import strategy_from_job

PROD_DB_PATH = "/home/amit/projects/AlphaLens/datastore/normalised/alphalens.duckdb"
MAX_WORKERS = 4  # same cap rationale as run_campaign.py's PASS2_MAX_WORKERS

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"
LOCK_FILE = RESULTS_DIR / "pass_queue_run.lock"


def _acquire_lock() -> None:
    if LOCK_FILE.exists():
        try:
            old_pid = int(LOCK_FILE.read_text().strip())
            os.kill(old_pid, 0)
            raise SystemExit(
                f"Another run_pass_queue.py appears active (PID {old_pid}, lock={LOCK_FILE}). "
                f"If that PID is actually dead, delete {LOCK_FILE} and retry."
            )
        except ProcessLookupError:
            pass
        except ValueError:
            pass
    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOCK_FILE.write_text(str(os.getpid()))


def _release_lock() -> None:
    try:
        LOCK_FILE.unlink()
    except FileNotFoundError:
        pass


def _run_one_job(job: Dict[str, Any]) -> Dict[str, Any]:
    """Runs entirely inside a worker thread — its own connection, its own
    Orchestrator/Portfolio, nothing shared with any other job."""
    conn = duckdb.connect(PROD_DB_PATH, read_only=True)
    conn.execute(f"PRAGMA threads={max(1, (os.cpu_count() or MAX_WORKERS) // MAX_WORKERS)}")
    try:
        strategy = strategy_from_job(job)
        config = BacktestConfig(
            start_date=job["start_date"], end_date=job["end_date"],
            initial_capital=float(job.get("initial_capital", 1_000_000)),
            max_tickers=int(job.get("max_tickers", 800)),
            min_history_days=int(job.get("min_history_days", 60)),
            capital_mode=job.get("capital_mode", "lump"),
            exit_variant=job.get("exit_variant", "unconstrained"),
        )
        result = BacktestOrchestrator(strategy, config).run_native(conn)
        return {"job": job, "phase": "complete", "result_obj": result}
    except Exception as e:
        return {"job": job, "phase": "error", "error": f"{type(e).__name__}: {e}"}
    finally:
        conn.close()


class PassProgressTracker:
    def __init__(self, total: int, progress_path: Path, log_path: Path):
        self.total = total
        self.done = 0
        self.errors = 0
        self.t_start = time.time()
        self.progress_path = progress_path
        self.log_path = log_path
        self._write()

    def record(self, entry: Dict[str, Any]) -> None:
        """entry (incl. its BacktestResult under "result_obj") is used only
        for this call's counters/log line, then discarded — retaining it
        past this point (e.g. in a growing list) leaks every job's full
        trade history for the rest of the run and was the real cause of
        the progressive per-job slowdown seen in pass1/pass2/pass3."""
        self.done += 1
        if entry.get("phase") == "error":
            self.errors += 1
        self._write()
        self._append_log(entry)

    def _write(self) -> None:
        elapsed = time.time() - self.t_start
        avg = elapsed / self.done if self.done else None
        remaining = self.total - self.done
        eta_s = avg * remaining if avg is not None else None
        progress = {
            "done": self.done, "total": self.total, "errors": self.errors,
            "elapsed_s": round(elapsed, 1),
            "avg_s_per_job": round(avg, 1) if avg else None,
            "estimated_minutes_remaining": round(eta_s / 60, 1) if eta_s is not None else None,
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        self.progress_path.parent.mkdir(parents=True, exist_ok=True)
        self.progress_path.write_text(json.dumps(progress, indent=2))

    def _append_log(self, entry: Dict[str, Any]) -> None:
        line = {
            "strategy": entry["job"]["strategy_family"],
            "band": entry["job"]["rank_band_id"],
            "top_n": entry["job"]["top_n"],
            "phase": entry["phase"],
        }
        if entry["phase"] == "complete":
            m = entry["result_obj"].metrics
            line["strategy_id"] = entry["result_obj"].strategy_id
            line["cagr"] = m.get("cagr")
            line["post_tax_cagr"] = m.get("post_tax_cagr")
            line["sharpe_ratio"] = m.get("sharpe_ratio")
            line["max_drawdown"] = m.get("max_drawdown")
            line["trade_count"] = entry["result_obj"].trade_count
        else:
            line["error"] = entry.get("error")
        with self.log_path.open("a") as f:
            f.write(json.dumps(line, default=str) + "\n")


def main(pass_file: str) -> None:
    _acquire_lock()
    try:
        _run(pass_file)
    finally:
        _release_lock()


def _run(pass_file: str) -> None:
    path = Path(pass_file)
    data = json.loads(path.read_text())
    jobs = data["jobs"]
    pass_name = path.stem  # e.g. "campaign_2026_09_06_pass1"

    progress_path = RESULTS_DIR / f"{pass_name}_progress.json"
    log_path = RESULTS_DIR / f"{pass_name}_run_log.jsonl"
    log_path.write_text("")  # fresh log for this run

    print(f"Running {pass_name}: {len(jobs)} jobs, {MAX_WORKERS} parallel workers")
    print(f"Progress: {progress_path}\nLog: {log_path}\n")

    tracker = PassProgressTracker(total=len(jobs), progress_path=progress_path, log_path=log_path)
    writer = FrameworkResultsDBWriter()

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(_run_one_job, job): job for job in jobs}
            for future in concurrent.futures.as_completed(futures):
                entry = future.result()
                obj = entry.pop("result_obj", None)
                if obj is not None:
                    writer.write(obj, engine="native", universe_cache_used=True, parity_checked=False)
                    entry["result_obj"] = obj  # restore for the log line (already persisted)
                tracker.record(entry)
                j = entry["job"]
                label = f"{j['strategy_family']}/M{j['rank_band_id']:02d}/top{j['top_n']}"
                if entry["phase"] == "complete":
                    m = obj.metrics
                    print(f"[{tracker.done}/{tracker.total}] {label}: CAGR={m.get('cagr')} "
                          f"Sharpe={m.get('sharpe_ratio')} trades={obj.trade_count}")
                else:
                    print(f"[{tracker.done}/{tracker.total}] {label}: ERROR — {entry.get('error')}")
    finally:
        # One connection is held open across the whole run (see db_writer.py's
        # CONNECTION LIFETIME note) — close it explicitly so it doesn't sit open
        # for the rest of the process, and so the next pass in a chained run
        # opens fresh rather than relying on process exit to release it.
        writer.close()

    print(f"\nDone: {tracker.done}/{tracker.total} ({tracker.errors} errors) "
          f"in {time.time() - tracker.t_start:.0f}s")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python3 run_pass_queue.py <pass_file.json>")
    main(sys.argv[1])
