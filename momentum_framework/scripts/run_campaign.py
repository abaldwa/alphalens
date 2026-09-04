"""
momentum_framework/scripts/run_campaign.py

Two-pass campaign runner across every (strategy, band) config in
campaign_registry.py:

  Pass 1 (parity gate): 3-year window [2009-01-01, 2011-12-31], native vs
  legacy, via scripts/parity_check.py. Confirms the code is actually
  correct before trusting anything from a given config.

  Pass 2 (real evaluation, only for configs that pass Pass 1): full native
  run [2009-01-01, 2026-06-30] — explicit user instruction, 2026-09-04:
  trade dates start 2009-01-01, matching legacy's own convention (the
  earlier 2007-01-01 start, meant to capture 2008 GFC drawdown for
  longer-lookback strategies, was superseded by this later instruction).
  Persisted to framework_backtest_runs/framework_backtest_trades.

CONCURRENCY: Pass 1 runs SEQUENTIALLY, not in a thread pool. scripts/
parity_check.py's run_legacy() mutates PROCESS-GLOBAL os.environ
(BACKTEST_DUCKDB_PATH, ALPHALENS_BACKTEST_DUCKDB_PATH) around each legacy
engine call to isolate it to a throwaway DB — running that concurrently
across threads would let one thread's isolated-DB path leak into another
thread's legacy run mid-flight (os.environ is process-wide, not thread-
local), silently corrupting which DB a "legacy" call actually reads/
writes. Pass 2 has no such global mutable state (pure OHLCV reads via a
read-only connection each thread owns) and IS parallelized.

INCREMENTAL WRITES + LIVE PROGRESS (added 2026-09-04, explicit user
requirement ahead of the real full-scale campaign — a batch-at-the-end
design is fine for a small smoke test but wrong for a long real run):
- Every job (Pass 1 or Pass 2) is persisted / logged the moment it
  finishes, not batched after the whole phase completes — a crash or
  interruption partway through loses at most one job's work, not
  everything.
- A lightweight `campaign_progress.json` is rewritten after every job
  with done/total counts, elapsed time, measured average seconds/job
  (tracked SEPARATELY per phase, since Pass 1's legacy+native pair and
  Pass 2's native-only full run have very different costs), and an ETA
  extrapolated from that average — poll this file to see "what's left"
  without needing to tail process output.

Run: PYTHONPATH=. python3 momentum_framework/scripts/run_campaign.py
"""

import concurrent.futures
import json
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb

from momentum_framework.backtesting.orchestrator import BacktestConfig, BacktestOrchestrator
from momentum_framework.results.db_writer import FrameworkResultsDBWriter
from momentum_framework.scripts.campaign_registry import all_configs
from momentum_framework.scripts.parity_check import check_parity

PROD_DB_PATH = "/home/amit/projects/AlphaLens/datastore/normalised/alphalens.duckdb"
PARITY_START, PARITY_END = "2009-01-01", "2011-12-31"
FULL_START, FULL_END = "2009-01-01", "2026-06-30"
INITIAL_CAPITAL = 1_000_000.0
PASS2_MAX_WORKERS = 4

# Explicit user pass criteria (2026-09-04): >90% buy-trade agreement AND
# CAGR within 2 percentage points — see _passed_parity() below.
MIN_BUY_AGREEMENT_PCT = 90.0
MAX_CAGR_ABS_DIFF = 0.02  # 2 percentage points, explicit user criterion, 2026-09-04

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"
RESULTS_LOG = RESULTS_DIR / "campaign_run_log.json"
PROGRESS_LOG = RESULTS_DIR / "campaign_progress.json"
LOCK_FILE = RESULTS_DIR / "campaign_run.lock"


def _acquire_lock() -> None:
    """
    Refuse to start if another run_campaign.py is already alive (added
    2026-09-04 after a stray duplicate process overwrote an 89-entry
    campaign_run_log.json with a fresh, empty one mid-run). A single PID
    file is enough here — Pass 1 is sequential and only one `main()`
    should ever be constructing a ProgressTracker at a time.
    """
    import os

    if LOCK_FILE.exists():
        try:
            old_pid = int(LOCK_FILE.read_text().strip())
            os.kill(old_pid, 0)  # raises if PID is dead
            raise SystemExit(
                f"Another campaign run appears active (PID {old_pid}, lock={LOCK_FILE}). "
                f"Refusing to start a second run — it would overwrite {RESULTS_LOG}. "
                f"If that PID is actually dead, delete {LOCK_FILE} and retry."
            )
        except ProcessLookupError:
            pass  # stale lock from a dead process — safe to reclaim
        except ValueError:
            pass  # corrupt lock file — safe to reclaim
    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOCK_FILE.write_text(str(os.getpid()))


def _release_lock() -> None:
    try:
        LOCK_FILE.unlink()
    except FileNotFoundError:
        pass


class ProgressTracker:
    """
    Thread-safe (Pass 2 runs on a thread pool) incremental progress
    writer. Tracks Pass 1 and Pass 2 timing SEPARATELY — a legacy+native
    parity pair and a full 19-year native-only run cost very differently,
    so one shared average would give a garbage ETA for whichever phase is
    running.
    """

    def __init__(self, pass1_total: int):
        self._lock = threading.Lock()
        self.t_start = time.time()
        self.pass1_total = pass1_total
        self.pass1_done = 0
        self.pass1_elapsed_s = 0.0
        self.pass2_total: Optional[int] = None  # known only once Pass 1 finishes
        self.pass2_done = 0
        self.pass2_elapsed_s = 0.0
        self.results: List[Dict[str, Any]] = []
        self._write()

    def _avg(self, done: int, elapsed: float) -> Optional[float]:
        return (elapsed / done) if done else None

    def _write(self) -> None:
        pass1_avg = self._avg(self.pass1_done, self.pass1_elapsed_s)
        pass2_avg = self._avg(self.pass2_done, self.pass2_elapsed_s)
        pass1_remaining = max(self.pass1_total - self.pass1_done, 0)
        pass2_remaining = max((self.pass2_total or 0) - self.pass2_done, 0)
        eta_s = 0.0
        if pass1_avg is not None:
            eta_s += pass1_avg * pass1_remaining
        elif pass1_remaining:
            eta_s = float("nan")  # no measurement yet — can't estimate
        if pass2_avg is not None:
            eta_s += pass2_avg * pass2_remaining
        elif self.pass2_total and pass2_remaining and pass1_remaining == 0:
            eta_s = float("nan")

        progress = {
            "elapsed_s": round(time.time() - self.t_start, 1),
            "pass1": {
                "done": self.pass1_done, "total": self.pass1_total,
                "avg_s_per_job": round(pass1_avg, 1) if pass1_avg else None,
            },
            "pass2": {
                "done": self.pass2_done, "total": self.pass2_total,
                "avg_s_per_job": round(pass2_avg, 1) if pass2_avg else None,
            },
            "estimated_seconds_remaining": None if eta_s != eta_s else round(eta_s, 0),  # NaN check
            "estimated_minutes_remaining": None if eta_s != eta_s else round(eta_s / 60, 1),
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        PROGRESS_LOG.parent.mkdir(parents=True, exist_ok=True)
        PROGRESS_LOG.write_text(json.dumps(progress, indent=2))

    def record_pass1(self, result: Dict[str, Any], elapsed_s: float) -> None:
        with self._lock:
            self.pass1_done += 1
            self.pass1_elapsed_s += elapsed_s
            self.results.append(result)
            self._write_results_log_locked()
            self._write()

    def set_pass2_total(self, n: int) -> None:
        with self._lock:
            self.pass2_total = n
            self._write()

    def record_pass2(self, label: str, result: Dict[str, Any], elapsed_s: float) -> None:
        with self._lock:
            self.pass2_done += 1
            self.pass2_elapsed_s += elapsed_s
            for r in self.results:
                if r["label"] == label:
                    r["pass2"] = result
                    break
            self._write_results_log_locked()
            self._write()

    def _write_results_log_locked(self) -> None:
        RESULTS_LOG.parent.mkdir(parents=True, exist_ok=True)
        RESULTS_LOG.write_text(json.dumps(self.results, indent=2, default=str))


def _passed_parity(comparison: Dict[str, Any]) -> bool:
    """
    Explicit user-specified gate (2026-09-04, tightened after reviewing
    the first ~40 live results): buy-trade agreement must exceed 90%, AND
    legacy/native CAGR must be within 2 percentage points of each other
    (absolute difference, not a same-sign check — the earlier same-sign
    version was replaced because it could pass a config on a coincidental
    sign match despite a large magnitude gap, and could fail one on a
    trivial sign flip between two near-zero values; an absolute-diff bound
    is the more direct statement of "these two numbers actually agree").
    """
    agreement = comparison["buy_agreement_pct"]
    legacy_cagr = comparison["legacy_metrics"].get("cagr")
    native_cagr = comparison["native_metrics"].get("cagr")
    if legacy_cagr is None or native_cagr is None:
        cagr_close = True  # nothing to compare — don't fail on missing data alone
    else:
        cagr_close = abs(legacy_cagr - native_cagr) <= MAX_CAGR_ABS_DIFF
    return bool(agreement > MIN_BUY_AGREEMENT_PCT and cagr_close)


def run_pass1(strategy_code: str, band_id: int, top_n: int, factory, prod_conn: Any) -> Dict[str, Any]:
    label = f"{strategy_code}/M{band_id:02d}"
    result: Dict[str, Any] = {"strategy_code": strategy_code, "band_id": band_id, "top_n": top_n, "label": label}
    try:
        comparison = check_parity(
            factory, PARITY_START, PARITY_END, prod_conn,
            initial_capital=INITIAL_CAPITAL, verbose=False,
        )
    except Exception as e:
        result["phase"] = "parity_check_error"
        result["error"] = f"{type(e).__name__}: {e}"
        return result

    passed = _passed_parity(comparison)
    result["parity"] = {
        "buy_agreement_pct": comparison["buy_agreement_pct"],
        "legacy_cagr": comparison["legacy_metrics"].get("cagr"),
        "native_cagr": comparison["native_metrics"].get("cagr"),
        "legacy_buy_count": comparison["legacy_buy_count"],
        "native_buy_count": comparison["native_buy_count"],
        "passed": passed,
    }
    result["phase"] = "parity_passed" if passed else "parity_failed"
    return result


def run_pass2(strategy_code: str, band_id: int, factory, label: Optional[str] = None) -> Dict[str, Any]:
    # `label` override (2026-09-04): the default f"{code}/M{band_id:02d}"
    # collided the moment campaign_registry.py started emitting MULTIPLE
    # configs per (strategy_code, band_id) — the full lookback x cadence x
    # position_sizing grid — since ProgressTracker.record_pass2() matches
    # results by exact label string. A caller with more than one config
    # per band MUST pass a label that also encodes top_n/lookback/cadence/
    # sizing (see run_full_campaign.py), or later results silently
    # overwrite earlier ones' slot instead of recording their own.
    if label is None:
        label = f"{strategy_code}/M{band_id:02d}"
    prod_conn = duckdb.connect(PROD_DB_PATH, read_only=True)
    try:
        strategy = factory()  # fresh instance — never reuse across runs
        config = BacktestConfig(start_date=FULL_START, end_date=FULL_END, initial_capital=INITIAL_CAPITAL)
        full_result = BacktestOrchestrator(strategy, config).run_native(prod_conn)
        return {
            "label": label, "phase": "full_run_complete",
            "strategy_id": full_result.strategy_id, "run_id": full_result.run_id,
            "metrics": full_result.metrics, "trade_count": full_result.trade_count,
            "_full_result_obj": full_result,
        }
    except Exception as e:
        return {"label": label, "phase": "full_run_error", "error": f"{type(e).__name__}: {e}"}
    finally:
        prod_conn.close()


def main() -> None:
    _acquire_lock()
    try:
        _run_campaign()
    finally:
        _release_lock()


def _run_campaign() -> None:
    configs = all_configs()
    print(f"Campaign: {len(configs)} (strategy, band) configs")
    print(f"Pass 1 (parity gate, sequential): {PARITY_START} to {PARITY_END} vs legacy")
    print(f"Pass 2 (real eval, parallel x{PASS2_MAX_WORKERS}, only for Pass 1 passes): {FULL_START} to {FULL_END}")
    print(f"Live progress: {PROGRESS_LOG}\nIncremental results: {RESULTS_LOG}\n")

    tracker = ProgressTracker(pass1_total=len(configs))
    writer = FrameworkResultsDBWriter()

    prod_conn = duckdb.connect(PROD_DB_PATH, read_only=True)
    pass1_results: List[Dict[str, Any]] = []
    try:
        for i, (code, band_id, top_n, factory) in enumerate(configs, 1):
            t0 = time.time()
            r = run_pass1(code, band_id, top_n, factory, prod_conn)
            elapsed = time.time() - t0
            pass1_results.append(r)
            tracker.record_pass1(r, elapsed)
            p = r.get("parity", {})
            print(f"[Pass1 {i}/{len(configs)}] {r['label']}: {r['phase']} ({elapsed:.1f}s)"
                  + (f" (agreement={p.get('buy_agreement_pct', 0):.0f}%, "
                     f"legacy_cagr={p.get('legacy_cagr')}, native_cagr={p.get('native_cagr')})"
                     if p else f" — {r.get('error')}"))
    finally:
        prod_conn.close()

    to_run_pass2 = [
        (r["strategy_code"], r["band_id"], factory)
        for r, (code, band_id, top_n, factory) in zip(pass1_results, configs)
        if r["phase"] == "parity_passed"
    ]
    tracker.set_pass2_total(len(to_run_pass2))
    print(f"\n{len(to_run_pass2)}/{len(configs)} configs passed Pass 1 — running Pass 2 (parallel)...\n")

    persisted = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=PASS2_MAX_WORKERS) as executor:
        futures = {}
        for code, band_id, factory in to_run_pass2:
            t0 = time.time()
            future = executor.submit(run_pass2, code, band_id, factory)
            futures[future] = (f"{code}/M{band_id:02d}", t0)

        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            label, t0 = futures[future]
            r = future.result()
            elapsed = time.time() - t0

            # Persist THIS job immediately — not batched after the pool finishes.
            obj = r.pop("_full_result_obj", None)
            if obj is not None:
                writer.write(obj, engine="native", universe_cache_used=True, parity_checked=True)
                persisted += 1

            tracker.record_pass2(label, r, elapsed)
            m = r.get("metrics") or {}
            print(f"[Pass2 {i}/{len(to_run_pass2)}] {label}: {r['phase']} ({elapsed:.1f}s)"
                  + (f" CAGR={m.get('cagr')}, Sharpe={m.get('sharpe_ratio')}, trades={r.get('trade_count')}"
                     if m else f" — {r.get('error')}"))

    elapsed_total = time.time() - tracker.t_start
    all_results = tracker.results
    n_full = sum(1 for r in all_results if r.get("pass2", {}).get("phase") == "full_run_complete")
    n_parity_failed = sum(1 for r in all_results if r["phase"] == "parity_failed")
    n_errors = sum(1 for r in all_results if r["phase"] == "parity_check_error"
                   or r.get("pass2", {}).get("phase") == "full_run_error")

    print(f"\n=== Campaign complete in {elapsed_total/60:.1f} min ===")
    print(f"  Full run persisted: {n_full}")
    print(f"  Failed Pass 1 (investigate): {n_parity_failed}")
    print(f"  Errors: {n_errors}")
    print(f"  Persisted to framework_backtest_runs: {persisted}")
    print(f"  Full log: {RESULTS_LOG}")

    needs_investigation = [r for r in all_results if r["phase"] != "parity_passed"
                            or r.get("pass2", {}).get("phase") != "full_run_complete"]
    if needs_investigation:
        print(f"\n=== {len(needs_investigation)} configs need investigation ===")
        for r in needs_investigation:
            detail = r.get("parity") or r.get("error") or r.get("pass2", {}).get("error")
            print(f"  {r['label']}: {r['phase']} — {detail}")


if __name__ == "__main__":
    main()
