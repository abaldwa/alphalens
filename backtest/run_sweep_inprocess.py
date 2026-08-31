"""
backtest/run_sweep_inprocess.py

Phase: Backtest sweep performance (A87 Stage 1 + 2)
Owner: Platform / Backtest
Consumers: operator CLI
    (`python3 -m backtest.run_sweep_inprocess --queue-file Q.json --report-suffix S`)

Runs a whole sweep in ONE process so every strategy shares the setup, instead
of rebuilding it in a subprocess per job.

THE MEASUREMENT THIS EXISTS FOR
-------------------------------
2026-08-14, on the 186-job sweep. The simulation is not the cost:

    technical   247.8s wall clock, 17.7s measured inside orchestrator.run()
    momentum     82.8s wall clock,  3.8s measured

Profiling the unmeasured remainder:

    _fetch_real_ohlcv                     39.5s
    _build_config derivations             12.7s
    close / volume pivots                  1.4s

All strategy-independent, all rebuilt 186 times, against a 4,275-day
simulation costing about 11 seconds. Roughly seven hours of duplicated setup
wrapped around 35 minutes of actual work.

STAGE 1 -- SHARE THE PANELS
Nothing here does the sharing itself: backtest/shared_panels.py memoises the
artifacts, and _fetch_real_ohlcv/_build_config consult it. This module's only
contribution is to call run_orchestrator_backtest() in-process, in a loop, so
those memos hit. Job 1 pays the setup; jobs 2..N do not.

STAGE 2 -- DATE-MAJOR SIGNAL REUSE
Technical screening is the largest measured phase (9.8s of 10.8s, 204 calls
at 48ms). It is a pure function of (template, date) -- never of the exit
policy, the capital mode, or what a run currently holds -- which is exactly
what backtest/core/screener_cache.py already exploits via the
technical_screener_cache table.

That cache is DISABLED under defer_db_writes (run_orchestrator_backtest.py's
deferred path leaves _screener_cache_conn unset), because the parallel queue
could not afford a live DuckDB connection per worker. In one process that
tradeoff is gone, so _job_kwargs DROPS defer_db_writes from every job: the
run then takes the immediate path, which wires _screener_cache_conn, and the
first strategy to need (template, date) computes it while every later
strategy reads it. With 126 technical jobs over 63 templates that is a 2x
reuse factor on the largest measured phase, before counting exit-variant
sweeps.

Verified equal, not assumed: three momentum strategies run through this path
reproduced the subprocess runs' cagr / max_drawdown / sharpe / n_trades to
ten decimal places. Sharing setup must not change a single number, and the
comparison is cheap enough that there is no excuse for asserting it instead.

WHY NOT JUST PARALLELISE HARDER
-------------------------------
Because the duplicated work scales with worker count, not against it. Two
workers rebuild the panels twice and hold two copies of a 7.1M-row frame --
which is what drove the OOM near-miss on 2026-08-14 (12.4GB RSS in one
worker, 3.0GB left on the box). Sharing is both faster AND lighter: one
resident copy for the whole sweep.

The cost is isolation. A crash takes the batch rather than one job, so every
completed strategy is appended to progress.jsonl and fsynced immediately, and
--resume skips what already succeeded. Same durability contract as
scripts/run_priority_backtest.py, and deliberately the same file format so
either runner can resume the other's work.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import traceback
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List

from backtest import shared_panels
from backtest.run_orchestrator_backtest import run_orchestrator_backtest

logger = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parents[1]
PROGRESS_ROOT = REPO / "backtest" / "reports" / "_progress"

#: Job fields that are not run_orchestrator_backtest kwargs.
_NON_KWARGS = {"kind"}

#: Queue job fields whose NAME differs from the function parameter. The queue
#: speaks CLI (--exit-variant, argparse dest `exit_variant`); main() then
#: forwards it as `exit_policy_variant=`. Running in-process skips the CLI, so
#: the rename has to happen here instead. Asserted complete at import time
#: below -- a field that is neither a kwarg nor mapped would otherwise be
#: silently dropped, and a dropped exit_variant means every job quietly runs
#: the DEFAULT exit policy while its report claims otherwise.
_FIELD_ALIASES = {
    "exit_variant": "exit_policy_variant",
}

#: Queue-only fields the CLI consumes without forwarding them by name.
_QUEUE_ONLY = {"annual_reset_no_top_up", "min_dsr_threshold", "lookback_days"}

#: Channel order: momentum first, per the operator's stated priority.
CHANNEL_PRIORITY = {"momentum": 0, "technical": 1, "fundamental": 2}


def _as_date(v: Any) -> Any:
    return date.fromisoformat(v) if isinstance(v, str) else v


def _job_kwargs(job: Dict[str, Any], enable_screener_cache: bool = False, force_immediate: bool = False) -> Dict[str, Any]:
    import inspect

    valid = set(inspect.signature(run_orchestrator_backtest).parameters)
    kw = {}
    for k, v in job.items():
        if k in _NON_KWARGS or k in _QUEUE_ONLY:
            continue
        name = _FIELD_ALIASES.get(k, k)
        if name not in valid:
            # Loud, not lenient. Silently dropping a field means the run
            # executes a different strategy than the queue asked for and
            # reports success.
            raise ValueError(
                f"queue field {k!r} is not a run_orchestrator_backtest parameter and has no entry "
                f"in _FIELD_ALIASES/_QUEUE_ONLY -- add one rather than letting it be dropped"
            )
        kw[name] = v
    for f in ("start_date", "end_date"):
        if f in kw:
            kw[f] = _as_date(kw[f])
    # defer_db_writes exists to keep a DuckDB connection out of a parallel
    # worker's long middle. Dropping it takes the immediate path, which wires
    # technical_screener_cache -- Stage 2's reuse.
    #
    # MEASURED 2026-08-14, and it does not pay for itself on a first pass:
    # two technical jobs on the immediate path took 89.9s and 174.7s, the
    # second WITH a panel-cache hit, against 247.8s on the deferred path
    # including full setup. Populating the cache costs one DuckDB write per
    # (template, date) -- 63 templates x 4,275 sessions -- and the sweep only
    # reads each key twice (control and filtered arms of the same template).
    # Paying 4,275 writes to save one re-screen is a losing trade.
    #
    # So it is opt-in. It becomes worthwhile when a template is run MANY
    # times, which is what an exit-variant sweep does (up to 9 jobs per
    # template) -- that is the case the cache was written for.
    #
    # force_immediate=True (A87 Phase 3, 2026-08-31): for momentum strategies
    # in a single-process sweep, the deferred path is redundant (no multi-worker
    # contention) and has silent-failure issues (DuckDB lock-retry exhaustion
    # can die the process before backtest.duckdb is touched). Momentum strategies
    # in Phase 2A/2B should always use immediate writes when run in-process.
    if force_immediate:
        kw.pop("defer_db_writes", None)
    elif not enable_screener_cache:
        kw.setdefault("defer_db_writes", True)
    else:
        kw.pop("defer_db_writes", None)
    return kw


def _describe(job: Dict[str, Any]) -> str:
    return (
        job.get("template_name")
        or job.get("preset")
        or f"lb{job.get('lookback_months')}"
    )


def drain_feature_log_spills(label: str = "sweep") -> int:
    """Bulk-load every pending feature-log spill file into the DB, once.

    The counterpart to run_orchestrator_backtest(defer_feature_log=True).
    Each job leaves its spill JSONL on disk instead of paying 11.5s of
    executemany + re-read inside its own write tail (measured 2026-08-19 on a
    31.2s warm job); this drains them all in ONE lock acquisition at the end.

    Crash-safe by construction: the spill files are the durable copy, so an
    OOM or a hard kill loses nothing that was written -- re-running this
    drains whatever is still there. Results (backtest_runs, strategy_signals)
    were already written per job and are never deferred.
    """
    from backtest.batch_common import exclusive_backtest_lock
    from backtest.core.feature_log import load_spill_file
    from backtest.run_orchestrator_backtest import _feature_log_spill_dir
    from config.settings import (
        BACKTEST_DUCKDB_PATH, DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS,
        DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S, DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S,
    )
    from datastore.api.db import get_duckdb_connection

    spills = sorted(_feature_log_spill_dir().glob("backtest_feature_log_spill_*.jsonl"))
    if not spills:
        return 0
    total = 0
    with exclusive_backtest_lock(label=f"{label}[feature-log-drain]"):
        with get_duckdb_connection(
            BACKTEST_DUCKDB_PATH, read_only=False, persist=False,
            retry_attempts=DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS,
            retry_base_delay_s=DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S,
            retry_max_delay_s=DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S,
        ) as conn:
            for path in spills:
                try:
                    total += load_spill_file(conn, path)
                except Exception:  # noqa: BLE001 -- one bad spill must not
                    # strand the rest; the file is left in place to retry.
                    logger.exception("feature-log spill failed to load: %s", path)
    print(f"feature-log drain: {total} rows from {len(spills)} spill file(s)", flush=True)
    return total


def run_sweep(
    jobs: List[Dict[str, Any]], report_suffix: str, resume: bool = True,
    stop_on_error: bool = False, enable_screener_cache: bool = False,
    shard: int = 0, num_shards: int = 1, defer_feature_log: bool = False,
) -> int:
    """shard/num_shards (2026-08-19): run only the jobs where
    `idx % num_shards == shard`.

    WHY SHARDING RATHER THAN A THREAD POOL. The whole point of this module is
    the process-lifetime memo in shared_panels -- the OHLCV frame, the derived
    config artifacts and (since 2026-08-19) the yearly rankings, which alone
    measured 72.0s per call and is identical for every strategy in the sweep.
    A memo cannot cross a process boundary, so run_strategy_queue's
    subprocess-per-job parallelism pays that 72s 1,260 times over. Threads
    would share the memo but not the GIL, and the simulation is Python-bound.

    Sharding gets both: N processes, each paying the setup ONCE and then
    reusing it across its ~1,260/N jobs. Setup cost goes from O(jobs) to
    O(shards).

    Each shard keeps its OWN progress file, so --resume is per shard and two
    shards can never interleave writes into one jsonl. Job report suffixes
    stay keyed by the GLOBAL index, so shard membership is not baked into any
    artifact name -- re-running with a different --num-shards resumes cleanly.
    """
    if not (0 <= shard < num_shards):
        raise ValueError(f"shard {shard} out of range for num_shards {num_shards}")
    suffix_dir = report_suffix if num_shards == 1 else f"{report_suffix}_shard{shard}"
    progress_dir = PROGRESS_ROOT / suffix_dir
    progress_dir.mkdir(parents=True, exist_ok=True)
    jsonl = progress_dir / "progress.jsonl"

    done: set[Any] = set()
    if resume and jsonl.exists():
        for line in jsonl.read_text().splitlines():
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue  # torn last line from a hard kill
            if rec.get("outcome") == "ok":
                done.add(rec["idx"])

    indexed = [(i, j) for i, j in enumerate(jobs) if i % num_shards == shard]
    # A job with no "channel" sorts last, like an unrecognised one -- str()
    # so the lookup is well-typed rather than keyed on None.
    indexed.sort(key=lambda pair: CHANNEL_PRIORITY.get(str(pair[1].get("channel")), 99))

    started = time.time()
    ok = failed = 0
    mine = len(indexed)
    todo = sum(1 for i, _ in indexed if i not in done)
    print(f"shard {shard}/{num_shards}: {mine} of {len(jobs)} jobs, {mine - todo} already done, running {todo}")
    print(f"progress: {progress_dir}")

    for idx, job in indexed:
        if idx in done:
            continue
        label = f"job{idx} {job.get('channel')} {_describe(job)}"
        t0 = time.time()
        try:
            run_orchestrator_backtest(
                **_job_kwargs(job, enable_screener_cache, force_immediate=job.get('channel') == 'momentum'), report_suffix=f"{report_suffix}_job{idx}",
                defer_feature_log=defer_feature_log,
            )
            outcome, err = "ok", None
            ok += 1
        except Exception as exc:  # noqa: BLE001 -- one job must not end the sweep
            outcome, err = "failed", f"{type(exc).__name__}: {exc}"
            failed += 1
            logger.error("%s FAILED: %s\n%s", label, exc, traceback.format_exc())
            if stop_on_error:
                raise
        dt = time.time() - t0

        # fsynced per job: a crash at hour six must cost only what was in
        # flight (the MultiBagger lesson).
        with jsonl.open("a") as fh:
            fh.write(json.dumps({
                "idx": idx, "outcome": outcome, "channel": job.get("channel"),
                "name": _describe(job), "filtered": job.get("min_adtv_cr") is not None,
                "duration_s": round(dt), "error": err,
                "at": datetime.now().isoformat(timespec="seconds"),
            }) + "\n")
            fh.flush()
            import os
            os.fsync(fh.fileno())

        st = shared_panels.stats()
        print(
            f"  [{ok + failed:>4}/{todo}] {label:<44} {outcome:>6} "
            f"{dt:6.1f}s  (panel hits {st['ohlcv_hits']}/{st['ohlcv_hits'] + st['ohlcv_misses']})",
            flush=True,
        )

    if defer_feature_log:
        # One bulk load for the whole shard, instead of one per job.
        drain_feature_log_spills(label=f"sweep[{report_suffix}]")

    total = time.time() - started
    print(
        f"\ndone: {ok} ok, {failed} failed in {total / 60:.1f} min "
        f"({total / max(1, ok + failed):.1f}s per job); shared_panels {shared_panels.stats()}"
    )
    return 1 if failed else 0


def main() -> int:
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--queue-file", required=True)
    ap.add_argument("--report-suffix", required=True)
    ap.add_argument("--no-resume", action="store_true")
    ap.add_argument("--stop-on-error", action="store_true", help="abort the sweep on the first failure")
    ap.add_argument("--limit", type=int, help="run only the first N jobs (for timing a sample)")
    ap.add_argument(
        "--shard", type=int, default=0,
        help="this process's shard index (0-based). See run_sweep's docstring.",
    )
    ap.add_argument(
        "--defer-feature-log", action="store_true",
        help=(
            "leave each job's feature-log spill file on disk and bulk-load them ONCE at "
            "the end of the shard, instead of inserting per job. Measured 2026-08-19: the "
            "per-job insert was 11.5s of a 31.2s warm job (7.6s executemany + 3.9s re-read) "
            "for a per-decision debug log. Safe under OOM: the spill files ARE the durable "
            "copy and nothing is held in memory, so a crash loses no feature rows and a "
            "re-run drains whatever remains. Results (backtest_runs/strategy_signals) are "
            "always written per job and never deferred."
        ),
    )
    ap.add_argument(
        "--num-shards", type=int, default=1,
        help=(
            "split the queue across this many PROCESSES, each running the jobs where "
            "idx %% num_shards == shard. Each shard pays the shared_panels setup once "
            "(OHLCV frame, config artifacts, and the 72s yearly rankings) and reuses it "
            "across its share of the jobs -- so setup cost is O(shards), not O(jobs), "
            "which is what subprocess-per-job parallelism cannot do. Launch one process "
            "per shard yourself; memory is the limit, not cores."
        ),
    )
    ap.add_argument(
        "--enable-screener-cache", action="store_true",
        help=(
            "Take the immediate path so technical_screener_cache is populated and reused. "
            "Measured slower on a single pass (the writes cost more than one re-screen saves); "
            "worth it only when the same template runs many times, e.g. an exit-variant sweep."
        ),
    )
    args = ap.parse_args()

    jobs = json.loads(Path(args.queue_file).read_text())["jobs"]
    if args.limit:
        jobs = jobs[: args.limit]
    return run_sweep(
        jobs, args.report_suffix, resume=not args.no_resume,
        stop_on_error=args.stop_on_error, enable_screener_cache=args.enable_screener_cache,
        shard=args.shard, num_shards=args.num_shards,
        defer_feature_log=args.defer_feature_log,
    )


if __name__ == "__main__":
    sys.exit(main())
