#!/usr/bin/env python3
"""
scripts/run_priority_backtest.py

Owner: Platform / Backtest
Consumers: operator CLI.

Runs a backtest queue in channel-priority order (momentum -> technical ->
fundamental) under an active memory governor, writing durable progress to
disk as it goes, and renders that progress in a terminal.

WHY THIS EXISTS
---------------
run_strategy_queue.py gates on free memory ONCE, before each job launches,
then does not look again. That is not enough here. Measured 2026-08-14: two
17-year technical jobs launched with 6.8GB free, and ninety seconds later one
worker alone was resident at 12.4GB with 3.0GB left on the box. The pre-flight
check passed and the danger arrived afterwards, which is exactly the shape of
the incidents already on record (systemd-oomd killing the scheduler, and
VS Code itself, on this machine).

So this supervisor samples memory WHILE jobs run, and acts:

  - soft floor  -> stop launching anything new, let running jobs drain
  - hard floor  -> SIGTERM the largest-RSS job and requeue it, then drop
                   concurrency to 1 for the remainder of the run

Killing our own job on our own terms is strictly better than letting the
kernel or systemd-oomd choose the victim: it picked VS Code once, and a
requeued job costs one job's time whereas an OOM-killed supervisor costs the
whole queue.

DURABILITY
----------
Every completed job is appended to progress.jsonl the moment it finishes, and
status.json is rewritten every poll. A crash at hour six therefore loses at
most the jobs actually in flight -- the earlier five hours are on disk and
--resume skips them. This is the lesson from the MultiBagger loss (40h of work
gone with the host, because nothing had been checkpointed).

State lives under backtest/reports/_progress/<suffix>/ -- on the real disk,
NOT /tmp, which is a RAM-backed tmpfs on this machine and would have the
progress log competing for the very memory this script exists to protect.

USAGE
-----
  # build a queue (momentum + technical + fundamental, control vs filtered)
  python3 scripts/run_priority_backtest.py --build-queue Q.json \
      --start-date 2009-04-01 --end-date 2026-06-30

  # run it
  python3 scripts/run_priority_backtest.py --queue-file Q.json \
      --report-suffix prio_20260814 --max-workers 2

  # watch it from another terminal
  python3 scripts/run_priority_backtest.py --status prio_20260814 --watch
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import psutil  # noqa: E402

from backtest.batch_common import available_mb  # noqa: E402
from backtest.run_strategy_queue import _job_to_cmd  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
PROGRESS_ROOT = REPO / "backtest" / "reports" / "_progress"

#: Priority order the user set: momentum first, then technical, then
#: fundamental. Jobs of an unlisted channel sort last, in queue order.
CHANNEL_PRIORITY = {"momentum": 0, "technical": 1, "fundamental": 2}

#: Stop LAUNCHING new jobs below this much available memory (MB).
DEFAULT_SOFT_FLOOR_MB = 3500
#: KILL the largest running job below this (MB). Set well above zero: by the
#: time the box is at a few hundred MB the kernel is already thrashing and a
#: SIGTERM may not be serviced in time.
DEFAULT_HARD_FLOOR_MB = 1800
#: How often to sample memory while jobs run.
POLL_INTERVAL_S = 5.0
#: How often results.txt is republished with strategies + CAGRs so far.
RESULTS_PUBLISH_INTERVAL_S = 120.0


# --------------------------------------------------------------------------
# queue construction
# --------------------------------------------------------------------------

def build_queue(start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """The full control-vs-filtered sweep across all three channels.

    Momentum is swept over lookback windows because that is its primary axis;
    technical over its 63 templates; fundamental over its 26 presets. Every
    strategy appears twice -- once unfiltered (control) and once with the
    three entry filters -- because a filter's effect is only readable against
    the same strategy without it.
    """
    from features.fundamental_composites import STRATEGY_CATALOG
    from systems.technical_analysis.screener.templates import TEMPLATES

    base = dict(
        kind="orchestrator", universe_spec="curated", initial_capital=1_000_000.0,
        capital_mode="lump", max_tickers=800, min_history_days=60,
        defer_db_writes=True, start_date=start_date, end_date=end_date, top_n=10,
    )
    filt = dict(min_adtv_cr=1.0, downtrend_filter_pct=0.15, circuit_band_pct=0.05)

    jobs: List[Dict[str, Any]] = []
    for extra in ({}, filt):
        # Momentum: atr_adaptive, matching the cadence-fix re-run (cad_20260814)
        # so these results are comparable with it.
        for lb in (3, 6, 9, 12):
            jobs.append({**base, "channel": "momentum", "lookback_months": lb,
                         "exit_variant": "atr_adaptive", **extra})
        # Technical/fundamental: risk_managed, matching the prior arms
        # (filt_light_20260814 / fundfix_20260814) for the same reason.
        for name in sorted(t.name for t in TEMPLATES):
            jobs.append({**base, "channel": "technical", "template_name": name,
                         "exit_variant": "risk_managed", "max_hold_days": 25, **extra})
        for preset in sorted(STRATEGY_CATALOG):
            jobs.append({**base, "channel": "fundamental", "preset": preset,
                         "exit_variant": "risk_managed", **extra})
    return jobs


def order_by_priority(jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Stable sort into channel-priority order.

    The original index is carried as `_idx` and is what --report-suffix uses,
    so a job's report filename does not change if the priority order ever
    does. Reordering must not silently re-point an existing report.
    """
    indexed = [{**j, "_idx": i} for i, j in enumerate(jobs)]
    return sorted(indexed, key=lambda j: CHANNEL_PRIORITY.get(j.get("channel"), 99))


# --------------------------------------------------------------------------
# durable progress
# --------------------------------------------------------------------------

class Progress:
    """Append-only job log + a rewritten status snapshot.

    The two files serve different readers: progress.jsonl is the durable
    record --resume trusts (append-only, one line per finished job, fsynced,
    so a torn write can lose at most the last line); status.json is the
    live view the terminal renders (rewritten wholesale, disposable).
    """

    def __init__(self, suffix: str):
        self.dir = PROGRESS_ROOT / suffix
        self.dir.mkdir(parents=True, exist_ok=True)
        self.jsonl = self.dir / "progress.jsonl"
        self.status = self.dir / "status.json"
        self.suffix = suffix

    def records(self) -> List[Dict[str, Any]]:
        if not self.jsonl.exists():
            return []
        out = []
        for line in self.jsonl.read_text().splitlines():
            if not line.strip():
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue  # torn last line from a hard kill; ignore it
        return out

    def completed_indices(self) -> Dict[int, str]:
        """Every recorded outcome, keyed by job index — the run's ledger.

        Last write wins, so a job killed and later re-run reads as whatever
        it finished as. Do NOT use this to decide what --resume skips; see
        succeeded_indices.
        """
        return {r["idx"]: r.get("outcome", "unknown") for r in self.records()}

    def succeeded_indices(self) -> set:
        """Job indices --resume may skip: the ones that actually succeeded.

        Observed live on 2026-08-14: resume skipped anything with a recorded
        line at all, so the momentum job the memory governor killed was
        dropped from the run entirely and its report never written. A memory
        event must cost time, never silently delete a strategy from the
        results -- the deletion is invisible, and the missing strategy would
        simply be absent from the comparison nobody was told was incomplete.

        Failures are retried on resume for the same reason: a job that died
        because the box was under pressure deserves another attempt, and one
        that fails deterministically will fail again visibly and cheaply.
        """
        return {r["idx"] for r in self.records() if r.get("outcome") == "ok"}

    def durations_by_channel(self) -> Dict[str, List[float]]:
        """Observed per-job durations, grouped by channel.

        Grouped, not pooled, because the channels are not remotely comparable
        in cost -- a 17-year momentum job and a fundamental preset differ by
        more than an order of magnitude, so one global mean would give an ETA
        that is wrong in a direction that changes as the queue drains. Read
        from the durable log rather than kept in memory so a --resume run
        inherits the timings the previous session measured.
        """
        by: Dict[str, List[float]] = {}
        for r in self.records():
            if r.get("outcome") == "ok" and r.get("duration_s"):
                by.setdefault(r.get("channel", "?"), []).append(float(r["duration_s"]))
        return by

    def record(self, rec: Dict[str, Any]) -> None:
        with self.jsonl.open("a") as fh:
            fh.write(json.dumps(rec) + "\n")
            fh.flush()
            os.fsync(fh.fileno())

    def write_status(self, payload: Dict[str, Any]) -> None:
        tmp = self.status.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, indent=1))
        tmp.replace(self.status)  # atomic; a reader never sees a half file


# --------------------------------------------------------------------------
# the supervisor
# --------------------------------------------------------------------------

class Supervisor:
    def __init__(self, jobs, suffix, max_workers, soft_floor_mb, hard_floor_mb, resume):
        self.jobs = order_by_priority(jobs)
        self.suffix = suffix
        self.max_workers = max_workers
        self.soft_floor = soft_floor_mb
        self.hard_floor = hard_floor_mb
        self.progress = Progress(suffix)
        self.started_at = time.time()
        self.mem_floor_seen = available_mb() or 0.0
        self.kills = 0

        # Only successes are skipped. A killed or failed job is retried, and
        # keeps its place in channel-priority order (see succeeded_indices).
        done = self.progress.succeeded_indices() if resume else set()
        self.pending = [j for j in self.jobs if j["_idx"] not in done]
        self.done_count = len(done)
        self.total = len(self.jobs)
        self.running: Dict[int, Dict[str, Any]] = {}  # _idx -> {proc, job, t0}

    # -- memory ------------------------------------------------------------

    def _rss_mb(self, proc: subprocess.Popen) -> float:
        """RSS of the job process and its children, in MB.

        Children matter: run_orchestrator_backtest spawns panel workers, and
        judging a job by the parent alone would systematically understate the
        biggest consumers -- the ones this governor exists to catch.
        """
        try:
            p = psutil.Process(proc.pid)
            total = p.memory_info().rss
            for c in p.children(recursive=True):
                try:
                    total += c.memory_info().rss
                except psutil.Error:
                    pass
            return total / 1024 / 1024
        except psutil.Error:
            return 0.0

    def _requeue(self, job: Dict[str, Any]) -> None:
        """Put a killed job back at its priority position, not at the end.

        Appending was the obvious thing and it was wrong: observed live on
        2026-08-14, a killed momentum job landed behind all 177 technical and
        fundamental jobs, so the highest-priority channel would have finished
        last. The whole point of the ordering is that momentum results arrive
        first, and a memory event must not silently invert that.

        It goes AFTER the pending jobs of its own channel rather than at the
        very front, so a job that is killed repeatedly cannot starve its
        siblings by being retried ahead of them every time.
        """
        rank = CHANNEL_PRIORITY.get(job.get("channel"), 99)
        pos = len(self.pending)
        for i, other in enumerate(self.pending):
            if CHANNEL_PRIORITY.get(other.get("channel"), 99) > rank:
                pos = i
                break
        self.pending.insert(pos, job)

    def _enforce_memory(self) -> None:
        avail = available_mb()
        if avail is None:
            return
        self.mem_floor_seen = min(self.mem_floor_seen, avail)
        if avail >= self.hard_floor or not self.running:
            return

        # Below the hard floor: kill the single largest job and requeue it.
        # (see _requeue for where it lands -- NOT the back of the queue)
        # One victim per poll, deliberately -- killing several at once would
        # usually be an overreaction to a transient spike.
        idx, entry = max(self.running.items(), key=lambda kv: self._rss_mb(kv[1]["proc"]))
        rss = self._rss_mb(entry["proc"])
        print(f"\n[MEM] {avail:.0f}MB available < {self.hard_floor}MB hard floor — "
              f"killing job {idx} ({entry['job'].get('channel')}, RSS {rss:.0f}MB) and requeueing it")
        entry["proc"].terminate()
        try:
            entry["proc"].wait(timeout=30)
        except subprocess.TimeoutExpired:
            entry["proc"].kill()
        self.progress.record({
            "idx": idx, "outcome": "killed_oom_guard", "channel": entry["job"].get("channel"),
            "rss_mb": round(rss, 1), "available_mb": round(avail, 1),
            "at": datetime.now().isoformat(timespec="seconds"),
        })
        del self.running[idx]
        self._requeue(entry["job"])
        self.kills += 1
        if self.max_workers > 1:
            # Concurrency is the variable we actually control. Having proven
            # this workload does not fit at the current width, do not keep
            # re-proving it for the remaining jobs.
            print(f"[MEM] reducing concurrency {self.max_workers} -> 1 for the rest of the run")
            self.max_workers = 1

    # -- status ------------------------------------------------------------

    def _eta_s(self) -> Optional[int]:
        """Remaining wall-clock, estimated per channel and divided by width.

        Cost per job is estimated from the durations this run has actually
        observed for that channel; a channel with nothing finished yet
        borrows the mean of whatever has (better than omitting it, which
        would quietly report an ETA for part of the queue as if it were the
        whole). Returns None until at least one job has completed -- a
        fabricated number early on is worse than an honest "--", because the
        operator uses this to decide whether to leave it running overnight.
        """
        by = self.progress.durations_by_channel()
        if not by:
            return None
        overall = sum(sum(v) for v in by.values()) / sum(len(v) for v in by.values())
        means = {ch: sum(v) / len(v) for ch, v in by.items()}

        pending_cost = sum(means.get(j.get("channel"), overall) for j in self.pending)
        # A running job has already served part of its expected cost; count
        # only what is left of it, floored at zero for jobs running long.
        running_cost = sum(
            max(0.0, means.get(e["job"].get("channel"), overall) - (time.time() - e["t0"]))
            for e in self.running.values()
        )
        width = max(1, min(self.max_workers, len(self.pending) + len(self.running)))
        return round((pending_cost + running_cost) / width)

    def _snapshot(self, state: str) -> Dict[str, Any]:
        elapsed = time.time() - self.started_at
        finished = self.done_count
        remaining = self.total - finished - len(self.running)
        return {
            "suffix": self.suffix, "state": state,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "total": self.total, "completed": finished,
            "running": len(self.running), "pending": remaining,
            "kills": self.kills, "max_workers": self.max_workers,
            "elapsed_s": round(elapsed),
            "eta_s": self._eta_s(),
            "available_mb": round(available_mb() or 0),
            "min_available_mb_seen": round(self.mem_floor_seen),
            "soft_floor_mb": self.soft_floor, "hard_floor_mb": self.hard_floor,
            "jobs_running": [
                {"idx": i, "channel": e["job"].get("channel"),
                 "name": e["job"].get("template_name") or e["job"].get("preset")
                         or f"lb{e['job'].get('lookback_months')}",
                 "filtered": e["job"].get("min_adtv_cr") is not None,
                 "rss_mb": round(self._rss_mb(e["proc"])),
                 "elapsed_s": round(time.time() - e["t0"])}
                for i, e in sorted(self.running.items())
            ],
        }

    # -- published results -------------------------------------------------

    def publish_results(self) -> None:
        """Rewrite results.txt: every strategy finished so far, with its CAGR.

        Read from each job's own report JSON rather than kept in memory, so
        the file is reconstructible after a crash and never depends on this
        process having been alive when a job finished. Written atomically --
        the operator is expected to be tailing it while it is being rewritten.

        Rows are grouped by channel in priority order and sorted by CAGR
        descending, because the question this file answers is "what is
        winning so far", not "what ran first".
        """
        reports = REPO / "backtest" / "reports"
        rows = []
        for rec in self.progress.records():
            if rec.get("outcome") != "ok":
                continue
            f = reports / f"orchestrator_{self.suffix}_job{rec['idx']}.json"
            if not f.exists():
                continue
            try:
                d = json.loads(f.read_text())
            except json.JSONDecodeError:
                continue
            m = d.get("metrics") or {}
            rows.append({
                "channel": rec.get("channel", "?"), "name": rec.get("name", "?"),
                "arm": "filtered" if rec.get("filtered") else "control",
                "cagr": m.get("cagr"), "dd": m.get("max_drawdown"),
                "sharpe": m.get("sharpe"), "trades": m.get("n_trades"),
                "mins": round((rec.get("duration_s") or 0) / 60, 1),
            })

        s = self._snapshot("publishing")
        eta = f"{s['eta_s'] // 60}m" if s.get("eta_s") else "--"
        out = [
            f"AlphaLens backtest results — {self.suffix}",
            f"updated {datetime.now().isoformat(timespec='seconds')}   "
            f"{s['completed']}/{s['total']} jobs   eta {eta}   "
            f"memory {s['available_mb']}MB (low-water {s['min_available_mb_seen']}MB)",
            "",
        ]
        for ch in sorted({r["channel"] for r in rows}, key=lambda c: CHANNEL_PRIORITY.get(c, 99)):
            group = [r for r in rows if r["channel"] == ch]
            traded = [r for r in group if (r["trades"] or 0) > 0]
            out += [
                f"== {ch.upper()}  ({len(group)} finished, {len(traded)} traded) ",
                f"  {'strategy':<30} {'arm':<9} {'CAGR':>8} {'maxDD':>8} {'Sharpe':>7} {'trades':>7} {'mins':>6}",
            ]
            for r in sorted(group, key=lambda r: (r["cagr"] is None, -(r["cagr"] or 0))):
                # A strategy that took no trades has no CAGR to report; "--"
                # says that, where 0.00% would read as a real flat result.
                cagr = f"{100 * r['cagr']:7.2f}%" if r["cagr"] is not None else "      --"
                dd = f"{100 * r['dd']:7.2f}%" if r["dd"] is not None else "      --"
                sharpe = f"{r['sharpe']:7.2f}" if r["sharpe"] is not None else "     --"
                out.append(f"  {r['name']:<30} {r['arm']:<9} {cagr} {dd} {sharpe} "
                           f"{r['trades'] if r['trades'] is not None else '--':>7} {r['mins']:>6}")
            out.append("")

        path = self.progress.dir / "results.txt"
        tmp = path.with_suffix(".txt.tmp")
        tmp.write_text("\n".join(out))
        tmp.replace(path)

    # -- main loop ---------------------------------------------------------

    def _launch(self, job: Dict[str, Any]) -> None:
        idx = job["_idx"]
        clean = {k: v for k, v in job.items() if k != "_idx"}
        cmd = _job_to_cmd(clean, idx, self.suffix)
        log = self.progress.dir / f"job{idx}.log"
        proc = subprocess.Popen(
            cmd, cwd=str(REPO), stdout=log.open("w"), stderr=subprocess.STDOUT,
            env={**os.environ, "PYTHONPATH": str(REPO),
                 # One BLAS thread per worker: measured +62% throughput on
                 # this box, because oversubscribed threads fight for the
                 # same cores AND each carries its own arena.
                 "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1", "MKL_NUM_THREADS": "1"},
            start_new_session=True,
        )
        self.running[idx] = {"proc": proc, "job": job, "t0": time.time()}

    def _reap(self) -> None:
        for idx in list(self.running):
            entry = self.running[idx]
            rc = entry["proc"].poll()
            if rc is None:
                continue
            self.progress.record({
                "idx": idx, "outcome": "ok" if rc == 0 else f"failed_rc{rc}",
                "channel": entry["job"].get("channel"),
                "name": (entry["job"].get("template_name") or entry["job"].get("preset")
                         or f"lb{entry['job'].get('lookback_months')}"),
                "filtered": entry["job"].get("min_adtv_cr") is not None,
                "duration_s": round(time.time() - entry["t0"]),
                "at": datetime.now().isoformat(timespec="seconds"),
            })
            del self.running[idx]
            self.done_count += 1

    def run(self) -> int:
        print(f"{self.total} jobs ({len(self.pending)} to run, {self.done_count} already done)")
        print(f"priority: momentum -> technical -> fundamental | workers={self.max_workers} "
              f"| floors: soft {self.soft_floor}MB / hard {self.hard_floor}MB")
        print(f"progress: {self.progress.dir}")

        stopping = False
        last_publish = 0.0

        def _stop(_signum, _frame):
            nonlocal stopping
            stopping = True
            print("\n[SIG] stopping: no new launches; waiting for running jobs")

        signal.signal(signal.SIGINT, _stop)
        signal.signal(signal.SIGTERM, _stop)

        while (self.pending or self.running) and not (stopping and not self.running):
            self._reap()
            self._enforce_memory()
            avail = available_mb() or 0
            while (not stopping and self.pending and len(self.running) < self.max_workers
                   and avail >= self.soft_floor):
                self._launch(self.pending.pop(0))
                avail = available_mb() or 0
            self.progress.write_status(self._snapshot("stopping" if stopping else "running"))
            # Republished on a time budget rather than every poll: it reopens
            # every finished job's report, which is cheap at 10 jobs and not
            # at 300, and the operator does not need CAGRs refreshed at 5s.
            if time.time() - last_publish >= RESULTS_PUBLISH_INTERVAL_S:
                self.publish_results()
                last_publish = time.time()
            time.sleep(POLL_INTERVAL_S)

        self._reap()
        self.publish_results()
        self.progress.write_status(self._snapshot("stopped" if stopping else "finished"))
        outcomes = self.progress.completed_indices()
        failed = [i for i, o in outcomes.items() if o != "ok"]
        print(f"\ndone: {len(outcomes)}/{self.total} recorded, {len(failed)} not ok, "
              f"{self.kills} memory kills, min available {self.mem_floor_seen:.0f}MB")
        return 1 if failed else 0


# --------------------------------------------------------------------------
# terminal status
# --------------------------------------------------------------------------

def render_status(suffix: str, watch: bool) -> int:
    p = Progress(suffix)
    if not p.status.exists():
        print(f"no status yet at {p.status}")
        return 1
    while True:
        try:
            s = json.loads(p.status.read_text())
        except (json.JSONDecodeError, FileNotFoundError):
            time.sleep(1)
            continue
        done, total = s["completed"], s["total"]
        pct = 100 * done / total if total else 0
        bar = "#" * int(pct / 2.5) + "." * (40 - int(pct / 2.5))
        eta = f"{s['eta_s'] // 60}m" if s.get("eta_s") else "--"
        out = [
            f"\n  {s['suffix']}  [{s['state']}]  {s['updated_at']}",
            f"  [{bar}] {done}/{total}  {pct:5.1f}%   elapsed {s['elapsed_s'] // 60}m  eta {eta}",
            f"  memory: {s['available_mb']}MB available  (low-water {s['min_available_mb_seen']}MB, "
            f"floors {s['soft_floor_mb']}/{s['hard_floor_mb']})",
            f"  workers {s['max_workers']}  running {s['running']}  pending {s['pending']}  "
            f"memory-kills {s['kills']}",
        ]
        for j in s["jobs_running"]:
            out.append(f"    - job{j['idx']:<4} {j['channel']:<12} {str(j['name']):<28} "
                       f"{'filtered' if j['filtered'] else 'control ':<9} "
                       f"RSS {j['rss_mb']:>6}MB  {j['elapsed_s'] // 60}m{j['elapsed_s'] % 60:02d}s")
        text = "\n".join(out)
        if not watch:
            print(text)
            return 0
        print("\033[2J\033[H" + text, flush=True)
        if s["state"] in ("finished", "stopped"):
            return 0
        time.sleep(5)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--build-queue", metavar="PATH", help="write the full 3-channel queue and exit")
    ap.add_argument("--start-date", default="2009-04-01")
    ap.add_argument("--end-date", default="2026-06-30")
    ap.add_argument("--queue-file")
    ap.add_argument("--report-suffix")
    ap.add_argument("--max-workers", type=int, default=2)
    ap.add_argument("--soft-floor-mb", type=float, default=DEFAULT_SOFT_FLOOR_MB)
    ap.add_argument("--hard-floor-mb", type=float, default=DEFAULT_HARD_FLOOR_MB)
    ap.add_argument("--no-resume", action="store_true")
    ap.add_argument("--status", metavar="SUFFIX", help="render progress for a run and exit")
    ap.add_argument("--watch", action="store_true", help="with --status, refresh every 5s")
    args = ap.parse_args()

    if args.status:
        return render_status(args.status, args.watch)

    if args.build_queue:
        jobs = build_queue(args.start_date, args.end_date)
        Path(args.build_queue).write_text(json.dumps({"jobs": jobs}, indent=1))
        counts: Dict[str, int] = {}
        for j in jobs:
            counts[j["channel"]] = counts.get(j["channel"], 0) + 1
        print(f"{len(jobs)} jobs -> {args.build_queue}  {counts}")
        return 0

    if not args.queue_file or not args.report_suffix:
        ap.error("--queue-file and --report-suffix are required to run")

    jobs = json.loads(Path(args.queue_file).read_text())["jobs"]
    return Supervisor(
        jobs, args.report_suffix, args.max_workers,
        args.soft_floor_mb, args.hard_floor_mb, resume=not args.no_resume,
    ).run()


if __name__ == "__main__":
    sys.exit(main())
