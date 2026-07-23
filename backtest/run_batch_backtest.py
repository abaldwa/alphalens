"""
backtest/run_batch_backtest.py

Owner: Platform / Backtest
Consumers: operator CLI (`python3 -m backtest.run_batch_backtest`)

Super-control for running multiple backtests (Phase 1/2/3, and any
future phase scripts) "in one go" without the operator having to invoke
each `run_phaseN_backtest.py` by hand and babysit memory.

Design
------
Each phase runs as its own subprocess (`python -m backtest.run_phaseN_
backtest ...`), never in-process and never concurrently:

- Isolated process, not a thread/coroutine, because BacktestEngine's
  walk-forward loop trains fresh sklearn/lightgbm/Optuna models per fold
  — large, fragmented numpy/pandas allocations that Python's own
  allocator does not reliably hand back to the OS even after `del` +
  `gc.collect()`. A subprocess's memory is guaranteed back the moment it
  exits; nothing else is.
- Sequential, not parallel, because every phase script already loads a
  full real OHLCV panel (backtest/run_phase1_backtest.py's
  `_fetch_real_universe`) into memory independently — running two at
  once means two full panels plus two Optuna studies resident
  simultaneously, which is exactly the OOM this control exists to avoid.

Sequencing
----------
Phases run in ascending order of known memory footprint, not the order
the user listed them in: Phase 1 (one Signal5D variant) is lightest,
Phase 3 (two variants: Signal5D baseline + Signal21D) is next, Phase 2
(multibagger model training on top of two variants: Signal5D baseline +
Signal63D+watchlist) is heaviest. Running light-to-heavy means a memory
problem shows up on the cheapest job first rather than after burning
time on the expensive ones, and leaves the most headroom for the
heaviest job (run last, with every earlier subprocess's memory already
fully reclaimed).

OOM guard
---------
Before starting each subprocess, checks system-available memory via
ingestion.scheduler.resource_guard (already used by the ingestion
scheduler for the same purpose — reused rather than reimplemented).
If available memory is below `--min-free-mb`, the runner waits and
re-checks (a previous subprocess's OS-level cleanup can lag slightly
behind process exit) up to `--wait-timeout-s` before aborting the batch
rather than launching a job likely to be OOM-killed. This is a start-of
-job gate, not an in-job throttle — once a phase subprocess is running,
its own memory behavior is that script's concern.
"""

import argparse
import json
import logging
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from backtest.batch_common import wait_for_headroom
from ingestion.scheduler.resource_guard import current_rss_mb

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

REPORTS_DIR = Path(__file__).resolve().parent / "reports"

# Ascending memory-footprint order (see module docstring) — NOT the
# order the user names phases in; --phases only selects the subset to
# run, this fixed order decides sequencing.
_PHASE_ORDER = ["1", "3", "2"]

_PHASE_MODULES = {
    "1": "backtest.run_phase1_backtest",
    "2": "backtest.run_phase2_backtest",
    "3": "backtest.run_phase3_backtest",
}

# Flags each phase script actually accepts (they don't share one CLI
# schema — phase1/phase2 use --max-real-tickers, phase3 uses
# --max-tickers, and only phase1 has --check-only/--min-history-days
# under that exact name).
_PHASE_ARG_MAP = {
    "1": {"max_tickers": "--max-real-tickers", "min_history": "--min-history-days"},
    "2": {"max_tickers": "--max-real-tickers", "min_history": "--min-history-days"},
    "3": {"max_tickers": "--max-tickers", "min_history": "--min-history"},
}


def _run_phase(phase: str, common_args: Dict[str, Optional[int]], extra_args: List[str]) -> Dict:
    module = _PHASE_MODULES[phase]
    flag_names = _PHASE_ARG_MAP[phase]
    cmd = [sys.executable, "-m", module]
    if common_args.get("folds") is not None:
        cmd += ["--folds", str(common_args["folds"])]
    if common_args.get("trials") is not None:
        cmd += ["--trials", str(common_args["trials"])]
    if common_args.get("max_tickers") is not None:
        cmd += [flag_names["max_tickers"], str(common_args["max_tickers"])]
    if common_args.get("min_history") is not None:
        cmd += [flag_names["min_history"], str(common_args["min_history"])]
    cmd += extra_args

    logger.info(f"run_batch_backtest: launching phase {phase} — {' '.join(cmd)}")
    started = time.monotonic()
    # A fresh child process per phase — this is the actual OOM guard
    # (module docstring): whatever BacktestEngine/Optuna allocated is
    # returned to the OS the instant this subprocess exits, regardless
    # of what Python's own allocator would have kept resident in-process.
    proc = subprocess.run(cmd, capture_output=False)
    elapsed_s = time.monotonic() - started
    logger.info(f"run_batch_backtest: phase {phase} exited {proc.returncode} in {elapsed_s:.0f}s")
    return {"phase": phase, "returncode": proc.returncode, "elapsed_s": elapsed_s}


def run_batch(
    phases: List[str],
    folds: Optional[int] = None,
    trials: Optional[int] = None,
    max_tickers: Optional[int] = None,
    min_history: Optional[int] = None,
    min_free_mb: float = 2048.0,
    wait_timeout_s: float = 600.0,
) -> Dict:
    unknown = [p for p in phases if p not in _PHASE_MODULES]
    if unknown:
        raise ValueError(f"unknown phase(s): {unknown} — valid phases are {sorted(_PHASE_MODULES)}")

    ordered = [p for p in _PHASE_ORDER if p in phases]
    common_args = {"folds": folds, "trials": trials, "max_tickers": max_tickers, "min_history": min_history}

    results = []
    for phase in ordered:
        wait_for_headroom(min_free_mb, wait_timeout_s, label="run_batch_backtest")
        result = _run_phase(phase, common_args, extra_args=[])
        results.append(result)
        if result["returncode"] != 0:
            logger.error(f"run_batch_backtest: phase {phase} failed (exit {result['returncode']}) — stopping batch")
            break

    summary = {
        "generated_at": datetime.now().isoformat(),
        "requested_phases": phases,
        "execution_order": ordered,
        "results": results,
        "all_passed": all(r["returncode"] == 0 for r in results) and len(results) == len(ordered),
    }
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    summary_path = REPORTS_DIR / f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(summary_path, "w") as fh:
        json.dump(summary, fh, indent=2, default=str)
    logger.info(f"run_batch_backtest: batch summary written to {summary_path}")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Super-control: run multiple backtest phases in one go, sequenced light-to-heavy "
            "and gated on free system memory to avoid OOM (see module docstring for why)."
        )
    )
    parser.add_argument(
        "--phases", nargs="+", default=["1", "2", "3"], choices=["1", "2", "3"],
        help="Which phases to run (default: all three). Actual execution order is always light-to-heavy (1, 3, 2).",
    )
    parser.add_argument("--folds", type=int, default=None)
    parser.add_argument("--trials", type=int, default=None)
    parser.add_argument("--max-tickers", type=int, default=None, help="Forwarded to each phase's ticker cap")
    parser.add_argument("--min-history", type=int, default=None, help="Forwarded to each phase's min-history-days")
    parser.add_argument(
        "--min-free-mb", type=float, default=2048.0,
        help="Required free system memory before launching each phase subprocess (default 2048MB)",
    )
    parser.add_argument(
        "--wait-timeout-s", type=float, default=600.0,
        help="Max seconds to wait for free memory to clear --min-free-mb before aborting the batch",
    )
    args = parser.parse_args()

    logger.info(f"run_batch_backtest: this process RSS at start: {current_rss_mb():.0f}MB")
    summary = run_batch(
        phases=args.phases, folds=args.folds, trials=args.trials, max_tickers=args.max_tickers,
        min_history=args.min_history, min_free_mb=args.min_free_mb, wait_timeout_s=args.wait_timeout_s,
    )
    print(json.dumps(summary, indent=2, default=str))
    sys.exit(0 if summary["all_passed"] else 1)


if __name__ == "__main__":
    main()
