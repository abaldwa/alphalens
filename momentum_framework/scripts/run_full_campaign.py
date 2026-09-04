"""
momentum_framework/scripts/run_full_campaign.py

Native-only full-grid run (2026-09-04, explicit user instruction: "I am
looking at full run" + position sizing as a dimension for every
strategy). Runs EVERY config from campaign_registry.py::all_configs()
(1,114 jobs after the position-sizing/full-lookback-grid expansion)
through the native engine's full-history backtest (FULL_START/FULL_END
from run_campaign.py, currently 2009-01-01 to 2026-06-30) — no legacy
Pass 1 parity check.

WHY NO PARITY GATE HERE: Pass 1 (89 representative configs, 2009-2011
window) already established — reviewed strategy-by-strategy — that
native is the more trustworthy engine (a confirmed legacy-side
rank_method-blind cache bug for R11/R12/R13, and no native-side defect
found for the trailing-return group's CAGR gap; both engines verified to
share the identical band-universe resolution function). Re-running the
slow legacy engine 1,114 times to re-confirm a conclusion already reached
would be pure waste. User's explicit call: native wins, proceed straight
to full evaluation.

Reuses run_campaign.py's ProgressTracker (incremental per-job writes to
campaign_progress.json / campaign_run_log.json — same crash-safety
guarantee as the two-pass runs) and run_pass2() (the native full-history
BacktestOrchestrator.run_native() call), plus the PID lock file so this
can never collide with another campaign process.

Run: PYTHONPATH=. python3 momentum_framework/scripts/run_full_campaign.py
"""

import concurrent.futures
import time
from typing import Any, Dict

from momentum_framework.results.db_writer import FrameworkResultsDBWriter
from momentum_framework.scripts.campaign_registry import all_configs
from momentum_framework.scripts.run_campaign import (
    PASS2_MAX_WORKERS,
    PROGRESS_LOG,
    RESULTS_LOG,
    ProgressTracker,
    _acquire_lock,
    _release_lock,
    run_pass2,
)


def main() -> None:
    _acquire_lock()
    try:
        _main()
    finally:
        _release_lock()


def _main() -> None:
    configs = all_configs()
    print(f"Full campaign (native-only, no parity gate): {len(configs)} configs")
    print(f"Live progress: {PROGRESS_LOG}\nIncremental results: {RESULTS_LOG}\n")

    # pass1_total=0 — this run has no Pass 1 phase; ProgressTracker's
    # pass1/pass2 split is repurposed here with all work reported under
    # "pass2" (the field name that already maps to run_pass2()'s cost
    # profile — a full-history native backtest — matching what this
    # script actually runs).
    #
    # IMPORTANT: record_pass2() only UPDATES an existing tracker.results
    # entry matched by label (r["pass2"] = result) — it never APPENDS one.
    # The two-pass scripts get away with this because record_pass1()
    # already appended every entry first. This script has no Pass 1 phase,
    # so every entry must be pre-seeded here or every single result would
    # silently vanish from campaign_run_log.json (caught via a smoke test
    # before this ran for real, not assumed).
    # Unique label per config (2026-09-04 fix): the full grid puts MANY
    # configs under one (strategy_code, band_id) — different lookback/
    # cadence/position_sizing combinations — so a label must encode all of
    # those or ProgressTracker.record_pass2()'s label-matching silently
    # collapses every config sharing a band onto one results-log slot (see
    # run_campaign.py::run_pass2's docstring for the mechanism). factory()
    # is called here ONLY to read back the real constructed params for the
    # label string; the actual run later calls factory() again for a truly
    # fresh instance, per the project's own "never reuse a strategy
    # instance across runs" rule (see project_windowed_backtest_analysis
    # memory) — cheap to double-construct since __init__ does no I/O.
    def _label_for(code: str, band_id: int, top_n: int, factory: Any) -> str:
        peek = factory()
        lb = getattr(peek, "lookback_months", "na")
        cadence = getattr(peek, "rebalance_cadence_days", "na")
        sizing = getattr(peek, "position_sizing", "equal")
        return f"{code}/M{band_id:02d}_top{top_n}_lb{lb}_{cadence}d_{sizing}"

    labels = [_label_for(code, band_id, top_n, factory) for code, band_id, top_n, factory in configs]
    assert len(set(labels)) == len(labels), "label collision — some configs share an identical label"

    tracker = ProgressTracker(pass1_total=0)
    tracker.results = [
        {"label": label, "strategy_code": code, "band_id": band_id, "top_n": top_n}
        for label, (code, band_id, top_n, _factory) in zip(labels, configs)
    ]
    tracker.set_pass2_total(len(configs))

    writer = FrameworkResultsDBWriter()
    persisted = 0
    errors = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=PASS2_MAX_WORKERS) as executor:
        futures = {}
        for label, (code, band_id, top_n, factory) in zip(labels, configs):
            t0 = time.time()
            future = executor.submit(run_pass2, code, band_id, factory, label)
            futures[future] = (label, t0)

        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            label, t0 = futures[future]
            r: Dict[str, Any] = future.result()
            elapsed = time.time() - t0

            obj = r.pop("_full_result_obj", None)
            if obj is not None:
                writer.write(obj, engine="native", universe_cache_used=True, parity_checked=False)
                persisted += 1
            if r.get("phase") == "full_run_error":
                errors += 1

            tracker.record_pass2(label, r, elapsed)
            m = r.get("metrics") or {}
            print(f"[{i}/{len(configs)}] {label}: {r['phase']} ({elapsed:.1f}s)"
                  + (f" CAGR={m.get('cagr')}, Sharpe={m.get('sharpe_ratio')}, trades={r.get('trade_count')}"
                     if m else f" — {r.get('error')}"))

    print("\n=== Full campaign complete ===")
    print(f"  Persisted to framework_backtest_runs: {persisted}")
    print(f"  Errors: {errors}")
    print(f"  Full log: {RESULTS_LOG}")


if __name__ == "__main__":
    main()
