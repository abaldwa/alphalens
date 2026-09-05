"""
momentum_framework/scripts/run_m13_topn_sweep.py

M13 top_n sweep (2026-09-05, explicit user instruction): the main
1,114-job campaign (run_full_campaign.py) ran M13 at only top_n=10 (the
smallest value in TOP_N_BY_BAND[13] = [10, 20, 30, 40] — see
campaign_registry.py's own docstring on why top_n was never part of the
"full grid" sweep). M13 is the full 800-stock ADTV universe, large enough
to support wider baskets meaningfully; this script fills in top_n=20/30/40
for EVERY strategy (base 9 signal-strategies' full lookback/cadence/
sizing grid, plus R14-R17's existing single config), WITHOUT touching or
duplicating the already-correct top_n=10 rows.

Reuses campaign_registry.py's BASE_STRATEGIES/WEIGHTED_STRATEGIES specs
directly (not all_configs(), which only ever emits top_n=10 for M13) so
this sweep can never drift from what a real M13 config actually looks
like. Reuses run_campaign.py's ProgressTracker/run_pass2/lock, same as
run_full_campaign.py.

Run: PYTHONPATH=. python3 momentum_framework/scripts/run_m13_topn_sweep.py
"""

import concurrent.futures
import os
import time
from functools import partial
from typing import Any, List, Tuple

from momentum_framework.results.db_writer import FrameworkResultsDBWriter
from momentum_framework.scripts.campaign_registry import (
    BASE_STRATEGIES,
    POSITION_SIZINGS,
    REBALANCE_CADENCES,
    WEIGHTED_STRATEGIES,
)
from momentum_framework.scripts.run_campaign import (
    PASS2_MAX_WORKERS,
    PROGRESS_LOG,
    ProgressTracker,
    _acquire_lock,
    _release_lock,
    run_pass2,
)
from pathlib import Path

M13_BAND_ID = 13
NEW_TOP_NS = [20, 30, 40]  # top_n=10 already exists from the main campaign

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"
M13_RESULTS_LOG = RESULTS_DIR / "m13_topn_sweep_run_log.json"


def _build_m13_configs() -> List[Tuple[str, int, Any, str]]:
    """(strategy_code, top_n, factory, label) tuples for the new M13 top_n values."""
    configs: List[Tuple[str, int, Any, str]] = []

    for spec in BASE_STRATEGIES:
        if M13_BAND_ID not in spec.bands:
            continue
        for top_n in NEW_TOP_NS:
            for lookback_months in spec.lookback_months_grid:
                for cadence in REBALANCE_CADENCES:
                    for sizing in POSITION_SIZINGS:
                        label = f"{spec.strategy_code}/M13_top{top_n}_lb{lookback_months}_{cadence}d_{sizing}"
                        factory = partial(spec.build, M13_BAND_ID, top_n, cadence, sizing, lookback_months)
                        configs.append((spec.strategy_code, top_n, factory, label))

    for code, bands, build in WEIGHTED_STRATEGIES:
        if M13_BAND_ID not in bands:
            continue
        for top_n in NEW_TOP_NS:
            label = f"{code}/M13_top{top_n}"
            weighted_factory = partial(build, M13_BAND_ID, top_n)
            configs.append((code, top_n, weighted_factory, label))

    return configs


def main() -> None:
    _acquire_lock()
    try:
        _main()
    finally:
        _release_lock()


def _already_done_labels() -> set:
    """Labels already persisted to framework_backtest_runs from a prior,
    interrupted run of this sweep (2026-09-05: the first two attempts both
    died mid-run — see this module's git history / conversation — leaving
    partial results behind). Rebuilds each existing row's label from its
    OWN config_json using the identical scheme _build_m13_configs() uses,
    so a resumed run skips exactly what's already there instead of
    duplicating it. Read-only DuckDB query — safe to call before the lock
    is acquired for the write-side of this run."""
    import json

    import duckdb

    from momentum_framework.results.db_writer import PROD_BACKTEST_DB_PATH

    if not PROD_BACKTEST_DB_PATH.exists():
        return set()
    conn = duckdb.connect(str(PROD_BACKTEST_DB_PATH), read_only=True)
    try:
        rows = conn.execute(
            "SELECT strategy_code, config_json FROM framework_backtest_runs WHERE band_id = ?",
            [M13_BAND_ID],
        ).fetchall()
    finally:
        conn.close()

    done = set()
    for code, cfg_json in rows:
        c = json.loads(cfg_json)
        top_n = c.get("top_n")
        if top_n not in NEW_TOP_NS:
            continue  # top_n=10 (main campaign) or unrelated — not this sweep's concern
        if "position_sizing" in c:
            label = f"{code}/M13_top{top_n}_lb{c.get('lookback_months')}_{c.get('rebalance_cadence_days')}d_{c.get('position_sizing')}"
        else:
            label = f"{code}/M13_top{top_n}"  # R14-R17's own-weighting configs
        done.add(label)
    return done


def _main() -> None:
    configs = _build_m13_configs()
    labels = [c[3] for c in configs]
    assert len(set(labels)) == len(labels), "label collision in M13 top_n sweep"

    already_done = _already_done_labels()
    if already_done:
        before = len(configs)
        configs = [c for c in configs if c[3] not in already_done]
        print(f"Resuming: skipping {before - len(configs)} already-persisted configs from a prior interrupted run")

    print(f"M13 top_n sweep (top_n in {NEW_TOP_NS}, every strategy): {len(configs)} configs")
    print(f"Live progress: {PROGRESS_LOG}\nResults log: {M13_RESULTS_LOG}\n")

    tracker = ProgressTracker(pass1_total=0)
    tracker.results = [
        {"label": label, "strategy_code": code, "band_id": M13_BAND_ID, "top_n": top_n}
        for code, top_n, _factory, label in configs
    ]
    tracker.set_pass2_total(len(configs))
    # Redirect this sweep's incremental log to its own file — never overwrite
    # the main campaign's 1,114-row campaign_run_log.json.
    import momentum_framework.scripts.run_campaign as run_campaign_module
    run_campaign_module.RESULTS_LOG = M13_RESULTS_LOG

    writer = FrameworkResultsDBWriter()
    persisted = 0
    errors = 0
    # M13_MAX_WORKERS override (added 2026-09-05 after a silent hard-kill —
    # signature consistent with OOM, not a Python exception — mid-run at
    # top_n=40, the heaviest basket size): wider top_n baskets carry more
    # positions/DB rows per worker than the main campaign's top_n<=15 grid,
    # so 4-way parallelism that was fine there can exceed available memory
    # here. Defaults to PASS2_MAX_WORKERS (4) if unset, matching prior
    # behavior exactly.
    max_workers = int(os.environ.get("M13_MAX_WORKERS", PASS2_MAX_WORKERS))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for code, top_n, factory, label in configs:
            t0 = time.time()
            future = executor.submit(run_pass2, code, M13_BAND_ID, factory, label)
            futures[future] = (label, t0)

        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            label, t0 = futures[future]
            r = future.result()
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

    print("\n=== M13 top_n sweep complete ===")
    print(f"  Persisted to framework_backtest_runs: {persisted}")
    print(f"  Errors: {errors}")
    print(f"  Full log: {M13_RESULTS_LOG}")


if __name__ == "__main__":
    main()
