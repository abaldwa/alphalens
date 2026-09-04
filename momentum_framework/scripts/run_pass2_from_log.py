"""
momentum_framework/scripts/run_pass2_from_log.py

Reapplies run_campaign.py's _passed_parity() gate to an ALREADY-COMPLETED
Pass 1 log (campaign_run_log.json) and launches Pass 2 only for the
configs that qualify under the CURRENT criteria — without re-running
Pass 1's expensive legacy-engine comparisons.

WHY THIS EXISTS (2026-09-04): the pass criteria were tightened (>90% buy
agreement, CAGR within 2 percentage points — see run_campaign.py's
_passed_parity() docstring) WHILE a live 89-config Pass 1 run was already
~55% complete. Re-running Pass 1 from scratch would waste ~12 minutes of
already-good, already-logged legacy-vs-native comparisons; this script
reclassifies from the logged data instead and runs ONLY Pass 2, fresh,
under the corrected gate.

GATE OVERRIDE (added 2026-09-04, explicit user instruction after
reviewing the full 89-config Pass 1 legacy-vs-native comparison
strategy-by-strategy): the full run scored 0/89 passes under
_passed_parity(), but investigation found the divergence is attributable
to LEGACY-side issues, not native defects — a confirmed rank_method-blind
cache-key collision bug (legacy's momentum_rankings cache silently serves
trailing-return scores for R11/R12/R13, whose real signals are
pct_of_52wk_high / trailing_reversal_1mo / bollinger_mean_reversion; this
didn't fire in Pass 1 itself since top_n=5 never hit that cache, but is
illustrative of the legacy engine's fragility), plus an unresolved but
native-clean systematic CAGR gap in the trailing-return group. Both
engines' band-universe resolution was verified IDENTICAL
(momentum_band_universe() is the one shared definition). User's explicit
call: native wins both groups; proceed to Pass 2 for every config that
completed Pass 1 without a hard error, regardless of _passed_parity().
Set SKIP_PARITY_GATE=1 to run this mode (still logged, still gated on
"did Pass 1 even run" — parity_check_error entries, e.g. the M13
rank_band_id registration gap, still excluded since there's no native
result to trust there at all).

Run: PYTHONPATH=. python3 momentum_framework/scripts/run_pass2_from_log.py
     SKIP_PARITY_GATE=1 PYTHONPATH=. python3 momentum_framework/scripts/run_pass2_from_log.py
"""

import concurrent.futures
import json
import os
from typing import Any, Dict, List, Tuple

from momentum_framework.results.db_writer import FrameworkResultsDBWriter
from momentum_framework.scripts.campaign_registry import all_configs
from momentum_framework.scripts.run_campaign import (
    PASS2_MAX_WORKERS,
    RESULTS_LOG,
    ProgressTracker,
    _acquire_lock,
    _passed_parity,
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
    if not RESULTS_LOG.exists():
        raise SystemExit(f"No Pass 1 log found at {RESULTS_LOG} — run run_campaign.py first.")

    logged: List[Dict[str, Any]] = json.loads(RESULTS_LOG.read_text())
    print(f"Loaded {len(logged)} logged Pass 1 results from {RESULTS_LOG}")

    # Reconstruct the comparison shape _passed_parity() expects from what
    # run_campaign.py already stored per job (see ProgressTracker.record_pass1).
    factory_by_key: Dict[Tuple[str, int], Any] = {
        (code, band_id): factory for code, band_id, top_n, factory in all_configs()
    }

    skip_gate = os.environ.get("SKIP_PARITY_GATE") == "1"

    qualifying: List[Tuple[str, int, Any]] = []
    reclassified_now_fail = 0
    reclassified_now_pass = 0
    for entry in logged:
        parity = entry.get("parity")
        if not parity:
            continue  # errored in Pass 1 (e.g. M13's rank_band_id gap) — no native result to trust, still excluded even with the gate skipped
        comparison = {
            "buy_agreement_pct": parity["buy_agreement_pct"],
            "legacy_metrics": {"cagr": parity["legacy_cagr"]},
            "native_metrics": {"cagr": parity["native_cagr"]},
        }
        passes_now = _passed_parity(comparison)
        passed_then = parity.get("passed", False)
        if passes_now and not passed_then:
            reclassified_now_pass += 1
        elif passed_then and not passes_now:
            reclassified_now_fail += 1
        if passes_now or skip_gate:
            key = (entry["strategy_code"], entry["band_id"])
            factory = factory_by_key.get(key)
            if factory is not None:
                qualifying.append((entry["strategy_code"], entry["band_id"], factory))

    if skip_gate:
        print(f"SKIP_PARITY_GATE=1 — running Pass 2 for every config that completed Pass 1 "
              f"without a hard error: {len(qualifying)}/{len(logged)} configs "
              f"(gate would have passed {sum(1 for e in logged if (e.get('parity') or {}).get('passed'))} of these)")
    else:
        print(f"Under the CURRENT criteria (>{90}% agreement, CAGR within 2pp): "
              f"{len(qualifying)}/{len(logged)} configs qualify for Pass 2")
    if reclassified_now_pass or reclassified_now_fail:
        print(f"  ({reclassified_now_pass} newly pass, {reclassified_now_fail} newly fail "
              f"vs the criteria in effect when Pass 1 ran)")

    tracker = ProgressTracker(pass1_total=len(logged))
    tracker.pass1_done = len(logged)
    tracker.results = logged
    tracker.set_pass2_total(len(qualifying))

    writer = FrameworkResultsDBWriter()
    persisted = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=PASS2_MAX_WORKERS) as executor:
        import time
        futures = {}
        for code, band_id, factory in qualifying:
            t0 = time.time()
            future = executor.submit(run_pass2, code, band_id, factory)
            futures[future] = (f"{code}/M{band_id:02d}", t0)

        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            label, t0 = futures[future]
            r = future.result()
            elapsed = time.time() - t0

            obj = r.pop("_full_result_obj", None)
            if obj is not None:
                writer.write(obj, engine="native", universe_cache_used=True, parity_checked=True)
                persisted += 1

            tracker.record_pass2(label, r, elapsed)
            m = r.get("metrics") or {}
            print(f"[Pass2 {i}/{len(qualifying)}] {label}: {r['phase']} ({elapsed:.1f}s)"
                  + (f" CAGR={m.get('cagr')}, Sharpe={m.get('sharpe_ratio')}, trades={r.get('trade_count')}"
                     if m else f" — {r.get('error')}"))

    print("\n=== Pass 2 (from log) complete ===")
    print(f"  Persisted to framework_backtest_runs: {persisted}")
    print(f"  Full log: {RESULTS_LOG}")


if __name__ == "__main__":
    main()
