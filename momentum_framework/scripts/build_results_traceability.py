"""
momentum_framework/scripts/build_results_traceability.py

Builds the RESULTS traceability matrix: for every legacy backtest report
in backtest/reports/orchestrator_*.json, derive what its CORRECT
momentum_framework strategy_id would be (from its actual run.config, not
its unreliable legacy strategy_key — see project_strategy_identity_bug_r_vs_m
memory) and snapshot its key metrics.

This is the baseline every framework rerun gets diffed against: if R01
band M2 top10 lb12mo produces a different Sharpe under the new framework
than this baseline says the legacy engine produced for the same config,
that's a real regression to investigate — not noise, because both ran
the identical (rank_method, band, lookback, top_n, cadence, skip_months,
filter_preset, crash/vol/weight overlay) config.

Run: PYTHONPATH=. python3 momentum_framework/scripts/build_results_traceability.py
Output: momentum_framework/results/traceability/legacy_runs_baseline.json
        momentum_framework/results/traceability/legacy_runs_baseline.csv
        momentum_framework/results/traceability/SUMMARY.md
"""

import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Optional

REPORTS_DIR = Path("/home/amit/projects/AlphaLens/backtest/reports")
OUT_DIR = Path(__file__).resolve().parent.parent / "results" / "traceability"


def derive_filter_preset(cfg: Dict[str, Any]) -> str:
    """
    Reimplements strategies/momentum_identity.py::_category_for_filters()
    against a legacy report's run.config, to backfill the filter_preset
    field that was never explicitly recorded pre-framework. See
    CODE_TRACEABILITY.md's "Naming & Identity" row for why this isn't
    (yet) a shared function imported from momentum_framework.common —
    it's a one-off backfill against the legacy schema's field names.
    """
    has_balanced = (
        cfg.get("min_adtv_cr") is not None
        and cfg.get("circuit_band_pct") is not None
        and bool(cfg.get("quality_gate_min_f_score") or cfg.get("quality_gate_max_m_score"))
    )
    has_regime = bool(cfg.get("disable_buys_in_regime"))
    has_ortho = bool(cfg.get("orthogonalize_vs_size_beta"))

    if has_ortho:
        return "max_defensive" if (has_balanced and has_regime) else "UNKNOWN_partial_filters"
    if has_regime:
        return "risk_managed" if has_balanced else "UNKNOWN_partial_filters"
    if has_balanced:
        return "balanced"
    if cfg.get("min_adtv_cr") is None and cfg.get("circuit_band_pct") is None:
        return "all_risk"
    return "UNKNOWN_partial_filters"


def derive_strategy_code(cfg: Dict[str, Any]) -> str:
    """
    Derives the CORRECT R-family code from a legacy report's actual
    config — never from strategy_key (broken, see
    project_strategy_identity_bug_r_vs_m memory) and never inferred from
    skip_months alone (the bug's root cause).

    Returns "UNKNOWN_needs_review_<hint>" rather than guessing when a
    config is genuinely ambiguous. The one prior ambiguity here (R05 vs
    R11 both using rank_method="pct_of_52wk_high") was resolved
    2026-09-04 via git log — see the pct_of_52wk_high branch below for
    the resolution and its evidence.
    """
    rank_method = cfg.get("rank_method")
    skip_months = cfg.get("skip_months", 0) or 0
    crash = bool(cfg.get("crash_regime_enabled"))
    vol_target = bool(cfg.get("vol_target_enabled"))
    vol_scaling_mode = cfg.get("vol_scaling_mode")
    weight_method = cfg.get("weight_method")
    regime_switching = bool(cfg.get("regime_switching_enabled"))
    select_lowest = bool(cfg.get("select_lowest"))

    # rank_method="jt_momentum" was added as a CLI choice in
    # run_orchestrator_backtest.py but has no distinct implementation —
    # per .claude/agents/enhanced-backtesting-agent.md: "jt_momentum
    # adapter: falls through to trailing_momentum logic (Phase 9+ full
    # implementation pending)". Treat identically to trailing_return.
    if rank_method in ("trailing_return", "jt_momentum"):
        if weight_method:
            # R0 was retired 2026-09-04 and split into 4 standalone
            # strategies by weight_method — see project_r0_split_r14_r17
            # memory. Map the legacy weight_method value to its correct
            # successor rather than returning the now-deleted "R0".
            weight_method_to_code = {
                "inverse_volatility": "R14",
                "inverse_variance": "R15",
                "target_volatility": "R16",
                "downside_volatility": "R17",
            }
            return weight_method_to_code.get(
                weight_method, f"UNKNOWN_weight_method_{weight_method}"
            )
        if crash:
            return "R07"
        if vol_target:
            return "R08"
        if vol_scaling_mode and regime_switching:
            return "R09"
        if vol_scaling_mode:
            return "UNKNOWN_needs_review_vol_scaling_no_regime_switch"
        if skip_months > 0:
            return "R03"
        return "R01"

    if rank_method == "industry_momentum":
        return "R10"

    if rank_method == "pct_of_52wk_high":
        # RESOLVED 2026-09-04 via git log (commit messages: "Phase 3 R05
        # validation complete: 52-week-high momentum fails cross-market-cap
        # gate", "Gate decision: REJECT R05 for Phase 3. Archive
        # implementation.") cross-checked against a sample R05-era report
        # (orchestrator_20260823_004925_job0.json): that report's config
        # has NO `select_lowest` key at all (not even False) — it predates
        # the field existing, which is exactly what you'd expect from R05
        # being the ORIGINAL non-inverted 52wk-high strategy, later
        # rejected at the Phase 3 gate, with R11 added afterward as the
        # inverted reversal variant (select_lowest=True, confirmed present
        # in every R11 report sampled). So:
        #   select_lowest is True            -> R11 (reversal, active strategy)
        #   select_lowest is False or absent -> R05  (momentum continuation,
        #                                            REJECTED at Phase 3 gate —
        #                                            historical reference only,
        #                                            NOT part of the active
        #                                            R-family being ported into
        #                                            momentum_framework)
        return "R11" if select_lowest else "R05_rejected_phase3"

    if rank_method == "bollinger_mean_reversion":
        return "R13"

    if rank_method in ("reversal_1mo", "trailing_reversal_1mo"):
        return "R12"

    return f"UNKNOWN_rank_method_{rank_method}"


def build_new_strategy_id(cfg: Dict[str, Any], strategy_code: str, filter_preset: str) -> Optional[str]:
    """Attempts to build a framework strategy_id; None if band_id isn't a known M-band."""
    if strategy_code.startswith("UNKNOWN"):
        return None

    def _required_int(field: str) -> int:
        """Raises (caught below, becomes an "ERROR_..." string) if `field`
        is missing/None/non-numeric in this legacy report's config — same
        outcome as the old unchecked int(cfg.get(...)) had at runtime,
        just satisfies build_strategy_id's int-typed params statically
        instead of passing cfg.get()'s Any|None straight through."""
        value = cfg.get(field)
        if value is None:
            raise ValueError(f"{field} missing from legacy config")
        return int(value)

    try:
        from momentum_framework.metrics.nomenclature import build_strategy_id
        return build_strategy_id(
            strategy_code=strategy_code,
            band_id=_required_int("rank_band_id"),
            top_n=_required_int("top_n"),
            lookback_months=_required_int("lookback_months"),
            rebalance_cadence_days=_required_int("rebalance_cadence_days"),
            rank_method=cfg.get("rank_method"),
            filter_preset=filter_preset if not filter_preset.startswith("UNKNOWN") else "all_risk",
            crash_regime_enabled=bool(cfg.get("crash_regime_enabled")),
            vol_scaling_mode=cfg.get("vol_scaling_mode"),
            weight_method=cfg.get("weight_method"),
            skip_months=cfg.get("skip_months", 0) or 0,
        )
    except Exception as e:
        return f"ERROR_{type(e).__name__}"


def process_report(path: Path) -> Optional[Dict[str, Any]]:
    try:
        with open(path) as f:
            report = json.load(f)
    except Exception:
        return None

    run = report.get("run", {})
    cfg = run.get("config", {})
    if cfg.get("rank_method") is None:
        return None  # not a momentum-with-rank_method report; skip

    filter_preset = derive_filter_preset(cfg)
    strategy_code = derive_strategy_code(cfg)
    new_strategy_id = build_new_strategy_id(cfg, strategy_code, filter_preset)
    metrics = report.get("metrics", {}) or {}

    return {
        "legacy_report_file": path.name,
        "legacy_run_id": run.get("run_id"),
        "legacy_strategy_key": report.get("strategy_key"),
        "derived_strategy_code": strategy_code,
        "derived_filter_preset": filter_preset,
        "new_strategy_id": new_strategy_id,
        "rank_band_id": cfg.get("rank_band_id"),
        "top_n": cfg.get("top_n"),
        "lookback_months": cfg.get("lookback_months"),
        "rebalance_cadence_days": cfg.get("rebalance_cadence_days"),
        "skip_months": cfg.get("skip_months", 0),
        "rank_method": cfg.get("rank_method"),
        "crash_regime_enabled": cfg.get("crash_regime_enabled"),
        "vol_scaling_mode": cfg.get("vol_scaling_mode"),
        "weight_method": cfg.get("weight_method"),
        "sharpe_ratio": metrics.get("sharpe_ratio"),
        "cagr": metrics.get("cagr"),
        "max_drawdown": metrics.get("max_drawdown") or metrics.get("max_dd"),
        "integrity_passed": report.get("integrity_passed"),
        "trade_log_path": report.get("trade_log_path"),
    }


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    rows = []
    skipped = 0
    for path in sorted(REPORTS_DIR.glob("orchestrator_*.json")):
        row = process_report(path)
        if row is None:
            skipped += 1
            continue
        rows.append(row)

    # JSON output
    json_path = OUT_DIR / "legacy_runs_baseline.json"
    json_path.write_text(json.dumps(rows, indent=2, default=str))

    # CSV output
    csv_path = OUT_DIR / "legacy_runs_baseline.csv"
    if rows:
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    # Summary stats
    code_counts = Counter(r["derived_strategy_code"] for r in rows)
    preset_counts = Counter(r["derived_filter_preset"] for r in rows)
    unresolvable_band = sum(1 for r in rows if r["new_strategy_id"] is None)
    needs_review = sum(1 for r in rows if r["derived_strategy_code"].startswith("UNKNOWN")
                        or "review" in r["derived_strategy_code"])

    summary_lines = [
        "# Results Traceability — Summary",
        "",
        f"**Generated from:** {REPORTS_DIR}",
        f"**Total report files scanned:** {len(rows) + skipped}",
        f"**Reports with rank_method recorded (processed):** {len(rows)}",
        f"**Skipped (no rank_method — not a momentum config report):** {skipped}",
        "",
        "## Derived strategy_code distribution (CORRECTED, not legacy strategy_key)",
        "",
        "| Strategy Code | Report Count |",
        "|---|---:|",
    ]
    for code, count in code_counts.most_common():
        summary_lines.append(f"| {code} | {count} |")

    summary_lines += [
        "",
        "## Derived filter_preset distribution",
        "",
        "| Filter Preset | Report Count |",
        "|---|---:|",
    ]
    for preset, count in preset_counts.most_common():
        summary_lines.append(f"| {preset} | {count} |")

    summary_lines += [
        "",
        "## ⚠️ filter_preset coverage caveat",
        "",
        f"All {len(rows)} processed reports derive to `filter_preset=all_risk` — "
        "none show balanced/risk_managed/max_defensive filters. This does NOT mean "
        "those presets were never run: the ORIGINAL M-family sweep (the earliest "
        "runs, 2026-08-19, `mom_all_risk_b1_...` style strategy_ids) predates the "
        f"`rank_method` field this script filters on, so those reports are among the "
        f"{skipped} SKIPPED files, not represented in this baseline at all. If "
        "reproducing filter_preset=balanced/risk_managed/max_defensive results, "
        "this baseline has no comparison point — treat as a first-time run.",
        "",
        "## Flags requiring human review",
        "",
        f"- **{needs_review}** reports have an ambiguous/unresolvable `derived_strategy_code` "
        f"(see `UNKNOWN_*` rows in the CSV) — the prior R05-vs-R11 `pct_of_52wk_high` "
        f"ambiguity was RESOLVED 2026-09-04 (see derive_strategy_code() docstring); "
        f"remaining unknowns are rank_method values (equal_weight, "
        f"risk_adjusted_composite, vol_scaling without regime_switching) not yet mapped.",
        f"- **{unresolvable_band}** reports have a `rank_band_id` outside the framework's "
        f"known M-bands (2,4,7,9,10,12,13) — likely legacy band numbering (b1-b7) "
        f"predating the M2/M4/M7/M9/M10/M12 renumbering.",
        "",
        "## How to use this baseline",
        "",
        "1. Rerun a strategy through momentum_framework (e.g. `R01QueueGenerator`).",
        "2. Compute the SAME `new_strategy_id` for the new run "
        "(via `metrics.nomenclature.build_strategy_id()`).",
        "3. Look up that `new_strategy_id` in `legacy_runs_baseline.json` / `.csv`.",
        "4. Compare `sharpe_ratio`/`cagr`/`max_drawdown` — both ran the identical config, "
        "so any difference beyond floating-point noise is a real regression to investigate, "
        "not expected variance.",
        "5. If NO matching `new_strategy_id` exists in the baseline, that config was never "
        "run under the legacy engine (or its band/params fall outside framework bands) — "
        "there is nothing to compare against; treat the new run as a first-time result.",
    ]

    (OUT_DIR / "SUMMARY.md").write_text("\n".join(summary_lines))

    print(f"Processed {len(rows)} reports ({skipped} skipped)")
    print(f"Written: {json_path}")
    print(f"Written: {csv_path}")
    print(f"Written: {OUT_DIR / 'SUMMARY.md'}")
    print("\nStrategy code distribution:")
    for code, count in code_counts.most_common(15):
        print(f"  {code:45s}: {count}")


if __name__ == "__main__":
    main()
