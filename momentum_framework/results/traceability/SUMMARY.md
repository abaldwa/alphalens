# Results Traceability — Summary

**Generated from:** /home/amit/projects/AlphaLens/backtest/reports
**Total report files scanned:** 6307
**Reports with rank_method recorded (processed):** 2220
**Skipped (no rank_method — not a momentum config report):** 4087

## Derived strategy_code distribution (CORRECTED, not legacy strategy_key)

| Strategy Code | Report Count |
|---|---:|
| R01 | 380 |
| UNKNOWN_needs_review_vol_scaling_no_regime_switch | 312 |
| R07 | 271 |
| R08 | 237 |
| R13 | 229 |
| R10 | 228 |
| R11 | 217 |
| R05_rejected_phase3 | 189 |
| R12 | 39 |
| R14 | 24 |
| R15 | 24 |
| R16 | 24 |
| R17 | 24 |
| R03 | 11 |
| UNKNOWN_rank_method_equal_weight | 7 |
| UNKNOWN_rank_method_risk_adjusted_composite | 4 |

## Derived filter_preset distribution

| Filter Preset | Report Count |
|---|---:|
| all_risk | 2220 |

## ⚠️ filter_preset coverage caveat

All 2220 processed reports derive to `filter_preset=all_risk` — none show balanced/risk_managed/max_defensive filters. This does NOT mean those presets were never run: the ORIGINAL M-family sweep (the earliest runs, 2026-08-19, `mom_all_risk_b1_...` style strategy_ids) predates the `rank_method` field this script filters on, so those reports are among the 4087 SKIPPED files, not represented in this baseline at all. If reproducing filter_preset=balanced/risk_managed/max_defensive results, this baseline has no comparison point — treat as a first-time run.

## Flags requiring human review

- **323** reports have an ambiguous/unresolvable `derived_strategy_code` (see `UNKNOWN_*` rows in the CSV) — the prior R05-vs-R11 `pct_of_52wk_high` ambiguity was RESOLVED 2026-09-04 (see derive_strategy_code() docstring); remaining unknowns are rank_method values (equal_weight, risk_adjusted_composite, vol_scaling without regime_switching) not yet mapped.
- **323** reports have a `rank_band_id` outside the framework's known M-bands (2,4,7,9,10,12,13) — likely legacy band numbering (b1-b7) predating the M2/M4/M7/M9/M10/M12 renumbering.

## How to use this baseline

1. Rerun a strategy through momentum_framework (e.g. `R01QueueGenerator`).
2. Compute the SAME `new_strategy_id` for the new run (via `metrics.nomenclature.build_strategy_id()`).
3. Look up that `new_strategy_id` in `legacy_runs_baseline.json` / `.csv`.
4. Compare `sharpe_ratio`/`cagr`/`max_drawdown` — both ran the identical config, so any difference beyond floating-point noise is a real regression to investigate, not expected variance.
5. If NO matching `new_strategy_id` exists in the baseline, that config was never run under the legacy engine (or its band/params fall outside framework bands) — there is nothing to compare against; treat the new run as a first-time result.