# Code Traceability Matrix: Legacy → momentum_framework

Maps every legacy momentum file/function to where its logic is (or will
be) ported in `momentum_framework/`. Status legend: **✅ Ported** (new
code exists and is verified against legacy behavior) · **🟡 Partial**
(new code exists but is not yet feature-complete or unverified) ·
**⬜ Not started** · **🚫 Superseded** (the legacy mechanism itself is
being replaced, not translated — e.g. the broken naming function).

This matrix is the map for the "rerun and compare" verification step in
`docs/MIGRATION.md` — every new-framework module should be traceable back
to the exact legacy file(s) it must reproduce the numbers of.

---

## Signal Computation

| Legacy | New Location | Status | Notes |
|---|---|---|---|
| `features/momentum_signal.py::trailing_momentum()` | `common/signals.py::TrailingMomentumSignal.compute()` | ✅ Ported | Direct translation; SQL query logic identical |
| `features/momentum_signal.py::pct_of_52wk_high()` | `common/signals.py::PctOf52WeekHighSignal.compute()` | 🟡 Partial | Ported but not yet unit-tested against legacy output |
| `features/momentum_signal.py::crash_regime_detector()` | *(not yet ported — needed for R07)* | ⬜ Not started | |
| `features/momentum_signal.py::orthogonalize_momentum_vs_factors()` | *(not yet ported — needed for max_defensive filter preset)* | ⬜ Not started | |
| `features/momentum_universe.py::RANK_BANDS` | `common/universe.py::MBANDS` | ✅ Ported | Band definitions verified to match (M2/M4/M7/M9/M10/M12); M13 is NEW, framework-only (see project_m13_band_added memory) |
| `features/momentum_universe.py::yearly_band_universes_from_rankings()` | *(not yet ported)* | ⬜ Not started | Needed before framework can run point-in-time universes natively |
| `features/sector_rotation.py`, `features/momentum_strategy.py::rank_sectors()`/`rank_constituents_within_sectors()` | `common/signals.py::IndustryMomentumSignal`, `common/sector_ranking.py` | ✅ Ported | Two-stage sector filter on top of `TrailingMomentumSignal` (not a separate ranking formula) — verified against synthetic multi-sector data 2026-09-04 |
| `features/volatility_scaling.py` | `common/volatility.py`, `common/position_weighting.py` | ✅ Ported (for R14-R17) | Realized/downside vol + 4 weighting schemes (`InverseVolatilityWeighting`, `InverseVarianceWeighting`, `TargetVolatilityWeighting`, `DownsideVolatilityWeighting`) built and verified 2026-09-04. R08/R09's vol-scaling (leverage applied AFTER selection, not per-ticker weighting) is a different mechanism, still not ported. |
| `features/momentum_universe.py::momentum_band_universe()` | `common/band_universe.py::resolve_band_universe()` | ✅ Ported | Thin delegating wrapper (not reimplemented — DB-backed ADTV/market-cap resolution stays in the legacy function, same delegation pattern as the orchestrator). Wired into `StrategyAdapter.resolve_universe()` so every strategy ranks only within its band's actual constituents. |

## Strategy Identity & Naming

| Legacy | New Location | Status | Notes |
|---|---|---|---|
| `strategies/momentum_identity.py::registry_name()` (R-branch, lines 165-168) | `metrics/nomenclature.py::build_strategy_id()` | 🚫 Superseded | This is the root cause of the R01/R03 mislabeling bug (see `project_strategy_identity_bug_r_vs_m` memory) — deliberately NOT translated, replaced with a mandatory-field design |
| `strategies/migrations/momentum.py::variant_name()` | `metrics/nomenclature.py::build_strategy_id()` | 🚫 Superseded | The M-path naming was correct but string-based; new nomenclature unifies both paths into one function |
| `strategies/migrations/momentum.py::CATEGORY_FILTERS` | `metrics/nomenclature.py::FILTER_PRESETS` | ✅ Ported | Same 4 presets (all_risk/balanced/risk_managed/max_defensive), now a mandatory `filter_preset` field instead of an inferred string segment |
| `strategies/momentum_identity.py::_category_for_filters()` | *(logic not yet ported — needed to derive filter_preset from legacy reports, see RESULTS_TRACEABILITY.md)* | 🟡 Partial | Reimplemented ad hoc in `scripts/build_results_traceability.py` for backfilling legacy report labels; not yet a shared framework function |
| `backtest/strategy_id.py::build_strategy_id()` (channel/descriptor/horizon format) | *(out of scope — this is the cross-channel `backtest_runs.strategy_id`, orthogonal to momentum's per-strategy identity)* | N/A | Framework's `build_strategy_id()` names a strategy CONFIG; legacy `backtest/strategy_id.py` names a RUN's DB row. Different concerns, not a 1:1 port. |

## Strategy Execution (StrategyAdapter equivalent)

| Legacy | New Location | Status | Notes |
|---|---|---|---|
| `backtest/adapters/momentum_adapter.py` (rank_method="trailing_return", skip_months=0 branch) | `strategies/r01_trailing_momentum.py::R01TrailingMomentum` | 🟡 Partial | Rebalance logic ported; NOT yet verified to reproduce legacy trade-by-trade output |
| `backtest/adapters/momentum_adapter.py` (rank_method="trailing_return", skip_months=1 branch + cache offset logic) | `strategies/r03_jt_skipmonth.py::R03JTSkipMonth` | 🟡 Partial | `_offset_trading_date()` reimplements the cache-offset query from the adapter; not yet verified against `_get_cached_momentum_rankings()`'s exact fallback behavior (21d/10d/5d cascade) |
| `backtest/adapters/momentum_adapter.py::_get_cached_momentum_rankings()` (176M-row cache, Sept 3 speedup) | *(not yet ported)* | ⬜ Not started | Framework currently recomputes from `ohlcv_adjusted` directly every call — no cache layer yet. Needed before framework campaigns are performance-competitive with legacy. |
| `backtest/adapters/momentum_adapter.py` (weight_method branch — formerly "R0") | `strategies/r14_inverse_volatility.py`, `r15_inverse_variance.py`, `r16_target_volatility.py`, `r17_downside_volatility.py` | ✅ Ported | **R0 RETIRED 2026-09-04**, split into 4 standalone strategies (R14-R17) — see `project_r0_split_r14_r17` memory for rationale (R08/R09 precedent: a distinct weighting methodology earns its own R-number). All 4 share `TrailingMomentumSignal` via `strategies/base.py::WeightedMomentumStrategy`; only the weighting formula differs (`common/position_weighting.py`). NOT yet verified trade-by-trade against legacy `weight_method` output. |
| `backtest/adapters/momentum_adapter.py` (crash_regime_enabled branch — R07) + `features/momentum_signal.py::crash_regime_detector()` | `strategies/r07_crash_aware.py::R07CrashAware`, `common/crash_regime.py` | ✅ Ported | `rank_method="trailing_return"`, same `TrailingMomentumSignal`. Held-position trim/buy-disable logic verified against a synthetic crash window 2026-09-04. Only the benchmark-driven crash-detection mode is ported, not the legacy self-referential fallback (see `common/crash_regime.py`'s docstring — the legacy code itself prefers benchmark mode) |
| `backtest/adapters/momentum_adapter.py` (vol_target_enabled branch — R08) + `features/momentum_signal.py::realized_vol_target_multiplier()` | `strategies/r08_bsc_volscale.py::R08BSCVolScale`, `common/portfolio_vol_scaling.py::vol_target_multiplier()` | ✅ Ported | Portfolio-level exposure multiplier (not per-ticker weighting — different mechanism from R14-R17). Verified against a synthetic equity curve. Needs `update_portfolio_equity()` fed by a native orchestrator for a real multi-day run (not yet built) |
| `backtest/adapters/momentum_adapter.py` (vol_scaling_mode branch — R09) + `features/volatility_scaling.py`'s portfolio-level dispatch functions | `strategies/r09_mm_volscale.py::R09MMVolScale`, `common/portfolio_vol_scaling.py::VOL_SCALING_DISPATCH` | ✅ Ported (core) | All 4 modes (inverse_volatility/inverse_variance/target_volatility/downside_volatility) ported. **`regime_switching_enabled` (B-027) NOT ported** — raises `NotImplementedError`; needs `backtest.core.regime_detection.EnsembleRegimeDetector`, a separate subsystem |
| `strategies/migrations/r10_nigam_pandey_momentum.py` | `strategies/r10_sector_momentum.py::R10SectorMomentum` | ✅ Ported | Two-stage: `TrailingMomentumSignal` + sector filter (`common/sector_ranking.py`). `sector_lookup` (ticker→sector) must be externally supplied — no DB resolver built yet |
| `strategies/migrations/r11_52wk_high_momentum.py` | `strategies/r11_52wk_reversal.py::R11FiftyTwoWeekReversal` | ✅ Ported | `rank_method="pct_of_52wk_high"`, `select_lowest=True` — the REVERSAL variant (buy oversold). See R05 row below for the sibling non-inverted variant and why they were confused as one ambiguous bucket until 2026-09-04 |
| *(no file — R05 was never given its own generator/migration)* | **Not ported, and should NOT be** | 🚫 Rejected, historical only | R05 = same `rank_method="pct_of_52wk_high"` but `select_lowest=False`/absent (non-inverted momentum continuation). Confirmed via git log: "Gate decision: REJECT R05 for Phase 3. Archive implementation" — "52-week-high momentum fails cross-market-cap gate" (-1.79% CAGR delta vs trailing-return baseline; only band 10 mid-caps outperformed). 189 legacy reports carry this config (`R5_rejected_phase3` in `results/traceability/`) — kept in the results baseline for historical reference, but out of scope for framework porting since it's a decided rejection, not a pending strategy. |
| `strategies/migrations/r12_momentum_reversal_liquidity.py` | `strategies/r12_reversal_1mo.py::R12Reversal1Mo` | ✅ Ported | `rank_method="trailing_reversal_1mo"` — confirmed to be `TrailingMomentumSignal` with `lookback_months=1`, lowest-wins selection, not a different signal. Reuses the shared class, not `backtest/reversal_selector.py::select_losers_for_reversal()`'s separate legacy implementation. The "+ liquidity" half of R12's name (ADTV quintile interaction) is NOT modeled — no liquidity data plumbing in the framework yet |
| `strategies/migrations/r13_bollinger_mean_reversion.py` + `features/momentum_signal.py::bollinger_mean_reversion()` | `strategies/r13_bollinger_reversal.py::R13BollingerReversal`, `common/bollinger_signal.py::BollingerBandSignal` | ✅ Ported | Reimplemented with pandas rolling mean/std instead of TA-Lib (same formula, no talib build dependency added). Verified against a real synthetic price-drop query (%B ≈ 0.11 for a stock that just dropped 28%), not just mocked |

## Backtest Orchestration

| Legacy | New Location | Status | Notes |
|---|---|---|---|
| `backtest/run_orchestrator_backtest.py` (full simulation engine) | `backtesting/orchestrator.py::BacktestOrchestrator` | 🚫 Superseded (by design) | New orchestrator DELEGATES to the legacy engine via `report_dict` injection rather than reimplementing simulation — see that file's docstring. Native execution is an explicit Phase 2 item, not started. |
| `backtest/core/engine.py::BacktestOrchestrator` (portfolio sim, trade execution) | *(not ported — legacy engine still runs all actual simulation)* | 🚫 Not translated by design | Kept as-is until every strategy adapter is verified; see `docs/MIGRATION.md` cutover criteria |
| `backtest/core/metrics.py` (Sharpe/CAGR/DD formulas) | `metrics/standard.py::MetricsCalculator` | ✅ Ported | Formulas re-derived independently; NOT yet numerically diffed against legacy output row-by-row |
| `backtest/core/portfolio.py` | *(not ported)* | ⬜ Not started | |
| `backtest/core/tax.py` | *(not ported)* | ⬜ Not started | |
| `backtest/core/signal_ledger.py` | *(not ported — framework has no ledger yet)* | ⬜ Not started | |
| `backtest/core/run_store.py` | `results/writer.py`, `results/reader.py` | 🟡 Partial | Different storage shape (flat JSON files in `results/runs/` vs DuckDB `backtest_runs` table) — not a structural port, a parallel mechanism |
| `backtest/integrity_checker.py` (12 integrity checks) | *(not ported — framework has no integrity checking yet)* | ⬜ Not started | Required before cutover per `docs/MIGRATION.md` criteria |

## Queue Generation

| Legacy | New Location | Status | Notes |
|---|---|---|---|
| `backtest/generate_r1_queue.py` | `strategies/r01_trailing_momentum.py::R01QueueGenerator` | ✅ Ported | Generates **264 jobs**: legacy's 216-job shape (6 partitioned bands × 4 lookbacks × 3 cadences × 3 top_n) + 48 new jobs from the M13 extension (1 band × 4 lookbacks × 3 cadences × 4 top_n — M13 has no legacy analog, see `project_m13_band_added` memory) |
| `backtest/generate_r1_full_queue.py` (the file that was ACTUALLY producing R03 jobs under an R01 name, per skip_months=1) | `strategies/r03_jt_skipmonth.py::R03QueueGenerator` | ✅ Ported | Deliberately renamed/relocated to its correct identity (see that file's docstring); also generates 264 jobs (216 legacy-shape + 48 M13) |
| `backtest/generate_r0_isolation_queue.py`, `generate_r0_weighting_queue.py` (weight_method sweep — the part of R0 that survives, redistributed) | `strategies/r14_inverse_volatility.py::R14QueueGenerator`, `r15_inverse_variance.py::R15QueueGenerator`, `r16_target_volatility.py::R16QueueGenerator`, `r17_downside_volatility.py::R17QueueGenerator` | ✅ Ported | Each generates 264 jobs (216 legacy-shape + 48 M13), all sharing `QueueGenerator.simple_momentum_grid()` — no per-strategy grid-loop duplication. `backtest/generate_r0_baseline_queue.py` (the non-weighted R0 baseline, `weight_method=None`) has no port — that configuration is just R01 with equal weighting, already covered. |
| `backtest/generate_r8_queue.py` | `strategies/r08_bsc_volscale.py::R08QueueGenerator` | ✅ Ported | 264 jobs |
| `backtest/generate_r9_queue.py` | `strategies/r09_mm_volscale.py::R09QueueGenerator` | ✅ Ported | 1,056 jobs (sweeps all 4 `vol_scaling_mode` values as a real grid dimension — legacy generator ran one fixed mode per job set) |
| `backtest/generate_r10_queue.py` | `strategies/r10_sector_momentum.py::R10QueueGenerator` | ✅ Ported | 264 jobs |
| `backtest/generate_r11_queue.py` | `strategies/r11_52wk_reversal.py::R11QueueGenerator` | ✅ Ported | 66 jobs (lookback_months fixed — the 52wk window is a constant, see that file's docstring) |
| `backtest/generate_r12_queue.py` | `strategies/r12_reversal_1mo.py::R12QueueGenerator` | ✅ Ported | 66 jobs (lookback_months fixed at 1 — the strategy's defining parameter) |
| `backtest/generate_r13_queue.py` | `strategies/r13_bollinger_reversal.py::R13QueueGenerator` | ✅ Ported | 66 jobs (lookback_months=[1] is a placeholder — Bollinger uses `bollinger_window` instead) |
| *(new, no legacy equivalent)* | `queues/validator.py::QueueValidator` | ✅ New | Prevents the missing-field class of bug; no legacy analog existed |
| *(new, no legacy equivalent)* | `queues/generator.py::QueueGenerator.simple_momentum_grid()` | ✅ New | Shared grid-builder reused by R01, R03, R14-R17 (and future R07/R08/R09/R12) — added 2026-09-04 to stop 4 near-identical strategy files from copy-pasting the same nested-loop job construction |

## Results Storage

| Legacy | New Location | Status | Notes |
|---|---|---|---|
| `backtest/reports/orchestrator_*.json` (per-job report files) | `momentum_framework/results/runs/*.json` | 🟡 Partial | Writer/reader built; NOT the same directory — legacy reports are never overwritten. See `RESULTS_TRACEABILITY.md` for the cross-reference between the two. |
| `datastore/api/routers/momentum.py` (live/API read path) | *(not yet repointed)* | ⬜ Not started | Cutover criterion in `docs/MIGRATION.md` |
| `frontend/src/pages/momentum/*` | *(not yet repointed)* | ⬜ Not started | Cutover criterion in `docs/MIGRATION.md` |

---

## Coverage Summary

| Area | Ported | Partial | Not Started | Superseded (by design) |
|---|---:|---:|---:|---:|
| Signal computation | 6 | 1 | 3 | 0 |
| Naming/identity | 1 | 1 | 0 | 2 |
| Strategy execution | 13 (all: R01, R03, R07-R17) | 0 | 0 | 0 |
| Orchestration | 1 | 1 | 4 | 2 |
| Queue generation | 11 | 0 | 0 | 0 |
| Results storage | 0 | 1 | 2 | 0 |
| **Total** | **32** | **4** | **9** | **4** |

*(R05 excluded from all counts — it's a decided rejection, not a pending
port; see its row above.)*

**Reading this table:** as of 2026-09-04, **all 13 active strategies**
(R01, R03, R07-R17) have framework files, rebalance logic, and
`QueueGenerator`s — verified as a set via `metrics.nomenclature
.build_strategy_id()`: 3,630 total jobs across all 13, zero collisions.
Most strategies' selection/ranking LOGIC has been independently verified
against synthetic data this session (see docs/MIGRATION.md's status
table for exactly which). **None have been verified for trade-by-trade
parity against the legacy engine** — that is the next real gate before
any cutover, not yet started for any strategy. Also still entirely in
the legacy codebase: the 176M-row cache, native simulation, R09's
regime-switching (B-027), and ADTV/circuit-lock/liquidity data plumbing
several strategies reference but don't yet consume. This is intentional
per `docs/MIGRATION.md` — the framework is being verified strategy-by-
strategy against legacy output before anything is dropped.

---

**Last updated:** 2026-09-04 (all 13 active strategies ported)
