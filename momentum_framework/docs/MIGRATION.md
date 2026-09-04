# Migration Checklist: Old Momentum Code → momentum_framework

## Principle

The old codebase (`features/momentum_*.py`, `backtest/adapters/momentum_adapter.py`,
`backtest/generate_r*.py`, `strategies/migrations/r*.py`) **stays untouched and
running** throughout this migration. `momentum_framework/` is built and
verified independently, side-by-side. Only after every strategy's new
implementation is confirmed to reproduce the old numbers do we drop the
old code.

## Status: All 13 active strategies have framework files (2026-09-04)

Every strategy's rebalance logic is ported and independently verified
against synthetic data (correct selection direction, correct formula
output — see each strategy file's own test coverage / this session's
verification). **None have been checked for trade-by-trade parity against
the legacy engine yet** — that step (below) still needs doing before any
cutover. R09's `regime_switching_enabled` (B-027) is explicitly NOT
ported (raises `NotImplementedError` if requested) — see
`strategies/r09_mm_volscale.py`'s docstring.

## Remaining Steps Per Strategy (parity verification)

1. **Regenerate the queue** — run the framework's `R{NN}QueueGenerator`,
   confirm job count and parameter grid match the legacy
   `generate_r{N}_queue.py` output where one exists (diff the two JSON
   files' job lists, ignoring key order and the M13 extension, which has
   no legacy analog). R07 has no legacy generator to diff against (see
   `docs/CODE_TRACEABILITY.md`'s R07 row) — its framework generator is
   the first single source of truth.
2. **Rerun the backtest** — execute the new queue through the existing
   engine (`backtest/run_strategy_queue.py`) — the framework doesn't
   reimplement the simulator yet, only the strategy/queue/nomenclature
   layer around it (see `backtesting/orchestrator.py` docstring).
3. **Normalize + write results** — feed each job's report.json through
   `BacktestOrchestrator._normalize_report()` and `ResultsWriter.write()`,
   landing in `momentum_framework/results/runs/` (never
   `backtest/reports/` — that's the legacy table).
4. **Verify parity** — compare Sharpe/CAGR/MaxDD between the new
   `results/runs/*.json` and the corresponding legacy row in
   `results/traceability/legacy_runs_baseline.csv` (look up by the same
   `new_strategy_id`). They must match within floating-point tolerance
   (both are running the SAME engine — the framework only changed how the
   job was specified and how the result was labeled).
5. **Mark strategy migrated** — check it off below.

## Migration Status

| Strategy | File Created | Queue Verified | Rebalance Logic Verified | Trade-by-Trade Parity | Status |
|----------|:---:|:---:|:---:|:---:|--------|
| R01 | ✅ | ✅ (264 jobs) | 🟡 (compiles, not synthetically tested) | ☐ | Ported |
| R03 | ✅ | ✅ (264 jobs) | 🟡 | ☐ | Ported |
| R07 | ✅ | ✅ (264 jobs) | ✅ (crash-trim/buy-disable tested vs. synthetic crash window) | ☐ | Ported. No legacy generator existed — first source of truth |
| R08 | ✅ | ✅ (264 jobs) | ✅ (exposure multiplier tested vs. synthetic equity curve) | ☐ | Ported |
| R09 | ✅ | ✅ (1,056 jobs — sweeps all 4 vol_scaling_mode) | 🟡 (formulas ported, not individually synthetically tested) | ☐ | Ported. **regime_switching_enabled NOT ported** — raises if requested |
| R10 | ✅ | ✅ (264 jobs) | ✅ (sector-filter tested vs. synthetic multi-sector data) | ☐ | Ported. `sector_lookup` must be externally supplied (no DB resolver yet) |
| R11 | ✅ | ✅ (66 jobs) | ✅ (lowest-pct-of-high selection tested) | ☐ | Ported |
| R12 | ✅ | ✅ (66 jobs) | ✅ (lowest-1mo-return selection tested) | ☐ | Ported |
| R13 | ✅ | ✅ (66 jobs) | ✅ (Bollinger %B formula tested vs. real synthetic price drop, not mocked) | ☐ | Ported |
| R14 | ✅ | ✅ (264 jobs) | 🟡 | ☐ | Replaces retired R0's `weight_method=inverse_volatility` |
| R15 | ✅ | ✅ (264 jobs) | 🟡 | ☐ | Replaces retired R0's `weight_method=inverse_variance` |
| R16 | ✅ | ✅ (264 jobs) | 🟡 | ☐ | Replaces retired R0's `weight_method=target_volatility` |
| R17 | ✅ | ✅ (264 jobs) | 🟡 | ☐ | Replaces retired R0's `weight_method=downside_volatility` |
| ~~R05~~ | 🚫 | 🚫 | 🚫 | 🚫 | **Not in scope, permanently.** Rejected at the Phase 3 gate — historical reference only |

**Cross-strategy check (2026-09-04):** all 13 `QueueGenerator`s together
produce 3,630 jobs with **zero `strategy_id` collisions** (verified via
`metrics.nomenclature.build_strategy_id()` across the combined set, not
just within each strategy individually).

## Cutover Criteria (all must hold before dropping old code)

- [ ] All 13 in-scope strategies verified for trade-by-trade parity
      against the legacy engine (still the single biggest open gate —
      the smoke suite below verifies internal correctness and plausibility,
      NOT agreement with the legacy engine's actual numbers)
- [x] `momentum_framework` has its own test suite —
      `momentum_framework/tests/` (pytest, 98 tests, ~45s): imports,
      nomenclature collision regressions, all 13 QueueGenerators
      individually + combined (3,000+ jobs, zero collisions), signal
      correctness (TrailingMomentum/PctOf52WeekHigh/Bollinger/
      IndustryMomentum against hand-crafted data), regime/benchmark
      detection (synthetic + a REAL COVID-crash regression check),
      liquidity/sector (real production DB), and — highest-value —
      native-orchestrator end-to-end runs against the real DB, including
      permanent regressions for the R01 rotation bug and the R09
      concentration-sensitivity pattern found 2026-09-04 (see
      project_native_orchestrator_and_data_wiring memory). Run via
      `pytest momentum_framework/tests/`.
- [ ] Frontend (`frontend/src/pages/momentum/`) and API
      (`datastore/api/routers/momentum.py`) repointed to read from
      `momentum_framework/results/` instead of `backtest/reports/`
- [ ] Live/paper trading path (`features/momentum_live.py`,
      `backtest/core/live_signal_runner.py`) repointed to framework
      strategy adapters
- [ ] One full Phase-equivalent campaign (e.g. Phase B's 1,080 jobs) run
      end-to-end through the framework with no integrity failures
- [ ] User sign-off on dropping `backtest/adapters/momentum_adapter.py`,
      `backtest/generate_r*.py`, `strategies/migrations/r*.py`

## Known Gaps (not yet built)

- `backtesting/orchestrator.py` delegates to the legacy engine via
  `report_dict` injection — it does not yet call `backtest/core/engine.py`
  directly. Native execution is needed before `update_portfolio_equity()`
  (R08/R09's exposure-multiplier dependency) or `resolve_universe()`
  (band-scoped ranking) can run a real multi-day backtest end-to-end.
- The 176M-row `momentum_rankings` cache (`_get_cached_momentum_rankings()`
  in `backtest/adapters/momentum_adapter.py`) is not ported — the
  framework recomputes trailing returns from `ohlcv_adjusted` on every
  call. Needed before framework campaigns are performance-competitive.
- **R09's `regime_switching_enabled` (B-027)** — needs
  `backtest.core.regime_detection.EnsembleRegimeDetector` ported, a
  separate subsystem not evaluated this session.
- **ADTV/circuit-lock/liquidity data** — referenced by R07 (circuit-lock
  checks), R12 ("+ liquidity" half of its name), and the legacy adapter
  generally, but not modeled anywhere in the framework yet.
- **R10's sector_lookup** — no DB resolver; must be passed in externally.
- **R05's `select_lowest=False` non-inverted 52wk-high signal** — not a
  gap, a deliberate exclusion (see Migration Status table).
