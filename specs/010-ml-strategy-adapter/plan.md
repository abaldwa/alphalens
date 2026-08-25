# Implementation Plan: ML Strategy Adapter - First-Class Integration

**Branch**: `010-ml-strategy-adapter` | **Date**: 2026-08-25 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/010-ml-strategy-adapter/spec.md`

**Note**: This plan describes the design and implementation strategy for integrating ML signals as a first-class StrategyAdapter, achieving parity with Technical/Momentum/Fundamental adapters in the backtest engine.

## Summary

**Primary Requirement**: Implement a new `MLStrategyAdapter` class that plugs ML models into `backtest/core/engine.py` using the existing `StrategyAdapter` protocol, enabling ML strategies to compete alongside Technical/Momentum/Fundamental strategies in backtests, parameter sweeps, and walk-forward validation.

**Technical Approach**: 
1. Implement `MLStrategyAdapter` in `backtest/adapters/ml_adapter.py` that wraps `backtest/core/ml_signal_engine.py` functions
2. Implement the three required methods: `generate_signals()` (load model, compute predictions, threshold to long/flat), `generate_portfolio()` (apply universe filtering and regime overlay), `execute_trades()` (rebalance logic identical to other adapters)
3. Register ML strategies in `strategy_registry` with domain='ml' and model metadata
4. Test parity: single-run backtest, parameter sweep, walk-forward validation, regime overlay compatibility
5. Ensure signal persistence to `strategy_signals` table for auditability

## Technical Context

**Language/Version**: Python 3.10+

**Primary Dependencies**: 
- `backtest/core/engine.py` (StrategyAdapter protocol)
- `backtest/core/ml_signal_engine.py` (existing model loading, prediction, signal generation)
- `baselines/` (pre-trained ML models: CatBoost, Ridge, etc.)
- `features/` (feature engineering pipelines)
- DuckDB (strategy_signals, strategy_sweep tables)

**Storage**: DuckDB (existing; no schema changes required for this phase)

**Testing**: pytest with existing test patterns (`tests/unit/test_*backtest*.py`)

**Target Platform**: Linux server (backtesting/batch processing; no live trading in this phase)

**Project Type**: Quantitative trading system (backtesting engine + strategy execution)

**Performance Goals**: 
- Single-symbol backtest completes in <5 seconds
- 100-symbol parameter sweep completes in <10 minutes
- Walk-forward validation (10 folds) completes in <2 minutes per strategy

**Constraints**: 
- Must not modify `backtest/core/engine.py` interface (other adapters depend on it)
- Must not alter `strategy_registry` schema
- ML adapter must support regime overlays (EMA-RSI) without special handling

**Scale/Scope**: 
- 1 new adapter class (~500 LOC)
- 1 adapter test file (~200 LOC)
- 3 integration tests (single-run, sweep, walk-forward)
- 4 new strategy registry entries (for different ML models)

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

✅ **Architecture Is Auditable** (Principle II):
- ML adapter MUST register in `strategy_registry` (done via assumption)
- ML signals MUST persist to `strategy_signals` table (FR-009)
- ML strategy trades MUST be traceable to signals (enforced by adapter design)

✅ **Backtest Numbers Are Trustworthy** (Principle III):
- ML adapter participates in identical rebalance cadence as other adapters (SC-002)
- Universe ranking uses point-in-time snapshots (inherited from engine, no adapter changes)
- Regime overlay compatibility is preserved (SC-002, edge case: regime + ML signals)

✅ **Data Integrity Under Concurrency** (Principle IV):
- ML adapter uses existing DuckDB write paths (no new writes outside `defer_db_writes`)
- Tests run in standard batch order (unit → integration → ML-heavy) — ML adapter tests are ML-heavy, run last
- No synthetic rows in real DB for verification (use in-memory test DB)

✅ **Feature Ingestion Is Wholesale** (Principle V):
- ML adapter reuses existing feature pipelines from `features/` (no new wholesale writes)
- No ticker-subset feature ingestion in this phase

✅ **Lazy Efficiency / YAGNI** (Principle I):
- Reuse `ml_signal_engine.py` existing functions, don't rewrite
- Implement only what's required for StrategyAdapter interface (3 methods)
- No over-engineering for future use cases (live trading, paper trading are separate specs)

✅ **Spec-First for Large Initiatives** (Principle VII):
- This feature is large (cross-module adapter integration) and spec-first workflow is being followed

## Project Structure

### Documentation (this feature)

```text
specs/[###-feature]/
├── plan.md              # This file (/speckit-plan command output)
├── research.md          # Phase 0 output (/speckit-plan command)
├── data-model.md        # Phase 1 output (/speckit-plan command)
├── quickstart.md        # Phase 1 output (/speckit-plan command)
├── contracts/           # Phase 1 output (/speckit-plan command)
└── tasks.md             # Phase 2 output (/speckit-tasks command - NOT created by /speckit-plan)
```

### Source Code (repository root)

```text
backtest/
├── core/
│   ├── engine.py           [EXISTING: StrategyAdapter protocol defined here]
│   ├── ml_signal_engine.py [EXISTING: model loading, prediction, signal logic]
│   ├── horizon.py          [EXISTING: shared with adapters]
│   ├── portfolio.py        [EXISTING: shared with adapters]
│   └── regime.py           [EXISTING: regime overlay logic]
├── adapters/
│   ├── technical_adapter.py    [EXISTING: Technical strategy adapter]
│   ├── momentum_adapter.py     [EXISTING: Momentum strategy adapter]
│   ├── fundamental_adapter.py  [EXISTING: Fundamental strategy adapter]
│   └── ml_adapter.py           [NEW: ML strategy adapter (this feature)]
├── run_orchestrator_backtest.py [EXISTING: orchestrator]
├── run_sweep_inprocess.py       [EXISTING: sweep runner]
├── walk_forward/
│   └── runner.py            [EXISTING: walk-forward validation]
└── tests/
    ├── test_backtest_engine.py     [EXISTING]
    ├── test_ml_adapter.py          [NEW: unit + integration tests for ML adapter]
    └── integration/
        └── test_ml_adapter_e2e.py  [NEW: end-to-end parity tests]

baselines/
├── catboost_v1.pkl     [EXISTING: pre-trained model]
└── [other models]

features/
├── momentum_universe.py [EXISTING: feature pipelines]
└── [other feature modules]

datastore/
└── schema/
    └── create_normalised.py [EXISTING: DuckDB tables including strategy_signals]

tests/
├── unit/
│   ├── test_adapters.py    [EXISTING: adapter unit tests]
│   └── test_ml_adapter.py  [NEW: ML adapter unit tests]
└── integration/
    └── test_backtest_parity.py [NEW: parity tests]
```

**Structure Decision**: No structural changes. ML adapter follows the existing adapter pattern:
- Lives in `backtest/adapters/ml_adapter.py` alongside Technical/Momentum/Fundamental adapters
- Implements identical `StrategyAdapter` protocol interface
- Tests live in `tests/unit/test_ml_adapter.py` and `tests/integration/test_ml_adapter_e2e.py`
- Reuses existing `ml_signal_engine.py` and feature pipelines — no new modules required

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| [e.g., 4th project] | [current need] | [why 3 projects insufficient] |
| [e.g., Repository pattern] | [specific problem] | [why direct DB access insufficient] |
