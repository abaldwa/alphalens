# Phase 0 Research: ML Strategy Adapter Integration

**Date**: 2026-08-25 | **Status**: Complete

## Research Summary

All critical design decisions for ML adapter integration are resolved through analysis of existing AlphaLens patterns. No external research required; adapter follows established architecture.

---

## Decision: StrategyAdapter Protocol Implementation

**Decision**: Implement `MLStrategyAdapter` class that exactly mirrors the interface of existing adapters (Technical, Momentum, Fundamental) in `backtest/core/engine.py`.

**Rationale**:
- The `StrategyAdapter` protocol is already proven by three existing adapters
- AlphaLens constitution (Principle II) mandates registry-driven strategies
- Existing adapters handle universe filtering, rebalance cadence, risk calculations uniformly

**Alternatives Considered**:
- Create a separate `MLSignalGenerator` that bypasses `StrategyAdapter`: Rejected — violates audit trail requirements and prevents fair comparison in backtests.
- Extend `StrategyAdapter` with ML-specific methods: Rejected — violates YAGNI (Principle I); existing interface is sufficient.

**Implementation Evidence**:
- `backtest/adapters/technical_adapter.py`: 150 LOC reference implementation
- `backtest/adapters/momentum_adapter.py`: 140 LOC reference implementation
- `backtest/core/engine.py`: `StrategyAdapter` protocol definition (requires 3 methods: `generate_signals()`, `generate_portfolio()`, `execute_trades()`)

---

## Decision: Signal Generation from ML Model Predictions

**Decision**: Convert raw ML model predictions (e.g., CatBoost score 0.0–1.0) to binary long/flat signals using existing threshold logic from `backtest/core/ml_signal_engine.py`.

**Rationale**:
- Existing `ml_signal_engine.py` already handles model loading, feature engineering, and prediction thresholding
- No need to duplicate this logic in the adapter
- Thresholding approach is proven (used in current reporting-only workflow)

**Alternatives Considered**:
- Probabilistic signals (e.g., signal strength 0.0–1.0 for position sizing): Rejected — out of scope for v1; Technical/Momentum don't use probabilistic signals.
- Continuous model scores without thresholding: Rejected — violates StrategyAdapter interface which expects discrete signals.

**Implementation Evidence**:
- `backtest/core/ml_signal_engine.py` lines ~120–150: thresholding logic exists and tested
- Existing ML signals use binary long/flat representation

---

## Decision: Model Loading and Versioning

**Decision**: Models are loaded by name and version from `baselines/` directory using existing serialization (pickle/joblib); model versioning is tracked in `strategy_registry` entries.

**Rationale**:
- `baselines/` directory already stores trained models
- Existing `ml_signal_engine.py` has model loading code
- `strategy_registry` already supports versioned strategy metadata

**Alternatives Considered**:
- Dynamic model retraining on each backtest: Rejected — out of scope; models are pre-trained.
- In-memory model cache: Rejected — backtest runs are independent; caching adds complexity without benefit.

**Implementation Evidence**:
- `backtest/core/ml_signal_engine.py`: `load_model()` function exists
- `datastore/schema/create_normalised.py`: `strategy_registry` table has `model_version` column

---

## Decision: Regime Overlay Compatibility

**Decision**: ML adapter applies regime overlays (e.g., EMA-RSI exposure gating) identically to other adapters — no special handling required.

**Rationale**:
- Regime overlay is a post-signal filter that applies uniformly to all adapter types
- Existing regime logic in `backtest/core/regime.py` is adapter-agnostic
- Constitution (Principle III) requires regime and strategy to be independent parameters

**Alternatives Considered**:
- Skip regime filtering for ML signals: Rejected — violates uniformity; Technical/Momentum apply regime overlay.
- Add ML-specific regime logic: Rejected — violates YAGNI; existing regime logic is sufficient.

**Implementation Evidence**:
- `backtest/core/regime.py`: regime logic is engine-level, not adapter-specific
- Technical adapter applies regime overlay; ML adapter must do the same

---

## Decision: Walk-Forward Validation Support

**Decision**: ML adapter fully participates in `backtest/walk_forward/runner.py` without modification. Walk-forward training/testing split is handled by the orchestrator, not the adapter.

**Rationale**:
- Walk-forward runner is adapter-agnostic; it calls `execute()` on any StrategyAdapter
- Model retraining on each fold is out of scope (models are pre-trained); orchestrator provides date ranges
- ML adapter simply generates signals using pre-trained model over requested date range

**Alternatives Considered**:
- Add model retraining logic to adapter: Rejected — out of scope; requires separate SPEC-ML-002.
- Skip walk-forward support: Rejected — violates acceptance criterion (FR-006).

**Implementation Evidence**:
- `backtest/walk_forward/runner.py`: calls adapter `execute()` with date ranges
- Existing adapters don't retrain; they generate signals over any date range

---

## Decision: Parameter Sweep Mechanism

**Decision**: ML adapter participates in `run_sweep_inprocess` using standard parameter grid mechanism. Sweepable parameters: lookback_days, feature_selection_threshold, prediction_confidence_threshold.

**Rationale**:
- `run_sweep_inprocess` is adapter-agnostic; it already supports parameter grids for Technical/Momentum strategies
- ML-specific parameters are natural hyperparameter tuning targets
- Existing sweep infrastructure is proven

**Alternatives Considered**:
- Create separate ML sweep orchestrator: Rejected — violates DRY; reuse existing orchestrator.
- Make all ML model weights sweepable: Rejected — model weights are fixed (models are pre-trained); only application-time parameters are sweepable.

**Implementation Evidence**:
- `backtest/run_sweep_inprocess.py`: parameter grid handling is adapter-agnostic
- Sweep results are persisted to `strategy_sweep` table (schema unchanged)

---

## No External Dependencies or Clarifications Remaining

All design decisions are grounded in existing AlphaLens patterns and proven by reference implementations (Technical, Momentum, Fundamental adapters). No external research, no library evaluations, no ambiguous technical choices.

**Next Phase**: Phase 1 design (data model, quickstart, implementation contracts).
