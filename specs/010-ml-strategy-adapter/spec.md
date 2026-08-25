# Feature Specification: ML Strategy Adapter - First-Class Integration

**Feature Branch**: `010-ml-strategy-adapter`

**Created**: 2026-08-25

**Status**: Draft

**Input**: User description: "Integrate ML Signal Engine as a first-class StrategyAdapter (parity with Technical/Momentum/Fundamental adapters). Current state: ML signals are computed in backtest/core/ml_signal_engine.py and only plugged into reporting. Technical, Momentum, and Fundamental strategies all use backtest/core/engine.py's StrategyAdapter protocol. Goal: make ML a full adapter so it competes in the same backtesting loop."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - ML Backtesting Parity (Priority: P1)

A quantitative researcher wants to backtest an ML-based trading strategy (e.g., a CatBoost classifier predicting daily returns) using the same backtesting infrastructure as Technical and Momentum strategies. Currently, ML models are computed outside the main backtest loop and only integrated at the reporting stage, making it impossible to:
- Compare ML signals fairly against other strategies in the same backtest run
- Test ML strategy performance on historical data with the same walk-forward validation
- Optimize ML parameters (lookback periods, feature sets) using the standard strategy sweep mechanism

**Why this priority**: This is the core value proposition. Until ML is a first-class adapter, it's a second-class strategy type that cannot compete fairly with technical/momentum approaches. This blocks adoption of ML models for live trading.

**Independent Test**: Can be fully tested by loading an ML strategy, running a backtest, and comparing its Sharpe/CAGR output against the same strategy run through Technical/Momentum adapters. Delivers: parity in backtesting capability.

**Acceptance Scenarios**:

1. **Given** a CatBoost ML strategy is registered in strategy_registry, **When** user runs a single-symbol backtest, **Then** the ML adapter executes in the main backtest loop (not post-hoc) and produces a trade ledger with P&L, drawdown, Sharpe, CAGR.
2. **Given** both an ML and a Technical strategy backtested over the same period, **When** user compares their metrics, **Then** both are computed using identical rebalance cadence, universe filtering, and risk calculations.
3. **Given** an ML strategy and a Technical strategy, **When** both are swept across parameter ranges, **Then** the sweep mechanism works identically for both (same grid, same parallelization).

---

### User Story 2 - ML Strategy Sweep & Parameter Optimization (Priority: P2)

A quantitative researcher wants to optimize ML strategy hyperparameters (e.g., model lookback period, feature selection threshold, prediction confidence threshold) by running a sweep over a parameter grid. Currently, ML models cannot participate in the standard `run_sweep_inprocess` orchestrator because they lack a StrategyAdapter interface.

**Why this priority**: Parameter optimization is critical for discovering whether an ML model adds alpha or just overfits. This is the primary workflow for model evaluation. It's blocked by lack of adapter parity.

**Independent Test**: Can be fully tested by running a parameter sweep on an ML strategy and confirming all combinations complete, with results written to strategy_sweep results table. Delivers: ML model hyperparameter tuning capability.

**Acceptance Scenarios**:

1. **Given** a parameter sweep config for an ML strategy (e.g., lookback=\[63, 126, 252\], confidence_threshold=\[0.55, 0.65, 0.75\]), **When** user runs the sweep, **Then** all 9 combinations execute and complete successfully.
2. **Given** sweep results for an ML strategy, **When** results are fetched, **Then** each combination's Sharpe/CAGR is recorded and can be ranked to identify optimal hyperparameters.

---

### User Story 3 - ML Walk-Forward Validation & Overfitting Detection (Priority: P3)

A quantitative researcher wants to validate that an ML strategy generalizes to out-of-sample data using walk-forward testing (e.g., train on 2019-2021, test on 2022, repeat). Currently, ML models cannot participate in the standard `walk_forward_runner` because they lack a StrategyAdapter interface.

**Why this priority**: Walk-forward validation is essential for confirming that an ML strategy doesn't overfit to historical data. It's a robustness check, so it's slightly lower priority than backtesting and sweep, but still critical before any live deployment.

**Independent Test**: Can be fully tested by running a walk-forward test on an ML strategy and confirming that train-period and test-period metrics are both recorded and can be compared. Delivers: ML model generalization validation.

**Acceptance Scenarios**:

1. **Given** a walk-forward config with 1-year train windows and 1-month test windows, **When** user runs it on an ML strategy, **Then** each fold completes and produces both train and test Sharpe/CAGR metrics.
2. **Given** walk-forward results, **When** train and test metrics are compared, **Then** in-sample and out-of-sample performance divergence can be measured to detect overfitting.

---

### Edge Cases

- What happens when an ML model's prediction is NaN or infinite (e.g., model crashed, feature engineering failed)? System must handle gracefully (e.g., skip signal for that day, log warning).
- What happens when an ML model's training data is insufficient (e.g., fewer than min_samples_for_training observations available)? System must reject and report clearly.
- What happens when an ML strategy is combined with a regime overlay (e.g., EMA-RSI exposure gating)? System must apply regime filtering to ML signals the same way it does for Technical/Momentum.
- What happens when an ML model is swept across a date range where the model doesn't have sufficient historical training data? System must handle gracefully (skip early dates, report).

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: ML adapter MUST implement the `StrategyAdapter` protocol (same interface as Technical/Momentum/Fundamental adapters) with methods: `generate_signals()`, `generate_portfolio()`, `execute_trades()`.
- **FR-002**: ML adapter MUST load pre-trained ML models from `baselines/` directory by model name and version.
- **FR-003**: ML adapter MUST generate daily signals from model predictions (e.g., CatBoost raw score → binary long/flat signal).
- **FR-004**: ML adapter MUST participate in the standard backtest engine loop (`backtest/core/engine.py`) with identical rebalance cadence and universe filtering as other adapters.
- **FR-005**: ML adapter MUST support parameter sweeps (hyperparameter grids) for model lookback period, feature thresholds, and prediction confidence thresholds via `run_sweep_inprocess`.
- **FR-006**: ML adapter MUST support walk-forward validation via `walk_forward_runner` with out-of-sample performance tracking.
- **FR-007**: ML adapter MUST handle missing/invalid predictions gracefully (NaN, infinite, out-of-range values) by logging warnings and skipping signals for affected dates.
- **FR-008**: ML adapter MUST support regime overlays (e.g., EMA-RSI exposure gating) the same way Technical/Momentum adapters do (apply regime filter to signals before trade execution).
- **FR-009**: ML adapter MUST persist signals to `strategy_signals` table with strategy_id, date, signal (long/flat), prediction_score, model_version for auditing.
- **FR-010**: ML strategy MUST be registrable in `strategy_registry` the same way other strategies are (domain='ml', version, parameters).

### Key Entities

- **MLStrategy**: Represents an ML-based trading strategy; has: strategy_id, model_name, model_version, lookback_days, feature_set, prediction_threshold, trained_date, model_path.
- **MLSignal**: Daily signal generated by an ML model; has: strategy_id, date, signal (long/flat/cash), prediction_score, feature_vector (snapshotted for auditability), model_version.
- **StrategyAdapter**: Existing protocol in `backtest/core/engine.py`; defines interface that Technical, Momentum, Fundamental adapters implement; ML adapter MUST implement the same interface.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: ML adapter can execute a single-symbol backtest and produce P&L, Sharpe, CAGR metrics with zero additional configuration beyond registering the ML strategy.
- **SC-002**: An ML strategy backtest produces identical rebalance counts, trade counts, and universe membership as a Technical strategy run over the same period (confirming participation in the same backtest loop).
- **SC-003**: Parameter sweep on an ML strategy completes all grid combinations successfully (e.g., 3x3x3=27 parameter combos) with consistent results across runs.
- **SC-004**: Walk-forward test on an ML strategy produces both in-sample and out-of-sample Sharpe/CAGR with < 20% variance between folds (confirming reasonable generalization, not overfitting to a single fold).
- **SC-005**: ML adapter handles missing predictions gracefully: if 5% of dates have NaN predictions, backtest continues with signals skipped for those dates, and a warning is logged (zero crashes).
- **SC-006**: Code review confirms ML adapter implements StrategyAdapter protocol identically to existing Technical/Momentum/Fundamental adapters (same method signatures, return types, error handling).

## Assumptions

- **Scope boundary**: ML adapter is in-scope only for backtesting and walk-forward validation in this phase; paper trading and live deployment of ML strategies are out of scope (documented as follow-up specs).
- **Model format**: Pre-trained ML models are persisted as serialized Python objects (pickle, joblib) in `baselines/` directory; model loading/serialization is already implemented in `backtest/core/ml_signal_engine.py` and will be reused, not reimplemented.
- **Feature engineering**: ML models already have feature engineering pipelines (e.g., momentum bands, fundamentals, delivery data) implemented in `features/` module; ML adapter will reuse these pipelines, not build new ones.
- **Signal generation**: ML model outputs (e.g., CatBoost prediction scores 0–1) will be converted to binary long/flat signals using existing thresholding logic in `ml_signal_engine.py` (e.g., score > 0.55 → long, else flat). This logic will be encapsulated in the adapter's `generate_signals()` method.
- **Regime compatibility**: Existing regime overlay (`backtest/core/regime.py` EMA-RSI) will work with ML signals without modification; ML adapter simply applies regime filtering post-signal-generation, same as other adapters.
- **Training data**: ML models are already trained (models stored in `baselines/`); this spec does NOT include retraining pipelines. Retraining is a separate spec (SPEC-ML-002 or similar).
- **Strategy registry**: ML strategies must be registered in `strategy_registry` with domain='ml' and versioned parameters; no changes to the registry schema are required.

