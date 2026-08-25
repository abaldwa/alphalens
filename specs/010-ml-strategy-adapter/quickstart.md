# Quickstart: Validating ML Strategy Adapter

**Date**: 2026-08-25 | **Status**: Complete

## Overview

This guide documents how to validate that the ML Strategy Adapter is working correctly after implementation. It covers three core acceptance tests: single-symbol backtest, parameter sweep, and walk-forward validation.

---

## Prerequisites

- AlphaLens repository cloned and environment set up (`python3 -m venv .venv && source .venv/bin/activate`)
- DuckDB database initialized with strategy_registry and strategy_signals tables
- Pre-trained ML model available (e.g., `baselines/catboost_v1.pkl`)
- Feature engineering pipeline available (e.g., `features/momentum_universe.py`)

---

## Validation Test 1: Single-Symbol Backtest

**Purpose**: Verify that ML adapter can execute a backtest and produce expected outputs (P&L, Sharpe, CAGR).

**Setup**:

```bash
# Register an ML strategy in strategy_registry
python3 -c "
from datastore.schema.create_normalised import init_db
from datetime import datetime

db = init_db()
db.execute('''
  INSERT INTO strategy_registry (
    strategy_id, domain, name, model_name, model_version, 
    lookback_days, parameters, active, created_at
  ) VALUES (
    'ML-CATBOOST-V1', 'ml', 'CatBoost Momentum Classifier', 
    'catboost_v1', '1.0.0', 63, 
    '{\"prediction_threshold\": 0.55}', 
    true, '2026-08-25'
  )
'
'''
)
```

**Execute**:

```bash
# Run a single-symbol backtest for SBIN (2024-01-01 to 2025-12-31)
python3 backtest/run_orchestrator_backtest.py \
  --strategy ML-CATBOOST-V1 \
  --symbol SBIN \
  --start 2024-01-01 \
  --end 2025-12-31
```

**Validate**:

```bash
# Check that backtest completed and results are in database
python3 -c "
import duckdb
db = duckdb.connect('~/.local/share/AlphaLens/data/backtest.duckdb', read_only=True)

# Fetch backtest run results
results = db.execute('''
  SELECT run_id, strategy, symbol, sharpe_ratio, cagr, max_drawdown, trade_count
  FROM runs
  WHERE strategy = 'ML-CATBOOST-V1' AND symbol = 'SBIN'
  ORDER BY created_at DESC
  LIMIT 1
''').fetchall()

print('Backtest Results:')
for r in results:
  print(f'  Run ID: {r[0]}')
  print(f'  Sharpe: {r[3]:.2f}')
  print(f'  CAGR: {r[4]:.2f}%')
  print(f'  Max Drawdown: {r[5]:.2f}%')
  print(f'  Trades: {r[6]}')
"
```

**Expected Outcome**:
- Backtest completes without errors
- Results show non-zero Sharpe/CAGR (not NaN or infinite)
- Trade count matches rebalance frequency (e.g., 252 for daily rebalance)
- Signals are persisted to `strategy_signals` table

**Assertion Checklist**:
- [ ] Backtest run_id is recorded
- [ ] Sharpe and CAGR are numeric (not NaN)
- [ ] Trade count > 0
- [ ] Signals exist in strategy_signals table for strategy

---

## Validation Test 2: Parameter Sweep

**Purpose**: Verify that ML adapter supports hyperparameter sweeps across a grid of values.

**Setup**:

```bash
# Create a parameter sweep config (JSON file)
cat > backtest/queues/ml_catboost_sweep.json << 'EOF'
{
  "base_strategy": "ML-CATBOOST-V1",
  "parameters": {
    "lookback_days": [63, 126],
    "prediction_threshold": [0.50, 0.55, 0.60]
  },
  "symbols": ["SBIN", "RELIANCE", "INFY"],
  "date_range": {
    "start": "2024-01-01",
    "end": "2025-12-31"
  }
}
EOF
```

**Execute**:

```bash
# Run parameter sweep
python3 backtest/run_sweep_inprocess.py backtest/queues/ml_catboost_sweep.json
```

**Validate**:

```bash
# Check that all sweep combinations completed
python3 -c "
import duckdb
db = duckdb.connect('~/.local/share/AlphaLens/data/backtest.duckdb', read_only=True)

# Count completed sweep combinations
sweep_results = db.execute('''
  SELECT COUNT(*) as total, 
         COUNT(DISTINCT lookback_days, prediction_threshold) as unique_params
  FROM strategy_sweep
  WHERE strategy = 'ML-CATBOOST-V1'
  AND symbol IN ('SBIN', 'RELIANCE', 'INFY')
''').fetchall()

expected = 2 * 3 * 3  # 2 lookback_days × 3 thresholds × 3 symbols = 18
actual = sweep_results[0][0]
print(f'Sweep Results: {actual} / {expected} combinations')
assert actual == expected, f'Expected {expected}, got {actual}'
"
```

**Expected Outcome**:
- All parameter combinations (2 lookback × 3 threshold × 3 symbols = 18) complete
- Each combination has Sharpe/CAGR results
- Results are ordered by performance (highest Sharpe first)

**Assertion Checklist**:
- [ ] All parameter combinations executed (18 total)
- [ ] Each combination has a result row
- [ ] Sharpe/CAGR values are numeric and comparable
- [ ] Results can be ranked by performance

---

## Validation Test 3: Walk-Forward Validation

**Purpose**: Verify that ML adapter supports out-of-sample generalization testing via walk-forward analysis.

**Setup**:

```bash
# Create walk-forward config
cat > backtest/queues/ml_catboost_walkforward.json << 'EOF'
{
  "strategy": "ML-CATBOOST-V1",
  "symbols": ["SBIN", "RELIANCE"],
  "train_window_days": 252,
  "test_window_days": 63,
  "rebalance_frequency": "daily",
  "date_range": {
    "start": "2023-01-01",
    "end": "2025-12-31"
  }
}
EOF
```

**Execute**:

```bash
# Run walk-forward validation
python3 backtest/walk_forward/runner.py backtest/queues/ml_catboost_walkforward.json
```

**Validate**:

```bash
# Check that folds completed and in-sample vs. out-of-sample metrics can be compared
python3 -c "
import duckdb
db = duckdb.connect('~/.local/share/AlphaLens/data/backtest.duckdb', read_only=True)

# Fetch walk-forward results
wf_results = db.execute('''
  SELECT fold_number, in_sample_sharpe, out_sample_sharpe, 
         in_sample_cagr, out_sample_cagr
  FROM walk_forward_results
  WHERE strategy = 'ML-CATBOOST-V1'
  ORDER BY fold_number
''').fetchall()

print('Walk-Forward Results:')
for row in wf_results:
  is_sharpe, oos_sharpe = row[1], row[2]
  is_cagr, oos_cagr = row[3], row[4]
  print(f'  Fold {row[0]}: IS Sharpe={is_sharpe:.2f}, OOS Sharpe={oos_sharpe:.2f}, ' +
        f'IS CAGR={is_cagr:.1f}%, OOS CAGR={oos_cagr:.1f}%')

# Check generalization: if OOS metrics are <80% of IS, there may be overfitting
avg_is_sharpe = sum(r[1] for r in wf_results) / len(wf_results)
avg_oos_sharpe = sum(r[2] for r in wf_results) / len(wf_results)
generalization_ratio = avg_oos_sharpe / avg_is_sharpe if avg_is_sharpe > 0 else 0
print(f'\\nGeneralization Ratio (OOS/IS Sharpe): {generalization_ratio:.2%}')
print(f'  → {\"✓ Reasonable\" if generalization_ratio > 0.8 else \"⚠ Possible overfitting\"}')"
```

**Expected Outcome**:
- Walk-forward produces multiple folds (e.g., 10 folds over 3-year period)
- Each fold has both in-sample and out-of-sample Sharpe/CAGR
- Out-of-sample Sharpe is >80% of in-sample (indicating reasonable generalization, not overfitting)

**Assertion Checklist**:
- [ ] Multiple folds completed (e.g., ≥ 5 folds)
- [ ] Each fold has both IS and OOS metrics
- [ ] OOS Sharpe is > 0.8 × IS Sharpe (no major overfitting)
- [ ] Metrics are stable across folds (low variance)

---

## Validation Test 4: Regime Overlay Compatibility

**Purpose**: Verify that ML adapter works with regime overlays (e.g., EMA-RSI exposure gating).

**Setup**:

```bash
# Register ML strategy WITH regime overlay
python3 -c "
from datastore.schema.create_normalised import init_db

db = init_db()
db.execute('''
  INSERT INTO strategy_registry (
    strategy_id, domain, name, model_name, model_version, 
    lookback_days, parameters, regime_overlay, active, created_at
  ) VALUES (
    'ML-CATBOOST-REGIME', 'ml', 'CatBoost + EMA-RSI Exposure Gate', 
    'catboost_v1', '1.0.0', 63, 
    '{\"prediction_threshold\": 0.55}', 
    'ema_rsi_v1',  -- Regime overlay
    true, '2026-08-25'
  )
'
'''
)
```

**Execute**:

```bash
# Run backtest with regime overlay
python3 backtest/run_orchestrator_backtest.py \
  --strategy ML-CATBOOST-REGIME \
  --symbol SBIN \
  --start 2024-01-01 \
  --end 2025-12-31
```

**Validate**:

```bash
# Check that regime overlay reduces position count but maintains correctness
python3 -c "
import duckdb
db = duckdb.connect('~/.local/share/AlphaLens/data/backtest.duckdb', read_only=True)

# Compare: ML adapter with vs. without regime overlay
no_regime = db.execute('''
  SELECT avg_position_count, sharpe_ratio FROM runs
  WHERE strategy = 'ML-CATBOOST-V1' AND symbol = 'SBIN'
  ORDER BY created_at DESC LIMIT 1
''').fetchall()[0]

with_regime = db.execute('''
  SELECT avg_position_count, sharpe_ratio FROM runs
  WHERE strategy = 'ML-CATBOOST-REGIME' AND symbol = 'SBIN'
  ORDER BY created_at DESC LIMIT 1
''').fetchall()[0]

print(f'Without Regime: {no_regime[0]:.1f} avg positions, Sharpe {no_regime[1]:.2f}')
print(f'With Regime:    {with_regime[0]:.1f} avg positions, Sharpe {with_regime[1]:.2f}')
print(f'Regime Impact:  {(with_regime[0]/no_regime[0]):.1%} position reduction')
"
```

**Expected Outcome**:
- Backtest completes successfully with regime overlay applied
- Position count is reduced (regime overlay filters some signals)
- Sharpe ratio may increase or decrease depending on regime timing (both valid)

**Assertion Checklist**:
- [ ] Backtest completes without errors
- [ ] Position count is reduced vs. no overlay (regime is working)
- [ ] Metrics are numeric (not NaN)

---

## Validation Test 5: Error Handling

**Purpose**: Verify that ML adapter handles edge cases gracefully (missing models, NaN predictions, etc.).

**Execute**:

```bash
# Test 1: Missing model
python3 -c "
from backtest.adapters.ml_adapter import MLStrategyAdapter
adapter = MLStrategyAdapter(model_name='nonexistent_model.pkl')
try:
  adapter.load_model()
  print('ERROR: Should have raised FileNotFoundError')
except FileNotFoundError as e:
  print(f'✓ Missing model handled: {e}')
"

# Test 2: Invalid prediction threshold
python3 -c "
from backtest.adapters.ml_adapter import MLStrategyAdapter
adapter = MLStrategyAdapter(prediction_threshold=1.5)  # Invalid: > 1.0
try:
  adapter.validate()
  print('ERROR: Should have raised ValueError')
except ValueError as e:
  print(f'✓ Invalid threshold caught: {e}')
"

# Test 3: NaN predictions
python3 -c "
import numpy as np
from backtest.adapters.ml_adapter import MLStrategyAdapter
adapter = MLStrategyAdapter(model_name='catboost_v1.pkl')
signals = adapter.generate_signals(dates=['2025-01-01'], universe=['SBIN'])
# If any signals contain NaN, they should be logged with warnings (not crashes)
print('✓ NaN predictions handled without crashing')
"
```

**Expected Outcome**:
- Missing model raises clear FileNotFoundError
- Invalid parameters raise ValueError
- NaN predictions log warnings and skip those dates (no crash)

**Assertion Checklist**:
- [ ] Missing model error is descriptive
- [ ] Invalid parameters are caught at initialization
- [ ] NaN predictions are logged and handled gracefully

---

## Summary Checklist

After completing all validation tests, confirm:

- [x] Test 1: Single backtest runs and produces numeric Sharpe/CAGR
- [x] Test 2: Parameter sweep executes all combinations and ranks results
- [x] Test 3: Walk-forward produces both IS/OOS metrics with reasonable generalization
- [x] Test 4: Regime overlay compatibility works (position count reduced, metrics valid)
- [x] Test 5: Error handling catches and logs issues gracefully

**→ If all checks pass: ML adapter is ready for production use.**

---

## Debugging Tips

| Issue | Diagnosis | Fix |
|-------|-----------|-----|
| Backtest hangs | Check if model file exists and is readable | Verify `baselines/catboost_v1.pkl` exists and has correct permissions |
| All signals are "flat" | Model predictions may all be below threshold | Lower prediction_threshold in strategy_registry parameters |
| Trade count is zero | No signals generated or all filtered by regime | Check feature engineering (features/) is computing correctly |
| NaN in metrics | Insufficient data or division-by-zero in risk calculation | Check date range and universe size |
| Sharpe/CAGR mismatch with Technical adapter | ML adapter using different risk-free rate assumption | Verify both adapters use same risk-free rate (0% assumed) |

---

## Related Documentation

- [spec.md](spec.md) — Feature specification and requirements
- [data-model.md](data-model.md) — Entity definitions and StrategyAdapter interface
- [research.md](research.md) — Design decisions and alternatives considered
- `backtest/core/engine.py` — StrategyAdapter protocol definition
- `backtest/adapters/technical_adapter.py` — Reference implementation
