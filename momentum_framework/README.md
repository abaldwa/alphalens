# Momentum Framework

A robust, modular framework for building, backtesting, and comparing momentum-based trading strategies.

## Architecture

```
momentum_framework/
├── common/              # Shared modules (signals, universe, registry)
├── backtesting/         # Backtesting orchestration & execution
├── queues/              # Queue generation framework
├── metrics/             # Metrics standardization & calculation
├── strategies/          # Individual strategy implementations (R0, R01, R07-R13)
├── results/             # Backtest results with standardized nomenclature
└── docs/                # Architecture & design documentation
```

## Quick Start

*(Reflects the real API as of 2026-09-04 — R01 and R14-R17 are implemented
and verified end-to-end; this is not aspirational.)*

### 1. Instantiate a Strategy
```python
from momentum_framework.strategies.r01_trailing_momentum import R01TrailingMomentum

strategy = R01TrailingMomentum(
    band_id=2, top_n=10, lookback_months=12, rebalance_cadence_days=21
)
print(strategy.describe())
```

### 2. Generate & Save a Backtest Queue
```python
from momentum_framework.strategies.r01_trailing_momentum import R01QueueGenerator

gen = R01QueueGenerator(start_date="2009-01-01", end_date="2026-06-30")
path = gen.save_queue("r01_full_campaign.json", "R01 validation grid")
# -> 264 jobs (216 legacy-shape + 48 from the M13 extension), validated
#    and duplicate-checked before the file is written.
```

Every strategy file (`r01_trailing_momentum.py`, `r03_jt_skipmonth.py`,
`r14_inverse_volatility.py`, ...) exposes this same
`{StrategyClass}` + `{StrategyClass}QueueGenerator` pair — see
`strategies/__init__.py` for the full roster and
`docs/CODE_TRACEABILITY.md` for what's ported vs. pending.

### 3. Extract & Analyze Results
```python
from momentum_framework.results import ResultsReader

reader = ResultsReader()
r01_results = reader.load_by_strategy("R01")
df = reader.to_dataframe(r01_results)
```

### 4. Compare Against the Legacy Baseline
```python
from momentum_framework.metrics.nomenclature import build_strategy_id
import pandas as pd, json

sid = build_strategy_id(strategy_code="R01", band_id=2, top_n=10,
                         lookback_months=12, rebalance_cadence_days=21,
                         rank_method="trailing_return")

baseline = pd.read_json("results/traceability/legacy_runs_baseline.json")
match = baseline[baseline["new_strategy_id"] == sid]
# Compare match["sharpe_ratio"] against your new run's Sharpe — same
# config on both sides, so any difference beyond floating-point noise
# is a real regression. See results/traceability/SUMMARY.md.
```

## Framework Components

### Common Modules
- **Signal Computation:** Trailing momentum, 52-week high, sector momentum
- **Universe Definition:** M-band definitions (M2, M4, M7, M9, M10, M12 — partitioned bands, top_n 5/10/15) plus M13 (full 800-stock ADTV universe, top_n 10/20/30/40; overlaps every other band by design — see `common/universe.py::TOP_N_BY_BAND`)
- **Registry:** Strategy parameter versioning (point-in-time)

### Backtesting Framework
- **Orchestrator:** Unified backtest execution engine
- **StrategyAdapter Protocol:** Common interface for all strategies
- **Portfolio Simulation:** Holdings tracking, rebalancing, execution
- **Metrics Engine:** Sharpe, CAGR, drawdown, returns, tax calculations

### Queue Generation Framework
- **Base Generator:** Common logic for creating job specifications
- **Queue Validators:** Ensures jobs are executable
- **Results Writer:** Standardized JSON queue format

### Metrics Standardization
- **Common Calculations:** Shared metric functions
- **Results Schema:** Standardized JSON report structure
- **Nomenclature:** Consistent naming for strategy_id, run_id, file paths

### Strategy Implementations
- **Strategy Base Class:** Common interface
- **Per-Strategy File:** r01_trailing_momentum.py, r03_jt_skipmonth.py, r07..r13 (pending), r14..r17_*.py
- **Strategy Registration:** Automatic discovery & versioning

### Results Management
- **Naming Convention:** `{date}_{strategy}_{band}_{config_hash}.json`
- **Results Reader:** Load & aggregate reports
- **Comparison Tools:** Cross-strategy performance tables
- **Archive:** Historical results with version control

## Results Nomenclature

### Standard Result Filename Format
```
{YYYY-MM-DD}_{STRATEGY}_{BAND}_{TOP_N}_{LOOKBACK_MO}_{REBALANCE_D}_{HASH}.json
```

Example:
```
2026-09-04_R01_M2_top10_lb12mo_21d_allrisk_a1b2c3d4.json
2026-09-04_R09_M12_top5_lb6mo_21d_allrisk_e5f6a7b8.json
```

### Standard Result Structure
```json
{
  "metadata": {
    "result_date": "2026-09-04",
    "framework_version": "1.0",
    "strategy": "R01",
    "run_id": "orch_momentum_20260904_084938_xyz"
  },
  "strategy_config": {
    "rank_method": "trailing_return",
    "band_id": 2,
    "lookback_months": 12,
    "top_n": 10,
    "rebalance_cadence_days": 21,
    "vol_scaling_mode": null,
    "exit_variant": "baseline"
  },
  "backtest_params": {
    "start_date": "2009-01-01",
    "end_date": "2026-06-30",
    "initial_capital": 1000000
  },
  "metrics": {
    "sharpe_ratio": 0.85,
    "cagr": 0.12,
    "max_drawdown": -0.35,
    "win_rate": 0.55,
    "annual_returns": [0.08, 0.15, -0.05, ...]
  },
  "integrity": {
    "passed": true,
    "checks": {
      "trade_log_consistency": true,
      "portfolio_value_continuity": true,
      "signal_ledger_match": true
    }
  },
  "trade_log_path": "results/2026-09-04/trades_{run_id}.csv",
  "equity_curve_path": "results/2026-09-04/equity_{run_id}.csv"
}
```

## Migration Plan

### Phase 1: Framework Creation (Complete)
- [x] Create folder structure
- [x] Implement common modules (signals, universe, band_universe, volatility, position_weighting, registry)
- [x] Implement backtesting framework (StrategyAdapter, BacktestOrchestrator — delegates to legacy engine, see docs/MIGRATION.md)
- [x] Implement queue generation framework (QueueGenerator, QueueValidator, shared simple_momentum_grid())
- [x] Implement metrics standardization (nomenclature.py, standard.py)
- [x] Create strategy base class (StrategyBase, WeightedMomentumStrategy)

### Phase 2: Strategy Implementation
- [x] R01 (the original strategy — trailing-return momentum, no skip)
- [x] R03 (Jegadeesh-Titman skip-month variant)
- [x] R14 (inverse-volatility weighted)
- [x] R15 (inverse-variance weighted, Barroso-Santa-Clara style)
- [x] R16 (target-volatility weighted, capped leverage)
- [x] R17 (downside-volatility weighted, Sortino style)
      — R14-R17 replace the retired R0 (see docs/CODE_TRACEABILITY.md)
- [ ] R07 (Crash-Aware Overlay)
- [ ] R08 (Barroso-Santa-Clara Vol-Target)
- [ ] R09 (Moreira-Muir Vol-Scaling — default paper-trading strategy)
- [ ] R10 (Sector Momentum)
- [ ] R11 (52-Week-High Reversal)
- [ ] R12 (1-Month Reversal + Liquidity)
- [ ] R13 (Bollinger Band Reversal)
- 🚫 R05 (52-Week-High Momentum) — rejected at the Phase 3 gate; historical
  reference only, deliberately excluded from porting (see
  docs/CODE_TRACEABILITY.md's R05 row)

### Phase 3: Validation & Testing
- [ ] Unit tests for framework
- [ ] Integration tests for strategies
- [ ] Results verification (compare with old codebase)
- [ ] Performance benchmarking

### Phase 4: Cutover
- [ ] Archive old codebase
- [ ] Update all imports to use new framework
- [ ] Deprecate old implementations

## Key Design Principles

1. **Modularity:** Each strategy is independent; common logic is shared
2. **Robustness:** Comprehensive error handling, validation, integrity checks
3. **Reproducibility:** All backtests are versioned and point-in-time reproducible
4. **Performance:** Caching, parallel execution, efficient data structures
5. **Auditability:** Complete trade logs, signal ledgers, and metrics trails
6. **Extensibility:** Easy to add new strategies or metrics

## Related Documentation

- [Architecture Design](docs/ARCHITECTURE.md)
- [Results Schema](docs/RESULTS_SCHEMA.md)
- [Strategy Implementation Guide](docs/STRATEGY_GUIDE.md)
- [Migration Checklist](docs/MIGRATION.md)
