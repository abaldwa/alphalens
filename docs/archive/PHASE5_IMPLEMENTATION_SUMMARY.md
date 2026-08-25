# Phase 5 Implementation Summary: Risk-Adjusted Composite Momentum

**Date Completed:** 2026-08-23  
**Commit:** 5bc1754f Phase 5 (R5): Implement risk-adjusted composite momentum strategy (Spec Section 8)  
**Status:** ✅ Implementation complete, ready for validation backtest  

---

## What Was Built

### Core Signal Function: `risk_adjusted_momentum_score()`

A new momentum signal combining 12-month and 6-month returns, each normalized by its own volatility:

```
risk_adj_score = (m12 / vol12 + m6 / vol6) / 2
```

**Location:** `features/momentum_signal.py`  
**Signature:**
```python
def risk_adjusted_momentum_score(
    price_panel: pd.DataFrame,
    tickers: List[str],
    as_of_date: str,
    volatility_measure: str = "daily_return_stddev",
    use_skip_month: bool = False,
    min_volatility: float = 0.001,
    winsorize_pct: float = 0.05,
) -> pd.Series
```

### Volatility Measures (Spec 8.2)

**1. `daily_return_stddev` (default)**
- Annualized standard deviation of daily log-returns (252-day window)
- Captures realized volatility from price movements
- Formula: `std(log(price[t] / price[t-1])) * sqrt(252)`

**2. `daily_price_stddev` (alternative)**
- Standard deviation of daily absolute price changes (252-day window)
- More conservative, captures raw price volatility
- No annualization (raw scale)

### Spec 8.4 Safeguards

✅ **Winsorization:** Clipped extreme scores at 5th/95th percentiles  
✅ **Volatility Floor:** Minimum volatility of 0.1% (default) to prevent div-by-zero  
✅ **Exclusion Tracking:** Logs counts of clipped observations  
✅ **Missing Data Handling:** Tickers with <126 days history excluded (never imputed)  
✅ **Raw+Winsorized Preservation:** Both scores available for analysis  

### Spec 8.5 Variants Built

1. **raw-price-stddev** — volatility_measure="daily_price_stddev"
2. **return-stddev** — volatility_measure="daily_return_stddev" (default)
3. **return-stddev-skip-1** — skip_months=1 + daily_return_stddev (12-7, 6-2 lookbacks)

---

## Integration Points

### MomentumAdapter Wiring
- **New Parameter:** `volatility_measure: str = "daily_return_stddev"`
- **Rank Method Dispatch:** Added `rank_method="risk_adjusted_composite"`
- **Location:** `backtest/adapters/momentum_adapter.py`
- **Behavior:** When `rank_method="risk_adjusted_composite"`, instantiates a rank_fn that calls `risk_adjusted_momentum_score()` with the configured volatility_measure

### CLI Arguments
**New Flag:**
```
--volatility-measure {daily_return_stddev, daily_price_stddev}
```
Default: `daily_return_stddev`  
Only active when `--rank-method risk_adjusted_composite`

**Updated Flag:**
```
--rank-method risk_adjusted_composite
```
(was previously "risk_adjusted", now matches adapter code)

### Strategy Identity (`_momentum_descriptor()`)
- Non-default volatility measures appended to strategy_key: `rank_risk_adjusted_composite_vol_daily_price_stddev`
- Ensures each variant has a unique identity in the database
- Full descriptor string handles all combinations

### Orchestrator Wiring
- Two MomentumAdapter instantiation sites updated (lines 1095, 1411)
- `volatility_measure` threaded through:
  - `_run_immediate()` → MomentumAdapter
  - `_run_deferred()` → MomentumAdapter  
  - Main `run()` function → CLI arg collection

---

## Testing

### Unit Tests Added (9 test cases)

**TestRiskAdjustedMomentumScore**
1. `test_basic_structure` — Uptrending ticker scores higher than noisy ticker
2. `test_insufficient_history_empty_result` — <252 days → empty result
3. `test_daily_price_volatility_measure` — alternative vol measure works
4. `test_skip_month_variant` — 12-7, 6-2 lookbacks differ from standard
5. `test_winsorization_caps_outliers` — extreme scores capped at percentiles
6. `test_empty_panel` — empty input → empty output
7. `test_ticker_not_in_panel` — missing ticker → excluded

**TestDailyVolatilityHelpers**
1. `test_daily_return_volatility_structure` — returns positive, reasonable annualized value
2. `test_daily_price_volatility_structure` — returns positive, reasonable level

### Non-Regression
✅ All 40 existing momentum_adapter tests still pass  
✅ No synthetic DB writes (all tests use in-memory DataFrames)  
✅ No test stubs policy verified

---

## Validation Queue

**File:** `backtest/queues/phase5_risk_adjusted_composite_validation.json`

**Strategy:** Fast validation (2019-2025, 7 years) with 4 discriminative jobs:
1. Band 1 (M1 large-caps) + return-stddev (default)
2. Band 1 (M1 large-caps) + price-stddev (alternative)
3. Band 9 (M9 mid-caps) + return-stddev-skip-1 (skip-month variant)
4. Band 9 (M9 mid-caps) + return-stddev (baseline mid-cap)

**Optimizations:**
- 7-year window (2019-2025) instead of 16 years → ~8-10 min per job
- 4 jobs instead of 8 → captures size effect + vol measure variance
- OHLCV snapshot caching enabled → first job prefetches, saves 15-20 sec/job
- Pre-computed snapshots → reduces DuckDB lock contention

**Expected Duration:** ~45 minutes total (including snapshot prefetch)

---

## Phase 5 Gate Requirements

Before proceeding to Phase 6:

1. **Backtest Results:**
   - [ ] Run validation queue (should complete in ~45 min)
   - [ ] Confirm Sharpe > 0.8 on at least 2 bands (shows signal viability)
   - [ ] Check rolling 1-year Sharpe stability (std < 0.5 for consistency)
   - [ ] Verify max drawdown < 55% (risk bound)

2. **Cross-Phase Comparison:**
   - [ ] Compare to Phase 4 (R4, sector momentum)
   - [ ] Confirm risk-adjusted scores show different mean/variance profile
   - [ ] Check correlation: expect modest (0.3-0.6) overlap, not redundant

3. **Robustness Validation:**
   - [ ] Bootstrap DSR check on final run (n_trials=18 with phases 1-5)
   - [ ] Regime breakdown: confirm performance is regime-dependent (expected)
   - [ ] Subperiod persistence: 2-3 year rolling windows should show consistent wins

4. **Code Quality:**
   - [ ] Run full test suite: `pytest tests/unit/ -q` should pass
   - [ ] No synthetic data, no DB writes during tests ✅ (verified)
   - [ ] Commit message includes "Co-Authored-By" ✅ (done)

---

## Known Limitations & Future Work

**Current Scope (Phase 5):**
- Pure-play momentum signal, no regime overlay yet (that's Phase 7)
- No position-sizing based on volatility (that's Phase 8, Barroso-Santa-Clara)
- No downside volatility (that's Phase 9, Moreira-Muir)

**What Spec 7.2 Flagged (Not Implemented):**
- Bootstrap confidence intervals for Sharpe (flagged for later prioritization)
- Reversal-robustness test (sign-flip check, would go in overfit_checks.py)
- These are reporting enhancements, not core signal changes

**Possible Enhancements (Post-Gate):**
- Adaptive volatility floors based on regime
- Multi-period lookbacks (e.g., 3/6/12 equal-weighted composite)
- Factor-orthogonalized risk-adjusted scores (regress out size/beta first)

---

## Files Modified

| File | Changes |
|------|---------|
| `features/momentum_signal.py` | +150 lines: risk_adjusted_momentum_score, _daily_return_volatility, _daily_price_volatility |
| `features/winsorize.py` | No changes (already existed from Phase 0) |
| `backtest/adapters/momentum_adapter.py` | +20 lines: volatility_measure param, rank_method dispatch |
| `backtest/run_orchestrator_backtest.py` | +50 lines: CLI args, _momentum_descriptor, adapter wiring |
| `tests/unit/test_momentum_signal.py` | +170 lines: 9 new test cases + helpers |
| `backtest/queues/phase5_...json` | NEW: validation queue definition |

**Total Additions:** ~390 lines of code + tests

---

## How to Run Phase 5 Validation

```bash
# Single command to run the validation queue
python3 backtest/run_strategy_queue.py backtest/queues/phase5_risk_adjusted_composite_validation.json

# Monitor progress
python3 -c "
import duckdb
db = duckdb.connect('~/.local/share/AlphaLens/data/backtest.duckdb', read_only=True)
print(db.execute('SELECT strategy_key, cagr, sharpe, max_dd FROM backtest_runs WHERE strategy_key LIKE \"%risk_adjusted%\" ORDER BY created_at DESC LIMIT 10').fetchall())
"

# After validation completes, review results
# See: backtest/reports/ for detailed metrics, rolling windows, regime breakdown
```

---

## Success Criteria

Phase 5 is **successful** when:
1. Validation backtest (2019-2025) shows Sharpe > 0.8 on ≥2 bands
2. Risk profile (volatility, max DD) is distinct from Phase 4's sector momentum
3. Rolling 1-year Sharpe shows reasonable consistency (not regime-locked, not erratic)
4. All tests pass, no regressions in existing functionality
5. Commit history is clean and documented

---

## Next: Phase 6

Phase 6 (R6) skips the numbering (spec 7.6 defines momentum_12_7 itself, and Phase 2 covered both 12-7 and 6-2 together) but may require a reporting extension for incremental IC analysis. See the plan for sequencing.

Phase 7 (R7) will layer a crash-aware overlay on top of Phase 5 (or Phase 1/3 base), using regime detection + drawdown thresholds.
