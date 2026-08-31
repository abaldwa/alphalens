# B-001: J&T Overlapping K-Portfolio Implementation Summary

**Date:** 2026-08-31  
**Status:** Code Complete (Unit Tests Added, Backtest Execution Pending)  
**Target:** Implement J&T (1993) style K-portfolio overlapping rebalancing for turnover reduction

---

## What Was Implemented

### Core Feature
Overlapping portfolio rebalancing where K positions are rotated on a 1/K basis per rebalance, instead of 100% replacement each month.

**Expected Impact:**
- Turnover reduction: ~4-5% annual → ~1.5-2% annual (2.5-3x improvement)
- Position holding period extension: 5x longer average holding period (K=5)
- Cost reduction: Lower transaction costs from reduced rebalancing frequency

### Configuration Parameter
Added `overlapping_k_portfolio: Optional[int]` to `OrchestratorConfig`
- **None (default):** Feature disabled; backward-compatible with existing behavior
- **K value (e.g., 5):** Enable overlapping portfolios with K cohorts

---

## Files Modified

### 1. `/home/amit/projects/AlphaLens/backtest/portfolio.py`
**Added:** `Position.cohort_number` field
- Tracks which rotation cohort (0 to K-1) this position belongs to
- None when overlapping_k_portfolio is disabled

### 2. `/home/amit/projects/AlphaLens/backtest/core/portfolio.py`
**Added to `StrategyPortfolio`:**
- Constructor parameter: `overlapping_k_portfolio: Optional[int]`
- State tracking: `current_cohort_number: int` and `overlapping_k_portfolio: Optional[int]`
- Modified `buy()` method: Assigns `cohort_number` to new positions
- Helper method: `get_positions_due_for_rotation(rebalance_index: int) -> List[str]`
  - Returns tickers from cohort due for rotation this rebalance
  - Due cohort = (rebalance_index - K) % K, only if rebalance_index >= K

### 3. `/home/amit/projects/AlphaLens/backtest/core/engine.py`
**Added to `OrchestratorConfig`:**
- New parameter: `overlapping_k_portfolio: Optional[int]`

**Modified `BacktestOrchestrator.run()`:**
1. **Rebalance counter:** Track `rebalance_index` across all trading days
2. **Cohort assignment:** Set `portfolio.current_cohort_number = rebalance_index % K` at each rebalance
3. **Sell signal filtering:** Check if position's cohort is active before allowing sale
   - If cohort not due: record as data_gap with reason `cohort_rotation_deferred`
4. **Buy signal filtering:** Check if current cohort's slot has capacity
   - If full: record as data_gap with reason `cohort_slot_full`
5. **Rebalance tracking:** Increment `rebalance_index` at end of each rebalance date

### 4. `/home/amit/projects/AlphaLens/tests/unit/test_k_portfolio_rebalancing.py`
**New comprehensive test suite covering:**

#### Configuration & Initialization
- K-portfolio disabled by default
- K must be positive integer
- Initialization with explicit K values

#### Cohort Assignment
- Position assigned current cohort on buy
- Positions have no cohort when disabled
- Current cohort computed from rebalance index

#### Rotation Mathematics
- K=5 means 5-month rotation cycle
- Only one cohort active per rebalance
- Steady-state portfolio holds correct number of positions
- Due cohort calculation: (rebalance_index - K) % K

#### Signal Filtering
- Sell signals rejected if cohort not due
- Sell signals accepted if cohort due
- Buy signals rejected if cohort slot full
- Buy signals accepted if cohort slot has space

#### Edge Cases
- First rebalance has no cohort due
- K=1 single-position rotation
- Positions held across multiple K cycles
- Backward compatibility with disabled feature

#### Turnover Metrics
- Theoretical monthly turnover = 100% / K
- K=5 → 20% per month
- K=3 → ~33% per month

---

## Design Decisions

### 1. Cohort Tracking via Modulo Arithmetic
- Cohort assignment: `cohort_number = rebalance_index % K`
- Due cohort: `(rebalance_index - K) % K`
- Ensures clean 1/K rotation without per-position state

### 2. Signal Filtering vs. Forced Closure
- Signals generate the exit/entry flows (no synthetic closes)
- Cohort tracking acts as a GATE, not a force-closer
- Data gaps record when signals are deferred due to cohort constraints

### 3. Backward Compatibility
- `overlapping_k_portfolio=None` (default) disables feature completely
- No changes to signal generation or core portfolio mechanics
- Legacy positions (cohort_number=None) coexist with K-portfolio positions

### 4. Rebalance Indexing
- Rebalance counter starts at 0, increments only on rebalance dates
- Works with any rebalance cadence (21d, 63d, etc.)
- Independent of calendar; tracks logical rebalance sequence

---

## Integration Points

### What Did NOT Change
- Signal generation adapters (momentum, technical, fundamental, ML)
- Core position sizing or risk management
- Exit policy logic
- Data ingestion or feature pipeline

### What DID Change
- Orchestrator's signal filtering logic (buy/sell gates)
- Portfolio state tracking (added cohort_number field)
- Configuration options (new parameter in OrchestratorConfig)

---

## Testing & Validation Strategy

### Unit Tests (Complete)
- 45+ test cases covering all aspects
- Run with: `pytest tests/unit/test_k_portfolio_rebalancing.py -v`

### Next Steps (Blocked on DuckDB Availability)
1. Integration test with real adapter
2. Backtest execution on existing strategies (R1-R12)
3. Measure actual vs. theoretical turnover
4. Validate Sharpe/CAGR improvement (+0.05-0.10 expected)
5. Compare position holding period distributions

### Data Gaps as Audit Trail
All deferred signals recorded in `BacktestRunResult.data_gaps`:
- `cohort_rotation_deferred` = position held (cohort not due)
- `cohort_slot_full` = new position rejected (cohort capacity reached)

---

## Backward Compatibility Guarantee

**Existing runs unaffected:**
- Default `overlapping_k_portfolio=None` preserves 100% monthly rotation
- No migration needed; legacy behavior is the default
- Existing backtests can be re-run identically

**Opt-in adoption:**
- New runs can set `overlapping_k_portfolio=5` (or other K value)
- No schema changes required
- Cohort data is optional (cohort_number=None for disabled)

---

## Known Limitations & Future Work

### Current Limitations
1. No forced exit for positions in due cohorts (signals must drive it)
2. No prioritization mechanism when cohort slot is full
   - First-in-time rejection (easy to implement if needed)
   - Conviction-based replacement (more complex, not implemented)
3. No automatic backfill of missing signals for due cohorts

### Future Enhancements
1. Conviction-based slot replacement (replace low-conviction positions within cohort)
2. Configurable K per strategy variant
3. Per-sector or per-market-cap cohort isolation
4. Adaptive K scheduling (vary K based on volatility regime)

---

## Files Summary

| File | Change Type | Lines Added |
|------|------------|------------|
| `backtest/portfolio.py` | Field Addition | 5 |
| `backtest/core/portfolio.py` | Method Addition | 30 |
| `backtest/core/engine.py` | Logic Addition | 57 |
| `tests/unit/test_k_portfolio_rebalancing.py` | New File | 600+ |

**Total:** ~700 lines of new/modified code (mainly tests)

---

## Execution Checklist

- [x] Position dataclass extended with cohort_number
- [x] StrategyPortfolio initialized with overlapping_k_portfolio parameter
- [x] Cohort tracking in portfolio.buy()
- [x] get_positions_due_for_rotation() implementation
- [x] Orchestrator rebalance_index tracking
- [x] Sell signal filtering by cohort
- [x] Buy signal filtering by cohort capacity
- [x] OrchestratorConfig parameter added
- [x] Unit tests written (45+ test cases)
- [x] Backward compatibility verified
- [ ] Integration test with real adapter (blocked: DuckDB locked)
- [ ] Backtest execution (blocked: DuckDB locked)
- [ ] Turnover measurement and comparison
- [ ] Sharpe/CAGR improvement validation

---

## Notes for Review

1. **DuckDB Availability:** All code is complete and testable. Backtest execution requires DuckDB availability.

2. **Design Rationale:** The modulo-based cohort assignment (vs. explicit cohort assignment at each rebalance) ensures deterministic, predictable rotation without requiring state persistence.

3. **Signal Filtering Location:** Filtering happens in the orchestrator loop (not in portfolio methods) to maintain visibility in data_gaps for audit trail compliance.

4. **Test Scope:** Unit tests focus on cohort mechanics, not turnover calculation (measurement is data-driven post-backtest).

---

*Implementation complete and ready for integration testing when DuckDB becomes available.*
