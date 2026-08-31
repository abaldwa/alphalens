# R13 Execution Verification Plan

**Date:** 2026-08-31  
**Strategy:** R13 (Bollinger Band Mean-Reversion)  
**Phase:** Phase 13  
**Status:** Ready for queue execution (pending DuckDB availability)

---

## 1. Validation Queue Status

### 1.1 Queue Configuration

| Parameter | Value | Notes |
|-----------|-------|-------|
| Queue file | `backtest/queues/r13_validation_2019_2025.json` | Validation only (fast) |
| Period | 2019-01-01 → 2025-12-31 | 7-year fast validation period |
| Bands tested | M1, M2 (bands 1-2 only) | Initial validation scope |
| Rank method | `bollinger_mean_reversion` | Oversold proximity scoring |
| Lookback | 20 days | Standard Bollinger Band (MA + 2σ) |
| Rebalance cadence | 21 days | 3-week cycle (intentional; faster mean-reversion capture) |
| Top N | 15 stocks per band | Contrarian oversold selection |
| Total jobs | 2 (parallel execution ready) | M1 and M2 backtests in parallel |
| Expected duration | 1–2 hours (2-way parallel) | Includes trade execution + integrity checks |
| Database mode | `defer_db_writes: true` | Concurrent execution safe |

### 1.2 Pre-Execution Checklist

- [ ] DuckDB available (check: `fuser ~/.local/share/AlphaLens/data/*.duckdb`)
- [ ] Scheduler stopped (`systemctl --user stop alphalens-scheduler.service`)
- [ ] Feature store snapshots current (`feature_store/snapshots/latest/`)
- [ ] Environment ready (`PYTHONPATH=$PWD; .venv/bin/python`)
- [ ] No concurrent backtest runs (`python3 -c "import duckdb; db = duckdb.connect('~/.local/share/AlphaLens/data/backtest.duckdb', read_only=True); print(db.execute('SELECT COUNT(*) FROM runs WHERE status=\"running\"').fetchall())"`)

### 1.3 Execution Command

```bash
PYTHONPATH=$PWD .venv/bin/python backtest/run_strategy_queue.py backtest/queues/r13_validation_2019_2025.json
```

**Expected output:**
```
Running 2 jobs in parallel:
  - r13_bollinger_reversion_band1 (M1: Nifty 50)
  - r13_bollinger_reversion_band2 (M2: NiftyNext50)
...
✅ Validation complete: M1 Sharpe=X.XX, M2 Sharpe=Y.YY
```

---

## 2. Test Strategy & Decision Gates

### 2.1 Phase 1: Validation (Bands 1–2)

**Objective:** Confirm signal strength across two representative bands before scaling to all 12.

**Execution:**
1. Run validation queue (`r13_validation_2019_2025.json`)
2. Extract Sharpe ratios for M1 and M2
3. Compare vs. expected range (0.50–0.70)
4. Record actual outcomes in this document

**Decision Gate: Sharpe Ratio Threshold**

| Outcome | Sharpe Range | Decision | Next Step |
|---------|--------------|----------|-----------|
| Weak signal | < 0.50 | ARCHIVE R13 | Document in backlog; retire strategy |
| Valid signal | 0.50–0.70 | EXPAND TO ALL BANDS | Proceed to Phase 2 |
| Strong signal | > 0.70 | FAST-TRACK | Consider for composite immediately |

**Expected Sharpe (0.50–0.70):** Mean-reversion effects typically smaller than momentum (R1 ≈ 0.68–0.95), so 0.50–0.70 is realistic for contrarian strategy.

### 2.2 Phase 2: Full-Band Expansion (if Sharpe ≥ 0.50)

**Objective:** Validate R13 across all 12 market-cap bands for production readiness.

**Execution:**
1. Register all band entries: `python3 strategies/migrations/r13_bollinger_mean_reversion.py --all-bands`
2. Run full queue: `backtest/queues/r13_full_2009_2026.json` (covers 2009–2026, all 12 bands)
3. Extract per-band Sharpe ratios (M1–M12)
4. Compare vs. band-specific benchmarks (R1 momentum Sharpe for same band)

**Expected duration:** ~4–5 hours (12 bands in parallel, subject to resource availability)

**Validation criteria:**
- [ ] Sharpe consistent across all bands (ρ correlation > 0.50 with band size/liquidity)
- [ ] Drawdown profile acceptable (< -40% maximum; ideally < -30%)
- [ ] Win rate > 40% (contrarian strategies often have lower frequency)
- [ ] No overfitting markers in walk-forward test (if robust-checks applied)

### 2.3 Phase 3: Robustness Verification

**Objective:** Ensure R13 holds up under stress and sub-period conditions.

**Tests:**
1. **Out-of-sample hold-out:** Reserve final 6 months (2025-07 to 2025-12) as hold-out; backtest only on 2019–2025-06
2. **Sub-period breakdown:** Compare 2019–2022 Sharpe vs. 2023–2025 Sharpe
   - If divergence > 0.30 Sharpe → Signal regime-dependent; document
3. **Benchmark comparison:** Compare R13 Sharpe vs. naive Bollinger Band strategy (no optimization):
   - e.g., "Buy when price < lower BB; Sell when price > MA" (baseline)
   - R13 vs. baseline Δ Sharpe should be > +0.10 to justify complexity
4. **Correlation check:** Verify R13 is independent of R1 momentum (correlation < 0.50)
   - If ρ > 0.70 → R13 is redundant; archive

---

## 3. Execution Timeline & Results

### 3.1 Validation Phase Results (Phase 1)

**Status:** Pending execution  
**Actual Sharpe (M1):** _____ (target: 0.50–0.70)  
**Actual Sharpe (M2):** _____ (target: 0.50–0.70)  
**Execution date:** ______  
**Execution duration:** ___ hours  
**Decision:** [ ] Archive | [ ] Expand to all bands | [ ] Fast-track

**Notes:**
```
[Backtest results summary to be filled after queue completion]
```

### 3.2 Full-Band Expansion Results (Phase 2, if applicable)

**Status:** Pending (contingent on Phase 1 Sharpe ≥ 0.50)  
**Execution date:** ______  
**Execution duration:** ___ hours  

**Per-band results:**
```
M1 Sharpe: ____ | Max DD: ___% | Win rate: ___% | Correlation to R1: ____
M2 Sharpe: ____ | Max DD: ___% | Win rate: ___% | Correlation to R1: ____
M3 Sharpe: ____ | Max DD: ___% | Win rate: ___% | Correlation to R1: ____
[... M4–M12 results ...]
```

### 3.3 Robustness Check Results (Phase 3, if applicable)

**Hold-out test (2025-07 to 2025-12):**
- Sharpe (in-sample 2019–2025-06): _____
- Sharpe (hold-out 2025-07–12): _____
- Degradation: ___% (acceptable if < 20%)

**Sub-period stability:**
- 2019–2022 Sharpe: _____
- 2023–2025 Sharpe: _____
- Divergence: _____ (document if > 0.30)

**Benchmark comparison (naive vs. optimized R13):**
- Naive BB Sharpe: _____
- R13 Sharpe: _____
- Δ Sharpe: _____ (target: > +0.10)

**Correlation to R1:**
- ρ(R13, R1): _____ (target: < 0.50)

---

## 4. Next Steps & Backlog Integration

### 4.1 If Sharpe ≥ 0.50 (Validation Passed)

1. **Update registry:** Full R13 entries active (all 12 bands)
2. **Signal integration:** Wire R13 signals into live signal generation pipeline
3. **Composite strategy:** Evaluate R13 inclusion in composite momentum portfolio
4. **Backlog items:**
   - [ ] B-XXX: Wire R13 signals to paper_trading module
   - [ ] B-XXX: Add R13 to composite strategy weighting scheme
   - [ ] B-XXX: Document R13 in live trading manual

### 4.2 If Sharpe < 0.50 (Validation Failed)

1. **Archive decision:** R13 archived as inactive strategy
2. **Backlog entry:** B-XXX: Investigate mean-reversion signal weakness; alternative bands or lookback parameters
3. **Documentation:** Add R13 to "Strategies Evaluated & Retired" section of STRATEGY_ARCHIVE.md

### 4.3 If Sharpe > 0.70 (Fast-Track)

1. **Immediate expansion:** All 12 bands registered and queued for production
2. **Priority:** Fast-track to composite strategy (high confidence)
3. **Live integration:** Evaluate immediate deployment to paper trading (if other gates pass)

---

## 5. Monitoring & Adjustments

### 5.1 During Validation Queue

- Monitor DuckDB lock holder: `fuser ~/.local/share/AlphaLens/data/*.duckdb`
- Monitor memory usage (watch for OOM): `free -h` and system logs
- Check queue progress: `sqlite3 datastore/pipeline/pipeline_log.db "SELECT * FROM pipeline_checkpoints WHERE step LIKE '%r13%' ORDER BY date DESC LIMIT 5;"`

### 5.2 Post-Validation Adjustments (If Needed)

If Sharpe 0.50–0.70 but shows concerning patterns (high drawdown, low win rate), consider:
- **Lookback adjustment:** Test 15-day or 25-day bands (vs. standard 20-day)
- **Top N adjustment:** Test N=10 or N=20 (vs. standard N=15)
- **Rebalance cadence adjustment:** Test 14-day or 28-day (vs. current 21-day)
- **Exit variant:** Test "momentum_exit" variant (hold until oversold condition reverts)

All adjustments will require new backlog items and re-runs.

---

## 6. Related Documentation

- **Strategy registry:** [docs/strategy-specification-registry.md](../strategy-specification-registry.md#r13-bollinger-band-mean-reversion-contrarian)
- **R13 implementation:** [strategies/migrations/r13_bollinger_mean_reversion.py](../../strategies/migrations/r13_bollinger_mean_reversion.py)
- **Validation queue:** [backtest/queues/r13_validation_2019_2025.json](../../backtest/queues/r13_validation_2019_2025.json)
- **Full queue:** [backtest/queues/r13_full_2009_2026.json](../../backtest/queues/r13_full_2009_2026.json)
- **Feature engineering:** [features/momentum_universe.py](../../features/momentum_universe.py) (RANK_BANDS definition)
- **Backtest engine:** [backtest/core/engine.py](../../backtest/core/engine.py) (execution logic)

---

## 7. Sign-Off & Approval

| Role | Name | Date | Signature |
|------|------|------|-----------|
| Strategy designer | — | — | — |
| Code reviewer | — | — | — |
| ML rigor reviewer | — | — | — |
| Execution lead | — | — | — |

---

**Document history:**
- 2026-08-31: Initial creation (B-031 & B-032 task)

