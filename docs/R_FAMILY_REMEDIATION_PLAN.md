# R-Family Strategy Remediation Plan

**Date:** 2026-08-25  
**Author:** Multi-Agent Review (Product Owner, Domain Expert, Backend Data Engineer, Backtest Reviewer, ML Rigor Reviewer)  
**Status:** ❌ **BLOCKING** — paper trading deployment of R10-R12 halted pending remediation  
**Severity:** 🔴 **CRITICAL** — all 5 independent reviewers reached consensus: **DO NOT SHIP**

---

## Executive Summary

On 2026-08-25, a multi-agent code review of R-family momentum strategies (R10, R11, R12) — presented as "Phase 3 Complete" and ready for paper trading — identified **9 critical issues** spanning strategy validity, data integrity, statistical rigor, and production safety.

**Most critical finding:** The headline results (16.4% CAGR, Sharpe 0.72 for R12 reversal) **exist nowhere in the system** — no runs in `backtest_runs` table, no persisted report JSONs, no traces. The validation queue configurations exist, but were never executed. Claims are unverifiable.

**Collective verdict across 5 independent reviewers:**
1. **Product Owner:** R10-R12 are scope creep; paper trading is blocked on scheduler/dispatch, not strategy quantity
2. **Domain Expert:** R12 likely captures circuit-breaker noise, not genuine behavior; sector concentration masquerading as liquidity effect
3. **Backend Data Engineer:** Infrastructure mostly ready; needs snapshot reconciliation + trade_log schema update
4. **Backtest Reviewer:** R12 failed robustness checks (fold_stability, benchmarks); DSR understates selection bias (100:1 trial count)
5. **ML Rigor:** Backtest results unverifiable; quintile bucketing unwired; signal-cadence mismatch (1mo signal, 3mo hold) unaddressed

**Decision:** Archive R10/R11/R12 from paper trading roadmap until all 9 remediation items (B-014–B-022) are complete and reviewed. Estimate: 2–3 weeks if issues are fixable, longer if findings warrant architectural changes.
               ├─→ B-002: Skip-month default application
               ├─→ B-022: Wire skip-month in configs
               ├─→ B-021: R12 depends on B-001
               │
               └──→ B-011: Validate R1-R12 Sharpe (gate for Phase 3)
```

---

## Phase 1: Foundation (Week 1)

### B-003: Fix momentum-strategy-audit prompt ⚠️ CRITICAL

**Blocker status:** YES — all R-family verdicts depend on this

**What:** Rewrite `docs/agents/strategy-audit-prompts.md` section "1. Momentum-Strategy-Audit Agent" with:
- Specification-as-code (15+ J&T checklist items)
- Forced deviation inventory (ALL deviations, not just notable)
- Citation requirements (exact pages/equations)
- Adversarial verification (what-if questions)
- Multi-pass review (reconsider after adversarial checks)
- Human expert gate (spot-check code before backtest)
- False positive QA tests (test with deliberate violations)

**Owner:** Claude drafts, you review  
**Time:** 2-3 hours (prompt rewrite + validation testing)  
**Validation:** Test against R1 code; should flag overlapping portfolios + skip-month as BLOCKERS

**Cross-reference:** See [MOMENTUM_REVIEW_PLAN.md § Review Phase 1](MOMENTUM_REVIEW_PLAN.md#review-phase-1-fix-the-audit-critical)

**Success criteria:**
- ✅ Audit finds overlapping portfolios as BLOCKER (not missed)
- ✅ Audit finds skip-month as HIGH (not missed)
- ✅ Audit recommends B-001 + B-002 creation
- ✅ False positive tests pass (agent rejects deliberately broken specs)

---

## Phase 2: Core Fixes (Weeks 2-3)

Once B-003 approved, these 5 items unblock everything else.

### B-001: Implement overlapping portfolios (ALL R-FAMILY) 🔴 BLOCKER

**Affects:** R1, R3, R4, R5, R6, R8, R9, R10, R11, R12 (100% of momentum strategies)

**What:** Refactor R1-R12 to use K overlapping sub-portfolios with 1/K monthly replacement

- Current: 100% portfolio turnover each month
- Target: K sub-portfolios, 1/K replaced each month (typically K=3 or K=12)
- Result: ~0.05-0.10 Sharpe improvement + ~2-3% cost savings annually

**Where:** `strategies/momentum_identity.py` (core ranking engine)

**Implementation:**
1. Refactor `portfolio_replacement()` → `overlapping_k_portfolio_replacement(k=3, monthly_turnover=1/k)`
2. Track K sub-portfolios separately (positions, weights, rebalance dates)
3. Update backtesting engine to handle K-portfolio state
4. Update paper-trading signal generator to respect K-portfolio allocation

**Owner:** Implementation required (Claude can draft, you review + test)  
**Time:** 4-6 hours (algorithm + testing)  
**Validation:** 
- Re-run R1 backtest; Sharpe should improve ~0.05-0.10
- Verify transaction costs match J&T expectations
- Test all R-family strategies with new overlapping structure

**Unblocks:** B-002, B-011, B-012, B-021, B-022 (and R1-R12 backtesting)

**Cross-reference:** [backlog_items.yaml § B-001](../backlog_items.yaml)

---

### B-017: Fix R8 rebalance cadence (21-30d monthly, not 252d annual) 🔴 BLOCKER

**Affects:** R8 only (but R8 is promised vol-scaling strategy)

**What:** R8 (Barroso & Santa-Clara vol-scaling) uses 252-day annual rebalance; spec requires monthly

**Current code:** Hardcoded 252-day frequency  
**Fix:** Change to 21-30 day monthly rebalance frequency

**Impact:** Annual → monthly is fundamental change
- Annual: vol-scaling benefit is lost (long periods of suboptimal exposure)
- Monthly: vol-scaling activates on schedule (matches Barroso & Santa-Clara spec)

**Where:** `strategies/barroso_santa_clara_vol_momentum.py` or wherever R8 is defined

**Implementation:**
1. Find rebalance frequency configuration
2. Change 252 → 21 (or 30 for calendar month-end)
3. Verify vol-scaling logic applies on new schedule
4. Test that target volatility updates monthly

**Owner:** Implementation + testing  
**Time:** 1-2 hours  
**Validation:** 
- Re-backtest R8; Sharpe should improve toward 0.75-0.85 range
- Verify vol scaling recalculates monthly

**Unblocks:** B-011 (R8 validation)

**Cross-reference:** [backlog_items.yaml § B-017](../backlog_items.yaml)

---

### B-018: R9 4-mode vol-scaling + regime gate implementation 🔴 BLOCKER

**Affects:** R9 only (but R9 is primary vol-scaling strategy)

**What:** R9 (Moreira & Muir 2017) requires TWO major features:
1. 4-mode discrete exposure bucketing (100%/75%/50%/25% based on vol quantiles)
2. Regime gate (EMA-RSI state machine) to dynamically select mode

**Current state:** Has continuous scaling formulas, no regime gate → tests 4 variants separately

**Fix:** Implement:
1. **Regime gate:** EMA-RSI or similar indicator to classify market regime (4 states)
2. **4-mode exposure logic:** Based on regime, set portfolio exposure (100%/75%/50%/25%)
3. **Integrate:** Single strategy with adaptive mode switching (not 4 separate backtests)

**Where:** Create or refactor `strategies/moreira_muir_vol_momentum.py` with regime gate

**Implementation:**
1. Implement EMA-RSI regime detector (4 states: low/med-low/med-high/high vol)
2. Map regimes → exposure targets (100%/75%/50%/25%)
3. Implement discrete mode bucketing (not continuous scaling)
4. Validate live-backtest parity (regime gate must be identical in both)

**Owner:** Implementation + careful design (regime gate is complex)  
**Time:** 6-8 hours (design + implementation + testing)  
**Validation:** 
- Backtest with regime gate enabled; compare to single-mode baseline
- Verify Sharpe improves to 0.75-0.85 range
- Check drawdown reduction vs. R1 base

**Unblocks:** B-011 (R9 validation)

**Cross-reference:** [backlog_items.yaml § B-018](../backlog_items.yaml)

---

### B-019: R10 sector momentum (fix individual stock → sector ranking) 🔴 BLOCKER

**Affects:** R10 only (sector momentum variant)

**What:** R10 should rank SECTORS by momentum, not individual stocks with sector labels

**Current:** Uses individual stock ranking (wrong methodology)  
**Fix:** Implement true sector-level momentum:
1. Calculate sector-level returns (aggregate OHLCV by sector)
2. Rank sectors (3/6/9/12 month lookbacks on sector returns)
3. Hold all stocks in top sectors (or top N by ADTV)

**Where:** Create or refactor `strategies/sector_momentum.py`

**Implementation:**
1. Group stocks by sector
2. Calculate sector-level returns for each lookback
3. Rank sectors by momentum
4. Build portfolio from top sectors
5. Verify portfolio is sector-concentrated (not stock-concentrated)

**Owner:** Implementation  
**Time:** 3-4 hours  
**Validation:** 
- Backtest R10 with new sector-level logic
- Expected Sharpe: 0.6-0.8 (lower than individual due to concentration)
- Verify portfolio sector concentration

**Unblocks:** B-011 (R10 validation)

**Cross-reference:** [backlog_items.yaml § B-019](../backlog_items.yaml)

---

### B-020: R11 ranking direction + threshold (inversion + 70% filter) 🔴 BLOCKER

**Affects:** R11 only (price-based momentum)

**What:** R11 (George & Hwang 2004 52-week high) has TWO critical inversions:

1. **Ranking direction:** Selects HIGH price-to-high (wrong); should select LOW
   - LOW = closer to bottom = stronger signal (contrarian)
   - HIGH = closer to top = weak signal (trend-following)
   
2. **70% threshold missing:** Should buy only when price < 70% of 52-week high
   - Current: Ranks all stocks
   - Required: Filter to price < 70% of high, then rank

**Fix:**
1. Invert ranking (select LOW price-to-high ratio, not HIGH)
2. Implement 70% threshold filter (buy: price < 0.70 * 52w_high)
3. Verify holding period and rebalance frequency

**Where:** `strategies/george_hwang_52week_high.py`

**Implementation:**
1. Change ranking to `sort ascending` (LOW ratios first)
2. Add threshold filter: `if price < 0.70 * high_52w: buy`
3. Test contrarian behavior (should underperform in strong bull markets)

**Owner:** Implementation  
**Time:** 1-2 hours (mostly logic inversion)  
**Validation:** 
- Backtest R11 with fixes; should show contrarian behavior
- Expected Sharpe: 0.6-0.8
- Verify results different from trend-following baseline

**Unblocks:** B-011 (R11 validation)

**Cross-reference:** [backlog_items.yaml § B-020](../backlog_items.yaml)

---

## Phase 3: Missing Implementations (Week 2-3 parallel)

These can run in parallel with Phase 2 core fixes.

### B-014: Implement R4 momentum variant 🔴 BLOCKER

**Affects:** R4 only (if promised in strategy suite)

**What:** R4 is listed in registry but not implemented

**Current state:** MISSING  
**Decision point:** Do you want R4 in final suite?

**If YES:**
1. Define R4 lookback periods in registry (which of 3/6/9/12? all of them?)
2. Implement R4 as J&T variant with specified lookbacks
3. Use same overlapping portfolio structure as R1 (after B-001)

**If NO:**
1. Remove R4 from registry
2. Update documentation (strategy suite is R1, R3, R5, R7-R12)

**Owner:** Decision (you) + implementation (if YES)  
**Time:** 0 hours (if NO) or 2-3 hours (if YES, after B-001)  
**Validation:** If implemented, backtest R4; Sharpe should match or exceed R1 baseline

**Unblocks:** B-011 (R4 validation, if implemented)

**Cross-reference:** [backlog_items.yaml § B-014](../backlog_items.yaml)

---

### B-016: Implement R6 momentum variant 🔴 BLOCKER

**Affects:** R6 only (if promised in strategy suite)

**What:** R6 is listed in registry with vague spec, no code

**Current state:** MISSING (spec incomplete)  
**Decision point:** Do you want R6 in final suite?

**If YES:**
1. Clarify R6 specification (lookback periods? momentum focus?)
2. Implement R6 using overlapping portfolio structure (after B-001)

**If NO:**
1. Remove R6 from registry
2. Update documentation

**Owner:** Decision (you) + implementation (if YES)  
**Time:** 0 hours (if NO) or 2-3 hours (if YES, after B-001)

**Unblocks:** B-011 (R6 validation, if implemented)

**Cross-reference:** [backlog_items.yaml § B-016](../backlog_items.yaml)

---

### B-015: Fix R5 registry (George & Hwang mismatch) 🔴 CRITICAL

**Affects:** R5 + future audits (registry authority)

**What:** R5 registry says "J&T variant" but code implements George & Hwang 52-week high

**Decision point:** 
- **Option A (RECOMMENDED):** Update registry to correctly document R5 as George & Hwang variant
- **Option B:** Reimplement R5 to match J&T spec

**Recommendation:** Option A (registry authority principle)
- Registry is source of truth for strategy specs
- Code matches published George & Hwang, not J&T
- Updating registry is lower risk than reimplementing

**If Option A:**
1. Update `docs/strategy-specification-registry.md` R5 section
2. Document as "George & Hwang (2004) 52-Week-High Momentum variant"
3. Update source papers reference
4. Move R5 to after R11 in registry (related strategies)

**If Option B:**
1. Reimplement R5 to use J&T overlapping portfolios
2. Remove George & Hwang logic from R5

**Owner:** You (decision) + Claude (implementation)  
**Time:** 0.5-1 hour (if Option A) or 2-3 hours (if Option B)  
**Validation:** If Option A, registry now matches code. If Option B, backtest new R5 variant.

**Unblocks:** B-011 (R5 validation)

**Cross-reference:** [backlog_items.yaml § B-015](../backlog_items.yaml)

---

## Phase 4: Configuration & Wiring (Week 3)

### B-002: Skip-month implementation (already exists, needs wiring)

**Status:** Implementation exists (`trailing_momentum_skip_recent()`), just needs configuration

**What:** Wire skip-month into R1-R12 strategy configs as default

**Current:** Skip-month is opt-in parameter; most backtests don't use it  
**Target:** Skip-month is default; explicitly disable if needed

**Where:** `backtest_exclusions.py` + strategy configuration files

**Implementation:**
1. Set `skip_months=True` as default for all momentum strategies
2. Update strategy registry to document skip-month as enabled
3. Test that skip-month calculates correctly

**Owner:** Configuration update + testing  
**Time:** 1 hour  
**Validation:** Re-run R1 backtest; Sharpe should match 0.90-0.95 (vs. 0.85 without skip)

**Unblocks:** B-022, B-011

**Cross-reference:** [backlog_items.yaml § B-002](../backlog_items.yaml)

---

### B-022: Document skip-month default in all configs

**Status:** Dependent on B-002

**What:** Once skip-month is wired as default, update all strategy configs to show this

**Where:** Strategy documentation, config files, backtest templates

**Owner:** Documentation  
**Time:** 1 hour

**Unblocks:** None (final documentation)

**Cross-reference:** [backlog_items.yaml § B-022](../backlog_items.yaml)

---

### B-021: R12 configuration fixes (after B-001 + B-002)

**Affects:** R12 only

**What:** R12 inherits overlapping portfolio fix (B-001) + skip-month default (B-002)

**Also:** Document ADTV floor as mandatory (no bypass option)

**Owner:** Configuration update  
**Time:** 0.5 hour (after B-001, B-002)

**Cross-reference:** [backlog_items.yaml § B-021](../backlog_items.yaml)

---

## Phase 5: Validation (Week 4)

### B-011: Verify R1-R12 Sharpe ratios vs. published benchmarks

**Status:** Depends on B-001, B-002, and all implementation fixes (B-014-B-020)

**What:** Once all blockers fixed, re-run R1-R12 backtests and compare to expected benchmarks

**Expected results (post-fixes):**
- R1: 0.68-0.95 Sharpe (overlapping portfolios + skip-month)
- R3: 0.91+ Sharpe (skip-month benefit)
- R7: ~0.80 Sharpe (crash-aware, lower but less drawdown)
- R8: 0.75-0.85 Sharpe (vol-scaling, after cadence fix)
- R9: 0.75-0.85 Sharpe (4-mode vol-scaling, after regime gate)
- R10: 0.6-0.8 Sharpe (sector momentum)
- R11: 0.6-0.8 Sharpe (contrarian price-based)
- R12: 0.7-0.9 Sharpe (India-specific momentum)
- R4, R6: Depends on implementation

**Validation:**
1. Run full R-family backtest suite
2. Compare Sharpe to expected benchmarks
3. Flag if >5% deviation (needs investigation)
4. Document assumptions (ADTV floors, market cap bands, etc.)

**Owner:** Automated validation + you review  
**Time:** 2 hours (backtests + comparison)

**Gate:** Must pass before Phase 3 trials can start

**Cross-reference:** [backlog_items.yaml § B-011](../backlog_items.yaml)

---

## Timeline Summary

| Phase | Task | Time | Blocker? | Depends on |
|-------|------|------|----------|-----------|
| **1** | B-003: Fix audit prompt | 2-3h | ✅ YES | Nothing |
| **2a** | B-001: Overlapping portfolios | 4-6h | ✅ YES | B-003 |
| **2b** | B-017: R8 cadence fix | 1-2h | ✅ YES | B-003 |
| **2c** | B-018: R9 regime gate + 4-mode | 6-8h | ✅ YES | B-003 |
| **2d** | B-019: R10 sector logic | 3-4h | ✅ YES | B-003 |
| **2e** | B-020: R11 ranking + threshold | 1-2h | ✅ YES | B-003 |
| **3a** | B-014: Implement R4 (if yes) | 2-3h | ✅ YES | B-001, Decision |
| **3b** | B-016: Implement R6 (if yes) | 2-3h | ✅ YES | B-001, Decision |
| **3c** | B-015: R5 registry fix (Option A preferred) | 0.5-1h | ✅ CRITICAL | B-003 |
| **4a** | B-002: Wire skip-month defaults | 1h | HIGH | B-001 |
| **4b** | B-022: Document skip-month | 1h | HIGH | B-002 |
| **4c** | B-021: R12 configuration | 0.5h | HIGH | B-001, B-002 |
| **5** | B-011: Validate Sharpe benchmarks | 2h | GATE | All Phase 2-4 |
| | | | | |
| | **TOTAL (critical path)** | **~20-22 hours** | | |
| | **TOTAL (with R4+R6)** | **~26-28 hours** | | |
| | **Elapsed time (serial)** | **3-4 weeks** | | |

---

## Parallel Work Streams

**Stream 1 (Core momentum fixes):** B-001 → B-017, B-018, B-019, B-020 (can run in parallel)
**Stream 2 (Missing implementations):** B-014, B-016 (can run in parallel after B-001)
**Stream 3 (Configuration):** B-002, B-022, B-021 (can run after their dependencies)
**Stream 4 (Validation):** B-011 (must be last)

**Estimated parallel time:** 2-3 weeks (vs. 4 weeks serial)

---

## Success Criteria

### Phase 1
- ✅ B-003 audit prompt accepts/rejects R1 correctly
- ✅ No false positives; catches all BLOCKER/CRITICAL items

### Phase 2
- ✅ B-001: R1-R12 backtests show Sharpe improvement +0.05-0.10
- ✅ B-017: R8 backtest shows vol-scaling activating monthly
- ✅ B-018: R9 backtest with regime gate enables dynamic mode switching
- ✅ B-019: R10 backtest shows sector-level concentration (not stock-level)
- ✅ B-020: R11 backtest shows contrarian behavior (not trend-following)

### Phase 3
- ✅ R4, R6 implemented and passing spec checklist (if included)
- ✅ R5 registry matches code

### Phase 4
- ✅ Skip-month default wired; backtests show Sharpe aligned with benchmarks
- ✅ R12 configuration includes mandatory ADTV floor

### Phase 5
- ✅ R1-R12 Sharpe ratios within ±5% of expected benchmarks
- ✅ Phase 3 trials can proceed (all momentum strategies validated)

---

## Risk & Mitigation

| Risk | Impact | Mitigation |
|------|--------|-----------|
| B-001 refactor breaks other strategies | HIGH | Test all R-family after each change; use feature branch |
| B-018 regime gate complexity | HIGH | Design document + code review before implementation |
| R4/R6 decisions delay timeline | MEDIUM | Make decision early; document in registry if excluding |
| R-family backtests OOM | MEDIUM | Use snapshot parquets; run single-symbol validation first |
| Live-backtest parity for R9 regime gate | MEDIUM | Test regime gate calculation identically in both |

---

## Cross-References

- **Backlog:** [backlog_items.yaml](../backlog_items.yaml) (B-001 through B-022)
- **Audit Results:** Summary above shows all 9 BLOCKER findings
- **Strategy Registry:** [docs/strategy-specification-registry.md](strategy-specification-registry.md)
- **Phase 3 Trials:** [docs/PHASE_3_TRIALS_GUIDE.md](PHASE_3_TRIALS_GUIDE.md) (awaits B-011 gate)
- **Momentum Review Plan:** [docs/MOMENTUM_REVIEW_PLAN.md](MOMENTUM_REVIEW_PLAN.md) (Phase 1-4 overview)

---

## Next Step

**Immediate:** Start Phase 1 (B-003 audit prompt rewrite)  
**Decision needed:** R4/R6 inclusion (yes/no) → informs Phase 3 timeline

Once B-003 approved, the critical path is clear: **B-001 immediately unblocks 5+ items.**

