# Momentum Strategy Review Plan — Phase 3 Foundation

**Date:** 2026-08-25  
**Scope:** Intensive review of R1-R12 momentum strategies before Phase 3 trials  
**Goal:** Ensure momentum is bulletproof before expanding to technical/fundamental strategies

---

## Critical Path: What Blocks Everything

```
B-003 (FIX AUDIT PROMPT)
  ↓ [REQUIRED BEFORE ALL OTHER AUDITS]
  ├─ B-001 (overlapping portfolios)
  ├─ B-002 (skip-month)
  ├─ B-004 (verify lookbacks)
  ├─ B-005 (verify universe filtering)
  ├─ B-006 (verify rebalance frequency)
  └─ B-010 (audit R3 skip-month)
  
  [ALL ABOVE MUST COMPLETE]
  ↓
  B-011 (verify R1-R12 Sharpe ratios)
  ↓
  [PHASE 3 TRIALS CAN BEGIN]
```

---

## Review Phase 1: Fix the Audit (CRITICAL)

### **Task 1: Rewrite momentum-strategy-audit prompt**

**What:** Update `docs/agents/strategy-audit-prompts.md` momentum-strategy-audit section

**7 Prevention Mechanisms to Add:**

1. **Specification-as-code** (15+ explicit checklist items)
   ```
   JEGADEESH & TITMAN (1993) SPECIFICATION CHECKLIST
   ☐ Overlapping portfolios: K sub-portfolios, 1/K replaced monthly (not 100% replacement)
   ☐ Ranking window: months -K to -2 (NOT -K to 0)
   ☐ Skip month: month -1 excluded from ranking (bid-ask bounce prevention)
   ☐ Rebalance frequency: monthly (30 calendar days, not 21d variants)
   ☐ Holding period: standard momentum (3/6/9/12 months)
   ☐ Universe: broad equity universe (no survivorship bias)
   ☐ Transaction costs: accounted for in backtests
   ☐ Bid-ask spread: modeled or verified as immaterial
   [... 7 more items]
   ```

2. **Forced Deviation Inventory** (enumerate ALL deviations, not just surprising ones)
   ```
   Create a table showing code vs. specification:
   
   | Requirement | J&T Spec | Code | Matches? | Risk |
   |-------------|----------|------|----------|------|
   | Overlapping portfolios | K sub-portfolios | 100% replacement | ❌ NO | BLOCKER |
   | Skip-month | -K to -2 | -K to 0 | ❌ NO | HIGH |
   | Rebalance frequency | 30d monthly | 21d (R7 only) | ✓ VARIANT | MEDIUM |
   | Universe | Broad | ADTV floor applied | ✓ RESTRICTED | MEDIUM |
   [... more rows]
   
   Risk column: BLOCKER, CRITICAL, HIGH, MEDIUM, LOW, or N/A
   ```

3. **Citation Requirements** (specific pages/equations, not paraphrasing)
   ```
   For each research claim, cite:
   - Source: "Jegadeesh & Titman (1993), The Journal of Finance Vol. 48 No. 1"
   - Page/section: "Page 65-80, Section 3 'Construction of Momentum Portfolios'"
   - Exact quote: "Portfolios are reformed every month, with 1/K of the portfolio replaced"
   - Not: "The paper discusses monthly rebalancing" (paraphrase, vague)
   ```

4. **Adversarial Verification** (what-if questions)
   ```
   After finding deviations, ask:
   "If overlapping portfolios are missing, would transaction costs match J&T?
    → NO — full replacement has 2-3x higher costs"
   
   "If skip-month is missing, would Sharpe ratio match J&T?
    → NO — bid-ask bounce reduces Sharpe by ~0.05-0.10"
   
   "Would results still be comparable to published research?
    → NO — fundamental differences in construction affect all metrics"
   ```

5. **Multi-Pass Review** (reconsider verdict after adversarial checks)
   ```
   Initial verdict: "Based on lookback periods and rebalance frequency, looks good"
   
   Adversarial check: "But overlapping portfolios missing..."
   
   Revised verdict: "⚠️ APPROVED WITH CRITICAL NOTES (can't approve without overlapping)"
   ```

6. **Human Expert Gate** (user spot-check before backtest approval)
   ```
   Agent output: "I found overlapping portfolios missing. Should I:
   a) Block backtest (BLOCKER severity)
   b) Require human code review (CRITICAL severity)
   c) Just note it (HIGH severity)
   
   Recommendation: (b) — requires human expert spot-check"
   ```

7. **False Positive QA Tests** (test agent with deliberate violations)
   ```
   Test case 1: R1 code with overlapping portfolios removed
   Expected verdict: BLOCKED (not APPROVED)
   
   Test case 2: R1 code with skip-month removed
   Expected verdict: APPROVED WITH CRITICAL NOTES (blocks live deployment)
   
   Test case 3: R1 code 100% correct
   Expected verdict: APPROVED (can proceed to backtest)
   ```

**Owner:** You (Claude can draft, you review)  
**Time:** 1-2 hours  
**Validation:** Test against R1 code (should find overlapping + skip-month issues)

---

## Review Phase 2: Audit R-Family Strategies (SERIAL, depends on Phase 1)

Once B-003 (audit prompt) is fixed, run audits in order:

### **Task 2: Audit R1 (Core Momentum)**

**Run:**
```
momentum-strategy-audit on R1 with improved prompt

Expected output:
- ❌ Overlapping portfolios missing (BLOCKER)
- ❌ Skip-month not implemented (HIGH)
- ✓ Lookback periods correct (3/6/9/12)
- ✓ Rebalance frequency correct (monthly)

Verdict: ⚠️ APPROVED WITH CRITICAL NOTES
Backlog items created:
- B-001 (BLOCKER) Overlapping portfolios
- B-002 (HIGH) Skip-month variant
- B-004 (MEDIUM) Lookback verification [APPROVED]
```

**Owner:** Automated (agent)  
**Time:** 15 minutes  
**Validation:** Compare verdict to expected deviations above

---

### **Task 3: Audit R3 (Skip-Month Variant)**

**Run:**
```
momentum-strategy-audit on R3 (skip-month variant)

Expected output:
- ❌ Overlapping portfolios missing (BLOCKER, same as B-001)
- ✓ Skip-month implemented correctly
- ✓ Lookback periods correct

Verdict: ⚠️ APPROVED WITH CRITICAL NOTES (B-001 blocks despite correct skip-month)
Backlog items:
- Link B-001 (now affects R1 AND R3)
```

**Owner:** Automated (agent)  
**Time:** 15 minutes  
**Validation:** Verify skip-month logic is correct in code

---

### **Task 4: Audit R4-R6 (Momentum Variants)**

**Run:** momentum-strategy-audit on R4, R5, R6 (can run in parallel)

**Expected:** All blocked by B-001 (overlapping portfolios missing)

**Owner:** Automated (agent)  
**Time:** 45 minutes total (15 min each in parallel)

---

### **Task 5: Audit R7 (Crash-Aware Momentum)**

**Run:**
```
momentum-strategy-audit on R7

Expected output:
- ❌ Overlapping portfolios missing (BLOCKER)
- ❌ Rebalance cadence 21d (not 30d monthly) [INTENTIONAL VARIANT]
- ✓ Regime-based position sizing (EMA-RSI gate)

Verdict: ⚠️ APPROVED WITH CRITICAL NOTES
New backlog items:
- B-001 link (also affects R7)
- B-008 (MEDIUM) Document 21d cadence rationale
```

**Owner:** Automated (agent)  
**Time:** 15 minutes  
**Validation:** Verify R7 cadence choice is intentional (crash detection priority)

---

### **Task 6: Audit R8-R9 (Vol-Scaling Variants)**

**Run:** momentum-strategy-audit on R8 (Barroso-Santa-Clara), R9 (Moreira-Muir) (parallel)

**Expected:** Both blocked by B-001 (overlapping portfolios)

**Owner:** Automated (agent)  
**Time:** 30 minutes total (15 min each in parallel)

---

### **Task 7: Audit R10-R12 (Diversity/Risk Variants)**

**Run:** momentum-strategy-audit on R10, R11, R12 (parallel)

**Expected:** All blocked by B-001

**Owner:** Automated (agent)  
**Time:** 45 minutes total (15 min each in parallel)

---

## Review Phase 3: Fix Blockers (IMPLEMENTATION)

Once all R1-R12 audits complete, backlog shows:

```
BLOCKER (prevent backtest):
└─ B-001: Overlapping portfolios [affects R1-R12]

HIGH (significant):
├─ B-002: Skip-month variant [affects R1, R4-R12]
└─ B-006: Verify rebalance frequency [needs doc]

MEDIUM:
├─ B-004: Lookback verification [already correct, just approve]
├─ B-005: Universe filtering doc [design choice, document]
├─ B-008: R7 cadence rationale [document 21d choice]
├─ B-009: Crash cadence testing [waiting on results]
└─ B-010: R3 skip-month verification [should be correct]
```

### **Task 8: Implement Overlapping Portfolios (B-001)**

**What:** Refactor R1-R12 to use K overlapping sub-portfolios with 1/K monthly replacement

**Where:** `strategies/momentum_identity.py` (core momentum engine)

**Implementation approach:**
1. Change from: `full_portfolio_replacement_monthly()`
2. To: `overlapping_k_portfolio_replacement(k=3, replacement_rate=1/k)`
3. For R1: likely K=3 or K=4 (Fama-French typically K=3)
4. Verify: Transaction costs drop; Sharpe approaches J&T baseline

**Owner:** Implementation required (not just review)  
**Time:** 4-6 hours (including refactor + testing)  
**Validation:** Re-run R1 backtest; Sharpe should improve ~0.05-0.10

**Blockers until done:** ALL R-family backtests blocked

---

### **Task 9: Implement Skip-Month Variant (B-002)**

**What:** Create skip-month ranking option (exclude month -1 from scoring)

**Where:** `strategies/momentum_identity.py` (ranking window)

**Implementation:**
1. Add parameter: `skip_most_recent_month: bool`
2. When True: rank on months -K to -2 (skip month -1)
3. When False: rank on months -K to 0 (original)
4. Test both: compare Sharpe ratios

**Owner:** Implementation required  
**Time:** 2-3 hours  
**Validation:** R1-skip should have Sharpe ~0.90-0.95 vs. R1-base ~0.85

**Depends on:** B-001 (overlapping portfolios must be done first)

---

### **Task 10: Document Design Choices (B-004 through B-009)**

**Quick verification audits (not implementation):**

- B-004: Lookback periods — already correct (3/6/9/12), just formally approve
- B-005: Universe filtering — document ADTV floor rationale (liquidity, execution)
- B-006: Rebalance frequency — document R7's 21d exception (crash detection priority)
- B-008: R7 cadence — link to Phase 7 testing results (82.7% crash miss, 55.5% catch)
- B-010: R3 skip-month — verify code (should be correct)

**Owner:** Documentation updates (you review)  
**Time:** 2-3 hours  
**Validation:** Code/registry match docs; future maintainers understand choices

---

## Review Phase 4: Validation Gate (B-011)

### **Task 11: Verify R1-R12 Sharpe vs. Published Benchmarks**

**Once B-001 + B-002 fixes deployed:**

1. Re-run R1-R12 backtest suite
2. Compare Sharpe ratios to expected benchmarks:
   - R1: 0.68-0.95 (should now match)
   - R3: 0.91+ (skip-month benefit visible)
   - R7: ~0.80 (crash-aware, lower Sharpe but less drawdown)
   - R8/R9: 0.75-0.85 (vol-scaling optimization)

3. If Sharpe within ±5% of expected → APPROVED
4. If Sharpe significantly different → investigate further

**Owner:** Automated validation  
**Time:** 30 minutes (backtests + comparison)

---

## Timeline Summary

| Phase | Task | Time | Owner | Blocker? |
|-------|------|------|-------|----------|
| **1** | Fix audit prompt (B-003) | 1-2h | You (Claude drafts) | ✅ YES |
| **2a** | Audit R1 | 15m | Automated | — |
| **2b** | Audit R3 | 15m | Automated | — |
| **2c** | Audit R4-R6 | 45m | Automated (parallel) | — |
| **2d** | Audit R7 | 15m | Automated | — |
| **2e** | Audit R8-R9 | 30m | Automated (parallel) | — |
| **2f** | Audit R10-R12 | 45m | Automated (parallel) | — |
| **3a** | Implement overlapping portfolios (B-001) | 4-6h | Implementation | ✅ BLOCKS PHASE 4 |
| **3b** | Implement skip-month variant (B-002) | 2-3h | Implementation | ✅ DEPENDS ON B-001 |
| **3c** | Document design choices (B-004-B-010) | 2-3h | Documentation | — |
| **4** | Validate R1-R12 Sharpe vs. benchmarks (B-011) | 30m | Automated | — |
| **PHASE 3 TRIALS CAN START** | — | — | — | After Phase 4 ✅ |

**Total time for momentum review:** ~1-2 weeks (with parallel automation)

---

## What You Review vs. What's Automated

| Activity | Reviewer | Notes |
|----------|----------|-------|
| **Audit prompt (B-003)** | You | Critical for accuracy; Claude drafts, you approve |
| **R1-R12 audits** | Agent runs; you review verdict | Agent finds deviations; you decide if backlog entries created |
| **Overlapping portfolios impl** | You | Core algorithm change; needs careful review |
| **Skip-month impl** | You | Parameter addition; needs validation |
| **Design docs** | You | Confirm choices are documented correctly |
| **Backtest validation** | You | Final gate; check Sharpe against benchmarks |

---

## Next Steps: What to Start With

**Option A (Thorough):**
1. Start Task 1: Rewrite momentum audit prompt (today)
2. Once approved, run Tasks 2-7 (R1-R12 audits) — automated, just monitor
3. Review backlog verdict; prioritize B-001 implementation

**Option B (Quick):**
1. Let me draft improved audit prompt as Claude
2. You review + approve in 30 min
3. We run R1 audit immediately to validate
4. Adjust prompt if needed before full R-family suite

**Which approach do you prefer?**

---

## See Also

- [backlog_items.yaml](backlog_items.yaml) — Full momentum backlog with dependencies
- [PHASE_3_TRIALS_GUIDE.md](PHASE_3_TRIALS_GUIDE.md) — Trials that start after Phase 2 complete
- [docs/agents/strategy-audit-prompts.md](agents/strategy-audit-prompts.md) — Current audit prompts
