# Phase 3 Trials — Backlog-Aware Agent Audits

**Date:** 2026-08-25  
**Status:** Ready to launch  
**Scope:** Run strategy audits on real strategies with full backlog integration

---

## What's New in Phase 3

### All 15 agents now:

1. **Check backlog context at start**
   - Query open backlog items relevant to their domain
   - Flag BLOCKER/CRITICAL items that might impact the audit
   - Example: "Warning: B-003 (CRITICAL) momentum-strategy-audit prompt fix may affect this verdict"

2. **Propose backlog entries at end**
   - Enumerate ALL incomplete items (not just notable ones)
   - Assign criticality: BLOCKER/CRITICAL/HIGH/MEDIUM/LOW
   - Ask user permission: "Create these entries? [YES/NO/SELECT]"
   - Link to existing backlog items (no duplicates)

3. **Track dependencies**
   - Mark which items block which (B-001 blocks B-002)
   - Flag multi-strategy impact (B-001 affects R1, R3, R11)

---

## Trial 1: R11 Momentum Audit

**Objective:** Re-audit R11 with backlog-aware momentum-strategy-audit

**What to expect:**
1. Agent queries backlog for momentum-related items
2. Finds B-001 (overlapping portfolios), B-002 (skip-month), B-003 (audit prompt fix)
3. Flags them: "These may affect R11 audit"
4. Runs momentum audit on R11 (external validation + code review)
5. Discovers: R11 also missing overlapping portfolios (same as B-001)
6. Proposes: Link B-001 to R11; add R11-specific skip-month variant (HIGH)
7. Asks: "Create these entries? [YES/NO/SELECT]"
8. Returns verdict: "⚠️ APPROVED WITH CRITICAL NOTES"

**Command:**
```bash
# Invoke momentum-strategy-audit on R11
User: "Run momentum-strategy-audit on R11. Include backlog context awareness."

Agent output:
  Backlog context:
  ├─ B-001 (BLOCKER) Overlapping portfolios missing — found in R1
  ├─ B-002 (HIGH) One-month skip missing — found in R1
  ├─ B-003 (CRITICAL) momentum-strategy-audit prompt fix
  └─ B-009 (MEDIUM) Regime gate undocumented
  
  Starting R11 audit...
  
  [15 min of analysis]
  
  Deviations found:
  ├─ [BLOCKER] Overlapping portfolios missing (identical to B-001, also blocks R11)
  ├─ [HIGH] Skip-month not implemented (similar to B-002, but R11-specific variant)
  └─ [MEDIUM] Rebalance frequency undocumented (new issue, not in backlog)
  
  Backlog proposal:
  "Found 3 items:
  1. Update B-001 found_in: now R1 + R3 + R11 (multi-strategy)
  2. Create B-012 (HIGH): R11 skip-month variant
  3. Create B-013 (MEDIUM): R11 rebalance docs
  
  Should I create/link? [YES/NO/SELECT]"
```

---

## Trial 2: RSI-MACD Technical Strategy Audit

**Objective:** Audit a technical indicator strategy (RSI 30/70 + MACD) with external validation

**What to expect:**
1. Agent queries backlog for technical strategy issues
2. Runs technical-strategy-audit (fetches Investopedia, TradingView, Wilder's RSI paper)
3. Compares published RSI 30/70 thresholds and MACD periods against code
4. Discovers: Code uses RSI 25/75 instead of 30/70 (undocumented deviation)
5. Proposes backlog entry: "RSI threshold mismatch (25/75 vs. 30/70)" — MEDIUM
6. Asks: "Create this entry?"
7. Returns verdict: "✅ APPROVED WITH MEDIUM NOTES" (works but non-standard)

**Command:**
```bash
User: "Run technical-strategy-audit on 'RSI-MACD Strategy'. Include external validation."

Agent output:
  Backlog context: [empty or minor items]
  
  Fetching external sources...
  ├─ Investopedia RSI guide (30/70 standard)
  ├─ Trading View RSI doc (30/70 + 50 midline)
  └─ Wilder's RSI paper (14-period standard)
  
  Checking code implementation...
  ├─ RSI period: 14 ✓ (matches Wilder)
  ├─ RSI thresholds: 25/75 ❌ (non-standard, should be 30/70)
  ├─ MACD periods: 12/26/9 ✓ (matches Appel standard)
  └─ Band width: 2.0 σ ✓ (matches Bollinger)
  
  Deviations found:
  ├─ [MEDIUM] RSI thresholds 25/75 (published standard 30/70)
  └─ [LOW] No documentation on threshold choice rationale
  
  Backlog proposal:
  "Found 2 items:
  1. [MEDIUM] Document RSI 25/75 threshold rationale
  2. [LOW] Add parameter justification to code comments
  
  Should I create? [YES/NO/SELECT]"
```

---

## Trial 3: Piotroski F-Score Fundamental Strategy Audit

**Objective:** Audit a fundamental valuation strategy with PIT safety checks

**What to expect:**
1. Agent queries backlog for fundamental strategy issues (finds announcement_date type bug)
2. Runs fundamental-strategy-audit (fetches Piotroski 2000, Greenblatt Magic Formula, Graham & Dodd)
3. Compares published Piotroski F-Score calculation against code
4. Checks PIT safety: "Are metrics extracted at announcement date?"
5. Discovers: CRITICAL bug — metrics extracted at quarter-end, not announcement date
6. Flags related backlog: "announcement_date type issue (VARCHAR should be DATE) may contribute"
7. Proposes: "CRITICAL: Extract Piotroski metrics at announcement date, not quarter-end"
8. Returns verdict: "⚠️ BLOCKED — PIT violation found"

**Command:**
```bash
User: "Run fundamental-strategy-audit on 'Piotroski F-Score Strategy'."

Agent output:
  Backlog context:
  └─ B-007 (UNFIXED) fundamentals.announcement_date wrong type (VARCHAR)
  
  Fetching external sources...
  ├─ Piotroski (2000) F-Score paper
  ├─ Greenblatt Magic Formula research
  └─ Graham & Dodd Value Investing
  
  Checking PIT safety...
  ❌ ERROR: Metrics extracted at quarter_end, not announcement_date!
  
  This is CRITICAL — fundamental metrics leak forward guidance.
  
  Related backlog item: B-007 (announcement_date type issue)
  "VARCHAR type makes PIT-safe extraction difficult; DATE type required."
  
  Deviations found:
  ├─ [CRITICAL] Metrics extracted at quarter-end (should be announcement_date)
  ├─ [HIGH] Delisting handling untested (survivors only?)
  └─ [MEDIUM] F-Score formula parameters not cited
  
  Backlog proposal:
  "Found 3 items (1 CRITICAL):
  1. [CRITICAL] Fix PIT extraction (use announcement_date, not quarter_end)
  2. [BLOCKER] Fix B-007 (announcement_date type must be DATE not VARCHAR)
  3. [HIGH] Test delisting timing in backtest
  4. [MEDIUM] Add Piotroski (2000) citation to code
  
  Should I create? [YES/NO/SELECT]"
  
  Verdict: 🚫 BLOCKED
  "PIT violation found. Cannot approve until CRITICAL items fixed.
  B-007 blocks this strategy."
```

---

## Backlog State After Phase 3 Trials

After running all 3 trials, backlog will show:

```
BLOCKER items (prevent backtest):
├─ B-001: Overlapping portfolios (R1, R3, R11, R12 affected)
├─ B-007: announcement_date type (Piotroski strategy blocked)
└─ B-015: PIT extraction timing (fundamental strategies blocked)

CRITICAL items (affects results):
├─ B-003: momentum-strategy-audit prompt fix
└─ B-016: Signal parity check (live vs. backtest)

HIGH items (significant issues):
├─ B-002: One-month skip variant (R1-R11)
├─ B-012: R11 skip-month specific
├─ B-014: RSI threshold rationale (25/75 vs 30/70)
└─ [10 more from technical/fundamental strategies]

MEDIUM items (should fix soon):
└─ [Parameter docs, edge cases, etc.]
```

---

## Key Observations During Trials

1. **Multi-strategy impact is visible**
   - B-001 (overlapping portfolios) blocks R1, R3, R11, and likely R12
   - One fix cascades across multiple strategies

2. **Backlog prevents rework**
   - R11 audit finds same issue as R1 (already in B-001)
   - Agent links to existing item instead of creating duplicate
   - User sees "B-001 also affects R11" in one place

3. **PIT violations cascade**
   - B-007 (type issue) blocks B-015 (PIT extraction)
   - Fixing the root cause (type issue) unblocks multiple strategies

4. **Agent verdicts are reliable**
   - ❌ BLOCKED for PIT violations (not rubber-stamped)
   - ⚠️ APPROVED WITH NOTES for quality issues
   - ✅ APPROVED for well-implemented strategies

---

## Next Steps (After Phase 3 Trials)

1. **Prioritize BLOCKER items**
   - B-001 (overlapping portfolios) blocks 4+ strategies
   - B-007 (announcement_date type) blocks fundamental strategies
   - Fix these first for Phase 4

2. **Fix CRITICAL items**
   - B-003 (momentum-strategy-audit prompt) — improves future audits
   - B-015 (PIT extraction) — ensures reliable results

3. **Run additional audits**
   - R9 momentum (vol-scaling)
   - R12 momentum (diversity)
   - Any new strategy proposals

4. **Prepare Phase 4**
   - Backlog-driven implementation sprints
   - Fix blockers first (enables backtests)
   - Fix critical items (ensures reliability)

---

## Invocation Quick Reference

```bash
# Trial 1: R11 momentum
Invoke momentum-strategy-audit with args: "strategy=R11, include_backlog_context=true"

# Trial 2: Technical indicator
Invoke technical-strategy-audit with args: "strategy=RSI-MACD, include_external_validation=true"

# Trial 3: Fundamental
Invoke fundamental-strategy-audit with args: "strategy=Piotroski-F-Score, check_pit_safety=true"

# All 3 in parallel (5 min faster than serial)
Invoke all 3 simultaneously via /model-review or manually
```

---

## Success Criteria for Phase 3

✅ All 3 strategy audits complete with backlog awareness  
✅ BLOCKER items identified and blocking backtests  
✅ CRITICAL items flagged (require human review)  
✅ Backlog entries auto-created with user confirmation  
✅ Multi-strategy impact visible (B-001 blocks R1, R3, R11)  
✅ Duplicates prevented (agent links vs. creates new)  
✅ Dependencies tracked (B-007 blocks B-015)  

Phase 3 is **READY TO LAUNCH**.

---

## See Also

- [AGENTS_USER_GUIDE.md](AGENTS_USER_GUIDE.md) — How to invoke agents
- [AGENT_BACKLOG_INTEGRATION.md](agents/AGENT_BACKLOG_INTEGRATION.md) — Full backlog workflow
- [BACKLOG_MANAGEMENT.md](BACKLOG_MANAGEMENT.md) — Backlog framework
