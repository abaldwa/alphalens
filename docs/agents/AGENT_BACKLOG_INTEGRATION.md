# Agent Backlog Integration — Complete Guide

**Last updated:** 2026-08-25

This guide shows how to integrate backlog creation capability into all 15 AlphaLens agents, ensuring incomplete work is automatically captured with appropriate criticality levels.

---

## Core Principle

Every agent should end with:

```
**Step N: Incomplete Items Audit**
Check for missing features, bugs, or edge cases found during review.
Propose backlog entries for each incomplete item.
Ask user for confirmation before creating entries.
```

---

## Implementation for Each Agent

### **Phase 2 Agents: Strategy Auditors**

#### **1. momentum-strategy-audit**

**Added Step 5: Backlog Proposal**

```markdown
**Step 5: Backlog Proposal (15 min)**

Check for incomplete items:
- Missing features from J&T spec (overlapping portfolios, skip-month, skip-month variant)
- Undocumented parameter choices (why 3/6/9/12 months? why these ADTV floors?)
- Edge cases untested (delisted company timing, corporate action handling)
- Regime compatibility verification incomplete

Example incomplete items found in R1:
- [BLOCKER] Overlapping portfolios missing (breaks J&T spec)
- [HIGH] Skip-month not implemented (Sharpe 0.05 lower)
- [MEDIUM] Parameter justification not documented

**Ask user:**
"Found 3 incomplete items. Should I create backlog entries?
1. Implement overlapping portfolios (BLOCKER)
2. Implement skip-month variant (HIGH)
3. Document parameter choices (MEDIUM)
[YES / NO / SELECT WHICH ONES]"

**If YES:** Create entries with:
- ID: auto-generate (B-001, B-002, B-003)
- Title: descriptive
- Criticality: BLOCKER/CRITICAL/HIGH/MEDIUM/LOW
- Found by: momentum-strategy-audit
- Found in: strategy name + date
- Depends on: link to related items (e.g., B-003 blocks B-001)
```

---

#### **2. technical-strategy-audit**

**Added Step 5: Backlog Proposal**

```markdown
**Step 5: Backlog Proposal (10 min)**

Check for incomplete items:
- Indicator calculation not matching Wilder/Appel original (RSI period, MACD EMA, bands width)
- Parameter thresholds not justified (why 30/70 and not 20/80?)
- Regime compatibility untested (does RSI overbought work in crashes?)
- PIT-safety edge cases (lookback calculation at market open/close?)
- Documentation gaps

Example incomplete items for RSI-MACD strategy:
- [HIGH] MACD period parameters not documented (12/26/9 vs. alternatives)
- [MEDIUM] RSI regime compatibility untested (crashes, gap days)
- [MEDIUM] PIT-safety: indicator value at rebalance open vs. close?

**Ask user:**
"Found 3 incomplete items. Should I create backlog entries?
[YES / NO / SELECT WHICH]"

**If YES:** Criticality guide:
- BLOCKER: Calculation doesn't match published formula (e.g., wrong RSI period)
- CRITICAL: PIT violation (forward-looking indicator values)
- HIGH: Parameter choice not documented (future maintainers confused)
- MEDIUM: Regime compatibility untested
- LOW: Documentation/comment improvements
```

---

#### **3. fundamental-strategy-audit**

**Added Step 5: Backlog Proposal**

```markdown
**Step 5: Backlog Proposal (10 min)**

Check for incomplete items:
- **CRITICAL: PIT-safety issues** (metrics at announcement date, not quarter-end?)
- Financial metric calculation deviations (P/E, P/B, ROE extracted correctly?)
- Delisted stock handling untested (removed at delisting or quarter-end?)
- Survivor bias verification missing (backtest universe ≠ live universe?)
- Forward guidance leakage untested (forecast lag > holdback period?)
- Benchmark selection not justified (value vs. growth?)

Example incomplete items for Piotroski F-Score strategy:
- [CRITICAL] Metrics extracted at announcement date (PIT-safe)?
- [CRITICAL] Delisted stock handling verification missing
- [HIGH] Piotroski F-Score formula not cited against Piotroski (2000)
- [MEDIUM] Survivor bias documentation missing

**Ask user:**
"Found 4 incomplete items. Should I create backlog entries?
Criticality breakdown:
- 2 CRITICAL (PIT violations if wrong)
- 1 HIGH (formula mismatch)
- 1 MEDIUM (documentation)
[YES / NO / SELECT WHICH]"

**If YES:** Note: CRITICAL items should block backtest until fixed.
```

---

### **Phase 3+ Agents: Data & Model**

#### **4. data-audit-agent**

**Backlog Proposal Example:**

```markdown
**Auto-Audit Backlog Items**

Post-backtest data audit found:
- [HIGH] OHLCV discontinuity at Fyers seam (2017-04-02): 69/473 tickers >2x jump
- [MEDIUM] Feature store partition integrity unverified (Stage 2 overwrites)
- [MEDIUM] Benchmark series availability gaps (NSE holiday 2026-06-26)

**Auto-ask user:**
"Data audit found 3 issues. Create backlog entries?"
[YES / NO]
```

---

#### **5. signal-parity-agent**

**Backlog Proposal Example:**

```markdown
**Signal Parity Backlog Items**

Comparison of live vs. backtested signals found:
- [CRITICAL] EMA-RSI regime gate applies differently in live (48h lag vs. daily)
- [HIGH] Universe filtering logic differs (live includes delisted; backtest excludes)

**Ask user:**
"Signal parity check found 2 discrepancies. Create backlog entries?"
[YES / NO]
```

---

#### **6. ml-model-audit-agent**

**Backlog Proposal Example:**

```markdown
**ML Model Training Backlog Items**

Found during training rigor audit:
- [CRITICAL] Random feature test failed: feature importance suggests lookahead
- [HIGH] Walk-forward fold overlap detected (40% data reuse across folds)
- [MEDIUM] Cross-validation random seed not documented

**Ask user:**
"Model audit found 3 issues (1 CRITICAL). Create backlog entries?"
[YES / NO]
```

---

### **Infrastructure Agents**

#### **7. memory-management-agent**

**Backlog Proposal Example:**

```markdown
**Memory Management Alerts**

During backtest execution:
- [MEDIUM] Memory grew 15% faster than baseline (8.5GB expected, 9.8GB used)
- [MEDIUM] Feature store snapshots would save 2.3GB (recommend using read-only snapshot)

**Ask user:**
"Memory optimization found 2 improvement opportunities. Create backlog entries?"
[YES / NO]
```

---

#### **8. enhanced-backtesting-agent**

**Backlog Proposal Example:**

```markdown
**Backtest Integrity Check Failures**

12 post-backtest integrity checks found:
- [CRITICAL] check_04_survivorship: 47 delisted positions held past delisting
- [CRITICAL] check_07_no_hpo_on_test: hyperparameter search on full data (no test set)
- [HIGH] check_02_pit: announcement date calculation ±1 day variance
- [PASS] check_05_costs, check_06_liquidity, [8 others]

**Auto-ask user:**
"Integrity checks found 2 CRITICAL failures. Create backlog entries + block backtest?"
[BLOCK & CREATE / CREATE ONLY / IGNORE]
```

---

### **Core Review Agents (Phase 1)**

#### **9. ml-rigor-reviewer**

**Backlog Proposal Example:**

```markdown
**ML Rigor Review Backlog Items**

Statistical review found:
- [HIGH] Sharpe ratio uses returns that weren't annualized (formula error)
- [MEDIUM] Cross-validation strategy doesn't use walk-forward (random k-fold)
- [MEDIUM] Prediction confidence intervals not reported

**Ask user:**
"ML rigor review found 3 issues. Create backlog entries?"
[YES / NO]
```

---

#### **10. domain-expert**

**Backlog Proposal Example:**

```markdown
**Domain Expert Backlog Items**

Indian equity market review found:
- [HIGH] Strategy doesn't account for circuit breaker halts (stocks can gap >10%)
- [MEDIUM] NSE trading hours assumption (9:15-15:30) not documented
- [LOW] Sector rotation aligned with monsoon/budget (cultural factor)

**Ask user:**
"Domain review found 3 market-specific items. Create backlog entries?"
[YES / NO]
```

---

#### **11. backtest-reviewer**

**Backlog Proposal Example:**

```markdown
**Backtest Engine Review Backlog Items**

Engine correctness audit found:
- [CRITICAL] Dividend adjustment applied retroactively (forward bias)
- [HIGH] Tax calculation uses end-of-year rate (should be FY-specific)
- [MEDIUM] Trade settlement lag not modeled (3 days for NSE)

**Ask user:**
"Backtest engine review found 3 issues (1 CRITICAL). Create backlog entries?"
[YES / NO]
```

---

#### **12. backend-data-engineer**

**Backlog Proposal Example:**

```markdown
**Data Infrastructure Review Backlog Items**

DuckDB schema review found:
- [HIGH] fundamentals.announcement_date is VARCHAR (should be DATE type)
- [MEDIUM] ohlcv_adjusted missing indexes on (ticker, date) for range queries
- [LOW] Feature store snapshot retention policy not documented

**Ask user:**
"Data infrastructure review found 3 items. Create backlog entries?"
[YES / NO]
```

---

### **Code Quality Agents**

#### **13. code-reviewer**

**Backlog Proposal Example:**

```markdown
**Code Review Backlog Items**

Correctness/simplification audit found:
- [HIGH] _equity_history calculation has off-by-one error in loop indexing
- [MEDIUM] Unused imports in strategies/momentum_identity.py (line 12-15)
- [MEDIUM] Magic number 21 should be REBALANCE_CADENCE_DAYS constant

**Ask user:**
"Code review found 3 items. Create backlog entries?"
[YES / NO]
```

---

#### **14. frontend-a11y-reviewer**

**Backlog Proposal Example:**

```markdown
**Frontend Accessibility Review Backlog Items**

Accessibility audit found:
- [HIGH] Backtest report table not keyboard navigable (ag-grid role="grid" missing)
- [MEDIUM] Chart tooltips don't have ARIA labels (screen reader blind)
- [MEDIUM] Light/dark theme toggle not persisted to localStorage

**Ask user:**
"Accessibility review found 3 issues (1 HIGH). Create backlog entries?"
[YES / NO]
```

---

### **Decision Agents**

#### **15. skeptic-tester**

**Backlog Proposal Example:**

```markdown
**Skeptic Test Backlog Items**

Failure mode analysis found:
- [CRITICAL] Strategy breaks if momentum reverses (2008, 2020 crashes unhandled)
- [CRITICAL] Universe filter excludes earnings-shocked stocks (survivorship bias)
- [HIGH] Backtest assumes perfect execution (no slippage model)
- [MEDIUM] Risk limit (max 2% per trade) not enforced in code

**Ask user:**
"Skeptic analysis found 4 high-risk items (2 CRITICAL). Create backlog entries?"
[YES / NO / BLOCK STRATEGY]
```

---

## Criticality Decision Tree

Use this to assign criticality when creating backlog entries:

```
Does this item block backtesting or deployment?
├─ YES → BLOCKER
└─ NO → Does it create incorrect results?
    ├─ YES → CRITICAL
    └─ NO → Is it a significant quality/risk issue?
        ├─ YES → HIGH
        └─ NO → Is it should-do-soon?
            ├─ YES → MEDIUM
            └─ NO → Is it nice-to-have?
                ├─ YES → LOW
                └─ NOT A BACKLOG ITEM
```

---

## Backlog Entry Template

When agent creates entry, use this structure:

```yaml
backlog_items:
  B-NNN:
    title: "[Category] Brief title (max 80 chars)"
    description: |
      What needs to be done and why.
      
      Current state: [what code does]
      Research/requirement: [what it should do]
      Impact: [user-facing consequences if not fixed]
      
      How to fix: [concrete steps]
    
    criticality: "BLOCKER | CRITICAL | HIGH | MEDIUM | LOW"
    found_by: "[agent name]"
    found_in: "[strategy/audit name] — [date]"
    depends_on: [list of backlog item IDs that must be fixed first]
    owner: null  # will be assigned later
    status: "open"
    
backlog_dependencies:
  B-NNN:
    description: "[brief]"
    blocks: [list of B-IDs this blocks]
    blocked_by: [list of B-IDs that must complete first]
```

---

## User Workflow: Creating Backlog Entries

### **Scenario 1: Agent Finds Issues, Creates Entries**

```
User: "Run momentum-strategy-audit on R1"

Agent: [completes audit]

Agent: "Found 3 incomplete items:
1. [BLOCKER] Overlapping portfolios missing
2. [HIGH] Skip-month not implemented
3. [MEDIUM] Parameter docs missing

Should I create backlog entries? [YES/NO/SELECT]"

User: "YES"

Agent: "Created B-001, B-002, B-003.
B-001 blocks backtest (BLOCKER).
B-002 dependency: needs B-003 fixed first.

Link: docs/backlog_items/B-001.yaml"
```

---

### **Scenario 2: User Selects Specific Items**

```
Agent: "Found 5 items. Which should I create entries for?
1. [BLOCKER] Overlapping portfolios
2. [HIGH] Skip-month
3. [MEDIUM] Docs
4. [LOW] Code comment
5. [LOW] Optimization suggestion"

User: "CREATE 1, 2, 3 only (not 4, 5)"

Agent: "Created B-001 (BLOCKER), B-002 (HIGH), B-003 (MEDIUM).
Skipped B-004, B-005 (LOW priority items)."
```

---

### **Scenario 3: Agent Finds Duplicate Backlog Item**

```
Agent: "Item 'Overlapping portfolios missing' already exists as B-001.
Should I link to existing item or create new entry? [LINK/CREATE]"

User: "LINK"

Agent: "Updated B-001 with reference: also found by 
technical-strategy-audit on R11 (2026-08-26).
Multi-strategy impact: blocks R1, R11, R12."
```

---

## Backlog Query Commands

Once all agents create backlog entries, users can query:

```bash
# Show all BLOCKER items
grep "criticality.*BLOCKER" docs/backlog_items/B-*.yaml | wc -l

# Show what blocks R1 backtest
grep -l "found_in.*R1" docs/backlog_items/B-*.yaml | \
  xargs grep "criticality.*BLOCKER"

# Show dependencies for B-001
grep -A 3 "blocks:" docs/backlog_dependencies/B-001.yaml

# Show all items found by momentum-strategy-audit
grep -l "found_by.*momentum-strategy-audit" docs/backlog_items/B-*.yaml | wc -l

# Show open items (not fixed)
grep "status.*open" docs/backlog_items/B-*.yaml
```

---

## Success Criteria

✅ All 15 agents end their audit with "Incomplete Items" section  
✅ Agents ask user permission before creating backlog entries  
✅ Criticality levels assigned correctly (BLOCKER blocks backtest, CRITICAL affects results)  
✅ Backlog entries linked with dependencies (B-001 blocks B-002)  
✅ Duplicate detection (agent finds existing B-001, links instead of creating new)  
✅ User can query backlog by: criticality, found_by agent, blocks/blocked_by, status

---

## See Also

- [BACKLOG_MANAGEMENT.md](BACKLOG_MANAGEMENT.md) — Full backlog workflow
- [AGENTS_USER_GUIDE.md](../AGENTS_USER_GUIDE.md) — How to invoke agents
- [AGENTS.md](../../AGENTS.md) — Agent specifications
