# Backlog Management — Agent-Driven Workflow

**Last updated:** 2026-08-25

When agents discover incomplete work, bugs, or deviations that need fixing, they should automatically propose backlog entries.

---

## Criticality Levels

All backlog items use one of five criticality levels:

| Level | Definition | Action | Example |
|-------|-----------|--------|---------|
| **BLOCKER** | Prevents strategy from backtesting or deploying live | Fix before any backtest | R1 overlapping portfolios missing; momentum breaks J&T spec |
| **CRITICAL** | Major functionality broken; strategy results unreliable | Fix before Phase 3 trial | PIT violation in valuation metric calculation |
| **HIGH** | Significant quality/risk issue; affects reliability | Fix this week | Skip-month not implemented; Sharpe ~0.05 lower than published |
| **MEDIUM** | Should be addressed soon; impacts optimization path | Fix within 2 weeks | Data audit edge case (delisted stock handling untested) |
| **LOW** | Nice-to-have improvement; no blocking impact | Fix opportunistically | Optimization: reduce transaction costs with overlapping portfolios |

---

## Backlog Entry Format

Each backlog item is a structured entry in `FeatureBacklog.md` (or `backlog_items.yaml` if separated):

```yaml
backlog_items:
  B-001:
    title: "Implement overlapping portfolios in R1"
    description: |
      R1 currently uses full portfolio replacement (100% turnover) each month.
      J&T (1993) uses K overlapping sub-portfolios; only 1/K replaced monthly.
      
      This affects:
      - Transaction cost structure (higher than published)
      - Portfolio smoothness (lower diversification benefit)
      - Sharpe ratio vs. J&T baseline
      
      Fix: Refactor strategies/momentum_identity.py to implement K-portfolio structure.
    criticality: "HIGH"
    found_by: "momentum-strategy-audit"
    found_in: "Phase 2 R1 audit (2026-08-25)"
    depends_on: []
    owner: null  # unassigned
    status: "open"
    
  B-002:
    title: "Add one-month skip to momentum ranking (R1 variant)"
    description: |
      J&T (1993) ranks on returns from month -K to month -2 (skipping month -1).
      AlphaLens ranks on all K months (month -K to month 0).
      
      Skip-month prevents bid-ask bounce effect; Fama-French data shows ~0.05-0.10 Sharpe improvement.
      
      Fix: Create R1-skip variant or update R3 skip-month implementation.
    criticality: "MEDIUM"
    found_by: "momentum-strategy-audit"
    found_in: "Phase 2 R1 audit (2026-08-25)"
    depends_on: []
    owner: null
    status: "open"
    
  B-003:
    title: "Fix momentum-strategy-audit false positive (R1 APPROVED premature)"
    description: |
      Phase 2 R1 audit returned APPROVED despite missing overlapping portfolios + skip-month.
      Root cause: Audit prompt lacked specific checklist items and forced deviation inventory.
      
      Fix: Update momentum-strategy-audit prompt with:
      - Specification-as-code (formal J&T definition with 15+ checklist items)
      - Forced deviation inventory (enumerate ALL deviations)
      - Citation requirements (specific pages/equations)
      - Adversarial verification (skeptic questions)
      - Multi-pass review (reconsider verdict after adversarial check)
      
      See: docs/AGENT_IMPROVEMENTS.md for complete fix template.
    criticality: "CRITICAL"
    found_by: "user-manual-review"
    found_in: "Post-Phase-2 R1 audit (2026-08-25)"
    depends_on: []
    owner: null
    status: "open"

backlog_dependencies:
  B-003:
    description: "momentum-strategy-audit improvements"
    blocks:
      - B-001  # Can't trust audit of overlapping portfolios until B-003 fixed
      - B-002  # Can't trust audit of skip-month until B-003 fixed
    blocked_by: []
    
  B-001:
    description: "overlapping portfolios implementation"
    blocks: []
    blocked_by:
      - B-003  # Need improved audit to verify overlapping implementation
```

---

## Agent Workflow: Creating Backlog Entries

### **Step 1: Identify Incomplete Items**

During any audit/review, agent should track:
- ❌ Missing features (e.g., overlapping portfolios)
- ❌ Code deviations from research (e.g., skip-month)
- ❌ Edge cases not handled (e.g., delisted stock timing)
- ❌ Verification gaps (e.g., PIT safety untested)
- ❌ Quality issues (e.g., documentation incomplete)

### **Step 2: Propose Backlog Entries**

At audit completion, agent should ask:

```
Found 3 incomplete items:

1. [BLOCKER] Overlapping portfolios not implemented
   - Impact: Breaks J&T momentum specification
   - Blocks: R1 strategy before live deployment
   
2. [HIGH] One-month skip not implemented
   - Impact: Sharpe ~0.05-0.10 lower than published
   - Blocks: Phase 3 validation
   
3. [MEDIUM] Skip-month documentation missing
   - Impact: Future maintainers confused
   - Blocks: None

Should I create backlog entries for all 3?
[YES / NO / SELECT WHICH ONES]
```

### **Step 3: Create Entries**

If user selects YES, agent creates structured entries:

```
Created backlog entries:

B-001: Implement overlapping portfolios in R1
  - Criticality: BLOCKER
  - Found by: momentum-strategy-audit
  - Status: open

B-002: Implement one-month skip
  - Criticality: HIGH
  - Found by: momentum-strategy-audit
  - Status: open

B-003: Document skip-month logic
  - Criticality: MEDIUM
  - Found by: momentum-strategy-audit
  - Status: open

Next: Should I also create dependency links? (B-001 blocks backtest until fixed)
```

### **Step 4: Track Dependencies**

Agent should identify and record:
- "B-001 blocks B-004" (can't deploy strategy until overlapping portfolios fixed)
- "B-002 depends on B-001" (skip-month implementation assumes overlapping portfolio structure)

---

## Integration with Agents

All 15 agents should follow this pattern at the end of their audit:

### **momentum-strategy-audit**
```markdown
## Backlog Proposal

Found incomplete items:
- [ ] Overlapping portfolios missing (BLOCKER)
- [ ] Skip-month not implemented (HIGH)
- [ ] Regime gate documentation incomplete (LOW)

Create backlog entries? [YES / NO]
```

### **technical-strategy-audit**
```markdown
## Backlog Proposal

Found incomplete items:
- [ ] RSI period justification missing (MEDIUM)
- [ ] MACD parameter drift (HIGH)
- [ ] Regime compatibility untested (CRITICAL)

Create backlog entries? [YES / NO]
```

### **fundamental-strategy-audit**
```markdown
## Backlog Proposal

Found incomplete items:
- [ ] PIT-safety audit flagged forward guidance (BLOCKER)
- [ ] Delisted stock handling untested (CRITICAL)
- [ ] Survivor bias documentation missing (MEDIUM)

Create backlog entries? [YES / NO]
```

### **enhanced-backtesting-agent**
```markdown
## Backlog Proposal

Integrity checks found:
- [FAIL] check_04_survivorship — delisted before position close
- [FAIL] check_07_no_hpo_on_test — hyperparameter search on full data
- [PASS] check_02_pit

Create backlog entries for failures? [YES / NO]
```

---

## Backlog Entry Lifecycle

```
open
  ↓ (user assigns to sprint)
in_progress
  ↓ (code changes committed)
pr_ready
  ↓ (PR merged)
merged
  ↓ (validated in next audit)
closed
```

---

## Backlog Query Commands

Once backlog is structured, users can query:

```bash
# Show all BLOCKER items
grep "criticality.*BLOCKER" FeatureBacklog.md

# Show dependencies of item B-001
grep -A 5 "B-001:" backlog_dependencies.yaml

# Show what blocks item B-003
grep -B 5 "B-003" backlog_dependencies.yaml | grep "blocked_by"

# Show all items found by momentum-strategy-audit
grep -l "found_by.*momentum-strategy-audit" backlog_items/*.yaml
```

---

## Examples from Phase 2

### Example 1: R1 Audit Found These Issues

```yaml
B-001:
  title: "Overlapping portfolios not implemented (R1)"
  criticality: "BLOCKER"
  found_by: "momentum-strategy-audit"
  found_in: "Phase 2 R1 test scenario (2026-08-25)"
  depends_on: []
  
B-002:
  title: "One-month skip not implemented (R1)"
  criticality: "HIGH"
  found_by: "momentum-strategy-audit"
  found_in: "Phase 2 R1 test scenario (2026-08-25)"
  depends_on: []

B-003:
  title: "Fix momentum-strategy-audit prompt (false positive on R1)"
  criticality: "CRITICAL"
  found_by: "user-manual-review"
  found_in: "Post-Phase-2 reflection (2026-08-25)"
  depends_on: []
```

**Dependencies:**
- B-003 blocks: B-001, B-002 (can't trust audit until prompt is fixed)

---

## When to NOT Create Backlog Entries

- **Already known issues** — Update existing backlog entry instead
- **User-requested deviations** — J&T doesn't require overlapping portfolios; user chose not to implement (document decision instead)
- **Future optimizations** — "Could improve Sharpe by 5% with X" → only if X is critical to strategy viability
- **Transient issues** — "API was slow today" → not a backlog item

---

## See Also

- [AGENTS_USER_GUIDE.md](AGENTS_USER_GUIDE.md) — How to invoke agents
- [AGENTS.md](../AGENTS.md) — Agent specifications
- [FeatureBacklog.md](../FeatureBacklog.md) — Main backlog (to be reorganized with backlog_items/dependencies sections)
