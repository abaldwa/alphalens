# R-Family Strategy Remediation Plan  

**Date:** 2026-08-25  
**Author:** Multi-Agent Review (Product Owner, Domain Expert, Backend Data Engineer, Backtest Reviewer, ML Rigor Reviewer)  
**Status:** ❌ **BLOCKING PAPER TRADING**  
**Severity:** 🔴 **CRITICAL**

---

## Executive Summary

Multi-agent review on 2026-08-25 identified **9 critical blockers** across R10-R12 strategies. Most critical: **headline results (16.4% CAGR, Sharpe 0.72 for R12) exist nowhere in the system** — zero backtests executed, claims unverifiable.

**Unanimous verdict:** All 5 independent reviewers agree — **DO NOT SHIP** R10-R12 to paper trading until remediation items B-014–B-022 are complete.

---

## Critical Findings

### 1. Unexecuted Validation Queues (Most Critical)
- Queue configs exist but were **never run**
- No rows in `backtest_runs` table for R10/R12 (searched 3,418 total rows)
- Claimed results exist only in self-authored memory
- **Implication:** Strategies may never have been backtested

### 2. Unwired Analysis Code
- `bucket_by_adtv_quintile()` fully implemented, but **ZERO call sites outside unit test**
- R12's "liquidity interaction" claim is purely aspirational

### 3. Signal-Cadence Mismatch (Critical Design Flaw)
- R12: 1-month reversal signal held for 3 months (quarterly rebalance)
- Signal mean-reverts long before rebalance, leaving stale positions
- Never tested with monthly (21d) cadence

### 4. Regime Dependence Untested
- 2019-2025 window dominated by COVID recovery (unusually strong bull market)
- No sub-period breakdown to validate reversal edge is stable

### 5. Selection Bias Underestimated
- DSR computed with `n_trials=2` instead of true program count (100+)
- R12 edge likely noise under proper multiple-testing correction

### 6. Robustness Checks Failed
- R12 reversal **FAILED** fold_stability and benchmarks checks
- Checks marked non-critical by design, but these are the most relevant

### 7. Data Infrastructure Gaps
- Backtest cache snapshot never reconciled against live data
- Known 960+ OHLCV gaps; snapshot could go stale silently

### 8. Reporting Schema Incomplete
- Trade-log CSV missing `sector` and `liquidity_bucket` columns
- R11 reporting breakdowns cannot be computed

### 9. Domain Validity Concerns
- Static sector labels applied to 2019-2025 history
- Liquidity effect may be concentrated sector bet

---

## Remediation Items (B-014–B-022)

**See:** [`backlog_items_r_family_remediation.yaml`](../backlog_items_r_family_remediation.yaml) for full details

| ID | Title | Status | Blocker | Estimate |
|---|---|---|---|---|
| **B-014** | Execute R10 validation queue | ⏳ | 🔴 High | 30 min exec + review |
| **B-015** | Execute R12 validation queue | ⏳ | 🔴 High | 40 min exec + review |
| **B-016** | Wire liquidity bucketing into R12 | ⏳ | 🟡 Med | 3-4 hours |
| **B-017** | Test monthly (21d) rebalance for R12 | ⏳ | 🟡 Med | 2-3 hours |
| **B-018** | Sub-period stability (2019-2022 vs 2023-2025) | ⏳ | 🟡 Med | 2-3 hours |
| **B-019** | Recompute DSR with true trial count | ⏳ | 🔴 High | 1-2 hours |
| **B-020** | Root-cause robustness check failures | ⏳ | 🔴 High | 4-6 hours invest |
| **B-021** | Add snapshot reconciliation check | ⏳ | 🟡 Med | 2-3 hours |
| **B-022** | Add sector/liquidity_bucket to trade_log | ⏳ | 🟡 Med | 1-2 hours |

---

## Timeline & Phases

### Phase 1: Validation (Parallel, ~1 hour)
**B-014 + B-015:** Execute unexecuted queues, verify results exist

**Decision point after Phase 1:**
- If B-015 Sharpe < 0.70 → Archive R12 immediately
- If B-015 passes → Proceed to Phase 2

### Phase 2: Design & Infrastructure Audits (Parallel, ~4 hours)
**B-018, B-019, B-020, B-021, B-022:**
- Regime stability test
- Selection bias audit
- Robustness failure investigation
- Snapshot reconciliation
- Trade-log schema

**Critical path:** B-020 (may take 6+ hours if complex)

### Phase 3: Conditional (If R12 Survives Phase 2)
**B-016 + B-017:**
- Wire liquidity bucketing
- Test monthly cadence variant

**Gate:** If either shows reversal edge vanishes, archive R12

### Phase 4: Decision & Integration (1-2 days)
- Review all findings
- Decide: R10 only? R10 + R12? Or archive all?
- Proceed to paper trading plumbing

---

## Success Criteria by Phase

| Phase | Success Metric | Failure Metric |
|---|---|---|
| **Phase 1** | Both queues complete, results land in `backtest_runs` | Runs fail, queues crash |
| **Phase 2** | Sub-periods both >0.70 Sharpe; DSR >0.6; robustness issues root-caused | Any phase fails, indicating reversal is not robust |
| **Phase 3** | Monthly cadence improves to >0.85 Sharpe; liquidity bucketing confirms edge | Edge disappears with illiquid names excluded; cadence has no effect |
| **Phase 4** | R10 APPROVED for composite; R12 approved/archived with clear rationale | Fundamental design flaws remain unfixed |

---

## Risk Mitigation

| Risk | Mitigation |
|---|---|
| Backtest execution fails | Pre-check scheduler status, DuckDB locks; run during off-peak |
| R12 findings warrant archival | This is a valid outcome; focus on R10 + paper trading plumbing instead |
| Robustness failures reveal unfixable design | Document limitation, accept, or archive; don't force-fit |
| Phase 2 audits take longer than estimated | B-020 is highest-risk; prioritize first; parallelize others |

---

## Decision Tree

```
B-014/B-015 Execute Queues
  ├─ Sharpe R12 < 0.60 → ARCHIVE R12, SKIP Phase 3
  ├─ Sharpe R12 0.60-0.80 → Continue to Phase 2
  └─ Sharpe R12 > 0.80 → HIGH confidence, continue

Phase 2 Audits (Parallel)
  ├─ Regime concentrated (2019-2022 >> 2023-2025) → ARCHIVE R12
  ├─ DSR < 0.5 → ARCHIVE R12
  ├─ Robustness unfixable → ARCHIVE R12
  └─ All pass → Continue to Phase 3

Phase 3 Conditional (If R12 survives)
  ├─ B-016: Edge disappears → ARCHIVE R12
  ├─ B-017: Monthly Sharpe < 0.80 → ARCHIVE R12
  └─ Both pass → R12 APPROVED with monthly cadence

Phase 4 Decision
  ├─ R10 APPROVED + R12 ARCHIVED → Proceed
  ├─ R10 APPROVED + R12 APPROVED → Proceed
  └─ Other findings → Document + proceed
```

---

## Next Steps

1. **Immediate (2026-08-25):** Create backlog items B-014–B-022 in FeatureBacklog.md
2. **Week 1 (2026-08-26):** Execute Phase 1 (B-014, B-015)
3. **Week 2 (2026-08-27–28):** Execute Phase 2 in parallel
4. **Week 3 (2026-09-02–06):** Phase 3 (if needed) + gate decision
5. **Long-term:** Archive remediation plan from "Phase Complete" status; resume R-family work only after paper trading is live

---

## References

- **Backlog Items:** [`backlog_items_r_family_remediation.yaml`](../backlog_items_r_family_remediation.yaml) — Full 9 items with implementation details
- **Memory Update:** Update `project_r_family_complete.md` status from "✅ Complete" to "⏳ Remediation Pending"
- **Related Work:**
  - Paper Trading Phase A–C (Scheduler, dispatch, composite definition) — **HIGHER PRIORITY**
  - ML40, ML41, ML42 — ML Signal Engine unification
  - T15, F7 — TA Template and Fundamental strategy registry migrations

---

**Bottom line:** DO NOT merge R10-R12 into any production flow until all 9 remediation items are complete and reviewed. All 5 reviewers are in unanimous agreement: these strategies are **not production-ready** in their current form.
