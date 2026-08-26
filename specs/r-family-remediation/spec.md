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

## Critical Blockers — R-Family Foundation (B-001–B-013)

**Status:** Parallel track to R10-R12 validation. Some items block paper trading; others are documentation/infrastructure.

### Prompt Fix & Audit Reliability (Most Critical)
- **B-003:** Fix momentum-strategy-audit prompt (audit had false positives; returned APPROVED despite missing overlapping portfolios + skip-month)
  - **Impact:** CRITICAL — audit verdicts unreliable until fixed
  - **Blocker for:** B-001, B-004, B-005, B-006, B-010 (all R-family audits depend on this)
  - **Timeline:** 2-3 hours (rewrite with specification-as-code, adversarial verification, citation requirements)
  - **Acceptance:** Audit agent correctly identifies J&T deviations; test with deliberate violations

### R-Family Implementation Issues (Post-B-003)

**Critical Strategy Fixes (High Priority):**
| Item | Issue | Status | Severity | Timeline |
|------|-------|--------|----------|----------|
| **B-026** | R8 rebalance cadence (252d annual → 21d monthly) | Code fix | HIGH | 1-2 hours |
| **B-027** | R9 architecture (4 separate runs → regime-adaptive) | Code refactor | HIGH | 2-3 hours |
| **B-029** | R11 ranking direction (highest → lowest reversal) | Code fix | HIGH | 1 hour |
| **B-028** | R10 ranking (individual → sector-level) | Code refactor | MEDIUM | 2-3 hours |
| **B-025** | R5 registry mismatch (J&T → George & Hwang) | Registry fix | MEDIUM | 30 min |

**Missing Implementations (Phase 1 Blockers):**
| Item | Issue | Status | Severity | Timeline |
|------|-------|--------|----------|----------|
| **B-023** | Implement R4 (George & Hwang size momentum) | Missing code | MEDIUM | 3-4 hours |
| **B-024** | Implement R6 (index-transition anomaly) | Missing code | MEDIUM | 3-4 hours |

**Verification & Audit (Post-B-003):**
| Item | Issue | Status | Severity | Timeline |
|------|-------|--------|----------|----------|
| **B-001** | Overlapping portfolios (1/K staggered replacement) | Implementation needed | HIGH | Phase 4+ |
| **B-002** | Skip-month variant | ✅ CLOSED (already implemented) | — | — |
| **B-004** | Verify R1 lookback periods (3/6/9/12mo) | Needs audit | HIGH | 30 min (after B-003) |
| **B-005** | Verify R1 universe filtering (ADTV floors) | Needs documentation | MEDIUM | 1 hour (after B-003) |
| **B-006** | Verify R1-R12 rebalance cadence | Needs audit | HIGH | 1-2 hours (after B-003) |
| **B-010** | Audit R3 skip-month implementation | Needs audit | MEDIUM | 30 min (after B-003) |
| **B-011** | Verify R1-R12 Sharpe vs. published benchmarks | Validation gate | MEDIUM | Phase 4 (after B-001/B-002) |

### Infrastructure & Documentation (Independent)
| Item | Issue | Status | Severity |
|------|-------|--------|----------|
| **B-007** | Fix fundamentals.announcement_date VARCHAR→DATE | Data schema | MEDIUM |
| **B-008** | Document R9 4-mode vol-scaling + regime gate | Code docs | MEDIUM |
| **B-009** | Document R7 21d crash-aware cadence | Code docs | MEDIUM |
| **B-012** | Optimize costs with overlapping portfolios | Optimization | LOW |
| **B-013** | Add code comments for J&T specification | Code quality | LOW |

### Decision Logic for B-001 through B-013

```
B-003: Fix momentum-strategy-audit prompt
  ├─ 2-3 hours
  ├─ Blocks: B-001, B-004, B-005, B-006, B-010
  └─ Then audit those items (1-2 hours each)

Result of B-003 audit:
  ├─ If B-001 confirmed missing → HIGH priority for Phase 4+ (performance limit)
  ├─ If B-004/B-005/B-006 correct → Close (document rationale)
  └─ If B-010 confirms skip-month → Close (implementation sound)

B-007–B-013: Independent infrastructure work
  ├─ Can run parallel to R10-R12 validation (B-014–B-022)
  ├─ B-007: DuckDB schema (1 hour, database migration)
  ├─ B-008/B-009: Documentation (30 min each)
  └─ B-012/B-013: Nice-to-have (post-Phase 4)
```

**Recommendation:** 
- Execute B-003 first (fixes audit reliability)
- Then run B-004–B-006, B-010 audits in parallel (2 hours total)
- B-007–B-009 can proceed in parallel to R10-R12 validation (B-014–B-022)
- B-001 marked as "acceptable simplification" per CLAUDE.md; not a paper-trading blocker, but performance improvement documented for Phase 4+

---

## R10-R12 Critical Findings (B-014–B-022)

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

### Phase 0: R-Family Foundation (Parallel, ~12-14 hours total) ⚙️

**Critical Blocker (must run first):**
- **B-003** (2-3 hours): Fix momentum-strategy-audit prompt — unblocks all R4-R11 fixes and audits

**After B-003 (run in parallel):**

**Critical Strategy Fixes (High Impact):**
- **B-026** (1-2 hours): Fix R8 rebalance cadence (252d → 21d)
- **B-027** (2-3 hours): Fix R9 regime-adaptive architecture (4 runs → 1 adaptive)
- **B-029** (1 hour): Fix R11 ranking direction (reversal logic)
- **B-028** (2-3 hours): Fix R10 sector-level ranking
- **B-025** (30 min): Fix R5 registry (George & Hwang, not J&T)

**Sub-total:** ~7-12 hours (run all in parallel after B-003)

**Missing Implementations (Parallel, longer timeline):**
- **B-023** (3-4 hours): Implement R4 (George & Hwang size momentum)
- **B-024** (3-4 hours): Implement R6 (index-transition anomaly)

**Verification & Documentation (Parallel):**
- **B-004, B-005, B-006, B-010** (2 hours total): Audit R1-R3 implementations
- **B-007, B-008, B-009** (1-2 hours): Infrastructure/docs (can overlap with R10-R12)

**Decision after Phase 0:**
- If B-003 reveals audit process is unsalvageable → Pause R-family work until fixed
- If B-026/B-027/B-029 fixes break existing Sharpe → Investigate, re-baseline all R-family
- If B-023/B-024 implementations done → R-family now has full 12 strategies
- If all pass → Proceed to Phase 1 (R10-R12 validation) with complete, corrected R1-R12

---

### Phase 1: R10-R12 Validation (Parallel, ~1 hour) 🧪
**B-014 + B-015:** Execute unexecuted queues, verify results exist

**Decision point after Phase 1:**
- If B-015 Sharpe < 0.70 → Archive R12 immediately; stop R10-R12 work
- If B-015 Sharpe 0.70–0.80 → Proceed to Phase 2
- If B-015 Sharpe > 0.80 → HIGH confidence; continue

**Parallel infrastructure (Phase 1 + Phase 2):**
- B-021, B-022: Data infrastructure gaps (snapshot reconciliation, trade-log schema)
- These proceed in parallel; no dependency on R10-R12 outcome

---

### Phase 2: Design & Robustness Audits (Parallel, ~4 hours) 🔍
**B-018, B-019, B-020 (critical path) + B-016, B-017 (conditional)**

All only execute if B-015 passes (Sharpe > 0.70):
- B-018: Regime stability (2019-2022 vs 2023-2025)
- B-019: Selection bias recomputation (DSR with true trial count)
- B-020: Root-cause robustness failures (fold_stability, benchmarks)
- B-016: Wire liquidity bucketing
- B-017: Test monthly (21d) rebalance cadence

**Critical path:** B-020 (may take 6+ hours if root cause is complex design flaw)

**Gates after Phase 2:**
- If regime concentrated (2019-2022 >> 2023-2025) → Archive R12
- If corrected DSR < 0.5 → Archive R12
- If robustness failures unfixable → Archive R12
- All pass → Continue to Phase 3

---

### Phase 3: Conditional Fine-Tuning (If R12 Survives Phase 2) ✨
**B-016 + B-017:**
- Wire liquidity bucketing into R12
- Test monthly cadence variant

**Gate:** If either shows reversal edge vanishes → Archive R12

---

### Phase 4: Decision & Integration (1-2 days) 📋
- Review all findings from Phase 0–3
- Decide: R10 only? R10 + R12? Or archive all?
- Finalize R-family composition for paper trading
- Proceed to paper trading plumbing (scheduler, dispatch, live signals)

---

## Success Criteria by Phase

| Phase | Success Metric | Failure Metric |
|---|---|---|
| **Phase 0** | B-003 audit prompt fixed; B-004–B-006/B-010 audits pass; B-007–B-009 complete | B-003 reveals unfixable audit bias; audits conflict with code reality |
| **Phase 1** | Both queues complete, results land in `backtest_runs` table | Runs fail, queues crash, or scheduler locks |
| **Phase 2** | Sub-periods both >0.70 Sharpe; corrected DSR >0.5; robustness issues root-caused | Regime concentrated, DSR noise, design flaws unfixable |
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
Phase 0: R-Family Foundation (B-003 critical, then parallel fixes)

Step 1: Fix audit prompt
  └─ B-003: Fix momentum-strategy-audit prompt (2-3 hours)
     ├─ If unfixable → PAUSE all R-family work
     └─ If fixed → Proceed to parallel steps

Step 2: Critical strategy fixes (Parallel, after B-003)
  ├─ B-026: Fix R8 rebalance 252d → 21d (1-2h)
  ├─ B-027: Fix R9 regime-adaptive arch (2-3h)
  ├─ B-029: Fix R11 reversal ranking (1h)
  ├─ B-028: Fix R10 sector ranking (2-3h)
  └─ B-025: Fix R5 registry (30m)
  
  After each fix:
  ├─ If Sharpe breakage → Investigate & revert or fix
  ├─ If passes → Backtest with corrected params
  └─ If all pass → Continue to missing implementations

Step 3: Missing implementations (Parallel, 3-4h each)
  ├─ B-023: Implement R4 (George & Hwang size)
  ├─ B-024: Implement R6 (index-transition)
  └─ After both complete: R-family now has full 12 strategies

Step 4: Verification (Parallel, 2 hours total)
  ├─ B-004–B-010: Audit R1-R3 (30m-2h)
  └─ B-007–B-009: Infrastructure prep (30m-1h)

Decision after Phase 0:
  ├─ If any critical fix breaks R-family → Root-cause & decide revert vs. fix
  ├─ If implementations complete → Proceed to Phase 1
  └─ If audits conflict → Document for Phase 1 review

Phase 1: R10-R12 Validation (B-014/B-015 Execute Queues)
  ├─ Sharpe R12 < 0.70 → ARCHIVE R12, SKIP Phase 2-3, proceed to Phase 4
  ├─ Sharpe R12 0.70-0.80 → Continue to Phase 2 (medium confidence)
  └─ Sharpe R12 > 0.80 → HIGH confidence, continue

Phase 2: Robustness Audits (Parallel, only if B-015 > 0.70)
  ├─ Regime concentrated (2019-2022 >> 2023-2025) → ARCHIVE R12
  ├─ Corrected DSR < 0.5 → ARCHIVE R12
  ├─ Robustness unfixable → ARCHIVE R12
  └─ All pass → Continue to Phase 3

Phase 3: Conditional Fine-Tuning (If R12 survives Phase 2)
  ├─ B-016: Wire liquidity bucketing
  │  └─ Edge disappears → ARCHIVE R12
  ├─ B-017: Test monthly 21d cadence
  │  └─ Sharpe < 0.80 → ARCHIVE R12
  └─ Both pass → R12 APPROVED with monthly cadence

Phase 4: Decision & Integration
  ├─ R10 APPROVED + R12 ARCHIVED → Finalize R10 only
  ├─ R10 APPROVED + R12 APPROVED → Finalize R10+R12 (monthly cadence)
  ├─ R10 ARCHIVED → Revert to simpler baseline
  └─ Document findings, proceed to paper trading plumbing
```

---

## Next Steps

### Phase 0: R-Family Foundation (Critical Path ~12-14 hours)
1. **Immediate (2026-08-25):** Backlog items B-001–B-029 created in backlog_items.yaml
2. **Step 1 (2026-08-26 morning):** Execute B-003 audit prompt fix (2-3 hours) — **GATE for all R4-R11 work**
3. **Step 2 (2026-08-26 ~11am–3pm):** Parallel strategy fixes (7-12 hours total):
   - B-026, B-027, B-029, B-028, B-025 (run in parallel)
   - Backtest each; if breaks, investigate vs. revert
4. **Step 3 (2026-08-27 morning):** Missing implementations (parallel, 6-8 hours):
   - B-023 (R4), B-024 (R6) 
   - These can proceed while Step 2 backtests run
5. **Step 4 (2026-08-27 afternoon):** Verification audits (2 hours):
   - B-004–B-010 (R1-R3 audits)
   - B-007–B-009 (infrastructure/docs)

### Phase 1–4: R10-R12 Validation (Only after Phase 0 complete)
6. **Phase 1 (2026-08-28 morning):** Execute B-014 (R10) + B-015 (R12) queues (~1 hour)
7. **Phase 2 (2026-08-28):** Run B-018–B-020 audits in parallel (4-6 hours)
8. **Phase 3 (2026-08-28–29):** Conditional B-016 + B-017 (if R12 survives, 3-4 hours)
9. **Phase 4 (2026-08-29–30):** Decision meeting + integration planning

### Timeline Summary
- **2026-08-26:** Phase 0 Steps 1-2 (audit fix + strategy fixes, 9-15h total)
- **2026-08-27:** Phase 0 Steps 3-4 (implementations + verification, 8-10h)
- **2026-08-28:** Phases 1-2 (R10-R12 queues + robustness audits, 5-7h)
- **2026-08-29–30:** Phase 3-4 (decision + integration)

**Long-term:** Archive remediation plan; resume R-family work only after paper trading is live

---

## References

- **Backlog Items:** [`backlog_items.yaml`](../backlog_items.yaml) — Full 29 items:
  - **B-001–B-013:** R-family foundation (overlapping portfolios, audit prompt, rebalance cadence, infrastructure)
  - **B-023–B-029:** R4–R11 strategy fixes (missing implementations, ranking/cadence bugs, registry mismatches)
  - **B-014–B-022:** R10-R12 multi-agent review findings (execution, robustness, schema)
- **Memory Update:** Update `project_r_family_complete.md` status from "✅ Complete" to "⏳ Remediation Pending"
- **Related Work:**
  - Paper Trading Phase A–C (Scheduler, dispatch, composite definition) — **HIGHER PRIORITY** (can proceed in parallel)
  - ML40, ML41, ML42 — ML Signal Engine unification
  - T15, F7 — TA Template and Fundamental strategy registry migrations

---

**Bottom line:** 

**Phase 0 (Foundation):** Fix B-003 audit prompt; fix R4-R11 strategy bugs (B-023–B-029, 7-12h); verify B-001–B-010 implementations. This ensures R-family strategy audit reliability AND all 12 strategies are correct, complete, and spec-compliant before R10-R12 validation.

**Phase 0 Scope Expansion:** Original Phase 0 had 9 items; now includes 7 critical R4-R11 fixes (B-023–B-029):
- **R4, R6:** Missing implementations (add 2 strategies)
- **R8, R9, R11:** Critical cadence/ranking/architecture bugs (break Sharpe if not fixed)
- **R10:** Sector ranking mismatch (design simplification)
- **R5:** Registry mismatch (J&T vs. George & Hwang)

**Phases 1–4 (R10-R12):** Execute all 9 remediation items (B-014–B-022) with decision gates at each phase. DO NOT ship R10-R12 to paper trading until all items complete and reviewed.

**All 5 independent reviewers agree:** Current R10-R12 implementations are **not production-ready** (unexecuted queues, unwired code, signal-cadence mismatch, selection bias, robustness failures). **ALSO:** R4-R11 have systematic implementation bugs preventing paper trading. **Comprehensive remediation (B-001–B-029) is mandatory** before integration into live trading.
