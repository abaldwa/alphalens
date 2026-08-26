# Implementation Plan: R-Family Strategy Remediation

**Branch**: `feature/r-family-remediation` | **Date**: 2026-08-26 | **Spec**: [spec.md](spec.md)

**Status**: BLOCKING PAPER TRADING | **Severity**: CRITICAL

**Input**: Remediation requirements from `/docs/R_FAMILY_REMEDIATION_PLAN.md`

---

## Executive Summary

Multi-agent review (2026-08-25) identified **9 critical blockers** preventing R10-R12 strategies from shipping to paper trading. Additionally, R4-R11 have systematic implementation bugs (registry mismatches, cadence errors, missing code, unwired analysis). 

**This plan:** Execute 29 remediation items (B-001–B-029) across 4 phases:
- **Phase 0 (Foundation):** Fix audit reliability (B-003), correct R4-R11 strategy bugs (B-023–B-029), verify implementations (B-001–B-013)
- **Phase 1 (Validation):** Execute R10-R12 backtest queues (B-014–B-015), verify results exist
- **Phase 2 (Robustness):** Sub-period stability, selection bias recomputation, root-cause failures (B-018–B-020)
- **Phase 3 (Fine-tuning):** Conditional optimization if R12 survives Phase 2 (B-016–B-017)
- **Phase 4 (Integration):** Final decision and paper-trading integration

**Unanimous Verdict**: DO NOT SHIP R10-R12 until all items complete.

---

## Technical Context

**Scope**: R-family momentum strategies (R1–R12) + validation infrastructure

**Dependencies**:
- `backtest/core/engine.py` — backtest engine & integrity checks
- `backtest/core/overfit_checks.py` — robustness validation (walk-forward, bootstrap)
- `backtest/core/ml_signal_engine.py` — model loading and signal generation
- `features/momentum_universe.py` — momentum signal computation
- `backlog_items.yaml` — Tracks 29 remediation items (B-001–B-029)
- `strategy_registry` — Strategy definitions and parameters
- DuckDB tables: `backtest_runs`, `strategy_signals`, `trade_log`
- Python 3.10+ | Pytest | CatBoost/Ridge models

**Key Blockers**:
1. **B-003**: momentum-strategy-audit prompt has false positives (blocks all R4-R11 audits)
2. **B-026–B-029, B-025**: R4–R11 strategy implementation bugs (cadence, ranking, registry)
3. **B-023, B-024**: R4, R6 missing implementations (new strategies not yet coded)
4. **B-014–B-015**: R10-R12 queues never executed (results don't exist)
5. **B-020**: Robustness check failures on R12 (fold_stability, benchmarks)

**Success Criteria**:
- All 29 backlog items have decision/action (completed, deferred, or archived)
- R10-R12 have executable backtest results in `backtest_runs` table
- R1–R9 verified correct per specification and audit-compliant
- Audit prompt fixes validated with adversarial test cases
- R12 sub-period breakdown confirms stability or justifies archival

---

## Constitution Check

**Principle II — Architecture Is Auditable** ✅
- All strategy definitions registered in `strategy_registry` (B-003 validates this)
- All generated signals persisted to `strategy_signals` table (B-022 adds missing columns)
- Audit trail traceable: signal → trade → portfolio return

**Principle III — Backtest Numbers Are Trustworthy** ✅
- Point-in-time universe ranking enforced in engine (B-006 audits this)
- Rebalance cadences verified per spec (B-026–B-027 fix bugs; B-006 audits)
- DSR multiple-testing correction applied (B-019 recomputes with true trial count)
- Regime overlay compatibility preserved (B-008 documents R9 4-mode system)

**Principle IV — Data Integrity Under Concurrency** ✅
- All writes through `defer_db_writes` (existing pattern)
- No synthetic rows in real DB (use isolated DB for verification)
- Backtest queue execution single-threaded (B-014–B-015)
- No mid-queue source edits during execution

**Principle V — Feature Ingestion Is Wholesale** ✅
- No ticker-subset feature backtests in this phase
- Reuses existing `momentum_universe.py` pipeline (wholesale by design)

**Principle VII — Spec-First for Large Initiatives** ✅
- Remediation plan specifies all 29 items upfront
- Multi-agent review completed before code changes
- Decision tree documented for each phase

---

## Project Structure

### Documentation (this feature)

```text
specs/r-family-remediation/
├── plan.md              # This file (implementation strategy)
├── spec.md              # Feature spec (R_FAMILY_REMEDIATION_PLAN.md)
├── research.md          # Phase 0 output (unknowns resolved)
├── data-model.md        # Phase 1 output (entities, state)
├── quickstart.md        # Phase 1 output (validation guide)
└── contracts/           # Phase 1 output (backtest API contracts)
```

### Source Code (repository root) — No structural changes required

```text
backtest/
├── core/
│   ├── engine.py               [EXISTING: backtest orchestration]
│   ├── ml_signal_engine.py     [EXISTING: model loading/prediction]
│   ├── overfit_checks.py       [EXISTING: robustness validation]
│   └── integrity_checker.py    [EXISTING: trade validation]
├── adapters/
│   └── momentum_adapter.py     [MODIFY: R8-R9 cadence fixes; B-026–B-027]
├── run_orchestrator_backtest.py [EXISTING: backtest runner]
└── run_strategy_queue.py       [EXISTING: queue executor]

features/
├── momentum_universe.py         [AUDIT: verify ADTV floors, lookback periods]
├── r4_george_hwang.py          [NEW: R4 size momentum strategy; B-023]
└── r6_index_transition.py      [NEW: R6 index-transition anomaly; B-024]

strategy_registry/
└── momentum_strategies.json     [MODIFY: Fix R5, add R4/R6, correct cadences]

datastore/
├── api/routers/backlog.py      [MODIFY: Backlog item tracking]
└── schema/create_normalised.py [MODIFY: announcement_date VARCHAR→DATE; B-007]

backtest/queues/
├── r10_validation.json         [EXISTING: queue definition (never run)]
└── r12_validation.json         [EXISTING: queue definition (never run)]

tests/
├── unit/test_momentum_strategies.py    [AUDIT: R1–R3]
├── integration/test_robustness.py      [NEW: B-018–B-020 verification]
└── quality/test_no_synthetic_data.py   [EXISTING: data integrity gate]
```

**Structure Decision**: No new subsystems. All changes are:
- Bug fixes in existing adapters (B-026–B-027)
- New strategy implementations (B-023–B-024)
- Audit prompt rewrite (B-003)
- Data schema fix (B-007)
- Backtest queue execution (B-014–B-015)

---

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| Multi-agent review conducted 2026-08-25 | Strategies were unverified; audit prompt unreliable | Single-agent review misses cross-domain issues (ML rigor + domain context + backtest correctness) |
| 4-phase rollout with gates | Strategies may fail validation; early exit avoids wasted work | Waterfall approach wastes time on Phase 1 if Phase 0 audit reveals unfixable bugs |
| Sub-period stability testing (B-018) | Selection bias concentrated in 2019-2022 COVID recovery window | Annual backtest cannot detect regime concentration; mandatory for claims of "robust reversal edge" |

---

## Phase Breakdown

### Phase 0: R-Family Foundation (Critical Path ~12-14 hours) ⚙️

**Objective**: Fix audit reliability, correct R4-R11 bugs, verify R1-R3 implementations.

**Critical Blocker (run first)**:
- **B-003** (2-3 hours): Fix momentum-strategy-audit prompt
  - Current: False positives; returned APPROVED despite missing overlapping portfolios + skip-month
  - Fix: Rewrite with specification-as-code, adversarial test cases, citation requirements
  - Unblocks: B-001, B-004, B-005, B-006, B-010 (all R4-R11 fixes depend on audit verification)

**Critical Strategy Fixes (run in parallel after B-003)**:
- **B-026** (1-2 hours): Fix R8 rebalance cadence (252d annual → 21d monthly)
- **B-027** (2-3 hours): Fix R9 architecture (4 separate runs → regime-adaptive single run)
- **B-029** (1 hour): Fix R11 ranking direction (highest → lowest reversal logic)
- **B-028** (2-3 hours): Fix R10 ranking (individual → sector-level)
- **B-025** (30 min): Fix R5 registry (J&T → George & Hwang)

**Missing Implementations (parallel, longer)**:
- **B-023** (3-4 hours): Implement R4 (George & Hwang size momentum)
- **B-024** (3-4 hours): Implement R6 (index-transition anomaly)

**Verification & Documentation (parallel)**:
- **B-004, B-005, B-006, B-010** (2 hours): Audit R1-R3 implementations
- **B-007, B-008, B-009** (1-2 hours): Infrastructure fixes (schema, docs)

**Gate After Phase 0**:
- ✅ B-003 audit prompt fixed and validated with adversarial tests
- ✅ B-026–B-029 fixes pass backtests (Sharpe unchanged or improved)
- ✅ B-023–B-024 implementations complete
- ✅ B-004–B-010 audits confirm R1-R11 are spec-compliant
- 🔴 If any fail: Root-cause, revert, or redesign before Phase 1

### Phase 1: R10-R12 Validation (~1 hour execution + review) 🧪

**Objective**: Execute unexecuted validation queues, verify results exist.

**Items**:
- **B-014** (30 min exec): Execute R10 validation queue, verify results in `backtest_runs`
- **B-015** (40 min exec): Execute R12 validation queue, verify Sharpe & metrics

**Parallel Infrastructure**:
- **B-021** (2-3 hours): Add backtest cache snapshot reconciliation check
- **B-022** (1-2 hours): Add `sector` and `liquidity_bucket` columns to trade_log

**Gate After Phase 1**:
- ✅ Both queues complete without scheduler lock or crashes
- ✅ Results visible in `backtest_runs` table with valid Sharpe/Calmar/DD metrics
- ✅ R10 Sharpe > 0.65 (minimum viability)
- 🔴 R12 Sharpe < 0.70 → ARCHIVE R12 immediately; proceed to Phase 4

### Phase 2: Design & Robustness Audits (~4 hours, conditional on Phase 1 pass) 🔍

**Objective**: Validate R12 robustness across regimes, selection bias, and design assumptions.

**Only execute if Phase 1 B-015 Sharpe > 0.70**:
- **B-018** (2-3 hours): Sub-period stability (2019-2022 vs 2023-2025 breakdown)
- **B-019** (1-2 hours): Recompute DSR with true trial count (n_trials=100+, not 2)
- **B-020** (4-6 hours, highest-risk): Root-cause fold_stability & benchmarks check failures

**Conditional Fine-Tuning (if time/findings warrant)**:
- **B-016** (3-4 hours): Wire liquidity bucketing into R12 analysis
- **B-017** (2-3 hours): Test monthly (21d) rebalance cadence variant

**Gate After Phase 2**:
- ✅ Sub-periods both > 0.70 Sharpe (regime not concentrated in 2019-2022)
- ✅ Corrected DSR > 0.5 (edge likely real, not noise)
- ✅ Robustness failures root-caused and justified (or design fixed)
- 🔴 Regime concentrated / DSR < 0.5 / unfixable design → ARCHIVE R12

### Phase 3: Conditional Fine-Tuning (conditional on Phase 2 pass) ✨

**Objective**: Optimize R12 (if it survives Phase 2).

**Items** (only if Phase 2 gate passes):
- **B-016**: Wire liquidity bucketing; verify edge doesn't disappear
- **B-017**: Test monthly 21d cadence; Sharpe must stay > 0.80

**Gate After Phase 3**:
- ✅ Both optimizations preserve or improve Sharpe
- 🔴 Edge disappears or Sharpe drops below 0.80 → ARCHIVE R12

### Phase 4: Decision & Integration (1-2 days) 📋

**Objective**: Finalize R-family composition and plan paper-trading integration.

**Items**:
- Review all Phase 0–3 findings
- Decide: R1-R9 only? R1-R10? R1-R10+R12 (monthly)?
- Document decision rationale
- Plan paper-trading dispatcher integration (separate feature)
- Archive any strategies that failed gates

---

## Success Criteria by Phase

| Phase | Success | Failure |
|-------|---------|---------|
| **Phase 0** | B-003 prompt fixed; B-026–B-029 fixes don't break Sharpe; B-023–B-024 complete; B-001–B-010 audits pass | Audit unfixable; fixes break core strategy Sharpe; implementations incomplete |
| **Phase 1** | Both queues execute, results in table, R10/R12 Sharpe > 0.65–0.70 | Queues fail, scheduler locks, results don't land |
| **Phase 2** | Sub-periods > 0.70, DSR > 0.5, robustness issues root-caused | Regime concentrated, selection bias noise, unfixable design flaws |
| **Phase 3** | Monthly cadence > 0.85 Sharpe, liquidity bucketing confirms edge | Edge disappears with fine-tuning, illiquid names break reversal |
| **Phase 4** | Clear decision: R10 APPROVED, R12 {APPROVED/ARCHIVED} with rationale | Ambiguous findings, blockers remain unfixed |

---

## Risk Mitigation

| Risk | Mitigation |
|---|---|
| Backtest execution fails (scheduler hang) | Pre-check: `systemctl --user status alphalens-scheduler.service`; use `fuser` to detect locks; run during off-peak (nights/weekends) |
| R12 findings warrant archival | **This is a valid outcome.** Focus on R10 + paper-trading plumbing instead. Reversal edge may require conditions we don't have (liquidity concentration, sector bet). |
| Robustness failures reveal unfixable design | Document limitation, accept edge case (e.g., "edge concentrated in bull markets"), or archive. Don't force-fit. |
| Phase 2 audits take > 8 hours | B-020 is highest-risk; prioritize first (2026-08-28 morning). Parallelize B-018–B-019 while B-020 runs. |
| B-003 audit prompt rewrite unsalvageable | Fallback: Disable adversarial checks, accept that audit reliability is lower, document risk. Proceed with manual spot-checks. |

---

## Timeline & Resource Allocation

**Total Duration**: 5-7 working days (2026-08-26 → 2026-08-30)

### Day 1 (2026-08-26): Phase 0 Step 1 — Critical Blocker
- **Morning (2h)**: B-003 audit prompt fix + validation
- **Blocker gate**: If unfixable, pause all R-family work

### Day 2 (2026-08-26 afternoon → Day 3 morning): Phase 0 Steps 2-3 — Parallel Fixes
- **Day 2 afternoon (6-8h)**: B-026–B-029, B-025 strategy fixes (parallel)
- **Day 3 morning (3-4h)**: B-023–B-024 implementations (parallel with backtests)

### Day 3 (2026-08-27 afternoon): Phase 0 Step 4 — Verification
- **2h**: B-004–B-010 audits + B-007–B-009 infrastructure

**Gate**: Phase 0 all items complete, no critical failures

### Day 4 (2026-08-28 morning): Phase 1 — Validation
- **1h**: B-014–B-015 queue execution + verification
- **Gate**: R12 Sharpe decision (proceed vs. archive)

### Day 4 (2026-08-28 afternoon): Phase 2 — Robustness (conditional)
- **4-6h**: B-018–B-020 audits (if R12 Sharpe > 0.70)

### Day 5 (2026-08-28–29): Phase 3 — Fine-tuning (conditional)
- **3-4h**: B-016–B-017 (if Phase 2 gate passes)

### Day 6 (2026-08-29–30): Phase 4 — Decision & Integration
- **4h**: Review findings, finalize composition, document rationale, plan paper-trading integration

---

## Next Steps (Immediate)

1. **Backlog items already created** (2026-08-25): See `backlog_items.yaml` for all 29 items (B-001–B-029)
2. **Start Phase 0 Step 1** (2026-08-26 morning): Execute B-003 audit prompt fix
3. **Monitor Phase 0 gate** (2026-08-26): If B-003 unfixable, escalate immediately
4. **Parallelize Phase 0 fixes** (2026-08-26 afternoon): B-026–B-029 + B-023–B-024 in parallel
5. **Execute Phase 1** (2026-08-28): Queue execution + Sharpe gate decision
6. **Phase 2 planning** (2026-08-28): If R12 survives, allocate B-020 (highest-risk, longest-running)

---

## Decision Tree (Quick Reference)

```
Phase 0: Foundation
├─ B-003: Fix audit prompt (2-3h)
│  └─ If unfixable → PAUSE all R-family work
│  └─ If fixed → Proceed to parallel fixes
├─ B-026–B-029, B-025: Fix strategy bugs (7-12h parallel)
│  └─ If Sharpe breaks → Root-cause & revert or fix
├─ B-023–B-024: Implement missing (6-8h parallel)
│  └─ If incomplete → Defer to Phase 5 (post-paper-trading)
└─ B-001–B-010: Verify implementations (2h)
   └─ If audits fail → Fix before Phase 1

Phase 1: Validation (1h execution)
├─ B-014–B-015: Execute queues
└─ If R12 Sharpe < 0.70 → ARCHIVE R12, skip to Phase 4
   If R12 Sharpe > 0.70 → Proceed to Phase 2

Phase 2: Robustness (4-6h, conditional)
├─ B-018–B-020: Sub-period, DSR, robustness failures
└─ If regime concentrated / DSR < 0.5 / unfixable → ARCHIVE R12, skip to Phase 4
   If all pass → Proceed to Phase 3

Phase 3: Fine-tuning (3-4h, conditional)
├─ B-016–B-017: Liquidity bucketing, monthly cadence
└─ If edge disappears → ARCHIVE R12
   If robust → Proceed to Phase 4

Phase 4: Decision (4h)
└─ Final: R1-R9? R1-R10? R1-R10+R12 (monthly)?
   Document rationale, plan paper-trading integration.
```

---

## Related Work (Parallel Tracks)

- **Paper Trading Phase A–C** (Higher Priority): Scheduler, dispatch, composite definition — can proceed in parallel with Phase 0-1
- **ML40, ML41, ML42**: ML Signal Engine unification (separate feature)
- **T15, F7**: TA Template and Fundamental registry migrations (separate feature)

---

## References

- **Backlog Items**: `backlog_items.yaml` (29 items: B-001–B-029)
- **Spec**: [R_FAMILY_REMEDIATION_PLAN.md](../../docs/R_FAMILY_REMEDIATION_PLAN.md)
- **Constitution**: `.specify/memory/constitution.md`
- **Decision Tree**: [Section above](#decision-tree-quick-reference)
