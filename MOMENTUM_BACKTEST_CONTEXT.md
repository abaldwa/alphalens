# AlphaLens Momentum Backtest — Context for Next Conversation

**Created:** 2026-09-04 | **Purpose:** Handoff document for continuing momentum strategy work

---

## Quick Reference: What Was Done This Conversation

### 🔍 Issue Identified
- R1 backtest taking 20+ hours (expected 2-3 hours)
- Root cause: Missing `rank_method` and `crash_regime_enabled` parameters in 5 queue generators
- Impact: 312 jobs across R0_isolation, R1_full, R1_queue, R8, R9 running with wrong strategy identities

### ✅ Root Cause Fixed
- **Commit:** `c7f3aaa9` (2026-09-04 06:25:50)
- **Change:** Added explicit parameters to 5 generators
- **Status:** Queue files regenerated; ready for re-run

### ⚠️ Why Results Lost
- New corrected queue started at 06:27:38
- Jobs ran successfully but `defer_db_writes=True` meant results weren't persisted to disk
- Process force-killed at 07:31 before final database write
- **Action needed:** Restart queue cleanly without interruption

---

## 📚 Core Documentation

### Primary: Comprehensive Analysis (THIS conversation)
📄 **`/home/amit/projects/AlphaLens/MOMENTUM_STRATEGY_ANALYSIS.md`** (823 lines)
- Part 1: All strategies R0-R13 with parameters, pseudo-code, descriptions
- Part 2: Bug investigation (why 6 jobs for R8/R9)
- Part 3: Versioning strategy (3-tier naming)
- Part 4: Detailed work list with bash commands
- Part 5: Execution summary
- Part 6: Next steps & recommendations

**Read this first.** It has everything you need for Part 1 (strategy details) and Part 4 (work list).

---

### Secondary: Agent's Deep Inventory
📄 **`/tmp/claude-1000/-home-amit-projects-AlphaLens/13de82d0-c271-4dd2-81d9-20bfbf5f8366/scratchpad/momentum_strategy_inventory.md`** (36KB)

Detailed breakdown of all 13 R-family strategies with:
- Full pseudo-code (15-20 lines each)
- Phase validation status
- Database query results
- Core algorithm descriptions

**Reference this** for in-depth strategy understanding when implementing Phase B/C.

---

### Tertiary: Pseudocode Reference
📄 **`/tmp/claude-1000/-home-amit-projects-AlphaLens/13de82d0-c271-4dd2-81d9-20bfbf5f8366/scratchpad/momentum_pseudocode_reference.py`** (32KB)

Working pseudocode for:
- Each strategy's core logic (R1, R5, R7-R13)
- Helper functions (ranking, vol computation, regime detection)
- Main backtest loop orchestration

**Use this** when validating backtest code or understanding signal generation.

---

## 🎯 Immediate Action Items

### Priority 1: Restart R1_full_campaign_216 (BLOCKING)
```bash
cd /home/amit/projects/AlphaLens
PYTHONPATH=. python3 backtest/run_strategy_queue.py \
  --queue-file backtest/queues/r1_full_campaign_216.json \
  --max-workers 1 2>&1 | tee /tmp/r1_full_clean.log

# Let this run to completion (~6-8 hours)
# Do NOT force-kill mid-run
```

**Expected output:**
- Job[0] completes in ~2-3 min (vs 11.4 min with bug)
- All 216 jobs persisted to database with strategy_id `R3_*_skip1mo`
- Final log should show: "run_strategy_queue: queue completed successfully"

**Success criteria:**
- [ ] CAGR distribution by band (expect 2-14% range, avoid -1.65%)
- [ ] Cache fallback returning ~1,292 tickers per rebalance
- [ ] All jobs show created_at timestamps in 2026-09-04 06:XX to 14:XX window

---

### Priority 2: Regenerate Sibling Queues (after P1)
```bash
# R0_isolation (60 jobs, ~30 min)
PYTHONPATH=. python3 backtest/run_strategy_queue.py \
  --queue-file backtest/queues/r0_isolation_2009_2026.json \
  --max-workers 1

# R8 (6 jobs, ~10 min)
PYTHONPATH=. python3 backtest/run_strategy_queue.py \
  --queue-file backtest/queues/r8_full_2009_2026.json \
  --max-workers 1

# R9 (6 jobs, ~10 min)
PYTHONPATH=. python3 backtest/run_strategy_queue.py \
  --queue-file backtest/queues/r9_full_2009_2026.json \
  --max-workers 1
```

**Combined:** ~50 min for all three.

---

## 📊 Database State

### Current Runs (Pre-Fix, from Early Morning 2026-09-04)
```
3,835 M-family runs (CAGR 0.08%, Sharpe 0.5226)
  246 R3 runs (CAGR 0.08%, Sharpe 0.5457) ← These are "R1" with skip=1, ran slow
   1,518 R1 runs (CAGR 0.06%, Sharpe 0.4022) ← Some of these are corrupted results
    7 R0 runs (CAGR 0.06%, Sharpe 0.3406)
```

### After P1 Completion (Expected)
```
Will add: 216 new R3 runs (skip_months=1) with correct parameters
Will add: 60 new R0_isolation runs (weight_method variations)
Will add: 6 new R8 runs (vol-target validation)
Will add: 6 new R9 runs (regime-switching validation)
```

---

## 🔧 Key Code Locations

**Strategy Definitions:**
- `backtest/generate_r*.py` — Queue generators (parameters)
- `features/momentum_signal.py` — Signal computation
- `backtest/adapters/momentum_adapter.py` — Cache & signal adapter

**Engine:**
- `backtest/core/engine.py` — Portfolio simulation
- `backtest/core/metrics.py` — CAGR, Sharpe, drawdown calcs
- `backtest/core/integrity_checker.py` — Trade validation

**Cache:**
- `backtest/cache/momentum_rankings.duckdb` — 176M pre-computed rankings
- Offset logic: momentum_adapter.py line ~611 (cache date shifting for skip_months)

---

## 📋 Work List — Copy-Paste Checklist

Use this to track progress in next conversation:

```
PHASE A: Core Momentum Validation (R0, R1, R3, R8, R9)
─────────────────────────────────────────────────────
Priority 1 (Blocker):
  [ ] Restart R1_full_campaign_216 cleanly
      Time: 6-8 hours
      Command: See "Immediate Action Items" above

Priority 2 (Dependent):
  [ ] Verify job[0] runtime < 2 min
  [ ] Query: SELECT COUNT(*) FROM backtest_runs WHERE strategy_id LIKE 'R3_%skip1mo%'
      Expect: 216 rows with created_at on 2026-09-04 after 06:00
  [ ] Check CAGR distribution (should be 2-14% range, not -1.65%)

Priority 3 (Parallel):
  [ ] Regenerate R0_isolation_60
  [ ] Regenerate R8_6
  [ ] Regenerate R9_6
  [ ] Total time: ~50 min

Acceptance:
  [ ] All 288 jobs (216+60+6+6) in database
  [ ] All have correct strategy_id with current implementation
  [ ] Sharpe > 0.30 average (vs 0.0002 pre-fix)

PHASE B: Alternative Approaches (R10, R11, R13)
──────────────────────────────────────────────
  [ ] Generate r10_sector_momentum.json
  [ ] Generate r11_52wk_reversal.json
  [ ] Generate r13_bollinger_reversal.json
  [ ] Run 18-job queue
  [ ] Time: ~1 hour

PHASE C: Ensemble (R12)
──────────────────────
  [ ] Verify multi_signal_ensemble in features/momentum_signal.py
  [ ] Generate r12_ensemble.json (6 jobs)
  [ ] Run backtest
  [ ] Time: ~30 min

PHASE D: Integrity & Reporting
──────────────────────────────
  [ ] Run backtest/core/integrity_checker.py on all Phase A/B/C runs
  [ ] Generate matrix: 6 bands × 5 strategies = 30 cells
  [ ] Publish report to http://localhost:5173/backtest-dashboard
  [ ] Time: 1-2 hours

PHASE E: Production Readiness (Future)
──────────────────────────────────────
  [ ] Declare production-ready strategy (likely R1 or R9)
  [ ] Deploy to paper_trading/
  [ ] Monitor live signals for 1 week
  [ ] Time: 1-2 weeks
```

---

## 🗝️ Key Findings from Investigation

1. **Cache Fallback Bug Fixed**
   - Before: Returned 1 ticker (LIMIT 1 clause)
   - After: Returns ~1,292 tickers per rebalance
   - Performance: 100x speedup in fallback path

2. **Skip-Months Logic Working**
   - Query date correctly offset by skip_months × 21 trading days
   - Example: Asking for 2009-04-01 with skip_months=1 queries cache at 2009-03-11
   - Implements Jegadeesh & Titman 1993 reversal-avoidance principle

3. **Strategy Identity Bug Root Cause**
   - Missing parameters → orchestrator defaults → wrong strategy assignment
   - R1 (skip_months=0, crash_regime=False) was being executed as R3 (crash_regime=True)
   - R3 with crash overlay: 11.4 min/job vs R1 baseline: 50-60 sec/job
   - 8-15x slowdown explains the 20+ hour complaint

4. **Parameter Versioning Need**
   - Proposed 3-tier naming: strategy_id + implementation_tag + code_hash
   - Prevents confusion between "same params, different implementation"
   - Recommended database schema: add `implementation_tag` field to config_json

---

## 📞 Questions & Answers from Investigation

**Q: Why only 6 jobs for R8/R9?**
A: Not about bug scope—it's about strategy design. R8 and R9 test ONE overlay approach per band (6 bands), unlike R0_isolation (60 jobs varying weight methods) or R1_full (216 jobs varying all parameters).

**Q: Should R0 run before R1?**
A: No dependency; they're independent backtest runs. Both query the same momentum_rankings cache. However, for clean data lineage, recommend regenerating all affected queues (R0, R1, R8, R9) together after bug fixes.

**Q: Why were results lost?**
A: `defer_db_writes=True` defers writes until job completes and acquires write lock. Force-killing process = in-memory results lost. Solution: restart without interruption, or remove defer_db_writes flag.

**Q: Which strategy should go to production?**
A: TBD after Phase A/B/C. Candidates: R1 (baseline, 0.06% CAGR pre-fix but likely better post-fix) vs R9 (regime-adaptive, more complex but better theoretical foundation).

---

## 🚀 Next Conversation Handoff

**Start with:**
1. Read `MOMENTUM_STRATEGY_ANALYSIS.md` (this conversation's findings)
2. Execute Priority 1 (restart R1_full_campaign_216)
3. Monitor first 3 jobs for performance (should see 2-3 min per job)
4. Use work list checklist to track Phase A/B/C progress

**Reference as needed:**
- Strategy inventory (agent's detailed doc) for algorithm details
- Pseudocode reference for implementation validation
- Git commits for understanding what changed (c7f3aaa9, 8aaf7a57)

**Critical:** Don't force-kill the queue. Let it run to completion. This will take 6-8 hours but is necessary to persist all results.

---

**Status:** ✅ Ready for next conversation | 🔴 Awaiting P1 execution | 📊 All strategy documentation complete

