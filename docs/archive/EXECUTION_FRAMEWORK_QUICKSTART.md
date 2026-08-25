# 🚀 AlphaLens Execution Framework — Quick Start Guide

**Version:** 2.0  
**Updated:** 2025-06-19  
**Environment:** Ubuntu | Python 3.14.4 | ./projects/AlphaLens

---

## 📋 What Changed From Original CLAUDE_CODE_PROMPTS.md

✅ **Automated execution reports** saved to `./execution_logs/`  
✅ **Intelligent agent selection** per prompt (not a standard pattern)  
✅ **Code review automation** (Medium after each prompt, High at phase end)  
✅ **85% coverage enforcement** with auto-fix and remediation tasks  
✅ **Paper trading metrics** auto-computed  
✅ **Gate checks** with PARTIAL PASS flag and auto-remediation  
✅ **Baseline tracking** with trend analysis  
✅ **Python3 compatibility** for Ubuntu  
✅ **pip-audit integration** for security tracking  
✅ **Utility scripts** for reporting and metrics  

---

## 🗂️ Project Structure

```
./projects/AlphaLens/
├── CLAUDE_CODE_PROMPTS_UPDATED.md        # Updated prompt file (USE THIS)
├── EXECUTION_FRAMEWORK_QUICKSTART.md     # This file
├── README.md                              # Project overview
├── config/                                # Configuration (settings, universe, holidays)
├── datastore/                             # DataStore (DuckDB + SQLite)
├── ingestion/                             # Data pipeline (scrapers, scheduler, quality)
├── features/                              # Feature computation
├── systems/                               # ML signal engine, consumer systems
├── backtest/                              # Backtesting framework
├── tests/                                 # Unit & integration tests
├── requirements/                          # Phase-specific dependencies
│
├── execution_logs/                        # 📝 EXECUTION REPORTS (auto-generated)
├── baselines/                             # 📊 BASELINE METRICS (auto-tracked)
├── paper_trading/
│   └── executions/                        # 📈 PAPER TRADING LOGS (auto-computed)
├── code_reviews/                          # 🔍 CODE REVIEW REPORTS
│
└── scripts/                               # UTILITY SCRIPTS
    ├── execution_report_generator.py      # Generate execution reports
    ├── baseline_tracker.py                # Track baseline metrics
    └── paper_trading_tracker.py           # Track paper trading metrics
```

---

## ⚡ Quick Start (5 Minutes)

### Step 1: Verify Setup
```bash
cd ./projects/AlphaLens

# Check Python version (must be 3.8+)
python3 --version

# Check project structure
ls -d config datastore ingestion features systems backtest tests requirements

# Check execution infrastructure
ls -d execution_logs baselines paper_trading code_reviews
```

**Expected output:**
```
Python 3.14.4
backtest
config
datastore
ingestion
features
systems
tests
requirements
baselines
code_reviews
execution_logs
paper_trading
```

### Step 2: Start Phase 0 — Prompt P0.1
```bash
# Copy this prompt to Claude Code:
# Open: CLAUDE_CODE_PROMPTS_UPDATED.md → PHASE 0 → P0.1

# The prompt includes:
# - Agents to use (Plan, general-purpose)
# - Skills to invoke (code-review, verify)
# - Expected execution report location
# - Test commands using python3
```

### Step 3: Claude Code Execution
When you paste P0.1 prompt into Claude Code:
1. Claude automatically triggers specified agents
2. Implements the prompt
3. Runs tests with coverage tracking
4. Triggers medium-effort code review
5. **Generates execution report** → `./execution_logs/PHASE_0_P0.1_YYYYMMDD_HHMMSS.md`

### Step 4: Review Report
```bash
# View latest report
cat execution_logs/PHASE_0_P0.1_*.md | less

# Or list all reports
ls -ltr execution_logs/ | tail -10
```

---

## 📊 Execution Report Structure

Each report includes:

```markdown
# 📊 PHASE_X — P[X].[Y] — Execution Report
**Status:** PASSED/FAILED/PARTIAL | **Duration:** HH:MM:SS

## 🎯 Executive Summary
- Completion %: XX%
- Coverage: XX% (target: ≥85%)
- Tests: XX/XX PASSED

## 🔧 Execution Details
- Agents Used: [Agent1, Agent2]
- Skills: [Skill1, Skill2]
- Steps: 1. Step 1 ✓, 2. Step 2 ✓, ...

## ✅ Tests & Coverage
- Unit Tests: XX/XX PASSED
- Coverage: XX%
- Code Review Findings: N

## 📝 Code Changes
- Files Modified: [file1, file2]
- Total Lines: +XXX -YYY

## 🧪 Paper Trading Integration
- Signals: XX | Trades: XX | Win Rate: XX% | Sharpe: X.XX

## 📈 Baseline Tracking
- Sharpe Trend: ↑ Improving / ↓ Degrading / → Stable

## 🔒 Phase Gate Check
- Coverage ≥85%: ✓ PASS / ✗ FAIL
- Tests Pass: ✓ PASS / ✗ FAIL
- [Other gates...]

## 🚀 Next Steps
[Recommendations for next prompt]
```

---

## 🔄 Phase Execution Flow

```
┌─────────────────────────────────────────────┐
│ 1. Copy Prompt from CLAUDE_CODE_PROMPTS.md  │
└────────────────┬────────────────────────────┘
                 │
┌────────────────▼────────────────────────────┐
│ 2. Paste into Claude Code                   │
│    Specify: Agents & Skills                 │
└────────────────┬────────────────────────────┘
                 │
┌────────────────▼────────────────────────────┐
│ 3. Claude Executes:                         │
│    • Agents run (parallel if independent)   │
│    • Implementation code                    │
│    • Tests (auto-run with pytest)           │
│    • Code review (Medium effort)            │
└────────────────┬────────────────────────────┘
                 │
┌────────────────▼────────────────────────────┐
│ 4. Auto-Generated Execution Report:         │
│    ./execution_logs/PHASE_X_P[X].[Y]_*.md   │
│    + metrics saved to baselines/            │
│    + paper trading logged to csv            │
└────────────────┬────────────────────────────┘
                 │
┌────────────────▼────────────────────────────┐
│ 5. Review Report                            │
│    ✓ PASSED → Proceed to next prompt        │
│    ✗ FAILED → Fix & retry                   │
│    ⚠ PARTIAL → Fix remediation tasks        │
└────────────────┬────────────────────────────┘
                 │
┌────────────────▼────────────────────────────┐
│ 6. Phase Completion                         │
│    • High-effort code review                │
│    • Update baselines                       │
│    • Manually commit to git                 │
│    • Proceed to next phase                  │
└─────────────────────────────────────────────┘
```

---

## 🎯 Key Commands

### View Latest Report
```bash
ls -ltr execution_logs/ | tail -1 | awk '{print $NF}' | xargs cat
```

### View All Reports for a Phase
```bash
ls -ltr execution_logs/PHASE_0_*.md
```

### Extract Coverage Metrics
```bash
grep "Coverage:" execution_logs/*.md | tail -10
```

### Extract Sharpe Trend
```bash
grep -E "Sharpe|Trend" execution_logs/PHASE_*.md | tail -20
```

### Check Gate Results
```bash
grep -A 10 "Phase Gate Check" execution_logs/PHASE_*_GATE_CHECK_*.md
```

### Analyze Baselines
```bash
python3 scripts/baseline_tracker.py
```

### View Paper Trading Metrics
```bash
python3 scripts/paper_trading_tracker.py
```

---

## 📈 Baseline Tracking

Baselines are saved to: `./baselines/baseline_metrics.json`

### Record Phase Metrics
```python
from scripts.baseline_tracker import BaselineTracker

tracker = BaselineTracker()
tracker.record_phase_metrics("P0", {
    "coverage": 85,
    "tests_passed": 28,
    "tests_total": 28,
})
```

### Get Trend Analysis
```python
trend = tracker.get_trend("P1", "sharpe")
# Returns: {"current": 0.95, "previous": 0.88, "trend": "↑"}
```

### Generate Report
```bash
python3 scripts/baseline_tracker.py
```

---

## 📋 Phase Checklist

### Before Starting Phase 0:
- [ ] Python 3.8+ installed (`python3 --version`)
- [ ] Project structure verified
- [ ] Execution infrastructure ready (`execution_logs/`, `baselines/`, etc.)
- [ ] Updated prompt file in place (`CLAUDE_CODE_PROMPTS_UPDATED.md`)

### For Each Prompt (P0.1, P0.2, ...):
- [ ] Copy prompt verbatim from updated file
- [ ] Specify agents and skills
- [ ] Run in Claude Code
- [ ] Wait for execution report
- [ ] Review: Coverage ≥85%? Tests pass?
- [ ] Check for remediation tasks (if PARTIAL)
- [ ] Proceed to next prompt or fix issues

### At Phase Completion (P0.7 → P0 Gate Check):
- [ ] Run high-effort code review
- [ ] Verify all gates pass
- [ ] Update baselines
- [ ] Manually commit to git
- [ ] Proceed to Phase 1 or fix blockers

---

## ⚠️ If Something Fails

### Scenario 1: Test Coverage < 85%
```markdown
❌ Gate Check FAILED: Coverage < 85% (82%)

📝 Remediation Tasks:
- [ ] Add tests for ingestion/quality/validator.py (2h)
- [ ] Add integration tests for DataStore API (3h)

Action:
1. Fix the failing tests
2. Re-run: python3 -m pytest tests/ --cov=. --cov-report=term
3. Verify coverage ≥85%
4. Re-run phase prompt
```

### Scenario 2: Code Review Findings
```markdown
🔍 Code Review (Medium Effort): 3 findings

1. Unused import in datastore/client.py:15 → FIXED
2. Type hint missing in validator.py:42 → FIXED
3. Consider helper function extraction → NOTED for future
```

### Scenario 3: Test Failures
```bash
# Re-run failing tests
python3 -m pytest tests/unit/test_scheduler.py -v

# If error occurs:
# Paste error into Claude Code:
# "The error is: [paste exact error]. Fix it and retry."
```

---

## 🔧 Utility Scripts

### 1. Execution Report Generator
```bash
python3 scripts/execution_report_generator.py
```
Generates markdown reports for executions.

### 2. Baseline Tracker
```bash
python3 scripts/baseline_tracker.py
```
Tracks and analyzes baseline metrics across phases.

### 3. Paper Trading Tracker
```bash
python3 scripts/paper_trading_tracker.py
```
Computes trading metrics: win rate, Sharpe, drawdown, etc.

---

## 📚 File Locations

| Item | Location |
|------|----------|
| Updated Prompts | `./CLAUDE_CODE_PROMPTS_UPDATED.md` |
| Execution Reports | `./execution_logs/PHASE_X_*.md` |
| Baseline Metrics | `./baselines/baseline_metrics.json` |
| Paper Trading Logs | `./paper_trading/executions/*.csv` |
| Code Reviews | `./code_reviews/*.md` |
| Utility Scripts | `./scripts/*.py` |

---

## 🎓 Example: Running P0.1

### Step 1: Find P0.1 Prompt
```bash
grep -A 100 "## P0.1 — Project Skeleton" CLAUDE_CODE_PROMPTS_UPDATED.md | head -50
```

### Step 2: Copy Prompt
```
📋 **PROMPT:**
```
Read docs/CLAUDE.md, docs/12_platform_architecture.md, and docs/specs/08_specifications.md 
sections SPEC-SYS-001 through SPEC-DS-007.

Create the full project skeleton:
...
```

### Step 3: Paste into Claude Code
Specify:
- Agents: Plan (design), general-purpose (implement)
- Skills: code-review (Medium), verify

### Step 4: Claude Code Execution
- Designs folder structure (Plan agent)
- Implements files (general-purpose agent)
- Runs tests with coverage tracking
- Triggers code review
- Generates report to: `./execution_logs/PHASE_0_P0.1_20250619_120000.md`

### Step 5: Review Report
```bash
cat ./execution_logs/PHASE_0_P0.1_20250619_120000.md
```

Output:
```markdown
# 📊 PHASE_0 — P0.1 — Execution Report
Status: PASSED | Duration: 12m 34s

## 🎯 Executive Summary
- Completion %: 95%
- Coverage: 86% ✓
- Tests: 28/28 PASSED ✓
- Critical Issues: None

...
```

### Step 6: Proceed
✓ Coverage ≥85%, tests pass → Ready for P0.2

---

## ✨ Best Practices

1. **Always use the updated file:** `CLAUDE_CODE_PROMPTS_UPDATED.md`
2. **Copy prompts verbatim:** Don't modify prompt text
3. **Specify agents upfront:** Tell Claude which agents to use
4. **Review reports immediately:** Don't skip this step
5. **Fix issues before proceeding:** Remediation is important
6. **Commit manually:** You control git decisions
7. **Track baselines:** Monitor improvements across phases
8. **Monitor paper trading:** Metrics should improve phase-to-phase

---

## 📞 Questions?

If something doesn't work:
1. Check the execution report first
2. Review error logs in the report
3. Copy exact error to Claude Code
4. Ask Claude to fix and retry

---

**Ready to start?** Open `CLAUDE_CODE_PROMPTS_UPDATED.md` and copy the P0.1 prompt into Claude Code. 🚀
