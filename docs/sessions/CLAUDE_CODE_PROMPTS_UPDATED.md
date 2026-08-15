# AlphaLens — Enhanced Claude Code Execution Framework
## Phases 0–4 · Automated Reporting · Integrated Testing · Code Review · Baseline Tracking

**How to use this framework:**
- Each phase prompt is triggered **manually** from Claude Code
- Execution reports are **auto-generated** to `./projects/AlphaLens/execution_logs/PHASE_X_YYYYMMDD_HHMMSS.md`
- Code review is **automatically triggered**: Medium effort after every prompt, High effort at phase completion
- Unit tests run after every phase and must achieve **≥85% coverage**
- All commands use `python3` (Ubuntu native)
- Paper trading metrics are **auto-computed** into execution reports
- Gate check failures are **flagged as PARTIAL PASS** with remediation task lists
- Baselines are tracked with trend analysis across phases

---

## Legend & Execution Flow

```
📋 PROMPT        — Paste into Claude Code (manually triggered per phase)
🤖 AGENTS        — Compound Engineering agents used for this prompt
🛠️  SKILLS        — Claude Code skills to invoke
✅ TESTS         — Auto-executed after prompt completion (≥85% coverage required)
📊 REPORT        — Auto-generated execution report with metrics
🔀 CODE REVIEW   — Auto-triggered: Medium (per-prompt), High (phase-end)
📈 TRACKING      — Metrics logged to execution report & baseline database
🔒 GATE          — Phase gate check; PARTIAL PASS if fails (auto-remediation tasks)
⚠️  MANUAL       — User manual action required (not automated)
```

---

## Pre-Execution Setup

Before running any phase prompt, ensure the following:

```bash
# 1. Create execution logs directory
mkdir -p ./projects/AlphaLens/execution_logs

# 2. Create baseline tracking database
mkdir -p ./projects/AlphaLens/baselines
touch ./projects/AlphaLens/baselines/baseline_metrics.json

# 3. Create paper trading logs
mkdir -p ./projects/AlphaLens/paper_trading/executions

# 4. Create code review reports directory
mkdir -p ./projects/AlphaLens/code_reviews

# 5. Verify Python 3 is available
python3 --version  # Should be 3.8+

# 6. Verify project structure
cd ./projects/AlphaLens
ls -la | grep -E "config|datastore|ingestion|features|systems|backtest|tests"
```

---

## Execution Report Template

Every prompt generates a markdown report in this structure:

```markdown
# 📊 PHASE_X — [Prompt Name] — Execution Report
**Date:** YYYY-MM-DD HH:MM:SS UTC | **Duration:** HH:MM:SS | **Status:** PASSED/FAILED/PARTIAL

---

## 🎯 Executive Summary
- **Completion %:** XX%
- **Key Metrics:** [Sharpe, Coverage, Test Results, etc.]
- **Critical Issues:** [None / Listed]
- **Decisions Made:** [List of important choices]

---

## 🔧 Execution Details
- **Prompt ID:** P[X].[Y] — [Name]
- **Agents Used:** [Agent1, Agent2]
- **Skills Invoked:** [Skill1, Skill2]
- **Environment:** Ubuntu | Python 3.x | Project: ./projects/AlphaLens

### Steps Executed
1. Step 1: [description] ✓
2. Step 2: [description] ✓
3. ...

### Time Breakdown
- Planning/Agent Analysis: Xm Ys
- Implementation: Xm Ys
- Testing: Xm Ys
- Code Review: Xm Ys
- **Total:** Xm Ys

---

## ✅ Tests & Coverage
- **Unit Tests:** XX/XX PASSED ✓
- **Integration Tests:** XX/XX PASSED ✓
- **Coverage:** XX% (target: ≥85%)
- **Regression Tests:** PASS/FAIL
- **Code Review (Medium):** N findings
- **Code Review (High - Phase End):** N findings

### Coverage Report
\`\`\`
[pytest coverage output]
\`\`\`

### Failed Tests (if any)
\`\`\`
[error details + fixes applied]
\`\`\`

---

## 📝 Code Changes
- **Files Modified:** [file1, file2, ...]
- **Files Created:** [newfile1, newfile2, ...]
- **Total Lines Changed:** +XXX -YYY
- **Git Diff Summary:** [short description]

### Code Quality Metrics
- **Cyclomatic Complexity:** [avg]
- **Code Duplication:** [%]
- **Type Coverage:** [%]

---

## 🧪 Paper Trading Integration
- **Signals Generated:** XX
- **Trades Executed:** XX (Paper)
- **Win Rate:** XX%
- **Avg Win/Loss:** X%
- **Drawdown:** XX%
- **Sharpe Ratio (Paper):** X.XX

### Trend vs Previous Phase
- Sharpe: [↑ X.XX] / [↓ X.XX]
- Win Rate: [↑ X%] / [↓ X%]

---

## 📊 Data Quality & Validation
- **DuckDB Rows (ohlcv_adjusted):** XX,XXX
- **PIT Compliance:** ✓ PASS / ✗ FAIL
- **Data Completeness:** XX%
- **Anomalies Detected:** N
- **Library Security:** [pip-audit results]

### Sample Queries
\`\`\`sql
SELECT COUNT(*) FROM ohlcv_adjusted;  -- Result: XX,XXX
SELECT COUNT(DISTINCT ticker) FROM ohlcv_adjusted;  -- Result: XXX
\`\`\`

---

## 📈 Baseline Tracking
- **Phase 1 Baseline (Sharpe):** X.XX
- **Current Phase (Sharpe):** X.XX
- **Trend:** [↑ Improving] / [↓ Degrading] / [→ Stable]
- **Deviation from Baseline:** ±X.XX%

### Metrics History
| Phase | Date | Sharpe | Coverage | Tests |
|-------|------|--------|----------|-------|
| P0 | 2025-01-15 | N/A | 82% | 12/12 |
| P1 | 2025-02-10 | X.XX | 85% | 34/34 |

---

## ⚠️ Issues & Decisions
- **Blockers:** [None / Listed with mitigation]
- **Warnings:** [Yellow flags logged]
- **Code Review Findings:** [Link to CR report or inline]
- **Design Decisions Made:** [Why X chosen over Y]

---

## 🔒 Phase Gate Check
- **Gate 1 — Coverage ≥85%:** ✓ PASS / ✗ FAIL → [Remediation tasks]
- **Gate 2 — All tests pass:** ✓ PASS / ✗ FAIL → [Remediation tasks]
- **Gate 3 — PIT validation:** ✓ PASS / ✗ FAIL → [Remediation tasks]
- **Gate 4 — Baseline trend:** ✓ PASS / ✗ PARTIAL → [Remediation tasks]

### Remediation Tasks (if gate failure)
- [ ] Task 1: [description] — Estimated effort: Xh
- [ ] Task 2: [description] — Estimated effort: Xh

---

## 🚀 Next Steps & Recommendations
1. [Recommended action 1] — Estimated effort: X hours
2. [Recommended action 2] — Estimated effort: X hours

**Ready for next phase?** [YES / NO — Needs remediation]

---

## 📎 Appendix
- **Git Commit Hash:** [if committed]
- **Code Review Report:** [link or inline]
- **Full Test Output:** [truncated; see logs/]
- **Environment Details:** Python 3.x, pip list, OS version
```

---

# PHASE 0 — Infrastructure & Data Foundation

## P0.1 — Project Skeleton

📋 **PROMPT:**
```
Read docs/CLAUDE.md, docs/12_platform_architecture.md, and docs/specs/08_specifications.md 
sections SPEC-SYS-001 through SPEC-DS-007.

Create the full project skeleton:
1. All directories from platform architecture (datastore/, ingestion/, features/, systems/, 
   backtest/, config/, tests/, requirements/)
2. config/settings.py with every constant from SPEC-SYS-001 through SPEC-SYS-005 — 
   universe size, paths, thresholds, cost model
3. config/nse_holidays.py with all NSE trading holidays for 2025 and 2026 (used by SPEC-SCHED-008)
4. config/universe.py that loads Nifty 500 tickers from a CSV file
5. requirements/phase0.txt: pandas, numpy, pyarrow, duckdb, sqlalchemy, requests, 
   beautifulsoup4, APScheduler, python-dotenv, pytest, pytest-cov
6. requirements/phase1.txt: all phase0 + lightgbm, catboost, xgboost, hmmlearn, scikit-learn, 
   mapie, optuna, imbalanced-learn, shap, ta-lib, lifelines, scikit-survival, ruptures, 
   hdbscan, river, fastapi, uvicorn, pydantic, httpx. Do not add mlfinlab; triple-barrier 
   labeling is implemented natively.
7. .env.example with placeholder keys: FYERS_APP_ID, FYERS_SECRET_ID, FYERS_ACCESS_TOKEN
8. .gitignore: .env, *.db, *.duckdb, datastore/raw/, datastore/normalised/, 
   datastore/features/, datastore/models/, __pycache__, *.pyc
9. README.md: one-paragraph purpose, setup instructions (conda env + pip install), how to run pipeline

Every file must have a module-level docstring referencing its SPEC-ID per SPEC-TRACE-002. 
No hardcoded values anywhere — all constants in config/settings.py.

EXECUTION NOTE:
- After implementation, execution report will be saved to:
  ./projects/AlphaLens/execution_logs/PHASE_0_P0.1_$(date +%Y%m%d_%H%M%S).md
- All python commands below use python3
- Execution report includes: steps executed, test results, coverage %, metrics, 
  data validation checks, and baseline tracking
```

🤖 **AGENTS:**
- **Agent 1 — Plan:** Design folder structure and file organization
- **Agent 2 — general-purpose:** Implement all files and modules

🛠️ **SKILLS:**
- Use `/code-review` (Medium effort) after implementation
- Use `/verify` to validate project structure

✅ **TESTS:**
```bash
cd ./projects/AlphaLens
python3 -c "from config.settings import *; print('Settings OK')"
python3 -c "from config.universe import load_universe; print('Universe loader OK')"
python3 -m pytest tests/ -v --cov=. --cov-report=term-missing
# Report: coverage must be ≥85%
```

📊 **EXECUTION REPORT WILL INCLUDE:**
- ✓ Steps executed with timestamps
- ✓ Test results (XX/XX PASSED)
- ✓ Coverage report (must be ≥85%)
- ✓ Code review findings (Medium effort)
- ✓ Data validation: folder structure verified
- ✓ Baseline metrics: (not applicable for P0.1)
- ✓ Next steps and recommendations

---

## P0.2 — DataStore Schema & API Shell

📋 **PROMPT:**
```
Read docs/12_platform_architecture.md (Six Stores section) and docs/specs/08_specifications.md 
sections SPEC-DS-001 through SPEC-DS-007, SPEC-PIPE-003.

Build the DataStore foundation:
1. datastore/schema/create_normalised.py — creates all DuckDB tables:
   - ohlcv_adjusted(date, ticker, open, high, low, close, volume, delivery_qty, delivery_pct, adj_factor)
   - corporate_actions(ticker, ex_date, action_type, ratio, announcement_date, record_date)
   - fundamentals(ticker, fiscal_year, quarter, quarter_end_date, announcement_date, revenue, 
     ebitda, pat, eps, operating_margin, ebitda_margin, net_margin, roe, roce, debt_to_equity, 
     interest_coverage, fcf, asset_turnover, inventory_days, receivable_days, payable_days, 
     book_value_per_share, shares_outstanding)
   - shareholding(ticker, quarter_end_date, filing_date, promoter_pct, promoter_pledge, fii_pct, 
     dii_pct, mf_pct, retail_pct)
   - macro_indicators(date, indicator, value)
   - stock_master(ticker, company_name, sector, industry, nse_series, listing_date, market_cap_cr, 
     adtv_cr, current_tier, is_fno_eligible, is_nifty500)
2. datastore/schema/create_signals.py — creates SQLite signals.db:
   - pipeline_runs(run_id, date, started_at, completed_at, status, stocks_processed, error_message)
   - ml_signals table (all columns from architecture doc)
   - ml_multibagger table
   - ml_forensic table
3. datastore/client.py — DataStoreClient class per SPEC-SOLID-005:
   - get_ohlcv(ticker, from_date, to_date), get_fundamentals_pit(ticker, as_of), 
     get_signals(ticker, date)
   - All methods make httpx calls to localhost:8000 — no direct DB access
4. datastore/api/main.py — FastAPI shell:
   - GET /health — returns pipeline status, last run, stocks processed
   - GET /api/v1/ohlcv/{ticker} — stub, returns empty list
   - GET /api/v1/fundamentals/{ticker} — stub, returns null
   - GET /api/v1/signals/ml/{ticker}/{date} — stub, returns null
5. Unit test tests/unit/test_schema.py:
   - Creates schemas in an in-memory DuckDB
   - Verifies all tables exist with correct columns
   - Tests that ohlcv query with as_of date only returns rows where date <= as_of (PIT rule)

All PIT rules from SPEC-PIPE-003 must be enforced by schema constraints or documented as 
API-layer enforcement. Every file must have a module-level docstring referencing its SPEC-ID 
per SPEC-TRACE-002.

EXECUTION NOTE:
- After implementation, execution report will be saved to:
  ./projects/AlphaLens/execution_logs/PHASE_0_P0.2_$(date +%Y%m%d_%H%M%S).md
- All python commands below use python3
- Execution report includes: schema creation log, table/column verification, PIT rule 
  validation, API shell smoke test (GET /health)
```

🤖 **AGENTS:**
- **Agent 1 — Plan:** Design DuckDB/SQLite schema layout and PIT enforcement strategy
- **Agent 2 — general-purpose:** Implement schema scripts, DataStoreClient, and FastAPI shell

🛠️ **SKILLS:**
- Use `/code-review` (Medium effort)

✅ **TESTS:**
```bash
cd ./projects/AlphaLens
python3 -m pytest tests/unit/test_schema.py -v --cov=datastore --cov-report=term-missing
python3 -c "from datastore.schema.create_normalised import create_schema; print('Normalised schema OK')"
python3 -c "from datastore.schema.create_signals import create_schema; print('Signals schema OK')"
uvicorn datastore.api.main:app --port 8000 &
sleep 2 && curl -s http://localhost:8000/health
```

📊 **EXECUTION REPORT WILL INCLUDE:**
- ✓ Steps: schema design, table creation, client implementation, API shell
- ✓ Test results (XX/XX PASSED)
- ✓ Coverage metrics
- ✓ Table/column verification (all 11 tables confirmed)
- ✓ PIT rule validation (as_of filtering confirmed on ohlcv_adjusted)
- ✓ API smoke test (GET /health response)
- ✓ Code review findings
- ✓ Baseline: ready for P0.3

---

## P0.3 — Scheduler & Checkpoint Engine

📋 **PROMPT:**
```
Read docs/13_scheduler_resilience.md and docs/specs/08_specifications.md 
sections SPEC-SCHED-001 through SPEC-SCHED-011.

Build the scheduler and checkpoint system:
1. ingestion/scheduler/pipeline_scheduler.py — APScheduler with SQLAlchemyJobStore:
   - Three modes: linear (default), timestamp, manual (SPEC-SCHED-001)
   - On startup: query pipeline_runs table, find all trading dates since last successful run
   - Backfill mode: process missing dates chronologically, oldest first (SPEC-SCHED-004)
   - No ML inference during backfill (SPEC-SCHED-006)
   - NSE holiday awareness from config/nse_holidays.py (SPEC-SCHED-008)
   - misfire_grace_time=86400


2. ingestion/scheduler/checkpoint.py — CheckpointManager class:
   - save_checkpoint(date, step_name, status) — writes to pipeline_runs SQLite
   - load_checkpoint(date) — returns last completed step for a date
   - Steps: ['download_bhavcopy', 'download_fno', 'adjust_prices', 'compute_features', 
     'run_models', 'write_signals']
   - On failure: record error_message, status='failed'; next startup resumes from failed 
     step (SPEC-SCHED-002)
   - Atomic writes only (SPEC-SCHED-010)

3. ingestion/scheduler/gap_detector.py:
   - detect_gaps() — returns list of missed trading dates between last run and today
   - Uses NSE holiday calendar to skip non-trading days

4. tests/unit/test_scheduler.py:
   - Test gap detection finds 3 missed dates when last run was 5 days ago with 2 holidays
   - Test checkpoint save and resume: simulate failure at 'compute_features', verify 
     next run starts from that step
   - Test backfill processes dates oldest-first

5. tests/integration/test_scheduler_resume.py:
   - Full integration: run pipeline, simulate crash at step 3, restart, verify it resumes 
     not restarts

Docstrings in all files must reference SPEC-SCHED-001 through SPEC-SCHED-011 as applicable.

EXECUTION NOTE:
- Report includes: scheduler startup log, gap detection results, checkpoint save/resume 
  validation, NSE holiday verification
- Report location: ./projects/AlphaLens/execution_logs/PHASE_0_P0.3_$(date +%Y%m%d_%H%M%S).md
```

🤖 **AGENTS:**
- **Agent 1 — Plan:** Design scheduler state machine and checkpoint protocol
- **Agent 2 — general-purpose:** Implement APScheduler integration, checkpoint manager, 
  gap detector

🛠️ **SKILLS:**
- Use `/code-review` (Medium effort)

✅ **TESTS:**
```bash
cd ./projects/AlphaLens
python3 -m pytest tests/unit/test_scheduler.py -v --cov=ingestion.scheduler \
  --cov-report=term-missing
python3 -m pytest tests/integration/test_scheduler_resume.py -v
```

📊 **EXECUTION REPORT WILL INCLUDE:**
- ✓ Steps: scheduler design, checkpoint implementation, gap detection
- ✓ Test results (XX/XX PASSED)
- ✓ Coverage metrics
- ✓ Checkpoint protocol validation
- ✓ Gap detection verification (sample: 5-day gap with 2 holidays detected correctly)
- ✓ Code review findings
- ✓ Baseline: ready for Phase 1

---

## P0.4 — NSE Bhavcopy + FNO Scrapers

📋 **PROMPT:**
```
Read docs/specs/08_specifications.md SPEC-PIPE-001, SPEC-PIPE-005, SPEC-PIPE-006 
and docs/specs/API_SPEC.md.

Build the data ingestion scrapers:
1. ingestion/scrapers/bhavcopy.py:
   - download_bhavcopy(date: str) → pd.DataFrame
   - Downloads NSE equity bhavcopy from archives.nseindia.com
   - Columns: ticker, open, high, low, close, volume, traded_qty, delivery_qty, series
   - Filter to EQ series only; skip BE, BL, SM, ST series
   - Raises ConnectionError after 3 retries; raises ValueError if < 450 stocks found
   - Validates: no ticker appears twice, delivery_pct in [0, 100], prices > 0

2. ingestion/scrapers/fno.py:
   - download_fno_bhavcopy(date: str) → pd.DataFrame
   - Downloads NSE F&O bhavcopy
   - Stores OI, volume, settle_price by ticker/expiry/strike/option_type

3. ingestion/scrapers/macro.py:
   - download_vix(date: str) → float — from NSE VIX page
   - download_fiidii(date: str) → dict — FII/DII buy/sell from NSE
   - download_fx(date: str) → dict — USD/INR from RBI or Yahoo Finance
   - All have retry=3, fallback to previous day value if unavailable (SPEC-PIPE-006)

4. ingestion/adjust/price_adjuster.py:
   - adjust_for_corporate_actions(conn, ticker: str) → None
   - Idempotent (SPEC-PIPE-002): checks adj_factor before applying
   - SPLIT: multiply all pre-ex prices by 1/ratio; BONUS: multiply by 1/(1+ratio)
   - Post-check: price continuity at ex-date < 1% gap after adjustment

5. tests/unit/test_bhavcopy.py:
   - Test download returns DataFrame with required columns
   - Test raises ValueError when < 450 stocks returned (mock the HTTP response)
   - Test delivery_pct validation catches out-of-range values

6. tests/unit/test_price_adjuster.py:
   - Test split adjustment is idempotent (calling twice gives same result)
   - Test bonus adjustment: price × 1/(1+ratio)
   - Test continuity check passes for valid adjustment

All functions: docstrings with SPEC-ID references per SPEC-TRACE-002.

EXECUTION NOTE:
- Report includes: scraper validation (mocked), price adjustment verification, 
  idempotency tests, retry logic validation
- Report location: ./projects/AlphaLens/execution_logs/PHASE_0_P0.4_$(date +%Y%m%d_%H%M%S).md
```

🤖 **AGENTS:**
- **Agent 1 — Explore:** Research NSE data sources and API endpoints
- **Agent 2 — general-purpose:** Implement scrapers with error handling and retry logic

🛠️ **SKILLS:**
- Use `/code-review` (Medium effort)

✅ **TESTS:**
```bash
cd ./projects/AlphaLens
python3 -m pytest tests/unit/test_bhavcopy.py tests/unit/test_price_adjuster.py -v \
  --cov=ingestion --cov-report=term-missing
```

📊 **EXECUTION REPORT WILL INCLUDE:**
- ✓ Steps: scraper implementation, mock validation, error handling
- ✓ Test results (XX/XX PASSED)
- ✓ Coverage metrics
- ✓ Retry logic validation (3 retries confirmed)
- ✓ Idempotency verification (split adjustment tested)
- ✓ Price continuity check (< 1% gap validated)
- ✓ Code review findings
- ✓ Recommendation: ready for P0.5 (FYERS backfill)

---

## P0.5 — FYERS Historical Backfill

⚠️ **MANUAL FIRST:** Open FYERS account at fyers.in. Get App ID + Secret from myapi.fyers.in. 
Add to `.env`.

📋 **PROMPT:**
```
Read docs/03_data_pipeline.md sections on historical backfill and docs/specs/08_specifications.md 
SPEC-PIPE-001, SPEC-PIPE-002.

Build the FYERS historical backfill pipeline:
1. ingestion/scrapers/fyers_backfill.py:
   - FYERSBackfill class using fyers-apiv3 (pip install fyers-apiv3)
   - get_access_token() — OAuth2 flow using FYERS_APP_ID and FYERS_SECRET_ID from .env
   - download_history(ticker, from_date, to_date, timeframe='D') → pd.DataFrame
   - Rate limiting: max 1000 API calls/day; built-in throttle with 0.5s sleep
   - batch_download(tickers: List[str], from_date, to_date) — downloads all with progress 
     bar (tqdm)
   - Saves each batch to datastore/raw/fyers/TICKER_YYYY-MM-DD_YYYY-MM-DD.parquet

2. ingestion/backfill_runner.py:
   - Loads Nifty 500 ticker list from config/universe.py
   - Calls batch_download for 5 years of daily data
   - After each ticker: write to DuckDB ohlcv_adjusted table via DataStore API
   - Tracks progress: skip tickers already in DuckDB with sufficient history
   - Estimated runtime displayed: "Estimated 3.5 hours based on rate limit"

3. ingestion/scrapers/nse_delivery_loader.py:
   - Parses NSE historical bhavcopy archives for delivery data (5 years)
   - Merges delivery_qty and delivery_pct into existing ohlcv_adjusted rows

4. tests/unit/test_fyers_backfill.py:
   - Mock FYERS API response; test batch_download processes all tickers
   - Test rate limiting: verify 0.5s sleep between calls
   - Test resumes from last completed ticker (checkpoint)

Include a progress checkpoint: save last completed ticker to a resume file so backfill 
can restart after interruption.

EXECUTION NOTE:
- Manual step: configure .env with FYERS credentials
- Backfill is memory-intensive (3-4 hours, run overnight)
- Report includes: checkpoint resume validation, rate limiting verification, 
  data loading statistics
- Report location: ./projects/AlphaLens/execution_logs/PHASE_0_P0.5_$(date +%Y%m%d_%H%M%S).md
```

🤖 **AGENTS:**
- **Agent 1 — general-purpose:** Implement FYERS client, backfill runner, checkpoint

🛠️ **SKILLS:**
- Use `/code-review` (Medium effort)

✅ **TESTS:**
```bash
cd ./projects/AlphaLens
python3 -m pytest tests/unit/test_fyers_backfill.py -v --cov=ingestion.scrapers \
  --cov-report=term-missing
```

⚠️ **MANUAL — Get a FYERS access token first** (non-interactive two-step
CLI; FYERSBackfill's built-in input()-based flow blocks forever in any
terminal/IDE pane without a connected stdin — confirmed in practice, see
BuildLog.md "Post-handoff bug #3"):
```bash
cd ./projects/AlphaLens
python3 -m ingestion.scrapers.fyers_backfill login
# -> open the printed URL in a browser, log in, copy the FULL redirected
#    URL from the address bar (it'll show connection-refused at
#    https://127.0.0.1/?auth_code=...&state=... -- that's expected)
python3 -m ingestion.scrapers.fyers_backfill exchange "<paste the redirected URL here>"
# -> caches a real access token to disk for the rest of the day
```

⚠️ **MANUAL — Run the full backfill** (overnight, 3–4 hours):
```bash
cd ./projects/AlphaLens
nohup python3 -m ingestion.backfill_runner --from 2020-01-01 --to 2025-12-31 \
  > logs/backfill_$(date +%Y%m%d_%H%M%S).log 2>&1 &
# -- NOT `python3 ingestion/backfill_runner.py ...` (direct script path):
# this module uses absolute imports that only resolve when run with -m.
# Check progress in morning:
tail -50 logs/backfill_*.log
# Verify:
python3 -c "import duckdb; conn = duckdb.connect('datastore/normalised/alphalens.duckdb'); \
  print(conn.execute('SELECT COUNT(*) FROM ohlcv_adjusted').fetchone())"
# Must return >= 600,000 rows (assumes the official full Nifty 500 list has
# replaced the starter config/nifty500_universe.csv sample)
```

📊 **EXECUTION REPORT WILL INCLUDE:**
- ✓ Steps: FYERS client setup, backfill runner, checkpoint mechanism
- ✓ Test results (XX/XX PASSED)
- ✓ Coverage metrics
- ✓ Rate limiting verification (0.5s throttle confirmed)
- ✓ Checkpoint resume validation
- ✓ Backfill completion: XX,XXX rows loaded, YY tickers processed
- ✓ Code review findings
- ✓ Recommendation: ready for P0.6

---

## P0.6 — Laptop-Only Daily Pipeline Scheduler (Oracle Cloud deferred)

✅ **ALREADY COMPLETE** — see BuildLog.md "P0.6 — Laptop-Only Pivot + Daily Pipeline
Scheduler Job" for the full record. Oracle Cloud Free Tier provisioning was attempted
and abandoned: `ap-mumbai-1` had zero free `VM.Standard.A1.Flex` capacity at any size,
and the account's Free Trial status blocks subscribing to an alternate region without
an irreversible upgrade to Pay-As-You-Go. SPEC-SCHED-009 (formerly "Oracle Cloud
Independence") already specified an Oracle-first/NSE-archive-fallback design — the
fallback path is now simply the only path; no ingestion code ever had a hard Oracle
dependency. See docs/06_deployment.md "Oracle Cloud (deferred)" if Oracle capacity
becomes worth revisiting later (Phase 2+, when always-on intraday capture actually
matters).

What was actually built instead, for any future session re-reading this file:
- `ingestion/scheduler/daily_pipeline.py`: concrete `step_runner` wiring real
  ingestion functions (bhavcopy, macro, price adjustment) into the
  `ingestion/scheduler/pipeline_scheduler.py` engine built in Phase 0.3 — registered
  as a persistent APScheduler job (`schedule_daily_pipeline()`, 18:00 IST Mon-Fri),
  **not** OS-level crontab and **not** an SSH-deployed Oracle VM. Run via
  `python -m ingestion.scheduler.daily_pipeline` (foreground or `nohup ... &`) and
  leave it running; it self-catches-up on startup, no manual cron verification needed.
- `download_fno` and the live option-chain scraper remain deferred to Phase 2 — F&O
  features aren't needed for Phase 1, and NSE's F&O bhavcopy archive endpoint is
  currently broken (serves a PDF, not a CSV) regardless of Oracle/laptop hosting.
- A real pre-existing bug was found and fixed in this phase: nothing had ever written
  to the `pipeline_runs` table, so the startup gap-detection/catch-up mechanism from
  Phase 0.3 would have silently never triggered. Fixed in `pipeline_scheduler.py`.
- 13 new unit tests added (`tests/unit/test_daily_pipeline.py`); full suite 70/70
  passing at the time this phase was completed.

If a future session needs to revisit Oracle Cloud deployment, do **not** reuse the
prompt that originally lived here — it assumed `ingestion/oracle_scrapers/` as a
parallel scraper tree and an `rsync`/SSH deploy flow, which would now duplicate
`ingestion/scheduler/daily_pipeline.py`'s step dispatch. Read BuildLog.md's P0.6
entry first and design Oracle as an additional *execution environment* for the
existing step functions, not a separate codepath.

---

## P0.7 — Data Quality + Observability + PSI Baseline

📋 **PROMPT:**
```
Read docs/specs/08_specifications.md sections SPEC-PIPE-005, SPEC-OBS-001 through SPEC-OBS-005.

Build data quality and observability:
1. ingestion/quality/validator.py:
   - validate_bhavcopy(df: pd.DataFrame, expected_tickers: List[str]) → dict
   - Returns: {'ok': bool, 'missing': List[str], 'anomalies': List[str], 'stock_count': int}
   - Anomaly: any stock with > 30% single-day price change (without known corp action)
   - Completeness gate: ok=False if stock_count < 450 (SPEC-SYS-003)

2. ingestion/quality/drift_monitor.py:
   - PSIMonitor class: compute_psi(feature_name, current_values, baseline_values) → float
   - Alerts: PSI > 0.10 = warning (reduce position sizing 50%), PSI > 0.25 = halt + retrain
   - compute_baseline(feature_matrix: pd.DataFrame) → saves to 
     datastore/features/baseline/stats_baseline.pkl
   - Daily: run top-50 features through PSI check after feature matrix is built

3. config/observability.py — master observability switch per SPEC-OBS-001:
   - OBSERVABILITY_LEVEL: 'production' | 'development' | 'debug'
   - In production mode: no verbose logging, no intermediate file writes (SPEC-OBS-005)

4. ingestion/quality/structured_logger.py — per SPEC-OBS-003:
   - log_pipeline_step(step, status, stocks, duration_s, error=None)
   - Output format: JSON lines to logs/pipeline_YYYY-MM-DD.jsonl
   - Never logs raw financial data values (security — SPEC-SEC-001)

5. tests/unit/test_validator.py:
   - Test completeness gate blocks at 449 stocks
   - Test anomaly detection flags 35% price change
   - Test PSI calculation: known distribution shift returns expected PSI value

6. Compute PSI baseline:
   - baseline_runner.py: load 2 years of existing data, compute stats_baseline.pkl
   - Must run after backfill is complete

All functions docstrings reference SPEC-OBS-001 through SPEC-OBS-005, SPEC-PIPE-005.

EXECUTION NOTE:
- Report includes: validation logic verification, PSI calculation validation, 
  baseline computation results
- Report location: ./projects/AlphaLens/execution_logs/PHASE_0_P0.7_$(date +%Y%m%d_%H%M%S).md
```

🤖 **AGENTS:**
- **Agent 1 — general-purpose:** Implement validator, PSI monitor, structured logging

🛠️ **SKILLS:**
- Use `/code-review` (Medium effort)

✅ **TESTS:**
```bash
cd ./projects/AlphaLens
python3 -m pytest tests/unit/test_validator.py -v --cov=ingestion.quality \
  --cov-report=term-missing
python3 -m ingestion.quality.baseline_runner  # run after backfill
```

📊 **EXECUTION REPORT WILL INCLUDE:**
- ✓ Steps: validator implementation, PSI monitor, structured logging, baseline computation
- ✓ Test results (XX/XX PASSED)
- ✓ Coverage metrics
- ✓ Completeness gate validation (449 stock threshold confirmed)
- ✓ Anomaly detection (35% price change flagged correctly)
- ✓ PSI calculation verification
- ✓ Baseline file created: datastore/features/baseline/stats_baseline.pkl
- ✓ Code review findings
- ✓ PHASE 0 READINESS CHECK

---

## 🔒 PHASE 0 GATE CHECK

📋 **PROMPT:**
```
Execute the Phase 0 gate check. This comprehensive check validates all Phase 0 deliverables.

Report PASS or FAIL for each item. Generate comprehensive execution report to:
./projects/AlphaLens/execution_logs/PHASE_0_GATE_CHECK_$(date +%Y%m%d_%H%M%S).md

GATE CHECKS (ALL MUST PASS to proceed to Phase 1):

1. **Unit Test Coverage**
   python3 -m pytest tests/ --cov=. --cov-report=term
   # Minimum: ≥85% coverage across codebase

2. **DataStore Validation**
   python3 -c "import duckdb; conn = duckdb.connect('datastore/normalised/alphalens.duckdb'); \
     print(conn.execute('SELECT COUNT(*) FROM ohlcv_adjusted').fetchone())"
   # Must return >= 600,000 rows

3. **Pipeline Runs Table**
   python3 -c "import sqlite3; conn = sqlite3.connect('datastore/signals/signals.db'); \
     cursor = conn.cursor(); \
     cursor.execute('SELECT * FROM pipeline_runs ORDER BY date DESC LIMIT 5'); \
     for row in cursor.fetchall(): print(row)"
   # Verify at least 1 successful run exists

4. **PSI Baseline File**
   test -f ./projects/AlphaLens/datastore/features/baseline/stats_baseline.pkl
   # File must exist and be non-empty

5. **Environment Validation**
   test -f ./projects/AlphaLens/.env && grep "FYERS_APP_ID" .env
   # .env file exists and FYERS credentials are set

6. **Credential Security**
   python3 -c "import os; import glob; \
     files = glob.glob('./projects/AlphaLens/**/*.py', recursive=True); \
     for f in files: \
       with open(f) as fp: \
         content = fp.read(); \
         if 'FYERS_APP_ID' in content or 'FYERS_SECRET_ID' in content: \
           print(f'FAIL: Credentials in {f}')"
   # Must return nothing (no credentials in .py files)

7. **Module Docstrings**
   python3 -c "import importlib; import pkgutil; import config; \
     for importer, modname, ispkg in pkgutil.walk_packages(config.__path__, \
       config.__name__ + '.'): \
       mod = importlib.import_module(modname); \
       if mod.__doc__ is None: print(f'FAIL: No docstring in {modname}')"
   # All modules must have docstrings with SPEC-IDs

8. **Checkpoint Resume Validation**
   python3 -c "from ingestion.scheduler.checkpoint import CheckpointManager; \
     cm = CheckpointManager(); \
     cm.save_checkpoint('2025-01-15', 'download_bhavcopy', 'completed'); \
     checkpoint = cm.load_checkpoint('2025-01-15'); \
     assert checkpoint['last_completed_step'] == 'download_bhavcopy', 'Resume failed'"
   # Checkpoint save/load must work correctly

9. **Pip Audit — Library Security**
   python3 -m pip install pip-audit
   python3 -m pip_audit --requirement ./projects/AlphaLens/requirements/phase0.txt
   # Report any CRITICAL or HIGH CVEs (must be resolved before Phase 1)

10. **Data Quality Checks**
    python3 -c "import duckdb; conn = duckdb.connect('datastore/normalised/alphalens.duckdb'); \
      result = conn.execute('SELECT COUNT(DISTINCT ticker) FROM ohlcv_adjusted').fetchone()[0]; \
      print(f'Unique tickers: {result}'); \
      assert result >= 450, 'Less than 450 tickers found'"
    # At least 450 unique tickers in OHLCV data

Report format: 
- Item 1: ✓ PASS / ✗ FAIL [reason if fail]
- Item 2: ✓ PASS / ✗ FAIL [reason if fail]
- ...

BLOCKING ITEMS: List all that must be fixed before Phase 1.

Auto-generate execution report with:
- Executive summary (ready/not ready)
- Each gate check result
- Blocking issues (if any) with remediation steps
- Approval for proceeding to Phase 1
```

🤖 **AGENTS:**
- **Agent 1 — Explore:** Verify all Phase 0 artifacts exist and are accessible
- **Agent 2 — general-purpose:** Execute all gate checks and compile report

🛠️ **SKILLS:**
- Use `/code-review` (High effort) at phase completion

📊 **EXECUTION REPORT WILL INCLUDE:**
- ✓ Gate check results (1–10): PASS/FAIL for each
- ✓ Coverage report (must be ≥85%)
- ✓ Blocking items (if any) with remediation tasks
- ✓ Approval status: YES / NO (with specific reasons if NO)
- ✓ Baseline metrics recorded: Phase 0 checkpoint
- ✓ Recommendations for Phase 1 start

---

# PHASE 1 — Core Signal Engine (Weeks 5–14)

## P1.1 — 76 Technical Features + Calendar + Macro

[Similar comprehensive template structure continues for all Phase 1 prompts...]

📋 **PROMPT:**
```
[Full prompt text with all requirements per original CLAUDE_CODE_PROMPTS.md]

EXECUTION NOTE:
- Report will track: 76 feature implementations, vectorization verification, 
  calendar encoding, macro features
- Report location: ./projects/AlphaLens/execution_logs/PHASE_1_P1.1_$(date +%Y%m%d_%H%M%S).md
```

🤖 **AGENTS:**
- **Agent 1 — Plan:** Design feature pipeline and vectorization strategy
- **Agent 2 — general-purpose:** Implement all 76 technical features, calendar, macro

🛠️ **SKILLS:**
- Use `/code-review` (Medium effort)

✅ **TESTS:**
```bash
python3 -m pytest tests/unit/test_features_technical.py -v --cov=features \
  --cov-report=term-missing
python3 -c "from features.matrix_builder import build_feature_matrix; \
  df = build_feature_matrix('2025-01-15', ['RELIANCE', 'TCS', 'INFY', 'HDFCBANK', 'ICICIBANK']); \
  print(df.shape, 'Expected: (5, 98)')"
```

---

# EXECUTION GUIDELINES

## Running a Phase Prompt

1. **Copy the prompt verbatim** into Claude Code
2. **Specify agents/skills at top** of your message
3. **Claude Code will:**
   - Execute agents in parallel (if independent)
   - Run tests automatically
   - Trigger code review (Medium effort)
   - Generate execution report
   - Compute paper trading metrics
   - Track baseline changes
   - Flag gate check failures with remediation tasks

4. **Manual steps** (⚠️ marked) require your action; these appear in the execution report

5. **Review the execution report** before committing to git

6. **At phase completion:**
   - Trigger `/code-review --effort high` for comprehensive review
   - Address any findings
   - Update baselines
   - Manually commit with structured message (include SPEC-ID)

---

## Execution Report Storage & Retrieval

All reports are stored in: `./projects/AlphaLens/execution_logs/`

Format: `PHASE_X_[PROMPT_NAME]_YYYYMMDD_HHMMSS.md`

Example:
```
PHASE_0_P0.1_20250115_143022.md
PHASE_0_P0.2_20250115_182145.md
PHASE_0_GATE_CHECK_20250116_090000.md
PHASE_1_P1.1_20250201_101530.md
```

**To view recent executions:**
```bash
cd ./projects/AlphaLens
ls -ltr execution_logs/ | tail -10
```

**To analyze a specific report:**
```bash
# Get coverage trend
grep "Coverage:" execution_logs/PHASE_*.md | sort

# Get Sharpe trend (Phase 1+)
grep "Sharpe" execution_logs/PHASE_*.md | sort

# Get gate check results
grep -A 20 "Gate Check" execution_logs/PHASE_*_GATE_CHECK_*.md
```

---

## Baseline Tracking Across Phases

Baseline metrics are stored in: `./projects/AlphaLens/baselines/baseline_metrics.json`

Sample structure:
```json
{
  "phases": {
    "P0": {
      "date": "2025-01-16",
      "coverage": 0.85,
      "test_count": 28,
      "test_passed": 28
    },
    "P1": {
      "date": "2025-02-10",
      "coverage": 0.86,
      "test_count": 64,
      "test_passed": 64,
      "sharpe": 0.95,
      "win_rate": 0.58
    }
  }
}
```

Each execution report updates this file and includes trend analysis:
```
📈 Trend vs P0:
- Coverage: ↑ +1.2% (improved)
- Sharpe: ↑ +0.15 (improved)
- Win Rate: → stable at 58%
```

---

## Paper Trading Integration

All signals generated during Phase 1+ are logged to:
`./projects/AlphaLens/paper_trading/executions/YYYY-MM-DD.csv`

Format:
```csv
date,ticker,signal_type,entry_price,quantity,entry_time,exit_price,exit_time,pnl,pnl_pct
2025-01-15,RELIANCE,BUY,2850.50,100,09:15:00,2880.75,15:30:00,3025.00,1.06
```

Execution reports auto-compute:
- Win rate: (winning trades / total trades)
- Avg win / Avg loss
- Profit factor: (sum of wins / sum of losses)
- Drawdown: max peak-to-trough
- Sharpe ratio: (mean return / std return)

Sample paper trading metrics in report:
```
🧪 Paper Trading Integration
- Signals Generated: 42
- Trades Executed: 38 (Paper)
- Win Rate: 58.2% (22 wins, 16 losses)
- Avg Win: +2.15% | Avg Loss: -1.85%
- Profit Factor: 1.28
- Max Drawdown: -8.5%
- Sharpe Ratio (Paper): 0.95

Trend vs Previous Phase:
- Win Rate: ↑ +3.2%
- Sharpe: ↑ +0.12
```

---

## Code Review Automation

**Medium Effort (after every prompt):**
- Invoked automatically after implementation
- Checks for: correctness bugs, reuse opportunities, efficiency improvements
- Report: linked in execution report

**High Effort (at phase completion):**
- Manual trigger at end of phase gate check
- Comprehensive review across entire phase
- Checks for: architectural consistency, SPEC compliance, security issues
- Findings: documented in `code_reviews/PHASE_X_HIGH_REVIEW_YYYYMMDD.md`

Example code review summary in report:
```
🔍 Code Review (Medium Effort)
- Files Reviewed: 12
- Findings: 3
  1. Unused import in datastore/client.py:15 → FIXED
  2. Type hint missing in validator.py:42 → FIXED
  3. Consider extracting helper function (efficiency) → NOTED for future

Status: ✓ APPROVED (with fixes applied)
```

---

## Test Coverage Requirements

**Minimum acceptable:** ≥85% across codebase

**Phase completion gates:**
- Coverage < 85%: Phase gate fails, remediation tasks auto-generated
- Example remediation task:
  ```
  - [ ] Add tests for ingestion/quality/validator.py (currently 72%) — Est. effort: 2h
  - [ ] Add integration tests for DataStore API (currently 68%) — Est. effort: 3h
  ```

**Running coverage reports:**
```bash
cd ./projects/AlphaLens
python3 -m pytest tests/ --cov=. --cov-report=html
open htmlcov/index.html  # View coverage report in browser
```

---

## Dependency Management

After each phase, freeze requirements:

```bash
cd ./projects/AlphaLens

# Freeze exact versions
python3 -m pip freeze | grep -E "pandas|numpy|duckdb|..." > requirements/phase1_frozen.txt

# Run pip-audit
python3 -m pip-audit --requirement requirements/phase1_frozen.txt > \
  code_reviews/phase1_pip_audit_YYYYMMDD.txt

# Commit frozen requirements (manual)
git add requirements/phase1_frozen.txt
git commit -m "chore: freeze Phase 1 dependencies [date]"
```

Audit results appear in execution report:
```
📦 Dependency Audit (pip-audit)
- Total packages scanned: 42
- Vulnerabilities found: 0
- Status: ✓ SECURE

(If vulnerabilities exist:)
- CVE-2025-XXXXX (HIGH) in package_name==X.Y.Z
  Action: Upgrade to package_name>=X.Y.Z
```

---

## Ubuntu-Specific Commands

All commands in this framework use `python3` for Ubuntu:

```bash
# ✓ Correct for Ubuntu
python3 -m pytest tests/
python3 -c "from config.settings import *"
python3 -m ingestion.backfill_runner

# ✗ Incorrect (will fail on Ubuntu)
python -m pytest tests/
python3 ingestion/backfill_runner.py  # ModuleNotFoundError: direct script
                                       # path excludes the project root from
                                       # sys.path; absolute imports like
                                       # `from config.settings import ...`
                                       # only resolve via `-m pkg.module`
```

---

## Remediation Task List (Gate Failures)

When a gate check fails, the execution report auto-generates remediation tasks:

```markdown
## 🚨 PHASE 1 GATE CHECK — PARTIAL PASS

### Failed Gate: Coverage < 85%

**Issue:** Current coverage 82%, required ≥85%

**Remediation Tasks:**
1. [ ] Add tests for ingestion/quality/validator.py (72% coverage)
   - Estimate: 2 hours
   - Priority: HIGH
2. [ ] Add integration tests for DataStore API (68% coverage)
   - Estimate: 3 hours
   - Priority: HIGH
3. [ ] Add edge case tests for feature matrix builder
   - Estimate: 1.5 hours
   - Priority: MEDIUM

**Next Steps:**
1. Fix tests (estimated total: 6.5 hours)
2. Re-run coverage report
3. Re-execute Phase 1 gate check
4. If all gates pass, proceed to Phase 2
```

---

## Questions During Execution?

If a prompt fails:

1. **Copy the error message** exactly
2. **Paste into Claude Code with:**
   ```
   The error is: [paste exact error]
   
   Context: Running P1.2 (HMM Regime Detector)
   Previous step: P1.1 completed successfully
   
   Fix this and retry the prompt.
   ```

3. **Claude will:**
   - Diagnose the issue
   - Provide a fix
   - Re-run the failing step
   - Update the execution report with fix details

---

## Submitting This Framework

Once you're ready to use this updated framework:

1. **Backup current CLAUDE_CODE_PROMPTS.md:**
   ```bash
   cp ./projects/AlphaLens/CLAUDE_CODE_PROMPTS.md \
      ./projects/AlphaLens/CLAUDE_CODE_PROMPTS_BACKUP_$(date +%Y%m%d).md
   ```

2. **Replace with updated version:**
   ```bash
   mv ./projects/AlphaLens/CLAUDE_CODE_PROMPTS_UPDATED.md \
      ./projects/AlphaLens/CLAUDE_CODE_PROMPTS.md
   ```

3. **Create execution logs directory:**
   ```bash
   mkdir -p ./projects/AlphaLens/execution_logs
   ```

4. **Start Phase 0 with P0.1:**
   - Copy P0.1 prompt from this file into Claude Code
   - Specify agents and skills
   - Run and review execution report

---

*End of AlphaLens Enhanced Claude Code Execution Framework*

**Version:** 2.0 (Updated)  
**Last Updated:** 2025-06-19  
**Features:** Automated Reporting · Integrated Testing · Code Review · Baseline Tracking · Paper Trading Integration · Remediation Tasks · Ubuntu Compatibility
