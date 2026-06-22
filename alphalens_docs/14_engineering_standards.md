# AlphaLens — Engineering Standards & Traceability
## RTM · Architecture Review · SOLID · Library Governance · Phase-Spec Alignment

---

## PART 1: REQUIREMENTS TRACEABILITY MATRIX (RTM)

Every spec maps to: a phase, a module, at least one test, and a verification method.

### Legend
- **Test Type:** U=Unit, I=Integration, R=Regression, H=HITL, S=Structural, M=Manual

### SPEC-SYS: System-Level

| Spec ID | Phase | Test ID | Type | Verification |
|---------|:-----:|---------|:----:|-------------|
| SPEC-SYS-001 | 0 | T-SYS-001 | U | Universe size matches config profile |
| SPEC-SYS-002 | 0 | T-SYS-002 | I | Full pipeline < 90 min |
| SPEC-SYS-003 | 0 | T-SYS-003 | U | 440 stocks → halt; 460 → proceed |
| SPEC-SYS-004 | 0 | T-SYS-004 | M | Weekly: Oracle instance alive |
| SPEC-SYS-005 | All | T-SYS-005 | M | Quarterly: du -sh datastore/ < 500GB |
| SPEC-SYS-011 | 2 | T-SYS-011 | U | Profile switch → stock count changes |

### SPEC-PIPE: Data Pipeline

| Spec ID | Phase | Test ID | Type | Verification |
|---------|:-----:|---------|:----:|-------------|
| SPEC-PIPE-001 | 0 | T-PIPE-001 | I | Download + validate ≥ 450 rows |
| SPEC-PIPE-002 | 0 | T-PIPE-002a,b,c,d | U | Bonus/split/dividend/multi-action tests |
| SPEC-PIPE-003 | 0 | T-PIPE-003a,b,c,d | U | Fund PIT, shareholding PIT, MF PIT, no quarter_end_date |
| SPEC-PIPE-004 | 1 | T-PIPE-004 | I | 500 stocks features < 15 min |
| SPEC-PIPE-005 | 0 | T-PIPE-005 | U | Inject nulls/outliers → flagged |
| SPEC-PIPE-006 | 0 | T-PIPE-006 | I | VIX + USD/INR in valid range |

### SPEC-FEAT: Feature Engineering

| Spec ID | Phase | Test ID | Type | Verification |
|---------|:-----:|---------|:----:|-------------|
| SPEC-FEAT-001 | 1 | T-FEAT-001 | U | 50-day stock → NaN for 252d features |
| SPEC-FEAT-002 | 2 | T-FEAT-002 | U | Bank ROE 15%, IT ROE 30% → both z≈0 in-sector |
| SPEC-FEAT-003 | 1 | T-FEAT-003 | U | Month 1 and 12 → sin/cos close (circular) |
| SPEC-FEAT-004 | 2 | T-FEAT-004 | U | Non-F&O ticker → 16 F&O features NaN |
| SPEC-FEAT-005 | 1 | T-FEAT-005 | U | Every stock has sector; no nulls |

### SPEC-MODEL: ML Models

| Spec ID | Phase | Test ID | Type | Verification |
|---------|:-----:|---------|:----:|-------------|
| SPEC-MODEL-001 | 1 | T-MODEL-001 | U | < 252 rows → InsufficientDataError |
| SPEC-MODEL-002 | 1 | T-MODEL-002a,b | U | Labels {-1,0,1} only; none beyond horizon |
| SPEC-MODEL-003 | 1 | T-MODEL-003 | I | train_years ∩ test_years = ∅ every fold |
| SPEC-MODEL-004 | 1 | T-MODEL-004 | U | SMOTE train only; validation untouched |
| SPEC-MODEL-005 | 1 | T-MODEL-005 | U | Save → registry updated; load → same output |
| SPEC-MODEL-006 | 1 | T-MODEL-006a,b,c | U+I | >60 blocked; <40 passes; runs before signals |
| SPEC-MODEL-007 | 1 | T-MODEL-007 | I | 63d validation coverage ≥ 85% |
| SPEC-MODEL-008 | 1 | T-MODEL-008 | I | New wins 2/3 → promoted; else kept |
| SPEC-MODEL-009 | 2 | T-MODEL-009 | R | Satyam-like → amber/red |
| SPEC-MODEL-010 | 2 | T-MODEL-010 | R | ≤ 2/50 Nifty 50 red; flags known frauds |

### SPEC-BT: Backtesting

| Spec ID | Phase | Test ID | Type | Verification |
|---------|:-----:|---------|:----:|-------------|
| SPEC-BT-001 | 1 | T-BT-001 | I | 9 rules checked; 0 violations |
| SPEC-BT-002 | 1 | T-BT-002a,b | U | Round-trip 0.40–0.50%; small-cap higher |
| SPEC-BT-003 | 1 | T-BT-003 | I | Universe includes delisted |
| SPEC-BT-004 | 1 | T-BT-004 | U | All 9 metrics present; std(Sharpe) reported |

### SPEC-DS: DataStore

| Spec ID | Phase | Test ID | Type | Verification |
|---------|:-----:|---------|:----:|-------------|
| SPEC-DS-001 | 0 | T-DS-001 | S | No external API calls in systems/ |
| SPEC-DS-002 | 1 | T-DS-002 | S | No duckdb.connect() in systems/ |
| SPEC-DS-003 | 0 | T-DS-003 | U | as_of before announcement → old data returned |
| SPEC-DS-004 | 1 | T-DS-004 | I | POST → upsert; no duplicates |
| SPEC-DS-005 | 3 | T-DS-005 | I | ML reads valuation from Damodaran |
| SPEC-DS-006 | 1 | T-DS-006 | U | Every Parquet feature has catalog entry |
| SPEC-DS-007 | 0 | T-DS-007 | I | DuckDB for analytical; SQLite for transactional |

### SPEC-SCHED: Scheduler

| Spec ID | Phase | Test ID | Type | Verification |
|---------|:-----:|---------|:----:|-------------|
| SPEC-SCHED-001 | 0 | T-SCHED-001 | U | Linear mode → no clock dependency |
| SPEC-SCHED-002 | 0 | T-SCHED-002 | I | Crash at step 7 → resume from 7 |
| SPEC-SCHED-003 | 0 | T-SCHED-003 | I | 30 days missed → 21 trading days detected |
| SPEC-SCHED-004 | 0 | T-SCHED-004 | U | Backfill order strictly ascending |
| SPEC-SCHED-005 | 0 | T-SCHED-005 | U | After run: pipeline_runs rows exist |
| SPEC-SCHED-006 | 0 | T-SCHED-006 | I | Backfill → inference steps skipped |
| SPEC-SCHED-007 | 1 | T-SCHED-007 | U | 45 days stale (interval 30) → retrain |
| SPEC-SCHED-008 | 0 | T-SCHED-008 | U | Republic Day not a gap |
| SPEC-SCHED-009 | 0 | T-SCHED-009 | M | Oracle scraper runs when laptop off |
| SPEC-SCHED-010 | 0 | T-SCHED-010 | U | Kill mid-write → no partial Parquet |
| SPEC-SCHED-011 | 0 | T-SCHED-011 | U | Dep failed → dependent skipped |

### SPEC-UI, SPEC-ALERT, SPEC-SEC, SPEC-QUALITY, SPEC-OBS

| Spec ID | Phase | Test ID | Type | Verification |
|---------|:-----:|---------|:----:|-------------|
| SPEC-UI-001 | 1 | T-UI-001 | H | HITL-05: SHAP explanations coherent |
| SPEC-UI-002 | 1 | T-UI-002 | M | All model scores visible |
| SPEC-UI-003 | 2 | T-UI-003 | H | HITL-03: Analogue quality |
| SPEC-UI-004 | 2 | T-UI-004 | H | HITL-06: Forensic trend |
| SPEC-UI-005 | 1 | T-UI-005 | H | HITL-07: Backtest scrutiny |
| SPEC-UI-006 | 1 | T-UI-006 | I | Render < 3 sec |
| SPEC-ALERT-001 | 1 | T-ALERT-001 | U | P&D → CRITICAL; exit>80 → HIGH |
| SPEC-ALERT-002 | 1 | T-ALERT-002 | U | Alert includes timestamp, ticker, type, reason |
| SPEC-SEC-001 | 0 | T-SEC-001 | S | grep for keys/passwords → zero |
| SPEC-SEC-002 | 0 | T-SEC-002 | S | No network bind in DB connections |
| SPEC-QUALITY-001 | All | T-QUALITY-001 | I | pytest --cov ≥ 80% |
| SPEC-QUALITY-002 | All | T-QUALITY-002 | S | All public functions have docstrings |
| SPEC-QUALITY-003 | All | T-QUALITY-003 | S | flake8 clean; type hints present |
| SPEC-OBS-001 | 0 | T-OBS-001 | U | False → NoOp; zero performance |
| SPEC-OBS-002 | 0 | T-OBS-002 | U | Each level → correct events |
| SPEC-OBS-003 | 0 | T-OBS-003 | U | observability.jsonl → valid JSON lines |
| SPEC-OBS-004 | 0 | T-OBS-004 | I | /api/v1/system/health returns metrics |
| SPEC-OBS-005 | 0 | T-OBS-005 | U | error level → no per-stock output |

### SPEC-SOLID, SPEC-LIB, SPEC-TRACE

| Spec ID | Phase | Test ID | Type | Verification |
|---------|:-----:|---------|:----:|-------------|
| SPEC-SOLID-001 | All | T-SOLID-001 | S | No .py file > 500 lines |
| SPEC-SOLID-002 | All | T-SOLID-002 | S | New model added without modifying existing |
| SPEC-SOLID-003 | All | T-SOLID-003 | U | Every model passes BaseModel interface |
| SPEC-SOLID-004 | All | T-SOLID-004 | S | Segregated client interfaces |
| SPEC-SOLID-005 | All | T-SOLID-005 | S | High-level imports abstractions |
| SPEC-LIB-001 | 0 | T-LIB-001 | S | All libraries have version pins |
| SPEC-LIB-002 | All | T-LIB-002 | M | Upgrade → test → commit protocol |
| SPEC-LIB-003 | All | T-LIB-003 | M | pip-audit quarterly |
| SPEC-LIB-004 | All | T-LIB-004 | S | Custom code only where no library exists |
| SPEC-TRACE-001 | All | T-TRACE-001 | M | RTM reviewed every phase gate |
| SPEC-TRACE-002 | All | T-TRACE-002 | S | Docstrings reference SPEC-IDs |
| SPEC-TRACE-003 | All | T-TRACE-003 | M | Commits reference SPEC-IDs |
| SPEC-TRACE-004 | All | T-TRACE-004 | S | Test docstrings reference SPEC-IDs |

### RTM Summary: 80 specs, 80 tests, 100% traceability

| Test Type | Count | Automated? |
|-----------|:-----:|:----------:|
| Unit (U) | 42 | Yes |
| Integration (I) | 18 | Yes |
| Regression (R) | 2 | Yes |
| Structural (S) | 8 | Yes (grep/AST scans) |
| HITL (H) | 5 | No — human judgment |
| Manual (M) | 5 | No — periodic review |

---

## PART 2: ARCHITECTURE IMPROVEMENTS

### Improvement 1: DataStore Client SDK
Add `datastore/client.py` — a thin wrapper over API calls. Consumer systems import
this class instead of calling httpx directly. If API routes change, only the client
class needs updating. Implements SPEC-SOLID-005 (Dependency Inversion).

### Improvement 2: Event Bus for Cross-System Notifications
Add `datastore/events.py` — file-based event bus (JSON lines). Systems emit events
(pnd_block_added, exit_urgent, valuation_updated, model_retrained). Other systems
poll events. Not Kafka — that's overkill for solo developer.

### Improvement 3: Pydantic Validation at Layer Boundaries
Add `datastore/schemas/` — Pydantic models validate data on write to DataStore.
If bhavcopy scraper writes garbage (high < low, negative volume), Pydantic catches
it before it corrupts features.

### Improvement 4: Config Validation on Startup
Add `config/validator.py` — validates all settings before pipeline runs. Fail fast
with clear errors, not obscure crashes mid-pipeline.

### Improvement 5: Comprehensive Health Endpoint
Expand `/api/v1/system/health` to aggregate pipeline status, DataStore freshness,
model staleness, drift status, and universe count.

---

## PART 3: SOLID PRINCIPLES

### SRP — Single Responsibility
Every module has one reason to change. bhavcopy.py downloads, price_adjuster.py adjusts,
technical.py computes features. No mixing.
**Automated check:** No .py file > 500 lines (SRP violation signal).

### OCP — Open/Closed
Use Model Registry pattern. Adding M-11 TFT in Phase 3 does NOT modify signal_5d.py.
```python
@ModelRegistry.register('signal_5d')
class Signal5dModel(BaseModel): ...
# Phase 3: just add a new file
@ModelRegistry.register('tft')
class TFTModel(BaseModel): ...
```

### LSP — Liskov Substitution
Every model implements BaseModel: train(), predict(), predict_proba(), save(), load().
LSP test: iterate ModelRegistry, call all 5 methods on each.

### ISP — Interface Segregation
DataStore client has separate interfaces: OHLCVClient, FundamentalsClient, SignalsClient.
TA System imports only OHLCVClient + SignalsClient. Not FundamentalsClient.

### DIP — Dependency Inversion
Pipeline runner depends on PipelineJob abstraction, not concrete functions.
Consumer systems depend on DataStoreClient, not DuckDB directly.

---

## PART 4: LIBRARY GOVERNANCE

**SPEC-LIB-001:** All requirements use >= pinning (not ==exact).
**SPEC-LIB-002:** Upgrade protocol: branch → upgrade → full test → backtest compare → merge.
**SPEC-LIB-003:** Quarterly `pip-audit`. Fix critical CVEs immediately.
**SPEC-LIB-004:** Prefer public libraries. Custom code only when no library exists.

---

## PART 5: TRACEABILITY STANDARDS

**SPEC-TRACE-002 — Every function docstring references SPEC-ID:**
```python
def apply_corporate_actions(conn, ticker):
    \"\"\"
    Implements: SPEC-PIPE-002
    PIT assumption: Uses ex_date from corporate_actions table.
    \"\"\"
```

**SPEC-TRACE-003 — Every commit references SPEC-ID:**
```
feat(SPEC-PIPE-002): implement corporate action adjustment engine
```

**SPEC-TRACE-004 — Every test references SPEC-ID:**
```python
def test_bonus_halves_price(self):
    \"\"\"Verifies: SPEC-PIPE-002\"\"\"
```

---

## PART 6: COMMENTING STANDARDS

### Module-level: purpose, spec IDs, dependencies, phase
### Function-level: spec reference, PIT assumptions, args, returns
### Inline: only WHY, never WHAT. Reference spec ID for non-obvious logic.
### Never comment: obvious operations, self-documenting variable names.

---

## PART 7: PHASE GATE CHECKLISTS

### Phase 0 → Phase 1 Gate
- [ ] All Phase 0 tests passing (T-PIPE-002, T-PIPE-003, T-DS-003, T-SCHED-002, etc.)
- [ ] Oracle scrapers running (T-SCHED-009 manual)
- [ ] Historical data: ≥ 600K OHLCV rows in DuckDB
- [ ] Checkpoint-resume verified (T-SCHED-002)
- [ ] No credentials in source (T-SEC-001)
- [ ] Observability emitting (T-OBS-003)
- [ ] RTM reviewed (T-TRACE-001)

### Phase 1 → Phase 2 Gate
- [ ] All Phase 1 tests passing
- [ ] Random feature test < 55%
- [ ] P&D pre-filter runs before signals (T-MODEL-006c)
- [ ] pytest --cov ≥ 80% on pipeline/
- [ ] Every function has docstring with SPEC-ID (T-TRACE-002)
- [ ] Paper trading started
- [ ] RTM reviewed

### Phase 2 → Phase 3 Gate
- [ ] Screener.in PIT verified (T-PIPE-003a)
- [ ] Sector z-scores working (T-FEAT-002)
- [ ] Forensic flags known frauds (T-MODEL-009)
- [ ] pip-audit clean (T-LIB-003)
- [ ] ≥ 3 months paper trading
- [ ] RTM reviewed

### Phase 3 → Phase 4 Gate
- [ ] Cross-system fusion verified (T-DS-005)
- [ ] Ensemble Sharpe ≥ 0.1 improvement
- [ ] Library versions audited
- [ ] RTM reviewed
