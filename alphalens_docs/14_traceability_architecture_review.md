# AlphaLens — Requirements Traceability & Architecture Review
## RTM · Architecture Improvements · Phase Alignment · Cross-Cutting Concerns

---

## Part 1: Requirements Traceability Matrix (RTM)

**Rule: Every spec must have at least one test. No spec without a test = no way to verify it was built correctly.**

Legend:
- **Unit** = automated unit test in tests/unit/
- **Integ** = automated integration test in tests/integration/
- **Regress** = automated regression test in tests/regression/
- **HITL** = Human-in-the-Loop test in tests/hitl/
- **Struct** = Structural test (lint, static analysis, file scan)
- **Manual** = Manual verification (one-time setup, visual check)

### SPEC-SYS: System-Level (5 specs)

| Spec ID | Spec Title | Phase | Test Type | Test ID | Acceptance Criteria |
|---------|-----------|:-----:|-----------|---------|---------------------|
| SPEC-SYS-001 | Universe Coverage | 0 | Unit | T-SYS-001 | `get_active_universe()` returns correct count per tier config |
| SPEC-SYS-002 | Pipeline Completion | 1 | Integ | T-SYS-002 | Full pipeline completes in < 90 min on reference hardware |
| SPEC-SYS-003 | Completeness Gate | 1 | Unit | T-SYS-003 | Pipeline halts model inference if complete_stocks < 450 |
| SPEC-SYS-004 | Availability | 1 | Manual | T-SYS-004 | Oracle scraper uptime > 95% over 30-day window |
| SPEC-SYS-005 | Storage Budgets | 0 | Struct | T-SYS-005 | `du -sh datastore/` < 500 GB after 1 year of data |

### SPEC-SYS-011: Universe Expansion (1 spec)

| Spec ID | Spec Title | Phase | Test Type | Test ID | Acceptance Criteria |
|---------|-----------|:-----:|-----------|---------|---------------------|
| SPEC-SYS-011 | Configurable Universe | 0–4 | Unit | T-SYS-011 | Changing TIER_THRESHOLD changes stock count correctly |

### SPEC-PIPE: Data Pipeline (6 specs)

| Spec ID | Spec Title | Phase | Test Type | Test ID | Acceptance Criteria |
|---------|-----------|:-----:|-----------|---------|---------------------|
| SPEC-PIPE-001 | OHLCV Ingestion | 0 | Unit+Integ | T-PIPE-001 | Bhavcopy parsed; ≥ 450 stocks per day; stored in DuckDB |
| SPEC-PIPE-002 | Corporate Action Adjustment | 0 | Unit | T-PIPE-002 | Bonus halves price; split adjusts correctly; no double-adjust; idempotent |
| SPEC-PIPE-003 | Point-in-Time Alignment | 0 | Unit | T-PIPE-003 | announcement_date used (never quarter_end_date); PIT violation = test failure |
| SPEC-PIPE-004 | Feature Performance | 1 | Integ | T-PIPE-004 | 76 features × 500 stocks in < 15 min |
| SPEC-PIPE-005 | Data Quality | 1 | Unit | T-PIPE-005 | PSI > 0.10 triggers alert; nulls > 1% flagged; ranges validated |
| SPEC-PIPE-006 | Macro Ingestion | 0 | Unit | T-PIPE-006 | VIX, USD/INR, Crude fetched; retry 3× on failure; fallback to prev day |

### SPEC-FEAT: Feature Engineering (5 specs)

| Spec ID | Spec Title | Phase | Test Type | Test ID | Acceptance Criteria |
|---------|-----------|:-----:|-----------|---------|---------------------|
| SPEC-FEAT-001 | Min History | 1 | Unit | T-FEAT-001 | Stock with < 252 days excluded; NaN for insufficient lookback |
| SPEC-FEAT-002 | Normalization | 2 | Unit | T-FEAT-002 | Z-scores computed per sector; clipped to [-5, +5] |
| SPEC-FEAT-003 | Cyclical Encoding | 1 | Unit | T-FEAT-003 | month sin/cos output range [-1, 1]; raw int never in features |
| SPEC-FEAT-004 | F&O Scope | 2 | Unit | T-FEAT-004 | Non-F&O stocks get NaN for 16 F&O features |
| SPEC-FEAT-005 | Sector Definitions | 2 | Unit | T-FEAT-005 | Min 5 stocks per sector for z-score; smaller use market-wide mean |

### SPEC-MODEL: ML Models (10 specs)

| Spec ID | Spec Title | Phase | Test Type | Test ID | Acceptance Criteria |
|---------|-----------|:-----:|-----------|---------|---------------------|
| SPEC-MODEL-001 | Training Data Min | 1 | Unit | T-MODEL-001 | Model refuses to train if < 252 rows per fold |
| SPEC-MODEL-002 | Triple-Barrier Labels | 1 | Unit | T-MODEL-002 | Labels ∈ {-1, 0, 1}; no label beyond horizon; balanced distribution |
| SPEC-MODEL-003 | Walk-Forward | 1 | Integ | T-MODEL-003 | Train years and test years never overlap; ≥ 3 folds |
| SPEC-MODEL-004 | Class Imbalance | 1 | Unit | T-MODEL-004 | SMOTE on train only; threshold ≠ 0.5; class weights logged |
| SPEC-MODEL-005 | Model Versioning | 1 | Struct | T-MODEL-005 | Model saved with version string; registry.json updated; 3 versions retained |
| SPEC-MODEL-006 | P&D Pre-Filter | 1 | Unit+Integ | T-MODEL-006 | Score > 60 = hard block; runs before signals; P&D step < signal step in pipeline |
| SPEC-MODEL-007 | Conformal Coverage | 1 | Integ | T-MODEL-007 | Empirical coverage ≥ 85% at α=0.10 on held-out validation |
| SPEC-MODEL-008 | Retrain Protocol | 1 | Integ | T-MODEL-008 | Snapshot → train → shadow → compare → promote only if wins 2/3 |
| SPEC-MODEL-009 | Forensic Classical | 2 | Regress | T-MODEL-009 | Beneish, Altman, Piotroski correct on known inputs |
| SPEC-MODEL-010 | Forensic ML | 2 | Regress | T-MODEL-010 | Flags > 70% of known frauds; ≤ 2/50 Nifty 50 false positives |

### SPEC-BT: Backtesting (4 specs)

| Spec ID | Spec Title | Phase | Test Type | Test ID | Acceptance Criteria |
|---------|-----------|:-----:|-----------|---------|---------------------|
| SPEC-BT-001 | Walk-Forward Integrity | 1 | Integ | T-BT-001 | All 9 rules checked automatically; violations = test failure |
| SPEC-BT-002 | Transaction Costs | 1 | Unit | T-BT-002 | Round-trip 0.40–0.50%; all 6 components; small-cap slippage |
| SPEC-BT-003 | Survivorship Bias | 1 | Integ | T-BT-003 | Delisted stocks in universe; forced-exit logged |
| SPEC-BT-004 | Performance Reporting | 1 | Integ | T-BT-004 | All 9 metrics reported per fold + aggregate; 4 benchmarks |

### SPEC-UI: User Interface (6 specs)

| Spec ID | Spec Title | Phase | Test Type | Test ID | Acceptance Criteria |
|---------|-----------|:-----:|-----------|---------|---------------------|
| SPEC-UI-001 | Daily Dashboard | 1 | HITL | T-UI-001 | Screen renders; signals, P&D, exits, regime all visible |
| SPEC-UI-002 | Signal Detail | 1 | HITL | T-UI-002 | SHAP waterfall displayed; all model scores shown |
| SPEC-UI-003 | Multibagger Watchlist | 2 | HITL | T-UI-003 | Top 20 ranked; survival curves; analogues present |
| SPEC-UI-004 | Forensic Alert | 2 | HITL | T-UI-004 | Red/amber stocks listed; score breakdown; trend chart |
| SPEC-UI-005 | Backtest Results | 1 | HITL | T-UI-005 | Fold results; integrity checks; benchmark comparison |
| SPEC-UI-006 | Performance | 1 | Integ | T-UI-006 | Dashboard renders in < 3 seconds |

### SPEC-ALERT: Alerting (2 specs)

| Spec ID | Spec Title | Phase | Test Type | Test ID | Acceptance Criteria |
|---------|-----------|:-----:|-----------|---------|---------------------|
| SPEC-ALERT-001 | Alert Types | 1 | Unit | T-ALERT-001 | All 9 alert types generated with correct priority |
| SPEC-ALERT-002 | Alert Delivery | 1 | Integ | T-ALERT-002 | Console + log output confirmed; email in Phase 2 |

### SPEC-SEC: Security (2 specs)

| Spec ID | Spec Title | Phase | Test Type | Test ID | Acceptance Criteria |
|---------|-----------|:-----:|-----------|---------|---------------------|
| SPEC-SEC-001 | Credentials | 0 | Struct | T-SEC-001 | No API keys in source code (grep scan); .env in .gitignore |
| SPEC-SEC-002 | Data Access | 0 | Struct | T-SEC-002 | No network-exposed database ports; OCI creds not in Python |

### SPEC-QUALITY: Code Quality (3 specs)

| Spec ID | Spec Title | Phase | Test Type | Test ID | Acceptance Criteria |
|---------|-----------|:-----:|-----------|---------|---------------------|
| SPEC-QUALITY-001 | Test Coverage | 1 | Struct | T-QUAL-001 | `pytest --cov` reports ≥ 80% for pipeline/; 100% for critical paths |
| SPEC-QUALITY-002 | Documentation | 1 | Struct | T-QUAL-002 | `pydocstyle` passes on all public functions |
| SPEC-QUALITY-003 | Code Style | 0 | Struct | T-QUAL-003 | `flake8` passes; no print(); no hardcoded paths |

### SPEC-DS: DataStore (7 specs)

| Spec ID | Spec Title | Phase | Test Type | Test ID | Acceptance Criteria |
|---------|-----------|:-----:|-----------|---------|---------------------|
| SPEC-DS-001 | Central Ownership | 1 | Struct | T-DS-001 | Consumer systems import only httpx, never duckdb/sqlite3 directly |
| SPEC-DS-002 | API-First Access | 1 | Integ | T-DS-002 | All consumer reads go through FastAPI; Swagger docs accessible |
| SPEC-DS-003 | PIT at API Level | 1 | Unit | T-DS-003 | as_of parameter enforces announcement_date/filing_date correctly |
| SPEC-DS-004 | Write-Back Protocol | 1 | Unit | T-DS-004 | Upsert works; schema validation rejects malformed; writes logged |
| SPEC-DS-005 | Cross-System Fusion | 3 | Integ | T-DS-005 | ML reads valuation_gap; TA reads hmm_regime; round-trip verified |
| SPEC-DS-006 | Feature Catalog | 1 | Struct | T-DS-006 | Every feature in Parquet has entry in feature_catalog.json |
| SPEC-DS-007 | Six Stores | 0 | Struct | T-DS-007 | All 6 store directories exist; DuckDB for analytical, SQLite for transactional |

### SPEC-SCHED: Scheduler (11 specs)

| Spec ID | Spec Title | Phase | Test Type | Test ID | Acceptance Criteria |
|---------|-----------|:-----:|-----------|---------|---------------------|
| SPEC-SCHED-001 | Flexible Modes | 1 | Unit | T-SCHED-001 | linear/timestamp/manual modes all work; no hardcoded clock |
| SPEC-SCHED-002 | Checkpoint-Resume | 1 | Integ | T-SCHED-002 | Crash at step 7 → resume from step 7 (steps 1–6 not re-run) |
| SPEC-SCHED-003 | Unlimited Backfill | 1 | Integ | T-SCHED-003 | 10-day gap detected and backfilled chronologically |
| SPEC-SCHED-004 | Chrono Order | 1 | Unit | T-SCHED-004 | Backfill processes dates oldest-first; verify ordering |
| SPEC-SCHED-005 | State Tracking | 1 | Unit | T-SCHED-005 | pipeline_runs and pipeline_checkpoints tables populated correctly |
| SPEC-SCHED-006 | No Inference Backfill | 1 | Integ | T-SCHED-006 | Backfill dates: features computed, signals NOT generated |
| SPEC-SCHED-007 | Retrain Catch-Up | 2 | Integ | T-SCHED-007 | Model overdue by 1.5× interval → retrain triggered |
| SPEC-SCHED-008 | Holiday Awareness | 0 | Unit | T-SCHED-008 | NSE holidays excluded from gap detection |
| SPEC-SCHED-009 | Oracle Independence | 0 | Manual | T-SCHED-009 | Oracle scrapers run while laptop is off; data in Object Storage |
| SPEC-SCHED-010 | Atomic Writes | 1 | Unit | T-SCHED-010 | Parquet write crash → no partial file in feature store |
| SPEC-SCHED-011 | Step Dependencies | 1 | Unit | T-SCHED-011 | Step skipped when dependency failed; reason logged |

### SPEC-OBS: Observability (5 specs)

| Spec ID | Spec Title | Phase | Test Type | Test ID | Acceptance Criteria |
|---------|-----------|:-----:|-----------|---------|---------------------|
| SPEC-OBS-001 | Master Switch | 1 | Unit | T-OBS-001 | ENABLED=False → NoOpObservability; zero overhead confirmed |
| SPEC-OBS-002 | Levels | 1 | Unit | T-OBS-002 | 'error' level: only errors logged; 'debug': everything logged |
| SPEC-OBS-003 | Structured Logging | 1 | Unit | T-OBS-003 | Events written as valid JSON lines to observability.jsonl |
| SPEC-OBS-004 | Metrics | 1 | Integ | T-OBS-004 | GET /api/v1/system/health returns step timings and counts |
| SPEC-OBS-005 | Production Mode | 1 | Unit | T-OBS-005 | LEVEL='error' → no per-stock logs; console minimal |

### SPEC-SOLID: SOLID Principles (5 specs)

| Spec ID | Spec Title | Phase | Test Type | Test ID | Acceptance Criteria |
|---------|-----------|:-----:|-----------|---------|---------------------|
| SPEC-SOLID-001 | Single Responsibility | 1 | Struct | T-SOLID-001 | No module > 500 lines; each file has one purpose |
| SPEC-SOLID-002 | Open/Closed | 1 | Struct | T-SOLID-002 | New features via new classes, not modifying existing |
| SPEC-SOLID-003 | Liskov Substitution | 1 | Unit | T-SOLID-003 | All models implement BaseModel; swappable without side effects |
| SPEC-SOLID-004 | Interface Segregation | 1 | Struct | T-SOLID-004 | No "god" interface; BaseModel is lean (6 methods max) |
| SPEC-SOLID-005 | Dependency Inversion | 1 | Struct | T-SOLID-005 | Models depend on abstract DataStore interface, not DuckDB directly |

### SPEC-LIB: Library Governance (4 specs)

| Spec ID | Spec Title | Phase | Test Type | Test ID | Acceptance Criteria |
|---------|-----------|:-----:|-----------|---------|---------------------|
| SPEC-LIB-001 | Version Pinning | 0 | Struct | T-LIB-001 | All requirements.txt use >= minimum version |
| SPEC-LIB-002 | Upgrade Protocol | All | Struct | T-LIB-002 | Library upgrade: branch → update → full test suite → merge |
| SPEC-LIB-003 | Security Audit | All | Struct | T-LIB-003 | `pip-audit` runs quarterly; CVEs addressed within 7 days |
| SPEC-LIB-004 | Prefer Public | All | Struct | T-LIB-004 | Custom code only when no public library exists; documented justification |

### SPEC-TRACE: Traceability (4 specs)

| Spec ID | Spec Title | Phase | Test Type | Test ID | Acceptance Criteria |
|---------|-----------|:-----:|-----------|---------|---------------------|
| SPEC-TRACE-001 | RTM Coverage | All | Struct | T-TRACE-001 | Every spec has ≥ 1 test; RTM audit passes (this document) |
| SPEC-TRACE-002 | Docstring Refs | 1 | Struct | T-TRACE-002 | Every public function docstring references its spec ID(s) |
| SPEC-TRACE-003 | Commit Refs | All | Struct | T-TRACE-003 | Every commit message references ≥ 1 spec ID |
| SPEC-TRACE-004 | Test-Spec Linkage | 1 | Struct | T-TRACE-004 | Every test function docstring starts with spec ID being tested |

### RTM Summary

| Section | Total Specs | Unit | Integ | Regress | HITL | Struct | Manual | Total Tests |
|---------|:----------:|:----:|:-----:|:-------:|:----:|:------:|:------:|:-----------:|
| SPEC-SYS | 6 | 2 | 1 | 0 | 0 | 1 | 1 | 5 |
| SPEC-PIPE | 6 | 5 | 1 | 0 | 0 | 0 | 0 | 6 |
| SPEC-FEAT | 5 | 5 | 0 | 0 | 0 | 0 | 0 | 5 |
| SPEC-MODEL | 10 | 5 | 4 | 2 | 0 | 1 | 0 | 10 |
| SPEC-BT | 4 | 1 | 3 | 0 | 0 | 0 | 0 | 4 |
| SPEC-UI | 6 | 0 | 1 | 0 | 5 | 0 | 0 | 6 |
| SPEC-ALERT | 2 | 1 | 1 | 0 | 0 | 0 | 0 | 2 |
| SPEC-SEC | 2 | 0 | 0 | 0 | 0 | 2 | 0 | 2 |
| SPEC-QUALITY | 3 | 0 | 0 | 0 | 0 | 3 | 0 | 3 |
| SPEC-DS | 7 | 2 | 2 | 0 | 0 | 3 | 0 | 7 |
| SPEC-SCHED | 11 | 5 | 4 | 0 | 0 | 0 | 1 | 11 |
| SPEC-OBS | 5 | 3 | 1 | 0 | 0 | 0 | 0 | 5 |
| SPEC-SOLID | 5 | 1 | 0 | 0 | 0 | 4 | 0 | 5 |
| SPEC-LIB | 4 | 0 | 0 | 0 | 0 | 4 | 0 | 4 |
| SPEC-TRACE | 4 | 0 | 0 | 0 | 0 | 4 | 0 | 4 |
| **Total** | **80** | **30** | **18** | **2** | **5** | **22** | **2** | **79** |

**Coverage: 79 tests for 80 specs (99%). SPEC-SYS-004 (availability) is manual-only with no automated proxy.**

---

## Part 2: Architecture Improvements

### Improvement 1: Abstract DataStore Interface (SPEC-SOLID-005)

The current design has consumer systems calling FastAPI via httpx. This is correct for
cross-process communication. But within the same process (e.g., the ingestion layer
writing to the normalised store), we need an abstraction layer so the code doesn't
depend on DuckDB directly.

```python
# datastore/interfaces.py — abstract interface

from abc import ABC, abstractmethod
import pandas as pd

class IDataStore(ABC):
    """Abstract DataStore interface. All data access goes through this."""

    @abstractmethod
    def get_ohlcv(self, ticker: str, from_date: str, to_date: str) -> pd.DataFrame: ...

    @abstractmethod
    def get_fundamentals_pit(self, ticker: str, as_of: str) -> dict: ...

    @abstractmethod
    def get_feature_matrix(self, date: str) -> pd.DataFrame: ...

    @abstractmethod
    def write_signal(self, signal: dict) -> None: ...

    @abstractmethod
    def get_active_universe(self, as_of: str) -> list: ...


class DuckDBDataStore(IDataStore):
    """Concrete implementation using DuckDB + Parquet."""
    def __init__(self, db_path: str): ...

class APIDataStore(IDataStore):
    """Concrete implementation using HTTP API calls (for consumer systems)."""
    def __init__(self, base_url: str): ...

class MockDataStore(IDataStore):
    """In-memory mock for unit testing. No disk I/O."""
    def __init__(self, test_data: dict): ...
```

**Why this matters:** Models depend on `IDataStore`, not `DuckDBDataStore`. You can swap
the backend (DuckDB → PostgreSQL, or DuckDB → cloud-hosted) without changing any model code.
Tests use `MockDataStore` for fast, deterministic testing without database setup.

### Improvement 2: Event Bus for Cross-System Communication

Currently, cross-system fusion happens via database polling (System B reads System A's
output from the signals table). This works but has no notification mechanism — System B
doesn't know when System A has written new data.

Add a lightweight in-process event bus:

```python
# datastore/events.py

class EventBus:
    """Simple pub/sub for intra-platform communication."""

    def __init__(self):
        self._subscribers = {}

    def subscribe(self, event_type: str, callback: callable):
        self._subscribers.setdefault(event_type, []).append(callback)

    def publish(self, event_type: str, data: dict):
        for callback in self._subscribers.get(event_type, []):
            callback(data)

# Usage:
# When ML engine writes signals:
event_bus.publish('ml_signals_written', {'date': '2026-05-20', 'count': 500})
# TA system subscribes and refreshes its cache:
event_bus.subscribe('ml_signals_written', ta_system.on_new_ml_signals)
```

**Phase:** 3 (when TA and Valuation systems come online). Not needed in Phase 1–2.

### Improvement 3: Configuration Validation on Startup

Add a startup check that validates all configuration before any pipeline step runs:

```python
# config/validator.py

def validate_config():
    """Run on every startup. Fail fast if config is invalid."""
    errors = []

    # Check DuckDB file exists and is accessible
    if not Path(settings.DUCKDB_PATH).exists():
        errors.append(f"DuckDB not found: {settings.DUCKDB_PATH}")

    # Check NSE holidays file has current year
    from config.nse_holidays import NSE_HOLIDAYS
    current_year = str(date.today().year)
    if not any(current_year in h for h in NSE_HOLIDAYS):
        errors.append(f"NSE holidays missing for {current_year}")

    # Check FYERS credentials
    if not os.environ.get('FYERS_APP_ID'):
        errors.append("FYERS_APP_ID not set in .env")

    # Check Oracle OCI config (if scrapers enabled)
    if settings.ORACLE_SCRAPERS_ENABLED:
        if not Path('~/.oci/config').expanduser().exists():
            errors.append("OCI config not found at ~/.oci/config")

    if errors:
        for e in errors:
            log.error(f"CONFIG ERROR: {e}")
        raise SystemExit("Configuration validation failed. Fix errors above.")
```

### Improvement 4: Database Migration System

As the schema evolves across phases (new tables, new columns), you need a migration system:

```python
# datastore/migrations/

# Migration files named sequentially:
# 001_initial_schema.py
# 002_add_signals_table.py
# 003_add_forensic_columns.py
# ...

# Each migration has up() and down() methods.
# On startup, run all pending migrations before pipeline.
```

Use `alembic` (SQLAlchemy) for SQLite migrations. For DuckDB, a lightweight custom
migration runner (DuckDB doesn't have Alembic support as of May 2026).

### Improvement 5: Health Check Endpoint with Dependency Status

Expand GET /api/v1/system/health to show per-dependency status:

```json
{
  "status": "healthy",
  "dependencies": {
    "duckdb_normalised": {"status": "ok", "size_mb": 450, "last_write": "2026-05-20T17:30:00"},
    "duckdb_signals": {"status": "ok", "size_mb": 120},
    "feature_store": {"status": "ok", "latest_date": "2026-05-20", "file_count": 1250},
    "oracle_storage": {"status": "ok", "last_sync": "2026-05-20T13:00:00"},
    "model_registry": {"status": "ok", "models_loaded": 7, "oldest_model_days": 18}
  },
  "pipeline": {
    "last_run": "2026-05-20",
    "status": "completed",
    "duration_seconds": 1842,
    "steps_completed": 16,
    "steps_failed": 0
  },
  "observability": {
    "enabled": true,
    "level": "info"
  }
}
```

---

## Part 3: Phase-Spec Alignment

### Phase 0 — Infrastructure (Weeks 1–4)

**Specs that must be satisfied before Phase 1 begins:**

| Spec | What must be true |
|------|-------------------|
| SPEC-SYS-001 | stock_master table populated; get_active_universe() returns ~500 |
| SPEC-SYS-005 | Storage structure created; < 100 GB used |
| SPEC-SYS-011 | Universe config profiles defined in settings.py |
| SPEC-PIPE-001 | OHLCV for 500 stocks × 5 years in DuckDB |
| SPEC-PIPE-002 | Corporate actions applied; price continuity < 1% at ex-dates |
| SPEC-PIPE-003 | Fundamental table has announcement_date; PIT test passes |
| SPEC-PIPE-006 | Macro data (VIX, FX, crude) loaded for 5 years |
| SPEC-DS-007 | All 6 stores created; DuckDB for analytical, SQLite for transactional |
| SPEC-SCHED-008 | NSE holidays file has current year + next year |
| SPEC-SCHED-009 | Oracle scrapers deployed and collecting data |
| SPEC-SEC-001 | .env file created; no credentials in source code |
| SPEC-SEC-002 | No network-exposed database ports |
| SPEC-QUALITY-003 | flake8 configured; pre-commit hook installed |
| SPEC-LIB-001 | All requirements.txt files created with version pins |
| SPEC-TRACE-003 | Git commit message template installed with spec ID placeholder |

**Cross-cutting applied in Phase 0:**
- [ ] Git pre-commit hook: flake8 + no print() + no hardcoded paths
- [ ] Commit message template: `[SPEC-XXX-NNN] description`
- [ ] .env template created with all required credential placeholders
- [ ] CLAUDE.md coding standards section read and acknowledged

### Phase 1 — Core Signal Engine (Weeks 5–14)

**Specs that must be satisfied before Phase 2 begins:**

| Spec | What must be true |
|------|-------------------|
| SPEC-PIPE-004 | 76 features compute in < 15 min for 500 stocks |
| SPEC-PIPE-005 | PSI drift monitor running daily; quality gate at 450 stocks |
| SPEC-FEAT-001 | Min 252 days enforced; NaN for insufficient history |
| SPEC-FEAT-003 | Cyclical encoding verified: sin/cos, no raw integers |
| SPEC-MODEL-001 | Models refuse training if < 252 rows per fold |
| SPEC-MODEL-002 | Triple-barrier labels validated; class balance checked |
| SPEC-MODEL-003 | Walk-forward: train/test never overlap |
| SPEC-MODEL-004 | SMOTE on train only; threshold optimized on validation |
| SPEC-MODEL-005 | Model versioning: registry.json + 3-version retention |
| SPEC-MODEL-006 | P&D pre-filter runs before signals; score > 60 = hard block |
| SPEC-MODEL-007 | Conformal coverage ≥ 85% at α=0.10 |
| SPEC-MODEL-008 | Retrain protocol: snapshot → train → shadow → compare → promote |
| SPEC-BT-001 | All 9 integrity rules pass automatically |
| SPEC-BT-002 | Transaction costs correct (round-trip 0.40–0.50%) |
| SPEC-BT-003 | Delisted stocks included in universe |
| SPEC-BT-004 | All 9 metrics reported; 4 benchmarks compared |
| SPEC-DS-001 | Consumer systems use API only (structural scan confirms) |
| SPEC-DS-002 | FastAPI running; Swagger at /docs accessible |
| SPEC-DS-003 | PIT enforcement at API level verified |
| SPEC-DS-004 | Write-back upsert working; schema validation active |
| SPEC-DS-006 | feature_catalog.json covers all 98 Phase 1 features |
| SPEC-SCHED-001 | Pipeline runs in linear mode without clock dependency |
| SPEC-SCHED-002 | Checkpoint-resume verified: crash at step 7 → resume from 7 |
| SPEC-SCHED-003 | 5-day gap detected and backfilled correctly |
| SPEC-SCHED-005 | pipeline_runs + pipeline_checkpoints tables populated |
| SPEC-SCHED-010 | Atomic Parquet write verified: crash → no partial file |
| SPEC-OBS-001 | Observability master switch works; NoOp class zero overhead |
| SPEC-OBS-003 | Events written as valid JSON lines |
| SPEC-SOLID-001 | No module > 500 lines |
| SPEC-SOLID-003 | All models pass BaseModel interface compliance test |
| SPEC-SOLID-005 | Models depend on IDataStore, not DuckDB directly |
| SPEC-TRACE-002 | All public functions have spec ID in docstring |
| SPEC-TRACE-004 | All test functions have spec ID in docstring |
| SPEC-QUALITY-001 | pytest --cov ≥ 80% for pipeline/; 100% for PIT, corp action, P&D |
| SPEC-ALERT-001 | All 9 alert types generate correctly |
| SPEC-UI-001 | Daily dashboard renders with real data |

**Cross-cutting applied in Phase 1:**
- [ ] Every new function: docstring with spec ID, inputs, outputs, PIT assumptions
- [ ] Every new file: module-level docstring with purpose and spec reference
- [ ] Every commit: spec ID in message
- [ ] Every test function: docstring starts with `Implements SPEC-XXX-NNN`
- [ ] Observability: on (level='debug' during development)
- [ ] Type hints on all public function signatures
- [ ] Logging: use `logging` module throughout; never print()

### Phase 2 — Fundamentals + Multibagger (Weeks 15–26)

**Additional specs to satisfy:**

| Spec | What must be true |
|------|-------------------|
| SPEC-FEAT-002 | Sector z-scores computed; clipped [-5, +5] |
| SPEC-FEAT-004 | F&O features: NaN for non-F&O stocks |
| SPEC-FEAT-005 | Sector definitions: min 5 stocks per sector |
| SPEC-MODEL-009 | Forensic classical scores correct on known inputs |
| SPEC-MODEL-010 | Forensic ML: > 70% known fraud detection |
| SPEC-SCHED-007 | Retrain catch-up working (overdue model → retrain) |
| SPEC-DS-006 | feature_catalog.json covers all 268 Phase 2 features |
| SPEC-UI-003 | Multibagger watchlist screen renders |
| SPEC-UI-004 | Forensic alert screen renders |
| SPEC-LIB-002 | First library upgrade cycle completed (branch → test → merge) |

### Phase 3 — Deep Learning + TA + Valuation (Weeks 27–38)

**Additional specs:**

| Spec | What must be true |
|------|-------------------|
| SPEC-DS-005 | Cross-system fusion verified: ML reads valuation_gap; TA reads regime |
| SPEC-LIB-003 | First quarterly security audit completed (`pip-audit`) |

### Phase 4 — FA + RL (Weeks 39+)

All remaining specs must be satisfied. Full RTM audit with 100% coverage.

---

## Part 4: Cross-Cutting Concerns Applied Per Phase

### Every Phase: Coding Standards Enforcement

```
EVERY FILE you create must have:
├── Module-level docstring with purpose + spec reference(s)
├── Type hints on all public function signatures
├── Spec ID in every public function docstring
├── Logging via logging module (never print())
├── Paths from config/settings.py (never hardcoded)
└── Unit test in corresponding tests/ directory

EVERY COMMIT must have:
├── Spec ID in commit message: "[SPEC-XXX-NNN] description"
├── All unit tests pass: pytest tests/unit/ -v
└── flake8 passes: flake8 . --max-line-length 120

EVERY TEST must have:
├── Docstring starting with: "Implements SPEC-XXX-NNN: description"
├── Clear assertion messages explaining what went wrong
└── Uses MockDataStore or fixtures (no real database dependency)
```

### Phase 0 Setup Checklist

```bash
# Pre-commit hook (created once in Phase 0)
cat > .git/hooks/pre-commit << 'EOF'
#!/bin/bash
echo "Running flake8..."
flake8 . --max-line-length 120 --exclude .venv,__pycache__ || exit 1
echo "Checking for print() in production code..."
grep -rn "print(" --include="*.py" ingestion/ features/ systems/ datastore/ | grep -v "test_" | grep -v "#.*print" && echo "ERROR: print() found in production code" && exit 1
echo "Checking for hardcoded paths..."
grep -rn "'/data/\|'/home/\|'C:\\\\" --include="*.py" ingestion/ features/ systems/ datastore/ | grep -v "test_" && echo "ERROR: hardcoded path found" && exit 1
echo "Pre-commit checks passed"
EOF
chmod +x .git/hooks/pre-commit

# Commit message template
cat > .gitmessage << 'EOF'
[SPEC-XXX-NNN] Short description

# Why: Brief explanation of the change
# Spec: Reference the spec ID(s) this implements
# Test: Which test(s) verify this change
EOF
git config commit.template .gitmessage
```

### Observability Per Phase

| Phase | OBSERVABILITY_LEVEL | Rationale |
|:-----:|:-------------------:|-----------|
| 0 | debug | Debugging data ingestion; see everything |
| 1 dev | debug | Building models; need per-stock diagnostics |
| 1 stable | info | Daily operation; step timing + errors |
| 2 | info | Stable pipeline; step timing + errors |
| 3 | info | Adding deep learning; monitor training |
| 4 | warning | Production-like; only warnings + errors |
| Production | error or warning | Minimal logging; maximum performance |

### Library Governance Per Phase

| Phase | Action |
|:-----:|--------|
| 0 | Pin all library versions in requirements/*.txt |
| 1 end | First `pip-audit` run; document any CVEs |
| 2 start | Evaluate LightGBM/CatBoost minor version upgrades; test before merge |
| 2 end | Second `pip-audit` run |
| 3 start | PyTorch version pinned; test TFT compatibility before upgrading |
| Every quarter | `pip-audit`; review changelogs for libraries with breaking changes |

### Upgrade Protocol (SPEC-LIB-002)

```
1. Create branch: git checkout -b upgrade/lightgbm-4.6
2. Update version in requirements/phase1.txt
3. pip install -r requirements/phase1.txt --upgrade
4. Run FULL test suite: pytest tests/ -v --cov
5. If any test fails: investigate, fix, or revert
6. Run 1 backtest fold to verify model performance unchanged
7. If Sharpe within 0.05 of previous: merge
8. If Sharpe drops > 0.05: investigate; likely version-specific behaviour change
```

---

## Part 5: Architecture Decisions — SOLID Principles

### SRP in Practice

| Module | Responsibility | Violation Example |
|--------|---------------|-------------------|
| `ingestion/scrapers/bhavcopy.py` | Download + parse NSE bhavcopy | ❌ Also computing features |
| `features/technical.py` | Compute 76 technical features | ❌ Also loading data from DB |
| `systems/ml_signal_engine/models/signal/signal_5d.py` | Train + predict 5d signals | ❌ Also writing to DataStore |
| `datastore/api/routers/ohlcv.py` | Serve OHLCV API endpoints | ❌ Also adjusting prices |

**Rule:** Each file does ONE thing. If you find yourself writing "and" in the module description, split it.

### OCP in Practice

Adding a new model (e.g., System 2: Technical Analysis) should require:
- Creating new files in `systems/technical_analysis/`
- Registering new API routes in `datastore/api/routers/`
- Adding new signal table columns (migration)

It should NOT require:
- Modifying any existing model code
- Changing the pipeline runner
- Modifying the checkpoint engine

### LSP in Practice

Every model inherits BaseModel. This means:
```python
# This must work for ANY model — Signal5d, PnDDetector, MultibaggerModel, etc.
def train_and_evaluate(model: BaseModel, X_train, y_train, X_val, y_val):
    model.train(X_train, y_train, X_val, y_val)
    preds = model.predict(X_val)
    importance = model.get_feature_importance()
    model.save(f"datastore/models/{model.name}_v{today}.pkl")
```

If PnDDetector.predict() returns a different shape than Signal5d.predict(), that violates LSP.

### ISP in Practice

BaseModel has 6 methods. That's lean. Don't add `visualize()`, `export_to_excel()`,
or `send_alert()` to BaseModel — those belong to separate interfaces.

### DIP in Practice

```python
# WRONG — model depends on concrete database
class Signal5dModel:
    def __init__(self):
        self.db = duckdb.connect('datastore/normalised/alphalens.duckdb')

# RIGHT — model depends on abstract interface
class Signal5dModel:
    def __init__(self, datastore: IDataStore):
        self.datastore = datastore
```

---

## Part 6: Public Library Usage

### Libraries Used vs Custom Code

| Need | Public Library | Version | Custom Code? | Justification |
|------|---------------|---------|:------------:|---------------|
| Gradient boosting | lightgbm, catboost, xgboost | ≥4.5, ≥1.2, ≥3.0 | No | Industry standard |
| Regime detection | hmmlearn | ≥0.3.2 | No | Standard HMM |
| Conformal prediction | mapie | ≥1.3.0 | No | Only mature Python conformal library |
| HPO | optuna | ≥4.7 | No | Best Bayesian TPE implementation |
| Class imbalance | imbalanced-learn | ≥0.12 | No | Standard SMOTE |
| Explainability | shap | ≥0.45 | No | De facto standard |
| Triple-barrier | AlphaLens native implementation | N/A | **Yes** | SPEC-MODEL-002 behavior implemented directly; avoids unavailable/non-open `mlfinlab` dependency |
| Survival analysis | lifelines, scikit-survival | ≥0.28, ≥0.23 | No | Standard implementations |
| Technical indicators | ta-lib | ≥0.6.8 | No | Fastest TA computation |
| Changepoint detection | ruptures | ≥1.1.9 | No | Reference implementation (PELT) |
| Clustering | hdbscan | ≥0.8.38 | No | Auto-k clustering |
| Drift detection | river | ≥0.21 | No | ADWIN reference implementation |
| Analytical DB | duckdb | ≥1.2.0 | No | Embedded columnar DB |
| Web API | fastapi | ≥0.115 | No | Fastest Python ASGI framework |
| Scheduler | APScheduler | ≥3.11 | No | Persistent job scheduling |
| Parquet I/O | pyarrow | ≥12.0 | No | Apache standard |
| Data manipulation | pandas | ≥2.2 | No | Standard |
| Deep learning | torch, pytorch-forecasting | ≥2.4, ≥1.1 | No | Industry standard |
| SSM | mamba-ssm | ≥2.0 | No | Reference implementation |
| RL | stable-baselines3 | latest | No | Standard RL library |
| PSI computation | — | — | **Yes** | ~50 lines; no mature public library for PSI |
| Checkpoint engine | — | — | **Yes** | ~200 lines; specific to our pipeline step model |
| Observability | — | — | **Yes** | ~150 lines; lighter than OpenTelemetry for our needs |
| Gap detector | — | — | **Yes** | ~100 lines; specific to NSE trading calendar |
| Feature catalog | — | — | **Yes** | ~50 lines; JSON schema validation |
| Position sizer | — | — | **Yes** | ~30 lines; rules-based, no library needed |

**Custom code total: ~580 lines across 6 utilities.** Everything else uses public libraries.

**Note on OpenTelemetry:** We chose custom observability (~150 lines) over OpenTelemetry because OTel requires a collector service, adds significant dependency weight, and is designed for distributed systems. Our single-laptop architecture doesn't need distributed tracing. If the system moves to cloud with multiple services, migrate to OTel at that point.
