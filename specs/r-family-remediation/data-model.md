# Phase 1 Design: Data Model & Entities

**Date**: 2026-08-26 | **Status**: Design | **For**: [plan.md](plan.md)

---

## Core Entities

### 1. RemediationItem (B-001 through B-029)

Represents a single remediation task from the comprehensive R-family audit.

**Fields**:
- `item_id` (string): Unique identifier (e.g., "B-003", "B-026")
- `title` (string): Short description (e.g., "Fix momentum-strategy-audit prompt")
- `description` (string): Detailed problem statement
- `category` (enum): One of "audit", "strategy_fix", "strategy_implementation", "infrastructure", "validation"
- `severity` (enum): "CRITICAL", "HIGH", "MEDIUM", "LOW"
- `status` (enum): One of "blocked", "pending", "in_progress", "complete", "deferred", "archived"
- `phase` (integer): 0–4 (which phase executes this item)
- `estimate_hours` (float): Expected effort
- `depends_on` (array[string]): List of blocking item IDs (e.g., B-026 depends_on B-003)
- `gate_condition` (string): Decision criterion (e.g., "Sharpe unchanged or improved", "DSR > 0.5")
- `gate_result` (enum): "PASS", "FAIL", "PENDING" (filled after execution)
- `decision_rationale` (string): Why item passed, failed, or was deferred

**Relationships**:
- `depends_on` → RemediationItem (many-to-many)
- `phase` → Phase (foreign key)
- `status` → ItemStatus (state machine)

**Example**:
```yaml
item_id: "B-003"
title: "Fix momentum-strategy-audit prompt"
category: "audit"
severity: "CRITICAL"
status: "in_progress"
phase: 0
estimate_hours: 2.5
depends_on: []
gate_condition: "Audit agent correctly identifies J&T deviations; test with deliberate violations"
gate_result: "PENDING"
```

---

### 2. RemediationPhase

Represents a phase of remediation execution (0–4).

**Fields**:
- `phase_id` (integer): 0–4
- `name` (string): "Foundation", "Validation", "Robustness", "Fine-Tuning", "Decision"
- `start_date` (date): Planned start
- `end_date` (date): Planned end
- `gate_condition` (string): Phase-level success criterion
- `gate_result` (enum): "PASS", "FAIL", "PENDING"
- `decision` (string): Outcome (e.g., "Proceed to Phase 1", "ARCHIVE R12", "Pause all work")
- `items` (array[RemediationItem]): Items in this phase

**Relationships**:
- `items` → RemediationItem (one-to-many)

**Example**:
```yaml
phase_id: 0
name: "Foundation"
start_date: "2026-08-26"
end_date: "2026-08-27"
gate_condition: "B-003 fixed and validated; B-026–B-029 don't break Sharpe; B-023–B-024 complete"
gate_result: "PENDING"
decision: ""
```

---

### 3. StrategyDefinition

Represents a momentum strategy (R1–R12) with specification, parameters, and validation state.

**Fields**:
- `strategy_id` (string): "R1"–"R12"
- `name` (string): e.g., "J&T Core Momentum (3/6/9/12mo)"
- `authors` (array[string]): e.g., ["Jegadeesh", "Titman"]
- `publication_year` (integer): e.g., 1993
- `parameters` (object): Strategy hyperparameters
  - `lookback_days` (array[integer]): e.g., [63, 126, 189, 252]
  - `rebalance_cadence_days` (integer): e.g., 21 (monthly)
  - `universe_filter`: "ADTV >= $X", "sector_neutral", etc.
  - `regime_gate` (string, optional): "EMA_RSI", "VIX_based", etc.
- `validation_status` (enum): "spec_compliant", "needs_fix", "fixed", "archived"
- `audit_finding` (object): Results from B-004–B-010 audits
  - `passes_spec_check` (boolean)
  - `lookback_verified` (boolean)
  - `adtv_floor_verified` (boolean)
  - `cadence_verified` (boolean)
  - `skip_month_verified` (boolean, if applicable)
- `backtest_metrics` (object, filled after Phase 1+)
  - `sharpe` (float)
  - `calmar` (float)
  - `max_drawdown` (float)
  - `fold_stability` (float, walk-forward correlation)
  - `dsr` (float, corrected DSR if Phase 2 runs)
- `sub_period_breakdown` (object, filled after B-018)
  - `sharpe_2019_2022` (float)
  - `sharpe_2023_2025` (float)
- `remediation_items` (array[RemediationItem]): Items addressing this strategy (e.g., B-026 fixes R8)

**Relationships**:
- `remediation_items` → RemediationItem (one-to-many)

**State Transitions**:
```
spec_compliant
  ├─ on audit_fail → needs_fix (B-004–B-010)
  │  └─ on remediation_complete → fixed (B-026–B-029)
  │     └─ on backtest_pass → spec_compliant
  └─ on robustness_fail → archived (B-020 gate fails)
```

**Example**:
```yaml
strategy_id: "R1"
name: "J&T Core Momentum (3/6/9/12mo)"
authors: ["Jegadeesh", "Titman"]
publication_year: 1993
parameters:
  lookback_days: [63, 126, 189, 252]
  rebalance_cadence_days: 21
  universe_filter: "ADTV >= $250K"
validation_status: "spec_compliant"
audit_finding:
  passes_spec_check: true
  lookback_verified: true
  adtv_floor_verified: true
  cadence_verified: true
backtest_metrics:
  sharpe: 0.72
  calmar: 0.45
  max_drawdown: -27.3
```

---

### 4. BacktestRun

Represents a completed backtest execution (existing DuckDB table, referenced here for clarity).

**Fields** (existing schema):
- `run_id` (uuid): Unique identifier
- `strategy_id` (string): e.g., "R12"
- `symbol` (string): Stock ticker (or "COMPOSITE" for multi-symbol)
- `start_date` (date)
- `end_date` (date)
- `sharpe` (float)
- `calmar` (float)
- `max_drawdown` (float)
- `total_return` (float, annual %)
- `num_trades` (integer)
- `created_at` (timestamp)

**Fields** (new, added by B-022 migration):
- `fold_id` (integer, optional): For walk-forward; which fold this run belongs to
- `robustness_check_status` (enum): "pass", "fail", "na"
- `robustness_check_failure_reason` (string, optional): e.g., "fold_stability < 0.5"

**Relationships**:
- `strategy_id` → StrategyDefinition
- `symbol` → Ticker (existing table)

---

### 5. RemediationDecisionPoint

Represents a gate or decision that halts/advances work.

**Fields**:
- `decision_id` (string): Unique identifier (e.g., "GATE_PHASE0", "GATE_B015_SHARPE")
- `phase` (integer): Which phase decision point is in
- `description` (string): What decision is being made
- `condition_type` (enum): "sharpe_threshold", "audit_pass", "robustness_pass", "timeline"
- `threshold_value` (float, optional): e.g., 0.70 for Sharpe
- `comparison_op` (enum, optional): ">", "<", "==", "!=", "all_pass"
- `outcome` (enum): "not_reached", "pass", "fail"
- `decision` (string): What to do next (e.g., "Proceed to Phase 1", "Archive R12")

**Example**:
```yaml
decision_id: "GATE_B015_SHARPE"
phase: 1
description: "Does R12 Sharpe exceed viability threshold?"
condition_type: "sharpe_threshold"
threshold_value: 0.70
comparison_op: ">"
outcome: "not_reached"
decision: "if pass: Phase 2; if fail: Archive R12"
```

---

## State Machines

### RemediationItem Status Flow

```
┌─────────────┐
│  blocked    │ (depends_on not yet complete)
└──────┬──────┘
       │ (depends_on complete)
       ▼
┌─────────────┐
│  pending    │ (ready to start)
└──────┬──────┘
       │ (start work)
       ▼
┌──────────────────┐
│  in_progress     │ (work underway)
└──────┬────────┬──────┘
       │        │
  complete   deferred
       │        │
       ▼        ▼
  [success]  [defer post-Phase-4]

       │
       │ (gate fails, or unfixable)
       ▼
┌─────────────┐
│  archived   │ (R-family strategy removed from consideration)
└─────────────┘
```

### RemediationPhase Status Flow

```
┌─────────────┐
│   PENDING   │ (not started)
└──────┬──────┘
       │ (first item starts)
       ▼
┌──────────────────┐
│  IN_PROGRESS     │
└──────┬────────┬──────┘
       │        │
    PASS      FAIL
       │        │
       ▼        ▼
  [continue]  [BLOCKED]
              (phase gate fails)
              (may retry or escalate)
```

### StrategyDefinition Validation State

```
spec_compliant
    │
    ├─ (audit finds violation) → needs_fix
    │                            │
    │                            ├─ (remediation complete) → fixed
    │                            │                           │
    │                            │                           └─ (backtest verify) → spec_compliant
    │                            │
    │                            └─ (unfixable) → archived
    │
    └─ (robustness check fails) → archived
```

---

## Data Dictionary: Key Metrics

### Sharpe Ratio

**Definition**: Annual risk-adjusted return = (Annual Return % - Risk-Free Rate) / Annual Volatility %

**Validation**:
- Valid range: -1.0 to 2.0 (above 2.0 likely backtest artifact)
- Gate threshold (viability): > 0.65 (minimum), 0.70+ (target)
- Units: Unitless ratio (always, never annualized separately)

**Example**: R1 Sharpe 0.72 means 0.72% risk-adjusted return per 1% volatility.

### Calmar Ratio

**Definition**: Annual Return % / Absolute Max Drawdown %

**Validation**:
- Valid range: 0.0 to 1.0 (positive = profitable, < 0.5 = acceptable, > 0.5 = excellent)
- Inverse relationship to drawdown (higher better)

### Max Drawdown (DD)

**Definition**: Peak-to-trough decline (%) from equity high to low point

**Validation**:
- Negative (e.g., -27.3%)
- Gate (acceptable): > -40% (anything worse is high risk)

### Fold Stability (Walk-Forward Metric)

**Definition**: Correlation of Sharpe across walk-forward folds (e.g., 10 yearly folds)

**Validation**:
- Range: 0.0–1.0
- Gate: > 0.5 (at least 50% correlation across folds)
- Below 0.5 = edge fragile, doesn't generalize

### DSR (Deflated Sharpe Ratio)

**Definition**: Sharpe adjusted for multiple testing bias (accounts for number of strategies tested)

**Formula**: DSR = Sharpe × sqrt(1 - (ln(n_trials) / 252 × Sharpe²))

**Validation**:
- n_trials = count of unique strategy configurations tried (R10-R12 came from 100+ momentum variants)
- Gate: DSR > 0.5 (edge is likely real, not noise)
- DSR < 0.5 = edge is statistically insignificant under multiple-testing correction

### Sub-Period Sharpe (2019-2022 vs 2023-2025)

**Definition**: Sharpe Ratio computed separately for:
- 2019-2022: COVID recovery, high vol, strong reversal
- 2023-2025: Normalized market, lower vol

**Validation**:
- Both > 0.70 Sharpe: Edge is regime-independent (good)
- 2019-2022 >> 2023-2025: Edge concentrated in bull/high-vol (bad, fragile)
- Gate: Both must be > 0.70 to pass Phase 2

---

## Validation Rules

### StrategyDefinition Validation

1. **Specification Compliance** (B-003 audit):
   - All parameters match published paper
   - Code locations cited for each claim
   - No undocumented modifications

2. **Audit Checklist** (B-004–B-010):
   - Lookback periods: [63, 126, 189, 252] or documented alternative
   - Universe filter: ADTV floor documented
   - Rebalance cadence: Matches spec (monthly for momentum, quarterly for reversal, etc.)
   - Skip-month: Implemented if applicable
   - Overlapping portfolios: If 1/K staggered, documented

3. **Backtest Validation** (Phase 1+):
   - Sharpe > 0.65 (viability)
   - Max DD > -40% (acceptable drawdown)
   - Fold stability > 0.5 (generalizable, not overfitted)
   - DSR > 0.5 (edge likely real, not noise)

4. **Sub-Period Validation** (B-018, if applicable):
   - Both 2019-2022 and 2023-2025 Sharpe > 0.70
   - Volatility metrics similar (not 3× higher in one period)

### RemediationItem Acceptance

1. **Status**: Must reach "complete"
2. **Gate**: Must pass gate_condition (or gate_result = "PASS")
3. **Documentation**: decision_rationale filled
4. **No blockers**: All depends_on items must be complete

---

## Relationships & Cardinality

| From | To | Relationship | Cardinality | Meaning |
|------|-----|------|-------|---------|
| RemediationItem | RemediationPhase | belongs_to | N:1 | Each item is in one phase |
| RemediationItem | RemediationItem | depends_on | N:N | Items may have multiple dependencies |
| StrategyDefinition | RemediationItem | addressed_by | 1:N | Each strategy has multiple remediation items |
| StrategyDefinition | BacktestRun | has | 1:N | Each strategy has many backtest runs (one per symbol, per parameter sweep) |
| RemediationPhase | RemediationDecisionPoint | has | 1:N | Each phase has one or more decision points |
| RemediationDecisionPoint | RemediationPhase | gates | N:1 | Each decision point gates a phase |

---

## Example: Phase 0 Dependency Graph

```
B-003 (Fix audit prompt)
  ├─ B-004 (Audit R1) depends_on B-003
  ├─ B-005 (Audit R1 universe) depends_on B-003
  ├─ B-006 (Audit R1-R12 cadence) depends_on B-003
  └─ B-010 (Audit R3 skip-month) depends_on B-003

B-026 (Fix R8 cadence) — no blocker, can start immediately
  └─ triggers B-026 backtest

B-027 (Fix R9 architecture) — no blocker, can start immediately
  └─ triggers B-027 backtest

[Other strategy fixes B-028, B-029, B-025 run in parallel]

B-023 (Implement R4) — no blocker, can start immediately
  └─ triggers R4 new implementation

B-024 (Implement R6) — no blocker, can start immediately
  └─ triggers R6 new implementation

Gate after Phase 0:
  └─ All items complete + gates pass → PROCEED to Phase 1
  └─ Any item fails + gate fails → ROOT_CAUSE + REVERT or REDESIGN
```

---

## Implementation Notes

- All entities stored in `backlog_items.yaml` or DuckDB tables (no new tables required)
- RemediationItem metadata can live in backlog_items.yaml under each item
- StrategyDefinition extends existing `strategy_registry` with audit fields
- BacktestRun uses existing DuckDB schema (B-022 adds columns)
- RemediationDecisionPoint calculated dynamically from gate conditions and backtest results

---

## Next: Phase 1 Contracts

See [contracts/](contracts/) directory for API and data format contracts.
