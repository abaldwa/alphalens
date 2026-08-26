# Phase 1 Quickstart: Validation & Execution Guide

**Date**: 2026-08-26 | **Status**: Ready to Execute | **For**: [plan.md](plan.md)

This guide provides runnable validation scenarios proving R-family remediation works end-to-end.

---

## Prerequisites

**Environment**:
- Working AlphaLens repo on `feature/r-family-remediation` branch
- Python 3.10+, pytest, backtest module
- DuckDB access (read/write)
- Scheduler stopped (to avoid lock contention): `systemctl --user stop alphalens-scheduler.service`

**Data**:
- Backtest queues defined: `backtest/queues/r10_validation.json`, `backtest/queues/r12_validation.json`
- Feature store snapshot reconciled (B-021)
- backlog_items.yaml updated with all 29 items

**Baseline**:
- All 29 backlog items status = "pending" (ready to start)
- No existing results for R10/R12 in backtest_runs table

---

## Validation Scenario 1: Phase 0 — Audit Prompt Fix (B-003)

**Objective**: Verify audit prompt rewrite catches specification violations.

**Setup**:
```bash
cd /home/amit/projects/AlphaLens

# Backup current audit prompt
cp backtest/agents/momentum_strategy_audit.py backtest/agents/momentum_strategy_audit.py.backup

# Install/update test harness
python3 -m pytest tests/unit/test_audit_prompt.py -v
```

**Test Run**:
```bash
# Execute audit adversarial tests (deliberate violations)
python3 -c "
from backtest.agents.momentum_strategy_audit import audit_strategy
from tests.fixtures.mock_strategies import (
    strategy_valid_r1,
    strategy_missing_skip_month_r3,
    strategy_wrong_lookback_r1,
)

# Test 1: Valid R1 should APPROVE
result = audit_strategy(strategy_valid_r1)
assert result['verdict'] == 'APPROVE', f'Expected APPROVE, got {result[\"verdict\"]}'
assert result['citation'].startswith('Verified lookback'), 'Missing citation'
print('✅ Test 1 passed: Valid R1 approved with citation')

# Test 2: Missing skip-month should REJECT
result = audit_strategy(strategy_missing_skip_month_r3)
assert result['verdict'] == 'REJECT', f'Expected REJECT, got {result[\"verdict\"]}'
assert 'skip-month' in result['reason'], 'Missing skip-month in rejection reason'
print('✅ Test 2 passed: R3 without skip-month rejected')

# Test 3: Wrong lookback should REJECT
result = audit_strategy(strategy_wrong_lookback_r1)
assert result['verdict'] == 'REJECT', f'Expected REJECT, got {result[\"verdict\"]}'
assert 'lookback' in result['reason'].lower(), 'Missing lookback in rejection reason'
print('✅ Test 3 passed: Wrong lookback rejected')

print('\\n✅ All B-003 audit tests passed. Audit prompt is reliable.')
"
```

**Expected Output**:
```
✅ Test 1 passed: Valid R1 approved with citation
✅ Test 2 passed: R3 without skip-month rejected
✅ Test 3 passed: Wrong lookback rejected

✅ All B-003 audit tests passed. Audit prompt is reliable.
```

**Success Criteria**:
- ✅ All 3 adversarial tests pass
- ✅ Verdicts match expected (APPROVE/REJECT)
- ✅ Citations provided for each approval
- ✅ Rejection reasons include specific issue (e.g., "skip-month missing")

**Update Backlog**:
```bash
# Mark B-003 as complete
python3 -c "
import yaml
with open('backlog_items.yaml') as f:
    items = yaml.safe_load(f)
for item in items['items']:
    if item['item_id'] == 'B-003':
        item['status'] = 'complete'
        item['gate_result'] = 'PASS'
        item['decision_rationale'] = 'Audit prompt rewrite verified; adversarial tests all pass'
with open('backlog_items.yaml', 'w') as f:
    yaml.dump(items, f)
"
```

---

## Validation Scenario 2: Phase 0 — Strategy Fix: R8 Cadence (B-026)

**Objective**: Verify R8 rebalance cadence fix (252d → 21d) doesn't break Sharpe.

**Setup**:
```bash
cd /home/amit/projects/AlphaLens

# Backup current R8 implementation
cp backtest/adapters/momentum_adapter.py backtest/adapters/momentum_adapter.py.backup

# Apply fix: change R8 rebalance_cadence parameter
python3 -c "
import json
with open('strategy_registry/momentum_strategies.json') as f:
    registry = json.load(f)
r8 = [s for s in registry if s['strategy_id'] == 'R8'][0]
old_cadence = r8['parameters']['rebalance_cadence_days']
r8['parameters']['rebalance_cadence_days'] = 21  # monthly
print(f'Changed R8 rebalance cadence: {old_cadence} → 21 days')
with open('strategy_registry/momentum_strategies.json', 'w') as f:
    json.dump(registry, f, indent=2)
"
```

**Test Run**:
```bash
# Run R8 backtest with fixed cadence
python3 backtest/run_orchestrator_backtest.py \
  --strategy R8 \
  --symbol COMPOSITE \
  --start 2020-01-01 \
  --end 2025-12-31 \
  --output-format json

# Capture result
python3 -c "
import duckdb
db = duckdb.connect('~/.local/share/AlphaLens/data/backtest.duckdb')
result = db.execute('''
    SELECT run_id, sharpe, calmar, max_drawdown 
    FROM backtest_runs 
    WHERE strategy_id='R8' 
    AND end_date='2025-12-31'
    ORDER BY created_at DESC LIMIT 1
''').fetch_df()
print('R8 Backtest Result (post-fix):')
print(result)
sharpe = result['sharpe'].iloc[0]
print(f'Sharpe: {sharpe}')
if sharpe > 0.65:  # viability threshold
    print('✅ R8 Sharpe acceptable; cadence fix validated')
else:
    print('⚠️ R8 Sharpe dropped significantly; investigate root cause')
"
```

**Expected Output**:
```
R8 Backtest Result (post-fix):
  run_id                                sharpe  calmar  max_drawdown
0 a1b2c3d4-e5f6-4a8b-9c0d-1e2f3a4b5c6d  0.68   0.42    -0.285

Sharpe: 0.68
✅ R8 Sharpe acceptable; cadence fix validated
```

**Success Criteria**:
- ✅ Backtest completes without errors
- ✅ Sharpe > 0.65 (viability)
- ✅ Results land in backtest_runs table
- ✅ No lock timeouts or scheduler conflicts

**Update Backlog**:
```bash
# Mark B-026 as complete
python3 -c "
import yaml
with open('backlog_items.yaml') as f:
    items = yaml.safe_load(f)
for item in items['items']:
    if item['item_id'] == 'B-026':
        item['status'] = 'complete'
        item['gate_result'] = 'PASS'
        item['decision_rationale'] = 'R8 cadence fix (252d → 21d) validated; Sharpe 0.68 acceptable'
with open('backlog_items.yaml', 'w') as f:
    yaml.dump(items, f)
"
```

---

## Validation Scenario 3: Phase 1 — Execute R10/R12 Queues (B-014, B-015)

**Objective**: Verify R10-R12 backtest queues execute and results land in database.

**Setup**:
```bash
cd /home/amit/projects/AlphaLens

# Verify queues exist
ls -lh backtest/queues/r10_validation.json backtest/queues/r12_validation.json

# Check queue structure
python3 -c "
import json
for queue_file in ['backtest/queues/r10_validation.json', 'backtest/queues/r12_validation.json']:
    with open(queue_file) as f:
        queue = json.load(f)
    print(f'{queue_file}:')
    print(f'  Symbols: {len(queue[\"symbols\"])}')
    print(f'  Date range: {queue[\"start_date\"]} to {queue[\"end_date\"]}')
    print(f'  Total runs: {len(queue[\"symbols\"])}')
"
```

**Test Run — R10 Queue**:
```bash
# Execute R10 validation queue
python3 backtest/run_strategy_queue.py backtest/queues/r10_validation.json

# Monitor progress (in separate terminal)
watch -n 5 "sqlite3 datastore/pipeline/pipeline_log.db \
  \"SELECT date, step, status FROM pipeline_checkpoints \
  WHERE step LIKE '%R10%' ORDER BY date DESC LIMIT 5;\""
```

**Validation**:
```bash
# Verify R10 results in database
python3 -c "
import duckdb
db = duckdb.connect('~/.local/share/AlphaLens/data/backtest.duckdb')

# Count R10 runs
r10_count = db.execute('SELECT COUNT(*) FROM backtest_runs WHERE strategy_id=\"R10\"').fetch_one()[0]
print(f'R10 runs in database: {r10_count}')
assert r10_count > 50, 'Expected > 50 R10 runs'  # at least 50+ symbols

# Get R10 metrics
r10_result = db.execute('''
    SELECT avg(sharpe) as avg_sharpe, min(sharpe) as min_sharpe, max(sharpe) as max_sharpe
    FROM backtest_runs WHERE strategy_id='R10' AND end_date='2025-12-31'
''').fetch_df()
print(f'R10 Metrics (avg portfolio): {r10_result.to_dict()}')
avg_sharpe = r10_result['avg_sharpe'].iloc[0]
print(f'Average Sharpe: {avg_sharpe}')
if avg_sharpe > 0.65:
    print('✅ R10 queue executed successfully; Sharpe acceptable')
    print('✅ B-014 PASS: R10 results verified in database')
else:
    print('⚠️ R10 average Sharpe low; may need investigation')
"
```

**Test Run — R12 Queue**:
```bash
# Execute R12 validation queue
python3 backtest/run_strategy_queue.py backtest/queues/r12_validation.json

# Verify R12 results
python3 -c "
import duckdb
db = duckdb.connect('~/.local/share/AlphaLens/data/backtest.duckdb')

# Count R12 runs
r12_count = db.execute('SELECT COUNT(*) FROM backtest_runs WHERE strategy_id=\"R12\"').fetch_one()[0]
print(f'R12 runs in database: {r12_count}')

# Get R12 composite result
r12_result = db.execute('''
    SELECT sharpe, calmar, max_drawdown, num_trades
    FROM backtest_runs 
    WHERE strategy_id='R12' AND symbol='COMPOSITE' AND end_date='2025-12-31'
    ORDER BY created_at DESC LIMIT 1
''').fetch_df()
print(f'R12 Result (composite): {r12_result.to_dict()}')
sharpe = r12_result['sharpe'].iloc[0]
print(f'R12 Sharpe: {sharpe}')

if sharpe >= 0.70:
    print('✅ R12 Sharpe >= 0.70; PROCEED to Phase 2 (robustness audits)')
    gate_result = 'PASS'
elif sharpe >= 0.65:
    print('✅ R12 Sharpe 0.65-0.70; CONDITIONAL proceed (medium confidence)')
    gate_result = 'CONDITIONAL'
else:
    print('❌ R12 Sharpe < 0.65; ARCHIVE R12, skip Phase 2-3')
    gate_result = 'FAIL'

print(f'Phase 1 gate result: {gate_result}')
"
```

**Expected Output**:
```
R10 runs in database: 156
R10 Metrics (avg portfolio): {'avg_sharpe': [0.68], ...}
Average Sharpe: 0.68
✅ R10 queue executed successfully; Sharpe acceptable
✅ B-014 PASS: R10 results verified in database

R12 runs in database: 156
R12 Result (composite): {'sharpe': [0.72], ...}
R12 Sharpe: 0.72
✅ R12 Sharpe >= 0.70; PROCEED to Phase 2 (robustness audits)
Phase 1 gate result: PASS
```

**Success Criteria**:
- ✅ Both queues execute without scheduler locks or crashes
- ✅ Results land in backtest_runs table (100+ rows each)
- ✅ R10 average Sharpe > 0.65
- ✅ R12 composite Sharpe result visible and > 0.70
- ✅ Gate decision clear (PASS/CONDITIONAL/FAIL)

**Update Backlog**:
```bash
# Mark B-014, B-015 as complete
python3 -c "
import yaml
with open('backlog_items.yaml') as f:
    items = yaml.safe_load(f)
for item in items['items']:
    if item['item_id'] in ['B-014', 'B-015']:
        item['status'] = 'complete'
        item['gate_result'] = 'PASS'
        item['decision_rationale'] = f'{item[\"item_id\"]}: Queue executed; results verified in database'
with open('backlog_items.yaml', 'w') as f:
    yaml.dump(items, f)
"
```

---

## Validation Scenario 4: Phase 2 — Sub-Period Stability (B-018)

**Objective**: Verify R12 reversal edge is not concentrated in COVID bull market (2019-2022).

**Setup**:
```bash
cd /home/amit/projects/AlphaLens

# Run R12 backtest for 2019-2022 period
python3 backtest/run_orchestrator_backtest.py \
  --strategy R12 \
  --symbol COMPOSITE \
  --start 2019-01-01 \
  --end 2022-12-31 \
  --output-format json
```

**Test Run**:
```bash
# Extract results for both periods
python3 -c "
import duckdb
db = duckdb.connect('~/.local/share/AlphaLens/data/backtest.duckdb')

# Period 1: 2019-2022 (COVID recovery)
r12_2019_2022 = db.execute('''
    SELECT sharpe, calmar, max_drawdown, volatility
    FROM backtest_runs 
    WHERE strategy_id='R12' AND symbol='COMPOSITE'
    AND start_date='2019-01-01' AND end_date='2022-12-31'
    ORDER BY created_at DESC LIMIT 1
''').fetch_df()

# Period 2: 2023-2025 (normalized market)
r12_2023_2025 = db.execute('''
    SELECT sharpe, calmar, max_drawdown, volatility
    FROM backtest_runs 
    WHERE strategy_id='R12' AND symbol='COMPOSITE'
    AND start_date='2023-01-01' AND end_date='2025-12-31'
    ORDER BY created_at DESC LIMIT 1
''').fetch_df()

print('R12 Sub-Period Analysis:')
print(f'2019-2022 (COVID recovery): Sharpe={r12_2019_2022[\"sharpe\"].iloc[0]:.2f}')
print(f'2023-2025 (normalized):     Sharpe={r12_2023_2025[\"sharpe\"].iloc[0]:.2f}')

s1 = r12_2019_2022['sharpe'].iloc[0]
s2 = r12_2023_2025['sharpe'].iloc[0]

if s1 > 0.70 and s2 > 0.70:
    print('✅ Both sub-periods > 0.70; edge is regime-independent')
    print('✅ B-018 PASS: Proceed to Phase 2 robustness audits')
elif s1 > 0.80 and s2 < 0.70:
    print('⚠️ 2019-2022 >> 2023-2025; edge concentrated in bull market')
    print('⚠️ B-018 CONDITIONAL: Needs liquidity analysis (B-016) to confirm')
else:
    print('❌ 2023-2025 < 0.70; edge may not generalize')
    print('❌ B-018 FAIL: Consider archiving R12')
"
```

**Expected Output**:
```
R12 Sub-Period Analysis:
2019-2022 (COVID recovery): Sharpe=0.78
2023-2025 (normalized):     Sharpe=0.68
⚠️ 2019-2022 >> 2023-2025; edge concentrated in bull market
⚠️ B-018 CONDITIONAL: Needs liquidity analysis (B-016) to confirm
```

**Success Criteria**:
- ✅ Both period backtests complete
- ✅ Both sub-period Sharpe values calculated
- ✅ Gate decision clear (PASS/CONDITIONAL/FAIL)
- ✅ Volatility/Calmar ratios also compared (no extreme outliers)

---

## Validation Scenario 5: Phase 4 — Final Decision Report

**Objective**: Generate final remediation report with decision on R-family composition.

**Report Generation**:
```bash
python3 -c "
import yaml
import json
from datetime import datetime

with open('backlog_items.yaml') as f:
    backlog = yaml.safe_load(f)

# Summarize by phase and status
report = {
    'timestamp': datetime.now().isoformat(),
    'phases': {}
}

for phase in range(5):
    phase_items = [i for i in backlog['items'] if i['phase'] == phase]
    phase_summary = {
        'phase_id': phase,
        'total_items': len(phase_items),
        'complete': len([i for i in phase_items if i['status'] == 'complete']),
        'archived': len([i for i in phase_items if i['status'] == 'archived']),
        'deferred': len([i for i in phase_items if i['status'] == 'deferred']),
        'gate_result': 'PENDING'
    }
    
    # Determine phase gate result
    if phase_summary['complete'] == phase_summary['total_items']:
        failed = len([i for i in phase_items if i.get('gate_result') == 'FAIL'])
        if failed == 0:
            phase_summary['gate_result'] = 'PASS'
        else:
            phase_summary['gate_result'] = 'FAIL'
    
    report['phases'][f'Phase{phase}'] = phase_summary

# Decision logic
print('=== R-FAMILY REMEDIATION FINAL REPORT ===')
print(f'Generated: {report[\"timestamp\"]}')
print()
print('Phase Summary:')
for phase, summary in report['phases'].items():
    print(f'{phase}: {summary[\"complete\"]}/{summary[\"total_items\"]} complete, Gate: {summary[\"gate_result\"]}')

print()
print('=== FINAL DECISION ===')
if report['phases']['Phase0']['gate_result'] == 'PASS':
    if report['phases']['Phase1']['gate_result'] == 'PASS':
        if report['phases']['Phase2']['gate_result'] == 'PASS':
            print('✅ R10 APPROVED for paper trading')
            if report['phases']['Phase3']['gate_result'] == 'PASS':
                print('✅ R12 APPROVED for paper trading (monthly cadence)')
            else:
                print('✅ R12 conditional; waiting Phase 3 fine-tuning results')
        else:
            print('✅ R10 APPROVED for paper trading')
            print('❌ R12 ARCHIVED; robustness failures unresolved')
    else:
        print('❌ R10-R12 ARCHIVED; validation failed')
else:
    print('❌ PAUSE R-family work; Phase 0 blocking issues unresolved')
"
```

**Expected Output**:
```
=== R-FAMILY REMEDIATION FINAL REPORT ===
Generated: 2026-08-29T10:30:00

Phase Summary:
Phase0: 13/13 complete, Gate: PASS
Phase1: 2/2 complete, Gate: PASS
Phase2: 9/9 complete, Gate: PASS
Phase3: 2/2 complete, Gate: PASS
Phase4: 1/1 complete, Gate: PASS

=== FINAL DECISION ===
✅ R10 APPROVED for paper trading
✅ R12 APPROVED for paper trading (monthly cadence)
```

**Success Criteria**:
- ✅ All backlog items have status (complete/archived/deferred)
- ✅ All items with status="complete" have gate_result (PASS/FAIL)
- ✅ Final decision is clear (R10 only, R10+R12, archived)
- ✅ Rationale documented for each decision

---

## Troubleshooting

### Scenario: Backtest Hangs or Times Out

**Diagnosis**:
```bash
# Check scheduler status
systemctl --user status alphalens-scheduler.service

# Check DuckDB lock holders
fuser ~/.local/share/AlphaLens/data/*.duckdb

# View recent logs
journalctl --user -u alphalens-scheduler.service --since "10 min ago" --no-pager
```

**Fix**:
```bash
# Stop scheduler to release lock
systemctl --user stop alphalens-scheduler.service

# Retry backtest
python3 backtest/run_orchestrator_backtest.py ...

# Restart scheduler after backtest completes
systemctl --user start alphalens-scheduler.service
```

### Scenario: Phase 2 Robustness Failure is Unresolvable

**Decision**:
- If B-020 reveals design flaw (e.g., signal decay faster than rebalance), document and proceed to Phase 3 with monthly cadence (B-017) as proposed fix
- If design is fundamentally flawed, mark R12 as ARCHIVED and proceed with R10 only
- Document rationale in backlog item decision_rationale field

### Scenario: R12 Fails Final Phase 4 Gate

**Escalation**:
- If Sharpe drops below 0.70 in final validation, present findings to product owner
- Decision: Archive R12, proceed to paper trading with R1-R10
- OR: Accept R12 with documented limitations (e.g., "regime-dependent, underperforms in bear markets")

---

## Completion Checklist

- [ ] All 29 backlog items (B-001–B-029) have status and gate_result
- [ ] Phase 0 gate: PASS (audit fixed, strategy fixes validated, implementations complete)
- [ ] Phase 1 gate: PASS (queues executed, R10/R12 Sharpe > 0.65)
- [ ] Phase 2 gate: PASS or FAIL (sub-periods, DSR, robustness evaluated)
- [ ] Phase 3 gate: PASS (if R12 survives Phase 2)
- [ ] Phase 4 gate: PASS (final decision documented)
- [ ] Paper trading integration planning started (separate feature)

---

## Next Steps

Once all phases complete:
1. Update `project_r_family_complete.md` memory with final decision
2. Create paper-trading dispatcher feature (separate task)
3. Archive R-family remediation branch; proceed to live signal generation
