# Price Continuity Remediation Strategy

**Date:** 2026-09-06  
**Scope:** 3,886 price gaps (>= 5%) across 1,684 tickers  
**Status:** Inventory complete; remediation plan ready

---

## Executive Summary

A comprehensive scan of corporate-action price discontinuities revealed **3,886 gaps >= 5%** across the equity universe. These gaps fall into three severity bands:

| Severity | Count | Impact | Remediation |
|----------|-------|--------|-------------|
| Critical (>= 50%) | 66 | Major backtest distortions | Must fix before backtesting |
| High (20-50%) | 147 | Significant impact | Fix in Phase 1 |
| Moderate (5-20%) | 3,673 | Borderline; mixed real/artifacts | Fix in Phase 2-3 |

**Key Insight:** Our prior 50-ticker investigation fixed ~1.3% of the problem. The 3,886 findings represent the complete inventory needed to understand data quality risk across all strategies (R9, R11, R7, R8, R12, etc.).

---

## Gap Distribution by Severity

```
Gaps >= 50%: 66 (most pre-2010, legacy data; but 5 are recent post-2020)
  Examples: CGPOWER (425.9%), JAYBARMARU (418.6%), GAEL (307%)
  Cause: Likely multi-leg corporate actions (split+bonus, demergers)
  Action: Investigate missing registry entries; some may be delisting artifacts

Gaps 20-50%: 147 (mix of historical and 2010-2020)
  Examples: INFY (98.7%), UNITECH (98.4%), ZYDUSLIFE (103.7%)
  Cause: Unlogged or partially-logged corporate actions
  Action: Empirical factor computation (pre/post OHLCV median)

Gaps 5-20%: 3,673 (mostly 2015+; recent activity ongoing)
  Examples: Normal 5-7% gaps on DIVIDEND dates
  Cause: Expected behavior for DIVIDENDs not backward-adjusted
  Action: Assess whether to fix all or threshold higher (10%)
```

---

## Top 20 Tickers by Remediation Priority

(Full list: `docs/findings_export_2026_09_06/remediation_priority_queue.csv`)

| Ticker | Max Gap | # Gaps | Avg Gap | Priority Score | Status |
|--------|---------|--------|---------|-----------------|--------|
| CGPOWER | 425.9% | 5 | 100.3% | 648.85 | ⏳ Pre-2010 (legacy) |
| JAYBARMARU | 418.6% | 5 | 173.8% | 637.90 | ⏳ Pre-2010 (legacy) |
| GAEL | 307.0% | 4 | 83.6% | 468.50 | ⏳ Pre-2010 (legacy) |
| ASHAPURMIN | 104.7% | 3 | 40.1% | 163.05 | ⚠️ **Already fixed (1 event), but has 3 gaps!** |
| NCC | 102.4% | 3 | 38.5% | 159.60 | ⏳ Pre-2010 (legacy) |
| ZYDUSLIFE | 103.7% | 2 | 54.5% | 159.55 | ⏳ Pre-2010 (legacy) |
| MAHSEAMLES | 103.1% | 2 | 54.2% | 158.65 | ⏳ Pre-2010 (legacy) |
| AURUM | 98.8% | 3 | 43.2% | 154.20 | ⚠️ 2020-2022 (recent) |
| INFY | 98.7% | 3 | 37.3% | 154.05 | ✅ Tier-1 stock |
| IVC | — | 14 | 28.5% | ~142.00 | ⚠️ Multiple gaps |

**Notable Finding:** ASHAPURMIN is already in our "fixed" list (empirical Demerger correction from 2008-09-15), yet shows 3 gaps (2006, 2007, 2008). This confirms:
- **Some tickers have multiple corporate events on different dates**
- **Our 50-ticker fix only addressed the most recent event per ticker**
- **Comprehensive remediation requires handling all events chronologically**

---

## Remediation Roadmap (3-Phase)

### Phase 1: Critical Gaps (High Priority, Short Timeline)
**Target:** Top-100 tickers by remediation score  
**Expected Coverage:** ~1,500-1,800 gaps (40-45% of total)  
**Effort:** 2-3 days (parallel batch jobs)  
**Approach:**
1. Export top-100 from `remediation_priority_queue.csv`
2. Run `compute_empirical_corrections.py --input top_100_tickers.csv`
3. Manually verify top-20 factors (>50% gaps require scrutiny)
4. Run `apply_empirical_corrections.py --apply --min-status ok`
5. Re-run affected backtests (especially R11, R9, R7)

**Dependencies:**
- Fyers token (already available per user)
- 3-4 hours compute time for empirical factor computation
- DB write lock (currently available)

**Expected Outcome:**
- 40-50% of gaps eliminated
- ASHAPURMIN, INFY, ZYDUSLIFE, AURUM fully corrected
- R11 backtest max DD expected to drop from 95%+ to 30-40%

### Phase 2: Remaining High-Impact (Medium Priority, 1-2 weeks)
**Target:** Tickers with max_gap_pct 20-50% or 5+ gaps  
**Expected Coverage:** ~1,500-2,000 gaps (40-50% of total)  
**Effort:** 1-2 weeks (includes manual investigation)  
**Approach:**
1. Triage by action type (DIVIDENDs vs SPLITs vs Demergers)
2. Cross-reference corporate_actions table for missing entries
3. Investigate Fyers segment migration (for pre-2017 data)
4. Empirical corrections for genuine events
5. Exclude known artifacts (e.g., 2007-01-02 legacy data joins)

**Expected Outcome:**
- 80-85% of major gaps eliminated
- Confidence in R9, R7, R8, R12 strategy backtests

### Phase 3: Ongoing Automation (Continuous)
**Target:** Daily integrity check + weekly triage  
**Expected Coverage:** Prevent future A2ZINFRA incidents  
**Effort:** <1 hour/week  
**Approach:**
1. Daily run of `check_corporate_action_continuity(5%)`
2. Findings inserted to `data_integrity_findings` table
3. Weekly review of new findings (especially gaps >20%)
4. Monthly batch-apply of new empirical corrections

**Expected Outcome:**
- No strategy will lose >$1M due to unaccounted gaps
- Data quality score trending upward
- Backtests trusted for paper trading

---

## Implementation Checklist

### Pre-Remediation
- [ ] Review top-20 tickers by priority score (CGPOWER, JAYBARMARU, GAEL, ...)
- [ ] Determine acceptable gap threshold for DIVIDENDs (currently 5%, consider raising to 10%)
- [ ] Allocate DB write lock for Phase 1 (~4 hours continuous)
- [ ] Verify Fyers token freshness

### Phase 1 (Ready to execute)
- [ ] Extract top-100 from `remediation_priority_queue.csv`
- [ ] Run `compute_empirical_corrections.py --input top_100_tickers.csv --output empirical_v4.csv`
- [ ] Review & approve top-20 factors in output CSV
- [ ] Run `apply_empirical_corrections.py --input empirical_v4.csv --apply`
- [ ] Verify audit trail in `ohlcv_ca_audit` table
- [ ] Re-run R11 M09/M02 backtests; compare metrics before/after

### Phase 2 (TBD after Phase 1)
- [ ] Analyze triage results by action type
- [ ] Identify patterns of missing corporate actions
- [ ] Plan registry corrections
- [ ] Schedule Phase 2 empirical batch

### Ongoing
- [ ] Set calendar reminder for weekly findings triage
- [ ] Document any new gap patterns discovered
- [ ] Update backtest exclusions if warranted

---

## Risk & Unknowns

### Known Risks
1. **Pre-2010 Legacy Data:** Many gaps are from 2006-2008 (before Fyers). Empirical factors from that era may be unreliable (low volume, sparse data).
   - **Mitigation:** Validate top-20 factors manually; consider excluding pre-2010 data from backtests.

2. **Delisting/Relisting Artifacts:** Some gaps may represent delisted tickers reborn with same code (e.g., UNITECH variants).
   - **Mitigation:** Cross-check BSE archives; flag for manual review.

3. **Multiple Legs, Partial Registry:** Some tickers have 5+ gaps (IVC, PTL) due to multiple events. Registry may only capture one leg.
   - **Mitigation:** Sort chronologically; compute compound factors.

4. **DIVIDEND Flood:** 3,673 gaps in 5-20% band are mostly DIVIDENDs (backward-adjustment skipped). Fixing all may be excessive.
   - **Mitigation:** Assess empirically if DIVIDEND adjustments impact backtest alpha; threshold decision required.

### Unknowns
1. **Is 5% threshold sustainable?** Daily check will generate ~10-20 findings/week. Ops workload unknown.
   - **Mitigation:** Monitor first month; adjust to 10% if unmanageable.

2. **How many Phase 1 fixes will actually stick?** Some empirical factors may be derived from stale/incorrect Fyers snapshots.
   - **Mitigation:** Re-validate post-apply; spot-check against NSE archives (2020+).

3. **Will fixing gaps help or hurt backtests?** Some strategies may have overfit to the data artifacts.
   - **Mitigation:** Expect some metrics to worsen; re-tune models post-fix.

---

## Files & References

**Inventory & Export:**
- `docs/findings_export_2026_09_06/findings_by_severity.csv` — All 3,886 gaps, ranked by size
- `docs/findings_export_2026_09_06/remediation_priority_queue.csv` — Top-100 by remediation score
- `docs/findings_export_2026_09_06/summary_statistics.csv` — Aggregate metrics

**Scripts Ready:**
- `scripts/export_price_continuity_findings.py` — Regenerate inventory anytime
- `scripts/compute_empirical_corrections.py` — Compute factors (existing; tested)
- `scripts/apply_empirical_corrections.py` — Apply fixes (existing; tested)

**Documentation:**
- `docs/DATA_INTEGRITY_ISSUES_2026_09.md` — Prior 50-ticker investigation
- `docs/R11_BACKTEST_INVESTIGATION_2026_09_06.md` — A2ZINFRA root-cause analysis
- `datastore/integrity/checks.py` — Automated daily check (5% threshold)

---

## Decision Points for User

1. **Phase 1 scope:** Proceed with top-100, or review priority queue first?
2. **DIVIDEND threshold:** Fix all 3,673+ DIVIDEND gaps, or increase threshold to 10%?
3. **DB lock allocation:** How long can you commit to Phase 1 (~4 hours)?
4. **Pre-2010 data:** Include legacy (2006-2010) in remediation, or focus on 2010+ only?

---

## Expected Impact on Backtests

**R11 (Reversal) — HIGHEST RISK:**
- Current: -96.6% max DD (M09 5d), -95.5% max DD (M02 21d)
- Expected post-fix: -30% to -40% max DD (normalized)
- Reason: R11 buys oversold stocks; artificial gaps look like opportunities

**R9 (Momentum) — MODERATE RISK:**
- Current: Likely inflated returns due to riding artificial bounces
- Expected: 10-20% reduction in CAGR post-fix

**R7 (Crash-Aware), R8 (Vol-Scaling) — LOW RISK:**
- These follow momentum signals; depend less on gap timing
- Expected: <5% impact

**All Strategies — COMMON BENEFIT:**
- Increased confidence in backtest results
- Safer paper trading deployment
- Clearer signal of true alpha vs. data artifacts

---

## Success Criteria

✅ **Phase 1 Complete When:**
- Top-100 tickers have empirical factors computed
- All factors validated (spot-checked against NSE/Fyers)
- Fixes applied & audit trail in place
- R11 M09 backtest re-run shows <50% max DD
- All 50 prior fixes still valid (no regressions)

✅ **Full Remediation Complete When:**
- 2,500+ gaps (>70% of total) resolved
- Daily integrity check running with <10 findings/week
- No strategy shows unaccounted >50% gap in recent data
- Backtest results stable under daily re-runs

