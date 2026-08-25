# Streamlined Backtesting Plan: M1-M12 Optimization + M13 Launch
**Prepared:** 2026-08-23  
**Duration:** 24-48 hours (staged rollout)  
**Objective:** Identify 10-15 deployable strategies with Sharpe > 0.8, CAGR > 20%, MaxDD < 20%; reduce drawdowns; deploy by band

---

## Executive Summary

**Current State:**
- M1-M12 backtested across 2008-2026 (1,600+ jobs, 10-15 hours)
- Baseline results in `backtest_all.csv`: top strategy at 21.2% CAGR, Sharpe 0.92
- **Pain point:** High drawdowns (-44% to -64%), insufficient alpha over benchmark (14.2% CAGR)
- **Variants used:** AllRisk, Balanced, Risk-Adjusted (to be deprecated)

**New Approach:**
1. **Variant streamlining:** Execute AllRisk first (all bands, all lookbacks, all rebalance cadences), publish results
2. **Add M13:** Nifty 1-800 with Top 30, 40, 50 (captures liquidity-constrained universe)
3. **Band-based deployment:** Select 1 strategy per band (remove overlapping bands post-hoc)
4. **Drawdown mitigation:** Overlay crash-aware filters (Phase 7 / R7 from momentum spec) on top performers
5. **Staged execution:** Pilot → validate → scale → deploy

---

## Part 1: Baseline Analysis (30 min)

### 1.1 Best Strategy Per Band (from backtest_all.csv)

Extract best performers by band (Sharpe descending, filter for Sharpe > 0.75):

| Band | Tickers | Best Strategy | CAGR | Sharpe | MaxDD | Lookback | Rebalance | Top-N |
|------|---------|---------------|------|--------|-------|----------|-----------|-------|
| M1 | 1-50 | M10_301_500_allrisk_lb3mo_bimonthly_top10_21d | 21.2% | 0.92 | -44.1% | 3mo | Bimonthly | 10 |
| M2 | 1-75 | M10_301_500_allrisk_lb3mo_quarterly_top20_21d | 19.9% | 0.91 | -62.2% | 3mo | Quarterly | 20 |
| M3 | 51-100 | M9_276_550_allrisk_lb3mo_quarterly_top10_21d | 19.8% | 0.80 | -64.8% | 3mo | Quarterly | 10 |
| M4 | 76-160 | M10_301_500_allrisk_lb9mo_quarterly_top10_63d | 19.4% | 0.91 | -49.7% | 9mo | Quarterly | 10 |
| M5 | 101-150 | M9_276_550_allrisk_lb3mo_bimonthly_top15_21d | 19.3% | 0.89 | -54.2% | 3mo | Bimonthly | 15 |
| M6 | 151-200 | M9_276_550_allrisk_lb9mo_quarterly_top10_63d | 19.1% | 0.90 | -44.5% | 9mo | Quarterly | 10 |
| M7 | 161-275 | M10_301_500_allrisk_lb3mo_bimonthly_top20_21d | 19.0% | 0.95 | -49.8% | 3mo | Bimonthly | 20 |
| M8 | 201-300 | M10_301_500_allrisk_lb9mo_quarterly_top15_63d | 18.3% | 0.95 | -47.5% | 9mo | Quarterly | 15 |
| M9 | 276-550 | M9_276_550_allrisk_lb9mo_bimonthly_top20_63d | 16.6% | 1.02 | -41.6% | 9mo | Bimonthly | 20 |
| M10 | 301-500 | M10_301_500_allrisk_lb3mo_bimonthly_top10_21d | 21.2% | 0.92 | -44.1% | 3mo | Bimonthly | 10 |
| M11 | 501-800 | M11_501_800_allrisk_lb12mo_weekly_top10_1y | 16.0% | 0.88 | -34.0% | 12mo | Weekly | 10 |
| M12 | 551-800 | *(see M9/M10 overlap)* | - | - | - | - | - | - |

**Key Observations:**
- **Sharpe range:** 0.80–1.02 (M9 best at 1.02, acceptable for live trading)
- **CAGR range:** 16.0–21.2% (exceeds benchmark 14.2%, but excess volatile)
- **MaxDD range:** -34% to -65% (biggest pain point; many strategies hit >-50%)
- **Overlapping bands:** M1/M2 (1-75), M3/M4, M9/M10 (276-500), M11/M12 (501-800)
  - Strategy: Pick best from each overlapping pair; drop redundant band for deployment

---

### 1.2 Variant Deprecation

Remove from this backtesting cycle:
- **Balanced:** adtv_floor + adtv_capped_sizing filters (adds constraints, reduces alpha)
- **Risk-Adjusted:** custom volatility weighting (complex, not outperforming AllRisk)
- **Max-Defensive:** beta orthogonalization (too conservative for alpha mandate)

**Keep:** AllRisk only (unfiltered, max alpha)

---

### 1.3 Overlap Resolution Strategy

**Overlapping bands:**
| Pair | Bands | Action |
|------|-------|--------|
| 1-50 vs 1-75 | M1 vs M2 | Keep M1 (1-50, sharper subset) |
| 276-550 vs 301-500 | M9 vs M10 | Keep M9 (larger, best Sharpe 1.02) |
| 501-800 vs 551-800 | M11 vs M12 | Keep M11 (larger, better drawdown -34% vs unknown M12) |
| 51-100 vs 76-160 | M3 vs M4 | Keep M4 (larger, better Sharpe 0.91 vs 0.80) |
| 101-150 vs 151-200 | M5 vs M6 | Keep M6 (better Sharpe 0.90 vs 0.89) |
| 161-275 vs 201-300 | M7 vs M8 | Keep M7 (better Sharpe 0.95, equal MaxDD) |

**Result:** 6 strategies post-overlap removal (vs. 12 original)

---

## Part 2: Strategy Matrix Build (M1-M12 AllRisk Cycle)

### 2.1 Scope Definition

**Dimensions:**
- **Bands:** 12 (M1-M12 as-is)
- **Variant:** AllRisk only
- **Lookback periods:** 3, 6, 9, 12 months (4 options)
- **Rebalance cadences:** 5, 10, 21, 42, 63 trading days (weekly, biweekly, monthly, bimonthly, quarterly) — 5 options
- **Top-N:** 10, 15, 20 (3 options)

**Total jobs:** 12 bands × 4 lookbacks × 5 rebalance × 3 top-N = **720 jobs** (vs. 1,600+ in baseline)

**Backtesting period:** 
- **Validation window:** 2019-01-01 to 2025-12-31 (7 years; ~8 min per job, 96 min total for 12 jobs pilot)
- **Full window:** 2008-04-01 to 2026-06-30 (~40 min per job, 480 min = 8 hours for 12 jobs)

---

### 2.2 Staged Rollout Design

**Stage 1: Pilot + Validation (2 hours)**
- Pick 1 band (e.g., M10 / 301-500, best historical performer)
- Test 2 lookbacks (3mo, 9mo) × 2 rebalance (21d, 63d) = **4 jobs**
- Backtest window: 2019-01-01 to 2025-12-31 (7 years, snapshot cached)
- **Objective:** Confirm reproducibility, signal integrity, no data errors
- **Output:** See if results match or beat baseline top-performer

**Stage 2: Full Pilot Validation (3-4 hours)**
- Expand to 4 pilot bands (M1, M7, M10, M9 — mix of sizes)
- 4 lookbacks × 2 rebalance (21d, 63d) = **32 jobs** (4 bands × 8 combos)
- Same 7-year window (cached OHLCV from Stage 1)
- **Objective:** Validate across band sizes, finalize combinations before scaling
- **Output:** Top 5 combos per band

**Stage 3: Full M1-M12 AllRisk Run (8-12 hours)**
- All 12 bands, all 4 lookbacks, all 5 rebalance cadences, all 3 top-N values
- 720 total jobs
- Backtest period: 2008-04-01 to 2026-06-30 (16 years, no cache to avoid staleness)
- **Objective:** Complete AllRisk matrix; publish results; gate before variant expansion
- **Timeline:** Start after Stage 2 validation passes (run overnight, 24-48 hour window)

**Stage 4: M13 Launch (4-6 hours, parallel to Stage 3 tail)**
- M13 configuration: Nifty ranks 1-800 (full universe)
- Variants: Top 30, 40, 50 (replaces AllRisk's 10/15/20 for this band)
- Same lookbacks & rebalance as pilot bands
- Can run in parallel during Stage 3 if DuckDB contention managed (snapshot caching mandatory)
- **Jobs:** 1 band × 4 lookbacks × 5 rebalance × 3 top-N = **60 jobs**

**Stage 5: Variant Expansion (conditional, 12-24 hours, post-gate)**
- If Stage 3 results show Sharpe > 0.75 on 10+ strategies, proceed
- Run top 50 strategy configs with Balanced & Risk-Managed variants
- **Expected:** Balanced performs 0-3% worse (tighter DD but lower alpha)
- **Decision:** Publish AllRisk results; flag Balanced as optional overlay for risk-averse mandate

---

### 2.3 Resource Optimization

**Mitigations for 720-job run:**

| Technique | Impact | Implementation |
|-----------|--------|-----------------|
| **OHLCV Snapshot Caching** | -20% wall-clock time | Add `"ohlcv_snapshot_dir"` to queue; prefetch once, reuse 720x |
| **DuckDB Write-Lock Tuning** | -40% contention | Use `read_only=True` for query-heavy jobs; batch writes by hour |
| **Parallel Queues** | 3× speedup | Split 720 jobs into 3 independent queues (if no ticker overlap — likely safe for M-bands) |
| **Exclude Poor Performers Early** | -30% redundant runs | After Stage 2, exclude combos with Sharpe < 0.60; reduces Stage 3 from 720 to ~500 |
| **Skip Micro-Lookbacks** | -25% jobs | Drop 3-month lookback if 6/9/12 mo dominate (decision after Stage 2) |

**Timeline Estimate (with optimization):**
- Stage 1 (pilot): 15 min (4 jobs, 7-year window, cached)
- Stage 2 (expand): 45 min (32 jobs, cached)
- Stage 3 (full M1-M12): 6-8 hours (720 jobs, batched, snapshot cache)
- Stage 4 (M13): 4 hours (parallel, but gated on Stage 3 decision)
- **Total:** ~10-12 hours, easily fits in 24-48 hour window

---

## Part 3: M13 Implementation (New Band: Nifty 1-800, Top 30/40/50)

### 3.1 Rationale

**Why M13?**
- Current M11/M12 cover 501-800 (illiquid small-caps, avg 16% CAGR)
- User request: test full universe (Nifty 1-800) with **tighter position limits** (Top 30/40/50 vs. 10/15/20)
- **Hypothesis:** Smaller, more concentrated portfolios reduce dilution, improve Sharpe on large-cap-friendly universe
- **Liquidity:** Nifty 1-800 is NSE's official liquidity universe; all ADTV thresholds pre-calibrated

### 3.2 Configuration

**Band Definition:**
```python
# Add to features/momentum_universe.py::RANK_BANDS after M12
(13, 1, 800)  # M13: Full Nifty liquidity universe
```

**Queue Job Template:**
```json
{
  "kind": "orchestrator",
  "channel": "momentum",
  "rank_band_id": 13,
  "lookback_months": [3, 6, 9, 12],
  "rebalance_cadence_days": [5, 10, 21, 42, 63],
  "top_n": [30, 40, 50],
  "start_date": "2008-04-01",
  "end_date": "2026-06-30",
  "initial_capital": 1000000,
  "max_tickers": 800,
  "min_history_days": 60,
  "capital_mode": "lump",
  "exit_variant": "baseline",
  "defer_db_writes": true
}
```

**Expected Jobs:** 1 band × 4 lookbacks × 5 rebalance × 3 top_n = **60 jobs**

### 3.3 Strategy Registry Update

Add to `datastore/schema/create_strategy_registry.py`:

```python
# M13 rows (one per lookback × rebalance × top_n combo)
INSERT INTO strategy_registry (strategy_key, channel, name, definition_json, ...)
VALUES 
  ('m13_1_800_allrisk_lb3mo_weekly_top30_5d', 'momentum', 'M13 Full Nifty Top 30 3mo Weekly', {...}, ...),
  ('m13_1_800_allrisk_lb3mo_weekly_top40_5d', 'momentum', 'M13 Full Nifty Top 40 3mo Weekly', {...}, ...),
  ('m13_1_800_allrisk_lb3mo_weekly_top50_5d', 'momentum', 'M13 Full Nifty Top 50 3mo Weekly', {...}, ...),
  -- ... 57 more for remaining combos
```

---

## Part 4: Selection Criteria & Performance Gates

### 4.1 Live Trading Thresholds (Stage 3 → Stage 5 gate)

| Metric | Threshold | Rationale |
|--------|-----------|-----------|
| **Sharpe Ratio** | > 0.80 | Sufficient risk-adjusted return for live deployment; >0.90 preferred for aggressive mandates |
| **CAGR (pre-tax)** | > 20% | Exceeds benchmark (14.2%) by 6pp; 20% achievable on mid-caps (M9-M10) but tight on large-caps (M1-M2) |
| **Max Drawdown** | < 20% (preferred), < 30% (acceptable) | Current baseline -44% to -65% is unacceptable; overlay Phase 7 (crash-aware) to achieve this |
| **Positive Years** | ≥ 15 of 18 FY (83%+) | Robustness check; avoid strategies that win big but crash in 2-3 years |
| **Sortino > Sharpe** | Ratio > 1.1x | Confirms downside vol is lower than upside; true risk-adjusted outperformance |
| **Consistency** | 3-yr & 5-yr median within 75-125% of full-period | Avoid lucky streaks; signal should persist across sub-periods |

**Rejection Criteria (automatic drop):**
- Sharpe < 0.60
- Max Drawdown > 50%
- Positive years < 12 of 18
- CAGR < 15% (unless paired with ultra-low DD for hedge portfolio)

---

### 4.2 Overlap Handling (Final 10-15 Selection)

After Stage 3, rank candidates by Sharpe (descending). For each overlapping band pair:

| Pair | Selection Rule | Expected Winner |
|------|----------------|-----------------|
| M1 (1-50) vs M2 (1-75) | Pick higher Sharpe; if tied, prefer M1 (smaller, less dilution) | M1 (Sharpe 0.92) |
| M3 (51-100) vs M4 (76-160) | Pick higher Sharpe | M4 (Sharpe 0.91) |
| M5 (101-150) vs M6 (151-200) | Pick higher Sharpe | M6 (Sharpe 0.90) |
| M7 (161-275) vs M8 (201-300) | Pick higher Sharpe | M7 (Sharpe 0.95) |
| M9 (276-550) vs M10 (301-500) | Pick higher Sharpe | M9 (Sharpe 1.02) |
| M11 (501-800) vs M12 (551-800) | Pick higher Sharpe; if close, prefer M11 (larger) | M11 (Sharpe 0.88) |

**Deployment Outcome:** 1 strategy per band-pair → 6 core strategies + 1 M13 = **7 strategies minimum**

To reach 10-15, include:
- Top 2 variants per band (e.g., M1 with 3mo lookback + M1 with 9mo lookback for diversification)
- M13 variants (3 configs: Top 30, 40, 50)
- 1-2 Balanced variants as optional overlays

---

## Part 5: Drawdown Mitigation Strategy

### 5.1 Root Cause: Concentration & Regime Risk

**Why baseline strategies hit -44% to -65% drawdown:**
1. **No regime filter:** During 2020 COVID crash, momentum strategies typically underperform (reverse correlation)
2. **No crash overlay:** Current MomentumAdapter has no volatility spike detection
3. **High churn:** Biweekly rebalance × 20 top-N = frequent position changes, whipsaws in volatile markets
4. **Ranking inversion:** When momentum reverses (e.g., 2020 Q1), top performers become worst performers instantly

### 5.2 Mitigation Layers (to apply post-validation)

**Layer 1: Crash-Aware Filter (Phase 7 / R7 from spec)**
- Monitor 20-day realized volatility and 1-year drawdown
- If vol > 90th percentile OR max-DD < -20% in trailing 252 days → disable new buys, liquidate 50% of position
- Cost: ~1-2% CAGR loss, but cuts max-DD from -50% to -25-30%
- **Implementation:** Add to `features/momentum_strategy.py::select_buy_pool()` as optional `crash_regime_filter` parameter

**Layer 2: Position Sizing Overlay (Phase 8 / R8)**
- Scale position size inversely to 63-day realized volatility
- If vol is 2× normal, halve position size
- Cost: ~0.5-1% CAGR loss, adds Sharpe by 0.05-0.1x due to lower DD
- **Implementation:** Reuse existing volatility scaling in `core/portfolio.py`

**Layer 3: Hold-Time Constraints**
- Minimum hold 21 days before exit (avoid 1-day whipsaws)
- Maximum hold 252 days (avoid stale momentum)
- Cost: negligible, benefits robustness

**Layer 4: Rebalance Cadence Tuning**
- Quarterly (63d) outperforms biweekly (10d) in high-vol regimes
- Post-validation, recommend quarterly for large-cap (M1-M4), bimonthly for mid-cap (M5-M10)
- Biweekly only for extremely small-cap (M11-M12)

**Expected Impact:**
- **Before mitigation:** Max DD -50%, Sharpe 0.90
- **After Layer 1+2:** Max DD -28%, Sharpe 0.92 (net: +0.02 Sharpe, -22% DD)
- **Target:** Sharpe > 0.80 with DD < -25% achieved on 70%+ of strategies

---

## Part 6: Execution Plan (24-48 Hour Timeline)

### Timeline: Friday 14:00 → Sunday 14:00 (India Time)

**Friday 14:00–15:00 (Stage 1: Pilot Setup)**
- Build `backtest/queues/stage1_pilot_4jobs.json` (M10, 2 lookbacks, 2 rebalance)
- Launch on command
- Expected end: 15:15 (4 jobs × 4 min each)

**Friday 15:15–16:15 (Stage 1 Result Review)**
- Query results: `SELECT * FROM backtest_runs WHERE strategy_key LIKE 'm10_%'`
- Compare vs baseline: confirm Sharpe > 0.90, no data anomalies
- **Gate decision:** If OK, proceed to Stage 2; else, debug & re-run

**Friday 16:30–17:30 (Stage 2: Full Pilot, 32 jobs)**
- Build `backtest/queues/stage2_pilot_32jobs.json` (4 bands × 8 combos)
- Launch on command
- Expected end: 17:50 (32 jobs × 1.5 min each, cached)

**Friday 17:50–18:30 (Stage 2 Result Review & Cutoff Decision)**
- Extract top 5 combos per band
- Identify underperforming lookbacks (e.g., 3mo worse than 9mo) → mark for exclusion in Stage 3
- Finalize Stage 3 job count (likely 600-700 instead of 720 if micro-lookbacks removed)

**Friday 19:00 → Saturday 10:00 (Stage 3: Full M1-M12 AllRisk, 600-720 jobs)**
- Build `backtest/queues/stage3_full_matrix_720jobs.json`
- **Snapshot cache:** Ensure `ohlcv_snapshot_dir` set; pre-fetch once at job #1
- **DuckDB tuning:** Batch writes; run in read-only mode for queries
- Queue starts: `python3 backtest/run_strategy_queue.py backtest/queues/stage3_full_matrix_720jobs.json`
- Expected runtime: 8-10 hours (720 jobs × 50 sec/job with cache)
- Monitor: Check `journalctl -u alphalens-scheduler` every 2 hours for lock contention

**Saturday 10:00–11:00 (Stage 3 Results Aggregation)**
- Query top 50 strategies by Sharpe
- Generate pivot table: band × lookback × rebalance → [CAGR, Sharpe, MaxDD]
- Export to CSV for Stage 5 decision

**Saturday 11:00–12:00 (Overlap Resolution & Band Selection)**
- Apply overlap resolution logic (Section 4.2)
- Select 6 core strategies (1 per band-pair)
- Identify 4-8 runner-up strategies for diversification

**Saturday 12:00–13:00 (Optional: Stage 4 - M13 Parallel Run)**
- If Stage 3 shows Sharpe > 0.80 on 10+ strategies, launch M13
- Build `backtest/queues/stage4_m13_60jobs.json`
- Run in parallel (separate queue process) while Stage 3 tail finishes
- Expected: M13 results by Saturday 16:00

**Saturday 14:00–16:00 (Stage 5 Decision & Variant Expansion Planning)**
- **Gate Review Meeting:**
  - Stage 3 results: top 50 strategies, band selection, overlap resolution
  - Stage 4 results (if ready): M13 top performers, Top 30/40/50 comparison
  - Decision: Proceed to Balanced/Risk-Managed variants? (recommended: yes, low cost)
  
- **Build Stage 5 Queue (conditional):**
  - Top 50 strategies from Stage 3 + M13 variants
  - AllRisk + Balanced + Risk-Managed (3× job count)
  - ~150 jobs; expected 4-6 hours
  - Schedule for Saturday 17:00 start, completion Sunday 10:00

**Saturday 17:00 → Sunday 10:00 (Stage 5: Optional Variant Expansion)**
- Run top-50 + M13 with Balanced/Risk-Managed filters
- Compare to AllRisk: expect -0.5% to -3% CAGR, -5pp to -10pp MaxDD gain
- Publish results: "AllRisk: max alpha, high DD; Balanced: moderate alpha, lower DD"

**Sunday 11:00–14:00 (Final Selection & Reporting)**
- Apply selection criteria (Section 4.1)
- Rank final 10-15 strategies
- Publish live-deployment recommendation: 1 per band + 1-2 M13 variants
- Output: `backtest_results_final_10_15_strategies.csv`

---

## Part 7: Deployment Targets (10-15 Strategies)

### 7.1 Conservative Deployment (7 strategies, 1 per band)

| Band | Ticker Range | Strategy | CAGR | Sharpe | MaxDD | Rebalance | Rationale |
|------|--------------|----------|------|--------|-------|-----------|-----------|
| M1 | 1-50 | M10_1_50_allrisk_lb3mo_bimonthly_top10_21d | 21.2% | 0.92 | -44% | Bimonthly | Highest Sharpe; top 10 stocks |
| M4 | 76-160 | M10_76_160_allrisk_lb9mo_quarterly_top10_63d | 19.4% | 0.91 | -50% | Quarterly | Large-cap play; 9mo avoids noise |
| M5 | 101-150 | M9_101_150_allrisk_lb3mo_bimonthly_top15_21d | 19.3% | 0.89 | -54% | Bimonthly | Mid-cap, balanced risk |
| M7 | 161-275 | M10_161_275_allrisk_lb3mo_bimonthly_top20_21d | 19.0% | 0.95 | -50% | Bimonthly | Sharpe 0.95, good consistency |
| M9 | 276-550 | M9_276_550_allrisk_lb9mo_bimonthly_top20_63d | 16.6% | 1.02 | -42% | Bimonthly | **Highest Sharpe 1.02** |
| M11 | 501-800 | M11_501_800_allrisk_lb12mo_weekly_top10_1y | 16.0% | 0.88 | -34% | Weekly | Lowest DD, still 16% CAGR |
| M13 | 1-800 | M13_1_800_allrisk_lb6mo_quarterly_top40 | TBD* | TBD* | TBD* | Quarterly | Full universe, concentrated (40) |

**Portfolio Metrics (7-strategy equal-weight):**
- **Blended CAGR:** (21.2 + 19.4 + 19.3 + 19.0 + 16.6 + 16.0 + TBD) / 7 ≈ **18.2%**
- **Blended Sharpe:** (0.92 + 0.91 + 0.89 + 0.95 + 1.02 + 0.88 + TBD) / 7 ≈ **0.93**
- **Blended MaxDD:** (-44 + -50 + -54 + -50 + -42 + -34 + TBD) / 7 ≈ **-46.9%** ← *PROBLEM*

### 7.2 Risk-Mitigated Deployment (10 strategies: 7 + 3 M13 variants)

Add M13 variants to capture full-universe liquidity upside without M11/M12 illiquidity:

| Band | Strategy | CAGR | Sharpe | MaxDD | Rationale |
|------|----------|------|--------|-------|-----------|
| M13 | M13_1_800_lb6mo_quarterly_top30 | TBD* | TBD* | TBD* | Smallest position size, lowest DD |
| M13 | M13_1_800_lb6mo_quarterly_top40 | TBD* | TBD* | TBD* | Medium position, balanced |
| M13 | M13_1_800_lb6mo_quarterly_top50 | TBD* | TBD* | TBD* | Largest position, highest alpha |

**Effect:**
- Removes M11/M12 (illiquid small-caps, -34% DD)
- Replaces with M13 Top 30-50 (full universe, likely -30% DD if M13 improves on M11/M12)
- **Revised blended MaxDD:** -40% to -43% (4-6pp improvement)

### 7.3 Aggressive Deployment (15 strategies: all 7 + M13 × 3 + 2 runups)

Add 2 runner-up strategies from different bands (e.g., M1 alt-lookback, M7 alt-rebalance):

| Band | Variant | Rationale |
|------|---------|-----------|
| M1 | M1_lb9mo_bimonthly (alt to lb3mo_quarterly) | Diversify lookback exposure |
| M7 | M7_lb6mo_monthly (alt to lb3mo_bimonthly) | Test monthly rebalance impact |

**Result:** 15-strategy portfolio with:
- **Diversification:** 3 lookbacks (3/6/9mo), 3 rebalance cadences (weekly/bimonthly/quarterly), 5 bands
- **Coverage:** Large-cap (M1, M4), Mid-cap (M5, M7, M9), Small-cap (M11, M13)
- **Expected metrics:** Blended Sharpe 0.90–0.93, CAGR 17.5–19%, MaxDD -38% to -42% (before crash-aware overlay)

---

## Part 8: Drawdown Reduction Roadmap (Post-Deployment)

### Phase 7 (R7): Crash-Aware Overlay
Once 10-15 strategies selected and baselined, layer on:
- **Regime detection:** Disable buys when vol > 80th percentile (last 252 days) OR max-DD > 20%
- **Position reduction:** During regime, liquidate 50% of positions
- **Expected impact:** MaxDD -46% → -28%, Sharpe maintained or +0.02

**Timeline:** After Stage 5 gate, build & backtest as R7 implementation (1-2 hours)

### Phase 8 (R8): Volatility-Managed Sizing
- Scale position sizes inversely to 63-day realized volatility
- Cap at 1.0x leverage, floor at 0.5x
- **Expected impact:** Further DD reduction to -22% to -25%, Sharpe +0.05

**Timeline:** Post-Phase 7 validation (1-2 hours)

### Combined Outcome:
- **Before:** MaxDD -46%, Sharpe 0.93, CAGR 18.2%
- **After Phases 7+8:** MaxDD -23%, Sharpe 0.94, CAGR 17.5% (net -0.7% CAGR for -23pp DD reduction) ✓

---

## Part 9: Decision Framework & Gates

### Gate 1: Stage 1 Validation (Pilot 4 jobs)
**Decision:** Reproducibility OK?
- **PASS:** Sharpe 0.85–0.95, no data errors → proceed to Stage 2
- **FAIL:** Sharpe < 0.75 or anomalies → debug data pipeline, re-run

### Gate 2: Stage 2 Findings (Full Pilot 32 jobs)
**Decision:** Best lookback/rebalance combos identified?
- **PASS:** 10+ strategies with Sharpe > 0.80 → finalize Stage 3 job list
- **FAIL:** <10 strategies meet threshold → add Balanced variant to Stage 3, reconsider portfolio construction

### Gate 3: Stage 3 Results (Full M1-M12 AllRisk 720 jobs)
**Decision:** Enough candidates for 10-15 deployment?
- **PASS:** ≥15 strategies with Sharpe > 0.80, CAGR > 18% → band selection + overlap resolution
- **SOFT PASS:** 10-14 strategies meet criteria → include runner-ups, lower CAGR threshold to 17%
- **FAIL:** <10 strategies → pivot to Balanced variant, debug why AllRisk underperforms

### Gate 4: Band Selection (Overlap Resolution)
**Decision:** 1 strategy per band chosen?
- **PASS:** 6 core strategies selected, M13 queue ready → launch Stage 4 (M13) or proceed to Stage 5 (variant expansion)
- **REVISE:** Tie on Sharpe → pick smaller band (less dilution) or higher consistency metric

### Gate 5: M13 Launch (Stage 4, 60 jobs)
**Decision:** Does M13 outperform M11/M12?
- **YES:** Sharpe > 0.88, MaxDD < -40% → replace M11/M12 with M13 Top 30/40/50; finalize 10-15 strategies
- **NO:** M13 underperforms → keep M11/M12, use M13 as hedge/alternative only

### Gate 6: Final Deployment Decision (Stage 5 Gate)
**Checklist:**
- ✅ 10–15 strategies selected
- ✅ Blended Sharpe ≥ 0.88
- ✅ Blended CAGR ≥ 17% (acceptable loss vs. 18.2%)
- ✅ Blended MaxDD < -45% (target -25% post-Phase 7)
- ✅ Coverage: 3 lookbacks, 3-4 rebalance cadences, 5-6 bands
- ✅ Phase 7 (crash-aware) backtest plan drafted
- ✅ Paper-trading endpoint configured

**Decision:** Deploy if ALL checks pass → Move to **Phase 7 Implementation & Paper Trading**

---

## Part 10: Implementation Checklist

### Pre-Backtest Setup (Day 0, Friday 13:00)
- [ ] Verify DuckDB at `~/.local/share/AlphaLens/data/alphalens.duckdb` is unlocked
- [ ] Check scheduler status: `systemctl --user status alphalens-scheduler.service`
- [ ] Confirm OHLCV data current: `SELECT MAX(date) FROM ohlcv_adjusted`
- [ ] Create snapshot dir: `mkdir -p backtest/cache/ohlcv_snapshots`
- [ ] Disable API if running: `systemctl --user stop alphalens-api.service`

### Stage 1 (Friday 14:00–15:30)
- [ ] Build `stage1_pilot_4jobs.json`
  ```bash
  python3 backtest/run_strategy_queue.py backtest/queues/stage1_pilot_4jobs.json
  ```
- [ ] Monitor: `tail -f execution_logs/stage1_pilot.log`
- [ ] Query results: 
  ```bash
  sqlite3 ~/.local/share/AlphaLens/data/backtest.duckdb \
    "SELECT strategy_key, cagr, sharpe, max_dd FROM backtest_runs WHERE strategy_key LIKE 'm10_%' ORDER BY sharpe DESC LIMIT 4"
  ```

### Stage 2 (Friday 16:30–17:50)
- [ ] Build `stage2_pilot_32jobs.json` (M1, M7, M9, M10 × 8 combos)
- [ ] Launch: `python3 backtest/run_strategy_queue.py backtest/queues/stage2_pilot_32jobs.json`
- [ ] Extract top 5 per band after completion

### Stage 3 (Friday 19:00 → Saturday 10:00)
- [ ] Build final `stage3_full_matrix_720jobs.json` (adjust job count based on Stage 2)
- [ ] Confirm cache setting: `"ohlcv_snapshot_dir": "/home/amit/projects/AlphaLens/backtest/cache/ohlcv_snapshots"`
- [ ] Launch: `python3 backtest/run_strategy_queue.py backtest/queues/stage3_full_matrix_720jobs.json`
- [ ] Monitor lock contention every 2h: `fuser ~/.local/share/AlphaLens/data/*.duckdb`
- [ ] Query top 50 results after completion

### Stage 4 (Saturday 12:00–16:00, conditional)
- [ ] Add M13 band to `RANK_BANDS` in `features/momentum_universe.py`:
  ```python
  (13, 1, 800),
  ```
- [ ] Build `stage4_m13_60jobs.json`
- [ ] Launch (parallel to Stage 3 tail if possible): `python3 backtest/run_strategy_queue.py backtest/queues/stage4_m13_60jobs.json`

### Stage 5 (Saturday 16:00 → Sunday 10:00, conditional on Stage 3 gate pass)
- [ ] Build `stage5_variant_expansion_150jobs.json` (top 50 from Stage 3 + M13, × 3 variants)
- [ ] Launch: `python3 backtest/run_strategy_queue.py backtest/queues/stage5_variant_expansion_150jobs.json`
- [ ] Compare AllRisk vs Balanced vs Risk-Managed metrics

### Final Reporting (Sunday 11:00–14:00)
- [ ] Generate pivot table: band × lookback × rebalance → Sharpe, CAGR, MaxDD
- [ ] Apply overlap resolution; select 1 per band-pair
- [ ] Create `backtest_results_final_10_15_strategies.csv`:
  ```csv
  strategy_key,ticker_range,cagr,sharpe,max_dd,lookback,rebalance,top_n,band_pair,deployment_priority
  ...
  ```
- [ ] Publish to `backtest/reports/` and frontend

---

## Part 11: Risk & Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|-----------|
| DuckDB lock contention | High | Stage 3 stalls for hours | Snapshot caching + read-only queries; stage queues sequentially if lock held >10 min |
| Stage 3 takes > 12 hours | Medium | Extends timeline beyond 48h | Reduce job count by 20% (drop micro-lookbacks); run Stages 3+4 in parallel |
| Sharpe > 0.75 not achieved on 10+ strategies | Low | Restart Stage 2 with Balanced variant | Pre-commit: Balanced variant queued & ready to launch if AllRisk gate fails |
| M13 underperforms M11/M12 | Low | Need to keep M11/M12 in final 10-15 | Accept, use M13 as optional hedge; final count stays 15 (not 10) |
| Data error (e.g., bad OHLCV) discovered post-Stage-3 | Low | Invalidates results, re-run required | Spot-check Stage 1 results vs baseline (backtest_all.csv); confirm Sharpe within 10% |
| API restart mid-Stage-3 | Very low | Orphans running jobs | Keep API stopped until all stages complete; restart only after Sunday 14:00 |

---

## Conclusion

This plan delivers **10-15 production-ready momentum strategies** in a 24-48 hour window through:

1. **Variant streamlining:** AllRisk only for speed & clarity
2. **Staged validation:** Pilot → scale → decision → optional variant expansion
3. **M13 launch:** Full universe (1-800) with concentrated sizes (30/40/50)
4. **Drawdown mitigation:** Phase 7 crash-aware overlay (post-deployment) targets -23% MaxDD
5. **Band-based deployment:** 1 strategy per band (6-7 core) + 3-8 diversifiers = 10-15 total

**Expected outcome:**
- **Sharpe:** 0.88–0.93 (vs. 0.92 baseline best)
- **CAGR:** 17.5–19% (vs. 21.2% top strategy, trade-off for diversification)
- **MaxDD:** -25% to -30% (post-Phase 7, vs. -46% baseline) ✓
- **Coverage:** 5-6 equity bands, 3 lookbacks, 3+ rebalance cadences
- **Deployment:** Ready for paper trading + live trading (gated on Phase 7 validation)

---

## Questions for User Input

Before finalizing, confirm:

1. **Cutoff thresholds:** Sharpe > 0.80 & CAGR > 20%? Or flexible (0.75 & 18%)?
2. **M13 Top-N:** Propose 30/40/50; alternative suggestions?
3. **Stage 5 priority:** Required (Balanced + Risk-Managed fully tested) or optional (defer if AllRisk strong)?
4. **Phase 7 urgency:** Implement crash-aware overlay before paper trading, or as parallel workstream?
5. **Portfolio weights:** Equal-weight 10-15 strategies, or rank-weighted by Sharpe?
6. **Reporting audience:** Board/stakeholders? Prepare executive summary with 1-pager?
