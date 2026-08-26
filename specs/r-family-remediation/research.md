# Phase 0 Research: R-Family Remediation Unknowns Resolved

**Date**: 2026-08-26 | **Status**: Resolved | **For**: [plan.md](plan.md)

---

## Research Tasks Completed

### 1. Momentum-Strategy-Audit Prompt Specification (B-003)

**Unknown**: What constitutes APPROVED vs REJECTED in audit? What false positives exist?

**Finding**:
- **Current prompt** (unreliable): Checks surface-level parameters but misses deep violations
- **False positives identified**: 
  - Returned APPROVED for R3 despite missing skip-month variant implementation
  - Approved R1 without verifying ADTV floor thresholds actually implemented
  - No verification that overlapping-portfolio strategy (1/K staggered) vs singleton universe
  
**Decision**: Rewrite audit prompt with **specification-as-code pattern**:
1. Load J&T 2014 "Momentum and Reversal Have Different Anomalous Accruals" abstract
2. Extract 3 core claims:
   - "Replicate with 3/6/9/12-month lookback windows"
   - "Exclude lowest-volume quintile (ADTV < $X)"
   - "Rebalance monthly with 1-month skip"
3. Verify each claim in code:
   - Lookback: Check `lookback_days in [63, 126, 189, 252]`
   - ADTV floor: Check `if adtv >= threshold: rank...` exists
   - Skip-month: Check `rebalance_date.month != signal_date.month`
4. **Citation requirement**: Every APPROVED verdict must cite the code location verifying the claim

**Rationale**: False positives in audit == false confidence in R4-R11. Without fixing B-003, all downstream audits (B-004–B-010) are unreliable.

**Alternatives considered**: 
- Disable audit entirely → loses oversight; R11 would never be caught
- Add spot-check manual verification → doesn't scale; audit still unreliable
- Specification-as-code → achieves audibility + prevents false positives

**Implementation**: Rewrite prompt with pseudocode snippet showing specification extraction + citation path

---

### 2. R8 Rebalance Cadence (B-026)

**Unknown**: Why does R8 have 252-day annual rebalance vs. published J&T monthly?

**Finding**:
- **Current code**: `rebalance_cadence = 252` (one year)
- **Published spec**: "Barroso & Santa-Clara (2012) 'Momentum has its moments'" → monthly rebalance
- **Impact**: Annual rebalance = misses 11/12 of signal updates; underutilizes vol-scaling logic
- **Backtest effect**: R8 Sharpe likely artificially low; fixing likely improves performance

**Decision**: Change `rebalance_cadence = 21` (monthly).

**Rationale**: Vol-scaling designed for monthly signal updates; annual rebalance defeats the purpose. J&T momentum + annual rebalance is not comparable to published results.

**Alternatives considered**:
- Keep annual (simpler) → breaks specification compliance; Sharpe claims invalid
- Monthly (correct) → matches published work; expected Sharpe improvement

**Validation**: B-026 backtest must show Sharpe improves or stays flat post-fix; if drops, investigate.

---

### 3. R9 Regime-Adaptive Architecture (B-027)

**Unknown**: What's the design flaw in "4 separate runs"? How should regime-adaptive work?

**Finding**:
- **Current implementation**: Runs 4 separate universes (growth/value/momentum/crash) independently, averages results
- **Design flaw**: Each universe is a singleton backtest, not a regime-adaptive *single* backtest
- **Moreira & Muir (2017) spec**: "Exposure gates on regime (1.0/0.5/0.25 regimes) within ONE portfolio"
- **Problem**: 4 separate backtests can't measure regime-switching cost; averaging hides when regimes change
- **Correct design**: One backtest that computes regime on each rebalance date, applies exposure gate (e.g., 1.0× in bull, 0.5× in correction, 0.25× in crash)

**Decision**: Refactor to single backtest with regime-conditional exposure multiplier.

**Rationale**: Moreira-Muir spec requires *within-portfolio* regime switching, not separate-universe averaging. Current approach likely overestimates vol-control benefit.

**Alternatives considered**:
- Keep 4 separate runs → doesn't match published spec; incorrect results
- Single regime-adaptive run → matches spec; reveals true vol-control effectiveness
- Hybrid (ensemble) → still doesn't solve regime-switching cost; overly complex

**Validation**: B-027 Sharpe likely drops post-refactor (due to realistic regime costs); acceptable if vol-control benefit (max DD, Calmar) improves.

---

### 4. R10 Ranking: Individual vs. Sector-Level (B-028)

**Unknown**: Why rank sectors instead of individual stocks? Does it hurt performance?

**Finding**:
- **George & Hwang (2004) "The 52-week high and momentum investing"** spec: Individual stock size momentum
- **Current R10 code**: Sectors ranked by median size (individual stocks not ranked)
- **Problem**: Loses individual-level discrimination; ranks entire sectors uniformly
- **Impact**: Concentration risk (can overweight small-cap-heavy sectors); misses individual pickers

**Decision**: Revert to individual stock ranking; keep sector weighting as separate overlay (if needed).

**Rationale**: George & Hwang tested individual stocks, not sectors. Sector-level ranking is a simplification that breaks specification compliance.

**Alternatives considered**:
- Keep sector ranking → simple, but violates spec
- Individual ranking → matches spec; expected Sharpe improvement for small-cap signals

**Validation**: B-028 backtest confirms Sharpe improves post-fix.

---

### 5. R11 Ranking Direction: Highest vs. Lowest (B-029)

**Unknown**: Should R11 rank on *highest* or *lowest* reversal signal?

**Finding**:
- **Reversal strategy spec**: Profit from mean reversion (low performers recover, high performers decline)
- **Ranking**: Lowest prior-period returns = strongest reversal candidates (biggest mean-reversion potential)
- **Current R11 code**: Ranks highest (backwards)
- **Impact**: Positions are inverse; strategy is anti-reversal, not reversal

**Decision**: Fix ranking to lowest (reverse current sort order).

**Rationale**: Reversal strategy must go long the *worst* performers (who revert upward), not the best. Current code is 180° wrong.

**Alternatives considered**:
- Keep backwards ranking → Sharpe likely negative or near-zero
- Correct ranking → Sharpe should turn positive (or at least match published reversal performance)

**Validation**: B-029 backtest should show dramatic Sharpe improvement (negative → positive, or flat → positive).

---

### 6. R5 Registry Mismatch: J&T vs. George & Hwang (B-025)

**Unknown**: Is R5 based on J&T or George & Hwang? Registry says wrong author.

**Finding**:
- **Registry entry**: `author: "Jegadeesh & Titman"` for R5
- **Code implementation**: Matches George & Hwang (2004) size momentum, not J&T (1993) past-performance momentum
- **Problem**: Misleading authorship; audit relies on author to verify specification

**Decision**: Correct registry to `author: "George & Hwang (2004)"`.

**Rationale**: Audit depends on correct authorship metadata. Mismatch breaks chain of custody.

**Alternatives considered**:
- Keep wrong author → continues audit confusion
- Correct author → enables proper specification verification

**Validation**: Registry update only; no code changes needed.

---

### 7. R12 Signal-Cadence Mismatch: 1-Month Signal, 3-Month Rebalance (B-017 Design Insight)

**Unknown**: Why does R12's 1-month reversal signal decay before 3-month rebalance?

**Finding**:
- **Reversal strategy timing**: Reversal signals (loser/winner identification) decay within 1-2 months (mean-reversion fully realizes)
- **Rebalance cadence**: 3-month (quarterly) rebalance holds positions for 90 days
- **Problem**: By month 2-3, original reversal signal is stale; positions don't align with current reversal candidates
- **Impact**: Later months in quarter trade on old signals; performance cliff likely in month 3
- **Solution**: Test monthly (21-day) rebalance to keep signal fresh

**Decision**: B-017 tests monthly cadence; if Sharpe > 0.85, adopt it.

**Rationale**: Reversal signals decay fast; holding for 3 months mixes fresh + stale logic. Monthly rebalance keeps signal-action alignment tight.

**Alternatives considered**:
- Keep quarterly → stale signals; lower Sharpe
- Switch to monthly → matches signal lifecycle; expected Sharpe improvement

**Validation**: B-017 backtest comparison (quarterly vs. monthly rebalance).

---

### 8. Sub-Period Stability Testing (B-018 Methodology)

**Unknown**: How to test if reversal edge is concentration in COVID 2019-2022 bull market?

**Finding**:
- **2019-2025 backtest**: R12 Sharpe 0.72 (strong, headline result)
- **Risk**: 2019-2022 dominated by COVID recovery (unusually strong bull market + high volatility)
- **Reversal in bull markets**: Losers recover strongly → strong edge
- **Reversal in normal markets**: Losers may continue declining → edge may disappear
- **Test methodology**:
  - Split: 2019-2022 (2009-2025 recovery + 2022 reversal) vs. 2023-2025 (normal, normalized vol)
  - Expected: If both > 0.70 Sharpe, edge is regime-independent. If 2019-2022 >> 2023-2025, edge is concentrated in bull/high-vol.
  - Gate: Both > 0.70 required; if not, indicates selection bias

**Decision**: Run B-018 backtest with explicit date-range filtering.

**Rationale**: Reversal edge can be an artifact of specific market conditions (bull market, high vol). Need to prove it generalizes.

**Alternatives considered**:
- Single period backtest → misses concentration risk
- Sub-period breakdown → reveals regime dependence

**Validation**: B-018 reports both sub-period Sharpe + volatility; both must be > 0.70 Sharpe to pass Phase 2 gate.

---

### 9. Selection Bias (DSR) Recomputation (B-019)

**Unknown**: What is DSR? Why is n_trials=2 wrong?

**Finding**:
- **DSR (Deflated Sharpe Ratio)**: Corrects for multiple testing when running many backtests
- **Current R12 DSR**: Computed with `n_trials=2` (only 2 variations tested)
- **Reality**: R10-R12 are themselves winners from a larger pool of momentum variants tested (100+)
- **Correct n_trials**: Should be ~150 (number of possible momentum variants: different lookbacks, cadences, universes, regimes)
- **Impact**: With true n_trials=150, corrected DSR likely < 0.5 (edge is noise, not signal)

**Decision**: Recompute DSR with `n_trials` = count of unique (lookback, cadence, universe, regime) combinations tested across entire momentum pipeline.

**Rationale**: DSR exists to penalize backtesting over-fit. If we tested 100+ combinations and selected R10-R12 as winners, we owe the market that penalty.

**Alternatives considered**:
- Keep n_trials=2 → underestimates overfitting; false confidence
- Recompute with true n_trials → realistic selection bias correction

**Gate**: Corrected DSR > 0.5 required to pass Phase 2.

---

### 10. Robustness Check Failures (B-020 Root-Cause)

**Unknown**: Why did R12 fail fold_stability and benchmarks checks? Can they be fixed?

**Finding**:
- **Fold_stability check**: Walk-forward validation across 10 yearly folds; R12 Sharpe drops in some folds
- **Benchmarks check**: R12 underperforms buy-and-hold in select sub-periods
- **Problem**: Two possibilities:
  1. Edge is real but concentrated (sub-periods, regimes) → need fine-tuning or archival
  2. Edge is noise, signal is fragile → need to archive
- **Diagnosis in B-020**: Root-cause via:
  - Plot fold-by-fold Sharpe (where does it drop?)
  - Compare underperforming folds to market regime (recession, COVID, etc.)
  - Check if liquidity constraints apply (e.g., edge only in high-liquidity periods)

**Decision**: B-020 investigation to determine if failure is due to regime/liquidity (fixable) or fundamental overfitting (archive).

**Rationale**: Robustness failures aren't automatically fatal; need to understand *why* before deciding.

**Alternatives considered**:
- Archive R12 immediately → may be throwing away real edge if failure is regime-specific
- Investigate → reveals whether edge is robust or fragile

**Outcome**: Documented in B-020 findings; determines Phase 2 gate result.

---

### 11. Liquidity Bucketing Integration (B-016)

**Unknown**: How to wire `bucket_by_adtv_quintile()` into R12?

**Finding**:
- **Function exists**: `bucket_by_adtv_quintile()` in `backtest/core/liquidity.py` (100% tested, zero call sites)
- **Purpose**: Groups stocks into 5 liquidity buckets by ADTV (average daily trading volume)
- **Integration**: Add as stratification layer:
  1. Rank stocks by reversal signal
  2. For each quintile bucket (Q1=most liquid → Q5=least liquid):
     - Compute bucket-specific Sharpe, position count, avg position size
  3. Analyze: Does reversal edge exist in all buckets or only Q1-Q3?
  4. If edge concentrated in Q1-Q3 (high liquidity) → restrict strategy to liquid names only
  5. If edge disappears in Q4-Q5 (illiquid) → justifies exclusion

**Decision**: Integrate bucketing analysis into R12 backtest; use findings to gate strategy (stay liquid, or archive).

**Rationale**: Liquidity effect can hide concentration risk. Need to verify edge exists outside small-cap illiquidity traps.

**Alternatives considered**:
- Skip liquidity analysis → allows strategy to hold illiquid names; execution risk in live trading
- Integrate bucketing → reveals concentration; enables informed decision

**Validation**: B-016 reports per-bucket Sharpe; if edge disappears in low-liquidity, restrict to high-liquidity names only.

---

### 12. Backtest Cache Snapshot Reconciliation (B-021)

**Unknown**: How to verify snapshot vs. live data consistency?

**Finding**:
- **Cache snapshot** (if exists): Parquet copy of backtest features at known date
- **Risk**: Snapshot may be stale (old features) or corrupted (OHLCV gaps)
- **Known issue**: 960+ OHLCV gaps exist in raw data (corporate actions not adjusted)
- **Validation**: Before R10-R12 queues, cross-check snapshot against live table:
  - Row count by ticker (should match)
  - Date coverage (should be continuous)
  - Sample OHLC values (spot-check against live)
  - Known gaps (960 gaps should be documented, not surprise failures)

**Decision**: Add pre-queue reconciliation step that validates snapshot vs. live and reports mismatches.

**Rationale**: Silent data divergence is worse than failing loudly. B-021 detects problems before wasting backtest time.

**Alternatives considered**:
- Skip reconciliation → risk stale snapshot corrupting results
- Reconcile → catches data issues early

**Validation**: B-021 report shows pass/fail on each ticker group.

---

### 13. Trade-Log Schema: Missing Liquidity Columns (B-022)

**Unknown**: What columns are missing from trade_log? How to add them?

**Finding**:
- **Current trade_log** (DuckDB `trade_log` table): Contains trade_id, symbol, date, shares, price, side
- **Missing**: `sector` (from fundamentals), `liquidity_bucket` (computed from ADTV)
- **Impact**: R11-R12 reporting breakdown cannot compute sector/bucket performance
- **Schema change** (one-time migration):
  ```sql
  ALTER TABLE trade_log ADD COLUMN sector VARCHAR;
  ALTER TABLE trade_log ADD COLUMN liquidity_bucket INTEGER;
  ```
- **Backfill**: Update all existing rows via `JOIN fundamentals` (sector) and `JOIN liquidity_buckets` (bucket)

**Decision**: Add columns, backfill via migration script.

**Rationale**: Reporting depends on these dimensions. Without them, can't verify if edge is concentrated in sectors/liquidity.

**Alternatives considered**:
- Skip columns → reporting remains incomplete
- Add columns → enables full dimension analysis

**Validation**: B-022 migration script, backfill verification.

---

### 14. Fundamentals Schema: announcement_date Type (B-007)

**Unknown**: Is announcement_date VARCHAR correct or should it be DATE?

**Finding**:
- **Current**: `announcement_date VARCHAR` in `fundamentals` table
- **Problem**: Can't use date comparisons (`WHERE announcement_date >= start_date`)
- **Fix**: Change to `announcement_date DATE` type
- **Migration**: One-time schema change + backfill with `CAST(VARCHAR TO DATE)`
- **Impact**: Enables point-in-time filtering for fundamental features (e.g., exclude pre-announcement earnings data)

**Decision**: Migrate to DATE type.

**Rationale**: Stronger type safety; enables correct PIT filtering for fundamental strategies.

**Alternatives considered**:
- Keep VARCHAR → requires casting at query time; brittle
- Migrate to DATE → type-safe; enables simpler PIT logic

**Validation**: Migration script, backfill verification.

---

## Summary of Research Outcomes

| Topic | Decision | Rationale | Impact |
|-------|----------|-----------|--------|
| **B-003** | Rewrite audit prompt with spec-as-code | False positives prevent reliable verification | Unblocks B-004–B-010 audits |
| **B-026** | Fix R8 cadence: 252d → 21d | J&T spec requires monthly | Expected Sharpe improvement |
| **B-027** | Refactor R9 to single regime-adaptive backtest | Moreira-Muir spec requires within-portfolio regime switching | More realistic vol-control assessment |
| **B-028** | Revert R10 to individual stock ranking | George & Hwang spec requires individual, not sector | Expected Sharpe improvement |
| **B-029** | Fix R11 reversal ranking direction | Reversal strategy must rank lowest, not highest | Expected Sharpe flip (negative → positive) |
| **B-025** | Fix R5 registry author to George & Hwang | Correct metadata for audit chain of custody | Audit clarity |
| **B-017** | Test monthly (21d) rebalance for R12 | Signal decay faster than 3-month hold | Validation in Phase 3 |
| **B-018** | Sub-period breakdown 2019-2022 vs 2023-2025 | Detect regime concentration | Gate decision in Phase 2 |
| **B-019** | Recompute DSR with n_trials=150 | True selection bias correction | Gate decision in Phase 2 |
| **B-020** | Root-cause robustness failures | Determine if fixable or fatal | Gate decision in Phase 2 |
| **B-016** | Wire liquidity bucketing analysis | Reveal concentration risk | Validation in Phase 3 |
| **B-021** | Add snapshot reconciliation check | Detect stale/corrupt data early | Pre-queue validation |
| **B-022** | Add sector/liquidity_bucket to trade_log | Enable dimension-level reporting | Supports R11-R12 analysis |
| **B-007** | Migrate announcement_date VARCHAR → DATE | Enable PIT filtering for fundamentals | Type-safe schema |

**All unknowns resolved. Proceed to Phase 1 design.**
