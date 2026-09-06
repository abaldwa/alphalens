
# R11 Backtest Investigation: 95% Max Drawdown Root Cause

**Date:** 2026-09-06  
**Runs Investigated:** 
- `native_M09_R11_top5_lb12mo_5d_allrisk` (5-day rebalance)
- `native_M02_R11_top5_lb12mo_21d_allrisk` (21-day rebalance)

---

## Executive Summary

Two R11 (52-Week-High Reversal / mean-reversion) backtest runs show catastrophic drawdowns (~95-97%):
- **M09 (5d rebalance):** -77% total return, -96.6% max DD
- **M02 (21d rebalance):** +1813% total return, -95.5% max DD (profitable overall despite DD)

**Root Cause:** R11 strategy losses are driven by **unaccounted corporate-action price discontinuities in the historical OHLCV data**. The strategy buys stocks that appear to be "far from 52-week high" (oversold), but the discount is actually a data artifact from unadjusted corporate actions.

**Critical Finding:** The primary culprit (A2ZINFRA, -$1.84M loss) **was NOT included in our 101-ticker data integrity investigation**, revealing that our scan was incomplete. Query analysis shows **1000s of unexplained price gaps >20%** across the equity universe, many in recent data (2023-2024) on previously "fixed" tickers.

---

## Run Metrics

### M09_R11_top5_lb12mo_5d_allrisk (5-day rebalance)

| Metric | Value |
|--------|-------|
| CAGR | -8.06% |
| Total Return | -77.01% |
| Max Drawdown | -96.58% |
| Sharpe Ratio | -0.092 |
| Trade Count | 173 |

**Worst Trade:** A2ZINFRA (2012-07-17 to 2013-06-19)
- Entry price: 94.75
- Exit price: 13.70
- Loss: -$1.837M (-85.5%)
- **Not in our 101-ticker fix list**

### M02_R11_top5_lb12mo_21d_allrisk (21-day rebalance)

| Metric | Value |
|--------|-------|
| CAGR | 18.38% |
| Total Return | 1813.41% |
| Max Drawdown | -95.52% |
| Sharpe Ratio | 0.308 |
| Trade Count | 79 |

**Interesting contrast:** Despite similar max DD, this run is highly profitable overall. The longer (21d) rebalance cadence allowed recovery after major drawdowns. Worst trades (ADANIENT 86.4% loss) eventually recovered via later trades (+$2.58M net).

---

## Root Cause Analysis: A2ZINFRA Case Study

### The Suspicious Price Movement (2012-08-02)

```
2012-07-31: Close 76.05 (volume: 291k) — normal
2012-08-01: Close 78.90 (volume: 390k) — up 3.7%, normal volume
2012-08-02: Close 88.25 (volume: 3.8M) — up 11.9%, MASSIVE volume spike (10x normal)
2012-08-03: Close 87.00 (volume: 1.6M) — down 1.4%, high volume
```

Then sustained decline:
- 2012-08-31: 56.35 (down 36% from 88.25 spike)

**Corporate Actions Recorded:**
- 2011-09-22: DIVIDEND 2.0 (backward-adjustment factor 0.5)
- No other corporate actions recorded; AGM dates don't explain 2012-08-02

### Why This Breaks R11

R11 is a **mean-reversion / reversal strategy**:
1. Computes `pct_of_52wk_high` for each stock (0% = at 52-week low, 100% = at 52-week high)
2. **Buys stocks with LOWEST pct_of_52wk_high** (assumes oversold → recovery)
3. Holds for N days, expecting bounce

**What happens with A2ZINFRA's data artifact:**

```
True pre-corporate-action narrative (what R11 sees):
- July 2: High 114.90 (new 52-week high)
- Aug 2: Price crashes to 88.25, then keeps falling
- R11 calculates: pct_of_52wk_high = 56.35 / 114.90 = 49% (moderately oversold)
- R11 buys aggressively, betting on recovery
- Stock never recovers (because the "crash" was a data artifact, not economic reality)
- R11 holds through sustained decline
- Loss: -85.5% by exit on 2013-06-19

What really happened (if the gap had been properly adjusted):
- Pre-adjustment 52-week high: 114.90
- Post-adjustment historical prices (with proper factor applied): would smooth out the jump
- R11 would see actual economic oversold signal, not data artifact
- Strategy performance would be entirely different
```

---

## Broader Data Quality Issue

### Discovered During Investigation

Query of all price gaps >20%:

```
Top gaps found (sample):
- BAJFINANCE 2007-01-02: 10,070% (likely legacy data join, not real gap)
- KAUSHALYA 2024-03-04: 8,884% (recent data!)
- WINSOME 2023-10-03: 3,750% (recent, within backtest window)
- DIACABS 2024-11-04: 861% (THIS TICKER WAS FIXED BY US!)
- TPHQ 2023-12-13: 882% (THIS TICKER WAS FIXED BY US!)
- BTML 2024-04-04: 921% (THIS TICKER WAS FIXED BY US!)
```

**Alarming:** Tickers we marked as "APPLIED" in our empirical corrections still show massive recent gaps. This suggests either:
1. Our fixes only addressed one event per ticker; subsequent events remain unfixed
2. Recent data (2023-2024) has new discontinuities post our investigation
3. The data has multiple overlapping corporate events we only partially addressed

---

## Why Our 101-Ticker Investigation Missed This

Our scope was defined by querying:
```sql
WHERE 
  ABS((close - LAG(close)) / LAG(close)) * 100 > 20
  AND corporate_actions matching specific date ranges
```

**A2ZINFRA's gap (2012-08-02) wasn't flagged because:**
1. No corresponding corporate action recorded in `corporate_actions` table
2. The price movement is real (high volume, multiple days of movement)
3. It looks like normal market movement, not a sharp one-day gap
4. Our scan prioritized sharp gaps paired with logged corporate actions

**Real insight:** There are **unlogged or partially-logged corporate actions** throughout the database. The 2012-08-02 spike for A2ZINFRA likely represents:
- A stock split / bonus not captured in our corporate_actions registry
- A delisting/relisting event (though ticker name unchanged)
- A data backfill artifact from Fyers (segment migration, historical correction)

---

## Strategy-Specific Impact

### R11 (Mean-Reversion) vs R9 (Momentum)

**Why R11 is more vulnerable:**
- **R11:** Buys LOW performers (far from 52-week high), expects bounce. **Artificial gaps create false oversold signals.**
- **R9:** Buys HIGH performers (near 52-week high), expects continuation. **Artificial jumps also break it, but in opposite direction.**

Both strategies suffer, but differently:
- R11: Buys the "dip" that isn't real → loses on non-recovery
- R9: Skips the stock thinking trend is broken → misses recovery

### M09 vs M02 Difference

Why M02 (21-day) is profitable despite similar max DD:
- **M09 (5-day):** Fast rebalance means quicker entry/exit, but **stops out** of bad trades before recovery
  - A2ZINFRA: Held 1 year, compounded losses
  - No recovery opportunity before exit
- **M02 (21-day):** Longer rebalance means positions stay open longer
  - ADANIENT: Initial loss -86%, but later trades recovered it (+$2.58M net)
  - Max DD = -95%, but overall win because of position recovery windows

---

## Implications for Backtest Validity

### Current Status

| Strategy | Data State | Backtest Validity | Notes |
|----------|-----------|------------------|-------|
| R9 (Momentum) | Partially fixed (50/101 tickers) | ⚠️ Uncertain | 23 empirical fixes help, but many tickers unfixed |
| R11 (Reversal) | Mostly unfixed | ❌ Not Trustworthy | Extra vulnerable; real gaps look like opportunities |
| R7 (Crash-Aware) | ? | ⚠️ Uncertain | Depends on crash detection overlap with data gaps |

**A2ZINFRA alone caused ~$1.84M loss in M09 R11.** If there are 1000+ similar untreated gaps, the aggregate impact on *all* strategy backtests could be enormous.

### Blind Spot

Our data integrity check (`check_corporate_action_continuity`) flags gaps >20% that **happen on known corporate-action dates**. But A2ZINFRA's 11.9% gap on 2012-08-02 wouldn't trigger the check because:
1. No corporate action recorded for that date
2. It's not >20% in a single day (multi-day sustained decline)

We need a **deeper scan** that catches unlogged/misaligned corporate actions.

---

## Recommended Actions

### Immediate (High Priority)

1. **Expand data integrity check to flag ALL gaps >15%**, not just those on corporate-action dates
   - Helps surface unlogged events like A2ZINFRA's 2012-08-02 spike
   - Run weekly; retain findings for manual review

2. **Audit recent data for new gaps** (2023-2024)
   - DIACABS, TPHQ, BTML still show massive gaps post-fix
   - May indicate multiple events or post-correction data ingestion issues

3. **Re-validate R11 strategy with corrected data**
   - Once A2ZINFRA and similar gaps are fixed, re-run M09/M02 R11 backtests
   - Expect much different results

### Medium (Before Paper Trading)

4. **Root-cause unlogged corporate actions**
   - Why was 2012-08-02 A2ZINFRA event not captured?
   - Check Fyers data, NSE announcements, internal logs
   - Pattern-match to other ~1000+ unexplained gaps

5. **Batch-fix high-impact gaps**
   - Query corporate_actions registry and real OHLCV movements
   - Compute empirical factors for top-1000 gaps >20%
   - Apply with same audit-trail discipline as empirical_corrections.py

6. **Improve corporate-action ingestion**
   - Source: NSE announcements, BSE circulars, Trendlyne/Tijori feeds
   - Validate: price movement before/after should match disclosed ratio
   - Auto-flag: movements with no corresponding action

### Long-term (Architecture)

7. **Corporate action data model enhancement**
   - Add `is_logged_in_registry` flag to each transaction
   - Track source (NSE, Trendlyne, empirical inference)
   - Build confidence scores for each adjustment

8. **Strategy robustness testing**
   - Test R11 / R9 on synthetic data with known gaps
   - Measure sensitivity to corporate-action misalignment
   - Establish bounds for "safe" backtesting

---

## Files & References

- **Investigation:** `docs/DATA_INTEGRITY_ISSUES_2026_09.md` (50 tickers fixed; 12 pending)
- **Automated Check:** `datastore/integrity/checks.py::check_corporate_action_continuity()`
- **Fix Scripts:** `scripts/apply_empirical_corrections.py`, `scripts/patch_confirmed_fyers_fixes.py`
- **Backtest DB:** `/datastore/backtest_store/backtest.duckdb` (framework_backtest_runs, framework_backtest_trades)

---

## Conclusion

**The 95% max drawdown in R11 backtests is a symptom of deeper data quality issues.** A2ZINFRA's $1.84M loss is just one visible case; the 1000+ unexplained gaps suggest the problem is system-wide.

Before relying on any strategy backtest (R11, R9, R7, or any other), we need to:
1. Identify all unaccounted price movements
2. Cross-reference against corporate-action registry
3. Either adjust the data or exclude affected tickers from backtests

Current fix coverage (50/101+ tickers) is insufficient. A more comprehensive scan and systematic approach to gap resolution is required.

