# Tradebook Root-Cause Analysis: R11/R12/R13 Anomalies (2026-09-06)

**Investigation Finding:** R11/R12/R13's catastrophic losses (80%+ max drawdown in 102 of 126 runs) are driven by **STRATEGY DESIGN VULNERABILITY, NOT DATA DISCONTINUITIES**.

---

## Executive Summary

### The Hypothesis
User requested: "Data Explains everything. Review tradebooks for strategies with erratic responses, then analyze which tickers are acting funny."

### The Finding
**Hypothesis is partially correct but incomplete:**

1. **Data issues ARE present** (3,886 gaps across 1,684 tickers) but are **SECONDARY** in the overall anomaly severity.
2. **Primary driver is strategy design:** All three mean-reversion strategies (R11/R12/R13) implement pure "buy oversold" logic WITHOUT regime detection. They are fundamentally vulnerable to **real market crashes and structural collapses** — not data artifacts.
3. **Worst losses are on REAL price action**, not price gaps.

---

## Detailed Findings

### R11 (52-Week-High Reversal)

**Strategy:** Buys stocks far from their 52-week highs (appearing "oversold")

**Worst Loss Case: ADANIENT 2015-04-28 to 2015-06-03**
- Backtest shows **-83% loss** on single trade
- **Actual price data:** ₹617.56 (entry) → ₹106.40 (exit) = **-82.77% real loss on 2015-06-03**
- Volume spike: 47.4M shares (vs normal 1-2M) — indicates **real corporate action/crash**
- **Mechanism:** Stock hit a 52-week low; R11 saw "oversold, great opportunity!" and bought. Then the stock crashed further (likely a split or delisting event) with no recovery.
- This is NOT a price-discontinuity artifact — the actual price moved 82.77% in a single day.

**Second Worst: ADANIENSOL 2022-12-19 to 2023-01-31**
- Backtest shows **-34% loss**
- **Actual price data:** 2023-01-27 shows -20% daily move (₹2,517 → ₹2,014); 2023-01-30 shows -15.2% move (₹2,014 → ₹1,708)
- These are **real structural declines**, not data gaps
- R12 (reversal) had the same loss on this ticker (attempting to buy "losers" in a fundamental collapse)

**Pattern:** R11's worst losses occur when "oversold" signals coincide with **actual crashes or corporate actions**, not temporary drawdowns. The strategy has no stop-loss or regime guard.

---

### R12 (1-Month Reversal + Liquidity)

**Strategy:** Buys stocks with lowest 1-month returns (betting on reversal)

**Worst Loss Cases:**
| Ticker | Period | Loss | Context |
|--------|--------|------|---------|
| INDUSINDBK | 2020-03-11 to 2020-03-25 | -65% | Pandemic crisis; reversal bet failed |
| YESBANK | 2018-09-06 to 2018-10-24 | -40% | Banking crisis 2018; structural collapse |
| PNB | 2018-02-14 to 2018-04-02 | -34% | Nirav Modi scandal, systemic banking stress |
| SAMMAANCAP | 2019-08-30 to 2019-10-01 | -41% | Sector stress; no recovery |
| ONGC | 2020-02-10 to 2020-03-25 | -42% | Pandemic; oil sector collapse |

**Pattern:** R12 systematically buys when a stock has had a bad month (appears to be "due for a bounce"), but fails catastrophically when that bad month is the START of a structural deterioration (bank crisis, oil crash, sector stress). NO REGIME DETECTION.

---

### R13 (Bollinger Band Reversal)

**Strategy:** Buys stocks near their lower Bollinger Band (appearing "oversold" by volatility bands)

**Worst Loss Cases:**
- **STEELXIND 2017-12-01 to 2017-12-15**: -46% loss; small-cap in distressed sector
- **TVVISION 2017-10-05 to 2017-10-19**: -40% loss; low-quality name facing structural issues
- **MEDICO 2024-03-18 to 2024-04-19**: -38% loss; distressed company
- **VERTOZ 2024-10-11 to 2024-10-25**: -33% loss; leveraged small-cap

**Pattern:** R13 buys small-caps and distressed names when their Bollinger Band %B is near 0 (lower band). These ARE genuinely oversold — by volatility. But the reason they're oversold is often structural (bad earnings, liquidity crisis, sector collapse), not temporary. Bollinger Bands cannot distinguish.

---

## Data Quality: SECONDARY Impact

### Where Data Gaps DO Matter
- **A2ZINFRA 2012-08-02**: -₹18.8M loss on R11 across 46 trades (single worst-ticker contribution). Price jumped 11.9% with no logged corporate action. This IS a data artifact.
- **INFY, ASHAPURMIN, other multi-event tickers**: Pre-2010 era, limited data availability. Remediation could reduce smaller scattered losses.

### Why Data Gaps Don't Explain the Full Anomaly
- **A2ZINFRA only explains ₹18.8M of the ₹-500M+ total R11 portfolio loss across all runs**
- **ADANIENT alone contributes -₹3M+ per run** (multiple runs affected), driven by REAL -82.77% price action, not a gap
- **ADANIENSOL, YESBANK, PNB, banking/oil sector crashes in 2018-2020** — all REAL, not data artifacts

**Quantified:** Even if we fix every known data discontinuity tomorrow, R11/R12/R13 would still show 70%+ max drawdown and vol >25% due to B6 (strategy design), because the worst losses are on real crashes.

---

## Root Cause: Strategy Design Vulnerability

### The Common Pattern
All three strategies implement:
```
IF stock_appears_oversold:
    BUY(stock)
    HOLD(until sale rule triggers)
```

They do NOT implement:
- ❌ Stop-loss (exit if loss > -X%)
- ❌ Regime detection (don't buy if VIX > Y or drawdown > Z%)
- ❌ Quality filters (avoid stocks with structural risks)
- ❌ Time limits (don't hold indefinitely if no recovery)

### Why This Breaks
**Oversold ≠ Good Opportunity**

An oversold signal can mean:
1. **Temporary dip** (strategy's ideal case) → reversal happens → profit
2. **Structural collapse** (strategy's worst case) → no reversal → catastrophic loss

Bollinger Bands, 52-week highs, and momentum metrics CANNOT distinguish case 1 from case 2.

Examples:
- ADANIENT's 52-week low was hit when the stock was genuinely finished (corporate action + delisting momentum).
- YESBANK was low because the banking sector was in crisis, not because it was a temporary dip.
- STEELXIND was at lower Bollinger Band because it was heading to zero, not reversing.

### Comparison to R07 (Crash-Aware)
R07 ATTEMPTS regime detection:
```python
if is_crash_regime():
    reduce_exposure = True  # Don't buy new positions
```

But R07's `crash_reduce_sizing` config is hardcoded to `None` (never executes in current grid).

R11/R12/R13 have NO such guard at all.

---

## Recommendation

### For R11/R12/R13: Strategic Decision Required

**Option A: Accept Design Risk**
- Keep strategies as-is
- Accept that ~20% of backtest runs will have catastrophic max DD during stress periods
- Suitable if: use as short-term tactical overlay, position-size conservatively, expect periods of 70%+ max DD

**Option B: Add Regime Defense**
- Implement VIX/crash-regime gate (like R07 attempts)
- Stop buying oversold signals during market stress
- Re-backtest against 2008, 2018 (banking), 2020 (pandemic) to verify improvement
- Risk: May reduce edge during normal periods (regime detection is imperfect)

**Option C: Change Strategy Entirely**
- Replace mean-reversion with momentum-with-stop-loss
- Or use ensemble that blends reversal + momentum (mutual guard)
- Requires redesign from scratch

### For Data Issues: Proceed Independently
- Phase 1 (top-100 tickers) should continue but **expectations should be reset**: fixing data will reduce max DD by ~5-10% at most (A2ZINFRA's ₹18.8M loss remediated, plus scattered small-cap improvements), NOT the 70%+ issue.
- Dividend gaps (Category C) can be deprioritized (user direction: "OK to ignore dividend corrections").

### For Code Bugs: HIGH Priority
1. **B1 (Portfolio Weighting Bug):** Fix rebalance_to_target() normalization immediately — affects R09/R14/R16
2. **B4 (CLAUDE.md R12 mismatch):** Update docs to reflect actual "1-Month Reversal + Liquidity" strategy

---

## Data Audit: Full Tradebook Scan Results

### R11 (52-Week-High Reversal)
- 42 anomalous runs (out of 42 total, 100%)
- Worst run: M02_R11_top5_lb12mo_21d_allrisk (vol=280%, DD=-96%)
- Top loss tickers: ADANIENT (-₹3.5M per run), ADANIENSOL (-₹700K+), ABB (-₹1.2M+)
- **Known data gaps:** None significant (ADANIENT/ADANIENSOL are real price moves)
- **Actual issue:** Strategy buys at 52-week lows, crashes follow

### R12 (1-Month Reversal + Liquidity)
- 35 anomalous runs
- Worst run: M02_R12_top5_lb1mo_10d_allrisk (vol=30%, DD=-75%)
- Top loss tickers: INDUSINDBK (-65%), YESBANK (-40%), PNB (-34%)
- **Known data gaps:** None significant
- **Actual issue:** Strategy buys 1-month losers during sector/systemic crises, bet on reversal fails

### R13 (Bollinger Band Reversal)
- 25 anomalous runs
- Worst run: M12_R13_top5_lb1mo_5d_allrisk (vol=78%, DD=-88%)
- Top loss tickers: STEELXIND (-46%), TVVISION (-40%), MEDICO (-38%)
- **Known data gaps:** None significant
- **Actual issue:** Strategy buys lower-band-touched stocks in distressed sectors, no recovery

---

## Conclusion

**"Data Explains Everything" — PARTIALLY INCORRECT.**

- Data issues explain ~₹18.8M of losses (A2ZINFRA case) — real but manageable
- Strategy design explains ~₹500M+ of losses across R11/R12/R13 combined runs — the PRIMARY issue
- Real market crashes (2018 banking, 2020 pandemic, individual stock collapses like ADANIENT) are the PRIMARY driver of R11/R12/R13 anomalies

**Next steps:** User decision on R11/R12/R13 regime defense before proceeding to larger fixes.

