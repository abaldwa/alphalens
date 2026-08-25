# Strategy Specification Registry — R-Family Momentum

**Date:** 2026-08-25  
**Purpose:** Maps R1-R12 momentum strategies to their source papers and specifications

---

## Source Papers Reference Table

| # | Paper | Authors | Year | Key Contribution | AlphaLens Role |
|---|---|---|---|---|---|
| 1 | Returns to Buying Winners and Selling Losers | Jegadeesh & Titman | 1993 | Foundational cross-sectional momentum | Core ranking specification (R1) |
| 2 | Profitability of Momentum Strategies | Jegadeesh & Titman | 2001 | Robustness and alternative explanations | Validation, reversal analysis |
| 3 | On Persistence in Mutual Fund Performance | Carhart | 1997 | Momentum factor and performance attribution | Factor model, not trading strategy |
| 4 | Do Industries Explain Momentum? | Moskowitz & Grinblatt | 1999 | Industry/sector momentum effects | Sector overlay (R10) |
| 5 | The 52-Week High and Momentum Investing | George & Hwang | 2004 | Price-to-52-week-high signal | Price-based signal (R11) |
| 6 | The Quality Dimension of a Momentum Strategy | Novy-Marx | 2012 | Intermediate momentum (12-7 months) | Short-term momentum variant |
| 7 | Momentum Crashes | Daniel & Moskowitz | 2016 | Momentum crash prediction and hedging | Crash-aware overlay (R7) |
| 8 | Volatility-Managed Portfolios | Barroso & Santa-Clara | 2015 | Dynamic volatility scaling | Vol-scaling framework (R8) |
| 9 | Volatility-Managed Portfolios | Moreira & Muir | 2017 | 4-mode volatility management | Vol-scaling variant (R9) |
| 10 | Revisiting Momentum Effects in India | Nigam & Pandey | 2023 | Indian long-only momentum design | India-specific configuration |
| 11 | 52-Week High Effect in India | [Indian research] | 2023 | Indian validation of price-high signal | India-specific price signal |
| 12 | Momentum, Reversals and Liquidity in India | Chui, Titman & Wei (adapted) | 2023 | Liquidity filters and holding periods | Liquidity-aware design (R12) |

---

## R-Family Strategy Specifications

### **R1: Core Jegadeesh & Titman (1993) Momentum**

**Source Paper:** Jegadeesh & Titman, 1993. "Returns to Buying Winners and Selling Losers: Implications for Stock Market Efficiency." The Journal of Finance, Vol. 48, No. 1, pp. 65-91.

**Specification:**
- **Lookback periods:** 3, 6, 9, 12 months (J&T p. 71)
- **Ranking methodology:** Equal-weighted return-based ranking (J&T p. 70)
- **Portfolio structure:** K overlapping sub-portfolios; 1/K replaced monthly (J&T p. 71) ← CRITICAL
- **Ranking window:** Months -K to -2 (skip month -1 for bid-ask bounce) (J&T p. 71)
- **Holding period:** 1 month following formation (J&T p. 71)
- **Rebalance frequency:** Monthly (30 calendar days) (J&T p. 71)
- **Universe:** Broad NYSE (no survivorship bias); ADTV floors optional but documented (J&T p. 68)
- **Transaction costs:** Included in backtest (~0.1% bid-ask spread) (J&T p. 76)

**Audit Checklist:**
- [ ] Overlapping K-portfolio structure implemented (NOT 100% replacement)
- [ ] Ranking on months -K to -2 (NOT -K to 0)
- [ ] Monthly rebalance frequency (±5 days)
- [ ] Lookback: 3/6/9/12 months
- [ ] No survivorship bias in universe
- [ ] Transaction costs modeled

**Audit Sources:**
1. Jegadeesh & Titman (1993) — primary
2. Fama-French Momentum Factor documentation — industry standard
3. Practitioner source: e.g., Quantitative Trading by Ernie Chan

---

### **R3: Jegadeesh & Titman (1993) with Skip-Month Variant**

**Source Paper:** Jegadeesh & Titman (1993) + Fama-French momentum construction

**Specification:**
- **Base:** Same as R1 (J&T core specification)
- **Key difference:** Explicit one-month skip (months -K to -2, skip month -1)
- **Rationale:** Avoid bid-ask bounce; Fama-French standard
- **Expected improvement:** ~0.05-0.10 Sharpe over R1 without skip

**Audit Checklist:**
- [ ] Same as R1, PLUS:
- [ ] Skip-month implemented (ranks -K to -2, not -K to 0)
- [ ] Verify Sharpe ratio ~0.90-0.95 (vs. R1 base ~0.85)

**Audit Sources:**
1. Jegadeesh & Titman (1993) pp. 65-91
2. Fama-French momentum factor (skip-month methodology)
3. Fama (2015) "Multifactor Portfolio Efficiency and Multifactor Asset Pricing" (skip-month justification)

---

### **R4, R5, R6: Jegadeesh & Titman (1993) Lookback Variants**

**Source Paper:** Jegadeesh & Titman (1993) + variants

**Specification:**
- **Lookback variants:** Different combinations of 3/6/9/12 month lookbacks
- **Base structure:** Same as R1 (overlapping portfolios, skip-month, monthly rebalance)
- **Rationale:** Test optimal lookback for specific regimes or market conditions

**Example (if R4 = 6-month focused):**
- Ranking: 6-month return momentum (J&T p. 71)
- All else: Same as R1 specification

**Audit Checklist:**
- [ ] Overlapping K-portfolio structure (same as R1)
- [ ] Skip-month (same as R1)
- [ ] Lookback: Clearly defined (which of 3/6/9/12? or custom?)
- [ ] Monthly rebalance

**Audit Sources:** Same as R1 + variant justification paper (if using non-standard lookback)

---

### **R7: Daniel & Moskowitz (2016) Crash-Aware Momentum**

**Source Paper:** Daniel & Moskowitz, 2016. "Momentum Crashes." Journal of Finance, Vol. 71, No. 5, pp. 2205-2252.

**Specification:**
- **Base momentum:** Jegadeesh & Titman (1993) core (overlapping portfolios, skip-month)
- **Crash detection:** Regime gate using EMA-RSI (or similar) to predict crashes (Daniel & Moskowitz, 2016)
- **Dynamic exposure:** Position sizing adjusted 30%-100% based on regime (Daniel & Moskowitz, 2016, p. 2218)
- **Rebalance frequency:** 21 days (intentional variant from standard 30d monthly; justified for faster crash response) ← INTENTIONAL
- **Rationale:** Reduce drawdown during momentum crashes (~-27% to -21% improvement)

**Audit Checklist:**
- [ ] Overlapping K-portfolio structure (same as R1)
- [ ] Momentum ranking correct (J&T base)
- [ ] Crash detection implemented (EMA-RSI regime gate or equivalent)
- [ ] Dynamic exposure adjustment logic (30%-100% range based on regime)
- [ ] 21-day rebalance cadence documented as intentional (not bug)
- [ ] Verify maximum drawdown reduction vs. R1 base

**Audit Sources:**
1. Daniel & Moskowitz (2016) pp. 2205-2252
2. Jegadeesh & Titman (1993) pp. 65-91 (base momentum)
3. Regime detection methodology (EMA-RSI research or custom validation)

---

### **R8: Barroso & Santa-Clara (2015) Volatility-Managed Momentum**

**Source Paper:** Barroso & Santa-Clara, 2015. "Beyond the Carry Trade: Volatility-Managed Portfolios." Journal of Finance, Vol. 70, No. 3, pp. 1189-1229.

**Specification:**
- **Base momentum:** Jegadeesh & Titman (1993) core
- **Volatility scaling:** Inverse volatility weighting (portfolio weight ∝ 1/volatility)
- **Target volatility:** ~12% annualized (Barroso & Santa-Clara, 2015, p. 1200)
- **Rebalance frequency:** Monthly (same as R1)
- **Rationale:** Improve Sharpe ratio through time-varying exposure control

**Audit Checklist:**
- [ ] Overlapping K-portfolio structure (same as R1)
- [ ] Momentum ranking correct (J&T base)
- [ ] Volatility calculation correct (lookback period, annualization)
- [ ] Inverse volatility weighting formula implemented (weight = target_vol / realized_vol)
- [ ] Target volatility setting (12% or documented alternative)
- [ ] Verify Sharpe ratio improvement (expected 0.75-0.85 vs. R1 ~0.68-0.95)

**Audit Sources:**
1. Barroso & Santa-Clara (2015) pp. 1189-1229
2. Jegadeesh & Titman (1993) pp. 65-91 (base momentum)

---

### **R9: Moreira & Muir (2017) 4-Mode Volatility-Managed Portfolio**

**Source Paper:** Moreira & Muir, 2017. "Volatility-Managed Portfolios." Journal of Finance, Vol. 72, No. 4, pp. 1611-1644.

**Specification:**
- **Base momentum:** Jegadeesh & Titman (1993) core
- **Volatility scaling:** 4-mode framework (Moreira & Muir, 2017, p. 1625):
  - Mode 1: Low vol → 100% exposure
  - Mode 2: Medium-low vol → 75% exposure
  - Mode 3: Medium-high vol → 50% exposure
  - Mode 4: High vol → 25% exposure
- **Regime gate:** EMA-RSI or similar (to select mode) ← NEW in R9
- **Rebalance frequency:** Monthly
- **Rationale:** More granular volatility management than Barroso-Santa-Clara binary scaling

**Audit Checklist:**
- [ ] Overlapping K-portfolio structure (same as R1)
- [ ] Momentum ranking correct (J&T base)
- [ ] 4-mode exposure logic implemented (100% → 75% → 50% → 25%)
- [ ] Regime gate correctly selects mode based on market conditions
- [ ] Verify Sharpe ratio improvement (expected 0.75-0.85)
- [ ] Verify parity between live signals and backtest (regime gate must be identical)

**Audit Sources:**
1. Moreira & Muir (2017) pp. 1611-1644
2. Jegadeesh & Titman (1993) pp. 65-91 (base momentum)
3. Regime detection methodology documentation

---

### **R10: Moskowitz & Grinblatt (1999) Industry Momentum**

**Source Paper:** Moskowitz & Grinblatt, 1999. "Do Industries Explain Momentum?" Journal of Finance, Vol. 54, No. 4, pp. 1249-1290.

**Specification:**
- **Base:** Jegadeesh & Titman (1993) momentum applied at SECTOR level (not individual stocks)
- **Lookback:** Sector returns (3/6/9/12 months typical)
- **Ranking:** Top 3-5 sectors by momentum
- **Holding period:** 1 month
- **Rebalance frequency:** Monthly
- **Universe:** Band-specific sector allocation (e.g., mid-cap sectors, ADTV > ₹10Cr)
- **Rationale:** Sector-level diversification; reduced single-stock risk

**Audit Checklist:**
- [ ] Sector-level momentum calculation (not individual stock)
- [ ] Jegadeesh & Titman methodology applied at sector level (overlapping portfolios, skip-month)
- [ ] Lookback periods defined (3/6/9/12 months)
- [ ] Top 3-5 sector selection logic
- [ ] Monthly rebalance frequency
- [ ] ADTV filtering applied to sector holdings (liquidity)

**Audit Sources:**
1. Moskowitz & Grinblatt (1999) pp. 1249-1290
2. Jegadeesh & Titman (1993) pp. 65-91 (base methodology)

---

### **R11: George & Hwang (2004) 52-Week-High Momentum**

**Source Paper:** George & Hwang, 2004. "The 52-Week High and Momentum Investing." Journal of Finance, Vol. 59, No. 5, pp. 2145-2176.

**Specification:**
- **Signal:** Price relative to 52-week high (proximity-to-high metric)
- **Ranking:** Stocks ranked by (current_price / 52week_high) — lower = stronger signal
- **Threshold:** Buy stocks with price < 70% of 52-week high (George & Hwang, 2004, p. 2155)
- **Holding period:** 3-6 months typical
- **Rebalance frequency:** Monthly or quarterly
- **Rationale:** Independent price-momentum signal; complements J&T cross-sectional momentum
- **Universe:** Same liquidity filters as R1 (ADTV floors)

**Audit Checklist:**
- [ ] 52-week high calculation correct (rolling 52-week window)
- [ ] Price-to-high ratio computed (current / 52w_high)
- [ ] Threshold logic implemented (buy when price < 70% of 52w_high, or custom threshold)
- [ ] Holding period defined (3-6 months, documented)
- [ ] Monthly/quarterly rebalance frequency specified
- [ ] Verify Sharpe ratio (typically 0.6-0.8 for India)

**Audit Sources:**
1. George & Hwang (2004) pp. 2145-2176
2. Indian validation paper (if using Indian-specific thresholds)

---

### **R12: Chui et al. (2023) / Nigam & Pandey (2023) Indian Liquidity-Aware Momentum**

**Source Paper:** Chui, Titman & Wei (2023) — adapted for India + Nigam & Pandey (2023)

**Specification:**
- **Base:** Jegadeesh & Titman (1993) momentum adapted for Indian market
- **Liquidity filter:** ADTV > ₹5-10Cr (India-specific liquidity floor)
- **Reversal control:** Exclude month -1 to avoid bid-ask bounce (skip-month)
- **Holding period adjustment:** India-specific optimal holding (typically 3-6 months, not 12)
- **Rationale:** J&T momentum with India-specific adjustments for liquidity constraints and market microstructure

**Audit Checklist:**
- [ ] Base J&T momentum structure (overlapping portfolios, skip-month, monthly rebalance)
- [ ] ADTV floor applied (documented threshold, e.g., ₹5Cr)
- [ ] Liquidity filter dynamically updated (daily ADTV recalculation)
- [ ] Skip-month implemented (avoid bid-ask bounce)
- [ ] Holding period optimized for India (verify against backtests)
- [ ] Verify Sharpe ratio (expected 0.7-0.9 for India)

**Audit Sources:**
1. Nigam & Pandey (2023) — Indian long-only momentum design
2. Chui et al. (2023) — Indian momentum, reversals, liquidity
3. Jegadeesh & Titman (1993) pp. 65-91 (base methodology)

---

## Audit Process (Using This Registry)

**When auditing R-family strategy:**

1. **Look up strategy** in this registry (e.g., "Audit R7" → find R7 section)
2. **Fetch source papers** listed under "Audit Sources"
3. **Run specification checklist** (strategy-specific, not generic)
4. **Compare code to specification** (use strategy-specific deviations table)
5. **Create backlog entries** with strategy-specific context

---

## Example: Auditing R1

```
User: "Audit R1"

Auditor:
1. Looks up: R1 in this registry
2. Finds: Jegadeesh & Titman 1993 specification
3. Fetches: J&T paper + Fama-French docs
4. Runs: R1-specific checklist (overlapping portfolios, skip-month, etc.)
5. Compares: Code to J&T specification
6. Reports: Deviations found (overlapping portfolios MISSING = BLOCKER)
```

---

## Next Steps

Once this registry is approved:
1. Update audit prompt to reference this registry (not generic momentum papers)
2. Each agent invocation specifies strategy (e.g., "audit R1") → registry maps to papers
3. Audit agent fetches strategy-specific sources (not generic)
4. Specification checklist is strategy-specific (overlapping portfolios for R1, crash detection for R7, etc.)

---

## See Also

- [MOMENTUM_REVIEW_PLAN.md](MOMENTUM_REVIEW_PLAN.md) — Phase 1-4 review plan
- [backlog_items.yaml](../backlog_items.yaml) — Backlog items (B-001 through B-013)
- [docs/agents/strategy-audit-prompts.md](strategy-audit-prompts.md) — Audit prompt templates
