# AlphaLens — Human-in-the-Loop (HITL) Test Cases
## Most Complex · Maximum Business Value · Require Human Judgment

**Purpose:** These test cases cannot be automated because they require domain expertise,
contextual judgment, and real-world market knowledge. A human tester with Indian market
knowledge must execute these before the system goes live.

**Selection criteria for HITL cases:** Each case selected because it (a) tests the boundary
between model output and human judgment, (b) has direct financial consequences if wrong,
and (c) tests the system's ability to surface the right information for human decision-making.

---

## HITL-01 · Promoter Pledge Spiral — Exit Decision
**Business value:** CRITICAL — prevents catastrophic position loss
**Complexity:** HIGH — requires understanding of India-specific governance risk dynamics

### Context
Promoters pledge shares as collateral for loans. When stock price falls, lenders issue
margin calls. Promoters sell pledged shares to meet calls, which drops price further,
triggering more calls. This spiral has destroyed several large Indian companies (Yes Bank,
DHFL, Vakrangee). The system must surface this risk and recommend exit before the spiral
accelerates.

### Test Setup
Use a stock where promoter pledge has risen from 5% → 32% over 6 months AND stock
price has fallen 25% in the same period. Forensic model has flagged `promoter_pledge_spiral_risk > 70`.

### What to Verify (Human)
1. **Does the exit alert correctly identify "Promoter Pledge Risk" as the exit type?**
   Not generic "Momentum Exhaustion" — the specific reason must be surfaced.

2. **Is the pledge trend chart visible?** The tester should see a 6-quarter chart showing
   pledge evolution. The acceleration in the last 2 quarters should be visually prominent.

3. **Does the system explain the cascade risk?**
   The alert should state: "Pledge at 32%. If price falls 10%, estimated margin call triggers
   additional 8% forced selling. Feedback loop risk."

4. **Is the recommended action proportionate to urgency?**
   With pledge > 30% AND price falling: urgency should be > 80 (exit today).
   Not urgency 55 (monitor).

5. **Does the system distinguish between high-pledge high-quality promoter vs distress?**
   Some quality companies temporarily pledge for legitimate expansion. The tester must
   verify the system does NOT flag these at the same urgency as distress cases.
   Test with a known case: Bajaj Finance promoter pledge history is clean → should not fire.

### Pass Criteria
- Exit type correctly labeled "Promoter Pledge Risk"
- Pledge trend chart present and accurate
- Cascade risk quantified (not just flagged)
- Urgency ≥ 80 for spike > 30% with falling price
- No false positive on clean Bajaj Finance-style promoter

### Document
Record: screenshot of alert, exit type label, urgency score, pledge chart, recommended action.

---

## HITL-02 · P&D During Legitimate News — False Positive Test
**Business value:** HIGH — prevents missing a genuine opportunity due to false P&D block
**Complexity:** VERY HIGH — requires distinguishing manipulation from genuine momentum

### Context
A genuine breakout (e.g., company wins large government contract) produces similar
technical signatures to a pump-and-dump: volume spikes 8x, price up 40% in 5 days,
delivery may be lower than usual (momentum traders). The P&D model must NOT block this.

### Test Setup
Construct two scenarios from historical data:

**Scenario A (genuine):** Pick a known genuine breakout from 2023–2024 Indian markets
where a Tier 2/3 stock surged on authentic fundamental news (contract win, order book
data, M&A). Verify the stock was subsequently covered by analysts confirming the news.

**Scenario B (manipulation):** Pick a known confirmed P&D episode from SEBI enforcement
actions. Use only data available at the time of the pump (not the subsequent SEBI finding).

### What to Verify (Human)
1. **Does Scenario A receive a P&D score < 40?**
   If the system blocks genuine breakouts, it destroys real alpha.
   The tester must verify the delivery data, volume profile, and news context all align.

2. **Does Scenario B receive a P&D score > 60?**
   The manipulation must be caught using only features available at the time.

3. **Are the distinguishing features visible?**
   For the genuine case: delivery % should be reasonably high (40%+),
   the gap between `parabolic_curve_score` and `delivery_accumulation_score` should be small.
   For the P&D case: delivery collapse is the key differentiator.

4. **Does the SHAP explanation help the user understand the score?**
   A SHAP waterfall showing "delivery_pct_collapse drove 35% of this score" is useful.
   A list of numbers without explanation is not.

5. **What is the false positive rate over the last 3 months of live data?**
   Acceptable: < 5% of genuine momentum stocks incorrectly blocked.
   The tester must manually review blocked stocks and classify each as TP or FP.

### Pass Criteria
- Genuine breakout: P&D score < 40
- Confirmed manipulation: P&D score > 60
- SHAP explanation correctly identifies key drivers
- FP rate over 3-month review < 5% of universe stocks

---

## HITL-03 · Multibagger Archetype Validation — Historical Analogue Quality
**Business value:** HIGH — core multibagger model value to user
**Complexity:** HIGH — requires deep knowledge of Indian market history

### Context
The multibagger model identifies 3 "historical analogues" for each candidate — prior stocks
that were in a similar technical and fundamental position and went on to 3x–10x. The quality
of these analogues directly determines whether the recommendation is credible.

### Test Setup
Run the multibagger model on a set of 20 current candidate stocks. For each stock, review
its 3 historical analogues.

### What to Verify (Human)
1. **Are the analogues genuinely similar?**
   If the current candidate is a B2B industrial company with an order book breakout and
   base formation after 2 years of sideways movement, the analogues should be similar
   companies (same sector, similar size, similar technical setup) — not a 2020 pharma
   COVID stock.

2. **Are the analogues drawn from the same market regime?**
   A 2020 bull-run compounder is NOT a valid analogue for a 2024 mid-cycle stock.
   Regime context must match.

3. **Do the analogue price charts look visually similar?**
   The system should show overlay charts. The tester must confirm the pattern resemblance
   is genuine, not just statistical correlation on irrelevant features.

4. **Does the archetype assignment make business sense?**
   "Long Base Breakout" assigned to a stock that's been consolidating 18 months and just
   crossed above its 200-day MA — correct.
   "Long Base Breakout" assigned to a stock that's been falling for 2 years — incorrect.

5. **Is the survival curve calibrated?**
   The tester should check: "Of stocks previously in this archetype with similar probability,
   what fraction actually 2x'd within 2 years?" The system's stated probability should
   roughly match the historical hit rate.

### Pass Criteria
- ≥ 80% of analogues judged as "genuinely similar" by the tester
- Archetype labels make business sense for ≥ 90% of candidates
- Survival curve stated probability within ±10% of historical hit rate for that archetype

---

## HITL-04 · Market Regime Transition — Model Behavior Under Stress
**Business value:** CRITICAL — system must handle crashes differently from uptrends
**Complexity:** HIGH — requires judgment on regime correctness and model dampening

### Context
During market crashes (e.g., March 2020, September 2022 India rate shock), the HMM
regime model should quickly classify the market as Bearish and reduce all position sizes
by 50%. Signal models running in Bearish regime should generate far fewer Buy signals.
The tester validates that the system would have behaved correctly in hindsight.

### Test Setup
Run the system on historical data spanning September–October 2022 (Nifty fell ~10% in
6 weeks) and verify model behavior at the transition points.

### What to Verify (Human)
1. **Did the HMM correctly transition to Bearish regime within 3–5 trading days of the drawdown starting?**
   A 10-day lag is too slow (most of the damage is done). A 2-day transition is ideal.
   The tester must look at the regime transition dates and compare to the actual market.

2. **Did position sizing halve correctly in Bearish regime?**
   Any signal that fired after the Bearish regime transition should show half the normal
   position size. The tester must verify this in the signal output log.

3. **Did Buy signal frequency drop materially in Bearish regime?**
   If the system generates the same number of Buy signals in a crash as in a bull market,
   the regime conditioning is not working. Acceptable drop: ≥ 50% fewer Buy signals.

4. **Were there any signals that should have been blocked by Bearish regime but weren't?**
   Manual review of all Buy signals during the crash period. Any high-risk signals that
   slipped through indicate a model conditioning failure.

5. **Did the regime correctly transition BACK to Bullish/Sideways within 2 weeks of the market recovery?**
   A model stuck in Bearish regime during a recovery misses the rebound entirely.

### Pass Criteria
- HMM regime transition: within 5 trading days of start of 10%+ drawdown
- Position sizes halved within 1 trading day of Bearish regime onset
- Buy signal frequency drops ≥ 50% in Bearish regime
- Recovery transition within 15 trading days of market bottom

---

## HITL-05 · Signal Explanation Quality — SHAP Interpretability
**Business value:** HIGH — user must trust and understand signals to act on them
**Complexity:** MEDIUM — requires judgment on whether explanations are coherent

### Context
Every signal must be explainable. The SHAP-based explanation must be:
(a) technically correct (the features listed really do drive the score),
(b) coherent with the user's intuition (a Buy signal explained by "RSI oversold" makes
    sense; one explained by "fiscal year month" does not), and
(c) consistent (the same stock in a similar position generates similar explanations
    on different days).

### What to Verify (Human)
1. **Review the top-5 SHAP drivers for 20 random Buy signals.**
   For each, ask: "Would a knowledgeable investor agree this feature justifies a Buy?"
   Target: ≥ 80% rated as "makes sense" by the tester.

2. **Are any explanations driven primarily by calendar features?**
   If "days_to_expiry" or "month_of_year" is a top-3 driver for many signals, the model
   may be overfitting to calendar seasonality. Flag for model investigation.

3. **Do the conformal intervals correctly reflect uncertainty?**
   For a stock with strong trend and confirming fundamentals, the interval should be
   narrower than for a volatile small-cap with no fundamental coverage.
   Tester must review 10 pairs (high-conviction vs low-conviction) and verify intervals differ.

4. **Does "exit type" language match the SHAP drivers?**
   If the exit type is "Thesis Broken" but the SHAP shows technical momentum exhaustion
   as the primary driver (not fundamental deterioration), there is a mismatch.
   Acceptable threshold: < 10% of exits have type/SHAP mismatch.

### Pass Criteria
- ≥ 80% of SHAP explanations rated "makes sense" by knowledgeable reviewer
- Calendar features not dominant drivers (< 5% of signals have calendar in top-3)
- Conformal intervals demonstrably wider for uncertain stocks
- < 10% exit type / SHAP mismatch

---

## HITL-06 · Forensic Score Drift — Early Warning System
**Business value:** CRITICAL — detect deterioration before it becomes catastrophic
**Complexity:** HIGH — requires multi-quarter longitudinal analysis

### Context
Frauds don't happen overnight. A company's forensic score should deteriorate over 4–8
quarters before the fraud is publicly exposed. The tester validates this by looking
retrospectively at companies that failed.

### What to Verify (Human)
1. **Run the forensic model on 8 quarters of historical data for 3 known fraud companies.**
   (Using data that was publicly available at each quarter — PIT-correct.)
   The forensic score should show clear upward trend over the 2 years before failure.

2. **Does the `promoter_pledge_spiral_risk` feature trigger correctly?**
   For IL&FS-style cases where pledge + falling price created a spiral, verify the feature
   captures the dynamic before it becomes publicly known.

3. **Is the "pattern match to known fraud" output useful?**
   When the forensic model flags a company, it should show the most similar historical
   fraud. Tester verifies: "Does Company X's financial profile genuinely resemble
   the stated analogue case?"

4. **What is the false positive rate on the Nifty 50?**
   Nifty 50 companies are large, well-audited firms. The forensic model should flag
   almost none of them as 'red'. Acceptable: ≤ 2/50 Nifty 50 stocks get 'red' flag.
   More than 5 = model is too aggressive.

5. **Does the time-series chart of forensic scores tell a coherent story?**
   The tester should be able to look at a 12-quarter forensic score trend for a
   flagged company and say: "Yes, I can see the deterioration beginning in Q3 2022."

### Pass Criteria
- Forensic score shows upward trend ≥ 4 quarters before confirmed failure (retrospective)
- ≤ 2/50 Nifty 50 stocks flagged 'red' at any point
- Pattern match analogue judged "genuinely similar" in ≥ 70% of cases
- 12-quarter trend charts present and coherent

---

## HITL-07 · Backtest Scrutiny — Is It Too Good to Be True?
**Business value:** CRITICAL — prevents deploying an overfitted model with false confidence
**Complexity:** VERY HIGH — requires deep backtesting expertise

### Context
Any backtest showing Sharpe > 2.0 or CAGR > 35% above benchmark should be treated with
extreme skepticism. The tester must actively try to find the flaw.

### What to Verify (Human)
1. **Review fold-by-fold Sharpe ratios.** Are they consistent?
   - Consistent (e.g., 1.1, 0.9, 1.3, 1.0, 1.2): plausible model
   - Inconsistent (e.g., 3.5, 0.2, 2.8, 0.1, 2.1): overfitted model

2. **Is performance concentrated in one time period?**
   If 80% of the alpha was generated in 2021 bull market, the model is riding beta.

3. **Does performance hold in the COVID crash (Feb–April 2020) and 2022 correction?**
   Any model that works only in bull markets is not a model — it's a beta trade.

4. **Are the signals actionable?**
   A signal on a ₹50L market cap stock with 100 shares/day volume is technically a
   signal but practically worthless. Verify that all backtest trades are on
   stocks with ADTV ≥ ₹50L.

5. **Run the random feature test (SPEC-BT-001) and review result.**
   The model should score ~50% on shuffled features. If it scores 60%+, the model
   has learned noise in the feature structure, not real patterns.

6. **Are there any stocks that appear disproportionately in profitable trades?**
   If 30% of all backtest profits come from 3 stocks, the model is not diversified —
   it found a historical coincidence.

### Pass Criteria
- Fold Sharpe std < 0.5
- Performance present in bear markets (not just 2021 bull)
- All trades on ADTV ≥ ₹50L stocks
- Random feature test score < 55%
- No single stock contributes > 10% of total backtest P&L

---

## HITL Execution Guidelines

### Who should run these tests
- Developer (for HITL-01, 02, 04, 07 — technical scenarios)
- Domain expert with Indian market knowledge (for HITL-03, 05, 06 — requires market context)

### How to record results
```
HITL Test: HITL-01
Date: ___________
Tester: ___________
Stock used: ___________
Pass/Fail per criterion:
  1. Exit type correctly labeled: PASS / FAIL — Notes: ___________
  2. Pledge trend chart present: PASS / FAIL — Notes: ___________
  3. Cascade risk quantified: PASS / FAIL — Notes: ___________
  4. Urgency ≥ 80: PASS / FAIL — Score observed: ___________
  5. No false positive on Bajaj Finance: PASS / FAIL — Notes: ___________
Overall: PASS / FAIL
If FAIL: GitHub issue created: #___________
```

### Re-test cadence
- After every major model retrain: re-run all 7 HITL tests
- After any change to P&D or forensic models: immediately re-run HITL-01, 02, 06
- Quarterly: full 7-test execution with fresh historical examples
