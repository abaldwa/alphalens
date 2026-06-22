# AlphaLens — Truthful Expectations
## What ML Can and Cannot Do for Indian Equity Investing

This document exists to prevent the most common failure mode in quant projects:
building something technically impressive that doesn't make money because expectations
were unrealistic. Read this before interpreting any backtest result.

---

## What the Evidence Actually Says

### The successes are real but narrow
Renaissance Technologies' Medallion Fund has compounded at ~66% annually before fees
using quantitative methods. Two Sigma, WorldQuant, and AQR have sustained edge for years.
These are real. But they employ hundreds of PhD-level quants, have proprietary data
costing tens of millions annually, and operate on margins that disappear when too much
capital chases the same signals.

### Most attempts fail
The base rate from QuantConnect and practitioner forums: the majority of backtests
that look promising do not hold up in live trading. The most common reasons are lookahead
bias, survivorship bias, and overfitting to the training period — exactly the issues
this system is designed to prevent.

### Academic anomalies decay
A 2020 study tested 97 published market anomalies. After publication, roughly half
decayed significantly or disappeared — arbitraged away or statistical artifacts.

---

## Components with Best Chance of Real Edge

### Highest probability:
- **P&D detection** — prevents catastrophic losses, not alpha generation
- **Forensic scoring** — catching a Yes Bank/Vakrangee BEFORE collapse has asymmetric value
- **Regime detection (HMM)** — scaling position sizes by regime is evidence-backed
- **Exit signal model** — combats behavioral bias (holding losers too long)

### Moderate probability:
- **12-month momentum (skip 1m)** — most robustly documented factor globally; works but volatile
- **Quality + momentum combination** — genuine edge of ~2–4% annually above benchmark
- **Governance features (India-specific)** — promoter pledge, rising institutional ownership
  have genuinely worked in India because retail systematically underweights governance risk

### Lower probability than documents imply:
- **Multibagger model** — concept sound but training data too thin, problem too complex
- **Most technical indicator combinations** — extensively studied, heavily arbitraged
- **RL meta-agent** — modest-edge inputs do not produce large-edge outputs

---

## Realistic Return Expectations

A well-implemented system that works as intended (not overfitted):
- **5–12% annualized alpha above Nifty 500** in good periods
- Substantial drawdowns and **1–2 year underperformance periods**
- Best Indian retail quants: **15–25% CAGR** vs Nifty's ~13–14%
- Transaction costs consume **30–50% of gross alpha**
- **NOT** 30–50% annual compounding as theoretical backtests may suggest

---

## Use This System As

✅ A stock research assistant that improves decision quality
✅ A systematic discipline enforcer (prevents panic selling)
✅ A forensic and P&D protection layer
✅ A screener that surfaces interesting candidates for human analysis
✅ A portfolio risk monitor

## Do NOT Use This System As

❌ An autonomous trading system
❌ A replacement for fundamental due diligence
❌ A guaranteed wealth generator
❌ A system that beats professional quant desks on their own public data

---

## The Deeper Question

If ML reliably identified 3x–10x multibaggers from public data, why would that
information remain undiscovered? The Indian market has ~1,500 institutional participants
including well-funded quant desks. Indian small/mid caps DO have meaningful informational
inefficiencies around governance and earnings quality signals — but assuming a 330-feature
LightGBM on public data finds consistent large alpha that professional desks have not
already found is a strong assumption to hold lightly.

Build it. Use it. Be rigorous. But calibrate expectations to **research assistant that
earns modest persistent alpha** rather than **money printing machine**.
