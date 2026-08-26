# Volatility Scaling Modes — Usage Guide

Four individual, testable functions for Moreira-Muir volatility scaling framework.

## Quick Start

```python
from features.volatility_scaling import (
    baseline,
    inverse_volatility,
    inverse_variance,
    target_volatility,
    downside_volatility,
)

# All functions take the same inputs:
multipliers = inverse_volatility(
    equity_curve=portfolio_values,  # pd.Series of portfolio values
    lookback_days=126,              # ~6 months (default)
    leverage_cap=2.0,               # Optional cap (None = uncapped)
)

# Output: pd.Series(index=dates, values=exposure_multipliers)
# Apply to position sizing: position_size = base_size * multiplier
```

---

## The Five Modes

### 0. baseline — Neutral Control (No Scaling)

```python
from features.volatility_scaling import baseline

multipliers = baseline(
    equity_curve,
    lookback_days=126,  # Ignored (included for API consistency)
    leverage_cap=None,  # Ignored (included for API consistency)
)
```

**Formula:** `multiplier = 1.0` (constant)

**Characteristics:**
- 🎯 No volatility-based adjustment (pure momentum signal)
- 🔬 Control/reference mode for measuring vol-scaling value-add
- ✅ Baseline for comparing improvement across other modes
- 📊 Always returns same position size (no dynamic scaling)

**Use Case:**
Compare how much value volatility scaling adds:
```python
baseline_returns = backtest_strategy(
    equity_curve, 
    sizing_mode=baseline  # No vol scaling
)
scaled_returns = backtest_strategy(
    equity_curve, 
    sizing_mode=inverse_volatility  # With vol scaling
)
# value_added = scaled_returns - baseline_returns
```

---

### 1. inverse_volatility

### 1. inverse_volatility — Aggressive in Low-Vol Regimes

```python
from features.volatility_scaling import inverse_volatility

multipliers = inverse_volatility(
    equity_curve,
    lookback_days=126,
    leverage_cap=None,  # Moreira-Muir default (uncapped)
)
```

**Formula:** `multiplier = 1 / σ`

**Characteristics:**
- 📈 High multipliers in low-vol regimes (aggressive sizing)
- 📉 Low multipliers in high-vol regimes (conservative sizing)
- ✅ Optimal for: Low baseline volatility + strong momentum signal (e.g., M7 band)
- ❌ Fails in: High baseline volatility (collapses positioning; loses 33%+ CAGR in M9)

**Band Performance:**
| Band | Volatility | Momentum | Result |
|------|-----------|----------|--------|
| M7   | Low       | High     | ✅ 38.95% CAGR (WINS) |
| M8   | Medium    | Very High| ⚠️ 43.98% CAGR (marginal, 0.41% below inverse_variance) |
| M9   | High      | Extreme  | ❌ 10.56% CAGR (CRASHES; loses 33.83% to downside_vol) |

**Example:**
```python
equity_curve = pd.Series([1_000_000, 1_001_000, 1_002_500, ...])
multipliers = inverse_volatility(equity_curve, lookback_days=126)
# If vol=15%, multiplier ≈ 1/0.15 = 6.67 (aggressive)
# If vol=45%, multiplier ≈ 1/0.45 = 2.22 (conservative)
```

---

### 2. inverse_variance — Most Stable Across Regimes

```python
from features.volatility_scaling import inverse_variance

multipliers = inverse_variance(
    equity_curve,
    lookback_days=126,
    leverage_cap=None,  # Moreira-Muir default (uncapped)
)
```

**Formula:** `multiplier = 1 / σ²`

**Characteristics:**
- 🎯 Quadratic penalty on volatility (smoother than 1/σ)
- ✅ No catastrophic failures across any band (most robust)
- 📊 Balanced aggression in low-vol, conservatism in high-vol
- Optimal for: Medium baseline volatility + high momentum (e.g., M8 band)

**Band Performance:**
| Band | Volatility | Momentum | Result |
|------|-----------|----------|--------|
| M7   | Low       | High     | ✅ 38.20% CAGR (strong; 0.75% below inverse_volatility) |
| M8   | Medium    | Very High| ✅ 44.39% CAGR (PEAK; marginal 0.41% above inverse_volatility) |
| M9   | High      | Extreme  | ⚠️ Missing data (likely underperforms downside_vol) |
| M4-M6| Mid-range | Varying  | ✅ Consistently wins in transition bands |

**Why It's Stable:**
- Squaring σ creates smooth penalty curve (avoids sharp cliffs)
- Less extreme swings between low/high vol regimes
- Only downside: slightly suboptimal peaks (vs specialized modes)

**Example:**
```python
# Same scenario as inverse_volatility, but quadratic penalty
# If vol=15%, multiplier ≈ 1/0.15² = 44.4 (very aggressive)
# If vol=45%, multiplier ≈ 1/0.45² = 4.9 (still meaningful)
```

---

### 3. target_volatility — Conservative Risk Management

```python
from features.volatility_scaling import target_volatility

multipliers = target_volatility(
    equity_curve,
    target_vol=0.15,        # Target 15% annualized vol
    lookback_days=126,
    leverage_cap=1.0,       # Default: never above 1.0 (neutral)
)
```

**Formula:** `multiplier = target_σ / realized_σ`

**Characteristics:**
- 🛡️ De-leverages when realized vol > target (risk control)
- 📉 Re-leverages when realized vol < target (opportunity)
- Conservative by design (cap=1.0; never over-leverage)
- Stabilizes portfolio volatility around target level
- ⚠️ Not for return maximization; use for risk management

**Band Performance:**
| Band | Use Case |
|------|----------|
| M3   | 14.71% CAGR (wins; rare non-inverse case) |
| M12  | 12.34% CAGR (competitive in micro-cap) |
| All  | Suboptimal vs other modes for alpha capture |

**Example:**
```python
equity_curve = pd.Series([...])  # Portfolio value over time
target_vol = 0.15  # Aim for 15% annual vol

multipliers = target_volatility(equity_curve, target_vol=0.15)

# If realized vol = 20% (> target):
#   multiplier = 0.15 / 0.20 = 0.75 (de-leverage)
# If realized vol = 10% (< target):
#   multiplier = 0.15 / 0.10 = 1.50 (but capped at 1.0 by default)
#   → actually 1.0 (neutral)
```

---

### 4. downside_volatility — Asymmetric (Upside Drift Sensitive)

```python
from features.volatility_scaling import downside_volatility

multipliers = downside_volatility(
    equity_curve,
    lookback_days=126,
    leverage_cap=None,  # Moreira-Muir default (uncapped)
)
```

**Formula:** `multiplier = 1 / downside_σ` where downside_σ = std(negative_returns_only)

**Characteristics:**
- 🔄 Only penalizes downside volatility (negative returns)
- ⬆️ Upside swings don't reduce position sizing
- ✅ Optimal for: High baseline vol with upside drift (strong momentum) (e.g., M9)
- ❌ Fails in: Low-vol with symmetric vol (over-leverages; loses 27%+ CAGR in M7)
- **Fragile:** Produces 33%+ performance swaps across bands

**Band Performance:**
| Band | Volatility Profile | Result |
|------|-------------------|--------|
| M7   | Low + Symmetric   | ❌ 11.23% CAGR (CRASHES; loses 27.72% to inverse_volatility) |
| M8   | Med + Upside Drift| ⚠️ 38.54% CAGR (underperforms inverse_variance by 5.85%) |
| M9   | High + Strong UO  | ✅ 44.39% CAGR (PEAK; 33.83% above inverse_volatility!) |

**Why It Works in M9, Fails in M7:**
```
M9 (High-Vol, Strong Momentum):
  • Realized vol = 45% (high)
  • Downside vol = 30% (asymmetric; upside drift from momentum)
  • downside_vol multiplier: 1/0.30 = 3.33 (aggressive)
  • inverse_vol multiplier: 1/0.45 = 2.22 (over-conservative)
  → downside_vol captures alpha downside_vol ignores upside ↑

M7 (Low-Vol, Symmetric):
  • Realized vol = 28% (medium-low)
  • Downside vol = 26% (symmetric; no upside drift)
  • downside_vol multiplier: 1/0.26 = 3.85 (over-aggressive!)
  • inverse_vol multiplier: 1/0.28 = 3.57 (balanced)
  → downside_vol over-leverages on mean-reverting position ✗
```

**Example:**
```python
# Trending market with upside drift
equity_curve = pd.Series([...])  # Strong uptrend

multipliers = downside_volatility(equity_curve, lookback_days=126)
# Ignores upside swings → higher multipliers
# Penalizes downside only → protective of crashes
```

---

## Decision Framework: Which Mode to Use?

### Choose By Deployment Strategy

| Strategy | Mode | CAGR | Sharpe | Drawdown | Notes |
|----------|------|------|--------|----------|-------|
| **Control/Baseline** | baseline | 30.50% | 1.05 | varies | 🔬 No vol scaling (reference) |
| **M7 Single Band** | inverse_volatility | 38.95% | 1.13 | -35.65% | ✅ Safe, stable |
| **M8 Single Band** | inverse_variance | 44.39% | 1.23 | -45.30% | ✅ Peak, stable |
| **M9 Single Band** | downside_volatility | 44.39% | 1.23 | -45.30% | ✅ Peak, but fragile |
| **Multi-Band (M7+M8+M9)** | Mixed (per band) | ~42.6% | ~1.20 | ~-42% | ✅ Optimal composite |
| **Risk Management** | target_volatility | 15-18% | 0.5-0.7 | -30% | 🛡️ Conservative |
| **Single Mode (All Bands)** | inverse_volatility | 15.31% | 0.59 | varies | 🎯 Most robust |

### Choose By Risk Profile

**Conservative (Lower Drawdown):**
→ `inverse_volatility` applied to M7 only
- 38.95% CAGR, -35.65% MaxDD
- Safest, most consistent, lowest leverage

**Aggressive (Maximum Returns):**
→ `inverse_variance` applied to M8
- 44.39% CAGR, -45.30% MaxDD
- Peak performance, single mode, stable across bands

**Extreme (Maximum Alpha):**
→ `downside_volatility` applied to M9
- 44.39% CAGR, -45.30% MaxDD
- Highest alpha capture, but band-specific (fails elsewhere)

**Balanced (Diversified):**
→ All three modes, each on optimal band (M7+M8+M9)
- ~42.6% CAGR, ~-42% MaxDD (estimated)
- Regime diversification, operational complexity

**Risk-Averse:**
→ `target_volatility` across all bands
- 11-18% CAGR, -30% to -40% MaxDD
- Most stable, lowest risk, de-leverages on volatility spikes

---

## Band Definitions (M1-M12)

Bands are defined by **rank percentile** (market-cap weighted momentum ranking):

```python
# From features/momentum_universe.py

RANK_BANDS = [
    (1, 1, 50),         # M1:  Largest cap, lowest momentum
    (2, 1, 75),         # M2:  Top 75 by cap (overlapping)
    (3, 51, 100),       # M3:  Rank 51-100
    (4, 76, 160),       # M4:  Rank 76-160 (overlapping)
    (5, 101, 150),      # M5:  Rank 101-150
    (6, 151, 200),      # M6:  Rank 151-200
    (7, 161, 275),      # M7:  Rank 161-275 (mid-cap sweet spot)
    (8, 201, 300),      # M8:  Rank 201-300 (peak returns)
    (9, 276, 550),      # M9:  Rank 276-550 (extreme momentum)
    (10, 301, 500),     # M10: Rank 301-500 (overlapping)
    (11, 501, 800),     # M11: Rank 501-800 (small-cap)
    (12, 551, 800),     # M12: Rank 551-800 (micro-cap, overlapping)
]
```

**Key:** Numbers are 1-indexed, inclusive on both ends. Rank-1 = highest market cap (largest). Rank-800 = smallest cap (micro).

---

## Testing Individual Modes

```python
import pandas as pd
from features.volatility_scaling import inverse_volatility, inverse_variance, \
    target_volatility, downside_volatility

# Load your equity curve
equity_curve = load_backtest_results()  # pd.Series(index=dates, values=portfolio_value)

# Test all four modes
inv_vol = inverse_volatility(equity_curve)
inv_var = inverse_variance(equity_curve)
tgt_vol = target_volatility(equity_curve, target_vol=0.15)
dwn_vol = downside_volatility(equity_curve)

# Compare
import matplotlib.pyplot as plt
fig, axes = plt.subplots(2, 2, figsize=(12, 8))

axes[0, 0].plot(inv_vol.iloc[-252:])
axes[0, 0].set_title('inverse_volatility')

axes[0, 1].plot(inv_var.iloc[-252:])
axes[0, 1].set_title('inverse_variance')

axes[1, 0].plot(tgt_vol.iloc[-252:])
axes[1, 0].set_title('target_volatility')

axes[1, 1].plot(dwn_vol.iloc[-252:])
axes[1, 1].set_title('downside_volatility')

plt.tight_layout()
plt.show()
```

---

## Run Tests

```bash
# Test individual modes
pytest tests/unit/test_volatility_scaling_modes.py -v

# Test specific mode
pytest tests/unit/test_volatility_scaling_modes.py::TestInverseVolatility -v

# Run all volatility tests (including legacy)
pytest tests/unit/ -k volatility -v
```

---

## Integration with Backtest Engine

To use individual modes in backtests:

```python
# Old way (monolithic function):
from features.momentum_signal import volatility_scaling_multiplier
multipliers = volatility_scaling_multiplier(
    equity_curve,
    scaling_mode="inverse_variance",
    lookback_days=126,
)

# New way (individual functions):
from features.volatility_scaling import inverse_variance
multipliers = inverse_variance(equity_curve, lookback_days=126)

# Both produce identical results; new way is more testable/composable
```

---

## Fragility Summary

**inverse_volatility:**
- 🎯 Wins: M7 (38.95%)
- ❌ Fails: M9 (10.56%, loses 33.83%)
- ✅ Robust: M1-M7 (no major crashes, but weak in M1-M6)

**inverse_variance:**
- 🎯 Wins: M8 (44.39%)
- ⚠️ Close: M7 (38.20%, loses 0.75%), M9 (missing data)
- ✅ Most stable: No catastrophic failures in any band

**downside_volatility:**
- 🎯 Wins: M9 (44.39%)
- ❌ Fails: M7 (11.23%, loses 27.72%)
- ⚠️ Weak: M2-M6 (underperforms other modes)

**target_volatility:**
- 🎯 Wins: None (suboptimal across all bands for alpha)
- ✅ Best for: Risk management (de-leverages on vol spikes)
- ⚠️ Trade-off: Lower returns for volatility control

---

## References

- **Moreira & Muir (2017):** "Volatility-Managed Portfolios" (the R9 framework)
- **Barroso & Santa-Clara (2015):** "Momentum has its Moments" (R8 target-vol logic)
- See `backtest/reports/R9_EXECUTIVE_SUMMARY.txt` for detailed band analysis
- See `backtest/reports/r9_fragility_analysis.md` for regime breakdown
