# EMA-RSI Regime Variant Implementation Plan

**Status:** Draft  
**Date:** 2026-08-22  
**Author:** Amit  
**Target Phase:** Phase 3 (Feature Delivery)  
**Branch:** feature/ema-rsi-regime-variant

---

## 1. Overview

Introduce **EMA-RSI regime variant** — a position-sizing overlay that modulates exposure across all strategies (Technical, Momentum) based on Nifty 50's intraday regime state.

### Core Logic

**Nifty 50 Regime Detection** (daily, evaluated at market open):
- **BULL** (100% exposure): Close > EMA(5) AND EMA(5) > EMA(10) AND RSI(14) > 55
- **BULL_WEAK** (75% exposure): Close > EMA(5) AND EMA(5) > EMA(10) AND 50 ≤ RSI(14) ≤ 55
- **CHOPPY** (50% exposure): Close < EMA(5) AND EMA(5) ≥ EMA(10) AND RSI(14) ≥ 45
- **CHOPPY_BEARISH** (25% exposure): Close < EMA(5) AND EMA(5) ≥ EMA(10) AND RSI(14) < 45
- **BEAR** (0% exposure): EMA(5) crosses below EMA(10)
- **UNDEFINED** (50% exposure): Default fallback (missing data, edge cases)

### Scope

- **Applies to:** All strategies (Technical All-Risk, Technical Balanced, Momentum All-Risk, etc.)
- **Mechanism:** Exposure multiplier applied at trade execution (backtest engine)
- **Data source:** Nifty 50 OHLCV (existing ingestion)
- **No new strategy IDs:** Regime is transparent overlay; existing backtests inherit regime filtering automatically
- **Feature flag:** Configurable on/off per backtest run (for comparative analysis)

---

## 2. Architecture & Design

### 2.1 Core Principle: Regime as Reusable Data Artifact

The regime module produces **standalone, strategy-agnostic data** (daily regime state + exposure levels). Strategies **opt-in** via config to apply or ignore it. This allows:

- **Multiple strategies** (Technical, Momentum, future Volatility) to independently use the same regime data
- **Strategy-specific logic** — one strategy may use regime to reduce size, another to drop out entirely, a third to ignore it
- **No hardcoding** — changing regime thresholds or adding new regimes doesn't require code changes to strategies
- **Testing isolation** — regime computation tested separately from strategy execution

### 2.2 Data Flow (Multi-Strategy Application)

```
┌─────────────────────────────────────────────────────────────┐
│ Nifty 50 OHLCV (DuckDB)                                     │
└────────────────────┬────────────────────────────────────────┘
                     ↓
┌─────────────────────────────────────────────────────────────┐
│ features/nifty_regime.py (Regime Detector)                 │
│ • Compute EMA(5), EMA(10), RSI(14)                         │
│ • Apply 6-regime logic (BULL → BEAR)                       │
│ • Output: {date, regime, exposure, ema_5, rsi_14, ...}   │
└────────────────────┬────────────────────────────────────────┘
                     ↓
┌─────────────────────────────────────────────────────────────┐
│ Feature Store (Parquet)                                     │
│ feature_store/hybrid/nifty_regime/YYYY/MM/                 │
│ • Persisted, versioned, reusable                           │
└──┬──────────────┬──────────────┬───────────────────────────┘
   │              │              │
   ↓              ↓              ↓
┌─────────────┐ ┌──────────────┐ ┌──────────────┐
│ Technical   │ │ Momentum     │ │ Future       │
│ Strategies  │ │ Strategies   │ │ Strategies   │
│ (via config)│ │ (via config) │ │ (via config) │
└──────┬──────┘ └──────┬───────┘ └──────┬───────┘
       ↓                ↓                ↓
   [Strategy 1:     [Strategy 2:    [Strategy 3:
    Reduce size     Drop out        Ignore
    in CHOPPY]      in BEAR]        regime]
       ↓                ↓                ↓
└──────────────────────┬──────────────────┘
                       ↓
            backtest/core/engine.py
            • Load regime state (date)
            • Apply strategy's regime rule
            • Execute trade with adjusted params
                       ↓
              Trade Execution Log
              (with regime state for audit)
```

### 2.2 Component Breakdown

#### A. Regime Detector (`features/nifty_regime.py`)
**Responsibility:** Compute Nifty 50 regime state for each trading day.

```python
def compute_nifty_regime(
    nifty_ohlcv: pd.DataFrame,
    ema_short: int = 5,
    ema_long: int = 10,
    rsi_period: int = 14,
) -> pd.DataFrame:
    """
    Returns DataFrame with columns:
      - date
      - close
      - ema_5
      - ema_10
      - rsi_14
      - regime: str (BULL, BULL_WEAK, CHOPPY, CHOPPY_BEARISH, BEAR, UNDEFINED)
      - exposure: float (1.0, 0.75, 0.50, 0.25, 0.0, 0.50)
      - ema_crossover: bool (EMA(5) crossed below EMA(10))
    """
    # Compute EMAs and RSI
    # Apply regime logic (6 branches)
    # Return state dataframe
```

**Outputs:**
- Daily regime state (date, regime, exposure)
- EMA crossover flag (for BEAR detection)
- Metrics for analysis (RSI, EMA spread)

**Storage:**
- Parquet: `feature_store/hybrid/nifty_regime/YYYY/MM/nifty_regime_YYYY-MM-DD.parquet`
- Backfill: 2009-01-01 to present (aligned with backtest data horizon)

#### B. Backtest Engine Integration (`backtest/core/engine.py`)
**Responsibility:** Load regime state and expose it to strategies; strategies decide how to use it.

**New engine behavior:**
1. Load regime data (daily) at backtest start
2. Fetch regime state for each trade date
3. Pass regime state to strategy execution logic
4. Strategy applies its own regime rule (reduce size, drop out, ignore)
5. Log regime state per trade (for audit trail)

**Generic regime loader (engine):**
```python
def load_regime(self, regime_type: str, start_date, end_date):
    """Load regime feature for backtest period."""
    if regime_type == 'ema_rsi':
        path = f'feature_store/hybrid/nifty_regime/{start_date.year}/...'
        return pd.read_parquet(path)  # Returns {date, regime, exposure, ...}
    elif regime_type == 'none':
        return None  # No regime filtering
    else:
        raise ValueError(f"Unknown regime: {regime_type}")

def get_regime_for_date(self, date: datetime) -> dict:
    """Look up regime state for a specific trade date."""
    return self.regime_df[self.regime_df['date'] == date].iloc[0]
```

**Strategy applies its own regime rule:**
```python
# Each strategy config declares HOW to apply regime
strategy_config = {
    'name': 'momentum_all_risk',
    'regime_type': 'ema_rsi',  # Which regime to use (or 'none')
    'regime_application': 'reduce_size',  # How to apply it
    # Options:
    #   - 'reduce_size': position_size *= regime.exposure
    #   - 'drop_out': skip trade entirely if regime.exposure == 0
    #   - 'none': ignore regime (always 1.0 exposure)
    #   - 'custom': use strategy-specific logic via callback
}

# In strategy execution:
def execute_trade(self, signal, position_size):
    regime = self.engine.get_regime_for_date(signal.date)
    
    if self.config['regime_application'] == 'reduce_size':
        position_size *= regime['exposure']
    elif self.config['regime_application'] == 'drop_out':
        if regime['exposure'] == 0:
            return None  # Skip trade entirely
    elif self.config['regime_application'] == 'none':
        pass  # Ignore regime
    
    notional = signal.target_price * position_size
    return self.engine.execute_trade(signal, position_size, regime=regime)
```

**Trade logging with regime state:**
```python
trade_log = {
    'run_id': run_id,
    'date': trade_date,
    'symbol': symbol,
    'entry_price': entry_price,
    'position_size': position_size,
    'regime': regime['regime'],  # NEW: 'BULL', 'CHOPPY', 'BEAR', etc.
    'regime_exposure': regime['exposure'],  # NEW: 1.0, 0.75, 0.5, etc.
    'nifty_rsi': regime['rsi_14'],  # NEW: for audit
    'nifty_ema_5': regime['ema_5'],  # NEW: for audit
    'nifty_ema_10': regime['ema_10'],  # NEW: for audit
    ...
}
```

**Config schema:**
```python
# New in strategy config files (strategies/momentum_identity.py, etc.)
STRATEGY_CONFIG = {
    'variants': {
        'all_risk': {
            'regime_type': 'ema_rsi',  # Use EMA-RSI regime
            'regime_application': 'reduce_size',  # Reduce size in choppy/bear
        },
        'balanced': {
            'regime_type': 'ema_rsi',
            'regime_application': 'drop_out',  # Skip trades in bear
        },
        'risk_managed': {
            'regime_type': 'ema_rsi',
            'regime_application': 'reduce_size',
        },
        'technical_baseline': {
            'regime_type': 'none',  # No regime filtering (baseline)
        },
    }
}
```

#### C. Feature Engineering Registry (`config/regime_features.py`)
**Responsibility:** Make regime features discoverable and reusable by any strategy.

```python
REGIME_REGISTRY = {
    'ema_rsi': {
        'name': 'Nifty 50 EMA-RSI Regime',
        'description': '6-state regime (BULL/BULL_WEAK/CHOPPY/CHOPPY_BEARISH/BEAR/UNDEFINED)',
        'path_pattern': 'feature_store/hybrid/nifty_regime/{year}/{month}/nifty_regime_*.parquet',
        'compute_fn': 'features.nifty_regime.compute_nifty_regime',
        'backfill_start': '2009-01-01',
        'columns': ['date', 'regime', 'exposure', 'rsi_14', 'ema_5', 'ema_10', 'close'],
        'thresholds': {  # Configurable for future tuning
            'rsi_bullish': 55,
            'rsi_weak': 50,
            'rsi_choppy': 45,
        },
    },
    # Future regimes can be added here without changing strategy code
    'vix_based': {
        'name': 'VIX-Based Regime',
        'description': '3-state regime (low/medium/high volatility)',
        'compute_fn': 'features.vix_regime.compute_vix_regime',
        ...
    },
}

def get_regime(regime_type: str, start_date, end_date) -> pd.DataFrame:
    """Load regime feature for any strategy."""
    if regime_type not in REGIME_REGISTRY:
        raise ValueError(f"Unknown regime: {regime_type}")
    
    spec = REGIME_REGISTRY[regime_type]
    # Load from feature store...
    return regime_df
```

### 2.3 Strategy-Specific Regime Application Examples

**Example 1: Momentum All-Risk (Reduce Size)**
```python
# strategies/momentum_identity.py
STRATEGY_CONFIG = {
    'name': 'momentum_all_risk',
    'regime': 'ema_rsi',
    'regime_application': 'reduce_size',
    # Result: Trades in BULL at 100%, CHOPPY at 50%, BEAR at 0% (no trade)
}

# Execution: position_size *= regime.exposure
# Impact: Lower notional in choppy markets, zero trades in bear
```

**Example 2: Technical Balanced (Drop Out)**
```python
# strategies/technical_balanced.py
STRATEGY_CONFIG = {
    'name': 'technical_balanced',
    'regime': 'ema_rsi',
    'regime_application': 'drop_out',
    # Result: Only trade in BULL/BULL_WEAK/CHOPPY; skip BEAR entirely
}

# Execution: if regime.exposure == 0, skip trade
# Impact: Avoid bear regime losses entirely; preserve capital
```

**Example 3: Risk-Managed (Custom Logic)**
```python
# strategies/momentum_risk_managed.py
STRATEGY_CONFIG = {
    'name': 'momentum_risk_managed',
    'regime': 'ema_rsi',
    'regime_application': 'custom',
}

def apply_custom_regime_logic(regime, signal):
    """Risk-managed variant can apply domain-specific logic."""
    if regime['regime'] == 'BEAR':
        # Don't skip, but use tighter stops
        return {
            'position_size_multiplier': 0.25,
            'stop_loss_tighter': True,
        }
    elif regime['regime'] == 'CHOPPY_BEARISH':
        return {
            'position_size_multiplier': 0.5,
            'stop_loss_tighter': False,
        }
    else:
        return {'position_size_multiplier': 1.0}

# Execution: Applies custom rules per regime
```

**Example 4: Baseline (No Regime)**
```python
# strategies/momentum_baseline.py (for A/B testing)
STRATEGY_CONFIG = {
    'name': 'momentum_baseline',
    'regime': 'none',
    'regime_application': 'none',
    # Result: Same behavior as before; used as control for regime impact analysis
}
```

**Key point:** All strategies consume the same regime data (`feature_store/hybrid/nifty_regime/`), but apply it differently. Changing regime computation has **zero impact** on strategies that use `regime: 'none'`.

---

## 3. Implementation Tasks

### Phase 1: Regime Detector (Standalone)
**Deliverable:** Nifty 50 regime feature computed and stored.

**Tasks:**
1. Create `features/nifty_regime.py`
   - [ ] Fetch Nifty 50 OHLCV from DuckDB
   - [ ] Compute EMA(5), EMA(10), RSI(14) using talib or manual
   - [ ] Implement 6-branch regime logic
   - [ ] Return regime state DataFrame
   
2. Create backfill script `scripts/backfill_nifty_regime.py`
   - [ ] Load full Nifty 50 history (2009-01-01 onward)
   - [ ] Compute regime for all dates
   - [ ] Write Parquet partitions (year/month)
   - [ ] Validate: all dates covered, no gaps
   
3. Unit tests `tests/unit/test_nifty_regime.py`
   - [ ] Test each of 6 regimes (boundary conditions)
   - [ ] Test EMA(5) < EMA(10) crossover detection
   - [ ] Test RSI thresholds (50, 55, 45)
   - [ ] Test fallback (missing data → UNDEFINED)

**Acceptance:**
- Regime feature exists in `feature_store/hybrid/nifty_regime/` for 2009-2026
- Unit tests pass; edge cases (data gaps, crossovers) validated
- Regime state matches manual spot-checks (e.g., recent market regimes)

---

### Phase 2: Backtest Engine Integration (Generic Regime Support)
**Deliverable:** Engine loads + exposes regime state; strategies apply independently via config.

**Core principle:** Engine is **strategy-agnostic**; each strategy decides how to use regime via config.

**Tasks:**
1. Create `config/regime_features.py` (Registry)
   - [ ] Define `REGIME_REGISTRY` (ema_rsi, future vix_based, etc.)
   - [ ] Each registry entry: path, loader, thresholds, columns
   - [ ] Make thresholds configurable (RSI bounds, EMA periods)
   - [ ] Support multiple regime types (not hardcoded to ema_rsi)

2. Modify `backtest/core/engine.py` (Regime Loader)
   - [ ] Add `load_regime(regime_type, start_date, end_date)` method
   - [ ] Implement `get_regime_for_date(date)` lookup
   - [ ] Load regime data on backtest init (full history into memory)
   - [ ] Handle missing regime data gracefully (log warning, return default)
   - [ ] Do NOT apply regime logic here (strategies do that)

3. Modify strategy base class / execution logic
   - [ ] Add `regime_type` and `regime_application` to strategy config
   - [ ] Read config in strategy's `execute_trade()` method
   - [ ] Fetch regime state from engine: `regime = engine.get_regime_for_date(date)`
   - [ ] Apply strategy-specific regime rule:
     - `'reduce_size'` → `position_size *= regime['exposure']`
     - `'drop_out'` → skip trade if `regime['exposure'] == 0`
     - `'custom'` → call strategy's custom logic
     - `'none'` → ignore regime (control group)
   - [ ] Log regime state with each trade

4. Update trade logging schema `backtest/core/metrics.py`
   - [ ] Add columns: `regime`, `regime_exposure`, `nifty_rsi_14`, `nifty_ema_5`, `nifty_ema_10`
   - [ ] Log regime state for all trades (even if not used)
   - [ ] Compute separate metrics by regime (for post-hoc analysis)

5. Modify strategy runners `backtest/run_orchestrator_backtest.py`
   - [ ] Validate `regime_type` specified in strategy config is in registry
   - [ ] Validate regime data exists before backtest start
   - [ ] Pass strategy config (including regime settings) to engine
   - [ ] No changes to backtest orchestration logic (engine handles it)

6. Unit tests `tests/unit/test_engine_regime.py`
   - [ ] Test `load_regime()` for each regime type (ema_rsi, none)
   - [ ] Test `get_regime_for_date()` lookup (date → regime dict)
   - [ ] Test missing regime data (fallback behavior)
   - [ ] Test strategy-specific application:
     - `reduce_size`: verify position_size reduced correctly
     - `drop_out`: verify trades skipped in BEAR regime
     - `custom`: verify custom logic applied
     - `none`: verify regime ignored
   - [ ] Test trade logging (regime columns populated)

**Acceptance:**
- Backtest runs with regime filter enabled
- Trades logged with regime state
- Position sizes reduced in CHOPPY/BEAR regimes (0 trades in BEAR if exposure=0)
- Metrics report separately (total return, Sharpe, etc.)

---

### Phase 3: Comparative Analysis & Validation
**Deliverable:** Backtest results showing regime impact; decision on rollout.

**Tasks:**
1. Run parallel backtests
   - [ ] Technical All-Risk (baseline, no regime)
   - [ ] Technical All-Risk (regime enabled)
   - [ ] Momentum All-Risk (baseline)
   - [ ] Momentum All-Risk (regime enabled)
   - Date range: 2019-01-01 to 2026-08-20 (consistent horizon)

2. Analysis
   - [ ] Compare Sharpe, Calmar, max drawdown (with vs. without regime)
   - [ ] Regime frequency distribution (% BULL/CHOPPY/BEAR over period)
   - [ ] Trade count reduction (expected if BEAR exposure=0)
   - [ ] Risk-adjusted return lift/drag

3. Documentation `scripts/regime_analysis.py`
   - [ ] Backtest comparison script (reproducible)
   - [ ] Visualization: regime timeline + equity curve
   - [ ] Summary table (metrics side-by-side)

4. Decision gate
   - [ ] Is regime overlay beneficial (Sharpe lift, drawdown reduction)?
   - [ ] Any pathological cases (e.g., regime flips too frequently)?
   - [ ] Rollout decision: enable by default, opt-in, or deprecated

**Acceptance:**
- Backtests complete without errors
- Regime impact quantified (table: baseline vs. regime for 4 strategy configs)
- Analysis document with recommendations

---

### Phase 4: Configuration & Extensibility
**Deliverable:** Regime system fully parameterized; strategies decouple from regime logic.

**Tasks:**
1. Strategy config updates (strategies/*.py)
   - [ ] Add `regime_type` field to all strategy configs (momentum_identity.py, technical_*.py)
   - [ ] Add `regime_application` field (reduce_size, drop_out, custom, none)
   - [ ] Document examples: Technical Balanced uses drop_out, Momentum All-Risk uses reduce_size
   - [ ] Ensure baseline variants can disable regime (regime_type: 'none')

2. Documentation
   - [ ] Update CLAUDE.md with regime variant explanation (how it works, config options)
   - [ ] Add regime config examples to FeatureBacklog.md (completed item)
   - [ ] Document `config/regime_features.py` (how to add new regimes)
   - [ ] Backtest report UI: show regime state in trade list (if applicable)
   - [ ] Create `docs/regime_variants.md` with examples (reduce_size vs. drop_out vs. custom)

3. Future extensibility path
   - [ ] Document how to add new regime types (VIX-based, sector-based)
   - [ ] Minimal change needed: new entry in `REGIME_REGISTRY`; no engine rework
   - [ ] Strategies can opt-in to new regimes in config without code changes

4. Rollout & A/B testing
   - [ ] If regime variant outperforms baseline, identify best `regime_application` per strategy
   - [ ] Plan gradual adoption: opt-in → default (if results justify)
   - [ ] Maintain baseline (regime_type: 'none') for perpetual comparison

**Acceptance:**
- Regime logic is fully parameterized via config (not hardcoded)
- Strategies decouple from regime implementation (they just read config)
- New regime types can be added without modifying strategy code
- Easy A/B testing: same strategy with/without regime (just toggle config)
- Trade audit trail includes regime state (for post-hoc analysis)

---

## 4. Integration Points & Dependencies

### Data Dependencies
- **Nifty 50 OHLCV:** Must be present in DuckDB (existing ingestion via Fyers)
- **Trading calendar:** Use NSE calendar for regime date alignment
- **Feature store:** Write-lock contention (use snapshot if running concurrent backtests)

### Code Dependencies
- **backtest/core/engine.py** — Modify execute_trade() and trade logging
- **features/** — New regime detector module
- **backtest/run_*.py** — Pass regime config to engine
- **tests/unit/** — New test suite for regime logic

### No Breaking Changes
- Existing backtests default to `apply_regime_filter=False` (opt-in)
- Regime feature computed independently; no impact on other features
- Trade schema backward-compatible (new columns appended)

---

## 5. Testing Strategy

### Unit Tests (Phase 1)
- Regime logic: 6 branches, boundary conditions, crossover detection
- Regime lookup: date → exposure (missing dates → fallback)
- Position sizing: size × multiplier calculation

### Integration Tests (Phase 2)
- End-to-end backtest with regime filter enabled
- Trade log validation (regime column populated for all trades)
- Comparative: baseline vs. regime backtests (same symbol, different filters)

### Validation Tests (Phase 3)
- Run 4 major backtests (Technical/Momentum × with/without regime)
- Spot-check regime state vs. manual Nifty 50 chart (recent dates)
- Metric consistency (no NaN, negative returns make sense)

### Edge Cases
- Missing regime data (early dates if backfill incomplete)
- EMA crossover on low-volatility days (should not flip regime randomly)
- RSI at exact threshold (50, 55, 45) — round-off errors
- BEAR regime: trades should be skipped (exposure=0)

---

## 6. File Changes Summary

### New Files
```
features/nifty_regime.py                     # EMA(5/10), RSI(14) computation
scripts/backfill_nifty_regime.py             # Backfill 2009-2026
scripts/regime_analysis.py                   # Comparative analysis (baseline vs. regime)
tests/unit/test_nifty_regime.py              # Regime logic (6 branches, thresholds)
tests/unit/test_engine_regime.py             # Engine + strategy integration
config/regime_features.py                    # Regime registry (ema_rsi, vix_based, ...)
docs/regime_variants.md                      # Examples: reduce_size vs. drop_out
EMA_RSI_REGIME_PLAN.md                       # This file (updated)
EMA_RSI_REGIME_RESULTS.md                    # Results post-backtest (decision)
```

### Modified Files
```
backtest/core/engine.py
  • Add load_regime(regime_type, start_date, end_date) method
  • Add get_regime_for_date(date) lookup
  • Pass regime state to strategy execution (do NOT apply logic)

backtest/core/metrics.py
  • Add columns: regime, regime_exposure, nifty_rsi_14, nifty_ema_5, nifty_ema_10

backtest/run_orchestrator_backtest.py
  • Validate regime_type in registry before backtest start
  • Verify regime data exists

strategies/momentum_identity.py
  • Add 'regime_type' and 'regime_application' to STRATEGY_CONFIG (per variant)

strategies/technical_*.py
  • Add 'regime_type' and 'regime_application' to STRATEGY_CONFIG

CLAUDE.md
  • Document regime variant overview (what it is, how to configure)
  • Link to docs/regime_variants.md

FeatureBacklog.md
  • Mark EMA-RSI regime as completed (Phase 3 item)
```

### No Changes (Regime-Agnostic)
```
features/momentum_universe.py              # Doesn't reference regime
features/momentum_strategy.py              # Reads regime from config only
datastore/schema/                          # No schema changes
backtest/core/integrity_checker.py         # Regime state is just another column
```

### Key Architectural Principle
- **Single source of regime data:** `feature_store/hybrid/nifty_regime/`
- **Single registry:** `config/regime_features.py` (list of all regime types)
- **Strategies decouple:** Each reads config; engine loads regime; strategy applies rule
- **Zero coupling:** Changing regime thresholds doesn't require strategy code changes

---

## 7. Success Criteria

| Criterion | Metric | Target |
|-----------|--------|--------|
| **Feature Completeness** | All 6 regimes implemented + tested | ✓ All pass |
| **Regime Coverage** | Backfill 2009-2026 with zero gaps | ✓ 100% dates |
| **Integration** | Backtest runs with regime enabled (4 configs) | ✓ All complete |
| **Regime Impact** | Quantify Sharpe/drawdown lift/drag | ✓ Decision documented |
| **No Regressions** | Baseline backtest metrics unchanged (regime disabled) | ✓ Bit-identical |
| **Documentation** | Plan + results + config documented | ✓ All in place |

---

## 8. Known Constraints & Risks

### Constraints
- **DuckDB write-lock:** Regime computation and backfill must not run during live ingestion or pytest
- **Nifty 50 data quality:** Regime reliability tied to Nifty OHLCV accuracy (no synthetic fills for gaps)
- **Feature store partition scheme:** Regime stored as daily Parquets; backtest must load efficient date range

### Risks
- **Over-filtering:** If BEAR regime is too aggressive (exposure=0), may miss reversals
- **Lag in regime detection:** RSI/EMA computed on daily close; intraday regime flips not captured
- **Regime churn:** If RSI oscillates near 50/55 threshold, regime can flip frequently
- **Benchmark skew:** Nifty-focused regime may not fit all universe strategies (micro-cap, sector)

### Mitigations
- Spot-check regime state on historical dates (manual validation)
- Run comparative backtests (quantify impact before rollout)
- Consider regime smoothing (e.g., 3-day moving consensus) if churn is high
- Document assumptions in CLAUDE.md

---

## 9. Timeline & Phases

| Phase | Task | Est. Duration | Owner |
|-------|------|---|-------|
| **Phase 1** | Regime detector + backfill + tests | 3-4 hours | Claude |
| **Phase 2** | Engine integration + logging | 2-3 hours | Claude |
| **Phase 3** | Comparative backtests + analysis | 4-6 hours (backtest runtime) | Claude + Manual review |
| **Phase 4** | Config + docs + decision | 1-2 hours | Claude |
| **Total** | | **10-15 hours (+ backtest wait time)** | — |

---

## 10. Future Extensibility (Zero Coupling)

This design enables new regimes with **zero changes to existing strategies or engine logic**.

### Adding a New Regime Type (Example: VIX-Based)

**Step 1:** Create new regime detector
```python
# features/vix_regime.py
def compute_vix_regime(vix_series: pd.Series) -> pd.DataFrame:
    """3-state regime: LOW (100%), MEDIUM (50%), HIGH (0%)."""
    return pd.DataFrame({
        'date': vix_series.index,
        'regime': vix_series.apply(lambda v: 'LOW' if v < 15 else 'MEDIUM' if v < 20 else 'HIGH'),
        'exposure': vix_series.apply(lambda v: 1.0 if v < 15 else 0.5 if v < 20 else 0.0),
        'vix_value': vix_series.values,
    })
```

**Step 2:** Register in registry (1 line)
```python
# config/regime_features.py
REGIME_REGISTRY = {
    'ema_rsi': {...},  # Existing
    'vix_based': {     # NEW — no code changes needed elsewhere
        'compute_fn': 'features.vix_regime.compute_vix_regime',
        'path_pattern': 'feature_store/hybrid/vix_regime/...',
        ...
    },
}
```

**Step 3:** Strategies opt-in via config (no code changes)
```python
# strategies/technical_vix_sensitive.py
STRATEGY_CONFIG = {
    'name': 'technical_vix_sensitive',
    'variants': {
        'all_risk': {
            'regime': 'vix_based',  # Just change this; no strategy logic changes
            'regime_application': 'reduce_size',
        }
    }
}
```

**Zero changes to:**
- `backtest/core/engine.py` (already generic)
- `backtest/core/metrics.py` (already logs all regime states)
- `features/momentum_universe.py` (doesn't reference regime)
- `backtest/run_orchestrator_backtest.py` (registry lookup is automatic)

### Extensibility Summary

| Future Regime | Steps Required | Code Changes |
|---|---|---|
| VIX-based | 1. Compute function, 2. Register, 3. Update strategy config | 0 files modified (add only) |
| Sector-based | 1. Compute function, 2. Register, 3. Update strategy config | 0 files modified (add only) |
| Custom thresholds | Update `REGIME_REGISTRY['ema_rsi']['thresholds']` | 1 line change |
| Regime smoothing | Add `smoothing_window` to registry; recompute | 1 registry entry change |

**Design principle:** Strategies are **configuration-driven**, not code-driven. New regimes are added to the registry, and existing strategies automatically inherit them via config.

---

## 11. Questions for User

- [ ] Should regime data backfill start from 2009-01-01, or later (e.g., 2015+)?
- [ ] Is BEAR (exposure=0) skipping trades, or reducing size to 1 share (position-taking cost)?
- [ ] Should regime state be logged to backtest output (for audit trail)?
- [ ] Any constraints on backfill timing (when can we run compute-heavy regime backfill)?

---

## Approval Gate

**Status:** Ready for implementation upon user sign-off.

**Next step:** User review and approval → Proceed to Phase 1 (Regime Detector).
