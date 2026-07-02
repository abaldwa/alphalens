# Phase 3 HITL Test Results

**Session:** P3.3 — Stacking Ensemble + Feature Selection  
**Date:** 2026-06-24  
**Tester:** Amit  
**Status:** PENDING EXECUTION — requires trained deep models and real market data

---

## HITL-04 · Market Regime Transition — Attention Map Verification

**Spec:** `alphalens_docs/tests/10_hitl_tests.md#HITL-04`  
**Phase 3 extension:** Verify that TFT attention maps change correctly during bear→bull transitions.

### Test Setup (when to run)
1. Train TFT overnight (M-11, `schedule_overnight_training()`) on ≥ 2 years of real data.
2. Identify a historical bear→bull transition in the dataset:
   - Candidate A: September–November 2022 (Nifty -10% then recovery)
   - Candidate B: March–June 2020 (COVID crash and V-shaped recovery)
3. Run `TFTSignalModel.get_attention_weights(X)` on:
   - 5 stocks with strong trend reversal (e.g., RELIANCE, HDFCBANK, TITAN, INFY, ICICIBANK)
   - Two windows: (a) 63-day window ending at the bottom, (b) 63-day window 1 month into recovery.

### What to Verify (Human)
1. **Do attention weights shift toward recent timesteps during recovery?**  
   In a trend reversal, the model should weight recent days (days 55–63) more heavily than
   the crash period (days 1–20). Verify `attn[0, -5:, :].mean() > attn[0, :20, :].mean()`
   for at least 3 of 5 stocks during recovery.

2. **Do attention maps differ materially between bear and bull windows?**  
   Cosine similarity between bear-window and bull-window attention maps should be < 0.90
   for at least 4 of 5 stocks. Maps that do not change = model not regime-aware.

3. **Does Buy probability increase post-recovery vs. during crash?**  
   `predict_proba(X_recovery)[:, 2]` (P(Buy)) should exceed
   `predict_proba(X_crash)[:, 2]` by at least 0.10 for ≥ 3 of 5 stocks.

4. **Do variable selection weights shift to momentum features during recovery?**  
   `get_shap_values()` (VSN weights) should show `momentum_3m`, `momentum_6m`,
   `close_sma50_ratio` increasing in importance during recovery vs. crash.

### Pass Criteria
- [ ] Attention weights shift toward recent timesteps during recovery (3/5 stocks)
- [ ] Bear vs. bull attention cosine similarity < 0.90 (4/5 stocks)
- [ ] P(Buy) increases ≥ 0.10 in recovery window (3/5 stocks)
- [ ] Momentum features gain weight post-recovery (qualitative, tester judgment)

### Execution Notes
```python
from systems.ml_signal_engine.models.deep.tft_model import TFTSignalModel
from datastore.client import DataStoreClient

client = DataStoreClient()
# Build 63-day feature windows for 5 stocks at two dates
# See tft_model._build_sequences() for the window construction pattern

model = TFTSignalModel()
model.load("datastore/models/tft_signal_21d_v20260624_fold0")

attn_crash    = model.get_attention_weights(X_crash)     # (5, 63, 63)
attn_recovery = model.get_attention_weights(X_recovery)  # (5, 63, 63)

# Recent-weight ratio: last 10 steps vs first 10 steps
recent_bear  = attn_crash[:, -10:, :].mean(axis=(1, 2))
recent_bull  = attn_recovery[:, -10:, :].mean(axis=(1, 2))
print("Attention shift (bull > bear):", recent_bull > recent_bear)
```

### Result (fill in after execution)
| Stock | Attn shift | Map similarity | P(Buy) increase | Momentum gain |
|-------|-----------|----------------|-----------------|---------------|
| RELIANCE | PENDING | PENDING | PENDING | PENDING |
| HDFCBANK | PENDING | PENDING | PENDING | PENDING |
| TITAN | PENDING | PENDING | PENDING | PENDING |
| INFY | PENDING | PENDING | PENDING | PENDING |
| ICICIBANK | PENDING | PENDING | PENDING | PENDING |

**Overall HITL-04 result:** ⬜ PENDING

---

## HITL-05 · Signal Explanation Quality — SHAP Interpretability

**Spec:** `alphalens_docs/tests/10_hitl_tests.md#HITL-05`  
**Phase 3 extension:** Verify stacking ensemble SHAP explanations are coherent.

### Test Setup (when to run)
1. Run daily inference with the trained ensemble for at least 5 trading days.
2. Collect the top-5 SHAP drivers for every Buy signal generated (target: ≥ 10 signals).
3. Collect stacking confidence scores alongside the SHAP values.

### What to Verify (Human)
1. **Review 10 SHAP explanations for Buy signals.**  
   For each, ask: "Would a knowledgeable Indian market investor agree this justifies a Buy?"
   Target: ≥ 8/10 (80%) rated "makes sense."  
   Example acceptable driver: "momentum_3m=+22% driven 35% of score."  
   Example unacceptable: "fiscal_month_sin drove 40% of score."

2. **Calendar features must not dominate.**  
   If any calendar feature (`month_sin`, `month_cos`, `days_to_expiry`) appears in the
   top-3 SHAP drivers for ≥ 20% of signals → flag for model investigation.

3. **Stacking confidence correlates with SHAP concentration.**  
   High-confidence signals (stacking_confidence > 0.75) should have more concentrated
   SHAP waterfall plots (fewer features, higher magnitudes) than low-confidence ones
   (stacking_confidence < 0.55). Verify visually for 5 high vs 5 low confidence signals.

4. **BiLSTM and TFT attention consistency with SHAP.**  
   For ≥ 5 Buy signals, compare TFT's top-5 variable-selection features with LightGBM's
   top-5 SHAP features. At least 2–3 features should overlap (sign that both models
   are responding to the same market structure).

5. **Exit type / SHAP mismatch check.**  
   Review 5 exit signals. If exit_type = "Thesis Broken" but SHAP shows only
   technical momentum exhaustion → mismatch. Target: < 10% mismatch rate.

### Pass Criteria
- [ ] ≥ 80% of SHAP explanations rated "makes sense" by knowledgeable reviewer
- [ ] Calendar features not dominant (< 20% of signals have calendar in top-3)
- [ ] High-confidence signals show more concentrated SHAP waterfall
- [ ] ≥ 2–3 feature overlap between TFT VSN and LightGBM SHAP top-5
- [ ] < 10% exit type / SHAP mismatch

### Execution Notes
```python
from systems.ml_signal_engine.inference.daily_inference import run_daily_inference

# Run inference for a day with at least 5 buy signals
signals = run_daily_inference(as_of="2026-06-23")
buy_signals = signals[signals["ensemble_signal"] == "Buy"]

# For each buy signal, review the SHAP waterfall:
for _, row in buy_signals.head(10).iterrows():
    ticker = row["ticker"]
    shap_top5 = eval(row["shap_top5"])   # stored as JSON
    confidence = row["stacking_confidence"]
    print(f"{ticker}: confidence={confidence:.2f}  top5={shap_top5}")
```

### Result (fill in after execution)
| # | Ticker | Top-3 SHAP drivers | Makes sense? | Notes |
|---|--------|-------------------|--------------|-------|
| 1 | PENDING | PENDING | PENDING | |
| 2 | PENDING | PENDING | PENDING | |
| 3 | PENDING | PENDING | PENDING | |
| 4 | PENDING | PENDING | PENDING | |
| 5 | PENDING | PENDING | PENDING | |
| 6 | PENDING | PENDING | PENDING | |
| 7 | PENDING | PENDING | PENDING | |
| 8 | PENDING | PENDING | PENDING | |
| 9 | PENDING | PENDING | PENDING | |
| 10 | PENDING | PENDING | PENDING | |

Calendar feature in top-3: __/10 signals  
High vs low confidence SHAP concentration: PENDING  
TFT VSN / LGBM SHAP overlap: PENDING  
Exit type / SHAP mismatch: __/5 exits  

**Overall HITL-05 result:** ⬜ PENDING

---

## Phase 3 Gate Status

| Gate | Requirement | Status |
|------|-------------|--------|
| HITL-04 | Attention maps show regime-awareness | ⬜ PENDING — requires trained TFT |
| HITL-05 | ≥ 80% SHAP explanations coherent | ⬜ PENDING — requires ≥ 10 live signals |
| Sharpe improvement | ≥ 0.10 vs Phase 2 baseline | ⬜ PENDING — requires real-data backtest |
| All 9 integrity rules | Must pass on real data | ⬜ PENDING — synthetic failures expected |
| Stacking min-weight | All base models ≥ 0.10 | ✅ PASSING (unit tests) |

**Prerequisites before re-running:**
1. Overnight TFT + BiLSTM training (4–6h CPU, `schedule_overnight_training()`)
2. Real OHLCV data in DataStore (≥ 2 years, ≥ 50 stocks)
3. Run `python -m backtest.run_phase3_backtest --real --folds 5`
