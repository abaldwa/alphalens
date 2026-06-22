# AlphaLens — Model Specifications
## All 16 Models · Inputs, Outputs, Training, Retraining

---

## Model Interface Standard
Every model must implement this interface:
```python
class BaseModel:
    def train(self, X_train, y_train, X_val, y_val) -> None
    def predict(self, X: pd.DataFrame) -> pd.DataFrame
    def predict_proba(self, X: pd.DataFrame) -> np.ndarray
    def save(self, path: str) -> None
    def load(self, path: str) -> None
    def get_feature_importance(self) -> pd.Series  # SHAP values
```

---

## Phase 1 Models

### M-01 · HMM Regime Detection
- **File:** `systems/ml_signal_engine/models/hmm/regime_detector.py`
- **Algorithm:** `hmmlearn.GaussianHMM`, n_components=4, covariance_type='full'
- **Purpose:** Classify each stock and the Nifty 50 into 4 regime states daily.
  Regime context conditions all downstream models.
- **Inputs (5 observables):**
  1. `daily_return` — (close/prev_close - 1)
  2. `log_return` — log(close/prev_close)
  3. `realized_vol_10d` — rolling 10d std of log returns × √252
  4. `volume_ratio_20d` — volume / SMA(volume, 20)
  5. `atr_pct` — ATR(14) / close × 100
- **Outputs (6 features per stock):**
  - `hmm_regime` — integer 0–3
  - `hmm_regime_prob_bullish` — probability of bullish state
  - `hmm_regime_prob_bearish` — probability of bearish state
  - `hmm_regime_duration` — days in current regime
  - `hmm_regime_transition` — binary flag: 1 if transitioned today
  - `hmm_regime_stability` — max(state_probabilities), proxy for confidence
- **States (labeled post-hoc by mean return):**
  - 0 = Bearish (negative mean return, high vol)
  - 1 = Sideways (near-zero return, low vol)
  - 2 = Volatile (high vol, ambiguous direction)
  - 3 = Bullish (positive mean return, medium vol)
- **Training:**
  ```python
  from hmmlearn import hmm
  model = hmm.GaussianHMM(n_components=4, covariance_type='full',
                           n_iter=1000, random_state=42)
  # Fit 10-20 random starts, keep best BIC
  best_model = min(
      [hmm.GaussianHMM(..., random_state=i).fit(obs) for i in range(20)],
      key=lambda m: -m.score(obs)
  )
  ```
- **Retrain:** Monthly + when log-likelihood drops below 10th percentile of training period
- **Run TWO HMMs:** (1) Market-wide on Nifty 50. (2) Per-stock. When Nifty HMM = Bearish →
  reduce all position sizes by 50%.
- **Scope:** All tiers

---

### M-02 · Signal Model 5d
- **File:** `systems/ml_signal_engine/models/signal/signal_5d.py`
- **Algorithm:** LightGBM 4.6 (primary) + CatBoost 1.2 + XGBoost 3.2 → stacking ensemble
- **Purpose:** Buy/Hold/Sell direction over next 5 trading days.
- **Inputs (Phase 1):** 76 core technical + 8 intraday + 7 calendar + 6 HMM + 14 macro = 111 features
- **Inputs (Phase 2+):** Adds 28 fundamental + 12 governance
- **Outputs:**
  - `signal_5d_buy_prob` — probability of Buy
  - `signal_5d_hold_prob` — probability of Hold
  - `signal_5d_sell_prob` — probability of Sell
  - `signal_5d_q10` — 10th percentile expected return (worst case)
  - `signal_5d_q50` — median expected return
  - `signal_5d_q90` — 90th percentile expected return (best case)
- **Labeling (Triple-Barrier):**
  ```python
  # Native SPEC-MODEL-002 implementation; mlfinlab is intentionally not used
  from systems.ml_signal_engine.training.labeling import compute_triple_barrier_labels

  labels = compute_triple_barrier_labels(
      close=close,
      atr=atr_series,
      horizon_days=5,
      profit_multiplier=1.5,
      stop_multiplier=1.5,
      vertical_barrier_days=5,
  )  # 1=Buy, -1=Sell, 0=Hold
  ```
- **Training:**
  ```python
  # Walk-forward, expanding window
  # SMOTE for imbalance
  from imblearn.combine import SMOTETomek
  smote_tomek = SMOTETomek(random_state=42)
  X_res, y_res = smote_tomek.fit_resample(X_train, y_train)
  # Optuna HPO — 100 trials on validation fold
  # Stacking: LightGBM + CatBoost out-of-fold predictions → LogisticRegression meta
  ```
- **Quantile training (run 3 separate models):**
  ```python
  lgb_q10 = lgb.LGBMRegressor(objective='quantile', alpha=0.10)
  lgb_q50 = lgb.LGBMRegressor(objective='quantile', alpha=0.50)
  lgb_q90 = lgb.LGBMRegressor(objective='quantile', alpha=0.90)
  ```
- **Retrain:** Monthly or when rolling 63d accuracy < 45% or PSI > 0.25 for 10+ days
- **Scope:** Tier 1–4

---

### M-03 · Signal Models 21d and 63d
- **File:** `systems/ml_signal_engine/models/signal/signal_21d.py`, `systems/ml_signal_engine/models/signal/signal_63d.py`
- **Algorithm:** Same as M-02 but with wider triple-barrier thresholds
- **Inputs (21d):** ~111 features + MF holdings
- **Inputs (63d):** ~165 features (adds 28 fundamental + 12 governance)
- **Barrier widths:** 21d = 3× ATR, 63d = 5× ATR
- **Outputs:** Same structure as M-02 but for respective horizons
- **Note:** 63d model only trains after Phase 2 fundamentals are flowing.
  Until then, 21d serves as the medium-term model.
- **Scope:** Tier 1–2 (both), Tier 3 (21d only), Tier 4 (excluded)
- **Retrain:** Same cadence as M-02. New quarterly fundamentals trigger incremental
  update for 63d model using LightGBM `init_model` warm-start.

---

### M-04 · Meta-Labeler
- **File:** `systems/ml_signal_engine/models/signal/meta_labeler.py`
- **Algorithm:** LightGBM (binary classification)
- **Purpose:** "Should I act on this signal?" — filters false positives from primary models.
- **Inputs:** All primary model features + primary model's Buy/Sell probability + conformal
  interval width + HMM regime + days_since_last_signal + current_drawdown_from_peak
- **Outputs:**
  - `meta_label_act` — binary: 1=Act, 0=Don't-Act
  - `meta_label_prob` — probability of Act
- **Labeling (CRITICAL DIFFERENCE from M-02):**
  Label = 1 if the signal was profitable AFTER transaction costs (~0.5% round-trip).
  NOT just "was the direction correct". A stock that goes up 0.3% after a Buy signal
  is directionally correct but labeled 0 (unprofitable after costs).
- **Retrain:** Simultaneously with primary signal models
- **Usage:** Signal at 72% + Meta Act at 68% = high conviction.
  Signal at 72% + Meta Don't-Act at 30% = skip regardless.
- **Scope:** Tier 1–3

---

### M-05 · Conformal Prediction
- **File:** `systems/ml_signal_engine/models/uncertainty/conformal.py`
- **Algorithm:** MAPIE >= 1.3.0 with Adaptive Conformal Inference (ACI)
- **Purpose:** Calibrated prediction intervals with guaranteed coverage for all signal models.
- **Key:** Use ACI variant (not standard CQR) — financial time series is non-exchangeable
  (standard conformal assumption violated by regime changes and temporal autocorrelation).
- **Implementation:**
  ```python
  from mapie.regression import MapieQuantileRegressor
  from mapie.conformity_scores import QuantileConformityScore

  # Wrap any trained model
  mapie = MapieQuantileRegressor(
      estimator=lgb_quantile_model,
      method="quantile",
      cv="split",
      alpha=0.1  # 90% coverage
  )
  mapie.fit(X_cal, y_cal)
  y_pred, y_pis = mapie.predict(X_test, alpha=0.1)
  # y_pis[:, 0, 0] = lower bound, y_pis[:, 0, 1] = upper bound
  ```
- **Outputs:** `conformal_lower`, `conformal_upper`, `conformal_width`
- **Recalibrate:** Every time underlying model is retrained + monthly ACI online update
- **Scope:** Tier 1–2

---

### M-06 · Pump & Dump Detector
- **File:** `systems/ml_signal_engine/models/pnd/pnd_detector.py`
- **Algorithm:** LightGBM (primary) + IsolationForest (anomaly layer, `scikit-learn`)
- **Purpose:** Pre-filter — runs BEFORE any buy signal reaches user. Hard block if score > 60.
- **Inputs:** 22 P&D features (see 01_features.md)
- **Outputs:**
  - `pnd_score` — float 0–100 (weighted combination of LightGBM + IsolationForest)
  - `pnd_phase` — categorical: 'accumulation'/'pump'/'dump'/'aftermath'/'normal'
  - `pnd_block` — binary: 1 if score > 60 (all buys blocked)
  - `pnd_flag` — binary: 1 if score > 40 (alert user)
- **Labeling:**
  Positive = confirmed P&D episode: ≥100% price rise in < 60 days + ≥50% fall within
  90 days + no recovery > 30% in next 180 days. Exclude corporate-action-driven moves.
- **Class imbalance:** ~1–3% positive rate. Use SMOTETomek + `scale_pos_weight` in LightGBM.
- **Threshold:** Never use 0.5. Optimize on validation fold: maximize recall at precision ≥ 70%.
- **CRITICAL:** This model blocks stocks from ALL downstream buy signals.
  If P&D score > 60, no buy signal fires regardless of what M-02/03 outputs.
  Also blocks from multibagger watchlist.
- **Retrain:** Quarterly + immediately when new confirmed P&D episode identified
- **Scope:** ALL tiers — highest priority for Tier 3–4

---

### M-07 · Exit Signal Model
- **File:** `systems/ml_signal_engine/models/exit/exit_signal.py`
- **Algorithm:** LightGBM (urgency regression) + Cox Proportional Hazards (`lifelines`)
- **Purpose:** For every held position: urgency score + exit type + survival curve.
- **Inputs:** All available features + position-specific:
  - `entry_price`, `current_price`, `unrealized_pnl_pct`
  - `days_held`, `peak_price_since_entry`, `drawdown_from_peak`
  - `sector_regime`, `current_pnd_score`
- **Outputs:**
  - `exit_urgency` — float 0–100
  - `exit_type` — categorical: 'thesis_broken'/'momentum_exhaustion'/
    'risk_management'/'target_achieved'/'opportunity_cost'/'pnd_exit'
  - `exit_survival_5d`, `_10d`, `_21d`, `_63d` — probability position remains profitable
- **Action thresholds:**
  - urgency > 80 → exit today (alert: URGENT)
  - urgency 60–80 → reduce 50% (alert: WARNING)
  - urgency 40–60 → monitor (shown on dashboard)
- **Cox PH implementation:**
  ```python
  from lifelines import CoxPHFitter
  cph = CoxPHFitter(penalizer=0.1)
  cph.fit(df, duration_col='days_held', event_col='position_gone_negative',
          formula='pnd_score + hmm_regime + momentum_3m + drawdown_from_peak')
  cph.predict_survival_function(X_new)
  ```
- **Always show exit TYPE:** "Promoter pledge rose 8% → 22%" is actionable. Bare "Sell" is not.
- **Retrain:** Monthly + trade outcomes feed incremental update
- **Scope:** Tier 1–3

---

## Phase 2 Models

### M-08 · Multibagger Detection Model
- **File:** `systems/ml_signal_engine/models/multibagger/multibagger_model.py`
- **Algorithm:** LightGBM (lambdarank objective, primary) + CatBoost + Random Survival Forest
- **Purpose:** Score every stock weekly by probability of 2x–10x returns over 1–5 years.
  Maintains top-20 weekly watchlist.
- **Run:** Weekly (Monday, after market close). Not daily — long-horizon signals don't change day-to-day.
- **Inputs:** 109 features (76 core + 33 multibagger-specific + 28 fundamental + 12 governance).
  After correlation pruning: 50–70 features.
- **Two-tower architecture:**
  - Technical tower: 76 core + 33 multibagger + momentum + volatility
  - Fundamental tower: 28 fundamental + 12 governance (sector-relative z-scored)
  - Fusion: concatenation (Option A, start here) → stacking (Option B, later)
- **Outputs:**
  - `mb_probability` — float 0–1
  - `mb_tier` — categorical: '2x'/'3x'/'5x'/'10x'
  - `mb_archetype` — 'long_base_breakout'/'post_crash_recovery'/
    'quiet_accumulator'/'sector_rotation_leader'
  - `mb_survival_6m`, `_12m`, `_18m`, `_24m`, `_36m`, `_60m`
  - `mb_shap_top5` — JSON list of top 5 SHAP feature contributions
  - `mb_analogues` — JSON list of 3 most similar historical multibaggers
- **Labeling:**
  ```python
  # Multi-tier label construction
  # Scan 5-year forward window for each stock-month
  labels = []
  for date, stock in universe:
      fwd_prices = get_prices(stock, date, date + timedelta(days=5*365))
      max_return = fwd_prices.max() / fwd_prices.iloc[0] - 1
      if max_return >= 9.0: label = '10x'
      elif max_return >= 4.0: label = '5x'
      elif max_return >= 2.0: label = '3x'
      elif max_return >= 1.0: label = '2x'
      else: label = 'none'
      labels.append(label)
  # EXCLUDE all P&D-flagged episodes from positive labels
  ```
- **Class imbalance:** ~2–5% positive rate. Use Focal Loss + SMOTETomek + lambdarank.
- **Lambdarank (ranking formulation preferred over classification):**
  ```python
  lgb_ranker = lgb.LGBMRanker(objective='lambdarank', metric='ndcg',
                                ndcg_eval_at=[10, 20])
  ```
- **Sector normalization:** Apply sector-relative z-scores BEFORE training.
  Minimum partition: Financials vs Non-Financials.
- **Retrain:** Quarterly (aligned with fundamental data refresh)
- **Scope:** Tier 1–3 only. Excluded from Tier 4 — false positive rate too high.

---

### M-09 · Forensic Accounting — Classical Scores
- **File:** `systems/ml_signal_engine/models/forensic/classical_scores.py`
- **Algorithm:** Pure formula computation. No ML. No training.
- **Purpose:** Detect financial manipulation using established academic models.
  Immediate value — requires no training data.
- **Models (7 + composite):**
  1. **Beneish M-Score** — M > -1.78 = likely manipulator
     Formula: -4.84 + 0.920×DSRI + 0.528×GMI + 0.404×AQI + 0.892×SGI
              + 0.115×DEPI - 0.172×SGAI + 4.679×TATA - 0.327×LVGI
  2. **Altman Z-Score** — Z < 1.81 = distress zone, Z > 2.99 = safe
  3. **Piotroski F-Score** — 9 binary signals, sum = 0–9 (higher = stronger)
  4. **Ohlson O-Score** — bankruptcy probability
  5. **Dechow F-Score** — earnings manipulation probability
  6. **Sloan Accrual Ratio** — (NI - CFO) / Total Assets (high = low earnings quality)
  7. **Benford's Law** — chi-squared test on digit distribution of financial figures
- **Outputs:**
  - All 7 individual scores (on their own scales)
  - `forensic_composite_score` — weighted 0–100 composite
  - `forensic_flag` — 'green' (< 25) / 'amber' (25–60) / 'red' (> 60)
- **These 30 features also feed directly into signal models** as features (Phase 2).
- **Retrain:** Never — deterministic formulas. Weights reviewed annually.
- **Scope:** Tier 1–3 (Tier 4: Beneish + pledge only)

---

### M-10 · Forensic Accounting — ML Ensemble
- **File:** `systems/ml_signal_engine/models/forensic/forensic_ml.py`
- **Algorithm:** LightGBM + XGBoost (supervised) + IsolationForest + 12 sector-specific models
- **Purpose:** ML fraud detection trained on confirmed Indian cases.
- **Training cases (positive labels):**
  Satyam, DHFL, IL&FS subsidiaries, Yes Bank, PC Jeweller, Vakrangee,
  Manpasand Beverages, Bhushan Steel, Kingfisher Airlines, Gitanjali Gems
- **Inputs:** 84 forensic features (Groups A–I in forensic specification)
- **Outputs:**
  - `fraud_probability` — float 0–1
  - `fraud_shap_top5` — top 5 SHAP drivers
  - `fraud_pattern_match` — nearest known fraud case by feature similarity
  - `forensic_meta_score` — combined classical (M-09) + ML score (0–100)
- **12 sector-specific models:** Banking/NBFC, Insurance, IT Services, Pharma, FMCG,
  Infrastructure/EPC, Auto, Metals/Commodities, Chemicals, Textiles, Telecom, Power/Energy
- **Class imbalance:** < 1% positive rate. Use Focal Loss + aggressive SMOTE +
  FN cost >> FP cost (false negatives = missed frauds = much worse than false positives).
- **Retrain:** Semi-annually + immediately when new confirmed Indian fraud case identified
- **Scope:** Tier 1–2 full ML, Tier 3 classical only (M-09), Tier 4 Beneish + pledge only

---

## Phase 3 Models (Deep Learning)

### M-11 · Temporal Fusion Transformer (TFT)
- **File:** `systems/ml_signal_engine/models/deep/tft_model.py`
- **Algorithm:** pytorch-forecasting TFT
- **Purpose:** Deep learning ensemble member. Handles mixed-frequency inputs natively.
  Provides attention-based interpretability.
- **Inputs:** Last 63 days of core technical features as temporal sequence +
  quarterly fundamentals as static covariates (sector, promoter holding, D/E ratio)
- **Outputs:** Return probability distribution + temporal + variable attention maps
- **Library:** `pytorch-forecasting`, PyTorch Lightning backend
- **Do NOT replace LightGBM with TFT.** Use as ensemble member only.
- **Final prediction:** Weighted average of LightGBM + CatBoost + TFT,
  weights learned by stacking meta-learner (M-13).
- **Retrain:** Quarterly. Expensive — schedule overnight.
- **Scope:** Tier 1

### M-12 · BiLSTM + Mamba-2
- **File:** `systems/ml_signal_engine/models/deep/bilstm_model.py`
- **Algorithm:** Bidirectional LSTM (2-layer, 128 hidden, dropout=0.3) + Mamba-2
- **Purpose:** Sequential pattern capture. BiLSTM for 21–63d sequences.
  Mamba-2 for 252d+ histories with linear memory.
- **Use Mamba-2 specifically** (`mamba-ssm >= 2.0`). NOT Mamba-1 (slow). NOT Mamba-3 (research-only).
- **Key use case:** Exit momentum exhaustion patterns, multibagger base formations.
- **Ubuntu/Linux only** for mamba-ssm. Windows users: use WSL2 or mambular library.
- **Scope:** Tier 1

### M-13 · Stacking Ensemble Meta-Learner
- **File:** `systems/ml_signal_engine/models/deep/stacking.py`
- **Algorithm:** LogisticRegression (primary meta-learner)
- **Purpose:** Combine LightGBM + CatBoost + XGBoost + TFT + BiLSTM/Mamba predictions.
- **CRITICAL:** Train on OUT-OF-FOLD predictions only. Never on full training data.
- **Minimum weight per base model = 0.1** — never completely zero out any model.
- **Keep meta-learner simple.** Complex meta-learner memorizes error patterns.

### M-14 · TabNet Feature Selection Validator
- **File:** `systems/ml_signal_engine/training/feature_selection.py`
- **Purpose:** Research tool. Not deployed in production pipeline.
  If BOTH TabNet attention + LightGBM SHAP agree a feature is unimportant → drop it.
- **Library:** `pytorch-tabnet`

---

## Phase 4 Models

### M-15 · PPO Reinforcement Learning Meta-Agent
- **File:** `systems/ml_signal_engine/models/rl/ppo_agent.py` (Phase 4 only)
- **Algorithm:** PPO via `stable-baselines3` + custom `gymnasium` environment
- **DEFER** — Do not start until all Phase 1–3 models are stable and paper-trading
  for 3+ months. Use rules-based position sizing until then.
- **State vector (30-dim):** signal probs (9) + meta (1) + conformal width (1) +
  HMM regime (4) + market regime (4) + P&D score (1) + forensic score (1) +
  portfolio state (5) + drift score (1) + recent accuracy (3)
- **5 sub-policies (one per regime):** Bull / Bear / Sideways / High-Vol / Transition
- **5-stage bootstrapping before live use:**
  1. Supervised baseline → generate 500K+ experiences
  2. Offline PPO on replay buffer
  3. Synthetic scenario augmentation
  4. Paper trading validation (3+ months)
  5. Live deployment with position size safety caps

### M-16 · PSI Drift Monitor + ADWIN
- **File:** `ingestion/quality/drift_monitor.py`
- **Build in Phase 1.** This IS the retrain trigger for all other models.
- **PSI:** Weekly computation for top 50 features vs baseline.
  PSI > 0.1 = moderate (alert + reduce positions 50% + schedule retrain).
  PSI > 0.25 = severe (halt new positions + immediate retrain).
- **ADWIN:** Daily accuracy stream monitoring per model. From `river` library.
- **Retrain protocol when triggered:**
  1. Snapshot current model
  2. Train new version on full available history
  3. Shadow-test on last 63 days
  4. Compare accuracy + calibration + SHAP feature rank stability
  5. Promote to production only if new model wins on 2 of 3 criteria

---

## Model Retraining Schedule

| Model | Frequency | Runtime Estimate |
|-------|-----------|-----------------|
| HMM (500 stocks) | Monthly | ~40 min total |
| Signal 5d | Monthly | ~30 min |
| Signal 21d | Monthly | ~30 min |
| Signal 63d | Quarterly | ~45 min |
| Meta-Labeler | With primary | ~15 min |
| P&D Detector | Quarterly | ~20 min |
| Exit Signal | Monthly | ~20 min |
| Multibagger | Quarterly | ~60 min |
| Forensic classical | Never | Deterministic |
| Forensic ML | Semi-annually | ~45 min |
| TFT/BiLSTM/Mamba | Quarterly | Overnight |
| Stacking meta | With any base | < 5 min |

---

## Position Sizing (Rules-Based until Phase 4 RL)

```python
def compute_position_size(signal_prob, meta_prob, conformal_width,
                           pnd_score, hmm_market_regime,
                           portfolio_value, current_cash):
    # Hard blocks
    if pnd_score > 60: return 0
    if signal_prob < 0.65: return 0
    if meta_prob < 0.50: return 0

    # Base size: 5% of portfolio
    base_size = 0.05 * portfolio_value

    # Scale up with conviction
    conviction = (signal_prob - 0.65) / 0.35  # 0 to 1
    size = base_size * (1 + conviction)  # 5% to 10%

    # Scale down with uncertainty (wide conformal interval)
    uncertainty_penalty = min(conformal_width / 0.10, 1.0)  # 10% width = full penalty
    size *= (1 - 0.5 * uncertainty_penalty)

    # Halve in bearish market regime
    if hmm_market_regime == 0:  # Bearish
        size *= 0.5

    # Cap at 10% of portfolio
    size = min(size, 0.10 * portfolio_value)

    # Ensure we have the cash
    size = min(size, current_cash * 0.95)

    return size
```
