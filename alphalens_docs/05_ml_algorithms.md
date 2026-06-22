# AlphaLens — ML Algorithm Specification
## Confirmed Choices · Library Versions · HPO Protocol

---

## Phase 1 Algorithms (Weeks 1–14)

| Algorithm | Task | Library | Intel CPU OK? |
|-----------|------|---------|---------------|
| LightGBM 4.6 | Signal models, P&D, exit, meta-labeler | `lightgbm>=4.5` | ✅ Yes |
| CatBoost 1.2 | Ensemble partner (categorical features) | `catboost>=1.2` | ✅ Yes |
| GaussianHMM | Regime detection | `hmmlearn>=0.3.2` | ✅ Yes |
| CQR / ACI | Conformal uncertainty | `mapie>=1.3.0` | ✅ Yes |
| IsolationForest | P&D anomaly pre-layer | `scikit-learn>=1.5` | ✅ Yes |
| SMOTETomek | Class imbalance | `imbalanced-learn>=0.12` | ✅ Yes |
| Optuna TPE | Hyperparameter optimization | `optuna>=4.7` | ✅ Yes |
| CoxPH | Exit timing survival | `lifelines>=0.28` | ✅ Yes |
| PELT | Changepoint detection | `ruptures>=1.1.9` | ✅ Yes |
| HDBSCAN | Stock clustering | `hdbscan>=0.8.38` | ✅ Yes |
| ADWIN | Drift monitoring | `river>=0.21` | ✅ Yes |

## Phase 3 Algorithms (Weeks 24–36)

| Algorithm | Task | Library | Notes |
|-----------|------|---------|-------|
| TFT | Deep ensemble 21d/63d | `pytorch-forecasting>=1.1` | Slow without GPU |
| BiLSTM | Sequential patterns | `torch>=2.4` | Overnight CPU |
| Mamba-2 | Long-sequence 252d | `mamba-ssm>=2.0` | Ubuntu/Linux only |
| TabNet | Feature selection | `pytorch-tabnet>=4.1` | Research tool only |

## Phase 4 (Week 36+)

| Algorithm | Task | Library | Prerequisite |
|-----------|------|---------|--------------|
| PPO | RL meta-agent | `stable-baselines3` | 3mo paper trading |

---

## Dropped (Never Implement)

| Algorithm | Reason |
|-----------|--------|
| GNN | Supply chain graph data unavailable |
| VAE | IsolationForest is sufficient |
| DQN | PPO is strictly better for continuous actions |
| Bayesian NNs | Conformal prediction supersedes |
| Random Forest | Dominated by gradient boosting |
| KMeans | Replaced by HDBSCAN |
| LLM alphas / ESG / Satellite | Data unavailable at scale |

---

## Optuna HPO Protocol

```python
import optuna
import lightgbm as lgb

def lgb_objective(trial, X_train, y_train, X_val, y_val):
    params = {
        'n_estimators':     trial.suggest_int('n_estimators', 100, 2000),
        'max_depth':        trial.suggest_int('max_depth', 3, 12),
        'learning_rate':    trial.suggest_float('learning_rate', 0.005, 0.3, log=True),
        'num_leaves':       trial.suggest_int('num_leaves', 20, 300),
        'min_child_samples':trial.suggest_int('min_child_samples', 5, 100),
        'subsample':        trial.suggest_float('subsample', 0.5, 1.0),
        'colsample_bytree': trial.suggest_float('colsample_bytree', 0.5, 1.0),
        'reg_alpha':        trial.suggest_float('reg_alpha', 1e-8, 10.0, log=True),
        'reg_lambda':       trial.suggest_float('reg_lambda', 1e-8, 10.0, log=True),
        'objective': 'multiclass', 'num_class': 3, 'verbosity': -1,
    }
    model = lgb.LGBMClassifier(**params)
    model.fit(X_train, y_train,
              eval_set=[(X_val, y_val)],
              callbacks=[lgb.early_stopping(50, verbose=False)])
    # Walk-forward Sharpe as objective (not accuracy)
    return compute_walk_forward_sharpe(model, X_val, y_val)

study = optuna.create_study(direction='maximize',
                             sampler=optuna.samplers.TPESampler())
study.optimize(lgb_objective, n_trials=100, timeout=3600)
```

**Rules:** Always optimize on validation fold. Never on test fold. Never on random split.

---

## Conformal Prediction (MAPIE ACI)

```python
from mapie.regression import MapieQuantileRegressor

# Use ACI (Adaptive Conformal Inference) for time-series non-exchangeability
mapie = MapieQuantileRegressor(estimator=lgb_q_model,
                                method="quantile", cv="split", alpha=0.10)
mapie.fit(X_cal, y_cal)
y_pred, y_pis = mapie.predict(X_test, alpha=0.10)
# y_pis[:, 0, 0] = lower bound  (90% coverage guaranteed)
# y_pis[:, 0, 1] = upper bound
```

---

## Class Imbalance Rules

| Model | Positive Rate | Primary Strategy | Secondary |
|-------|-------------|-----------------|-----------|
| Multibagger | ~2–5% | Focal Loss + lambdarank | SMOTETomek |
| P&D Detector | ~1–3% | SMOTETomek + scale_pos_weight | IsolationForest |
| Forensic ML | < 1% | Focal Loss + cost matrix | SMOTE |

**Threshold optimization (always):**
```python
from sklearn.metrics import precision_recall_curve
prec, rec, thresholds = precision_recall_curve(y_val, y_prob)
# Select threshold maximizing F1 (or precision@target_recall)
f1 = 2 * prec * rec / (prec + rec + 1e-8)
optimal_threshold = thresholds[f1.argmax()]
```

---

## Incremental Model Updates (warm-start)

```python
# LightGBM warm-start — 10x faster than full retrain
new_model = lgb.LGBMClassifier(**best_params, n_estimators=200)
new_model.fit(new_data_X, new_data_y, init_model=existing_model)
```

---

## Intel CPU Optimization (oneDAL)

```python
# Install: pip install daal4py
# Accelerates LightGBM, XGBoost, CatBoost inference on Intel hardware
from daal4py.sklearn.ensemble import GBTDAALClassifier
fast_model = GBTDAALClassifier.convert_model(trained_lgb_model)
predictions = fast_model.predict(X)  # 2–5x faster on Intel CPU
```
