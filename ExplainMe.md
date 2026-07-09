# AlphaLens — ExplainMe FAQ

Living FAQ built from the truthful-mode "Explain-Me Walkthrough" series
(PHASE X, prompts X.0–X.10) in `CLAUDE_CODE_PROMPTS.md`. Each section below
corresponds to one module of that series and is only added once that module
has been reviewed and finalized — this file should never contain unverified
or in-progress answers.

## Model Thresholds: SIGNAL_THRESHOLD, META_THRESHOLD, PND_BLOCK_THRESHOLD

**Q: What are `SIGNAL_THRESHOLD`, `META_THRESHOLD`, and `PND_BLOCK_THRESHOLD` (`config/settings.py:194-196`), how are they calculated, and what happens when a ticker crosses them?**

### PND_BLOCK_THRESHOLD = 60 — real, wired, static

This one is a fixed constant actually used in production, in `systems/ml_signal_engine/models/pnd/pnd_detector.py:45,219,244`.

**How it's calculated:** the P&D (pump-and-dump) detector produces a `pnd_score` per ticker per day from a set of classical, rule-based red flags computed in `features/pnd_features.py` — delivery-volume collapse relative to its 4-week average, delivery spikes (>1.5x the 4-week average), and related volume/price anomaly features. These are combined into a single 0-100 score (not a probability from a trained model — a heuristic composite).

**Implication:** `pnd_score > 60` → `pnd_block = True`. A blocked ticker is removed from the buy-candidate universe for that day **before** the signal/meta-label models ever see it (`daily_inference.py`'s pipeline runs PND as a pre-filter, not a post-filter) — so a high P&D score overrides everything else the ML models might otherwise say about that ticker. There's a second, softer tier: `PND_FLAG_THRESHOLD = 40` marks a ticker as "flagged" (visible in dashboards/alerts) without blocking it outright.

**Why static, not tuned:** unlike the signal/meta-label models below, this is a rule-based score, not a trained classifier — there's no validation-fold threshold-tuning step for it, so 60/40 are the same kind of fixed, judgment-call constants as `MIN_MODEL_ACCURACY` or `PSI_SEVERE_THRESHOLD` elsewhere in `settings.py`.

### SIGNAL_THRESHOLD = 0.65 and META_THRESHOLD = 0.50 — defined but dead; real thresholds are dynamically tuned

Grepping the full codebase (excluding `settings.py` itself) turns up **zero** references to either constant. The actual signal-generation and meta-labeling models do not use fixed thresholds at all:

- **Signal model** (`systems/ml_signal_engine/models/signal/base_signal_model.py`): produces per-class probabilities (5-day and 63-day horizon classifiers), then `_optimize_thresholds()` (line 514) picks a **separate threshold per class**, maximizing one-vs-rest F1 on a held-out validation fold — never a hardcoded 0.65. The class whose probability clears its own tuned threshold by the largest margin wins; "Hold" wins if no class clears its threshold. This re-tunes every time the model retrains (`SPEC-MODEL-004`, whose own comment says explicitly: "threshold never 0.5 default").
- **Meta-labeler** (`systems/ml_signal_engine/models/signal/meta_labeler.py`): decides Act/Don't-Act on top of the signal model's call. `_optimize_precision_threshold()` picks the threshold that maximizes precision subject to a minimum recall floor, again tuned on validation data via `tune_threshold()` (line 171) — its docstring likewise says "never 0.5 default." The class default of `0.5` (line 80) is only ever the *starting* value before `train()`/`tune_threshold()` overwrite it.

**Calculation, when real tuning runs:** both use the same shape of optimization — sweep candidate thresholds against a probability array from the trained model, score each candidate by the objective (F1 for the signal model, precision-subject-to-recall-floor for the meta-labeler), pick the winner. This happens once per training/retraining cycle, not per inference call — a given trained model version carries its own tuned threshold(s) saved alongside it (`base_signal_model.py:264`, `meta_labeler.py:231`), reloaded at inference time.

**Implication of the dead settings:** `SIGNAL_THRESHOLD`/`META_THRESHOLD` read as if the system runs on a fixed 0.65/0.50 cutoff — it doesn't. These appear to be leftover defaults from before the dynamic-tuning design (`SPEC-MODEL-004`) was implemented, never removed once real tuning replaced them.

**Recommended fallback use (per user decision, 2026-07-04):** rather than delete them, treat `SIGNAL_THRESHOLD = 0.65` and `META_THRESHOLD = 0.50` as the **fallback values** to use if a trained model somehow has no saved tuned threshold (e.g. a corrupted/incomplete model artifact, or a bootstrap run before any real training has happened). This gives the dead constants a real purpose — a safety net — without changing the primary tuned-threshold behavior. Not yet wired into code; see `FutureDevelopment.md`.

## AlphaLens.ML

### Daily Insights — Expected Return / Confidence, and why Meta/Interval/P&D/Regime show empty

**Q: How is "Expected Return" and "Confidence" on the Top Buy Signals cards derived — calculated or hardcoded?**

Both are real model outputs, not display heuristics:

- **Confidence** = `buy_prob`, the `signal_5d` model's own predicted probability for the "buy" class — a stacking-ensemble LightGBM classifier (`systems/ml_signal_engine/models/signal/base_signal_model.py:145`), trained per SPEC-MODEL-004 with per-class F1-optimized thresholds (see the threshold section above), not a bare 0.5 cutoff.
- **Expected Return** = `q50_return`, the median-quantile output of one of **three independent LightGBM quantile regressors** trained on continuous forward return (`objective="quantile", alpha=0.10/0.50/0.90`, `base_signal_model.py:439-462`). `q10`/`q90` give the downside/upside band shown as "Range: X to Y" — these are genuinely different models from the buy/hold/sell classifier, sharing the same feature set.

Both numbers come from `daily_inference.py`'s `signal_5d` scoring step, which runs daily — verified live for 2026-07-03/07-04.

**Q: Why do Meta / Interval / P&D / Regime show empty on the Top Buy Signals table?**

Root cause: each model in the daily pipeline writes **its own row** in `ml_signals`, keyed by `(date, ticker, model_name)` — there is no fused, one-row-per-ticker view. `daily_inference.py:238-253` writes `meta_label`/`meta_prob` onto a **separate row with `model_name='meta_labeler'`**, not onto the `signal_5d` row. Same for `pnd_detector` (P&D) and `hmm_market` (Regime — and that one is additionally scoped to a sentinel ticker `'MARKET'`, never a per-ticker row at all). `conformal_lower`/`conformal_upper` (Interval) are null for a different reason: a `conformal_signal5d` model is trained and sits in `datastore/models/registry.json`, but nothing in `daily_inference.py` ever calls it or writes those columns — it's fully unwired (see `FutureDevelopment.md`, "wire multibagger/forensic/21d/63d/conformal/SHAP into the daily scheduler").

The Daily Insights table (`hub.js`) and the Top Buy Signals list both read `top_buys`, which is filtered to `model_name = 'signal_5d'` only — so Meta/P&D/Regime are structurally guaranteed to read null there even on days every model ran cleanly. The full per-model breakdown for a given ticker/date **does** exist and is visible on Signal Deep Dive (`GET /api/v1/signals/ml/{ticker}/{date}`, unfiltered by model_name).

**Q: Why aren't 21-day/63-day recommendations shown on Daily Insights?**

`signal_21d`/`signal_63d` are trained models registered in `registry.json`, but `daily_inference.py` only scores `signal_5d` daily — the 21d/63d horizon watchlist (`WATCHLIST_MODELS` in `scripts/run_daily_paper_trading.py:106`) queries `top_buys` for those model names and gets `[]` back every day (verified live for 2026-07-03), because nothing ever writes rows for them. This is the same "trained but not wired into the daily scheduler" gap as multibagger/forensic/conformal — see `FutureDevelopment.md`.

**Q: Why don't the stocks recommended in Paper Trading show up in Daily Insights' Top Buy Signals?**

They should, and mostly do — Paper Trading's Pending Actions are proposed off the exact same `signal_5d` `buy_prob` ranking Daily Insights uses (`reason: "buy_prob=0.XX"` in every pending row). The two lists can visibly diverge for two structural reasons, not a bug: (1) Daily Insights' top_buys is capped to `n=5`, while Pending Actions considers a wider candidate set before `PortfolioSimulator.can_buy()` gates (cash/sector/position caps) reject some — a stock outside the top-5 shown on Daily Insights can still appear as a Pending Action if it cleared the bot's internal ranking cutoff; and (2) once a stock is already an open position, it's excluded from new-entry candidates entirely, so it can vanish from Pending Actions while still ranking high on Daily Insights' pure signal list.

### Signal Deep Dive — the three models shown under "All Model Scores"

Only three model rows are ever populated for a given ticker/date because these are the only three `daily_inference.py` scores per-ticker (Regime is market-wide, Exit only fires for held positions):

- **`signal_5d`** — 5-day-forward buy/hold/sell classifier plus the three quantile regressors described above. This is the only model the paper-trading bot actually trades off.
- **`meta_labeler`** — a secondary gate on top of `signal_5d`: given the signal model already called "buy", `meta_labeler` estimates the probability that call is worth acting on (`meta_label_prob`), meant to suppress low-conviction buy calls before they reach a human or the bot. Trained via `_optimize_precision_threshold()` (maximize precision subject to a recall floor), never a 0.5 default.
- **`pnd_detector`** — pump-and-dump risk score (`pnd_score`, 0-100) from delivery-volume/price-anomaly features (`features/pnd_features.py`). `pnd_score > 60` sets `pnd_block=True`, which removes the ticker from every buy list network-wide (`daily_inference.py` runs P&D as a pre-filter before signal/meta scoring even happens) — a high P&D score overrides everything else.

**Q: SHAP — "Why This Signal" always shows "no SHAP data"; what needs to change?**

Confirmed by code search: no module anywhere in `systems/ml_signal_engine/` computes SHAP values at inference time (`import shap` / `shap.TreeExplainer` appear only in `systems/ml_signal_engine/models/training/feature_selection.py`, a training-time feature-selection tool, not inference explainability). `shap_top5_json` exists in the `ml_signals` schema and API contract, but nothing ever populates it — this isn't a "runs weekly" gap like multibagger, it's **never been built**. To make this panel real: add a SHAP-value computation step to `daily_inference.py`'s `signal_5d` scoring loop (e.g. `shap.TreeExplainer(signal_model._lgbm_classifier)`, top-5 |value| features per ticker, serialized to `shap_top5_json`) — logged as a backlog item, see `FutureDevelopment.md`.

**Q: 30-Day Regime History comes back blank — why?**

It isn't actually empty — `GET /api/v1/macro/regime/history?days=30` returns real rows (verified live: 2026-07-02, 2026-07-03, both `sideways`, `hmm_regime_prob` ≈0.9998). It *looks* blank because there are currently only **2 days** of `hmm_market` history in the table — a 2-point line drawn across a 30-day-wide canvas reads as visually empty. This will fill in naturally as more days accumulate; no code fix needed, just time.

### Multibagger Watchlist — tiers, archetypes, survival curves

**Q: What does `mb_tier: "10x"` mean, and what other values exist?**

`mb_tier` is a deterministic bucketing of `mb_probability` (`systems/ml_signal_engine/models/multibagger/multibagger_model.py:96-101`) — it is **not** a separately-trained 5-class model, only one binary classifier ("did this stock multibag within the 3-year label window") is actually trained; tier is a fixed post-hoc mapping of that one calibrated probability:

| `mb_probability` | `mb_tier` |
|---|---|
| ≥ 0.80 | `10x` |
| ≥ 0.60 | `5x` |
| ≥ 0.45 | `3x` |
| ≥ 0.30 | `2x` |
| < 0.30 | `none` |

**Q: What are the archetypes?**

Four labels, also rule-based (not model output) — `_classify_archetype()`, `multibagger_model.py:120-143`, computed from the 33 multibagger features:
- **`post_crash_recovery`** — recovery-strength feature ≥ 80 (a stock rebounding hard off a prior crash/drawdown).
- **`long_base_breakout`** — long consolidation base (≥100 days) with a tight range (≤10) — classic base-and-breakout technical pattern.
- **`quiet_accumulator`** — high "quiet accumulation" score (≥60) with a moderately tight base (≤12) — steady accumulation without a dramatic base or crash setup.
- **`sector_rotation_leader`** — the default/fallback archetype when none of the above thresholds are met.

**Q: Survival at 6M/12M/18M/24M/36M all showing 100% — what does that mean, is it a bug? [Calibration check completed 2026-07-04]**

`mb_survival_Xm` is standard survival-analysis S(t): probability the stock **has not yet** hit the 2x multibag threshold by month X, from a Random Survival Forest fit on (duration_months, event) pairs (`_survival_at`, `multibagger_model.py:459-521`).

**Root cause found — this is a labeling defect, not a model-fit fluke.** `load_multibagger_training_data_from_db()`'s `_duration_months()` (`multibagger_model.py:672-679`) computes the length of the **fixed 3-year observation window** (label-window-start to most-recent date), not the actual time the 2x crossing happened for positive tickers. Verified against real OHLCV via a read-only query (2026-07-04): across 1,138 eligible tickers, `duration_months` ranges 36.5–41.3 with a median of 37.55 — i.e. **every single row, positive or negative, gets essentially the same ~37-month duration**, regardless of whether/when it actually multibagged within that window. Positive rate is a real 25.9% (295/1,138), so the binary label itself is fine — the survival-analysis *time* dimension is what's broken: the RSF is being trained on data that says "outcome happened (or didn't) by ~37 months" with no information at all about *when* within the window it happened for positives.

Consequence: the fitted RSF's survival step function has virtually no event times before ~36 months in its training data, so it has nothing to base a below-36-month probability estimate on — evaluating S(6)/S(12)/S(18)/S(24) necessarily returns ≈1.0 for every ticker by construction of the training data, not because of anything specific to BHEL/LAURUSLABS or a degenerate fit. This is systemic across the whole model, not a two-ticker anomaly.

**Real fix needed** (not a config tweak): `_duration_months()` needs to compute the actual trading date the ticker's rolling price first crossed `min_return_multiplier`× the window-start price, for positive tickers — a proper time-to-event value — instead of the fixed window length. Tracked in `FutureDevelopment.md` as a labeling-logic fix, upgraded from "needs investigation" to "root cause identified, fix scoped."

**Q: Backtest — how do I trigger one, and what does "Integrity FAILED" mean?**

Backtests are triggered manually via CLI, not from the UI — `python3 backtest/run_phase1_backtest.py` / `run_phase2_backtest.py` / `run_phase3_backtest.py`, each its own `argparse` script (`--folds`, `--trials`, `--max-real-tickers`/`--max-tickers`, `--quick` for phase1/2). Each phase targets a different model set (phase1: Signal5D + MetaLabeler + P&D + Exit; phase2: Signal63D + multibagger watchlist filter; phase3: full system). There's no single script that runs all of 5d/21d/63d/multibagger in one pass today — each phase script picks its own model(s) internally.

"Integrity FAILED" means one of `backtest/integrity_checker.py`'s **7 critical checks** failed — these gate whether the backtest's numbers can be trusted at all, and a failure here means the CAGR/Sharpe/win-rate numbers shown should be treated as unreliable regardless of how good they look:
1. `check_01_walk_forward` — train dates must strictly precede test dates in every fold (no leakage from a random split).
2. `check_02_pit` — point-in-time feature correctness (no future information in features).
3. `check_03_corp_actions` — corporate actions (splits/bonuses) correctly adjusted.
4. `check_04_survivorship` — universe isn't survivorship-biased (delisted stocks included).
5. `check_05_costs` — realistic Indian transaction costs applied.
6. `check_06_liquidity` — trades sized against realistic liquidity, not fantasy fills.
7. `check_07_no_hpo_on_test` — hyperparameter tuning never touched the test fold.

Any critical-check failure raises before the backtest completes, so a report file only ever exists with `integrity_passed: true` **or** an honestly-flagged `false` with the specific failing check(s) listed — never a silently-passing broken backtest.

### Paper Trading — target/stop and `can_buy()` rejections

**Q: What does "Rejected — can_buy() gates rejected this trade (cash/sector/position cap)" mean?**

`PortfolioSimulator.can_buy()` (`backtest/portfolio.py:145-163`) checks three gates before any buy, in order: (1) not already held, (2) sizing produces a non-zero share quantity and its cost doesn't exceed available cash, (3) buying wouldn't push that sector's exposure above `MAX_SECTOR_PCT = 40%` of total equity (`config/settings.py`). A single position is also capped at `MAX_POSITION_PCT = 10%` of equity by `position_size()`. So this message specifically means: the stock was a genuine buy-signal candidate, but executing it would have breached one of these three caps (most commonly the 40% sector cap, if several open positions already sit in the same sector) — not a data or model problem.

**Q: Why are Price Target and Stop Loss fixed at +15%/-7.5%?**

These come from `RuleBasedExitPolicy` (`systems/ml_signal_engine/models/exit/rule_based_exit_policy.py:39-41`), a deliberately mechanical stand-in for the real `ExitSignalModel` — its own docstring states `ExitSignalModel` cannot train until ≥200 real closed paper-trading positions exist (there are only 2 trading days of history as of 2026-07-04, nowhere near enough), so this flat 2:1 target:stop rule (matching `TripleBarrierLabeler`'s `profit_multiplier=2.0`/`stop_multiplier=1.0` convention, but expressed as flat percentages rather than per-position ATR) is bootstrapping real closed-trade variety for that model's eventual training set. Once ~200 closed positions exist, this is meant to be swapped for the real ATR-scaled `ExitSignalModel`. Tracked as a to-do, not forgotten — see `FutureDevelopment.md`.

