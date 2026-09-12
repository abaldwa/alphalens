# ML/DL Models — Research Brief & Handoff

**Purpose of this document**: this is a handoff from a Claude Code *remote* session
(cloud sandbox, code-only checkout, no access to the real DuckDB/feature store) to
whoever continues this work locally (Claude on VS Code, or a person) with actual
access to the live data: **Top 800 ADTV universe, 300+ computed features per
ticker, 17 years of history, an expanded/dynamic sector taxonomy**.

Everything in Sections 1–2 below is based on **reading the code in this repo**,
not on running it or seeing real data — treat file/line references as verifiable
facts, and treat the forward-looking recommendations (Sections 3–6) as research
guidance to validate against your actual data, not conclusions already proven.
The goal of this document is that **no one has to redo this code review or
paper search from scratch** — pick up at Section 6 (diagnostics) with real data.

All scale-specific discussion below (feature counts, universe size, sector
handling) is written for the **800+ ticker / 300+ feature / expanding sector**
context — not the smaller 200-stock / ~20-feature setup the current code
happens to be hardcoded for (see Section 2.3).

---

## 1. Current code inventory — what's actually implemented

Verified by reading source directly (not README claims):

| Component | File | Status |
|---|---|---|
| Random Forest cycle classifier (market/sector/stock, 3-way bull/neutral/bear) | `alphalens/core/cycle/classifier.py` | **Implemented.** Walk-forward `TimeSeriesSplit` (5-fold) CV, `class_weight="balanced"`, feature importances logged. |
| Gaussian HMM stock regime detector (3-state) | `alphalens/core/patterns/hmm.py` | **Implemented.** Fits per-stock on return + log volume ratio; auto-labels states by mean return. |
| Genetic strategy discovery (DEAP-style, hand-rolled) | `alphalens/core/strategy/discovery.py` | **Implemented.** Population 30, 20 generations, fitness = Sharpe + win-rate on **10 symbols** per candidate evaluation. |
| "ML ensemble" signal confidence (LightGBM/XGBoost/LSTM/RF) | `alphalens/core/signals/generator.py`, `alphalens/ml/training/signal_trainer.py`, `alphalens/ml/inference/predictor.py` | **NOT implemented.** `ml/training/` and `ml/inference/` contain only empty `__init__.py` files. `_load_model()` in `generator.py` looks for a joblib file that is never produced by anything in the repo. `_ml_confidence()` therefore always falls back to `_rule_based_confidence()` — a hand-coded indicator-alignment heuristic. |
| LSTM ("positional model") | referenced in `requirements.txt` comment only | **NOT implemented.** No `torch`/`nn.Module`/`LSTM` usage found anywhere in the codebase outside `requirements.txt` and doc comments. |
| Cycle model training entry point | `main.py` | **Not wired.** `main.py` logs `"run python main.py --train-cycles"` but `--train-cycles` is **not registered** in the `argparse` parser (only `--init`, `--backfill`, `--dashboard`, `--scheduler` exist). `scheduler/jobs.py`'s EOD job calls `CycleClassifier.classify_all_and_store()` (inference) but never `train_all()` — there is currently no automated or CLI-triggered training path for the cycle classifier. |
| Trained model artifacts on disk | `alphalens/models/` (configured dir) | **None exist.** No `.pkl` files found anywhere in the repo/session. |
| Actual data (DuckDB/SQLite/feature store) | `data/alphalens.duckdb`, `data/alphalens.db` (configured paths) | **Do not exist in this repo/session.** Entire checkout is 1.7MB, code only, no `data/` directory, no `.gitignore` hiding one. This is expected — a personal trading system's runtime DB is generated locally via `--backfill`, not committed to git — but it means this session could not inspect real data and everything below is analysis-of-code, not analysis-of-data. |

## 2. Design assumptions in the current code that don't match the new scale

These will need to change, independent of any research/modeling decision:

1. **Universe size**: `get_all_symbols()` / `nifty200_stocks` table assumes ~200
   tickers. Target universe is now **Top 800 by ADTV**.
2. **Sector taxonomy is hardcoded and small**: `cycle/classifier.py`'s
   `_build_sector_dataset()` hardcodes exactly **6** sector index columns
   (`sector_it_close`, `sector_bank_close`, `sector_auto_close`,
   `sector_fmcg_close`, `sector_pharma_close`, `sector_metal_close`) — not the
   12 the README claims, and not dynamic. An expanding/changing sector count
   needs sector as a **joined categorical field**, not a fixed Python dict.
3. **Stock-level model trains on a 30-stock subsample**: `_build_stock_dataset()`
   explicitly samples `min(30, len(all_symbols))` "for speed." At 800 tickers ×
   300 features this throws away the majority of the cross-section that would
   otherwise be available for pooled training (see Section 5.1).
4. **Feature set is small (~15–30 features per timeframe)**: `ml/features/pipeline.py`'s
   `SIGNAL_FEATURES` dict lists roughly 15 (intraday) to 30 (long_term) named
   features. This is an order of magnitude below the 300+ features now
   available and will need a genuinely different feature-selection/reduction
   strategy, not just appending more names to the list (Section 5.2).

## 3. Realistic performance expectations (general, not data-specific)

Stated plainly, and consistent with what was already communicated in this
conversation — **do not expect any of the below to reliably predict price
direction with high confidence**:

- Tabular models (RF/LightGBM/XGBoost) on technical+fundamental features are
  best understood as **ranking/filtering tools**, not direction predictors.
  Retail-scale daily-frequency directional accuracy in the roughly 52–58%
  range is a commonly cited rough band in ML-for-trading discussion — this is
  a general impression from the field, **not a specific documented statistic**,
  and should not be treated as a target or guarantee.
- Deep learning (LSTM) is harder to justify at small scale (~200 tickers) but
  the argument weakens at 800 tickers × 300 features × 17 years, **if** the
  data is pooled across the cross-section rather than trained per-stock
  (Section 5.1). Still expect it to be the highest-effort, least certain
  payoff of the model types under consideration.
- The cycle classifier (regime detection) is the most tractable ML component
  here — regime labels are structurally more persistent than next-day returns.
  Its realistic role is as a **conditioning signal**, not a standalone trade
  trigger.
- The HMM pattern detector is the weakest component — 3-state Gaussian HMMs on
  daily return+volume are prone to unstable state reassignment as new data
  arrives (general property of HMM re-fitting). Treat its output as lagging,
  not leading.
- The genetic strategy discovery module carries the highest overfitting risk
  currently in the codebase: evolving parameters over 20 generations against
  Sharpe/win-rate on only 10 evaluation symbols is a small, fixed sample
  relative to an 800-ticker universe. This needs correction before trusting
  any "discovered" strategy (see Bailey/López de Prado papers, Section 7.5).
- Expect **alpha decay** even if a genuine edge is found — signal strength
  weakening over time as conditions change is a widely discussed phenomenon
  in quant finance; no specific decay rate can be quantified without live
  monitoring.
- The system's realistic honest goal: a better-organized decision aid that
  screens out weak setups and prioritizes stronger ones — not an oracle for
  price direction.

## 4. Reframing: more tractable sub-problems than "predict direction"

Rather than one model predicting up/down, the literature suggests these are
more tractable targets — and map more naturally onto a 300-feature, 800-ticker
setup:

1. **Meta-labeling** — use ML to decide *whether to take* / *how to size* a
   trade already flagged by a rule-based strategy (your existing
   `strategy/library.py`), rather than using ML to generate the primary
   signal. Fundamentally easier problem than direction prediction because the
   search space is pre-narrowed.
2. **Cross-sectional ranking** — predict relative outperformance across the
   800-ticker universe rather than absolute direction; cancels out
   market-wide moves, aligns with classic factor-investing methodology.
3. **Volatility/risk forecasting** — volatility is far more forecastable than
   returns (volatility clustering is one of the more robust findings in
   empirical finance); could improve ATR-based stop/target logic
   (`signals/exit.py`) directly.
4. **Backtest-overfitting correction** — statistically discount fitness scores
   for how many strategy variants were tried (directly relevant to the
   genetic discovery module's small eval sample).
5. **Calibrated uncertainty** — produce honest confidence intervals (wider at
   noisier horizons) instead of a single point "confidence" score, which is
   what both the current heuristic and the planned ML confidence score do
   today.

## 5. Scale-specific architecture recommendations (800 tickers × 300 features)

### 5.1 Pool the panel — don't train per-stock or on a subsample

The current code's instinct (shared model, but on a 30-stock subsample) should
be extended, not abandoned: train on the **full pooled panel** — all 800
tickers × all time periods stacked into one training set — rather than
per-stock models or a small subsample. This is the approach used in the most
directly relevant academic reference for this exact setup (see Gu, Kelly, Xiu,
Section 7.1). Pooling substantially increases effective sample size and is a
better fit for 300 features than subsampling 30 stocks ever was for ~20.

### 5.2 300 features requires active dimensionality management, not just bigger models

More features is **not automatically better** — many technical indicator
families are highly correlated by construction (e.g. multiple RSI/MA-derived
variants). At this scale:
- Run a regularized baseline (Lasso / Elastic Net) to see how much signal
  survives shrinkage before committing to tree ensembles.
- Don't rely on default (Gini-based) Random Forest importance with 300
  correlated features — it's known to be biased toward correlated/high-
  cardinality features (Strobl et al., Section 7.2). Use permutation
  importance or SHAP instead.
- Group features by family (technical / fundamental / macro / sector-relative)
  and test each group's marginal contribution rather than throwing all 300 in
  blindly — especially important for the rarer multi-bagger label, which has
  few positive examples to constrain 300 dimensions.

### 5.3 Sector as a categorical feature, not a fixed dict

With sector count expanding/changing, replace the hardcoded 6-sector dict in
`cycle/classifier.py` with sector as a joined **categorical feature** inside a
pooled model (LightGBM/CatBoost handle categoricals natively). This avoids the
failure mode of newly added or thinly populated sectors having too little data
to support their own dedicated sub-model.

### 5.4 Recommended model architecture: multiple models, split by goal *and* by horizon

Per your explicit confirmation that separate models per goal are acceptable
(no need for one universal model):

- **Track A — Multi-bagger discovery**: one pooled panel model across all 800
  tickers, sector as a categorical feature, fundamentals/quality-growth
  features weighted heavily (Piotroski F-Score, profitability, Fama-French
  5-factor — Section 7.4), rare-event-aware labeling (long-horizon extreme-
  return label, not a buy/no-buy label) and evaluation (imbalanced-learning
  methods), explicit survivorship-bias correction in universe construction
  (must include historically delisted/failed names, not just current Top 800).
- **Track B — Short/medium trades (5d / 21d / 63d)**: recommend **separate
  models per horizon**, not one model with a horizon parameter, because the
  5-day and 63-day horizons plausibly reflect **opposite effects**
  (short-term reversal vs. medium-term momentum — Section 7.3). Each trained
  as a pooled panel model across all 800 tickers (per 5.1), with meta-labeling
  layered on top of existing rule-based strategies, and calibrated confidence
  (conformal prediction) per horizon rather than a shared confidence scale.

## 6. Diagnostics to run locally against the real data — do this before building anything

This is the concrete, actionable part of the handoff — things that require
real data access and were the reason this session could not go further:

1. **Feature correlation/redundancy scan** across all 300+ features — identify
   near-duplicate feature families before feeding them into any model.
2. **Class balance check** for whatever multi-bagger label gets defined (e.g.
   "returned >300% over next 3 years") — expect severe imbalance; quantify it.
3. **Survivorship bias check** on historical universe construction — does the
   17-year history include stocks that were later delisted, merged, or
   dropped from Top 800 ADTV, or only current constituents?
4. **Point-in-time correctness of fundamental fields** — verify fields like
   `pe_ratio`, `roe`, `eps_growth_yoy` reflect what was *knowable at the time*,
   not restated/adjusted figures (a common source of look-ahead bias in
   backtests using fundamental data).
5. **Sector taxonomy coverage** — confirm every ticker maps to a sector, check
   for sectors with very few constituents (small-sample risk if not handled
   as a categorical feature per 5.3).
6. **Horizon-specific label correlation check** — before assuming 5d/21d/63d
   need separate models, empirically check whether forward 5-day and forward
   63-day returns are positively or negatively correlated with recent
   short-term price strength in your actual data (tests the reversal-vs-
   momentum hypothesis from Section 7.3 directly).
7. **Genetic discovery eval-sample audit** — check how many *distinct*
   symbols/time-windows the current 10-symbol fitness evaluation actually
   covers relative to the 800-ticker universe; this quantifies the overfitting
   exposure flagged in Section 3.

## 7. Full paper/reference list (consolidated, with confidence levels)

Confidence levels reflect how certain I am the citation (title/authors/venue)
is correctly remembered, not how certain the finding itself is scientifically
established. **Verify exact titles/years/venues before citing formally** —
these are research leads, not confirmed bibliographic records.

### 7.1 Large cross-section / many-feature return prediction (most directly relevant to your scale)
- Gu, S., Kelly, B. & Xiu, D. (2020), "Empirical Asset Pricing via Machine
  Learning," *Review of Financial Studies*. **High confidence.** Benchmarks
  linear/tree/neural methods on a large cross-section with many firm
  characteristics — closest academic match to an 800-ticker × 300-feature
  setup. General recollection (not re-verified in detail): gains from
  ML over linear models are real but incremental, not transformative.

### 7.2 Feature dimensionality / regularization / importance
- Tibshirani, R. (1996), "Regression Shrinkage and Selection via the Lasso,"
  *Journal of the Royal Statistical Society, Series B*. **High confidence.**
- Zou, H. & Hastie, T. (2005), "Regularization and Variable Selection via the
  Elastic Net," *Journal of the Royal Statistical Society, Series B*.
  **High confidence.**
- Strobl, C., Boulesteix, A.-L., Zeileis, A. & Hothorn, T. (2007), "Bias in
  Random Forest Variable Importance Measures: Illustrations, Sources and a
  Solution," *BMC Bioinformatics*. **Moderate confidence** on exact venue/year
  — underlying finding (RF importance bias with correlated features) is well
  established.
- Lundberg, S.M. & Lee, S.-I. (2017), "A Unified Approach to Interpreting
  Model Predictions," *NeurIPS* (SHAP). **High confidence.**
- He, H. & Garcia, E.A. (2009), "Learning from Imbalanced Data," *IEEE
  Transactions on Knowledge and Data Engineering*. **Moderate confidence** —
  standard survey reference for the multi-bagger rare-event/class-imbalance
  problem.

### 7.3 Short-term reversal vs. medium-term momentum (Track B horizon design)
- Jegadeesh, N. (1990), "Evidence of Predictable Behavior of Security
  Returns," *Journal of Finance*. **High confidence.** Short-term reversal.
- Lehmann, B.N. (1990), "Fads, Martingales, and Market Efficiency,"
  *Quarterly Journal of Economics*. **Moderate confidence** on exact
  title/venue — another foundational short-term reversal reference.
- Jegadeesh, N. & Titman, S. (1993), "Returns to Buying Winners and Selling
  Losers: Implications for Stock Market Efficiency," *Journal of Finance*.
  **High confidence.** 3–12 month momentum — brackets the 63-day horizon.
- Moskowitz, T.J., Ooi, Y.H. & Pedersen, L.H. (2012), "Time Series Momentum,"
  *Journal of Financial Economics*. **Moderate confidence** on exact
  citation details — per-asset time-series momentum, relevant to medium-term
  horizon design.

### 7.4 Multi-bagger / long-horizon fundamentals (Track A)
- Piotroski, J.D. (2000), "Value Investing: The Use of Historical Financial
  Statement Information to Separate Winners from Losers," *Journal of
  Accounting Research*. **High confidence.** F-Score fundamentals screen.
- Novy-Marx, R. (2013), "The Other Side of Value: The Gross Profitability
  Premium," *Journal of Financial Economics*. **Moderate confidence** on
  exact title/venue.
- Fama, E.F. & French, K.R. (1993), "Common Risk Factors in the Returns on
  Stocks and Bonds," *Journal of Financial Economics*. **High confidence.**
- Fama, E.F. & French, K.R. (2015), "A Five-Factor Asset Pricing Model,"
  *Journal of Financial Economics*. **High confidence** on general title;
  adds profitability/investment factors relevant to "quality compounders."
- Carhart, M.M. (1997), "On Persistence in Mutual Fund Performance," *Journal
  of Finance*. **High confidence.** Four-factor model (adds momentum).
- Asness, C., Frazzini, A. & Pedersen, L.H., "Quality Minus Junk" (AQR
  working paper). **Low-moderate confidence** on exact title/year/venue —
  worth searching for rather than citing as-is.
- Peter Lynch, *One Up On Wall Street* — **not academic/peer-reviewed**,
  practitioner book, source of the "ten-bagger" term and qualitative
  screening heuristics. Flagged explicitly as non-academic.

### 7.5 Backtest overfitting / validation methodology (esp. for genetic discovery module)
- López de Prado, M., *Advances in Financial Machine Learning* (Wiley, 2018).
  **High confidence book exists**; specific chapters on meta-labeling,
  triple-barrier labeling, and combinatorially purged cross-validation are
  directly relevant. Not fully read in this session — treat as a pointer to
  the right chapters.
- Bailey, D.H. & López de Prado, M. (2014), "The Deflated Sharpe Ratio:
  Correcting for Selection Bias, Backtest Overfitting, and Non-Normality,"
  *Journal of Portfolio Management*. **Moderate confidence** on exact
  title/venue/year.
- Bailey, D.H., Borwein, J., López de Prado, M. & Zhu, Q.J. (2014), "The
  Probability of Backtest Overfitting," *Journal of Computational Finance*.
  **Moderate confidence** on exact title/venue/year — directly relevant to
  the genetic discovery module's small (10-symbol) evaluation sample.

### 7.6 Volatility forecasting
- Engle, R.F. (1982), "Autoregressive Conditional Heteroscedasticity with
  Estimates of the Variance of United Kingdom Inflation," *Econometrica*.
  **High confidence.** Original ARCH paper.
- Bollerslev, T. (1986), "Generalized Autoregressive Conditional
  Heteroskedasticity," *Journal of Econometrics*. **High confidence.** GARCH.

### 7.7 Calibrated uncertainty
- Vovk, V., Gammerman, A. & Shafer, G., *Algorithmic Learning in a Random
  World* (Springer, 2005). **Moderate confidence** on exact citation —
  foundational conformal prediction reference.

### 7.8 Theoretical background (why direction prediction is hard)
- Fama, E.F. (1970), "Efficient Capital Markets: A Review of Theory and
  Empirical Work," *Journal of Finance*. **High confidence.** Standard
  argument for why any edge is more likely in narrower places (relative
  ranking, risk timing, execution) than raw next-day direction.

---

## 8. Open questions for whoever continues this locally

- What is the actual definition of "multi-bagger" for labeling purposes
  (e.g., return threshold, holding period)? This drives the entire Track A
  labeling scheme and hasn't been decided in this conversation.
- Does the 17-year history include delisted/merged/failed companies, or only
  current Top 800 ADTV constituents (survivorship bias risk, Section 6.3)?
- Are the 300 features already deduplicated/pruned, or is that still pending
  (Section 5.2)?
- Is there existing infra for point-in-time fundamental data (as opposed to
  latest-known/restated values)?
- Confirm whether `main.py --train-cycles` should be wired up as-is, or
  whether the cycle classifier itself should be redesigned for the 800-ticker
  pooled/categorical-sector approach before investing in a CLI entry point
  for it (Section 5.1, 5.3).
