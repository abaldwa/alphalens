# AlphaLens.ML Screen Column Glossary (ML32)

Documentation-only deliverable per FeatureBacklog.md ML32 — a plain-English
glossary for the ML screens' columns, produced directly from the existing
schema (`datastore/api/schemas.py`) rather than adding any new code. No
model logic changed.

## Signal Deep Dive / Full Universe (`ml/signal.html`, `ml/universe.html`)

Backed by `MLSignalRow`/`SignalUniverseRow` (`datastore/api/schemas.py`),
sourced from the `ml_signals` DuckDB table (Store 4).

| Column | Meaning |
|---|---|
| Ticker | NSE symbol. Links to `technical/chart.html` (price chart); the small 🔎 icon opens `ml/signal.html` (this ticker's full detail view) — both open in a new tab (A69 convention). |
| Buy Prob | `buy_prob` — signal_5d's own probability that its call is "buy" (0-1). The only model AlphaLens actually trades paper positions off of. |
| Q50 Return | `q50_return` — signal_5d's median (50th percentile) forecast forward return over its holding horizon, from its quantile-regression head. `Q10`/`Q90` are the same model's 10th/90th percentile bounds (shown as an "Interval" on the detail view), used as a rough confidence band, not a guarantee. |
| Meta Label Prob | `meta_prob`/`meta_label_prob` — meta_labeler's estimate of whether signal_5d's call is worth acting on at all (a secondary filter, not a return forecast). `meta_label` is its Act/Don't-Act decision at meta_labeler's tuned threshold. |
| P&D Score | `pnd_score` — pnd_detector's 0-100 pump-and-dump risk score for that ticker/day from volume/price anomaly features. `pnd_block=true` (not separately shown as a column, but enforced upstream) removes a ticker from all buy lists regardless of buy_prob. |
| Forensic | `forensic_flag_label` — forensic_ml's 5-level taxonomy (green/yellow/orange/red/black) carried forward from `ml_forensic`'s own most recent weekly scoring run (forensic doesn't score daily, so this can be several days stale — the detail view's "as of {date}" badge shows how stale). |
| MB Probability / MB Prob | `mb_probability` — the MultibaggerModel's probability estimate, carried forward from `ml_multibagger`'s own most recent (typically weekly, Sunday) run. **Not a return multiplier prediction** — `mb_tier` ("10x"/"5x"/"3x"/"2x"/"none") is a deterministic probability-band bucketing (`mb_probability >= 0.80` → "10x"), not a forecast that the stock will actually return 10x (see `js/api.js`'s `MB_TIER_BANDS`/`mbTierLabel`). |
| Basis (ML23, universe table only) | A short 2-feature summary derived client-side from `shap_top5_json` (signal_5d's own top-5 SHAP feature attributions for that call), e.g. `sma_50_ratio (+0.12), rs_vs_nifty500_21d (+0.08)` — the two largest-magnitude contributors by absolute SHAP value. The full 5-feature breakdown with bars remains on the ticker detail view's "SHAP — Why This Signal" panel. |
| Exit Urgency | `exit_urgency` — rule_based_exit_policy's 0-100 urgency score for exiting an existing position in this ticker; `exit_type` is the specific triggering rule (thesis_broken / momentum_exhaustion / risk_management / target_achieved / opportunity_cost / pnd_exit). |
| Hmm Regime / Regime Prob | `hmm_regime`/`hmm_regime_prob` — the market-wide HMM regime classifier's current state and confidence (ticker='MARKET' in the underlying table, fused onto every ticker's row read-time). |
| Conformal Interval | `conformal_lower`/`conformal_upper` — signal_5d's conformal-prediction bounds around its point forecast (distinct from the Q10/Q90 quantile-regression bounds — two different uncertainty-quantification methods on the same model). |
| In Training Universe | `in_training_universe` — whether this ticker was part of the model's training universe at scoring time (vs. scored out-of-universe). |
| Is Backfill | `is_backfill` (A43) — whether this row's `write_signals` step ran as a catch-up/backfill invocation rather than the same-day live schedule. `None` for rows predating A43. |

## Multibagger (`ml/multibagger.html`)

Backed by `MultibaggerRow`, sourced from `ml_multibagger`.

| Column | Meaning |
|---|---|
| MB Probability | Same field as above — the model's probability estimate, not a return multiplier. |
| MB Tier | Deterministic probability-band label, see above. |
| MB Archetype | `mb_archetype` — a categorical label for the type of setup the model associates with this ticker (e.g. small-cap breakout, turnaround), not independently verified in this glossary pass. |
| Survival 6/12/18/24/36m | `survival_Nm` — the model's estimated probability the position survives (doesn't hit a defined failure condition) N months out. |

## Forensic (`forensic/*.html`, forensic badges on ML screens)

Backed by `ForensicRow`, sourced from `ml_forensic`.

| Column | Meaning |
|---|---|
| Beneish M-Score | `beneish_m` — earnings-manipulation risk score (lower/more-negative = lower risk by the published Beneish model's convention). See FeatureBacklog.md FO3 for a known permanently-NaN AQI-term gap. |
| Altman Z-Score | `altman_z` — bankruptcy-risk score (higher = safer). See FeatureBacklog.md FO1/FO9 for known NaN gaps (missing real market-cap/current-assets inputs for some tickers). |
| Piotroski F-Score | `piotroski_f` — 0-9 fundamental-strength score (higher = stronger). |
| Ohlson O-Score | `ohlson_o` — bankruptcy-probability score (logistic-regression based; higher = higher estimated bankruptcy probability). |
| Dechow F-Score | `dechow_f` — earnings-quality/manipulation-risk score. See FeatureBacklog.md FO2 for a known always-NaN production gap (no real employee-count/share-issuance/book-to-market inputs yet). |
| Sloan Accrual | `sloan_accrual` — accruals ratio; large positive values are historically associated with lower forward returns (accrual anomaly). |
| Benford MAD | `benford_mad` — mean absolute deviation of the ticker's reported-figure leading-digit distribution from Benford's Law's expected distribution; higher suggests a higher chance of manufactured/rounded figures. `benford_detail_json` (FO5) carries the full chi-square/p-value/per-digit breakdown behind this summary number. |
| Forensic Composite | `forensic_composite` — 0-100 blended score across the above; `forensic_flag` (bool) is true above `FORENSIC_BLOCK_THRESHOLD` (60); `forensic_flag_label` is the 5-level green/yellow/orange/red/black taxonomy shown as the badge across ML screens. |
| Forensic ML Prob | `forensic_ml_prob` — a separate ML classifier's own probability estimate of forensic risk, distinct from the composite of classical formula-based scores above. |
| Pattern Match | `pattern_match` — which known historical forensic-failure pattern (if any) this ticker's figures most resemble. |

## Exit Urgency (`ml/exit_urgency.html`)

Same `exit_urgency`/`exit_type`/`exit_survival_5d/21d/63d` fields as the
Signal Deep Dive table above (`rule_based_exit_policy`'s output), filtered/
sorted for currently-held positions specifically.

## Tickers missing company name and/or sector

**Not run in this pass.** Generating this list requires a live read against
`datastore/normalised/alphalens.duckdb`'s `universe` table
(`SELECT ticker, company_name, sector FROM universe WHERE company_name IS
NULL OR trim(company_name) = '' OR sector IS NULL OR trim(sector) = ''
ORDER BY ticker`), but at the time this documentation was produced
(2026-07-13) the DB file was continuously held by the live
`ingestion.scheduler.daily_pipeline` process (PID visible via `ps aux |
grep daily_pipeline`) — DuckDB does not support a concurrent read-only
connection while another process holds the file open read/write, and this
task's boundaries explicitly rule out touching/restarting a live
scheduler/systemd process to force a window open.

**To generate this list**, run the query above (e.g. via
`datastore/api/db.get_duckdb_connection(DUCKDB_PATH, read_only=True,
persist=False)` in a short-lived script, or through a running
`datastore/api` instance) during a window when the scheduler isn't
mid-step, or add a lightweight `GET /api/v1/universe/data-quality/missing-name-sector`
endpoint if this becomes a recurring need — not built here since it wasn't
part of ML32's explicit scope (a one-off list, not a monitored feature).
