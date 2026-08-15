# Data & Model Audit — 2026-07-29

Full data-availability, ML-training-status, and scheduler-coverage audit across
the AlphaLens product suite (Technical, Fundamental/Valuation, Momentum, ML
signals, Multibagger, Forensic). Produced via three parallel read-only
investigations (data inventory, ML model/training inventory, scheduler job
inventory) plus synthesis.

## 1. Data status by strategy family

| Strategy | Data health | Issue |
|---|---|---|
| **Technical Analysis screener** | **Healthy** (was misreported as degraded) | `ohlcv_adjusted` fully current. `ta_signals` row count (8.7k-10.5k vs ~11.5k universe) initially looked like a coverage gap, but investigation confirmed `ta_signals` is an **alerts table** by design — it only stores tickers scoring a full 1.0 match on one of the 42 screener templates ("partial matches are NOT stored as long-lived alerts" per its own docstring). Row count varies naturally with market conditions, not a bug. Of 274 tickers in one day's `ohlcv_adjusted` but absent from `ta_signals`: 237 were evaluated and legitimately scored <1.0 on all templates, 37 were missing from that day's feature Parquet (a `compute_features` upstream question, not a `check_ta_alerts` issue). See item #8 resolution below. |
| **Fundamental/Valuation screens** | Degraded | Underlying `fundamentals`/`shareholding` data is current (Q1 FY27, as expected for quarterly cadence); but `valuation_signals` output is **10 days stale** (last computed 07-19). **Backfill for the Fundamental strategy family (2026-07-20 → 07-28) has not completed** — separate from and in addition to the daily_pipeline backfill. |
| **Momentum strategy** | Degraded | Missing `momentum_rankings` for 2026-07-28 specifically (not the known 06-26 holiday — a real gap; confirmed 2026-07-30: `momentum_rankings` covers only 10 of the 11 real trading days from 07-15 to 07-29). All **5 wired-in momentum strategies** share the same config (top 15 stocks / 6-month trailing lookback / monthly rebalance / grace = 2 cycles, per `features/momentum_live.py`'s 2026-07-14 production decision) and differ only by market-cap rank band (`features/momentum_universe.py::RANK_BANDS`): `band1_top15_6m_m_g2` (Rank 1-50), `band2_top15_6m_m_g2` (Rank 51-100), `band3_top15_6m_m_g2` (Rank 100-150 — the original single-band default, `DEFAULT_STRATEGY_ID`), `band4_top15_6m_m_g2` (Rank 150-200), `band5_top15_6m_m_g2` (Rank 100-200). Each is fully independent — its own live ranking, rebalance schedule/suggestions, and recorded trades/holdings. `momentum_rebalance_suggestions` is empty (0 rows) in production — **not dead code** (confirmed 2026-07-30: it's written by `step_compute_momentum` in `ingestion/scheduler/daily_pipeline.py:1526` and read/actioned by `datastore/api/routers/momentum.py`, with test coverage in `tests/unit/test_momentum_router.py`). It's empty because it only populates on each strategy's monthly "rebalance day" (`momentum_live.is_rebalance_day`), and all 5 strategies currently have `next_rebalance_date = NULL` in `momentum_rebalance_state` (last updated 2026-07-29 18:43) — meaning `is_rebalance_day` has never evaluated true since this feature launched (ML38, 2026-07-14/15), so no suggestion has ever been generated yet. `momentum_trades` is also empty (0 rows, expected — no manual trades recorded yet). Worth a follow-up: confirm whether `next_rebalance_date` being NULL for all 5 strategies is itself a bug in `features/momentum_live.py::next_rebalance_date` (e.g. a trading-day lookup returning nothing) or genuinely means no scheduled rebalance date has been computed yet. |
| **Core ML signals** (`run_models`/`write_signals`) | **Healthy** | Daily, full-universe (11.5k+ rows/day), current through today — this is the one thing working correctly |
| **Multibagger scoring** | Degraded/Blocked | Only 4 run dates ever; 10 days stale; underlying model hasn't actually retrained since 07-09 (see §2) |
| **Forensic scoring** | Degraded | Same stale cadence as multibagger; one run (07-11) wrote only 50/2317 rows — a partial/crashed run |

**Cross-cutting root cause for most of this**: multibagger/forensic/valuation all
share a ~10-day staleness window starting right around 2026-07-20 — this lines
up exactly with the 7-day pipeline gap (2026-07-20 to 2026-07-28) the scheduler
was backfilling as of this audit. Multibagger/forensic run on separate weekly
cron jobs, not inside `daily_pipeline`'s STEPS, so backfilling daily_pipeline
does not automatically catch them up — nor does it cover the Fundamental
strategy's own backfill, which is tracked separately and has not completed.

## 2. ML model training status

| Model | Last real training | Status |
|---|---|---|
| HMM regime detector | 2026-07-13 | Trained on schedule, **but** the per-ticker `hmm_regime_*` feature columns it's supposed to produce couldn't be found populated anywhere in `feature_panel_staging` — only a separate index-level `market_regimes` table exists. Needs a dedicated follow-up; don't trust this model's live wiring yet. |
| PnD detector, Signal-5D | 2026-07-13 | On track (28-day retrain interval, next due ~08-10). Flag: PnD's severe 0.39%→50% class resampling hasn't been checked for train/test leakage. |
| Signal-63D | Skipping as "not overdue" | Consistent with 28-day cadence, not a bug |
| **Multibagger scorer** | **2026-07-09**, confirmed healthy | **[RESOLVED, 2026-07-30 — original "bug" was a false positive]**: the scheduler itself was never broken. Investigation found the real Wed 23:00 cron run (2026-07-22, `job_run_log` id 126) correctly read `registry.json` and logged "no models overdue — skipping" — i.e. 2026-07-09's model genuinely isn't overdue yet under the retrain-interval policy, same as Signal-63D above. The repeated `"registry.json not found"` rows that made this look broken every week were **test-run pollution**: `tests/unit/test_model_training_nightly.py` and `test_model_training_overdue_union.py` isolated `MODELS_DIR`/`PIPELINE_LOG_DB_PATH` in their fixtures but never isolated `config.settings.DUCKDB_PATH`, so every local/CI test run of these files silently inserted a real "skipped" row into **production** `alphalens.duckdb`'s `job_run_log` — a violation of the project's no-synthetic-writes-to-production-DB policy. Confirmed via direct query: 20 near-instant, ~0-duration rows clustered at pytest run timestamps, plus exactly one genuine 23:00-timestamped cron row. **Fixed**: both test fixtures now patch `DUCKDB_PATH` to an isolated temp file; new regression test (`test_job_run_log_write_is_isolated_from_production_duckdb`) asserts the production table is untouched by these tests. 10/10 tests pass. Committed as `a40a44c` on this branch. No scheduler/production code was changed — nothing there was actually broken. |
| Forensic ML | Unconfirmed | No registry entry found for a learned forensic model — unclear if it retrains at all vs. running a static/classical scorer. Needs a targeted check. |
| Wavelet/entropy/pattern-score features (33 columns feeding several models) | **[RESOLVED, 2026-07-29 — wiring already fixed]** | Was: only 0.09% of historical rows populated, all after 2026-04-08, causing `sanity_check` all-NaN failures during backfill. Investigation found the batch-staging rework already landed same-day on this branch (commits `9c46fe8`, `9914406`) — `scripts/feature_backfill.py` → `panel_staging.py` → `matrix_builder.py::compute_full_range_chunk_panels` now calls the same `compute_advanced_technical_features`/`compute_pattern_scores` functions as the live pipeline, and `test_panel_staging.py::TestBatchStagingMatchesPerDateSequential` proves byte-identical output vs. the live path for historical dates (72 tests passed). **The wiring is correct — but the existing `feature_panel_staging.duckdb` data still reflects pre-fix backfill runs.** A real multi-year historical backfill still needs to be launched to repopulate these columns; not yet scheduled/run (see action item below — this is a costly, long-running job, deliberately not launched automatically). |
| `receivable_days_change`, `inventory_days_change`, `dilution_3y` | **[ROOT CAUSE FOUND, 2026-07-30 — real, live, unresolved]** | Confirmed via direct check on 2026-07-29 (today's live run, not just backfill): these are structurally NaN for **every ticker, every day**, including blue-chip names (RELIANCE/TCS/INFY). Traced to source: `ingestion/scrapers/screener.py` (the primary Trendlyne-based scraper) hardcodes `inventory_days: None, receivable_days: None` at scrape time (lines 745-746) — its free-tier source can't reliably parse these. A script that looks purpose-built to backfill these derived ratios from other raw balance-sheet data, `scripts/recompute_fundamental_ratios.py`, exists but **is never registered in `pipeline_scheduler.py`** — nobody runs it. Net effect: `sanity_check` has failed on this exact 3-column set on **every single day since it was added**, live and backfilled alike — not a one-off or backfill-specific gap. This is more severe than originally logged (see reprioritized action list). |

## 3. Scheduler coverage — is everything actually scheduled?

Mostly yes, but several jobs are silently broken rather than missing:

- **`daily_pipeline`** (18:00 Mon-Fri) — scheduled correctly; the 2026-07-29
  18:26-18:44 run itself **failed** with 0 stocks processed — early-stage
  failure (bhavcopy/adjust_prices), separate from the 07-20→07-28 backfill.
- **`morning_catchup`** (07:30 Mon-Fri) — scheduled, but stale/failing ("gap
  days still incomplete") — consistent with the same backlog.
- **`weekend_feature_backfill`, `weekend_fundamentals`,
  `promoter_pledge_backfill`, `nse_xbrl_fundamentals`** — **[INVESTIGATED,
  2026-07-30]**: two distinct issues, not four independent bugs.
  `promoter_pledge_backfill` and `nse_xbrl_fundamentals` were failing on a
  real code bug — `scripts/backfill_promoter_pledge_nse.py` and
  `scripts/backfill_fundamentals_nse_xbrl.py` were run via subprocess
  without `PYTHONPATH`, causing `ModuleNotFoundError: No module named
  'config'` (confirmed via `journalctl`, recurred 07-11/07-18) — **already
  fixed** in commit `48722e0` (2026-07-21), which added an explicit
  `sys.path.insert()` to both scripts; verified present and working
  (ran with `--help` from a non-repo-root cwd, exit 0). `weekend_feature_backfill`/
  `weekend_fundamentals` failed once (07-18) on a transient DuckDB lock
  conflict (multiple weekend jobs bunching up after a scheduler restart),
  then **zero `job_run_log` rows for any of the 4 jobs since 07-18** — not a
  code bug at all, but the scheduler service being OOM-killed/crash-looping
  through the 07-19→07-26 window (`systemd-oomd killed 2 process(es)`,
  matches `project_systemd_oomd_scheduler_kills.md`). **Action needed**:
  confirm all 4 jobs succeed on the next Saturday window (2026-08-01) now
  that the scheduler has been stable since — the code fix already landed,
  this just needs to be observed running end-to-end once.
- **`model_training_multibagger`** — **[RESOLVED, 2026-07-30 — false alarm]**:
  scheduled correctly (Wed 23:00) and confirmed actually healthy; see §2 —
  the apparent "registry.json not found" failures were test-suite writes
  polluting production `job_run_log`, now fixed (commit `a40a44c`). No
  scheduler code was ever broken.
- **`model_training_deep_models`** (tft, bilstm, Thu 23:00) — **[INVESTIGATED,
  2026-07-30 — not a registration bug]**: confirmed correctly registered
  every startup (`journalctl` shows the CronTrigger added each time,
  identical pattern to phase1/phase2/multibagger). Zero `job_run_log` rows
  is explained by two factors: (1) every Thursday 23:00 window since this
  job was wired up (2026-07-09) has landed during a scheduler outage —
  07-16's fire was missed by >24h and silently dropped by APScheduler's
  misfire grace period, 07-23's fire happened while the service was down
  for the whole window (the documented 07-22/23 laptop-suspend outage) —
  and **weekly cron jobs have no startup catch-up/replay mechanism**, unlike
  the daily pipeline's `schedule_morning_catchup`. This is a scheduler-wide
  gap affecting every weekly job (phase1/phase2/multibagger too — they just
  happened to land on days the scheduler was up), not a bounded code bug, so
  no fix was applied — flagged as a backlog item (see Priority list). (2)
  Even if it had fired, `tft`/`bilstm` were last trained 2026-07-01 with a
  28-day interval — not yet overdue as of 07-29, so it would only have
  logged a "no models overdue" success row anyway.
- **Wavelet/entropy/pattern-score historical backfill** — **[RESOLVED,
  2026-07-29]** the wiring gap is already fixed on this branch (see §2); no
  DAG change needed. What remains is an operational decision: launch a real
  multi-year backfill run to repopulate `feature_panel_staging.duckdb` for
  historical dates. Not yet launched — this is a long-running/expensive job
  the user should explicitly schedule.
- **Fundamental strategy backfill (2026-07-20 → 07-28)** — in progress /
  incomplete as of this audit; needs to be tracked to completion separately
  from the daily_pipeline backfill.
- **`daily_backup`** — skipped deliberately (Backblaze credentials unset) — a
  known config choice, not a defect.
- **`job_health_check`** — `next_run_time` is null, meaning it may have
  dropped off the schedule after its last run.
- **2026-07-28 `data_integrity_check`** — **[NEW, 2026-07-30]** failed with
  **30 critical finding(s)** (see `data_integrity_findings` table,
  status='pending') — this is a hard dependency for `compute_features`, so
  it cascaded to skip `compute_features`, `check_ta_alerts`,
  `compute_momentum`, `run_models`, `write_signals`, `sanity_check`, and
  `paper_trade` for that entire day. Not yet investigated — see reprioritized
  list, item #1.

## Priority action list (reprioritized 2026-07-30)

Reordered around actual severity/blast-radius, not discovery order. The
biggest change: what was logged as "Unconfirmed" (old #6) turned out to be
a **live, daily, production data gap** — more severe than the backfill-only
issues it was filed alongside, so it now leads the list. The 07-20→07-28
backfill (old #1) is also demoted from "let it finish" to "blocked" — it
will not self-resolve, because it's failing on this same structural gap
plus a newly found 07-28 data-integrity blocker, not a transient issue.

1. **[NEW TOP PRIORITY]** Fix the `receivable_days_change`/
   `inventory_days_change`/`dilution_3y` data gap (see §2) — this is failing
   `sanity_check` on **every single day, live and backfilled**, not just the
   07-20→07-28 backlog. Two possible fixes, either is in scope: (a) wire
   `scripts/recompute_fundamental_ratios.py` into `pipeline_scheduler.py` so
   it actually populates these derived ratios, or (b) if that script can't
   fully solve it (source data may genuinely be unavailable), adjust
   `sanity_check`'s all-NaN gate to not hard-fail on columns that are
   structurally unavailable from the current data source, so `paper_trade`
   stops losing forward-time days over data that was never going to arrive.
   Needs a decision on which approach before implementing.
2. **[2026-08-05, expanded]** `data_integrity_check` blocks backfill for **all 12 gap dates** (07-20→08-04) — **root cause identified, fix needed.** `check_null_sweep` flags **165 features** as `critical` (100% NaN) for each backfill date. These are features dependent on data sources **not yet ingested** for those dates: fundamentals (`pe_ratio`, `roce`, `altman_z`, `debt_to_equity`…), F&O (`iv_call`, `pcr_oi`, `futures_basis_pct`…), shareholding (`promoter_pct`, `fii_pct`, `dii_pct`…), MF holdings (`mf_pct`, `mf_scheme_count`…), HMM (`hmm_regime`, `hmm_regime_duration`…), and other PIT-dependent columns. The feature Parquet exists (written by the Fyers staged backfill recompute, which only covers price/volume-dependent features), so `check_null_sweep` sees 165 columns at 100% NaN → raises → blocks `compute_features` → `run_models` → `write_signals` for every gap date. **Fix: ingest the missing data sources** (fundamentals, shareholding, MF holdings, F&O) for the gap dates **before** running `compute_features`. Suppressing via `_SANITY_KNOWN_SPARSE_COLUMNS` is wrong — these features are not permanently sparse in normal operation.
3. Complete the Fundamental strategy backfill (07-20→07-28) — still not
   done; likely blocked by the same #1 root cause for any day whose
   fundamentals feed into valuation scoring.
4. Let the daily_pipeline backfill (07-20→07-28) finish — **downgraded
   from "in progress, will complete" to "blocked"**: `sanity_check` has
   failed on 07-20, 07-21, 07-27 and `paper_trade` failed/skipped on
   07-21→07-24, all traceable to #1/#2 above, not transient retry noise.
   Resolving #1 and #2 is the actual path to clearing this.
5. Confirm whether forensic's ML component retrains at all, and pin down
   where `hmm_regime_*` actually lives (the `receivable_days_change`/
   `inventory_days_change`/`dilution_3y` half of this item is now resolved
   above — this is just the HMM/forensic half still open).
6. ~~Debug the weekend backfill jobs' `exit code 1` failures~~ **[RESOLVED,
   2026-07-30]**: the real code bugs (`promoter_pledge_backfill`,
   `nse_xbrl_fundamentals` — missing `PYTHONPATH`/`sys.path` causing
   `ModuleNotFoundError: No module named 'config'`) were already fixed in
   commit `48722e0` (2026-07-21), verified working. **Action still needed**
   (time-gated, not effort-gated): confirm `weekend_feature_backfill`/
   `weekend_fundamentals` succeed on the next Saturday (2026-08-01) — they
   haven't run at all since 07-18 due to the scheduler's OOM-kill/crash-loop
   outage, not a code issue, so this just needs to be observed once.
7. Build a missed-weekly-job catch-up mechanism (mirrors
   `schedule_morning_catchup`) so `model_training_deep_models` and other
   weekly jobs stop silently losing their fire time during outages — a
   reviewed design task, not urgent (no weekly model is currently overdue).
8. **[Open decision, unchanged]** Launch a real multi-year historical
   feature backfill for the wavelet/entropy/pattern-score block — the code
   fix already landed (§2); this is purely a "run the expensive job"
   decision. Lower urgency than #1/#2 since it doesn't affect `sanity_check`
   for the 3-column gap above (different column set).
9. Momentum "strategies of choice" — product decision, see Next Steps below.
   Not a bug, not urgent.

### Already resolved (kept for record)
- ~~Fix `model_training_multibagger`'s registry.json path resolution~~ — false
  alarm; test-suite pollution, fixed in commit `a40a44c`.
- ~~Find out why `model_training_deep_models` has never fired~~ — root cause
  found (see #7 above for the follow-up design task).
- ~~Add a historical backfill path for the wavelet/entropy/pattern-score
  feature block~~ — wiring already correct (commits `9c46fe8`/`9914406`);
  only the "launch the job" decision remains (#8 above).
- ~~Investigate `ta_signals`' partial universe coverage~~ — not a bug, it's
  an alerts-only table by design.

## Open decision — pending user go-ahead

- **Launch a real multi-year historical feature backfill** (`scripts/feature_backfill.py`
  or `scripts/feature_backfill_hybrid.py`) to repopulate the wavelet/entropy/
  pattern-score feature block (33 columns, see item #5) across historical
  dates in `feature_panel_staging.duckdb`. The code-level wiring bug is
  already fixed (§2, item #5) — this is purely a "run the job" decision, not
  a code change. Deliberately not launched automatically: it's a long-running,
  resource-intensive job (multi-year, full-universe recompute) that should be
  explicitly scheduled/kicked off by the user rather than triggered as a
  side effect of an audit.

## Next Steps

- **Update the Momentum "strategies of choice"**: review the 5 wired-in
  rank-band momentum strategies (§1 — `band1_top15_6m_m_g2` Rank 1-50,
  `band2_top15_6m_m_g2` Rank 51-100, `band3_top15_6m_m_g2` Rank 100-150
  (current default), `band4_top15_6m_m_g2` Rank 150-200, `band5_top15_6m_m_g2`
  Rank 100-200) and decide which band(s) should actually be the live/traded
  strategy going forward, given none has yet reached a rebalance day (see §1
  — `next_rebalance_date` is still NULL for all 5) or produced a real trade.
