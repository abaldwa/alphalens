# Unified Signal Generator — Deep Analysis & Refactoring Plan

Companion to `BacktestUmbrellaPlan.md`. That plan built the shared backtest core
(Phases 0–5, all marked DONE). This one addresses what it did not: **the live
daily paths were never migrated onto it.**

Bottom line, in the user's words: *we cannot go live with something based on
trades generated in a different generator from the backtests.* Today, we would.

---

## 1. Diagnosis — what is actually true today

`BacktestUmbrellaPlan.md` Phase 5 is marked DONE, and its code genuinely exists
and is correct. The gap is that it was **built but never connected**, and the
pre-existing live paths were left running beside it rather than replaced.

### 1.1 The unified paper-trading path is unreachable

`PaperTradingRunner.propose_today()` (`backtest/paper_trading/live_runner.py:172`)
is written exactly right — it takes a `StrategyAdapter` by injection and
validates `adapter.channel`, so it *would* reuse the backtest generator verbatim.

Verified by grep: **zero production call sites.** Only its own definition and 26
references across three test files. The router
`datastore/api/routers/paper_trading_unified.py` is mounted (`main.py:209`) but
exposes only `/pending`, `/accept`, `/reject`, `/gate_status`, `/state` — **there
is no propose endpoint**, and no scheduler step calls the runner. It can only
approve pending actions that nothing is able to create.

Nothing outside `backtest/adapters/` and `run_orchestrator_backtest.py:59-61`
ever constructs a `MomentumAdapter`, `TechnicalAdapter` or `FundamentalAdapter`.

### 1.2 Each channel has a second, separately-written live selection rule

| Channel | Backtest generator | Live generator | What is shared |
|---|---|---|---|
| Momentum | `MomentumAdapter` → `momentum_strategy.select_buy_pool` | `features/momentum_live.py:77` `compute_daily_ranking` | only `decide_grace_transitions` |
| Technical | `TechnicalAdapter.generate_signals:373` | `alerts/daily_alert_checker.py:168` `evaluate` | `ScreenerEngine` (evaluation only) |
| Fundamental | `FundamentalAdapter` | `routers/fundamentals.py:254,314,459` | `matches_screener_preset` predicate only |
| ML | *none* (`ml_adapter.py` is a result translator, has no `generate_signals`) | `daily_inference.py` | nothing |

**Momentum divergence.** `momentum_live.compute_daily_ranking` does not call
`rank_universe` or `select_buy_pool`. It calls `trailing_momentum` directly,
sorts, and sets `in_top_n = rank <= TOP_N` (line 123). The entire
`select_buy_pool` filter chain — ADTV floor, circuit-lock proxy, downtrend
filter, quality gate, regime disable, size/beta orthogonalization,
`min_momentum` floor — **does not exist in the live path.**

**Technical divergence.** Live persists every template match with
`score >= 1.0 - 1e-9` (`daily_alert_checker.py:152`). The backtest takes
`sorted(in_universe, key=-score)[:top_n]` *after* five entry filters
(`_filtered_candidates:322`) plus the regime gate (`_is_buys_disabled:316`).
Live therefore has no top-N cap, no ADTV floor, no downtrend/quality/regime
filter, and admits only perfect matches while the backtest also ranks partial
ones. Same template, same date, materially different holdings.

**Fundamental divergence.** The adapter ranks matches by `_composite_strength`
and truncates to `top_n`; the router returns all matches, unranked, uncapped.
This has already caused a real defect — the comment at `fundamentals.py:436-448`
documents `/scores` being the one of **three** `PRESET_EXCLUDED_SECTORS` call
sites that skipped sector exclusion, shipping methodologically invalid numbers
to four frontend pages.

### 1.3 `TOP_N` is hardcoded where the registry already has the answer

`features/momentum_live.py:42` sets `TOP_N = 15` as a module constant and applies
it to **every** rank band identically (line 123).

The registry already declares the correct per-strategy value:

```
momentum:all_risk_b1_1-50_lb3mo_weekly_top10
  {"band_id":1,"rank_start":1,"rank_end":50,"lookback_months":3,
   "rebalance_frequency":"weekly","top_n":10,"grace_cycles":2}
momentum:all_risk_b1_1-50_lb3mo_weekly_top15
  {... "top_n":15 ...}
```

So `top_n` is a **declared, per-strategy, per-band parameter** — 780 `all_risk`
rows, 780 `balanced`, 780 `max_defensive`, 780 `risk_managed` — and the live
path overrides all of them with one constant. The user is right that this cannot
be fixed to 15: two registry strategies differing *only* in `top_n` are
indistinguishable live.

### 1.4 Signals do not land in one table

`strategies/signals.py`'s own docstring names its intended consumers as
"the technical alert checker and daily inference (`source="live"`)". Verified
importers: `backtest/core/engine.py`, `backtest/core/signal_ledger.py`,
`backtest/paper_trading/live_runner.py`. **No live path imports it.**

Current state of `strategy_signals` after this session's runs:

```
('backtest', 'momentum:top10_6m_xr20_unconstrained', 116)
('backtest', 'technical:A1',                          14)
('backtest', 'fundamental:quality_compounder',          6)
```

`source` is 100% `backtest`. Live signals live in three other tables in a
different database (`datastore/signals/signals.duckdb`): `ta_signals`,
`ml_signals`, `valuation_signals` — different schemas, no `strategy_key`, no
`run_id`, no ranking, no conviction. A live paper trade still cannot be traced
back to the signal that produced it, which is the exact audit gap A94 was
written to close.

### 1.5 The existing quality gate cannot see any of this

`tests/quality/test_one_generator_per_channel.py:101` sets
`SCAN_DIRS = ["backtest", "systems", "scripts", "features"]` and only inspects
classes carrying a `channel` attribute plus `generate_signals`. All three live
divergences are function-based, and the fundamental one lives in `datastore/`.
The gate passes today while every divergence above is live. **It is measuring
the wrong thing, which is worse than not measuring.**

---

## 2. The decision you asked for: which Technical rule is "better"?

Neither, because they are answering different questions — and that is why this
drifted silently.

- `TechnicalAdapter.generate_signals` answers **"what should the portfolio
  hold?"** It caps at `top_n`, applies liquidity/downtrend/quality/regime
  filters, and ranks. It is the rule whose performance was actually measured.
- `DailyAlertChecker.evaluate` answers **"which stocks matched a template
  today?"** No cap, no portfolio context. That is a watchlist/notification.

**Recommendation: keep both, but re-scope them and make the dependency
one-directional.**

1. `ta_signals` is demoted to what it actually is — an **alerts/watchlist**
   feed. It stays uncapped and unranked. Nothing downstream may treat it as a
   holdings decision.
2. **Every holdings decision — backtest, paper, live — comes from the adapter.**
   No exceptions. This is non-negotiable, because it is the only rule with
   measured performance behind it.
3. The alert feed becomes a *consumer* of the same `ScreenerEngine` evaluation
   the adapter already uses, so a template edit cannot change one without the
   other.

The reason to prefer the backtested rule is not that it is more sophisticated —
it is that **going live on the alert rule means deploying capital against a rule
whose returns were never measured.** The uncapped rule has no backtest. That
settles it regardless of which is theoretically nicer.

---

## 3. Target architecture

One principle, stated as a testable invariant:

> **A holdings decision for a strategy is produced by exactly one function, and
> backtest / paper / live differ only in the data source feeding it and what is
> done with the output — never in the rule itself.**

```
                    strategy_registry  (declares top_n, bands, filters, exits)
                              │
                              ▼
              ┌─────────  StrategyAdapter  ─────────┐     ONE generator per channel
              │   generate_signals(universe,        │     (MomentumAdapter,
              │       as_of_date, horizon_bucket)   │      TechnicalAdapter,
              └───────────────┬─────────────────────┘      FundamentalAdapter)
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
  BacktestOrchestrator   PaperTradingRunner    LiveSignalRunner   ← NEW, thin
  (historical panel)     (.propose_today)      (today's data)
        │                     │                     │
        └─────────────────────┴─────────────────────┘
                              ▼
                   strategies.signals.write_signals()
                              ▼
                      strategy_signals  ledger
                 source = backtest | paper | live
```

`ta_signals` / `ml_signals` / `valuation_signals` continue to exist as their own
feeds, but stop being holdings authorities.

---

## 4. Phased plan

Sequenced so that **each phase is independently shippable and reversible**, and
so the highest-risk item (live behaviour change) comes only after parity is
*proven*, not asserted.

### Phase A — Make the divergence visible and un-reintroducible
*No behaviour change. Do this first; everything else is measured against it.*

- **A1.** Widen `test_one_generator_per_channel.py`: add `datastore/` to
  `SCAN_DIRS`, and extend detection beyond "class with `channel` +
  `generate_signals`" to catch module-level selection functions. Add every known
  divergence to `KNOWN_VIOLATIONS` with a ticket, so the list can only shrink.
- **A2.** New test `tests/quality/test_no_hardcoded_strategy_params.py` — fails
  on module-level `TOP_N`/`LOOKBACK_MONTHS`/`GRACE_CYCLES` constants in any
  live path, since the registry is the declared source.
- **A3.** **Parity harness** (`tests/integration/test_live_backtest_parity.py`):
  for a given strategy and date, run the adapter and the live path over the
  *same* inputs and diff the selected sets. It will fail loudly on day one —
  that is the point. This is the acceptance test for Phases C/D/E.

*Exit criteria:* the gate fails for the right reasons and the parity diff is
quantified per channel.

### Phase B — Close the ledger
*Low risk, high audit value, unblocks tracing.*

- **B1.** Wire `daily_alert_checker`, `momentum_live` and `daily_inference` to
  also call `strategies.signals.write_signals(..., source="live")`. Additive
  dual-write, exactly the pattern `ml_dual_write.py` already established in
  Phase 3 — existing tables keep their writes unchanged.
- **B2.** Backfill `strategy_key`/`strategy_version` onto live rows so a live
  signal names the registry revision it came from.

*Exit criteria:* `select source, count(*) from strategy_signals group by 1`
returns all three of `backtest`, `paper`, `live`.

### Phase C — De-hardcode Momentum
*This is item 3 in your list and the smallest real fix.*

- **C1.** Delete `TOP_N`/`LOOKBACK_MONTHS`/`GRACE_CYCLES` from `momentum_live`.
  Read each strategy's `definition_json` from `strategy_registry` (it already
  carries `top_n`, `rank_start`, `rank_end`, `lookback_months`,
  `grace_cycles`, `rebalance_frequency`).
- **C2.** Replace `compute_daily_ranking`'s inline sort with
  `momentum_strategy.rank_universe` + `select_buy_pool`, so the live path picks
  up the ADTV/downtrend/quality/regime filters it currently lacks.
- **C3.** Run the Phase A3 parity harness to zero for Momentum.

*Risk:* live picks **will change** — that is the correction, not a regression.
C2 must ship with a before/after diff of today's picks for review.

### Phase D — Split Technical's two questions
- **D1.** Re-scope `ta_signals` to alerts-only, documented in its DDL comment.
- **D2.** Build `LiveSignalRunner` (thin: today's universe + today's features →
  `adapter.generate_signals`) and make the holdings path use it.
- **D3.** Point `daily_alert_checker` at the same `ScreenerEngine` evaluation
  the adapter uses, so a template edit cannot diverge them.
- **D4.** Parity harness to zero for Technical.

### Phase E — Fundamental
- **E1.** Collapse the three `PRESET_EXCLUDED_SECTORS` call sites into one.
- **E2.** Make the `/screener` and `/scores` routers thin readers over
  `FundamentalAdapter` output rather than re-implementing selection.
- **E3.** Parity harness to zero for Fundamental.

### Phase F — Connect paper trading (finishes Umbrella Phase 5)
- **F1.** Add `POST /api/v1/paper_trading2/{channel}/{strategy_id}/propose`,
  calling `PaperTradingRunner.propose_today` with the registry-declared adapter.
- **F2.** Add a `step_propose_paper_trades` scheduler step.
- **F3.** Retire `run_daily_paper_trading.py`'s dependency on the frozen legacy
  `backtest/engine.py` (already a tracked `legacy_engine_import` violation,
  ticket "PHASE-5: delete backtest/engine.py").

### Phase G — Enforce
- **G1.** `KNOWN_VIOLATIONS` empty; parity harness green in CI.
- **G2.** Hard invariant test: no code path may set `live_eligible = true`
  except the human-gated Gate-7 flow.

---

## 5. Sequencing and risk

| Phase | Behaviour change | Risk | Reversible |
|---|---|---|---|
| A | none | none | n/a |
| B | none (additive writes) | low | yes |
| C | **live Momentum picks change** | medium | yes |
| D | **live Technical holdings change** | medium-high | yes |
| E | fundamental API results change | medium | yes |
| F | new paper-trading capability | low | yes |
| G | none | none | n/a |

Phases C, D and E each change what the system recommends. None should ship
without its parity diff reviewed by you first — the whole point is that these
paths have been silently disagreeing, so the diff *is* the deliverable.

**Do not start Phase C, D or E before Phase A3 exists.** Without the parity
harness there is no way to prove convergence, and this plan would repeat the
exact failure mode of the original: built correctly, marked DONE, never
connected, never verified.

---

## 6. Open decisions for you

1. **Alert feed scope (D1).** Confirm `ta_signals` becomes watchlist-only and no
   longer feeds any holdings decision. My recommendation is §2.
2. **Live pick changes (C/D).** Ship the corrected picks immediately once parity
   is proven, or stage them behind a flag for a review period?
3. **ML channel.** `ml_adapter.py` has no `generate_signals` and is a recorded
   `missing_generator` violation. Bring ML under the same contract in this
   effort, or explicitly scope it out and leave it on its own path?
4. **`MomentumBacktester`.** Its ranking now delegates to the shared primitives
   (ML40), so it is a second *engine* but no longer a second *rule*. ~15 scripts
   plus `systems/copilot` still construct it. Retire it in this effort or leave
   it?

---

## 7. Phase H — One measurement layer for every channel (added 2026-08-18)

Agreed with the user this session, together with two decisions that close §6:
**`MomentumBacktester` is to be retired** (§6 item 4), and **ML comes under the
same contract** (§6 item 3). This section covers the measurement half — the
metrics and tax that turn a simulation into a reported number — and it is
sequenced **ML40-2.2 BEFORE ML40-2.1**, inverting the branch runbook's order.

### Why 2.2 (measurement) before 2.1 (simulation)

The runbook put the simulation loop first because it is the A83 blocker. Three
findings from this session's investigation reverse that:

1. **There is a live, user-visible wrong number today.**
   `datastore/api/routers/momentum.py:648` computes the Holding Dashboard's
   "tax due" and "post-tax value" — over REAL recorded trades and contributions,
   not a backtest — through `momentum_tax`'s per-transaction model: tax charged
   on winning trades only, no loss set-off, no ₹1.25 lakh LTCG exemption. Every
   orchestrator-driven channel uses `core/tax.py`'s FY-netted engine with the
   asymmetric set-off. Same holdings, two different tax answers depending on
   which screen is open, and the momentum one is systematically OVERSTATED.
   That is a defect, not a refactor, and it does not need the simulation loops
   merged to fix.

2. **Merging simulations while two yardsticks exist cannot be validated.**
   2.1's acceptance test is "the unified loop reproduces the standalone
   engine's results". If metrics and tax still differ between the two paths, a
   parity failure cannot be attributed — is the simulation wrong, or is the
   measurement of it wrong? Unifying measurement FIRST makes 2.1's parity diff
   mean exactly one thing.

3. **2.2 is smaller and reversible; 2.1 changes published numbers.**
   Measurement can be unified with no change to any decision the strategies
   make. That is the right thing to ship first on a branch that also has to
   stay shippable.

### What the investigation actually found (2026-08-18)

Recorded because two of these contradict the one-line description of ML40-2.2
in `FeatureBacklog.md`:

- **`momentum_metrics.py` is not a rival module — it is a DEPENDENCY of the
  shared one.** `backtest/core/metrics.py:30` imports `xirr`, `churn_factor`
  and `return_population_zscores` from it. Those three are already the shared
  cross-channel primitives; only the module's NAME says momentum. Deleting the
  module as "the momentum copy" would break the shared metrics module.
- **The one real divergence was annualization, and it is now fixed.**
  `core/metrics.sharpe_ratio` hardcoded 252, correct for the orchestrator's
  daily equity curves and wrong for a per-rebalance momentum curve — the same
  weekly returns read **2.46 vs 1.12**, a sqrt(252/52) overstatement. That
  single mismatch is the entire reason a second Sharpe implementation was
  written. `infer_periods_per_year()` + an optional `periods_per_year` argument
  now let one implementation serve every cadence, with the 252 default
  unchanged so no published number moves.
- **The tax regime was declared twice** — identical `STCG_RATE`/`LTCG_RATE`/
  `LTCG_HOLDING_DAYS` literals in `core/tax.py` and `momentum_tax.py` — and
  `features/momentum_strategy.py`, a LIVE path, read them from the module being
  retired. Now one declaration in `core/tax.py`. **Done.**

### H1 — Rehome the shared primitives *(no behaviour change)* — ✅ DONE 2026-08-18

Move `xirr`, `churn_factor`, `return_population_zscores` out of
`backtest/momentum_metrics.py` into `backtest/core/metrics.py`, which is where
their only cross-channel consumers already are. Leave re-exports in
`momentum_metrics.py` so the ~15 `scripts/run_momentum_*.py` keep working until
H4 repoints them.

*Why first:* nothing else in H can proceed while the shared module imports from
the module being retired.

*Acceptance:* `core/metrics.py` imports nothing from `momentum_metrics.py`;
full unit suite unchanged. **Both met.** `xirr`, `churn_factor` and
`return_population_zscores` now live in `core/metrics.py`; `momentum_metrics.py`
re-exports them (verified they are the SAME objects, not copies) so the ~13
`scripts/run_momentum_*.py` keep working until H4. The four non-script
consumers were repointed at the real home: `datastore/api/portfolio_nav.py`,
`datastore/api/routers/momentum.py`, `backtest/export_trade_book.py`,
`backtest/technical_reporting.py` and `systems/copilot/backtest_bridge.py`.
One thing the move nearly broke and the type checker caught: `_NEAR_ZERO_STD`
sat inside the extracted region, so it left `momentum_metrics.py` with
`sharpe_sortino_calmar` still using it — restored there, duplicate removed from
`core/metrics.py`.

### H2 — One tax engine, including the live surface *(fixes a wrong number)* — ✅ DONE 2026-08-18

Repoint `datastore/api/routers/momentum.py` from `momentum_tax.compute_total_tax`
/`post_tax_ending_value` to `core/tax.py`'s FY-netted engine.

*This changes a displayed number, downward, and that is the correction.* Ship it
with a before/after for one real strategy_id so the size of the change is on the
record rather than discovered.

*Acceptance:* no module outside `momentum_tax.py` itself imports it; the Holding
Dashboard's tax equals what the orchestrator would compute for the same trades.

### H3 — One metrics entry point per channel

Every channel's report is built by `core/metrics.compute_metrics()`, called with
the cadence its curve actually has (`infer_periods_per_year` where not daily).
Technical, Fundamental and ML already route through it; Momentum does so only
via `momentum_adapter`, not via the standalone engine.

*Acceptance:* a test asserting that for one strategy per channel, the reported
Sharpe/Sortino/Calmar/CAGR come from `core/metrics` — the same
declaration/implementation-split shape as `tests/quality/test_registry_is_load_bearing.py`.

### H4 — Retire `MomentumBacktester` (ML40-2.3, user-approved §6.4)

Repoint the 13 `scripts/run_momentum_*.py` and `systems/copilot/backtest_bridge.py`
onto `momentum_adapter` + `BacktestOrchestrator`, then delete the class,
`momentum_metrics.py` and `momentum_tax.py`.

*Gate:* H4 must not start until ML40-2.1 has produced a parity diff the user has
reviewed. The class backs published external results; deleting it is the last
step, not the first.

### H5 — ML under the same contract (§6.3, user-approved)

`ml_adapter.py` has no `generate_signals` and is a recorded `missing_generator`
violation, and `ml_signals` is still an ML-private table. Bring ML onto the
`StrategyAdapter` protocol and dual-write into `strategy_signals` (`source="live"`),
the pattern `ml_dual_write.py` already established.

### Sequencing

| Step | Behaviour change | Risk | Gate |
|---|---|---|---|
| H1 | none | none | full suite |
| H2 | **displayed tax falls** | low | before/after on one real strategy_id |
| H3 | none | low | new per-channel metrics test |
| ML40-2.1 | **momentum results may move** | high | parity diff reviewed by user |
| H4 | deletes the standalone engine | medium | 2.1 parity accepted first |
| H5 | new ML ledger rows (additive) | low | ledger shows source=live for ML |

---

## 8. Status audit — what is actually left (2026-08-18)

Verified against the code, not against this document's own claims.

| Step | State | Evidence |
|---|---|---|
| **A1** widen the one-generator gate | ⏳ not started | `SCAN_DIRS = ["backtest", "systems", "scripts", "features"]` — `datastore/` still unscanned, so the routers that re-implement selection are invisible to it |
| **A2** no-hardcoded-params test | ⏳ not started | no such file in `tests/quality/` |
| **A3** parity harness | ⏳ **not started — and it gates C, D and E** | no `tests/integration/test_live_backtest_parity.py` |
| **B1/B2** close the ledger | ⏳ not started | `strategy_signals` holds 0 rows; still no `source='live'` or `'paper'` writer. (A108 added the supersede contract that will keep it clean once they exist.) |
| **C1** de-hardcode momentum | ⏳ not started | `features/momentum_live.py:41-43` still declares `LOOKBACK_MONTHS = 6`, `TOP_N = 15`, `GRACE_CYCLES = 2` at module level while the registry declares all three |
| **C2/C3** | ⏳ blocked on A3 | |
| **D1-D4** technical split | ⏳ not started | no `LiveSignalRunner` class anywhere |
| **E1** collapse `PRESET_EXCLUDED_SECTORS` | ⏳ not started | 21 references across non-test code |
| **E2/E3** | ⏳ not started | |
| **F1** `/paper_trading2/.../propose` | ⏳ partial | `paper_trading_unified.py` exists with the `/api/v1/paper_trading2` prefix, but has no `propose` route |
| **F2** scheduler step | ⏳ not started | |
| **F3** retire legacy `backtest/engine.py` | ⏳ not started | 5 importers remain (3 tracked in `KNOWN_VIOLATIONS`, plus `backtest/run_phase1_backtest.py` and `backtest/iterative_retrain.py`) |
| **G1/G2** enforce | ⏳ blocked | `KNOWN_VIOLATIONS` still holds 4 entries: 1 `missing_generator` (ml) + 3 `legacy_engine_import` |
| **H1** rehome shared primitives | ✅ done 2026-08-18 | `core/metrics.py` imports nothing from `momentum_metrics.py` |
| **H2** one tax engine | ✅ done 2026-08-18 | Holding Dashboard now taxes through `core/tax.py` |
| **H3** one metrics entry point | ⏳ next | needs the per-channel assertion test |
| **ML40-2.1** unify the simulation loop | ⏳ **the critical path** | still two loops; gates H4 and A83 |
| **H4** retire `MomentumBacktester` | ⏳ blocked on ML40-2.1 parity | 13 scripts + `systems/copilot/backtest_bridge.py` construct it |
| **H5** ML under the contract | ⏳ not started | `ml_adapter` still has no `generate_signals` — the one `missing_generator` violation |

### The two things that actually matter next

1. **A3, the parity harness.** The plan's own rule is "do not start Phase C, D
   or E before A3 exists", and nothing in C/D/E has started — so that rule has
   not yet been broken. It is the single highest-leverage missing piece: without
   it there is no way to prove the live and backtest paths agree, and §1's
   finding (they currently do not) stays unquantified.

2. **ML40-2.1.** It blocks H4 and A83 and is the last structural duplication.
   H1-H3 exist to make its parity diff interpretable — with one measurement
   layer, any residual difference is the simulation itself.

Everything in Phase H that could be done without touching a simulation is now
done. The remainder of this plan is gated on those two items, in that order.

---

## 9. Execution plan — A through H in dependency order (2026-08-18)

§4 gives the phases; this section gives the **order to actually build them in**,
which is not the same thing. Two constraints reshuffle it:

- **A3 gates C, D and E** (the plan's own rule) — so A3 is not "phase A work to
  get out of the way", it is the long pole and must start first.
- **A3 for a channel is only writable once that channel has a live path worth
  diffing.** For Momentum the live path exists (`compute_daily_ranking`); for
  ML it does not exist at all (H5), and for Technical the holdings side does not
  exist yet (D2). So A3 is built **per channel, next to the channel's fix**, not
  as one up-front monolith. This is the single correction this section makes to
  §4's ordering.

### Work order

| # | Item | Depends on | Behaviour change | Parallelizable |
|---|---|---|---|---|
| 1 | **A1** widen the gate to `datastore/` + module-level selection fns | — | none | ✅ |
| 2 | **A2** no-hardcoded-params test | — | none | ✅ |
| 3 | **A3-core** parity harness *skeleton* + Momentum case | A1/A2 land first so the gate names what A3 must diff | none (fails red) | — |
| 4 | **C1** de-hardcode `momentum_live` from the registry | A2, A3-core | live momentum params become per-strategy | — |
| 5 | **C2/C3** `rank_universe`+`select_buy_pool` in the live path | C1 | **live momentum picks change** | — |
| 6 | **B1/B2** dual-write live signals into `strategy_signals` | C2 (write the corrected picks, not the old ones) | none (additive) | ✅ with 7 |
| 7 | **E1** collapse the 5 `PRESET_EXCLUDED_SECTORS` sites | — | none if done as pure de-dup | ✅ with 6 |
| 8 | **A3-fundamental + E2/E3** routers become thin readers | E1 | fundamental API results change | — |
| 9 | **H3** one metrics entry point per channel | H1, H2 (done) | none | ✅ with 7/8 |
| 10 | **D2** `LiveSignalRunner` + **A3-technical** | A3-core, H3 | new holdings path (not yet wired) | — |
| 11 | **D1/D3/D4** re-scope `ta_signals`, share the ScreenerEngine eval | D2 | **live technical holdings change** | — |
| 12 | **ML40-2.1** unify the simulation loop | H3 | **momentum results may move** | — |
| 13 | **H4** retire `MomentumBacktester` + `momentum_metrics`/`momentum_tax` | 12's parity diff accepted by user | deletes the standalone engine | — |
| 14 | **H5** ML under the `StrategyAdapter` contract + **A3-ml** | H3 | new ML ledger rows (additive) | — |
| 15 | **F1/F2** propose endpoint + scheduler step | B1, and each channel's adapter live | new paper-trading capability | — |
| 16 | **F3** delete legacy `backtest/engine.py` (5 importers) | H5 (ml_adapter stops wrapping it) | none if importers already moved | — |
| 17 | **G1/G2** `KNOWN_VIOLATIONS` empty; live_eligible invariant | 1–16 | none | — |

### Why this differs from §4's A→G reading

- **B moved after C.** §4 puts B second because it is low-risk. But B1 wires the
  live paths to write into the ledger, and C2 changes what those paths select.
  Wiring first means deliberately recording picks we already know are wrong, and
  A108's supersede contract would then have to reconcile them. Cheaper to
  correct the rule, then record it.
- **E1 pulled early.** It is the one item with a real, already-shipped defect
  behind it (§1.2: `/scores` skipped sector exclusion) and it has no dependency
  on anything. It should not wait behind the momentum work.
- **A3 split three ways.** See above — a single up-front A3 cannot be written
  for ML or for Technical holdings, because neither path exists yet.
- **H3 pulled before D2 and 2.1.** D2 and 2.1 are both validated by comparing
  reported numbers. Doing them while two metrics entry points exist repeats
  exactly the attribution problem §7 describes for tax.

### Checkpoints requiring the user, not just a green suite

Three items change what the system recommends or publishes. Each stops for
review with its diff as the deliverable, per §5:

1. **Step 5 (C2)** — before/after of today's live momentum picks.
2. **Step 11 (D1/D3)** — before/after of today's technical holdings.
3. **Step 12 (ML40-2.1)** — the momentum parity diff; H4 does not start until
   this is accepted.

Step 8 (E2/E3) changes fundamental API results but only by applying filters that
were always intended, so it is reviewable as a diff without a hold.

### Critical path

`A1 → A3-core → C1 → C2 → H3 → 2.1 → H4`. Everything else (A2, B, E, D, H5, F)
hangs off it and can be parallelized against it. The two longest items are
**A3-core** (a harness that must feed identical inputs to two differently-shaped
code paths) and **ML40-2.1**.


---

## 10. Phase A delivered — and what the parity harness actually measured (2026-08-18)

A1, A2 and A3-core are implemented, validated by mutation, and green.

### A1 — the gate now sees function-based generators

`SCAN_DIRS` gains `datastore/`, but that alone changed nothing: no module
under `datastore/` constructs a `Signal()` or declares a `generate_signals`
class. That is the point of §1.5 confirmed by measurement — **the gate was
blind not because it looked in too few directories, but because it only
recognised generators shaped like a class.** Three detectors were added:

| Detector | Catches | Deleted by |
|---|---|---|
| build-a-universe **and** score it in one function | `momentum_live.compute_daily_ranking` | C2 |
| any call to the pure selection predicate `matches_screener_preset` | 2 fundamentals-router functions | E2 |
| re-deriving `score >= 1.0 - 1e-9` outside `ScreenerEngine` | `daily_alert_checker` | D3 |

Two things the build surfaced that the plan had wrong:

- **§1.2 undercounted the fundamental divergence.** It named three
  `matches_screener_preset` call sites; the gate found a fourth,
  `get_fundamentals_pillar_summary` — the home page's "today's
  recommendation count" card, which re-runs the selection to COUNT it. The
  number on the landing page is derived from the uncapped, unranked rule.
- **The momentum detector must require BOTH halves.** A bare
  "calls a momentum primitive" rule flagged `routers/momentum.py:315`,
  which uses `trailing_momentum` only to fill a 20-day-return display
  column. Requiring universe-construction *and* scoring in the same
  function separates selection from display with no permanent allowlist.

### A2 — hardcoded parameters

Scoped to live paths only (`momentum_live`, the alerts and inference
packages, `datastore/api`). The 8 `scripts/run_momentum_*.py` sites are
deliberately **not** policed: a research runner pinning the parameters of
the one experiment it exists to run deploys no capital, and policing it
would add entries that can never be removed.

One distinction the detector encodes: `momentum_signal.LOOKBACK_MONTHS =
[3, 6, 9, 12]` is a MENU of supported lookbacks, not a chosen value. A
scalar is a decision; a container is an enumeration. Only scalars are
flagged.

### A3 — the harness, and the finding that corrects §1.2

**§1.2 predicted momentum parity would fail on day one. It does not, and
the reason is more useful than the prediction.**

Measured on 2026-08-14, rank band 3, identical universe fed to both paths:

```
=== PARITY DIFF: momentum / band3_top15_6m_m_g2 [all_risk] @ 2026-08-14 ===
  universe scored : 50     backtest 15   live 15
  agreed          : 15     Jaccard 1.000
```

This is correct, not a harness bug. `build_category_presets` defines
`all_risk` as the "unfiltered baseline (zero kwargs)", and every
`MomentumAdapter` filter defaults to off/None — so with no filters
configured, "rank then take the top N" genuinely IS the same rule on both
sides. **That parity is now asserted as a hard invariant**, protecting the
one place the two paths already agree.

The real gap is structural, not per-date:

> `features/momentum_live.py` cannot express a filtered category at all.
> Its `STRATEGIES` entries carry only `band_id`, `label`, `rank_start`,
> `rank_end`, `strategy_id`. The registry declares four cumulative
> categories. **Three of the four are unrepresentable live**, so a
> `balanced` or `max_defensive` strategy would run COMPLETELY UNFILTERED in
> production while its backtest applied the whole chain.

And a caution that changes how C1 should be verified: **today this is
invisible.** At the production liquidity floor (0.1cr — the value
`run_momentum_recommended_strategies.py:111` actually uses, *not*
`settings.MIN_ADTV_CR`, which is 0.0 under the active profile) nothing is
excluded; band 8's least liquid name still trades 0.18cr. The filtered and
unfiltered selections coincide by accident of parameter values. Raising the
floor to 25cr moves the held set by 6-8 names in bands 7 and 8.

A diff-based test would therefore pass today and keep passing until the day
someone tightens a filter — which is the day it would matter. The C1 gate is
asserted structurally instead.

**Sensitivity guard.** Because every parity result above is "no
difference", the harness carries a test that raises the floor until it must
bind and asserts the selection moves. Without it, an ignored kwarg or an
empty panel would produce a perfect and entirely meaningless parity score.
An early draft of the harness had exactly that defect: it applied
`settings.MIN_ADTV_CR` (0.0) and reported flawless parity for a filter that
filtered nothing.

### Revised status

| Step | State |
|---|---|
| A1 | ✅ done — 4 tracked violations, mutation-validated |
| A2 | ✅ done — 3 tracked violations, both directions validated |
| A3-core | ✅ done — momentum; technical/fundamental slots skip with reasons |
| A3-technical | blocked on D2 (no live holdings path exists to diff) |
| A3-fundamental | blocked on E1 (would measure sector-exclusion inconsistency) |

Next per §9: **C1** — wire `momentum_live` to `definition_json`, which the
xfail in the harness is written to detect the moment it lands.


---

## 11. C1 delivered — momentum live reads the registry (2026-08-18)

`features/momentum_live.py` no longer declares `TOP_N`, `LOOKBACK_MONTHS` or
`GRACE_CYCLES`. Each of the 7 live strategies now carries a `registry_key`,
and `strategy_params()` reads `top_n` / `lookback_months` / `grace_cycles`
from that row's `definition_json`.

### Why this was far safer than §5's risk table assumed

Two facts checked before writing any code:

1. **All 7 live strategies map onto existing registry rows whose declared
   values are IDENTICAL to the constants removed** (`top_n=15`,
   `lookback_months=6`, `grace_cycles=2`). C1 is therefore a pure rewiring:
   it changes where the answer comes from, not what the answer is. No live
   pick moves today.
2. **`momentum_trades` and `momentum_contributions` are both EMPTY.** The
   live `strategy_id` values are persisted in those tables, so remapping
   them costs nothing right now and would have been a data migration once
   real trades accumulated. This was the right moment to do it.

§5 rated C as "live Momentum picks change / medium risk". For C1 alone that
is not true, and the reason is worth keeping: the risk in C is concentrated
entirely in **C2**, where the filter chain starts running.

### The one deliberate behaviour change: it now fails loudly

`strategy_params()` raises `StrategyParamsUnavailable` when the registry has
no active row, or when a row declares only some of the three parameters.
There is **no fallback default**, and that is the whole point — a fallback is
what made the constants dangerous. A run with a silent default proceeds,
looks healthy, and trades parameters nobody approved. Stopping is loud and
recoverable; trading the wrong parameters is neither.

The partial-row case is rejected for the same reason: a row declaring
`top_n` but not `grace_cycles` would supply two real values and one invented
one, which is worse than supplying none.

### Gate movements

- `tests/quality/test_no_hardcoded_strategy_params.py` — `KNOWN_VIOLATIONS`
  is now **empty**. The rule is absolute: the next hardcoded live parameter
  fails outright.
- The parity harness reads parameters through `strategy_params()` too, so
  its diff can never be an artefact of the harness and the live path
  disagreeing about `top_n`.
- The C-phase xfail moves from C1 to **C2** and its assertion is unchanged:
  `momentum_live` now DECLARES `category="all_risk"` honestly, but still
  cannot APPLY the other three categories' filter chains.

### Tests

`tests/unit/test_momentum_live_registry_params.py` (7 tests) covers the
contract, including the two failure modes above. The 5 existing
`test_momentum_live.py` tests that monkeypatched the constants now patch
`strategy_params` instead — their need was always legitimate (a 29-day
fixture cannot exercise a 6-month lookback), so only the injection point
moved, onto the same function production reads through.

### Remaining in Phase C

**C2** — replace `compute_daily_ranking`'s inline sort with
`rank_universe` + `select_buy_pool`, which is where the live picks actually
change and where §5's medium risk really lives. Note from §10: the ADTV
floor does not bind at current parameters, so C2 must be reviewed on the
filter chain it enables, not on a same-day diff that will likely show zero.


---

## 12. C2 + C3 delivered — and the divergence they found (2026-08-18)

`compute_daily_ranking` no longer ranks inline. It calls
`features.momentum_strategy.rank_universe` (the ranking) and
`select_buy_pool` (the filter chain) — the same two functions
`MomentumAdapter` and `MomentumBacktester` call.

### The live picks DID change, and the reason was not the filter chain

`all_risk` declares no selection filters, so `select_buy_pool` is a no-op
for every strategy the live path runs today. The expectation was therefore a
zero diff. Measured against the pre-C2 behaviour on 2026-08-14:

| Band | Rank range | Names changed (of 15) |
|---|---|---|
| 1, 2, 3, 4, 6 | 1-200, 201-300 | **0** |
| 7 | 301-500 | **8** |
| 8 | 501-800 | **6** |

**Root cause — two implementations of "trailing return" that were never the
same function.** `features/momentum_signal.trailing_momentum` (the conn-based
one the live path used) selects each ticker's last `lookback_days` rows with

```sql
ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC)
```

i.e. **each ticker's own last 126 trading rows**. The backtested
`trailing_momentum_from_panel` uses the shared calendar window across the
whole universe. For a name that trades every day these are identical — which
is exactly why bands 1-4 and 6 show a zero diff. For a name with trading
gaps, "126 of its own rows" reaches further back in calendar time, so it
measured a longer, different window:

```
AZAD   conn 0.7351  panel 0.7058      max divergence on common tickers: 0.140
AVALON conn 0.8478  panel 0.8415      (14 percentage points)
```

It also changed the *eligible set*: the conn path scored 196 tickers in band
7 where the panel path scored 190. Six names (DIACABS, E2E, GSPL, JSWDULUX,
MTARTECH, STLTECH) accumulate 126 rows only by reaching outside the panel
window.

So the pre-C2 live path was ranking illiquid bands on a per-ticker window
that no backtest ever used. **The published band-7 and band-8 results were
measured on the calendar-aligned rule; live was running the other one.** The
change is the correction, and it moves live onto the rule with measured
returns — §2's principle, applied.

Verified after C2: live selection equals `MomentumAdapter`'s selection
**exactly, for all 7 bands**.

### C2's most important addition: it refuses rather than skips

`_buy_pool_kwargs` translates a strategy's registry-declared `filter_ids`
into `select_buy_pool` arguments. A declared filter this path cannot apply —
`quality_gate`, `hmm_regime`, `size_beta_orthogonalized`, all of which need
backtest-time inputs the live path has no source for — raises
`StrategyNotRunnableLive`.

It does **not** drop the filter and proceed. Dropping it would reproduce
precisely the defect this phase exists to remove: a `balanced` strategy
running completely unfiltered while its backtest applied the whole chain,
silently and looking healthy. `adtv_capped_sizing` is exempt because
`filter_registry` types it `sizing`, not `entry`/`universe` — it changes how
much is bought, not what is selected.

The registry stays authoritative throughout: which filters a strategy has
(`filter_ids`) and their parameters (`resolve_filters`) are read, never
restated.

### C3 — parity to zero for Momentum

The parity harness now parametrizes over **every** live rank band. Band 3
alone was not enough: before C2 the live path agreed with the adapter on
bands 1-4 and 6 while disagreeing on 7 and 8, and a band-3-only test reported
perfect parity throughout. A test that cannot fail for the reason it exists
is not evidence.

### Gate movements

- `test_one_generator_per_channel.py`: the `live_selection_function` entry
  for `compute_daily_ranking` is **deleted** — resolved, not appeased.
  `rank_universe`/`select_buy_pool` are deliberately not scoring primitives:
  calling the shared implementation is delegation, the same standard
  `DELEGATING_COMPOSITES` applies to `TechnicalComboAdapter`.
- The C-phase xfail remains, now correctly scoped: the live path can APPLY a
  declared filter, but only `adtv_floor` and `circuit_lock_proxy` have live
  data sources, so 3 of 4 registry categories still cannot run live. They now
  fail loudly instead of running unfiltered — the gap is contained, not
  closed.

### Still open in Phase C

Nothing. C1, C2 and C3 are done. The remaining momentum gap is a **data**
gap, not a code one: wiring live quality-score, HMM-regime and market-cap/beta
sources would make the other three categories runnable.


---

## 13. E1 and B1 delivered (2026-08-18)

### E1 — sector exclusion has one owner

`features.fundamental_composites.is_sector_excluded(preset, sector)` is now
the only place the question is answered. The two branches in the fundamentals
router are gone: they tested `preset in PRESET_EXCLUDED_SECTORS` and passed
`sector` only then, which was a **second decision about whether an exclusion
applies**, alongside the predicate that already decides it. Passing `sector`
unconditionally is behaviour-identical (the lookup is an empty set for a
preset with no exclusions) and leaves one site that knows the rule. `/scores`
and `FundamentalAdapter` now call the helper too.

A finding worth recording, because it looks like a bug in the table and is
not: **`PRESET_EXCLUDED_SECTORS` has 14 keys, `SCREENER_PRESETS` has 9.** The
extra five are composite-SCORE strategies (`moat`, `sector_leader`, `qglp`,
`longevity`, ...) which `matches_screener_preset` rejects outright with
"Unknown screener preset". This is exactly why the shared owner is a helper
that answers the sector question alone rather than the predicate: `/scores`
deals in score strategies, so routing it through the predicate was never
possible. Recorded as a test.

`tests/quality/test_sector_exclusion_has_one_owner.py` keeps it collapsed —
AST-based, so a mention in a docstring is not mistaken for a read.
Mutation-validated. mypy debt fell 1,320 → 1,319.

### B1 — live momentum signals reach the ledger

`momentum_live.record_live_signals()` dual-writes the day's selection into
`strategy_signals` with `source="live"`, wired into the scheduler's
`step_compute_momentum`. `momentum_rankings` is unchanged — the same additive
pattern `ml_dual_write.py` established.

Why it matters: `strategy_signals` is the only table that ties a signal to
the registry REVISION that produced it (`strategy_key` + `strategy_version`).
`momentum_rankings` records a bare local `strategy_id` with no version, so a
live pick could never be traced back to its declaration — the audit gap A94
exists to close. The ledger was 100% `source='backtest'` before this.

Two deliberate choices:

- **Only selected names are written**, as `action="buy"`. The full ranking is
  already in `momentum_rankings`; a row per scored ticker per band per day is
  what turns this table from millions of rows into hundreds of millions,
  which is why `write_signals` refuses holds unless explicitly asked.
- **The ledger write is non-fatal.** A failure logs loudly but does not take
  down the momentum step: the rankings the dashboard serves are already
  committed, and the ledger is an audit record, not the product.

Tests write to an isolated temp DuckDB; the production ledger was verified
untouched afterwards.

### Still open

**B2** (backfill `strategy_key`/`strategy_version` onto existing live rows)
is a no-op today: there are no pre-existing live rows to backfill, because
nothing wrote any until now. B1 for **technical** (`daily_alert_checker`) and
**ML** (`daily_inference`) remains; ML's is properly part of H5.


### H3 — one measurement layer, pinned

`tests/quality/test_one_measurement_layer.py` asserts every metric primitive
(sharpe/sortino/calmar/cagr/max_drawdown/xirr/win_rate/turnover/churn) is
DEFINED in exactly one module. Callers may import from anywhere; a second
`def` may not appear.

`backtest/momentum_metrics.py` is a tracked exception with 4 duplicate
definitions (`cagr`, `sharpe_sortino_calmar`, `win_rate`,
`rolling_window_summary`), scheduled for deletion by H4. Re-exports are
explicitly not duplicates — `momentum_metrics` re-exports `xirr` and
`churn_factor` from core after H1, and a gate that punished re-exports would
punish the very pattern that fixed the duplication.

A third test pins the cadence argument itself, because that is the defect
that CAUSED the duplication: a Sharpe hardcoding 252 periods/year is wrong
for every non-daily curve, and "wrong" meant a weekly-rebalance strategy
reporting 2.46 where the truth is 1.12. One implementation can only serve
every channel while the cadence is an input, so `periods_per_year` must
exist and must default to None.

Mutation-validated.

## §14 — D2 + A3-technical delivered (2026-08-18)

`backtest/core/live_signal_runner.py` — `LiveSignalRunner`, the third caller of
the one generator per channel, alongside `BacktestOrchestrator` and
`PaperTradingRunner`. It owns no selection logic: `signals_for()` gates on
readiness, delegates to `adapter.generate_signals`, and records the result in
the A94 ledger with `source="live"`; `target_holdings()` is its buy-side, in the
adapter's own ranking order.

The A103 readiness gate moved to `backtest/core/readiness_gate.py`
(`check_readiness`), shared by the paper and live runners. `PaperTradingRunner`
now calls it instead of holding its own copy — behaviour identical, and its 26
existing tests still pass unchanged.

**A3-technical (harness slot filled).** `build_technical_parity_report` diffs the
live holdings path against the backtested rule for template A1 on the latest
real trading day: **zero diff** (mutation-validated — truncating
`target_holdings` to 5 makes the test fail). A second test measures the D1/D3
gap directly: held names are a strict subset of the alert feed's full matches,
so today the two paths agree about what matched; D3 removes the possibility
that they stop agreeing.

`tests/quality/mypy_baseline.json` regenerated (1,166 → 1,165): extracting the
readiness gate cleared `backtest/paper_trading/live_runner.py`'s last entry.

### Checkpoint — step 11 (D1/D3/D4) needs review before it ships
D3 changes what the live technical holdings actually are (`daily_alert_checker`
and the adapter start sharing one `ScreenerEngine` evaluation), and §9 lists it
as a user-review checkpoint. D2 is wired but nothing has been cut over to it.

## §15 — D1/D3/D4 delivered (2026-08-18)

**D3 — one evaluation.** `ScreenerEngine.screen_all(date, limit, templates)` is
new and owns the batch run: load the date's feature Parquet once, evaluate every
declared template through the same `_screen_df` a single-template `screen()`
call uses, and log-and-continue past a template that raises. `DailyAlertChecker.
evaluate()` is now a thin wrapper over it — it previously ran its own loop
reaching through to the engine's private `_load_df`/`_screen_df`, so a change to
how the screener evaluates a template did not necessarily reach the alert feed.

Verified byte-identical before/after on 2026-08-14: 63 templates, 11,549 full
matches, same tickers in the same per-template order.

**The full-match rule has one owner.** `score >= 1.0 - 1e-9` existed in three
places (the engine, twice in the alert checker). It is now
`engine.FULL_MATCH_EPSILON` + `engine.is_full_match()`, with the remaining call
site in `_write_all_results` kept as an explicit guard on the WRITE, since
`ta_signals` is contractually full-match-only (SPEC-TA-008) and that is its only
writer. `KNOWN_VIOLATIONS`' `duplicate_full_match_rule` entry is deleted, and the
quality gate now also asserts the owner still exports the rule — otherwise the
test would have passed if the rule vanished everywhere.

**D1 — `ta_signals` is alerts-only**, stated in the table's DDL comment, the
module docstring and the class docstring: uncapped and unranked by design, never
a holdings authority. Holdings come from `TechnicalAdapter` via
`LiveSignalRunner` into `strategy_signals`.

**D4 — Technical parity is zero.** Two integration assertions:
`test_technical_live_selection_matches_the_backtested_rule` (live == backtested
selection) and `test_the_alert_feed_and_the_screener_share_one_evaluation`
(the alert feed's matched set is exactly `screen()`'s, same order).

Mutation-validated, all four: truncating `screen_all`'s results, widening
`FULL_MATCH_EPSILON` to 1e-3, and re-inlining the threshold in the alert checker
each fail the gate that covers them.

Two existing tests were updated, not weakened: they monkeypatched `resolve_date`
on the alert-checker module, and date resolution now lives with the evaluation
in `ScreenerEngine`.

## §16 — E2/E3 delivered (2026-08-18)

`FundamentalAdapter.select_candidates(universe, as_of_date)` is extracted from
`generate_signals` — everyone the preset matches, before entry filters and the
top_n cut. That split is the right seam for the live screener: entry filters and
top_n are portfolio-construction decisions ("how much can I buy", "how many
slots"), and `/screener` answers only "what matches".

`GET /fundamental/screener` and `/pillar_summary` now call it through one
`_matched_tickers()` helper instead of re-implementing the adapter's four
dispatch branches inline. The router's copy had already drifted once —
composite-score strategies were evaluated live with no sector exclusion at all —
and that class of divergence is invisible, because both sides return a plausible
list of tickers. The DB connection is opened only for the three bespoke presets
that read raw PIT financials.

The three bespoke preset names are imported from the adapter rather than spelled
out a second time in the router.

**Verified on the latest real feature day**: all 9 screener presets return
IDENTICAL ticker sets before and after. The bespoke presets were checked
separately (the PIT path is slow).

**E3**: `test_fundamental_live_selection_matches_the_backtested_rule` is live and
passing — the parity harness now has no skipped channel slots left except ML.
Mutation-validated: truncating `_matched_tickers` fails both it and the
home-page-count test.

`KNOWN_VIOLATIONS` loses both remaining `live_selection_function` entries. What
is left is 3 `legacy_engine_import` (F3) and 1 `missing_generator` (H5).

One typing fix at source: `_sector_map()` yields `Optional[str]` values (a
ticker whose sector was never sourced), so the adapter's `sector_lookup` is
typed honestly rather than cast, and `Signal.sector` goes through a `_sector()`
helper — `.get(ticker, "Unknown")` covered a missing key but not a stored null.

## §17 — F1/F2 delivered (2026-08-18)

**`backtest/core/live_adapter_factory.py`** — `build_live_adapter(channel,
strategy_id, as_of_date, conn=, top_n=)` returns `(adapter, universe)`: the same
adapter a backtest of that strategy would build, configured from its registry
row, plus the universe it should see today. An adapter's constructor kwargs ARE
the strategy, so assembling them per call site is how a live path comes to run
something the backtest never measured.

It refuses rather than guesses:
* an unsupported declared filter raises `StrategyNotRunnableLive` (same rule and
  message shape as `momentum_live`'s), because running a strategy without its
  declared filters is running a different strategy;
* `top_n` is required for technical and fundamental — their registry rows
  declare the entry rule but genuinely not a holdings count, and momentum's
  declared `top_n` always wins over a caller-supplied one (asserted);
* ML raises up front naming PHASE-H5, rather than failing at the registry
  lookup with what looks like a missing migration.

**F1 — `POST /api/v1/paper_trading2/{channel}/{strategy_id}/propose`**: builds
the adapter through the factory, hands it to `PaperTradingRunner.propose_today`,
returns the queued actions. Nothing executes; every action still needs a human
accept(). `StrategyNotRunnableLive` → 409 (valid request, cannot honour a
declared filter today), `DefinitionNotFound` → 404, other `ValueError`/`KeyError`
→ 400. The DuckDB connection stays open across `generate_signals`, since the
three bespoke fundamental presets read raw PIT financials while running.

**F2 — `step_propose_paper_trades`**: iterates ACTIVE `strategy_deployments`
rows and proposes for each. NOT backfillable, for exactly `paper_trade`'s
reason — a proposal for a day already past was never genuinely live, and
backfilling one would inflate Gate 7's forward-day count. One strategy's refusal
or failure is logged and skipped; the loop continues. Both handlers are
documented in `exception_catalog.py` (whose three existing pins had drifted with
the insertion and are re-pinned).

There are zero rows in `strategy_deployments` today, so the step is a no-op
until something is deployed — which is the correct behaviour, not a gap.

Mutation-validated: turning the per-deployment `continue` into a `break`, having
the endpoint skip the queue, and letting a caller's `top_n` override momentum's
declared one each fail their test.

## §18 — Momentum sizing policy decided and implemented (2026-08-18)

Three user decisions, taken after the ML40-2.1 parity diff exposed that the
shared loop ran momentum at roughly 45% invested:

1. **`investable_pct = 1.0`** — momentum is fully invested. Its equal-weight
   slot is `1/top_n` (6.67% at top_n=15); the D21 bucket's 3% ceiling would
   clip that to less than half.
2. **No sector diversification** — momentum's risk control is the universe
   itself, the top 800 names by ADTV. `max_sector_pct` goes to 1.0.
3. **`top_n` is honoured** — the book is divided into the strategy's own slot
   count.

**Implementation.** `OrchestratorConfig` gains `n_target_positions` and
`sizing_overrides`. The orchestrator now derives the slot count as
`config.n_target_positions or adapter.top_n`, falling back to
StrategyPortfolio's 10 only for an adapter that declares no top_n — it
previously passed nothing, so EVERY strategy on EVERY channel sized for 10
slots regardless of its top_n. `run_orchestrator_backtest._sizing_overrides_for`
returns momentum's two overrides and `None` for every other channel; this is
deliberately not a global change.

**Measured effect** (band 101-150, 2018-2019, unfiltered top-15): shared-loop
CAGR **0.98% → 2.80%**, against the standalone's ~3.2%/yr. Buys move 54 → 49
(bigger positions, so cash binds slightly sooner).

### The parity gap is NOT a sizing problem — corrected finding

An instrumented `can_buy` over the same window: **62 accepted, 1 rejected, out
of 63 offers**. Sizing caps, the sector cap, cash and the ADTV floor rejected
essentially nothing. The orchestrator was never OFFERED the other buys, so the
remaining 75-vs-49 gap lives in `MomentumAdapter`'s rotation rule versus
`MomentumBacktester`'s, not in the portfolio. It is cumulative: the two agree
exactly through 2018 (14/14, 6/6, 3/3) and diverge progressively through 2019
(4 vs 1, 4 vs 0, 4 vs 0). That is the next thing to isolate before H4.

Two earlier attributions in §17's diff were wrong and are retracted: the caps
were never binding (raising them 3%→5% and 20%→25% changed the buy count not at
all), and the first 10-year run was misconfigured — it omitted
`exit_policy_cadence="rebalance"`, which every real momentum run sets.

## §19 — Momentum's universe redefined (2026-08-18, user decision)

**Pure-play momentum is not a Technical strategy.** Momentum strategies under
the Technical umbrella stay Technical — they use technical indicators. The
band-based strategies below are the pure-play ones, and this section governs
only those.

### The universe: ADTV first, market cap second

    1. top 800 by trailing ADTV as of the refresh date   <- the risk control
    2. rank THAT set by market cap, slice the band (1-50, 51-100, ...)

The order is the decision, not an implementation detail. Ranking by market cap
first and filtering by liquidity second leaves a band short whenever an
illiquid large-cap holds a slot; filtering first means every band is always 50
real, tradeable names. This is also why momentum carries no sector cap — the
universe IS the risk control.

**What was actually happening before:** when `rank_band_id` is set (every
momentum band run), `config.universe_provider` is REPLACED by the market-cap
band provider, which discards the PIT ADTV top-800 filter entirely. Momentum
bands have been pure market-cap ranks over every tracked ticker, with no
liquidity gate at all.

### The 21-trading-day grid

Both rankings refresh every 21 trading days, just before the monthly
strategies rebalance. A strategy uses the most recent snapshot at or before
its own rebalance date, so every cadence composes without knowing about the
grid: weekly/biweekly take whatever is current, monthly takes that day's,
bimonthly every 2nd, quarterly every 3rd. Re-ranking daily would let a name
leave the universe on a day no strategy trades — a sell nobody decided.

`features/momentum_universe.py` gains `ADTV_UNIVERSE_TOP_N`,
`UNIVERSE_REFRESH_TRADING_DAYS`, `universe_refresh_dates`,
`universe_snapshot_date`, `liquid_universe` and `momentum_band_universe` —
THE one definition, which backtest, paper trading and live all resolve
through. 11 tests, order-of-operations mutation-validated.

### Deprecated for momentum only (logic, UI and APIs)

grace cycles, asymmetric exit band (`exit_rank`), ADTV-capped sizing
(`max_pct_of_adtv`), `trailing_stop_pct`, per-ticker HMM regime,
`min_momentum` floor, volume-weighted sizing. The strategy is the plain
List-1-vs-List-2 swap: sell what left the top N, buy what entered.

**Retained:** sticky promotion (on MARKET CAP rank only, never ADTV — a name
that falls out of the liquid universe is sold, because liquidity is a
tradability constraint), buy-side filters, rebalance every N trading days from
run start, FY tax withholding, annual capital reset.

### No reconciliation with MomentumBacktester

Explicit user decision: fresh start, no parity target against the standalone
engine. **The requirement that replaces it is that there be ONE generator
logic across backtest, paper trading and live.** ML40-2.1's parity diff is
therefore no longer a gate on H4.

---

## §20 — The deprecation reaches the registry, the API, the schema and the UI (2026-08-18)

§19 removed the seven knobs from the *engine*. They were still declared in
four other places, each of which is a surface a user or another service can
read. A knob that no code honours but every schema still advertises is worse
than one that works: it reads as configuration and behaves as decoration.

### The registry (`strategies/migrations/momentum.py`)

`definition_json` no longer carries `grace_cycles`. `exit_criterion_json`
changes from

```json
{"variant": "rank_grace", "grace_cycles": 2, "exit_rank": <top_n>}
```

to

```json
{"variant": "rank_exit", "exit_rank": <top_n>}
```

`exit_rank` stays only because it is now *derived*: with the asymmetric band
gone, the exit rank IS `top_n`, so the field states a consequence rather than
offering a choice.

Two tests hold the line: one asserts the new shape, one scans every generated
row's definition/entry/exit for any of the six deprecated param names. The
second was mutation-validated (adding `grace_cycles` back to the definition
fails it; reverting passes).

### The 1,440 stale registry rows, explained

`build_rows()` generates 1,680 keys. The DB holds 3,120 **active** momentum
rows. The 1,440-row difference is not drift — it is a superseded band
definition: the stale keys use *overlapping* boundaries (`b3_100-150`,
`b4_150-200`) where `RANK_BANDS` now uses contiguous ones (`101-150`,
`151-200`). Every current key is already active, so the migration only revises;
the 1,440 need retiring, not rewriting.

### The API and schema

`momentum_strategy_configs` loses `grace_period`, `exit_rank` and
`trailing_stop_pct` — including from its UNIQUE key, which is why this needs a
table rebuild rather than three `ALTER`s. `momentum_trades` and
`momentum_rebalance_suggestions` lose `grace_remaining`.

`downtrend_filter_pct` and `hmm_regime_filter` **stay**. Both are BUY-side
filters, and buy-side filters are retained; the deprecated "per-ticker HMM
regime" was a per-holding exit condition, which is a different thing. The
form's "Tier 1: Asymmetric Entry/Exit & Trailing Stop" section is gone
entirely — both its fields were deprecated — and "Tier 2" is relabelled
"Buy-Side Filters", there being no tiers left to number.

`momentum_trades.exit_rank` is **kept**: it records the rank a holding
actually had when it was sold. That is an observation, not a knob.

### A live bug this surfaced

`daily_pipeline.step_momentum_rebalance` still read `s["grace_remaining"]`
off every suggestion and wrote it back onto the open trade. §19 had already
removed that key from `compute_rebalance_suggestions`' output, so the
scheduler's momentum rebalance step would have raised `KeyError` on the next
rebalance day. Found by following the schema change into its callers, not by
a test — no test covered that step's write path.

The step now carries no state between cycles at all, which is the honest
expression of a plain list swap.

### Copilot

`RebalanceRules` loses `grace_cycles` and `min_momentum`, so the LLM prompt no
longer offers them and `backtest_bridge` no longer passes them. `from_dict`
now filters against the dataclass's field set, so specs persisted before today
load (minus the dead knobs) instead of raising `TypeError`.

### Still blocked

The DB-side migration — revising the 1,680 current rows and retiring the 1,440
stale ones, plus rebuilding the three momentum tables — has not run. The code
is ready; the write needs an approved window.
