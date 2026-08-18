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

### H1 — Rehome the shared primitives *(no behaviour change)*

Move `xirr`, `churn_factor`, `return_population_zscores` out of
`backtest/momentum_metrics.py` into `backtest/core/metrics.py`, which is where
their only cross-channel consumers already are. Leave re-exports in
`momentum_metrics.py` so the ~15 `scripts/run_momentum_*.py` keep working until
H4 repoints them.

*Why first:* nothing else in H can proceed while the shared module imports from
the module being retired.

*Acceptance:* `core/metrics.py` imports nothing from `momentum_metrics.py`;
full unit suite unchanged.

### H2 — One tax engine, including the live surface *(fixes a wrong number)*

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

