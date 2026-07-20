# Unified Backtest & Paper Trading Umbrella

## Context

AlphaLens currently has four recommendation channels (Technical, Fundamental, ML, Momentum) but their backtesting is fragmented: `backtest/engine.py` drives only the ML signal-model stack; `backtest/momentum_backtest.py` is a completely separate, standalone engine built the same week (2026-07-19) that duplicates portfolio/cash-flow/metrics logic with a different CAGR methodology; Technical has only a historical win-rate lookup (`strategy_confidence.py`), not a real backtest; Fundamental has nothing. Paper trading exists only for ML signals (Gate 7, ≥90 forward days before live capital), while Momentum has a separate manual trade journal, and Technical/Fundamental have neither.

This plan unifies all of this under one `Backtest` module and menu item, with Paper Trading as a downstream stage of the same pipeline (backtest → paper trade → live), standardized strategy-horizon classification and position sizing across channels, support for 1/3/5/10/20-year backtests, required metrics (CAGR, XIRR, final capital, churn, win rate, max drawdown, cash position, SIP mode), and full per-decision feature-vector logging to support a feature-reengineering/model-finetuning feedback loop.

**Decisions confirmed with user:**
- **CAGR/XIRR methodology**: calendar-day/365.25 CAGR + XIRR is the canonical primary metric everywhere (correctly handles SIP cash flows). Engine.py's trading-day-annualized CAGR is kept only as a secondary/legacy field for backward comparability with existing ML reports — not the standard going forward.
- **Benchmark gap — REVERSED 2026-07-20, no longer accepted, fixed.** Originally: `index_ohlcv` only had Nifty history from 2023-07-03, accepted as a permanent gap with no backfill effort. This was wrong — investigated live and found the 2023-07-03 floor was an artifact (every one of the 15 tracked indices started on that exact date with zero gaps since, meaning the backfill had simply never been run earlier), not a real NSE data-availability limit. Confirmed directly against NSE's own `ind_close_all` archive (the same endpoint the user's `nse-historical-data`/niftyindices.com leads ultimately trace back to — neither is an independent source, both hit this same archive) that real daily index data exists back to **~2012-03-12**. The actual blocker: NSE renamed every CNX-prefixed index to its current Nifty-prefixed name around 2015-11-06 ("CNX Nifty"→"Nifty 50", "CNX Finance"→"Nifty Financial Services", etc., "S&P CNX Nifty" even earlier back to 2012) and `ingestion/scrapers/nse_indices.py`'s `TRACKED_INDICES` filter only ever matched current names — every pre-rename row was silently dropped even though the data was reachable. Fixed by adding `HISTORICAL_INDEX_ALIASES` (13 of 15 tracked indices have a verified real historical alias; "Nifty Healthcare Index" and "Nifty Oil & Gas" genuinely don't — both launched after the CNX-era archives checked, left unmapped rather than guessed) and canonicalizing aliased names to their current form before the `TRACKED_INDICES` filter runs. Also fixed a related latent bug caught in the same pass: the "stale/cached archive file" staleness check only parsed the current `Index Date` format (`DD-Mon-YYYY`) — pre-2016 files stamp it `DD-MM-YYYY`, which silently produced `NaT` and disabled the check entirely for any historical date; now tries both formats. 11 tests added/passing in `tests/unit/test_nse_indices.py` (alias canonicalization, unmapped-name-stays-dropped, both staleness-check date formats), no regressions, quality-gate failures confirmed pre-existing/unrelated. **Backfill executed against the live DB**: `scripts/backfill_index_ohlcv.py --from-date 2012-03-13 --to-date 2023-07-02` (bounded to the real archive start through just before existing coverage, avoiding ~7 years of guaranteed 404s pre-archive and redundant re-fetching of the ~753 already-covered dates) — see the dated note below for the run's outcome. Once complete, benchmark-relative metrics (excess_return, benchmark_cagr) become real and non-null back to ~2012 instead of only 2023-07+, for every channel's backtests.
- **Momentum's manual trade journal**: fold it into the unified paper-trading flow in Phase 5, accepting some workflow migration, so all four channels end up on one consistent paper-trading schema/UI instead of Momentum staying a special case.
- **No cross-channel/multi-strategy capital pooling.** Each strategy (channel × horizon bucket, e.g. "Technical 21-day", "Momentum MultiBagger") runs its own independent backtest, walk-forward, and paper-trading cycle against its own dedicated capital base. There is no combined/blended portfolio anywhere in this design — confirmed as a hard requirement, not just an initial-scope limitation. `core/portfolio.py` must not assume or later grow a shared-pool concept; each `BacktestRun`/paper-trading instance is scoped to exactly one strategy and one capital base, full stop.
- **Tax modeling**: fixed at today's rates — 12.5% LTCG, 20% STCG — applied uniformly across the entire backtest period, with no historical tax-rate table. Tax is computed once per Financial Year (April 1 – March 31) on that FY's **net realized profit** (gains netted against losses within the FY, per actual Indian tax treatment), and modeled as a single cash outflow from the strategy's capital base on the last day of the FY (March 31). This outflow is a dated cash-flow event feeding XIRR like any SIP contribution, just negative and once-a-year.
- **Horizon-bucket position-sizing percentages**: confirmed 2026-07-20 — use the plan's starter defaults (5-day 2%/15% sector, 21-day 3%/20%, 63-day 4%/20%, 1-year 5%/25%, MultiBagger 5% with a longer min-hold/25% sector), implemented as overridable config in `backtest/core/horizon.py`'s `HORIZON_SIZING`, not hardcoded — a strategy operator can override per run via `sizing_for(bucket, overrides=...)`. **Implemented and tested** (`backtest/core/horizon.py`, `tests/unit/test_core_horizon.py`).
- **Walk-Forward retraining cadence**: confirmed 2026-07-20 — varies by horizon bucket rather than a flat monthly cadence for every strategy. Each bucket's `default_rebalance_cadence_days` in `horizon.py` doubles as its retrain cadence (5-day → every 5 trading days, 21-day → ~monthly — matching the user's original "learn based on signals generated that month" example, 63-day → ~quarterly, 1-year/MultiBagger → annual/event-driven). Phase 2.5's `WalkForwardRunner` reads this from `horizon.py` rather than taking a separate cadence parameter, so sizing and retraining stay driven by one shared table.
- **Fundamental channel's pre-2020 data gap**: confirmed 2026-07-20 — reject rather than silently run. `BacktestRun.__post_init__` now hard-blocks any `channel="fundamental"` run with `start_date` before **2020-01-01**, raising a `ValueError` explaining the real-data gap (Known Data Gaps #1) rather than allowing a backtest to complete on near-empty data and produce a misleadingly clean equity curve. **Implemented and tested** (`backtest/core/run_context.py::FUNDAMENTAL_MIN_START_DATE`, `tests/unit/test_core_run_context.py`).

## Architecture

Replace today's 3.5 independent implementations with a shared core + thin per-channel adapters:

```
backtest/
  core/
    run_context.py   # BacktestRun: run_id, parent_run_id, channel, strategy_id, horizon_bucket,
                      #   universe, date_range, capital_mode (lump/SIP), config_hash
    horizon.py        # NET-NEW: HorizonBucket enum {D5, D21, D63, Y1, MULTIBAGGER, CUSTOM}
                      #   + per-bucket default holding-period / rebalance cadence / max-position-pct,
                      #   used by all 4 channel adapters and reconciled with ml_signals'
                      #   exit_survival_5d/21d/63d columns and multibagger's SURVIVAL_HORIZONS_MONTHS
    portfolio.py       # REFACTORED from backtest/portfolio.py: add SIP injection (generalize
                      #   momentum_backtest.py's _monthly_injection_dates()/cash_flows pattern),
                      #   unified position sizing off horizon.py's table, cash-position tracking,
                      #   MAX_POSITION_PCT/MAX_SECTOR_PCT enforced identically for all channels
    metrics.py         # NET-NEW unified module: cagr (calendar/365.25, primary) + xirr (generalized
                      #   from momentum_metrics.xirr()) + legacy trading-day cagr (secondary field)
                      #   + final_capital + n_stocks_churned (generalized churn_factor) + win_rate +
                      #   profit_factor + max_drawdown + cash_position_series + benchmark_cagr/
                      #   excess_return (explicitly None + flagged pre-2023)
    costs.py           # ALREADY SHARED (IndianTransactionCosts) — no change
    tax.py              # NET-NEW, generalized from momentum_tax.py: fixed-rate FY-end tax engine
                      #   (12.5% LTCG / 20% STCG on net FY realized profit, charged as a cash
                      #   outflow every March 31) — reused by every channel, not momentum-only
    feature_log.py     # NET-NEW: per-decision feature vector logger, see Feedback Loop below
    engine.py           # NET-NEW shared orchestrator (walk-forward fold loop, trade execution,
                      #   calls adapter.generate_signals() + core.portfolio + core.metrics +
                      #   core.feature_log), built fresh rather than refactored out of
                      #   backtest/engine.py — see "engine.py: wrap, don't refactor" below.
  adapters/
    ml_adapter.py             # WRAPS the existing backtest/engine.py::BacktestEngine as-is —
                              #   backtest/engine.py itself is NOT modified (confirmed 2026-07-20,
                              #   see below). ml_adapter.py is a thin StrategyAdapter-conforming
                              #   caller into the existing, unmodified class.
    technical_adapter.py       # NET-NEW: fold-based equity-curve backtest from screener/indicator
                              #   signals; strategy_confidence.py kept as a separate cross-check,
                              #   not deleted
    fundamental_adapter.py     # NET-NEW: first backtest capability this channel has ever had
    momentum_adapter.py        # REFACTORED from momentum_backtest.py: keep trailing_momentum_from_panel
                              #   signal logic, delegate portfolio/SIP/metrics to core/
  paper_trading/
    live_runner.py    # NET-NEW: same adapters + core.portfolio, fed live daily data instead of
                      #   historical panels, writes to the same run-record schema with mode="paper",
                      #   reuses portfolio_state.py persistence and Gate-7 90-day counting logic,
                      #   generalized to all 4 channels
    approval_queue.py  # generalized from paper_trading_step.py's pending/{date}.json pattern
```

**Adapter contract** every channel implements:
```python
class StrategyAdapter(Protocol):
    channel: Literal["technical", "fundamental", "ml", "momentum"]
    def generate_signals(self, universe, as_of_date, horizon_bucket) -> list[Signal]
    def feature_vector(self, ticker, as_of_date) -> dict  # for feature_log.py
```

### `backtest/engine.py`: wrap, don't refactor (confirmed 2026-07-20)

The original Phase 1 plan called for extracting orchestration logic out of `backtest/engine.py` (a live module — its `BacktestEngine` backs the existing `/ml-backtest` frontend page and `run_phase{1,2,3}_backtest.py`) into the new shared `core/engine.py`, leaving only ML-specific logic behind. That's real risk for uncertain near-term benefit: it requires touching working code, plus golden-file regression proof it behaves identically afterward, before a single new channel gets any value from it.

**Revised approach**: `backtest/engine.py` is **not modified at all**. `core/engine.py` (the new shared orchestrator) is built fresh and used only by the three genuinely new adapters — Technical, Fundamental, Momentum. `adapters/ml_adapter.py` is a thin `StrategyAdapter`-conforming wrapper that calls the existing, unmodified `BacktestEngine` as a black box. Cost: the walk-forward fold loop is duplicated between the old `engine.py` and the new `core/engine.py` until/unless ML is migrated onto the shared path later, on its own separate, lower-pressure schedule. Benefit: zero risk to the live ML backtest path in this initiative — nothing about `/ml-backtest` or `run_phase{1,2,3}_backtest.py`'s behavior changes as a side effect of building the other three channels.

### Standardized position sizing (horizon.py)

One sizing table keyed by horizon bucket, not by channel — this is the actual standardization. Exact percentages are placeholders needing final sign-off during Phase 0, but the shape is:

| Horizon bucket | Max position % of capital | Max sector % | Default rebalance cadence |
|---|---|---|---|
| 5-day | ~2% | 15% | daily/every 5d |
| 21-day | ~3% | 20% | monthly |
| 63-day | ~4% | 20% | quarterly |
| 1-year | ~5% | 25% | annual review |
| MultiBagger | ~5%, longer min-hold | 25% | event-driven (quality-gate re-eval) |

### Feature-vector logging & feedback loop

New DuckDB table `backtest_feature_log` (run_id, ticker, as_of_date, horizon_bucket, feature values, signal_output, decision_taken), written incrementally during every run — not just summary dataclasses like today's `FoldResult`/`Trade`. Flow: run → adapter logs feature vector at every decision → researcher queries `backtest_feature_log` joined to `BacktestRunResult` to find losing trades → re-engineers features / retrains offline → reruns with a new `run_id` carrying `parent_run_id` so the UI can do a "compare to parent run" view. Optionally also export a parquet snapshot per run_id for offline retraining portability.

## Signal generation for 20-year backtests

Both existing engines already compute signals **live** from raw OHLCV at run time — neither depends on a pre-materialized signals table, so no 20-year signal backfill is required for Technical/ML/Momentum in general. Two exceptions requiring real engineering work:
- **MultiBagger/forensic-gated strategies**: `ml_multibagger`/`ml_forensic` tables only hold a few weeks of data and aren't wired into any live-compute path. A live-compute path for quality_scores/quality_gate must be added to the ML adapter before MultiBagger-horizon backtests can run over meaningful history — its own gated sub-task in Phase 2, not a Phase 2 blocker for the other adapters.
- **Shareholding-dependent signals**: `shareholding` table only goes back to 2016 — any strategy depending on it is capped at ~10 years regardless of channel.

## Walk-Forward Module (distinct from backtest fold-splitting, and from paper trading)

This is a third mode, sitting between Backtest and Paper Trading, and must be its own module — not folded silently into either:

- **Backtest** (§ Standard Backtesting Algorithm, step 2) uses walk-forward *fold splitting* purely as a statistical validation technique (train/test splits over historical data, all computed in one run, used to check the strategy isn't overfit).
- **Walk-Forward module** (this section) is a full **historical forward-replay**: pick a start date (e.g. 2010-01-01), and step forward period-by-period (the re-learn/rebalance cadence is horizon-bucket-dependent — e.g. monthly for a 21-day strategy) as if living through that history one period at a time. At each step: only data available up to that point is used, the model/rule is retrained or refreshed using signals generated in that specific period (matching the user's example — "start doing trades and start learning based on the signals generated in that specific month"), a trade decision is made, and the run moves to the next period. It never sees the future at any step, mirroring exactly what paper trading will do live, but replayed against history so it can span 2010→today in minutes instead of 15 years.
- **Paper Trading** (Phase 5) is the same day-driver loop as Walk-Forward, except the "next period's data" comes from the live daily pipeline instead of a historical replay, and each proposed action needs human approval before execution (Gate-7 policy).

**Architectural implication**: Walk-Forward and Paper Trading should share one **day-driver** implementation (`core/day_driver.py`, net-new) parameterized by a `DataSource` (historical-replay vs live-feed) and an `approval_mode` (auto vs human-gated) — not two separate re-implementations of "advance one period, retrain if due, generate signals, size positions, execute, log." This also means the model-retraining hook used by Walk-Forward is the exact same one Phase 6's fine-tuning loop (below) will drive — Walk-Forward is effectively "fine-tuning dry-run mode."

```
backtest/
  walk_forward/
    day_driver.py     # NET-NEW, shared with paper_trading/live_runner.py: advance one period,
                      #   retrain-if-due, generate signals, size, execute, log — parameterized by
                      #   DataSource (historical | live) and approval_mode (auto | human-gated)
    runner.py          # NET-NEW: WalkForwardRunner — start_date, retrain_cadence (per horizon
                      #   bucket), replays day_driver.py against historical data end-to-end,
                      #   writes to the same BacktestRun/BacktestRunResult schema with mode="walk_forward"
```

**Retraining granularity**: retrain cadence is horizon-bucket-driven by default (e.g. every rebalance date for 5-day/21-day strategies, quarterly for 63-day, annually for 1-year/MultiBagger) but overridable per run — this needs confirming with the user during Phase 0 alongside the position-sizing table, since "retrain every month using that month's signals" (the user's example) implies a monthly cadence specifically, which may not match every horizon bucket's natural rebalance frequency.

**Output**: a Walk-Forward run produces the exact same metrics set as a Backtest run (CAGR, XIRR, final capital, churn, win rate, max drawdown, cash position) plus a period-by-period retraining log (which model version was active for which stretch) — this log is exactly what Phase 6's fine-tuning loop consumes to decide whether a retrain actually helped.

## Truthful Review: Gaps in the Original Plan (added after codebase verification)

The original plan's "Known Data Gaps" section covered depth/density issues but missed several **correctness** issues that would make backtest results silently wrong rather than just incomplete. Verified against the actual code — these are not hypothetical:

1. **Survivorship bias is real and currently active in every backtest.** A partial fix exists — `ingestion/scrapers/nse_delisted_companies.py` populates a `delisted_companies` table, and `config/build_universe.py::build_historical_universe_from_delisted` can union it in — but it's gated behind an `include_delisted=False` default in `features/momentum_universe.py`, and **no caller anywhere in the codebase passes `include_delisted=True`**. Every existing backtest (including momentum's published results) runs against the current-snapshot Nifty500 CSV, which by construction excludes every company that has since delisted, merged, or fallen out of the index — inflating returns. This must be fixed as part of Phase 1/2, not treated as a future nice-to-have, since it invalidates comparisons against any results produced before the fix.

**Update 2026-07-20 (done — wired through, not just available):** every real backtest-purpose caller of `features/momentum_universe.py` now passes `include_delisted=True`: all 7 `scripts/run_momentum_*.py` experimentation scripts, `systems/copilot/backtest_bridge.py`'s Copilot backtest path, and (via the fixed `yearly_band_universes()`, which previously had **no** `include_delisted` parameter at all — a real bug, not just an unused convenience) any future caller of that wrapper. `features/momentum_live.py`'s live daily-ranking call is deliberately left at the default `False` — documented inline why: `include_delisted=True` is only correct for a *historical* as_of_date, since a stock already delisted as of "today" would otherwise wrongly earn a live rank-band slot off its frozen last-known price.

**A second, more serious bug was caught while testing this fix**, not by inspection: `build_historical_universe_from_delisted()` always opened its own read-write DuckDB connection to `DUCKDB_PATH`, but in production it's invoked from inside `full_rank_universe()`, which already holds an open `read_only=True, persist=False` connection to that **same file**. DuckDB permits one read-write connection OR many read-only connections per file, never both — so the instant `include_delisted=True` was actually exercised against a real database, it would have thrown `Connection Error: Can't open a connection to same database file with a different configuration than existing connections`, in test and in production alike. This had never fired because nothing had ever called it with `include_delisted=True` until this fix. Resolved by giving `build_historical_universe_from_delisted()` an optional `conn` parameter so it reuses the caller's already-open connection instead of ever opening a second one; `_all_candidate_tickers`/`full_rank_universe` now pass `normalised_conn` through automatically — no new parameter needed on any of the outer functions.

Tests: 6 new (`tests/unit/test_momentum_universe.py::TestYearlyBandUniversesIncludeDelisted` — default-excludes, include_delisted=True-surfaces-it, and the connection-reuse regression test; `tests/unit/test_build_historical_universe.py::test_conn_param_reuses_caller_connection_instead_of_opening_a_new_one`), all against real seeded DuckDB tables (a real `delisted_companies` row with a real historical price), no mocks. 88 tests passing across the full momentum/copilot-bridge test surface, no regressions; quality-gate failures confirmed pre-existing/unrelated.

**Still open**: this only fixes the *candidate pool*. Gap #5 (no point-in-time index/universe *membership* — "who was actually in Nifty500 on a past date," not just "who hasn't delisted") remains unaddressed; the user separately noted 2026-07-20 that real historical Nifty index values/constituent data can be sourced from niftyindices.com, which is worth investigating as the real fix for Gap #5 rather than the "accepted approximation" the plan currently documents.
2. **Fundamentals are not truly point-in-time — restatements overwrite in place.** `enforce_pit_fundamentals` correctly filters by `announcement_date <= as_of`, but the underlying `fundamentals` table has no versioning key (PK is `ticker, fiscal_year, quarter`); a later-corrected filing overwrites the original row via upsert. An `as_of_ingested` audit column was added but explicitly does not preserve the original as-reported values. Net effect: a Fundamental-channel backtest run today "sees" restated numbers for old quarters that were not actually known to a trader at that historical date — this is lookahead bias baked into the data layer, not the backtest logic. Flag as a data-architecture fix (a `fundamentals_history`/append-only variant), separate from and likely blocking trustworthy Fundamental-channel backtests.
3. **Rights-issue adjustment is ingested but not applied automatically.** `price_adjuster.py`'s `_action_factors()` is a no-op for RIGHTS; the real adjustment logic (`rights_adjuster.py`) only runs via a manually-invoked script. Any backtest spanning a rights issue will see an artificial price discontinuity unless someone remembered to run the script for that ticker. Needs wiring into the standard adjustment pipeline before rights-affected tickers can be trusted in long-horizon backtests.
4. **No merger/spin-off handling at all.** The `corporate_actions.action_type` enum doesn't even include MERGER or SPINOFF, and `backtest/portfolio.py` has no logic for what happens to an open position when a held company disappears mid-backtest (delisting, acquisition). Today this likely fails silently (position just stops updating) or crashes. Needs an explicit policy (e.g. mark-to-last-price-and-close, or model the swap ratio if available) before 10-20yr backtests, where this will happen dozens of times, can be trusted.
5. **No point-in-time index/universe membership.** There's no `index_membership`/constituents table — "the universe" is always today's Nifty500 list applied retroactively, not "who was actually in Nifty500 on 2015-01-01." This compounds survivorship bias: even with delisted names added back, the universe still isn't historically accurate (a stock that was mid-cap in 2012 and only entered Nifty500 in 2020 gets treated as always-eligible, or vice versa). This is a larger data-sourcing problem (historical index constituent lists aren't NSE's free public data) — the plan should flag it as an accepted approximation rather than silently ignore it, and document the resulting bias direction (tends to overstate returns by including future winners earlier than an investor could have known to buy them).

**Update 2026-07-20 (decided methodology, not just an approximation anymore):** the user confirmed the intended fix directly — at every rebalance date, rank the full candidate pool (today's active universe UNION real `delisted_companies` tickers, per today's Gap #1 fix) by real PIT market cap, and take the top N as that date's index-membership proxy. This is exactly what `RANK_BANDS` (rank 1-50 as a "Nifty 50" proxy, etc.) already did for Momentum — the only gap was that nothing generalized it past rank 200 or exposed it for other channels. Added `features/momentum_universe.py::nifty500_proxy_universe()` / `yearly_nifty500_proxy_universes()` — thin named wrappers around the existing `rank_band_tickers`/`yearly_band_universes` machinery with `rank_end=NIFTY500_PROXY_RANK=500` and `include_delisted=True` as the default (the one function in this module where defaulting to `True` is correct, since being a historical-membership proxy is its entire purpose). Confirmed via a real test that `rank_band_tickers`'s own `max(rank_end, MAX_TRACKED_RANK)` already lifts the 200 cap with no code change needed — verified against 250 real seeded tickers, all correctly ranked. This closes Gap #5's methodology question for good; what's still genuinely open is *wiring* — Technical/Fundamental backtests via the new unified `BacktestOrchestrator` don't yet have a `universe_provider` that calls this (Momentum is the only channel whose universe construction was ever built out this way); that's a real but separate integration task, not a data-sourcing problem anymore.

The user separately flagged niftyindices.com and, later, `github.com/hotessy/nse-historical-data` as possible sources of real historical Nifty data — investigated and clarified these were actually about the separate Benchmark-gap item (real historical index *values*, not constituent *membership*); see that item's 2026-07-20 update in the Context section above for the outcome (real fix, not this gap). Neither source offers constituent/membership lists, so Gap #5 specifically stays answered by the market-cap-ranking proxy (`nifty500_proxy_universe`) below, not by either of those leads.
6. **No liquidity/capacity constraint.** `position_size()`/`can_buy()` cap by portfolio-% and sector-%, but `adtv_cr` only feeds cost/slippage modeling, never a hard position-size cap. A backtest can currently "buy" ₹5L of a stock trading ₹2L/day of volume with only a cost penalty, not a rejection — unrealistic for small/mid-cap strategies at scale. Should add an ADTV-based cap (e.g. position ≤ X% of trailing ADTV) to `core/portfolio.py` in Phase 1.
7. ~~No multi-channel combined-portfolio backtest.~~ **Resolved — by design, not a gap.** Confirmed: strategies never share a capital pool. Each channel × horizon-bucket strategy always backtests, walk-forward-tests, and paper-trades against its own dedicated capital base. `core/portfolio.py` is scoped to exactly one strategy per run; no blended/multi-adapter portfolio concept should be built, now or later.
8. **Risk-adjusted metrics beyond Sharpe/max-drawdown are absent from the metrics list.** The user's required list (CAGR, XIRR, final capital, churn, win rate, max drawdown, cash position) doesn't include Sortino or Calmar ratio, which matter more than Sharpe for equity strategies with fat left tails (a real risk here given small/mid-cap Indian equities). Recommend adding both to `core/metrics.py` as secondary fields — cheap to compute once the equity curve exists, and useful in the results view.
9. ~~Tax-regime changes over a 20-year window are not modeled.~~ **Resolved — by design, not a gap.** Confirmed: use fixed current-day rates (12.5% LTCG, 20% STCG) uniformly across the whole backtest period, no historical tax-rate table. Tax computed once per Financial Year (April 1 – March 31) on that FY's net realized profit, charged as a cash outflow on March 31 of each FY — see Context decisions above and `core/tax.py` in the architecture section below.
10. **No reproducibility/determinism guarantee.** ML-adapter backtests involve model training with random initialization; nothing in the plan specifies seeding or reproducibility guarantees for a rerun to be comparable to its `parent_run_id` baseline. Should be a one-line requirement in Phase 1: every `BacktestRun` records the random seed(s) used, and reruns with an unchanged config + seed must produce bit-identical results (a good regression test for the Phase 1 refactor, too).
11. **Precise definition of "stocks churned" is unspecified.** The plan and the user's ask both say "churn"/"how many stocks churned" without defining it — distinct-tickers-traded over the period? Turnover ratio (value bought+sold / average portfolio value)? Should be pinned down in `core/metrics.py`'s spec before implementation; recommend both (a `n_distinct_tickers_traded` count and a `turnover_ratio`) since they answer different questions.

## No-Mock-Data Policy for Backtest + Backtest Testing

This project already enforces a zero-stub/zero-synthetic-data policy via `tests/quality/` (see prior fixes to `engine.py`, stub API endpoints, and `registry.py`). That policy is extended explicitly to this entire initiative, in both directions:

- **The backtest/walk-forward/paper-trading engines themselves must never fall back to mock, synthetic, or fabricated data** — no placeholder prices, no fabricated fundamentals, no fake corporate-action factors, no synthetic universe lists. If required data is missing for a given date/ticker (e.g. a gap in `ohlcv_adjusted`, a missing `index_ohlcv` benchmark point before 2023, a ticker with no `delisted_companies` record), the correct behavior is to **exclude that ticker/period and record the exclusion**, not to interpolate, default, or synthesize a value. Every `BacktestRunResult` should carry a `data_gaps` field logging what was excluded and why, so results are never silently computed over invented data.
- **Tests of the backtest module must run against real historical data, not fixtures/mocks of it.** Unit tests for `core/portfolio.py`, `core/metrics.py`, `core/tax.py` sizing/math logic may use small hand-computed numeric fixtures (e.g. "given these five prices, CAGR should equal X") since that's testing arithmetic, not market realism — but integration/regression tests for adapters, the day-driver, and end-to-end backtest/walk-forward runs must execute against real slices of `alphalens.duckdb`/`signals.duckdb` (e.g. a fixed real date range and real ticker list), consistent with the existing project rule of never inserting synthetic rows into the real DuckDB for verification — use a read-only real-data slice or an isolated copy of real data, never fabricated rows.
- **No mocking the data layer in adapter tests.** Adapters must not be tested against a mocked `DataStore`/DB client that returns invented OHLCV/fundamentals — that would validate the adapter's logic against data shapes that might not match reality (e.g. missing columns, unexpected NaNs from real gaps) and defeats the purpose of catching the Truthful Review gaps (#1-6) in testing. Where isolation from the live DB is needed for speed, use a real point-in-time snapshot/export of actual historical data as a frozen test fixture, not synthetic values.
- This is enforced the same way the existing policy is: `tests/quality/` gets new checks scoped to `backtest/`, `adapters/`, `paper_trading/`, and `walk_forward/` verifying no stub/mock/random-data patterns are present in either the production code paths or the test suite covering them.

## Known Data Gaps to Flag (not silently assumed away)

1. **Fundamentals density audited 2026-07-20 — worse than assumed, blocking for long-horizon Fundamental backtests.** Real row counts by year (from `fundamentals.announcement_date`, isolated read-only copy of `alphalens.duckdb`): 2005-2019 combined = 186 rows total across all tickers (e.g. 2015: 11 rows/5 tickers, 2018: 33 rows/15 tickers); coverage only becomes usable from **2020 onward** (2020: 1,842 rows/1,746 tickers; 2021+: 7,000-10,000+ rows/1,800-2,900 tickers/year). Null rates for `total_equity` are also high even in the usable era (61.7% post-2012, vs 12.5% pre-2012 on a tiny sample) and 3.9% of post-2012 rows fail `quality_flag`. **Conclusion: the Fundamental channel cannot support genuine 10-year or 20-year backtests today — real usable history is ~6 years (2020+).** This must be surfaced as a hard constraint, not a caveat: Fundamental-channel backtest UI/API should reject or clearly warn on any requested window starting before 2020, rather than silently running on near-empty data and returning a misleadingly clean-looking equity curve.
2. **OHLCV clean start date confirmed: 2006-01-01.** 2005 has only 5,926 rows / 24 tickers (unusable — likely partial-year backfill artifact); 2006 onward is consistently dense (214,690 rows/1,104 tickers in 2006, rising to 350k+ rows/1,600+ tickers by 2011 and staying in that range through the present). Technical/ML/Momentum backtest start dates should clip to **2006-01-01**, not the nominal 2005-01-03 table minimum, and any long-lookback indicator (e.g. 200-day MA) should additionally pad its effective start by the lookback window to avoid partial-window corruption right at the 2006 boundary.
3. `mf_holdings` (2 months only) is out of scope for any long-horizon backtest.
4. Canonical CAGR change is a breaking change for `run_phase{1,2,3}_backtest.py` output and the existing `/ml-backtest` page — old field kept alongside new during migration, not replaced in place.

## Phased Rollout

**Phase 0 — Data prerequisites & gap audit (no code) — DONE 2026-07-20**
Fundamentals density audit by year: **done, see Known Data Gaps #1** — usable Fundamental history is 2020+ only, not 20 years. OHLCV clean start date: **confirmed 2006-01-01**, see Known Data Gaps #2. Horizon-bucket position-sizing percentages: still using plan defaults (§ Standardized position sizing) pending final user sign-off — implemented as overridable config, not hardcoded, so this doesn't block Phase 1 start.
*Tests*: the audit scripts themselves are throwaway/one-off, but their output (verified-clean start date, density thresholds) becomes a checked-in constant consumed by Phase 1's tests — add a regression test asserting real row counts in `ohlcv_adjusted`/`fundamentals` for the chosen start date still meet the documented density threshold, so a future data regression is caught automatically rather than silently degrading 20-year backtests.

**Phase 1 — Shared core abstraction — DONE 2026-07-20**
Built `backtest/core/` (`run_context.py`, `horizon.py`, `tax.py`, `metrics.py`, `portfolio.py`, `feature_log.py`, `engine.py`) plus `backtest/adapters/ml_adapter.py` and Store 6 (`datastore/schema/create_backtest.py`, `config/settings.py::BACKTEST_DUCKDB_PATH`). Per the confirmed 2026-07-20 decision (§ "`backtest/engine.py`: wrap, don't refactor"), `backtest/engine.py` was verified untouched throughout (`git diff --stat backtest/engine.py` — empty).

**What each piece does**:
- `run_context.py` — `BacktestRun`/`BacktestRunResult`, the Fundamental pre-2020 hard guard, `config_hash` for reproducibility.
- `horizon.py` — `HorizonBucket` + the confirmed position-sizing table, overridable per run.
- `tax.py` — FY-netted 12.5%/20% LTCG/STCG engine, March-31 cash-flow events.
- `metrics.py` — the unified CAGR/XIRR/Sortino/Calmar/win-rate/turnover/benchmark-flagging module.
- `portfolio.py` — `StrategyPortfolio`: horizon-bucket sizing, SIP injection, ADTV hard cap, min-holding-days enforcement, `force_close()` for delisting reconciliation, `tax_transactions()` bridge.
- `feature_log.py` — `FeatureLogWriter`, batched writes to `backtest_feature_log`, `query_feature_log()` read side.
- `engine.py` — `BacktestOrchestrator`, implementing the Standard Backtesting Algorithm end to end (point-in-time universe hook, rebalance-date iteration by horizon-bucket cadence, delisting reconciliation before new signals, SIP injection, cost-aware buy/sell, data-gap recording instead of fabrication, feature logging, final metrics via `core/metrics.py`) — used by the three genuinely new Phase 2 adapters, NOT by ML.
- `adapters/ml_adapter.py` — a **result-schema translator**, not a `StrategyAdapter`: `BacktestEngine.run_full_backtest()`'s existing self-contained walk-forward output is mapped onto the shared `BacktestRunResult` shape (documented field gaps — XIRR/Sortino/Calmar/turnover/tax are `None`, not fabricated, since `BacktestEngine`'s own pipeline doesn't compute them) so Phase 3's unified UI can list an ML run in the same table as the other three channels without touching `BacktestEngine`'s internals.

186 tests passing across `tests/unit/test_core_*.py`, `test_schema_backtest.py`, `test_ml_adapter.py`, plus the full existing backtest suite (118 tests) still green — no regressions, verified against `tests/quality/`'s no-stub/no-synthetic-data gate (one unrelated pre-existing failure in `features/regime_signal.py`, not touched by this work).
*Tests*: all done — arithmetic-fixture unit tests for `metrics.py`/`tax.py`/`horizon.py` (No-Mock-Data Policy's carve-out for pure formula testing); orchestration-mechanics tests for `engine.py` using deterministic flat-price fixtures (same convention as the existing `tests/unit/test_momentum_backtest.py::_flat_price_panel`) covering no-op runs, buy/sell sequencing, data-gap recording instead of fabrication, delisting force-close, SIP total_contributed growth, and feature-log integration against a real (in-memory) DuckDB table; `ml_adapter.py` tested against a real `BacktestResults` instance (the actual dataclass type it consumes, not a mock).

**Phase 2 — Per-channel adapter migration — DONE 2026-07-20**
Built all three, consistent with the "wrap, don't refactor" principle: none of `backtest/momentum_backtest.py`, `systems/technical_analysis/screener/engine.py`, or `features/fundamental_composites.py` were modified (`git diff --stat` on all three — empty).
- `momentum_adapter.py` — a genuine `StrategyAdapter` (unlike `ml_adapter.py`): momentum ranking is stateless-per-date, so it drives `core/engine.py` directly. Reuses `features/momentum_signal.py::trailing_momentum_from_panel` (already pure/in-memory). `backtest/momentum_backtest.py`'s standalone `MomentumBacktester` is left in place, continuing to serve its existing callers (external published artifacts, `scripts/run_momentum_experimentation.py`) unchanged — the two coexist rather than one replacing the other outright.
- `technical_adapter.py` — the Technical channel's first real fold-based backtest (`strategy_confidence.py`'s historical win-rate lookup stays as a separate cross-check, not replaced). Discovered during this build: the daily feature Parquet store (`config.settings.FEATURES_DAILY_DIR`) has real materialized coverage from **2007-01-03 to today (4,837 files)** — much better than assumed, meaning Technical backtests don't need any live-recompute path; they read the same snapshots the production `/screener/run/{template}` endpoint reads, guaranteeing zero drift between backtested and live signal logic.
- `fundamental_adapter.py` — the Fundamental channel's first backtest capability at all. Reuses the real `SCREENER_PRESETS`/`matches_screener_preset` logic backing the live `/api/v1/fundamental/screener` endpoint. Relies on the already-built `FUNDAMENTAL_MIN_START_DATE` guard (Phase 1) rather than adding its own date-gating.

All three adapters share the same buy/sell rotation pattern (hold while matching, sell when it drops out) and were proven to plug into `core/engine.py`'s `BacktestOrchestrator` end to end (see `TestOrchestratorIntegration` in `test_momentum_adapter.py`).

237 tests passing (36 new this phase) across the adapter test files plus the full existing suite — no regressions, quality gate clean (same pre-existing unrelated `regime_signal.py` item).
*Tests*: deterministic-fixture tests for rotation mechanics (same `_flat_price_panel`-style convention as the existing `test_momentum_backtest.py`) for each adapter, plus one real-data integration test per adapter (real OHLCV via `load_price_panel` for Momentum, the real `ScreenerEngine` + real Parquet store for Technical, the real feature store for Fundamental) — all pass or skip cleanly on a DB lock, never fall back to fabricated data.

**Phase 2.5 — Walk-Forward module — DONE 2026-07-20**
Built `backtest/walk_forward/runner.py`'s `WalkForwardRunner`. **Design simplification discovered while building**: the originally-planned separate `day_driver.py` (shared with Phase 5's `paper_trading/live_runner.py`) turned out to already be `core/engine.py`'s `BacktestOrchestrator` — "advance one period, retrain-if-due, generate signals, size, execute, log" is exactly its existing loop, so rather than duplicate it, `BacktestOrchestrator` gained an optional `refit()` hook (called on adapters that implement it, at a configurable cadence — defaulting to the horizon bucket's own rebalance cadence, confirmed 2026-07-20 as varying per bucket rather than flat-monthly) and a `refit_log` output field. `WalkForwardRunner` is a thin `mode="walk_forward"`-validating wrapper around the same orchestrator. Phase 5's `live_runner.py` will follow the same pattern (live `DataSource` + human-approval gate layered on the same orchestrator) rather than a third independent loop implementation.
*Tests*: the mandatory **lookahead-leakage test** — done, two variants: (1) a single-period test fuzzing all price data after `as_of_date` to an absurd value and confirming the signal at `as_of_date` is unchanged; (2) a full-run test comparing a truncated-at-boundary clean panel against a truncated-at-boundary fuzzed panel and confirming identical holdings/metrics at the boundary. Both pass. Also tested: refit fires at the configured cadence and is a no-op (empty `refit_log`) for adapters without a `refit()` method (all three Phase 2 adapters today — none has learnable state yet, this is the extension point a future ML-style adapter uses). 242 tests passing total, no regressions, quality gate clean, all four live modules still untouched.

**Phase 3 — Unified run records + API — DONE 2026-07-20**
Built `backtest/core/run_store.py` (`save_run_result`/`get_run`/`list_runs`/`get_run_lineage`, idempotent upsert on `run_id`) and a NEW router `datastore/api/routers/backtest_runs.py` (`GET /api/v1/backtest/runs`, `/runs/{id}`, `/runs/{id}/lineage`, `/runs/{id}/feature_log`), registered in `main.py` alongside the existing `backtest_reports` router under the same `/api/v1/backtest` prefix but disjoint sub-paths (`/runs` vs `/reports`) — consistent with "wrap, don't refactor": `backtest_reports.py` itself is untouched (`git diff --stat` empty), so `/ml-backtest` keeps working unchanged. `main.py`'s only diff is the 2 additive lines every router registration requires. No write endpoints: runs are written by `run_store.py` from wherever a `BacktestOrchestrator`/`WalkForwardRunner` run is kicked off, not from an HTTP request — deliberately keeping "who can trigger an expensive multi-year backtest" out-of-band rather than an open POST endpoint (left for Phase 5/6's background job runner to formalize). The `live_eligible` Phase 6 safety column is exposed read-only in every response and never settable through this router.

**Update 2026-07-20 (open item #4, done):** `run_phase{1,2,3}_backtest.py` now dual-writes into the unified schema. Built `backtest/adapters/ml_dual_write.py` — an additive, best-effort helper that takes the real, already-produced `BacktestResults` object each script computes, translates it via the existing unmodified `ml_adapter.py`, and persists it via the existing unmodified `run_store.py`. Wired into all three scripts with a 6-8 line additive diff each (`git diff --stat` confirms no other lines touched, `engine.py`/`portfolio.py` untouched). Horizon-day-to-bucket mapping: 5→5_day, 21→21_day, 63→63_day (the three horizons these scripts actually run). Failures inside the dual-write (DB lock, unmapped horizon, etc.) are logged and swallowed, never raised — a report script's primary job (printing the gate result, writing its JSON report) can never break because of this being best-effort bookkeeping. 5 new tests in `tests/unit/test_ml_dual_write.py` using a real `BacktestResults` instance and a real in-memory `backtest_runs` table (round-trip via `list_runs`, horizon mapping, unmapped-horizon no-op, date-range derivation, exception-swallowing). 195 backtest-umbrella tests passing, no regressions, quality-gate failures confirmed pre-existing/unrelated (verified via `git stash`).

271 tests passing (24 new this phase: 13 for `run_store.py`, 11 for the router, using a real `TestClient(app)` against a real in-memory `backtest_runs` table, not mocked responses), no regressions, quality gate clean, all live modules (`backtest_reports.py`, `backtest/engine.py`, `momentum_backtest.py`, `ScreenerEngine`, `fundamental_composites.py`) still untouched.
*Tests*: API contract tests for every endpoint against a real persisted run in a real (in-memory) `backtest_runs` table — done, including 404s, channel/mode/strategy_id filtering, `parent_run_id` lineage chains, and `data_gaps`/`live_eligible` round-tripping. Regression-tested that the existing `backtest_reports.py` router/tests are unaffected by the new router's registration.

**Phase 4 — Frontend unified "Backtest" menu item — DONE 2026-07-20**
Built `frontend/src/shared/api/backtest.ts` (typed client for Phase 3's `/api/v1/backtest/runs*` endpoints — a separate module from the existing `backtest_reports`-backed types, since it's a different API surface) and `frontend/src/pages/backtest/BacktestPage.tsx`: a run list (filterable by channel/mode) + click-to-expand run detail showing the full metrics set (CAGR/XIRR/final capital/max drawdown/win rate/Sortino/Calmar/turnover/distinct-tickers-traded/benchmark-status), the `data_gaps` table (visible, not hidden — reinforces the No-Mock-Data Policy in the UI itself), the `parent_run_id` lineage chain, and the feature-log inspector (paginated to the first 100 rows client-side). New top-level nav entry (`frontend/src/lib/ui/nav.ts`, sibling to Technical/Fundamental/ML Signals/Momentum, NOT nested under ML) and route (`/backtest` in `router.tsx`). Per the "wrap, don't refactor" pattern applied throughout: `frontend/src/pages/ml/backtest.tsx` and the legacy `backtest_reports.py`-backed flow are left completely untouched, coexisting rather than being retired in this pass — deferred, same reasoning as leaving `run_phase{1,2,3}_backtest.py`'s dual-write undone in Phase 3.

**No run-configuration/trigger UI was built** — consistent with Phase 3's API design decision (no POST/trigger endpoint), triggering a potentially expensive multi-year backtest run stays an out-of-band decision (a script today, Phase 5/6's background job runner later), not something exposed to a browser click yet.

`tsc --noEmit` clean, `oxlint` clean, production `npm run build` succeeds. Manually verified in a real browser (Vite dev server + Playwright screenshot): the nav entry renders as a correct top-level sibling section, the page mounts and routes correctly, and — since the route was checked against a live API process that hadn't yet loaded this session's new backend code — the page's error-handling path was exercised for real: it shows an honest "could not reach" message rather than any fabricated placeholder rows, which is itself the No-Mock-Data Policy's requirement showing up correctly in the UI layer. Full request/response contract was independently verified via Phase 3's `TestClient`-based router tests against a real (in-memory) `backtest_runs` table (11 passing).
*Tests*: contract-level coverage via Phase 3's router tests (real TestClient, real table, not mocked).

**Update 2026-07-20 (open item #1, done):** the live-data round-trip is now verified. Attempted first via `run_phase1_backtest.py` (which now dual-writes per open item #4) — this surfaced a real, pre-existing blocker unrelated to today's work: `ExitSignalModel` training requires ≥200 real closed paper-trading positions, and only 3 exist today (`systems/ml_signal_engine/models/exit/exit_signal.py`'s own no-synthetic-fallback guard correctly refused to run rather than fabricate). So the ML channel's dual-write path is real and wired but not yet exercisable until enough real paper-trading history accumulates — noted here rather than worked around. Seeded a genuine run through the **Momentum** channel instead: `MomentumAdapter` run through `BacktestOrchestrator` over a real 2-year OHLCV panel (`RELIANCE`/`TCS`/`INFY`/etc. via `features.momentum_signal.load_price_panel`, 2018-01-01 → 2019-12-31, ₹10L lump capital), persisted via `run_store.save_run_result()` into the real `BACKTEST_DUCKDB_PATH`. Confirmed queryable three ways: directly via `run_store.list_runs()`, via `curl http://localhost:8000/api/v1/backtest/runs` after restarting the live API process (confirmed with the user first) to pick up this session's routers, and via a full Playwright browser round-trip — run list populated with the real run, click-through renders the full metrics grid (CAGR 0.3%, final capital ₹10,05,003, max drawdown -1.1%, Sortino/Calmar, 20 trades, 7 distinct tickers, honest "insufficient benchmark history" flag since the window predates `index_ohlcv`'s 2023-07 start) and the feature-log inspector (0 rows — this ad-hoc seed run wasn't given a `FeatureLogWriter`, expected). Screenshots and the one-off seeding script were kept in the session scratchpad, not committed — a repeatable seeding path belongs in Phase 5/6's actual background job runner, not a hand-rolled script.

**Phase 5 — Paper Trading as downstream stage — DONE 2026-07-20**
Built `backtest/paper_trading/approval_queue.py` (the human-approval queue, generalized from the ML-only `pending/{date}.json` pattern to be `(channel, strategy_id)`-scoped: `paper_trading/pending/{channel}/{strategy_id}/{date}.json`, `paper_trading/executions/{channel}/{strategy_id}/{date}.json`, Gate-7 counting per strategy rather than one app-wide track) and `backtest/paper_trading/live_runner.py` (`PaperTradingRunner`: `propose_today()` generates real signals via any channel's adapter and queues them — never auto-executes; `accept()` executes against a persisted `StrategyPortfolio` and advances the Gate-7 counter; `reject()` marks the decision without touching the portfolio, but still counts as a real reviewed day). New router `datastore/api/routers/paper_trading_unified.py` at `/api/v1/paper_trading2/{channel}/{strategy_id}/*` — a deliberately unglamorous prefix chosen to avoid any path collision with the existing `/api/v1/paper_trading/*` router, which is completely untouched (`git diff --stat` empty on it and every other existing ML paper-trading module: `portfolio_state.py`, `paper_trading_step.py`, `paper_trading_tracker.py`).

**Real bug caught and fixed during this build**: the router's `accept`/`reject`/`state` endpoints construct a fresh `PaperTradingRunner` per request with no memory of the strategy's horizon bucket — but creating a strategy's very first portfolio state requires one. Fixed by making `horizon_bucket`/`initial_capital` optional on `PaperTradingRunner`, required only when no persisted state exists yet (and supplied via the `accept` request body only on that first call), with an explicit `ValueError` — not a silent wrong-default — if they're needed and missing. Caught by a test before it could have caused a confusing 500 in practice.

**Momentum's manual `momentum_trades` journal fold-in**: the real migration of production rows is still NOT done — that stays a decision for the table's owner. **Update 2026-07-20 (open item #2, dry-run done):** built `scripts/migrate_momentum_paper_trading_dry_run.py`, a read-only (`read_only=True, persist=False` against the live `alphalens.duckdb` — never locks out the scheduler/API, SPEC-SCHED-013) report of how each real `momentum_trades` row would map onto the unified schema (buy/sell `PendingAction`s, distinct Gate-7-eligible trading days, total capital deployed) — writes nothing (no DB rows, no `paper_trading/` files; a dedicated test patches `Path.write_text` to prove it). It explicitly refuses to invent the two values the destination `StrategyPortfolio` requires that `momentum_trades` has no concept of — `initial_capital` and `horizon_bucket` — surfacing them as a `REQUIRED_HUMAN_DECISION` field per strategy rather than defaulting them. Run against the real live table: currently empty (0 rows), so there was nothing to migrate as of this pass — the script is ready for whenever real rows exist. 8 tests in `tests/unit/test_migrate_momentum_paper_trading_dry_run.py` against a real in-memory `momentum_trades` table (open/closed trade counts, unmappable-row flagging on NULL price data, Gate-7 day dedup, capital-deployed sum, the required-decision flag, empty-table handling, and the no-writes guarantee).

**Update 2026-07-20 (open item #3, done):** the frontend "Paper Trading" panel is now built, in the same nav section per the original plan note. Added `frontend/src/shared/api/paper_trading.ts` (typed client for `/api/v1/paper_trading2/*`) and a `PaperTradingPanel` component inside `BacktestPage.tsx` (channel/strategy_id/date selector → Gate-7 status, cash/position/trade-count state summary, and a pending-actions table with inline fill-price + Accept/Reject buttons calling `accept`/`reject` directly). Strategies with no existing state (first-ever action) show an explicit message rather than a broken form, since accepting a strategy's very first action requires a `horizon_bucket`/`initial_capital` the panel doesn't yet collect — deferred rather than silently defaulted, consistent with the backend's own explicit-error design (see Phase 5's caught bug above). `tsc --noEmit`, `oxlint`, and `npm run build` all clean; not yet verified in a live browser against real pending actions (same "no real runs yet" blocker as the main run list — see open item #1).

342 tests passing (23 new: 12 for `approval_queue.py`, 11 for `live_runner.py`, 10 for the router — using real `TestClient` calls against isolated `tmp_path` directories, never the real `paper_trading/` tree), no regressions, all existing ML paper-trading modules and tests untouched and passing.
*Tests*: full propose → accept → portfolio-updated → Gate-7-incremented cycle tested end-to-end per the router and the runner directly; reject-still-counts-as-a-day tested explicitly (matching existing ML semantics); state persistence across separate `PaperTradingRunner` instances (simulating separate process runs) tested; the horizon-bucket bug above is now a regression test (`test_accept_requires_horizon_bucket_on_first_ever_action`).

**Phase 6 — Automated Fine-Tuning / Self-Improvement Loop**
See the dedicated section below — this is a distinct system built on top of Phases 1-5, not inside them, and should not start until the backtest/walk-forward foundation is verified trustworthy (Truthful Review gaps #1-6 resolved or explicitly accepted).
*Tests*: promotion-logic tests using real historical run pairs (a genuinely-better real candidate vs a genuinely-worse one) to confirm the statistical-significance gate correctly accepts/rejects each — this must be tested with real backtest output, not fabricated metric values, since the whole point is catching whether the significance test itself is sound. A dedicated test asserting `live_eligible` can never be set to true by any code path in this module (only by the existing human-gated Gate-7 flow) — this is a safety-critical invariant and should be enforced as a hard test, run on every CI pass, not just at initial build time.

## Standard Backtesting Algorithm

The whole point of a shared `core/engine.py` orchestrator is that every channel runs through the *same* algorithm, differing only in `adapter.generate_signals()`. This is the canonical procedure every adapter must conform to — it should live as a docstring/spec on `core/engine.py` itself, not just in this plan.

```
INPUT: channel, horizon_bucket, universe_spec, date_range, capital_mode (lump | sip),
       initial_capital, sip_config?, random_seed

1. UNIVERSE CONSTRUCTION (point-in-time, per rebalance date — not once at run start)
   for each rebalance_date in schedule(horizon_bucket, date_range):
       universe(rebalance_date) = tickers listed AND not-yet-delisted as of rebalance_date
                                    (include_delisted=True — see Gap #1)
                                   ∩ point-in-time index/liquidity filter if universe_spec requires it
       # never use today's ticker list retroactively (Gap #5)

2. WALK-FORWARD FOLD SPLIT (applies even to non-ML channels, for consistency)
   folds = split(date_range, train_window, test_window, step)
   # ML adapter: train_window used for model fit; Technical/Fundamental/Momentum adapters
   # have no "training" step but still report fold-level metrics on the same date boundaries,
   # so cross-channel comparison over the same fold is apples-to-apples

3. FOR EACH fold:
     FOR EACH rebalance_date in fold.test_window:
       a. SIGNAL GENERATION
          signals = adapter.generate_signals(universe(rebalance_date), rebalance_date, horizon_bucket)
          feature_log.record(run_id, rebalance_date, adapter.feature_vector(ticker, rebalance_date))
                                                                    # for every candidate, not just picks
       b. CORPORATE ACTION / DELISTING RECONCILIATION (on existing open positions, before sizing new ones)
          for each open position:
              if ticker delisted/merged since last rebalance: apply close-out policy (Gap #4),
                  realize P&L, release capital
              else: adjust position for any split/bonus/rights factor since last rebalance
       c. POSITION SIZING (core/portfolio.py, horizon-bucket table, §3)
          for each signal (ranked by conviction if oversubscribed):
              size = min(max_position_pct(horizon_bucket) * portfolio_value,
                         max_sector_pct(horizon_bucket) constraint,
                         adtv_cap(ticker, rebalance_date),   # Gap #6
                         available_cash)
       d. CASH FLOW EVENTS
          if capital_mode == sip and rebalance_date is an injection date:
              cash += sip_config.amount   # before or alongside step (c), per momentum's existing pattern
       e. ORDER EXECUTION (costs.py — already shared)
          apply IndianTransactionCosts (brokerage, STT, slippage as f(size, adtv)) to every buy/sell
       f. PORTFOLIO UPDATE
          mark-to-market all positions, record cash_position, record equity_curve point
   
     g. FOLD METRICS (core/metrics.py, computed once per fold and once for the full run)
        cagr (calendar/365.25, primary) + legacy trading-day cagr (secondary)
        xirr (from actual dated cash flows — handles both lump and SIP uniformly)
        final_capital, max_drawdown, win_rate, profit_factor, sortino, calmar
        n_distinct_tickers_traded, turnover_ratio                    # Gap #11
        benchmark_cagr / excess_return  → null + flagged if fold predates 2023-07 (accepted gap)

4. INTEGRITY / OVERFIT CHECKS (already exist for ML in overfit_checks.py/integrity_checker.py —
   this step generalizes them as a mandatory final stage for ALL channels, not ML-only)
   - deflated_sharpe_ratio, random_feature_test equivalents run against the adapter's signal
     to catch a Technical/Momentum/Fundamental strategy that's curve-fit to the backtest window,
     not just ML models
   - run is flagged (not blocked) if integrity checks fail — visible in the results UI, not silent

5. PERSIST
   BacktestRun record (run_id, parent_run_id, channel, horizon_bucket, config_hash, random_seed)
   BacktestRunResult (all metrics from 3g, aggregated across folds)
   backtest_feature_log rows (from 3a)
   equity_curve + cash_position time series

OUTPUT: run_id → queryable via unified API/UI (Phase 3-4)
```

Key standardization points this enforces that don't exist today:
- **Same fold/rebalance-date scaffolding for every channel**, so a 5-day Technical strategy and a 1-year Fundamental strategy can be compared on the same wall-clock periods.
- **Corporate-action/delisting reconciliation is a mandatory step every adapter goes through**, closing Gap #4 structurally rather than leaving it to each adapter to remember.
- **Integrity/overfit checks apply to all four channels**, not just ML — a Technical screener template or a Momentum ranking rule can be just as curve-fit as an ML model; today only ML gets checked.
- **feature_log.record() happens for every candidate signal, not just the ones ultimately picked** — needed for the feature-reengineering loop to answer "what did the model/rule see for stocks it passed on," not just for the winners.

## Phase 6: Automated Fine-Tuning / Self-Improvement Loop — Requirements

**Scope note first**: this sits outside the Backtest/Walk-Forward/Paper-Trade modules proper — those three produce *results*; this is a separate system that *consumes* those results to improve models/rules and *produces new candidate versions* to feed back in. Calling it "Reinforcement Learning" overstates what's being asked for here (there's no MDP/reward-policy learning implied by the user's description) — what's actually being requested is an **automated evaluate → re-engineer/retrain → re-run → compare → promote-or-reject loop**, running unattended in the background across the day, with full state history. Note this connects to (but is distinct from) the project's already-planned "RL Meta-Agent" gate referenced in `alphalens_docs/11_phase_delivery_plan.md` (line ~522-542: "RL agent validated in paper trading for 3+ months before any live use") — if a true RL agent is added later, it would be one more strategy type running through this same loop, not a replacement for it.

**Requirements:**

1. **Background execution.** Backtest and Walk-Forward runs execute as scheduled background agents throughout the day (reuse the existing `CronCreate`/scheduled-agent pattern already used by `ops-monitor`/`scrum-master` in this repo's agent ecosystem) — not on-demand only. Each run targets one strategy (channel × horizon bucket) at a time, consistent with the no-pooled-capital rule above.
2. **Full state history, never overwritten.** Every run (backtest, walk-forward, paper-trade) persists as an immutable `BacktestRun`/`BacktestRunResult` + `backtest_feature_log` record (per the core schema above) with a `parent_run_id` chain. A **model registry** table (net-new, `backtest_model_registry`: model_version_id, channel, horizon_bucket, hyperparameters, feature_set_hash, training_data_window, produced_by_run_id, status ∈ {candidate, champion, retired}) tracks every trained model/rule-set version and which run produced it — nothing is ever deleted, only superseded.
3. **Retraining trigger.** A scheduled evaluator inspects recent Walk-Forward/Paper-Trade results per strategy; if trailing performance (e.g. last N periods' win rate, CAGR, or Sharpe) degrades past a threshold, or on a fixed cadence, it triggers a fine-tuning cycle: feature re-engineering candidates and/or hyperparameter search, producing one or more new `candidate` model versions.
4. **Evaluation, not blind promotion.** Every candidate is run through Backtest + Walk-Forward before being considered — a candidate is only promoted from `candidate` to `champion` status if it beats the current champion by a statistically defensible margin (reuse `overfit_checks.py`'s deflated Sharpe / significance testing, not a raw point-estimate comparison, to avoid promoting noise). This step should be reviewable by the existing `ml-rigor-reviewer`/`backtest-reviewer` agents or the `model-review` skill before being trusted unattended.
5. **Rollback.** Promotion is reversible — if a newly promoted champion underperforms in subsequent live Walk-Forward/Paper-Trade periods, the loop (or a human) can revert to the prior champion version by `model_version_id`, with no data loss since nothing was overwritten.
6. **Hard boundary with live capital.** This loop is only permitted to promote models within Backtest/Walk-Forward/Paper-Trade — it must never be permitted to affect real trading capital directly. Promotion to "live-eligible" still requires passing the existing Gate 7 (≥90 real forward paper-trading days) and explicit human sign-off, unchanged. This is a hard requirement given the project's stated risk philosophy (never risk capital on an unvalidated signal source) and should be enforced structurally (e.g. a `live_eligible` flag that only a human-approved action can set), not left as a convention.
7. **Auditability.** Every fine-tuning cycle's trigger reason, candidates generated, evaluation results, and promotion/rejection decision are logged and queryable — this is what makes the loop "auto-correcting" legible rather than a black box, and is what a future incident review would need.

This phase should be scoped as its own follow-on plan once Phases 0-5 are built and the Truthful Review's data-correctness gaps (#1-6) are resolved — running an automated retraining loop on top of a foundation with active survivorship bias or non-point-in-time fundamentals would let the loop "learn" to exploit those biases, silently.

## Sequencing Dependencies

- Phase 0 must resolve (or explicitly waive) data-density/clean-start-date questions before Phase 2's technical/fundamental adapters attempt 20-year runs, or results will look plausible but be silently wrong.
- Phase 1's `core/metrics.py` CAGR decision must land before Phase 2 — every adapter's metrics wiring depends on it.
- MultiBagger live-compute quality-gate blocks only MultiBagger-horizon backtests, not the rest of Phase 2.
- Phase 4 depends on Phase 3's schema existing, but UI scaffolding can start once the schema is drafted, without waiting for full backend completion.
- Phase 5 depends on Phase 1-2's shared core/portfolio (reuses `portfolio_state.py` + `core.portfolio`), not on Phase 4 being finished — could ship API-only first if paper trading is wanted sooner than the full frontend.
- Phase 2.5 (Walk-Forward) depends on Phase 2 having at least one working adapter (Momentum recommended first) and shares `core/day_driver.py` with Phase 5 (Paper Trading) — build `day_driver.py` once during Phase 2.5, Phase 5 reuses it rather than reimplementing.
- Phase 6 (fine-tuning loop) depends on Phases 0-5 being complete AND the Truthful Review's correctness gaps (#1 survivorship, #2 point-in-time fundamentals, #3 rights adjustment, #4 merger/delisting handling) being resolved or explicitly accepted — starting Phase 6 earlier risks an automated loop learning to exploit those data biases.

## Critical Files

- `backtest/engine.py`, `backtest/portfolio.py`, `backtest/momentum_backtest.py`, `backtest/momentum_metrics.py`, `backtest/momentum_tax.py`, `backtest/strategy_confidence.py`
- `datastore/api/routers/backtest_reports.py`, `datastore/api/routers/paper_trading.py`, `datastore/api/routers/momentum.py` (trade journal)
- `frontend/src/lib/ui/nav.ts`, `frontend/src/pages/ml/backtest.tsx`
- `config/settings.py` (PAPER_TRADING_REQUIRE_APPROVAL, position caps)
- `systems/ml_signal_engine/models/multibagger/multibagger_model.py`, `datastore/schema/create_signals.py`
- `features/momentum_universe.py` (`include_delisted` flag — Gap #1), `config/build_universe.py` (`build_historical_universe_from_delisted`), `ingestion/adjust/price_adjuster.py` + `ingestion/adjust/rights_adjuster.py` (Gap #3), `datastore/api/pit.py` (`enforce_pit_fundamentals` — Gap #2)
- `alphalens_docs/11_phase_delivery_plan.md` (existing RL Meta-Agent gate — Phase 6 context)

## Verification

- After Phase 1: rerun `run_phase{1,2,3}_backtest.py`, diff old vs new engine.py output on identical inputs to confirm no behavior regression (aside from the intentional CAGR field addition).
- After each Phase 2 adapter: run a backtest on a known historical window per channel and manually sanity-check CAGR/XIRR/win-rate against the channel's existing standalone results (e.g. momentum's external artifact numbers) before decommissioning the old path.
- After Phase 3: confirm `/api/v1/backtest/*` returns identical data to the legacy `backtest_reports.py` passthrough for existing ML reports.
- After Phase 4: manually run a 1yr/5yr/20yr backtest per channel through the new UI, confirm metrics render, confirm feature-log inspector returns rows.
- After Phase 5: run paper trading for one channel end-to-end (signal → pending approval → accept → position tracked), confirm Gate-7-style counting still works generalized across channels.
- After Phase 2.5: run a Walk-Forward replay from a historical start date (e.g. 2010-01-01) for one strategy, confirm each period only used data available as of that period (spot-check a few periods for lookahead), confirm retraining log records a model version per period.
- After Phase 6: verify a full cycle end-to-end on a non-critical strategy — trigger fires, candidate generated, candidate backtested+walk-forward-tested, promotion decision logged with its statistical justification, and confirm the `live_eligible` flag remains false throughout (i.e. Phase 6 cannot touch live capital regardless of its own decisions).
