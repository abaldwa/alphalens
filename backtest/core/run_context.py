"""
backtest/core/run_context.py

Phase: Unified Backtest & Paper Trading Umbrella, Phase 1
Owner: Platform / Backtest
Consumers: backtest/core/engine.py, backtest/core/feature_log.py (once
built), Phase 3's backtest_runs DuckDB table + API

BacktestRun / BacktestRunResult: the run-record schema every mode
(backtest, walk_forward, paper) and every channel writes into. Carries
the fields the plan requires for the feature-reengineering feedback loop
(parent_run_id chains) and for Phase 6's model registry (random_seed,
config_hash reproducibility).

Deliberately excludes any notion of pooled/shared capital across
strategies (BacktestUmbrellaPlan.md, confirmed 2026-07-20: "each Strategy
would run its own backtest... on a Strategy Capital base") — one
BacktestRun is always scoped to exactly one channel + one horizon_bucket
+ one capital base.
"""

import hashlib
import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Literal, Optional

from backtest.core.horizon import HorizonBucket

RunMode = Literal["backtest", "walk_forward", "paper"]
Channel = Literal["technical", "fundamental", "ml", "momentum"]
# "annual_reset" added 2026-08-12 — the user's third performance measure: each
# Indian FY opens on `initial_capital`, booked profit is withdrawn after tax at
# the FY boundary, and a losing year is topped back up. See
# backtest/core/portfolio.py::AnnualResetConfig.
CapitalMode = Literal["lump", "sip", "annual_reset"]

# Phase 0 audit, 2026-07-20 (BacktestUmbrellaPlan.md "Known Data Gaps" #1): real
# fundamentals coverage is a handful of rows total before 2020 (2005-2019 combined
# = 186 rows across all tickers; 2020 alone = 1,842 rows/1,746 tickers). Confirmed
# user decision: reject rather than silently run on near-empty data.
FUNDAMENTAL_MIN_START_DATE = date(2020, 1, 1)


def config_hash(config: Dict[str, Any]) -> str:
    """Deterministic hash of a run's config dict, for reproducibility checks
    (Truthful Review #10: unchanged config + seed must produce bit-identical
    results on rerun) and for de-duplicating accidental identical reruns."""
    canonical = json.dumps(config, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


@dataclass
class BacktestRun:
    channel: Channel
    strategy_id: str  # operator-assigned name for this specific strategy definition
    horizon_bucket: HorizonBucket
    mode: RunMode
    universe_spec: str
    start_date: date
    end_date: date
    capital_mode: CapitalMode
    initial_capital: float
    sip_amount: Optional[float] = None
    sip_cadence_days: Optional[int] = None
    # capital_mode="annual_reset" only. The LTCG regime is a RUN-LEVEL input
    # here, not a reporting choice, because the tax determines how much cash is
    # withdrawn at each FY boundary, which changes the capital available next
    # year, which changes which trades execute. The two regimes are therefore
    # genuinely different simulations and cannot be derived from one trade book
    # the way the lump run's regimes are.
    annual_reset_ltcg_rate: Optional[float] = None
    annual_reset_ltcg_exemption: Optional[float] = None
    annual_reset_regime_label: Optional[str] = None
    # [2026-08-13] Whether a losing FY is topped back up to base_capital.
    # True (default) is the original measure. False withdraws surplus exactly
    # as before but injects nothing after a loss, leaving the strategy to earn
    # its way back on the capital it has left.
    #
    # Like the LTCG regime above, this is a RUN-LEVEL input rather than a
    # reporting choice: a smaller book sizes smaller positions and can fail
    # can_buy outright, so the two variants take genuinely different trades and
    # neither can be derived from the other's trade book.
    annual_reset_top_up_after_loss: bool = True
    random_seed: int = 0
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    parent_run_id: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    config: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # one strategy, one capital base — never a multi-channel/pooled run (see module docstring)
        if not self.strategy_id:
            raise ValueError("strategy_id is required — a run must be scoped to exactly one strategy")
        if self.channel == "fundamental" and self.start_date < FUNDAMENTAL_MIN_START_DATE:
            raise ValueError(
                f"Fundamental-channel runs cannot start before {FUNDAMENTAL_MIN_START_DATE.isoformat()} — "
                f"real fundamentals coverage is near-empty before 2020 (Phase 0 audit, 2026-07-20; see "
                f"BacktestUmbrellaPlan.md 'Known Data Gaps' #1). Requested start_date={self.start_date.isoformat()}."
            )

    @property
    def config_hash(self) -> str:
        return config_hash(self.config)


@dataclass
class BacktestRunResult:
    run: BacktestRun
    metrics: Dict[str, Any]  # asdict(BacktestMetrics) from core/metrics.py
    data_gaps: List[Dict[str, Any]] = field(default_factory=list)  # No-Mock-Data Policy: excluded tickers/periods, never fabricated
    integrity_passed: Optional[bool] = None
    integrity_detail: Dict[str, Any] = field(default_factory=dict)
    # Walk-Forward mode only (Phase 2.5): which model/rule version was active for
    # which stretch — [{"as_of_date": ..., "model_version": ...}, ...]. Empty for
    # plain backtest/paper runs that never call adapter.refit().
    refit_log: List[Dict[str, Any]] = field(default_factory=list)
    # REV17 (2026-07-21 review): the same-day-close vs. next-day-open fill
    # convention used to be an undocumented, silent simplification — now an
    # explicit, recorded choice (OrchestratorConfig.execution_timing) so
    # every report states which produced it.
    execution_timing: str = "same_day_close"
    # STEP 3b (2026-08-13) — REAL per-phase wall-clock timings, from
    # backtest/instrumentation.py::PhaseTimings.as_dict():
    # {"total_seconds", "measured_seconds", "unattributed_seconds",
    #  "phases": {name: {"seconds", "calls", "pct_of_measured", "ms_per_call"}}}.
    #
    # Note the deliberate name. `execution_timing` directly above is a FILL
    # POLICY ("same_day_close" / "next_day_open") that has nothing to do with
    # elapsed time — and because it reads like instrumentation and is recorded
    # on every run, this project spent months believing it had timing data
    # while all 500 historical runs carried none. That is why the backtest
    # redesign's speed target was a guess. These two fields must never be
    # conflated or merged.
    #
    # Empty for every run predating this field and for callers that set
    # collect_timings=False.
    phase_timings: Dict[str, Any] = field(default_factory=dict)
    # Per-Bull/Bear/Sideways-segment performance (backtest/core/
    # regime_breakdown.py) — [{"regime": "bull", "start_date": ..., "cagr":
    # ..., "win_rate": ..., "n_trades": ..., ...}, ...]. Empty when the
    # orchestrator wasn't given a regime_conn (regime breakdown is opt-in,
    # not required for every run).
    regime_breakdown: List[Dict[str, Any]] = field(default_factory=list)
    # capital_mode="annual_reset" only (2026-08-12) — one row per Indian FY:
    # opening_capital, closing_equity, realised_pnl, tax, withdrawn_pretax,
    # withdrawn, withdrawal_tax_drag, topped_up, return_on_opening_pct,
    # opened_above_base. Empty for lump/sip.
    #
    # This IS measure 3's deliverable. The engine computed it correctly in the
    # first smoke test but nothing carried it out of the portfolio object, so
    # the run produced trades under the right capital regime and then discarded
    # the ledger — caught 2026-08-12 before the 390-job sweep launched.
    #
    # opening_capital is deliberately the capital the year ACTUALLY started
    # with, which drifts above base_capital in good years (a near-fully-invested
    # book cannot withdraw down to the base without selling positions the
    # strategy never signalled). Never present these returns as fixed-base
    # returns — see AnnualResetConfig's docstring.
    fy_ledger: List[Dict[str, Any]] = field(default_factory=list)
    # [STEP 5, 2026-08-13] One row per FY: assessed / paid / deferred /
    # cash_after. Tax used to be deducted once from the closing value in
    # _finalize, so every rupee of it compounded inside the portfolio for the
    # rest of the run and was written off at the end. It is now a real cash
    # outflow at each FY boundary, and this ledger is the audit trail.
    #
    # A non-zero `deferred` is not an error: a near-fully-invested book can owe
    # more at 31 March than it holds in cash. It has to be VISIBLE, though,
    # because an unpaid balance keeps compounding until settled — the exact
    # effect being removed.
    tax_ledger: List[Dict[str, Any]] = field(default_factory=list)
    # [A89, 2026-08-13] The canonical cross-application identity,
    # "{channel}:{name}" -- the same key the registry, the API and the
    # frontend use. Four schemes coexisted (variant_id, strategy_id+run_id,
    # template_name+exit_variant, and a localStorage string), so no view could
    # link to another and the client had to re-derive identity by parsing
    # strings. Emitted by the engine now, which is what lets that parsing be
    # deleted rather than maintained.
    strategy_key: Optional[str] = None
    # [A90, 2026-08-13] The benchmark's equity curve, same shape and dates as
    # `equity_curve` below: [{"date": "YYYY-MM-DD", "equity": float}].
    #
    # The strategy curve was already carried; the index one was computed by
    # _build_benchmark_curve to derive excess return and then discarded. A
    # report could therefore draw the strategy's path but never the line it is
    # supposed to be judged against, which is the comparison the whole report
    # exists to make.
    #
    # Empty when no index series covered the window. Never interpolated, and
    # never substituted from another index: a benchmark you did not choose is
    # worse than no benchmark, because it looks like one you did.
    benchmark_curve: List[Dict[str, Any]] = field(default_factory=list)
    # Accounting identities broken by this run (backtest/ledger_invariants.py).
    # Empty is the only acceptable value; a non-empty list means the simulation
    # itself is wrong, not that the strategy performed badly.
    ledger_violations: List[str] = field(default_factory=list)
    # Which of EXIT_POLICY_VARIANTS (backtest/core/engine.py) this run used —
    # threaded through from BacktestOrchestrator(exit_policy_variant=...) so
    # experiment comparison can group/filter runs by exit strategy. None for
    # any caller that doesn't pass it (e.g. older direct BacktestOrchestrator
    # construction that predates the 6 selectable variants).
    exit_policy_variant: Optional[str] = None
    # Convenience single-label summary of regime_breakdown: the regime with
    # a strict majority of this run's n_days, or None when no single regime
    # dominates (or regime_breakdown is empty — no regime_conn was given).
    # regime_breakdown itself remains the source of truth for the full
    # per-regime split; this is only a queryable shorthand.
    regime_label: Optional[str] = None
    # Filesystem path to this run's trade_log_{run_id}.csv (written by
    # BacktestOrchestrator._write_trade_log()), so a saved run row can be
    # joined back to its trade-level detail without recomputing the path
    # convention. None only if trade-log writing itself failed.
    trade_log_path: Optional[str] = None
    # (2026-08-08) Daily mark-to-market portfolio value —
    # [{"date": "YYYY-MM-DD", "equity": float}, ...], one entry per trading
    # day. Previously the equity curve existed only inside run() and was
    # discarded after compute_metrics consumed it, so nothing downstream
    # could compute a genuine time-weighted return over an arbitrary window:
    # rolling 2/3/4/5-year returns, drawdown recovery, or any "what was this
    # worth on date X" question had to be approximated from realized trade
    # P&L, which ignores open positions entirely. Carried on the RESULT
    # (and so into the report JSON) rather than into metrics_json, because
    # run_store deliberately strips the similarly-sized cash_position_series
    # from list_runs() to keep that table's rows small — this belongs with
    # the same treatment, not in the hot listing path.
    equity_curve: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["run"]["horizon_bucket"] = self.run.horizon_bucket.value
        return d
