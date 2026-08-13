"""
backtest/core/report.py

A83: the one report shape every channel emits.

Today each channel writes its own: `ta_comparison_report.py` produces one
document, `scripts/run_momentum_dynamic_report.py` another, and the
orchestrator's `backtest_runs` row a third. They overlap heavily and agree on
almost nothing -- different field names, different units, different
definitions behind the same word. The frontend absorbs that in per-channel
adapters, which works but means every new consumer has to absorb it again.

This module defines the target: `StrategyReport`, mirroring exactly the shape
the UI already renders (frontend/src/features/backtest-report/core/types.ts),
so the adapters can eventually be deleted rather than rewritten.

Two conventions are enforced here rather than left to each writer, because
both have already produced wrong numbers in this codebase:

1. A RETURN IS A RATE. Every field named *_cagr, *_xirr or *_rate is an
   annualised fraction (0.243 = 24.3%/yr), never a total over the window and
   never a percentage. `as_rate()` is the only sanctioned way to build one.
   A "3-year return of 33%" is meaningless beside a "5-year return of 61%";
   as rates both are 10%/yr, the same strategy.

2. NULL IS NOT ZERO. Every metric is Optional. A metric the engine cannot
   supply is None with an entry in `pending` naming the backlog item that will
   supply it -- never 0.0. A strategy with no drawdown data and a strategy
   with no drawdown are different facts, and conflating them makes the first
   one look like the best strategy in the table.

Trade-level P&L is deliberately NOT a rate: a single trade's return over a
three-day hold is a trade outcome, not a period performance measure, and
annualising it produces absurd numbers.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# units
# ---------------------------------------------------------------------------


def as_rate(value: Optional[float], *, already_fraction: bool = True) -> Optional[float]:
    """Normalise an annualised return to a fraction.

    `already_fraction=False` converts a percentage (24.3 -> 0.243). It does
    NOT annualise: a figure that is not already a rate cannot be made into one
    without knowing its window, and guessing is how a 33.1%/yr three-year
    median once got re-derived into ~10%/yr.
    """
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if v != v:  # NaN
        return None
    return v / 100.0 if not already_fraction else v


def pct_to_fraction(value: Optional[float]) -> Optional[float]:
    """For non-rate percentages: win rate, drawdown, share-of-years."""
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return None if v != v else v / 100.0


# ---------------------------------------------------------------------------
# the contract
# ---------------------------------------------------------------------------


@dataclass
class PendingField:
    """Why a metric is absent, and what will supply it."""

    backlog_id: str
    reason: str


@dataclass
class Returns:
    cagr_pre_tax: Optional[float] = None
    cagr_post_tax: Optional[float] = None
    xirr: Optional[float] = None
    sip_xirr: Optional[float] = None
    final_capital: Optional[float] = None
    total_contributed: Optional[float] = None
    benchmark_cagr: Optional[float] = None
    excess_return: Optional[float] = None
    # A98: which index the comparison was against. Two excess returns are not
    # comparable without it.
    benchmark_index_name: Optional[str] = None
    benchmark_caveat: Optional[str] = None


@dataclass
class RollingWindow:
    """One window length's distribution of ANNUALISED returns.

    Every field here is a rate. Both engines already produce them annualised
    (`ta_comparison_report` computes ((e1/e0) ** (1/years) - 1) * 100;
    `momentum_metrics` returns cagr_pct), so a consumer must convert units
    only -- never re-derive.
    """

    window_years: float
    min_cagr: Optional[float] = None
    median_cagr: Optional[float] = None
    max_cagr: Optional[float] = None
    positive_share: Optional[float] = None
    n_windows: Optional[int] = None


@dataclass
class YoyReturn:
    fy_label: str
    return_pct: Optional[float] = None  # a fraction, for one financial year


@dataclass
class Consistency:
    rolling: List[RollingWindow] = field(default_factory=list)
    yoy: List[YoyReturn] = field(default_factory=list)


@dataclass
class Risk:
    max_drawdown: Optional[float] = None
    sharpe: Optional[float] = None
    sortino: Optional[float] = None
    calmar: Optional[float] = None
    volatility: Optional[float] = None


@dataclass
class TradeQuality:
    n_trades: Optional[int] = None
    n_closed_trades: Optional[int] = None
    n_open_trades: Optional[int] = None
    win_rate: Optional[float] = None
    profit_factor: Optional[float] = None
    avg_hold_days: Optional[float] = None
    churn_per_year: Optional[float] = None
    # Per-trade outcomes, NOT rates -- see the module docstring.
    avg_winner_pct: Optional[float] = None
    avg_loser_pct: Optional[float] = None
    turnover_ratio: Optional[float] = None


@dataclass
class IncomeMode:
    """capital_mode="annual_reset" only (A88)."""

    target_withdrawal: Optional[float] = None
    total_withdrawn: Optional[float] = None
    total_injected: Optional[float] = None
    years_survived_pct: Optional[float] = None
    n_years: Optional[int] = None
    top_up_after_loss: Optional[bool] = None


@dataclass
class StrategyReport:
    """One strategy's results, identical in shape across all four channels."""

    strategy_key: str
    label: str
    channel: str
    setup: Dict[str, Any] = field(default_factory=dict)
    returns: Returns = field(default_factory=Returns)
    consistency: Consistency = field(default_factory=Consistency)
    risk: Risk = field(default_factory=Risk)
    trade_quality: TradeQuality = field(default_factory=TradeQuality)
    income: Optional[IncomeMode] = None
    equity_curve: List[Dict[str, Any]] = field(default_factory=list)
    benchmark_curve: List[Dict[str, Any]] = field(default_factory=list)
    trade_book_url: Optional[str] = None
    # Dotted path -> why it is absent. "returns.cagr_post_tax": PendingField(...)
    pending: Dict[str, PendingField] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["pending"] = {
            k: {"backlog_id": v.backlog_id, "reason": v.reason}
            for k, v in self.pending.items()
        }
        return out


# ---------------------------------------------------------------------------
# building one from an orchestrator run
# ---------------------------------------------------------------------------

#: Metrics a run may not carry. T13 computes all four in-engine now, so these
#: apply only to runs recorded BEFORE that landed -- they are cleared below
#: whenever the values are actually present.
ORCHESTRATOR_PENDING = {
    "consistency.rolling": PendingField(
        "T13", "This run predates rolling windows being computed in-engine."
    ),
    "consistency.yoy": PendingField(
        "T13", "This run predates year-on-year returns being computed in-engine."
    ),
    "trade_quality.churn_per_year": PendingField(
        "T13", "This run predates churn being computed in-engine."
    ),
    "trade_quality.avg_winner_pct": PendingField(
        "T13", "This run predates average winner/loser being computed in-engine."
    ),
}


def _income_from_ledger(fy_ledger: Optional[List[Dict[str, Any]]]) -> Optional[IncomeMode]:
    """Income-mode headline figures from the per-FY ledger (A88).

    Only produced for capital_mode="annual_reset" runs, which are the only
    ones with a ledger. Returning None elsewhere is deliberate: a lump-sum run
    has no withdrawal behaviour, and reporting zeros would make it look like
    an income strategy that never paid out.

    `years_survived_pct` counts years that ended at or above the base without
    needing a top-up. In the no-top-up variant that is the survival question
    the whole variant exists to answer, since a run refunded every April
    cannot go broke however badly it trades.
    """
    if not fy_ledger:
        return None
    n_years = len(fy_ledger)
    withdrawn = sum(float(r.get("withdrawn") or 0.0) for r in fy_ledger)
    topped_up = sum(float(r.get("topped_up") or 0.0) for r in fy_ledger)
    survived = sum(
        1
        for r in fy_ledger
        if not (r.get("topped_up") or 0.0) and not (r.get("topup_forgone") or 0.0)
    )
    return IncomeMode(
        total_withdrawn=withdrawn,
        total_injected=topped_up,
        years_survived_pct=(survived / n_years) if n_years else None,
        n_years=n_years,
        # Read off the ledger rather than a config the report may not have:
        # the two variants produce genuinely different trade books, so which
        # one ran is a property of the result, not of the request.
        top_up_after_loss=bool(fy_ledger[0].get("top_up_after_loss"))
        if "top_up_after_loss" in fy_ledger[0]
        else None,
    )


def from_run_result(
    result: Any,
    *,
    strategy_key: Optional[str] = None,
    label: Optional[str] = None,
    channel: Optional[str] = None,
    trade_book_url: Optional[str] = None,
) -> StrategyReport:
    """Build the shared report from a BacktestRunResult.

    Deliberately tolerant of missing keys: this runs against runs written
    before several of these fields existed, and a KeyError on a historical run
    would make old results unreadable rather than merely incomplete.
    """
    metrics: Dict[str, Any] = getattr(result, "metrics", None) or {}
    run = getattr(result, "run", None)
    # A89: prefer the key the engine emitted. Falling back to a caller-supplied
    # one keeps runs recorded before A89 readable, but a run that states its
    # own identity is authoritative over anything reconstructed.
    strategy_key = getattr(result, "strategy_key", None) or strategy_key
    if not strategy_key:
        raise ValueError(
            "No strategy_key: the run did not emit one (pre-A89) and none was "
            "supplied. A report that cannot identify its strategy cannot be "
            "linked to, compared, or deployed."
        )

    def m(key: str) -> Optional[float]:
        v = metrics.get(key)
        if v is None:
            return None
        try:
            f = float(v)
        except (TypeError, ValueError):
            return None
        return None if f != f else f

    status = metrics.get("benchmark_status")
    report = StrategyReport(
        strategy_key=strategy_key,
        label=label or strategy_key,
        channel=channel or getattr(run, "channel", "") or "",
        setup={
            "start_date": str(getattr(run, "start_date", "") or "") or None,
            "end_date": str(getattr(run, "end_date", "") or "") or None,
            "capital_mode": getattr(run, "capital_mode", None),
            "initial_capital": getattr(run, "initial_capital", None),
            "exit_policy_variant": getattr(result, "exit_policy_variant", None),
        },
        returns=Returns(
            # A86: `cagr` is whichever basis the run was executed on, and
            # `tax_basis` says which. Assigning it to the wrong field is how a
            # post-tax figure gets compared with a pre-tax one and the gap
            # read as skill, so the assignment is driven by the recorded basis
            # rather than assumed.
            cagr_pre_tax=(
                m("cagr_other_basis") if metrics.get("tax_basis") == "post_tax"
                else m("cagr")
            ),
            cagr_post_tax=(
                m("cagr") if metrics.get("tax_basis") == "post_tax"
                else m("cagr_post_tax")
            ),
            xirr=m("xirr"),
            final_capital=m("final_capital"),
            total_contributed=m("total_contributed"),
            benchmark_cagr=m("benchmark_cagr"),
            excess_return=m("excess_return"),
            benchmark_index_name=metrics.get("benchmark_index_name"),
            benchmark_caveat=(
                f"Benchmark status: {status}" if status and status != "ok" else None
            ),
        ),
        risk=Risk(
            max_drawdown=m("max_drawdown"),
            sharpe=m("sharpe"),
            sortino=m("sortino"),
            calmar=m("calmar"),
        ),
        trade_quality=TradeQuality(
            n_trades=metrics.get("total_trades") or metrics.get("n_trades"),
            n_closed_trades=metrics.get("n_trades"),
            win_rate=m("win_rate"),
            profit_factor=m("profit_factor"),
            avg_hold_days=m("avg_days_held"),
            churn_per_year=m("churn_per_year"),
            avg_winner_pct=m("avg_winner_pct"),
            avg_loser_pct=m("avg_loser_pct"),
            turnover_ratio=m("turnover_ratio"),
        ),
        consistency=Consistency(
            rolling=[
                RollingWindow(
                    window_years=float(str(label).rstrip("y")),
                    min_cagr=w.get("min_cagr"),
                    median_cagr=w.get("median_cagr"),
                    max_cagr=w.get("max_cagr"),
                    positive_share=w.get("positive_share"),
                    n_windows=w.get("n_windows"),
                )
                for label, w in sorted((metrics.get("rolling_returns") or {}).items())
                if isinstance(w, dict) and w.get("n_windows")
            ],
            yoy=[
                YoyReturn(fy_label=r.get("fy_label", ""), return_pct=r.get("return_pct"))
                for r in (metrics.get("fy_returns") or [])
                # A partial financial year is not a year's return. Including
                # it drags every "share of positive years" figure around with
                # a stub period.
                if not r.get("partial")
            ],
        ),
        income=_income_from_ledger(getattr(result, "fy_ledger", None)),
        equity_curve=list(getattr(result, "equity_curve", None) or []),
        benchmark_curve=list(getattr(result, "benchmark_curve", None) or []),
        trade_book_url=trade_book_url,
        pending=dict(ORCHESTRATOR_PENDING),
    )

    # Only claim a metric is pending when it is actually absent -- a stale
    # pending entry on a populated field tells the reader the number is
    # missing while showing it to them. T13 supplies four of these in-engine
    # now, so they clear themselves as soon as a run carries them.
    if report.consistency.rolling:
        report.pending.pop("consistency.rolling", None)
    if report.consistency.yoy:
        report.pending.pop("consistency.yoy", None)
    if report.trade_quality.churn_per_year is not None:
        report.pending.pop("trade_quality.churn_per_year", None)
    if report.trade_quality.avg_winner_pct is not None:
        report.pending.pop("trade_quality.avg_winner_pct", None)
    if report.returns.cagr_post_tax is None:
        report.pending["returns.cagr_post_tax"] = PendingField(
            "A86", "Post-tax CAGR is not emitted by this run."
        )
    if not report.equity_curve:
        report.pending["equity_curve"] = PendingField(
            "A90", "This run predates the equity curve being carried into the report."
        )
    if not report.benchmark_curve:
        report.pending["benchmark_curve"] = PendingField(
            "A90", "No index series covered this run's window."
        )
    return report
