"""
tests/unit/test_core_report.py

A83: the shared backtest report contract.

The tests worth reading are the ones about units and about absence. Both
conventions exist because breaking them produced plausible-looking numbers
that were wrong:

- annualising a figure that was already annualised understated every Technical
  rolling return by roughly the window length;
- a missing metric rendered as 0.0 makes a strategy with no data look like the
  best-behaved strategy in the table.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from backtest.core.report import (
    ORCHESTRATOR_PENDING,
    StrategyReport,
    as_rate,
    from_run_result,
    pct_to_fraction,
)


# --- units -----------------------------------------------------------------


def test_as_rate_passes_a_fraction_through_unchanged():
    """The rule that matters: a rate arrives as a rate and is not re-derived."""
    assert as_rate(0.243) == 0.243


def test_as_rate_converts_a_percentage_without_annualising():
    assert as_rate(24.3, already_fraction=False) == 0.243


def test_as_rate_rejects_nan_rather_than_propagating_it():
    """NaN reaching a report renders as "NaN%" in a table cell, which reads as
    a broken page rather than as missing data."""
    assert as_rate(float("nan")) is None
    assert as_rate(None) is None


def test_pct_to_fraction_is_separate_from_as_rate():
    """Win rate and drawdown are percentages but NOT rates; keeping the two
    conversions separate is what stops a drawdown being labelled '%/yr'."""
    assert pct_to_fraction(62.5) == 0.625


# --- absence ---------------------------------------------------------------


@dataclass
class _Run:
    channel: str = "technical"
    start_date: str = "2016-04-01"
    end_date: str = "2026-03-31"
    capital_mode: str = "lump"
    initial_capital: float = 1_000_000.0


@dataclass
class _Result:
    metrics: Dict[str, Any] = field(default_factory=dict)
    run: _Run = field(default_factory=_Run)
    equity_curve: List[Dict[str, Any]] = field(default_factory=list)
    benchmark_curve: List[Dict[str, Any]] = field(default_factory=list)
    exit_policy_variant: Optional[str] = None


def test_missing_metrics_are_none_not_zero():
    r = from_run_result(_Result(), strategy_key="technical:A1")
    assert r.returns.cagr_pre_tax is None
    assert r.risk.max_drawdown is None
    assert r.risk.sharpe is None


def test_absent_metrics_name_the_backlog_item_that_supplies_them():
    r = from_run_result(_Result(), strategy_key="technical:A1")
    assert r.pending["consistency.rolling"].backlog_id == "T13"
    assert r.pending["returns.cagr_post_tax"].backlog_id == "A86"
    assert r.pending["equity_curve"].backlog_id == "A90"


def test_a_populated_metric_is_not_also_reported_as_pending():
    """A stale pending entry on a populated field tells the reader the number
    is missing while showing it to them."""
    res = _Result(
        metrics={"cagr_post_tax": 0.19},
        equity_curve=[{"date": "2020-01-01", "equity": 100.0}],
        benchmark_curve=[{"date": "2020-01-01", "equity": 100.0}],
    )
    r = from_run_result(res, strategy_key="technical:A1")
    assert r.returns.cagr_post_tax == 0.19
    assert "returns.cagr_post_tax" not in r.pending
    assert "equity_curve" not in r.pending
    assert "benchmark_curve" not in r.pending


def test_nan_metrics_are_treated_as_absent():
    res = _Result(metrics={"cagr": float("nan"), "sharpe": float("nan")})
    r = from_run_result(res, strategy_key="technical:A1")
    assert r.returns.cagr_pre_tax is None
    assert r.risk.sharpe is None


# --- benchmark labelling ---------------------------------------------------


def test_benchmark_index_name_is_carried(monkeypatch):
    res = _Result(
        metrics={
            "benchmark_cagr": 0.12,
            "excess_return": 0.04,
            "benchmark_index_name": "Nifty Midcap 150",
            "benchmark_status": "ok",
        }
    )
    r = from_run_result(res, strategy_key="momentum:x", channel="momentum")
    assert r.returns.benchmark_index_name == "Nifty Midcap 150"
    assert r.returns.benchmark_caveat is None


def test_a_degraded_benchmark_status_becomes_a_visible_caveat():
    """A null excess return with no explanation reads as a bug; the reason has
    to travel with the number."""
    res = _Result(metrics={"benchmark_status": "insufficient_benchmark_history"})
    r = from_run_result(res, strategy_key="technical:A1")
    assert "insufficient_benchmark_history" in (r.returns.benchmark_caveat or "")


# --- robustness ------------------------------------------------------------


def test_a_run_predating_these_fields_still_produces_a_report():
    """Historical runs must stay readable. A KeyError here would make old
    results unreadable rather than merely incomplete."""
    r = from_run_result(_Result(metrics={}), strategy_key="ml:signal_21d", channel="ml")
    assert isinstance(r, StrategyReport)
    assert r.channel == "ml"


def test_to_dict_is_json_shaped():
    r = from_run_result(_Result(), strategy_key="technical:A1")
    d = r.to_dict()
    assert d["strategy_key"] == "technical:A1"
    assert set(d["pending"]["consistency.rolling"]) == {"backlog_id", "reason"}
    assert ORCHESTRATOR_PENDING["consistency.rolling"].backlog_id == "T13"
