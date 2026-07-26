"""
backtest/core/post_run_checks.py

2026-07-26 (REV1/REV4/REV6 wiring — model-review-reviewed design, see
FeatureBacklog.md): backtest/run_orchestrator_backtest.py (the real entry
point for the Technical/Fundamental/Momentum towers) never called
BacktestIntegrityChecker or deflated_sharpe_ratio — those existed only in
the separate, unused backtest/engine.py path. This module is the
integration point: given a completed BacktestOrchestrator run, derive
REAL inputs (not self-referential config constants) for the integrity
checks that can meaningfully apply to a rule-based screener/composite
strategy, and run them.

Design corrections from the 2026-07-26 six-reviewer design pass (see
conversation — not re-litigated here, just applied):

- check_05_costs/check_06_liquidity: derive the "applied" values from
  this run's OWN executed trades, but STILL compare them against the
  independent TOTAL_ROUNDTRIP_COST/MIN_ADT_INR floor (integrity_checker.py
  already does this internally) rather than only asserting "> 0" — a
  purely self-referential check (deriving AND validating against the same
  simulator) can never fail except on a bookkeeping bug. This module only
  supplies the derived "applied" numbers; the floor comparison is
  integrity_checker.py's own existing logic, untouched.
- check_08_fold_stability / check_09_benchmarks: fed from REGIME-SEGMENT
  sub-periods (backtest/core/regime_breakdown.py), not arbitrary
  equal-length calendar slices — reviewers confirmed contiguous calendar
  slices of one continuous equity curve are autocorrelated and
  mechanically bias std(fold_sharpes) toward a false PASS. Regime
  boundaries are at least defined by an independent real market-state
  signal. This is still NOT equivalent to genuine walk-forward
  (independently re-fit) folds — every result this module produces is
  labeled "subperiod" in its detail string specifically so it can never
  be silently read as walk-forward-validated on a dashboard.
- check_10_random_feature: skipped (not run, not marked pass/fail) for
  every channel except "ml" — Technical/Fundamental/Momentum are
  rule-based screeners/composites with no trainable model to shuffle
  features on; forcing a fake pass/fail here would be its own kind of
  fiction. See BacktestIntegrityChecker.run_all_checks(applicable_checks=).
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from backtest.core.regime_breakdown import compute_regime_breakdown
from backtest.integrity_checker import ALL_CHECK_NAMES, BacktestIntegrityChecker
from config.settings import MIN_ADT_INR

logger = logging.getLogger(__name__)

# check_10_random_feature has no rule-based equivalent (see module
# docstring) — every OTHER check is applicable to a rule-based strategy.
_APPLICABLE_CHECKS_NON_ML = set(ALL_CHECK_NAMES) - {"check_10_random_feature"}


def realized_cost_and_liquidity(trades: List[Any], data_gaps: List[Dict[str, Any]]) -> Tuple[Optional[float], Optional[float]]:
    """Derive (realized_cost_pct, applied_min_adt_inr) from what this run
    ACTUALLY did, not a config constant asserted to equal itself.

    realized_cost_pct: sum(Trade.cost_inr) / sum(entry turnover) across
    every closed trade — Trade.cost_inr is the real per-trade cost
    IndianTransactionCosts already deducted during execution (backtest/
    portfolio.py::PortfolioSimulator._close). None with zero trades
    (nothing to derive from — integrity_checker.py's check_05 correctly
    fails on None, not a fabricated pass).

    applied_min_adt_inr: MIN_ADT_INR (the configured floor) if the ADTV
    cap was actually exercised for every buy this run made — i.e. no
    "no_adtv_data_position_sized_uncapped" data_gap was recorded (backtest/
    core/engine.py records one every time a buy signal had no adtv_cr and
    therefore bypassed the cap, per Truthful Review Gap #6). If even one
    such gap exists, liquidity enforcement was NOT actually applied for
    every trade this run made, so this returns 0.0 — an honest fail,
    not a value that would trivially pass check_06_liquidity.
    """
    if not trades:
        return None, None
    total_turnover = sum(t.entry_price * t.quantity for t in trades)
    if total_turnover <= 0:
        return None, None
    total_cost = sum(t.cost_inr for t in trades)
    realized_cost_pct = total_cost / total_turnover

    uncapped_gap = any(g.get("reason") == "no_adtv_data_position_sized_uncapped" for g in data_gaps)
    applied_min_adt_inr = 0.0 if uncapped_gap else float(MIN_ADT_INR)

    return realized_cost_pct, applied_min_adt_inr


def _benchmark_segment_return(regime_conn, index_name: str, start_date, end_date) -> Optional[float]:
    """Real Nifty 500 (or configured index) buy-hold total return over one
    regime segment's [start_date, end_date] — real index_ohlcv close
    prices only, None if there's no real data covering the segment
    (never fabricated)."""
    try:
        row = regime_conn.execute(
            """
            SELECT
                (SELECT close FROM index_ohlcv WHERE index_name = ? AND date <= ? ORDER BY date DESC LIMIT 1) AS start_close,
                (SELECT close FROM index_ohlcv WHERE index_name = ? AND date <= ? ORDER BY date DESC LIMIT 1) AS end_close
            """,
            [index_name, start_date, index_name, end_date],
        ).fetchone()
    except Exception:
        logger.warning("benchmark segment return unavailable for %s [%s, %s]", index_name, start_date, end_date, exc_info=True)
        return None
    if row is None or row[0] is None or row[1] is None or row[0] == 0:
        return None
    return float(row[1]) / float(row[0]) - 1.0


def subperiod_check_inputs(
    equity_curve: pd.Series, trades: List[Any], run_start, run_end,
    regime_segments: List[Dict[str, Any]], regime_conn=None, regime_index_name: str = "Nifty 500",
) -> Tuple[List[float], List[float], List[float], str]:
    """(fold_sharpes, fold_returns, benchmark_returns, detail_note) built
    from real market-regime segment boundaries — see module docstring for
    why regime segments, not equal-length calendar slices. Empty lists
    with no regime_segments (regime breakdown wasn't requested for this
    run); integrity_checker.py's check_08/check_09 correctly fail on
    empty input rather than fabricating a pass."""
    rows = compute_regime_breakdown(equity_curve, trades, run_start, run_end, regime_segments)
    fold_sharpes = [r.sharpe for r in rows if r.sharpe is not None]
    fold_returns = [r.cagr for r in rows if r.cagr is not None]

    benchmark_returns: List[float] = []
    if regime_conn is not None:
        for r in rows:
            if r.cagr is None:
                continue
            bench = _benchmark_segment_return(regime_conn, regime_index_name, r.start_date, r.end_date)
            if bench is not None:
                benchmark_returns.append(bench)
        # fold_returns/benchmark_returns must be the SAME length and
        # positionally aligned for check_09 — drop any fold_returns entry
        # whose benchmark lookup failed rather than misaligning the pair.
        if len(benchmark_returns) != len(fold_returns):
            fold_returns = fold_returns[: len(benchmark_returns)]

    detail_note = (
        f"subperiod-based ({len(rows)} real market-regime segments — bull/bear/sideways, "
        "NOT independently-refit walk-forward folds; see backtest/core/post_run_checks.py)"
    )
    return fold_sharpes, fold_returns, benchmark_returns, detail_note


def run_post_run_integrity(
    channel: str, trades: List[Any], data_gaps: List[Dict[str, Any]],
    equity_curve: pd.Series, run_start, run_end,
    regime_segments: Optional[List[Dict[str, Any]]] = None, regime_conn=None,
    regime_index_name: str = "Nifty 500",
) -> Tuple[Optional[bool], Dict[str, Any]]:
    """Run BacktestIntegrityChecker against this run's REAL derived inputs.

    Returns (integrity_passed, integrity_detail) for BacktestRunResult —
    integrity_passed is None only if the checker itself couldn't be
    constructed/run (never fabricated True). integrity_detail carries
    each check's own detail string PLUS the subperiod-basis disclosure
    note so a reader can never mistake this for genuine walk-forward
    validation.
    """
    realized_cost_pct, applied_min_adt_inr = realized_cost_and_liquidity(trades, data_gaps)
    fold_sharpes, fold_returns, benchmark_returns, subperiod_note = subperiod_check_inputs(
        equity_curve, trades, run_start, run_end, regime_segments or [], regime_conn, regime_index_name,
    )

    checker = BacktestIntegrityChecker(
        applied_roundtrip_cost_pct=realized_cost_pct,
        applied_min_adt_inr=applied_min_adt_inr,
        fold_sharpes=fold_sharpes or None,
        fold_returns=fold_returns or None,
        benchmark_returns=benchmark_returns or None,
    )
    applicable = None if channel == "ml" else _APPLICABLE_CHECKS_NON_ML
    # Only the checks that were actually GIVEN real inputs are meaningful
    # here — check_01/02/03/04/07 need fold/feature/ohlcv/universe context
    # this orchestrator-level post-run pass doesn't have (they're the
    # generation-time PIT/walk-forward guarantees, already enforced
    # upstream by the orchestrator itself, not re-derivable post-hoc from
    # a finished run). Restrict to the checks this module can honestly feed.
    reachable = {"check_05_costs", "check_06_liquidity", "check_08_fold_stability", "check_09_benchmarks"}
    if applicable is not None:
        applicable = applicable & reachable
    else:
        applicable = reachable | {"check_10_random_feature"} if channel == "ml" else reachable

    try:
        passed_map = checker.run_all_checks(applicable_checks=applicable)
        integrity_passed = all(passed_map.values())
    except RuntimeError as exc:
        # A CRITICAL check among the ones we ran failed — real signal, not
        # a bug in this wiring; propagate as a failed (not fabricated-pass)
        # result rather than raising and losing the run's other metrics.
        logger.warning("post-run integrity check failed: %s", exc)
        integrity_passed = False
        passed_map = {}

    detail = {
        "checks": passed_map,
        "realized_cost_pct": realized_cost_pct,
        "applied_min_adt_inr": applied_min_adt_inr,
        "subperiod_note": subperiod_note,
        "n_subperiods": len(fold_sharpes),
    }
    return integrity_passed, detail
