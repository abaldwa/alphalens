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


def realized_cost_and_liquidity(
    trades: List[Any], data_gaps: List[Dict[str, Any]],
) -> Tuple[Optional[float], Optional[float]]:
    """Derive (realized_cost_pct, applied_min_adt_inr) from what this run
    ACTUALLY did, not a config constant asserted to equal itself.

    realized_cost_pct: sum(Trade.cost_inr) / sum(entry turnover) across
    every closed trade — Trade.cost_inr is the real per-trade cost
    IndianTransactionCosts already deducted during execution (backtest/
    portfolio.py::PortfolioSimulator._close). None with zero trades
    (nothing to derive from — integrity_checker.py's check_05 correctly
    fails on None, not a fabricated pass).

    applied_min_adt_inr: [BUG FIX, 5th fundamental-strategies review,
    item 4] previously this unconditionally echoed back the config
    constant MIN_ADT_INR whenever no "no_adtv_data_position_sized_
    uncapped" gap was recorded — i.e. the persisted "applied" figure was
    never actually DERIVED from real per-ticker ADTV, only asserted equal
    to the constant it should have been compared against. Trade.adtv_cr
    (backtest/portfolio.py — populated at buy time from Signal.adtv_cr,
    see core/portfolio.py::StrategyPortfolio.buy/_close) now carries the
    REAL ADTV (INR crore) each executed trade was sized/capped against.
    This returns the minimum such value actually observed across this
    run's trades (INR, converted from crore) — the tightest liquidity
    constraint genuinely enforced this run, not a constant echoed back.
    Falls back to echoing MIN_ADT_INR (unchanged prior behavior) ONLY
    when no trade carries a real adtv_cr at all (e.g. Trade objects
    predating this field, or a channel that genuinely never populates
    Signal.adtv_cr) — that fallback case's detail string tags itself
    "unverified_against_real_adtv" so a reader can tell "verified against
    real per-trade data" apart from "no real data to verify against". If
    even one buy bypassed the cap outright (uncapped_gap), liquidity
    enforcement was NOT applied for every trade this run made, so this
    still returns 0.0 — an honest fail, not a value that would trivially
    pass check_06_liquidity.
    """
    if not trades:
        return None, None
    total_turnover = sum(t.entry_price * t.quantity for t in trades)
    if total_turnover <= 0:
        return None, None
    total_cost = sum(t.cost_inr for t in trades)
    realized_cost_pct = total_cost / total_turnover

    uncapped_gap = any(g.get("reason") == "no_adtv_data_position_sized_uncapped" for g in data_gaps)
    if uncapped_gap:
        return realized_cost_pct, 0.0

    real_adtv_cr_values = [
        getattr(t, "adtv_cr", None) for t in trades if getattr(t, "adtv_cr", None) is not None
    ]
    if real_adtv_cr_values:
        applied_min_adt_inr = min(real_adtv_cr_values) * 1e7  # crore -> INR
    else:
        applied_min_adt_inr = float(MIN_ADT_INR)

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


# [BUG FIX, 6th fundamental-strategies review, item 2] check_08_fold_stability
# / check_09_benchmarks are only meaningful (std/comparison across multiple
# independent periods) with at least this many subperiods — the same
# informal threshold the reviewer's real-DB probe used (1 segment under the
# CLI's default regime-method vs. 21 under a finer one). Below this, the
# checks can still technically run/pass/fail, but a PASS is structurally
# indistinguishable from "nothing was actually tested" — surfaced loudly
# below rather than left silent.
MIN_MEANINGFUL_SUBPERIODS = 3


def subperiod_check_inputs(
    equity_curve: pd.Series, trades: List[Any], run_start, run_end,
    regime_segments: List[Dict[str, Any]], regime_conn=None, regime_index_name: str = "Nifty 500",
    regime_method: Optional[str] = None,
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
    if len(rows) < MIN_MEANINGFUL_SUBPERIODS:
        method_desc = regime_method or "default"
        insufficient_msg = (
            f"only {len(rows)} regime segment(s) found for regime_method={method_desc!r} over "
            f"[{run_start}, {run_end}] — check_08_fold_stability/check_09_benchmarks need at least "
            f"{MIN_MEANINGFUL_SUBPERIODS} to be statistically meaningful; a PASS here reflects "
            "'nothing to test', not 'strategy confirmed stable'"
        )
        logger.warning(insufficient_msg)
        detail_note = f"{detail_note}; {insufficient_msg}"
    return fold_sharpes, fold_returns, benchmark_returns, detail_note


def run_post_run_integrity(
    channel: str, trades: List[Any], data_gaps: List[Dict[str, Any]],
    equity_curve: pd.Series, run_start, run_end,
    regime_segments: Optional[List[Dict[str, Any]]] = None, regime_conn=None,
    regime_index_name: str = "Nifty 500", regime_method: Optional[str] = None,
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
    # [BUG FIX, 5th fundamental-strategies review, item 4] lets a reader
    # of the persisted detail JSON tell "applied_min_adt_inr was verified
    # against real per-trade ADTV data" apart from "no trade carried real
    # ADTV data, so the config constant was echoed back unverified" —
    # otherwise both cases look identical in the audit trail.
    applied_min_adt_inr_verified_against_real_data = any(
        getattr(t, "adtv_cr", None) is not None for t in trades
    )
    fold_sharpes, fold_returns, benchmark_returns, subperiod_note = subperiod_check_inputs(
        equity_curve, trades, run_start, run_end, regime_segments or [], regime_conn, regime_index_name,
        regime_method,
    )

    # [2026-07-28 third model-review, item 3] Scale check_12's minimum-trades
    # floor with this run's actual duration instead of the fixed constant
    # default — a 4-year run and a 3-month run shouldn't be held to the same
    # absolute trade count. ~60 calendar days per required trade is a
    # conservative approximation (comfortably below even a single monthly
    # rebalance's expected trade cadence for a top-10 portfolio), floored at
    # the checker's own MIN_TRADES_FLOOR (5) so short runs aren't penalized
    # below the pre-existing baseline.
    calendar_days = max((run_end - run_start).days, 0)
    duration_scaled_floor = max(BacktestIntegrityChecker.MIN_TRADES_FLOOR, calendar_days // 60)

    checker = BacktestIntegrityChecker(
        applied_roundtrip_cost_pct=realized_cost_pct,
        applied_min_adt_inr=applied_min_adt_inr,
        fold_sharpes=fold_sharpes or None,
        fold_returns=fold_returns or None,
        benchmark_returns=benchmark_returns or None,
        n_trades=len(trades),
        min_trades_floor_override=duration_scaled_floor,
    )
    applicable = None if channel == "ml" else _APPLICABLE_CHECKS_NON_ML
    # Only the checks that were actually GIVEN real inputs are meaningful
    # here — check_01/02/03/04/07 need fold/feature/ohlcv/universe context
    # this orchestrator-level post-run pass doesn't have (they're the
    # generation-time PIT/walk-forward guarantees, already enforced
    # upstream by the orchestrator itself, not re-derivable post-hoc from
    # a finished run). Restrict to the checks this module can honestly feed.
    reachable = {
        "check_05_costs", "check_06_liquidity", "check_08_fold_stability",
        "check_09_benchmarks", "check_12_flat_equity_curve",
    }
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
        #
        # [BUG FIX, 5th fundamental-strategies review, item 5] run_all_checks
        # computes every applicable check's result into self._results_cache
        # BEFORE it raises on a critical failure — but this handler used to
        # simply discard that fully-computed dict and persist "checks": {},
        # hiding which OTHER checks passed/failed on a run that failed for
        # one specific reason. Recover the per-check breakdown from the
        # checker's own cache instead of losing it.
        logger.warning("post-run integrity check failed: %s", exc)
        integrity_passed = False
        cached = getattr(checker, "_results_cache", None) or {}
        passed_map = {name: result.passed for name, result in cached.items()}

    detail = {
        "checks": passed_map,
        "realized_cost_pct": realized_cost_pct,
        "applied_min_adt_inr": applied_min_adt_inr,
        "applied_min_adt_inr_verified_against_real_data": applied_min_adt_inr_verified_against_real_data,
        "subperiod_note": subperiod_note,
        "n_subperiods": len(fold_sharpes),
        "insufficient_subperiods_for_meaningful_check": len(fold_sharpes) < MIN_MEANINGFUL_SUBPERIODS,
    }
    return integrity_passed, detail
