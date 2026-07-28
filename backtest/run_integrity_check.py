"""
backtest/run_integrity_check.py

Phase: Fundamental Strategy Catalog backtest validation (2026-07-25)
Owner: Platform / Backtest
Consumers: Part C of the fundamental-strategy-catalog backtest plan (all
26 STRATEGY_CATALOG strategies); reusable for any channel, not fundamental-specific.

BacktestIntegrityChecker (backtest/integrity_checker.py) and
WalkForwardValidator (systems/ml_signal_engine/training/walk_forward.py)
both already exist, but nothing in this codebase automatically wires them
up for a completed BacktestOrchestrator run — every field
(folds/fold_sharpes/fold_returns/benchmark_returns/applied cost) is
caller-supplied. This module is that missing wiring: given a completed
run's in-memory equity curve (BacktestRunResult.equity_curve, a
date-indexed pd.Series of portfolio value) plus a benchmark equity curve
over the same window, it builds walk-forward folds via the EXISTING
WalkForwardValidator (no fold-split logic duplicated here) and calls its
EXISTING run_integrity_checks(), which delegates to
BacktestIntegrityChecker.run_all_checks() (also not duplicated here).

Deliberately reads only equity curves, not database-persisted summary
rows (backtest_runs only stores aggregate metrics, not a daily
equity-curve series) — this must be called with the in-memory
BacktestRunResult from the SAME run() call the orchestrator just
produced, before that object goes out of scope, not reconstructed later
from a DB row.
"""

import logging
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

from config.settings import MIN_ADT_INR, TOTAL_ROUNDTRIP_COST
from systems.ml_signal_engine.training.walk_forward import WalkForwardValidator

logger = logging.getLogger(__name__)

TRADING_DAYS_PER_YEAR = 252


def build_fold_metrics(
    equity_curve: pd.Series, n_folds: int = 3, embargo_days: int = 0,
) -> Tuple[List[float], List[float], List[Tuple[pd.DataFrame, pd.DataFrame]]]:
    """
    Fold-level (annualized Sharpe, total return) pairs computed from one
    equity curve, using WalkForwardValidator.split_data() for the actual
    fold boundaries (not reimplemented here).

    Parameters
    ----------
    equity_curve : pd.Series
        Date-indexed portfolio (or benchmark) value series, e.g.
        BacktestRunResult.equity_curve.
    n_folds : int
        Passed through to WalkForwardValidator — needs more distinct
        fiscal years of data than this to avoid raising (see that
        class's split_data docstring).
    embargo_days : int
        Passed through to WalkForwardValidator.split_data.

    Returns
    -------
    (fold_sharpes, fold_returns, folds)
        One (sharpe, return) pair per fold, in fold order, plus the raw
        (train_df, test_df) fold list itself — the latter is needed
        as-is for BacktestIntegrityChecker's check_01_walk_forward, which
        checks chronological ordering directly on the fold frames, not
        on derived metrics. A fold with fewer than 2 test observations
        contributes (0.0, 0.0) to the metrics rather than raising — a
        too-short test window is a real (if degenerate) fold outcome,
        not missing data.
    """
    df = pd.DataFrame({"date": pd.to_datetime(equity_curve.index), "value": equity_curve.to_numpy()})
    folds = WalkForwardValidator(n_folds=n_folds).split_data(df, n_folds=n_folds, embargo_days=embargo_days)

    fold_sharpes: List[float] = []
    fold_returns: List[float] = []
    for _, test_df in folds:
        if len(test_df) < 2 or test_df["value"].iloc[0] == 0:
            fold_sharpes.append(0.0)
            fold_returns.append(0.0)
            continue
        daily_returns = test_df["value"].pct_change().dropna()
        total_return = float(test_df["value"].iloc[-1] / test_df["value"].iloc[0] - 1.0)
        sharpe = (
            float(daily_returns.mean() / daily_returns.std() * np.sqrt(TRADING_DAYS_PER_YEAR))
            if daily_returns.std() > 0 else 0.0
        )
        fold_sharpes.append(sharpe)
        fold_returns.append(total_return)
    return fold_sharpes, fold_returns, folds


def run_strategy_integrity_check(
    equity_curve: pd.Series,
    benchmark_equity_curve: pd.Series,
    feature_df: Optional[pd.DataFrame] = None,
    ohlcv_df: Optional[pd.DataFrame] = None,
    universe_tickers: Optional[Set[str]] = None,
    historical_tickers: Optional[Set[str]] = None,
    applied_roundtrip_cost_pct: Optional[float] = None,
    applied_min_adt_inr: Optional[float] = None,
    hpo_dataset: Optional[str] = None,
    n_folds: int = 3,
    embargo_days: int = 0,
    n_trades: Optional[int] = None,
) -> Dict[str, bool]:
    """
    Run every SPEC-BT-001 integrity check (backtest/integrity_checker.py)
    against one completed backtest run, given its equity curve and a
    benchmark equity curve over the same window. Wires all 7
    CRITICAL_CHECKS, not just the walk-forward/Sharpe/benchmark trio —
    see Caveat below for the one thing it still can't fabricate for you.

    Parameters
    ----------
    equity_curve : pd.Series
        The strategy's date-indexed portfolio value (BacktestRunResult.equity_curve).
    benchmark_equity_curve : pd.Series
        A buy-and-hold benchmark's equity curve over the SAME date range
        (e.g. Nifty 500 — caller's responsibility to fetch and align;
        this module does not fabricate one).
    feature_df : pd.DataFrame, optional
        Needs a `date` column and either `announcement_date`/`filing_date`
        (for real fundamentals-derived columns) for check_02_pit to pass.
    ohlcv_df : pd.DataFrame, optional
        Needs an `adj_factor` column for check_03_corp_actions.
    universe_tickers, historical_tickers : set, optional
        For check_04_survivorship — `historical_tickers` must include at
        least ~1% delisted/removed names not in `universe_tickers`, or
        this fails on purpose (see that check's own docstring).
    applied_roundtrip_cost_pct : float, optional
        Defaults to config.settings.TOTAL_ROUNDTRIP_COST if not supplied.
    applied_min_adt_inr : float, optional
        Defaults to config.settings.MIN_ADT_INR if not supplied — pass the
        ACTUAL liquidity floor the backtest applied, not just this default,
        if the run used something stricter.
    hpo_dataset : str, optional
        Defaults to "none" — true for every one of these 26 strategies
        (hardcoded, documented-not-tuned weights; no HPO step exists to
        touch the test fold). Only pass something containing "train"/
        "validation" if a real HPO step actually ran.
    n_folds, embargo_days
        Passed through to build_fold_metrics for both curves.
    n_trades : int, optional
        Total closed-trade count for the whole run (not per-fold) — feeds
        check_12_flat_equity_curve's minimum-trade-count floor (2026-07-28
        second model-review). None (default) never fails that check on its
        own; only pass this if the caller actually has a real trade list to
        count (never a fabricated/estimated value).

    Returns
    -------
    dict
        {check_name: passed} for all checks WalkForwardValidator.
        run_integrity_checks() runs — see that method's docstring.

    Raises
    ------
    RuntimeError
        Propagated from BacktestIntegrityChecker.run_all_checks() if any
        CRITICAL_CHECKS check fails — including simply omitting one of
        the optional-but-required-for-that-check arguments above.

    Caveat
    ------
    None of this function's checks can substitute real context with a
    default that makes the check meaningless — e.g. passing an
    `applied_min_adt_inr` that doesn't match what the backtest actually
    enforced would make check_06 pass on paper while lying about what ran.
    The defaults above are genuinely correct defaults for these 26
    strategies specifically (no HPO, config's own cost/liquidity floors),
    not placeholders to silence the checker.
    """
    fold_sharpes, fold_returns, folds = build_fold_metrics(equity_curve, n_folds=n_folds, embargo_days=embargo_days)
    _, benchmark_fold_returns, _ = build_fold_metrics(benchmark_equity_curve, n_folds=n_folds, embargo_days=embargo_days)

    results: Dict[str, Any] = {
        "folds": folds,
        "feature_df": feature_df,
        "ohlcv_df": ohlcv_df,
        "universe_tickers": universe_tickers,
        "historical_tickers": historical_tickers,
        "applied_roundtrip_cost_pct": (
            applied_roundtrip_cost_pct if applied_roundtrip_cost_pct is not None else TOTAL_ROUNDTRIP_COST
        ),
        "applied_min_adt_inr": applied_min_adt_inr if applied_min_adt_inr is not None else MIN_ADT_INR,
        "hpo_dataset": hpo_dataset if hpo_dataset is not None else "none",
        "fold_sharpes": fold_sharpes,
        "fold_returns": fold_returns,
        "benchmark_returns": benchmark_fold_returns,
        "n_trades": n_trades,
    }
    return WalkForwardValidator(n_folds=n_folds).run_integrity_checks(results)
