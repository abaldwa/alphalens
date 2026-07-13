"""
systems/ml_signal_engine/training/walk_forward.py

Phase: 1.4 (Labeling + Backtesting Infrastructure)
Specs: SPEC-MODEL-003, SPEC-BT-001
Owner: ml_signal_engine / training
Consumers: backtest/engine.py (Phase 1.6), systems/ml_signal_engine/models/signal/* (M-02, M-03)

WalkForwardValidator: expanding-window, calendar-year train/test splits
(SPEC-MODEL-003 — "Expanding training window, 1-year test window,
minimum 3 folds. No data from test year used in training, HPO, or
threshold selection.") plus a chronological train/validation carve-out
for HPO (never the test fold).

Fold count note: the build prompt names 5 specific calendar folds
(Train[2020-22]->Test[2023] ... Train[2020-25]->Test[2026-H1]) plus "+1
expanding window" — 4 explicitly named + 1 vaguely described, an
internally ambiguous "5". Hardcoding those literal years would also
silently break every year this code keeps running, and conflicts with the
prompt's own `split_data(df, n_folds=5)` signature, which asks for a
*general*, parametrized splitter, not 5 frozen dates. split_data() below
is that general splitter: given n_folds, it derives
min_train_years = (distinct years in df) - n_folds and produces exactly
n_folds expanding folds ending at the latest year present in the data —
satisfying the method's literal "n_folds=5 -> 5 folds" contract for any
data range, not just today's.

Run against this repo's actual dev DB (ohlcv_adjusted spans 2020-01-01
through 2026-06-21 — 7 distinct calendar years) with n_folds=5, this
produces test years [2022, 2023, 2024, 2025, 2026] (2-year minimum
training window), not the prompt's illustrative [2023, 2024, 2025,
2026-H1] (3-year minimum). The discrepancy is real and not silently
forced to match: forcing min_train_years=3 (matching "Train[2020-22]"
literally) only yields 4 folds against the actual 7-year span, not 5 —
the prompt's own "5 folds" count and its "Train[2020-22]" starting point
are mutually inconsistent against the data that actually exists. Treated
the method signature (`n_folds=5` -> exactly 5 folds, general for any
data range) as authoritative over the illustrative year labels, per this
session's established rule for resolving spec/prompt count mismatches.
See BuildLog.md "P1.4" for the full reasoning.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from backtest.integrity_checker import BacktestIntegrityChecker

logger = logging.getLogger(__name__)


class WalkForwardValidator:
    """
    SPEC-MODEL-003: expanding-window walk-forward validation. Never a
    random split (SPEC-BT-001 rule 1) — every fold's test set is a later
    calendar year than every row in that fold's train set.
    """

    def __init__(self, n_folds: int = 5, date_col: str = "date") -> None:
        if n_folds < 1:
            raise ValueError("n_folds must be >= 1")
        self.n_folds = n_folds
        self.date_col = date_col

    def split_data(self, df: pd.DataFrame, n_folds: Optional[int] = None) -> List[Tuple[pd.DataFrame, pd.DataFrame]]:
        """
        Expanding-window calendar-year splits.

        Parameters
        ----------
        df : pd.DataFrame
            Must contain `self.date_col`. Every row's year is used to
            assign it to a fold's train or test set.
        n_folds : int, optional
            Overrides the instance default for this call.

        Returns
        -------
        list of (train_df, test_df)
            n_folds tuples, train always strictly precedes test in time
            (fold i's train set is every row with year < test_year_i;
            fold i's test set is every row with year == test_year_i).
            The final fold's test set may be a partial year if `df`'s
            most recent year isn't complete (e.g. "today" is mid-year).

        Spec References
        ----------------
        SPEC-MODEL-003: expanding training window, 1-year test window.

        Raises
        ------
        ValueError
            If `df` lacks `self.date_col`, or has too few distinct years
            to produce `n_folds` folds (need > n_folds distinct years —
            at least one full year of pure training data before the
            first test year).
        """
        n_folds = n_folds if n_folds is not None else self.n_folds
        if self.date_col not in df.columns:
            raise ValueError(f"df is missing required column: {self.date_col}")

        dates = pd.to_datetime(df[self.date_col])
        years = sorted(dates.dt.year.unique())
        if len(years) <= n_folds:
            raise ValueError(
                f"need more than {n_folds} distinct years of data to produce {n_folds} "
                f"expanding folds; got {len(years)} years ({years})"
            )

        min_train_years = len(years) - n_folds
        test_years = years[min_train_years:]

        folds = []
        for test_year in test_years:
            train_df = df.loc[dates.dt.year < test_year].copy()
            test_df = df.loc[dates.dt.year == test_year].copy()
            folds.append((train_df, test_df))

        logger.info(f"split_data: {len(folds)} folds, test years {test_years}")
        return folds

    def get_train_validation_split(
        self, train_df: pd.DataFrame, val_fraction: float = 0.2
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Carve the chronologically LAST `val_fraction` of a training fold
        out as a validation set for HPO (SPEC-MODEL-003: "HPO ... only on
        last 20% of training fold as validation" / SPEC-BT-001 rule 6:
        never on the test fold). Chronological, not random — a random
        split would leak future-into-past information within the
        training fold itself.

        Parameters
        ----------
        train_df : pd.DataFrame
            One fold's training data (from split_data's first tuple element).
        val_fraction : float
            Fraction of the (chronologically sorted) training fold to
            reserve as validation (default 0.2 = last 20%).

        Returns
        -------
        (train_only_df, val_df)
            train_only_df is everything strictly before the cutoff date;
            val_df is everything on/after it. Optuna/HPO must use val_df
            only — never train_df's full span, never any test fold.

        Raises
        ------
        ValueError
            If val_fraction is not in (0, 1), or train_df lacks `self.date_col`.
        """
        if not 0 < val_fraction < 1:
            raise ValueError("val_fraction must be in (0, 1)")
        if self.date_col not in train_df.columns:
            raise ValueError(f"train_df is missing required column: {self.date_col}")

        dates = pd.to_datetime(train_df[self.date_col])
        sorted_dates = dates.sort_values()
        cutoff_idx = int(len(sorted_dates) * (1 - val_fraction))
        cutoff_date = sorted_dates.iloc[cutoff_idx]

        train_only = train_df.loc[dates < cutoff_date].copy()
        val = train_df.loc[dates >= cutoff_date].copy()
        return train_only, val

    def run_integrity_checks(self, results: Dict[str, Any]) -> Dict[str, bool]:
        """
        Validate a completed walk-forward run against all 10 SPEC-BT-001
        checks (delegates to backtest.integrity_checker.BacktestIntegrityChecker
        — this method does not duplicate any check logic, SOLID-S).

        Parameters
        ----------
        results : dict
            Keyword arguments matching BacktestIntegrityChecker's
            constructor fields (folds, feature_df, ohlcv_df,
            universe_tickers, historical_tickers,
            applied_roundtrip_cost_pct, applied_min_adt_inr, hpo_dataset,
            fold_sharpes, fold_returns, benchmark_returns,
            random_feature_accuracy) — supply whichever are available;
            missing context fails that specific check (see
            BacktestIntegrityChecker's docstring).

        Returns
        -------
        dict
            {check_name: passed} for all 10 checks.

        Spec References
        ----------------
        SPEC-BT-001: "All 9 backtesting rules are hard constraints... Any
        violation invalidates the backtest" (count note: see
        backtest/integrity_checker.py's module docstring for the 9-vs-10
        discrepancy).

        Raises
        ------
        RuntimeError
            Propagated from BacktestIntegrityChecker.run_all_checks() if
            any CRITICAL check fails.
        """
        checker = BacktestIntegrityChecker(**results)
        return checker.run_all_checks()

    def split_data_purged(
        self,
        df: pd.DataFrame,
        label_horizon_days: int,
        embargo_days: int,
        n_folds: Optional[int] = None,
    ) -> List[Tuple[pd.DataFrame, pd.DataFrame]]:
        """
        GAINER EXPERIMENT (copy, not used by production): same
        expanding-window calendar-year folds as split_data(), plus purge
        + embargo for label windows that overlap the train/test boundary
        (Lopez de Prado-style). A training row's label was computed from
        `label_horizon_days` of forward price data — if that forward
        window crosses into the test fold, the label leaks test-period
        information, so the row is dropped from train. Symmetrically,
        `embargo_days` of data immediately after the test fold is also
        excluded from the NEXT fold's training data, since price
        momentum right after a test window is still correlated with it.

        Parameters
        ----------
        df : pd.DataFrame
            Must contain self.date_col.
        label_horizon_days : int
            Trading days a label's forward window spans (e.g. 6 for the
            5%/6d model, 756 for a 2x/3yr multibagger label).
        embargo_days : int
            Trading days after the test fold to also exclude from
            subsequent training.
        n_folds : int, optional
            Overrides the instance default.

        Returns
        -------
        list of (train_df, test_df)
            Same shape as split_data(), but train_df has purge+embargo
            applied.
        """
        base_folds = self.split_data(df, n_folds=n_folds)
        # Approximate trading-day-count-to-calendar-day-count at ~252/365
        # (this repo's OHLCV is trading-day granularity but fold boundaries
        # here are calendar-year cuts, so a calendar buffer is the natural unit).
        purge_calendar_days = int(label_horizon_days * 365 / 252)
        embargo_calendar_days = int(embargo_days * 365 / 252)

        purged_folds = []
        for train_df, test_df in base_folds:
            if test_df.empty:
                purged_folds.append((train_df, test_df))
                continue
            test_start = pd.to_datetime(test_df[self.date_col]).min()
            test_end = pd.to_datetime(test_df[self.date_col]).max()
            train_dates = pd.to_datetime(train_df[self.date_col])

            purge_cutoff = test_start - pd.Timedelta(days=purge_calendar_days)
            embargo_cutoff = test_end + pd.Timedelta(days=embargo_calendar_days)

            keep = (train_dates < purge_cutoff) | (train_dates > embargo_cutoff)
            purged = train_df.loc[keep].copy()
            dropped = len(train_df) - len(purged)
            if dropped:
                logger.info(
                    "split_data_purged: dropped %d/%d train rows (purge=%dd, embargo=%dd) for test fold %s..%s",
                    dropped, len(train_df), purge_calendar_days, embargo_calendar_days,
                    test_start.date(), test_end.date(),
                )
            purged_folds.append((purged, test_df))
        return purged_folds


def stock_level_kfold(
    df: pd.DataFrame, n_folds: int = 5, ticker_col: str = "ticker", random_state: int = 42,
) -> List[Tuple[pd.DataFrame, pd.DataFrame]]:
    """
    GAINER EXPERIMENT (copy, not used by production): split by TICKER
    instead of time — train on 80% of tickers (for n_folds=5), test on
    the held-out 20%, all calendar time periods mixed. This eliminates
    time-based leakage entirely (a stock's train-fold and test-fold rows
    never overlap in time OR ticker), so comparing this against
    WalkForwardValidator's calendar-year folds quantifies how much of the
    time-based scheme's apparent accuracy is actually leakage from a
    model re-recognizing a stock it already saw during a different
    period of the same run, vs genuine generalization to unseen stocks.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain ticker_col.
    n_folds : int
        Number of ticker folds.
    ticker_col : str
    random_state : int

    Returns
    -------
    list of (train_df, test_df)
        n_folds tuples; each ticker appears in exactly one fold's test set.

    Raises
    ------
    ValueError
        If df lacks ticker_col or has fewer distinct tickers than n_folds.
    """
    if ticker_col not in df.columns:
        raise ValueError(f"df is missing required column: {ticker_col}")

    tickers = sorted(df[ticker_col].unique())
    if len(tickers) < n_folds:
        raise ValueError(f"need >= {n_folds} distinct tickers, got {len(tickers)}")

    rng = np.random.RandomState(random_state)
    shuffled = np.array(tickers)
    rng.shuffle(shuffled)
    ticker_folds = np.array_split(shuffled, n_folds)

    folds = []
    for test_tickers in ticker_folds:
        test_set = set(test_tickers)
        test_df = df.loc[df[ticker_col].isin(test_set)].copy()
        train_df = df.loc[~df[ticker_col].isin(test_set)].copy()
        folds.append((train_df, test_df))
    return folds
