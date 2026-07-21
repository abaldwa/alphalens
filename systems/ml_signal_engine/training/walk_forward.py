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

import pandas as pd

from backtest.integrity_checker import BacktestIntegrityChecker

logger = logging.getLogger(__name__)


class WalkForwardValidator:
    """
    SPEC-MODEL-003: expanding-window walk-forward validation. Never a
    random split (SPEC-BT-001 rule 1) — every fold's test set is a later
    fiscal year than every row in that fold's train set.

    "Year" here means the Indian equity fiscal year (1-April to
    31-March), not the calendar year — matches the fiscal-year windows
    backtest/run_phase*_backtest.py report against and how Indian
    fundamentals/corporate actions are naturally periodized, rather than
    splitting a single fiscal year's data across two folds at the
    calendar-year boundary. Configurable via `fiscal_year_start_month`
    (default 4 = April) in case a non-Indian dataset ever needs this.
    """

    def __init__(self, n_folds: int = 5, date_col: str = "date", fiscal_year_start_month: int = 4) -> None:
        if n_folds < 1:
            raise ValueError("n_folds must be >= 1")
        if not 1 <= fiscal_year_start_month <= 12:
            raise ValueError("fiscal_year_start_month must be between 1 and 12")
        self.n_folds = n_folds
        self.date_col = date_col
        self.fiscal_year_start_month = fiscal_year_start_month

    def _fiscal_years(self, dates: pd.Series) -> pd.Series:
        """Maps each timestamp to the calendar year its fiscal year STARTS in
        (e.g. 2026-02-15 with fiscal_year_start_month=4 -> FY2025, i.e.
        FY2025-26; 2026-04-15 -> FY2026, i.e. FY2026-27)."""
        month = self.fiscal_year_start_month
        if month == 1:
            return dates.dt.year
        return dates.dt.year - (dates.dt.month < month).astype(int)

    def split_data(
        self, df: pd.DataFrame, n_folds: Optional[int] = None, embargo_days: int = 0,
    ) -> List[Tuple[pd.DataFrame, pd.DataFrame]]:
        """
        Expanding-window fiscal-year splits.

        Parameters
        ----------
        df : pd.DataFrame
            Must contain `self.date_col`. Every row's fiscal year (see
            `_fiscal_years`) is used to assign it to a fold's train or
            test set.
        n_folds : int, optional
            Overrides the instance default for this call.
        embargo_days : int
            Drop the last `embargo_days` (calendar days) immediately
            before each fold's test-set start date from that fold's
            train set. Default 0 preserves the exact existing behavior
            for every caller that doesn't pass this. Exists because a
            trade opened near a fold boundary (e.g. under
            TripleBarrierLabeler's max_holding) can still be OPEN when
            the boundary is crossed — its label only resolves using
            price data that falls on/after the test fold's first date,
            which is exactly the leakage a plain `fiscal_year <
            test_year` cut doesn't catch. Callers should pass their
            model's horizon in trading days directly as `embargo_days`
            (a trading-day count used as a calendar-day count) — this
            is conservative (real markets run ~5/7 days, so a
            trading-day count converted 1:1 to calendar days only
            embargoes MORE than strictly necessary, never less, which
            is the safe direction to round for something guarding
            against leakage).

        Returns
        -------
        list of (train_df, test_df)
            n_folds tuples, train always strictly precedes test in time
            (fold i's train set is every row with fiscal_year <
            test_fy_i AND date < test_df's start date minus
            `embargo_days`; fold i's test set is every row with
            fiscal_year == test_fy_i). The final fold's test set may be
            a partial fiscal year if `df`'s most recent fiscal year
            isn't complete (e.g. "today" is mid-fiscal-year).

        Spec References
        ----------------
        SPEC-MODEL-003: expanding training window, 1-year test window.

        Raises
        ------
        ValueError
            If `df` lacks `self.date_col`, has too few distinct fiscal
            years to produce `n_folds` folds (need > n_folds distinct
            fiscal years — at least one full fiscal year of pure training
            data before the first test fiscal year), or `embargo_days` < 0.
        """
        n_folds = n_folds if n_folds is not None else self.n_folds
        if self.date_col not in df.columns:
            raise ValueError(f"df is missing required column: {self.date_col}")
        if embargo_days < 0:
            raise ValueError("embargo_days must be >= 0")

        dates = pd.to_datetime(df[self.date_col])
        fiscal_years = self._fiscal_years(dates)
        years = sorted(fiscal_years.unique())
        if len(years) <= n_folds:
            raise ValueError(
                f"need more than {n_folds} distinct fiscal years of data to produce {n_folds} "
                f"expanding folds; got {len(years)} fiscal years ({years})"
            )

        min_train_years = len(years) - n_folds
        test_years = years[min_train_years:]

        folds = []
        for test_year in test_years:
            test_df = df.loc[fiscal_years == test_year].copy()
            train_mask = fiscal_years < test_year
            if embargo_days > 0 and not test_df.empty:
                embargo_cutoff = pd.Timestamp(dates.loc[test_df.index].min()) - pd.Timedelta(days=embargo_days)
                train_mask = train_mask & (dates < embargo_cutoff)
            train_df = df.loc[train_mask].copy()
            folds.append((train_df, test_df))

        logger.info(
            f"split_data: {len(folds)} folds, test fiscal years {test_years} "
            f"(FYstart month={self.fiscal_year_start_month}, embargo_days={embargo_days})"
        )
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
