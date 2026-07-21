"""
backtest/iterative_retrain.py

Owner: Platform / Backtest
Consumers: backtest/run_iterative_backtest.py (CLI), datastore/api/routers/
backtest_runs.py (trigger endpoint)

Iterative MetaLabeler retraining loop: "run a backtest, retrain the
entry-filter model on the trades it took, run again, repeat" — without
letting the loop overfit to its own backtest history. See
BuildLog.md/conversation history for the reviewed design; summarized
here:

1. A full fiscal year is reserved as a HOLDOUT that no tuning iteration
   ever trains on or is selected using — evaluated exactly once, at the
   very end. `select_holdout_fiscal_year()` below picks WHICH fiscal
   year: not simply "the most recent complete one" but the most recent
   one whose longest-horizon trades are guaranteed to have fully
   resolved by today (a trade opened near a too-recent fiscal year's end
   might still be open, giving that year incomplete/provisional labels —
   using it as holdout would silently understate label quality). This
   is why, run today, a 1-year-horizon strategy's holdout lands one full
   fiscal year further back than a 5-day-horizon strategy's would.
2. Everything strictly BEFORE the holdout fiscal year is "tuning
   territory." Everything from the holdout year's start through today is
   excluded from training entirely (not just from the holdout) — the
   holdout must sit at the chronological tail of the data actually used,
   never in the middle, so tuning-territory walk-forward CV stays
   strictly chronological.
3. Each iteration is a full `BacktestEngine.run_full_backtest()` over
   tuning territory only, using the existing walk-forward CV
   (WalkForwardValidator's fiscal-year folds + embargo, already wired
   into BacktestEngine) — no parallel CV implementation here.
4. What varies between iterations: a small FIXED grid of MetaLabeler
   hyperparameters (see DEFAULT_HYPERPARAM_GRID) — not Optuna/random
   search, which would compound the multiple-testing problem the
   promotion gate exists to correct for.
5. Promotion gate: an iteration is kept only if it beats the current
   best on raw Sharpe, its deflated Sharpe ratio (corrected for the
   cumulative trial count so far) clears `min_dsr_threshold`, AND its
   MetaLabeler's mean `overfit_checks.random_feature_test` accuracy
   (averaged across folds, via `BacktestEngine.run_full_backtest(
   collect_fold_models=True)`) stays at/below `max_random_feature_accuracy`
   — a model that still scores durably above ~50% on permuted features is
   fitting noise in the real features, not signal, regardless of what its
   Sharpe/DSR say.
6. Stopping rule: plateau (N consecutive non-promoting iterations) or a
   max-iteration budget — never "until a target metric is hit." There is
   deliberately no target-metric parameter anywhere in this module.
7. Every iteration is written as its own `backtest_runs` row
   (parent_run_id shared across one loop invocation) for a full audit
   trail; the final holdout run gets its own row too, `mode="holdout_eval"`.
"""

import json
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd

from backtest.core.feature_log import FeatureLogWriter, query_feature_log
from backtest.engine import BacktestEngine, BacktestResults
from backtest.overfit_checks import deflated_sharpe_ratio, random_feature_test
from config.timezone import now_ist
from features.technical import CORE_TECHNICAL_FEATURES
from systems.ml_signal_engine.models.signal.meta_labeler import MetaLabeler
from systems.ml_signal_engine.training.walk_forward import WalkForwardValidator

logger = logging.getLogger(__name__)

# Small, fixed, enumerable grid — see module docstring item 4. 3 x 3 x 2 =
# 18 candidate configurations; class_weight="balanced" is always applied
# (MetaLabeler's own default — see meta_labeler.py) so it isn't a grid axis.
DEFAULT_HYPERPARAM_GRID: List[Dict[str, Any]] = [
    {
        "n_estimators": n_estimators, "max_depth": max_depth, "learning_rate": learning_rate,
        "class_weight": "balanced", "random_state": 42, "verbose": -1,
    }
    for n_estimators in (100, 200, 300)
    for max_depth in (3, 5, 7)
    for learning_rate in (0.03, 0.1)
]

DEFAULT_MAX_ITERATIONS = len(DEFAULT_HYPERPARAM_GRID)
DEFAULT_PLATEAU_PATIENCE = 5
DEFAULT_MIN_DSR_THRESHOLD = 0.95
# SPEC-BT-001's random-feature-test doc: 48-52% expected band for a model
# that isn't fitting noise; > 0.55 is that doc's stated noise-fitting
# threshold. Used as the promotion gate's ceiling (mean across folds).
DEFAULT_MAX_RANDOM_FEATURE_ACCURACY = 0.55
RANDOM_FEATURE_TEST_REPEATS = 5


@dataclass
class HoldoutSelection:
    """What `select_holdout_fiscal_year` decided, and why — the explainability
    record for "what data did this run drop and why" (item 4 of the review)."""

    holdout_fiscal_year: int  # calendar year the FY starts in (e.g. 2024 = FY2024-25)
    holdout_start: pd.Timestamp
    holdout_end: pd.Timestamp
    skipped_fiscal_years: List[int]  # complete FYs skipped as an unresolved-label buffer
    resolution_buffer_days: int
    as_of_date: pd.Timestamp

    def explain(self) -> str:
        skipped = ", ".join(f"FY{y}-{str(y + 1)[-2:]}" for y in self.skipped_fiscal_years) or "none"
        fy = self.holdout_fiscal_year
        return (
            f"Holdout = FY{fy}-{str(fy + 1)[-2:]} ({self.holdout_start.date()} to {self.holdout_end.date()}). "
            f"Skipped as an unresolved-label buffer (as of {self.as_of_date.date()}, "
            f"resolution_buffer_days={self.resolution_buffer_days}): {skipped}. "
            f"A more recent complete fiscal year was available but a trade opened near its end could still "
            f"be open today given this strategy's horizon, so its labels aren't fully resolved yet — using "
            f"it as holdout would silently grade against incomplete outcomes."
        )


def select_holdout_fiscal_year(
    as_of_date: pd.Timestamp, resolution_buffer_days: int, fiscal_year_start_month: int = 4,
    max_lookback_years: int = 8,
) -> HoldoutSelection:
    """
    Picks the most recent COMPLETE fiscal year whose trades are guaranteed
    to have fully resolved by `as_of_date`, given `resolution_buffer_days`
    (pass the strategy's horizon_days — trading-day count used as a
    calendar-day count, same conservative convention as
    WalkForwardValidator.split_data's embargo_days).

    Example: as_of_date=2026-07-21, resolution_buffer_days=252 (~1-year
    horizon) -> the most recent complete fiscal year (FY2025-26, ending
    2026-03-31) is skipped because 2026-03-31 + 252 days runs past
    2026-07-21 (its trades aren't all resolved yet); FY2024-25 (ending
    2025-03-31) is not skipped (2025-03-31 + 252 days clears
    2026-07-21) and becomes the holdout.

    Raises
    ------
    ValueError
        If no fiscal year within `max_lookback_years` clears the buffer
        (a pathologically long horizon relative to available history).
    """
    validator = WalkForwardValidator(fiscal_year_start_month=fiscal_year_start_month)
    as_of_date = pd.Timestamp(as_of_date)
    current_fy = int(validator._fiscal_years(pd.Series([as_of_date])).iloc[0])

    skipped: List[int] = []
    for offset in range(1, max_lookback_years + 1):
        candidate_fy = current_fy - offset
        fy_start = pd.Timestamp(year=candidate_fy, month=fiscal_year_start_month, day=1)
        fy_end = fy_start + pd.DateOffset(years=1) - pd.Timedelta(days=1)
        if fy_end + pd.Timedelta(days=resolution_buffer_days) <= as_of_date:
            return HoldoutSelection(
                holdout_fiscal_year=candidate_fy, holdout_start=fy_start, holdout_end=fy_end,
                skipped_fiscal_years=skipped, resolution_buffer_days=resolution_buffer_days,
                as_of_date=as_of_date,
            )
        skipped.append(candidate_fy)

    raise ValueError(
        f"no fiscal year within {max_lookback_years} years clears the {resolution_buffer_days}-day "
        f"resolution buffer as of {as_of_date.date()} — horizon is too long relative to available history"
    )


@dataclass
class IterationRecord:
    iteration: int
    run_id: str
    hyperparams: Dict[str, Any]
    sharpe_mean: float
    win_rate_mean: float
    n_trials_so_far: int
    dsr: float
    random_feature_accuracy: Optional[float]
    promoted: bool
    rejection_reason: Optional[str]
    runtime_seconds: float
    dropped_candidates: Dict[str, int]  # decision_taken -> count, from backtest_feature_log


@dataclass
class RetrainLoopResult:
    loop_run_id: str
    holdout_selection: HoldoutSelection
    iterations: List[IterationRecord]
    best_iteration_index: Optional[int]
    best_hyperparams: Optional[Dict[str, Any]]
    stopped_reason: str
    holdout_results: Optional[BacktestResults]
    holdout_run_id: Optional[str]
    holdout_runtime_seconds: Optional[float]
    total_runtime_seconds: float
    excluded_buffer_rows: int  # rows dropped entirely (too-recent-to-resolve buffer period)


class RetrainLoop:
    """Orchestrates the iterative MetaLabeler retrain loop described in the module docstring."""

    def __init__(
        self,
        engine_kwargs: Dict[str, Any],
        strategy_id: str,
        feature_log_writer: Optional[FeatureLogWriter] = None,
        conn: Optional[Any] = None,
        hyperparam_grid: Optional[List[Dict[str, Any]]] = None,
        max_iterations: int = DEFAULT_MAX_ITERATIONS,
        plateau_patience: int = DEFAULT_PLATEAU_PATIENCE,
        min_dsr_threshold: float = DEFAULT_MIN_DSR_THRESHOLD,
        max_random_feature_accuracy: float = DEFAULT_MAX_RANDOM_FEATURE_ACCURACY,
        folds: int = 4,
    ) -> None:
        """
        Parameters
        ----------
        engine_kwargs : dict
            Every BacktestEngine constructor kwarg EXCEPT feature_log_writer/
            run_id/meta_labeler_params (this loop sets those per-iteration) —
            ohlcv, pnd_detector, exit_model, signal_model_cls, sector_map,
            horizon_days, benchmark, universe_tickers, historical_tickers, etc.
        conn : DuckDB connection, optional
            Used to write per-iteration backtest_runs rows. None skips
            audit-row writing (e.g. for a dry-run/test invocation).
        """
        self.engine_kwargs = engine_kwargs
        self.strategy_id = strategy_id
        self.feature_log_writer = feature_log_writer
        self.conn = conn
        self.hyperparam_grid = hyperparam_grid or DEFAULT_HYPERPARAM_GRID
        self.max_iterations = min(max_iterations, len(self.hyperparam_grid))
        self.plateau_patience = plateau_patience
        self.min_dsr_threshold = min_dsr_threshold
        self.max_random_feature_accuracy = max_random_feature_accuracy
        self.folds = folds

    def run(self, combined_ohlcv_max_date: pd.Timestamp, as_of_date: Optional[pd.Timestamp] = None) -> RetrainLoopResult:
        """
        Parameters
        ----------
        combined_ohlcv_max_date : pd.Timestamp
            The most recent date real OHLCV data is actually available
            through — used as the default `as_of_date` for holdout
            selection (see select_holdout_fiscal_year) rather than
            wall-clock "now", since what actually determines whether a
            fiscal year's trades have resolved is data availability, not
            the calendar (real ingestion can lag "today" by days).
        as_of_date : pd.Timestamp, optional
            Overrides `combined_ohlcv_max_date` for holdout selection.
        """
        run_started = time.monotonic()
        as_of_date = pd.Timestamp(as_of_date) if as_of_date is not None else pd.Timestamp(combined_ohlcv_max_date)
        horizon_days = self.engine_kwargs.get("horizon_days", 5)

        holdout = select_holdout_fiscal_year(as_of_date, resolution_buffer_days=horizon_days)
        logger.info(f"iterative_retrain: {holdout.explain()}")

        loop_run_id = f"retrainloop_{as_of_date.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

        iterations: List[IterationRecord] = []
        best_score = float("-inf")
        best_iteration_index: Optional[int] = None
        best_hyperparams: Optional[Dict[str, Any]] = None
        n_trials_so_far = 0
        plateau_counter = 0
        stopped_reason = "max_iterations"

        excluded_buffer_rows = 0

        for i in range(self.max_iterations):
            hyperparams = self.hyperparam_grid[i]
            n_trials_so_far += 1
            iter_started = time.monotonic()
            iter_run_id = f"{loop_run_id}_iter{i}"

            engine = BacktestEngine(
                feature_log_writer=self.feature_log_writer, run_id=iter_run_id,
                meta_labeler_params=hyperparams,
                **self.engine_kwargs,
            )
            if i == 0 and excluded_buffer_rows == 0:
                # Rows strictly AFTER the holdout year's end — the
                # unresolved-label buffer (skipped fiscal years +
                # in-progress current fiscal year, see
                # select_holdout_fiscal_year's docstring) — are used
                # nowhere in this run: not in tuning (which stops at
                # holdout_start), not in the holdout evaluation (which
                # stops at holdout_end). Reported once (same _combined
                # panel is rebuilt per iteration, v1 accepts this
                # redundancy for per-iteration auditability — see
                # BuildLog.md — so the count is identical every time).
                excluded_buffer_rows = int((engine._combined["date"] > holdout.holdout_end).sum())

            results = engine.run_full_backtest(
                self.strategy_id, to_date=holdout.holdout_start - pd.Timedelta(days=1),
                folds=self.folds, collect_fold_returns=True, collect_fold_models=True,
            )

            sharpe = results.aggregate.get("sharpe_mean") or 0.0
            win_rate = results.aggregate.get("win_rate_mean") or 0.0
            n_obs = len(results.fold_returns) if results.fold_returns is not None else 0
            dsr = (
                deflated_sharpe_ratio(sharpe=sharpe, n_trials=n_trials_so_far, n_obs=max(n_obs, 2), returns=results.fold_returns)
                if n_obs >= 2 else 0.0
            )

            random_feature_accuracy = None
            promoted = False
            rejection_reason = None
            if dsr < self.min_dsr_threshold:
                rejection_reason = f"dsr {dsr:.3f} < min_dsr_threshold {self.min_dsr_threshold}"
            elif sharpe <= best_score:
                rejection_reason = f"sharpe {sharpe:.3f} did not beat current best {best_score:.3f}"
            else:
                # Only run the (relatively expensive — n_repeats fresh
                # LightGBM refits per fold) random-feature test once the
                # cheaper Sharpe/DSR checks already pass — no point
                # noise-testing a candidate that would be rejected anyway.
                random_feature_accuracy = self._mean_random_feature_accuracy(results)
                if random_feature_accuracy is None:
                    rejection_reason = "no fold_models collected — cannot run random_feature_test"
                elif random_feature_accuracy > self.max_random_feature_accuracy:
                    rejection_reason = (
                        f"random_feature_test accuracy {random_feature_accuracy:.3f} > "
                        f"max_random_feature_accuracy {self.max_random_feature_accuracy} — fitting noise, not signal"
                    )
                else:
                    promoted = True

            if promoted:
                best_score = sharpe
                best_iteration_index = i
                best_hyperparams = hyperparams
                plateau_counter = 0
            else:
                plateau_counter += 1

            dropped_candidates = self._dropped_candidate_counts(iter_run_id)

            iter_runtime = time.monotonic() - iter_started
            record = IterationRecord(
                iteration=i, run_id=iter_run_id, hyperparams=hyperparams, sharpe_mean=sharpe,
                win_rate_mean=win_rate, n_trials_so_far=n_trials_so_far, dsr=dsr,
                random_feature_accuracy=random_feature_accuracy, promoted=promoted,
                rejection_reason=rejection_reason, runtime_seconds=iter_runtime,
                dropped_candidates=dropped_candidates,
            )
            iterations.append(record)
            logger.info(
                f"iterative_retrain: iteration {i} ({iter_runtime:.1f}s) sharpe={sharpe:.3f} "
                f"dsr={dsr:.3f} promoted={promoted}{'' if promoted else f' ({rejection_reason})'}"
            )

            self._write_backtest_run_row(
                run_id=iter_run_id, parent_run_id=loop_run_id, mode="tuning",
                start_date=holdout.holdout_start - pd.Timedelta(days=365 * 10), end_date=holdout.holdout_start,
                config={"hyperparams": hyperparams, "folds": self.folds, "horizon_days": horizon_days},
                results=results,
                extra_metrics={
                    "dsr": dsr, "n_trials_so_far": n_trials_so_far, "promoted": promoted,
                    "random_feature_accuracy": random_feature_accuracy,
                },
            )

            if plateau_counter >= self.plateau_patience:
                stopped_reason = "plateau"
                break

        holdout_results = None
        holdout_run_id = None
        holdout_runtime = None
        if best_hyperparams is not None:
            holdout_started = time.monotonic()
            holdout_run_id = f"{loop_run_id}_holdout"
            holdout_engine = BacktestEngine(
                feature_log_writer=self.feature_log_writer, run_id=holdout_run_id,
                meta_labeler_params=best_hyperparams, **self.engine_kwargs,
            )
            # One-shot: trains on all tuning-territory data (from_date left
            # open, to_date=None means "everything up to holdout_end"), the
            # test-only fold is the holdout year itself. folds=1 forces
            # exactly one fold instead of walk-forward-CV-ing inside the
            # holdout too — the holdout gets exactly one evaluation, ever.
            holdout_results = holdout_engine.run_full_backtest(
                self.strategy_id, to_date=holdout.holdout_end, folds=1, collect_fold_returns=True,
            )
            holdout_runtime = time.monotonic() - holdout_started
            logger.info(f"iterative_retrain: holdout eval ({holdout_runtime:.1f}s) sharpe={holdout_results.aggregate.get('sharpe_mean')}")
            self._write_backtest_run_row(
                run_id=holdout_run_id, parent_run_id=loop_run_id, mode="holdout_eval",
                start_date=holdout.holdout_start - pd.Timedelta(days=365 * 10), end_date=holdout.holdout_end,
                config={"hyperparams": best_hyperparams, "folds": 1, "horizon_days": horizon_days},
                results=holdout_results, extra_metrics={"best_iteration_index": best_iteration_index},
            )
        else:
            logger.warning("iterative_retrain: no iteration was promoted — skipping the holdout evaluation")

        total_runtime = time.monotonic() - run_started
        return RetrainLoopResult(
            loop_run_id=loop_run_id, holdout_selection=holdout, iterations=iterations,
            best_iteration_index=best_iteration_index, best_hyperparams=best_hyperparams,
            stopped_reason=stopped_reason, holdout_results=holdout_results, holdout_run_id=holdout_run_id,
            holdout_runtime_seconds=holdout_runtime, total_runtime_seconds=total_runtime,
            excluded_buffer_rows=excluded_buffer_rows,
        )

    def _mean_random_feature_accuracy(self, results: BacktestResults) -> Optional[float]:
        """
        Runs overfit_checks.random_feature_test per fold (using
        BacktestResults.fold_models — see run_full_backtest's
        collect_fold_models docstring) and returns the mean accuracy
        across folds. None if no fold collected a model (e.g. every fold
        had too few Act-labeled rows to train a MetaLabeler at all).

        A FRESH MetaLabeler is constructed per fold from the collected
        lgbm_params rather than reusing the fold's production model —
        random_feature_test retrains whatever model it's given in place,
        and that production model has already done its job (the fold's
        simulation already ran with it).
        """
        if not results.fold_models:
            return None
        accuracies = []
        for fm in results.fold_models:
            fresh_model = MetaLabeler(lgbm_params=fm["lgbm_params"])
            accuracy = random_feature_test(
                model=fresh_model, X_train=fm["X_train"], y_train=fm["y_train"],
                X_test=fm["X_test"], y_test=fm["y_test"], feature_cols=CORE_TECHNICAL_FEATURES,
                n_repeats=RANDOM_FEATURE_TEST_REPEATS,
            )
            accuracies.append(accuracy)
        return float(sum(accuracies) / len(accuracies))

    def _dropped_candidate_counts(self, run_id: str) -> Dict[str, int]:
        """Explainability (item 4): what backtest_feature_log recorded this
        iteration dropping and why — grouped by decision_taken. Returns {}
        if no connection/writer is wired up (e.g. a dry-run)."""
        if self.conn is None or self.feature_log_writer is None:
            return {}
        rows = query_feature_log(self.conn, run_id)
        counts: Dict[str, int] = {}
        for row in rows:
            counts[row["decision_taken"]] = counts.get(row["decision_taken"], 0) + 1
        return counts

    def _write_backtest_run_row(
        self, run_id: str, parent_run_id: str, mode: str, start_date: pd.Timestamp, end_date: pd.Timestamp,
        config: Dict[str, Any], results: BacktestResults, extra_metrics: Dict[str, Any],
    ) -> None:
        if self.conn is None:
            return
        metrics = dict(results.aggregate)
        metrics.update(extra_metrics)
        self.conn.execute(
            """
            INSERT INTO backtest_runs
                (run_id, parent_run_id, channel, strategy_id, horizon_bucket, mode, universe_spec,
                 start_date, end_date, capital_mode, initial_capital, random_seed, config_hash, config_json,
                 created_at, metrics_json, data_gaps_json, integrity_passed, integrity_detail_json, live_eligible)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, FALSE)
            ON CONFLICT (run_id) DO NOTHING
            """,
            [
                run_id, parent_run_id, "ml", self.strategy_id, "custom", mode, "iterative_retrain",
                start_date.date(), end_date.date(), "fixed", self.engine_kwargs.get("initial_capital", 1_000_000.0),
                self.engine_kwargs.get("random_state", 42), str(hash(json.dumps(config, sort_keys=True, default=str))),
                json.dumps(config, default=str), now_ist().replace(tzinfo=None), json.dumps(metrics, default=str),
                json.dumps([]), results.integrity_passed, json.dumps(results.integrity_detail, default=str),
            ],
        )
