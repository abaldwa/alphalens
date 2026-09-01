#!/usr/bin/env python3
"""
Walk-forward validation for regime detection algorithms.

For each market cap band and algorithm:
1. Split data into train/test windows (rolling)
2. Detect regime on test window
3. Compare against actual price movement (win/loss indicator)
4. Calculate accuracy, precision, recall
5. Recommend best algorithm per market cap

Output: CSV files in /tmp/alphalens_regime_validation/
"""

import logging
from datetime import date as date_type, timedelta
from pathlib import Path
from typing import Any, Dict, List
import sys

import pandas as pd
import numpy as np

project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from contracts.regime_detector import (  # noqa: E402
    MARKET_CAP_BANDS,
    EMARegimeDetector,
    RSIRegimeDetector,
    VolatilityRegimeDetector,
    EnsembleRegimeDetector,
)

logger = logging.getLogger(__name__)

# Walk-forward parameters (can be tuned per market cap)
TRAIN_WINDOW_DAYS = 252  # 1 year training
TEST_WINDOW_DAYS = 63  # ~3 months testing
STEP_DAYS = 21  # Move forward by 1 month at a time


class RegimeWalkForwardValidator:
    """Validate regime detection using walk-forward analysis."""

    def __init__(self, market_cap_band: str):
        self.market_cap_band = market_cap_band

    def load_full_ohlcv(
        self, start_date: date_type, end_date: date_type
    ) -> pd.DataFrame:
        """Load full OHLCV for market cap band."""
        detector = EMARegimeDetector(self.market_cap_band)
        df = detector.load_index_ohlcv(start_date, end_date)
        return df

    def compare_regime_to_actual(self, train_df: pd.DataFrame, test_df: pd.DataFrame, detector: Any) -> Dict[str, Any]:
        """
        Compare regime predictions to actual price movement.

        Regime accuracy based on:
        - Bull regime → actual return > median → TP (true positive)
        - Bear regime → actual return < median → TP
        - Choppy regime → Regardless (we don't care about choppy accuracy)
        """
        # Train detector on train_df (state is held in detector for walk-forward)
        detector.detect(train_df)

        # Predict on test_df
        test_regime = detector.detect(test_df)
        test_regime['regime'] = test_regime['regime']

        # Calculate actual returns
        test_df_copy = test_df.copy()
        test_df_copy['actual_return'] = test_df_copy['close'].pct_change()

        median_return = test_df_copy['actual_return'].median()

        # Combine regime + actual return
        comparison_df = pd.DataFrame({
            'regime': test_regime['regime'],
            'actual_return': test_df_copy['actual_return'],
        })

        # Score: did regime predict correctly?
        def score_prediction(row: pd.Series) -> Any:
            regime = row['regime']
            ret = row['actual_return']

            if pd.isna(ret):
                return None

            if regime == 'Bull':
                return 1 if ret > median_return else 0
            elif regime == 'Bear':
                return 1 if ret < median_return else 0
            else:  # Choppy
                return None  # Don't score choppy predictions

        comparison_df['score'] = comparison_df.apply(score_prediction, axis=1)

        # Calculate metrics
        scored = comparison_df['score'].dropna()
        if len(scored) == 0:
            return {
                'accuracy': 0.0,
                'n_predictions': 0,
                'bull_correct': 0,
                'bear_correct': 0,
            }

        accuracy = scored.mean()

        bull_df = comparison_df[comparison_df['regime'] == 'Bull']
        bear_df = comparison_df[comparison_df['regime'] == 'Bear']

        bull_correct = bull_df['score'].mean() if len(bull_df) > 0 else 0.0
        bear_correct = bear_df['score'].mean() if len(bear_df) > 0 else 0.0

        return {
            'accuracy': float(accuracy),
            'n_predictions': int(len(scored)),
            'bull_correct': float(bull_correct),
            'bear_correct': float(bear_correct),
            'n_bull': int((comparison_df['regime'] == 'Bull').sum()),
            'n_bear': int((comparison_df['regime'] == 'Bear').sum()),
            'n_choppy': int((comparison_df['regime'] == 'Choppy').sum()),
        }

    def run_validation(
        self,
        start_date: date_type,
        end_date: date_type,
        train_window_days: int = TRAIN_WINDOW_DAYS,
        test_window_days: int = TEST_WINDOW_DAYS,
        step_days: int = STEP_DAYS,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Walk forward: slide train/test windows across entire period.

        Returns: dict of algorithm_name -> list of validation results
        """
        logger.info(f"Loading OHLCV for {self.market_cap_band}")
        full_df = self.load_full_ohlcv(start_date, end_date)

        if full_df.empty:
            logger.warning(f"No data for {self.market_cap_band}")
            return {}

        logger.info(f"  Loaded {len(full_df)} trading days")

        # Initialize detectors
        detectors = {
            'ema': EMARegimeDetector(self.market_cap_band),
            'rsi': RSIRegimeDetector(self.market_cap_band),
            'volatility': VolatilityRegimeDetector(self.market_cap_band),
            'ensemble': EnsembleRegimeDetector(self.market_cap_band),
        }

        results: Dict[str, List[Dict[str, Any]]] = {algo: [] for algo in detectors.keys()}

        # Walk forward
        idx = 0
        window_count = 0
        while idx + train_window_days + test_window_days < len(full_df):
            train_start_idx = idx
            train_end_idx = idx + train_window_days
            test_end_idx = idx + train_window_days + test_window_days

            train_df = full_df.iloc[train_start_idx:train_end_idx]
            test_df = full_df.iloc[train_end_idx:test_end_idx]

            train_start_date = train_df.index[0].date()
            test_start_date = test_df.index[0].date()
            test_end_date = test_df.index[-1].date()

            logger.info(
                f"  Window {window_count}: train {train_start_date} | test {test_start_date}..{test_end_date}"
            )

            # Test each detector
            for algo_name, detector in detectors.items():
                try:
                    metrics = self.compare_regime_to_actual(train_df, test_df, detector)
                    metrics['window'] = window_count
                    metrics['train_start_date'] = train_start_date
                    metrics['test_start_date'] = test_start_date
                    metrics['test_end_date'] = test_end_date
                    results[algo_name].append(metrics)
                except Exception as e:
                    logger.error(f"    {algo_name} failed: {e}")
                    continue

            idx += step_days
            window_count += 1

        return results

    def summarize_results(self, results: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
        """Summarize walk-forward results."""
        summary: Dict[str, Dict[str, Any]] = {}

        for algo_name, windows in results.items():
            if not windows:
                continue

            accuracies = [w['accuracy'] for w in windows]
            n_predictions = [w['n_predictions'] for w in windows]

            summary[algo_name] = {
                'n_windows': len(windows),
                'avg_accuracy': float(np.mean(accuracies)),
                'std_accuracy': float(np.std(accuracies)),
                'min_accuracy': float(np.min(accuracies)),
                'max_accuracy': float(np.max(accuracies)),
                'avg_predictions_per_window': float(np.mean(n_predictions)),
            }

        return summary


def run_all_validations(
    start_date: date_type,
    end_date: date_type,
    output_dir: Path = Path('/tmp/alphalens_regime_validation'),
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Run walk-forward validation for all market cap bands."""
    output_dir.mkdir(parents=True, exist_ok=True)

    all_summaries: Dict[str, Dict[str, Dict[str, Any]]] = {}
    all_results_dfs = []

    for market_cap_band in MARKET_CAP_BANDS.keys():
        logger.info(f"\nValidating regime detection for: {market_cap_band}")
        logger.info("=" * 60)

        validator = RegimeWalkForwardValidator(market_cap_band)

        try:
            results = validator.run_validation(start_date, end_date)

            if not results:
                logger.warning(f"  No results for {market_cap_band}")
                continue

            summary = validator.summarize_results(results)
            all_summaries[market_cap_band] = summary

            # Write detailed results to CSV
            for algo_name, windows in results.items():
                df = pd.DataFrame(windows)
                output_file = output_dir / f'{market_cap_band}_{algo_name}_walkforward.csv'
                df.to_csv(output_file, index=False)
                logger.info(f"  ✓ {algo_name:12} -> {output_file}")

                # Prepare for combined summary
                df['market_cap_band'] = market_cap_band
                df['algorithm'] = algo_name
                all_results_dfs.append(df)

            # Write summary for this market cap
            summary_df = pd.DataFrame(summary).T
            summary_file = output_dir / f'{market_cap_band}_summary.csv'
            summary_df.to_csv(summary_file)

        except Exception as e:
            logger.error(f"Error validating {market_cap_band}: {e}")
            continue

    # Write combined summary across all market caps
    if all_results_dfs:
        combined_df = pd.concat(all_results_dfs, ignore_index=True)
        combined_file = output_dir / 'combined_walkforward_results.csv'
        combined_df.to_csv(combined_file, index=False)
        logger.info(f"\n✓ Combined results -> {combined_file}")

    return all_summaries


def print_validation_report(summaries: Dict[str, Dict[str, Dict[str, Any]]]) -> None:
    """Print readable validation report."""
    print("\n" + "=" * 80)
    print("REGIME DETECTION WALK-FORWARD VALIDATION REPORT")
    print("=" * 80)

    for market_cap_band in sorted(MARKET_CAP_BANDS.keys()):
        if market_cap_band not in summaries:
            continue

        print(f"\n{market_cap_band.upper()}")
        print("-" * 80)

        algos = summaries[market_cap_band]
        for algo_name in sorted(algos.keys()):
            metrics = algos[algo_name]
            print(
                f"  {algo_name:15} | Accuracy: {metrics['avg_accuracy']:.1%} ± {metrics['std_accuracy']:.1%} | "
                f"Range: {metrics['min_accuracy']:.1%} - {metrics['max_accuracy']:.1%} | "
                f"n={metrics['n_windows']}"
            )

        # Recommend best algorithm
        best_algo = max(
            algos.items(), key=lambda x: x[1]['avg_accuracy']
        )
        print(f"\n  ✓ RECOMMENDED: {best_algo[0]} ({best_algo[1]['avg_accuracy']:.1%} accuracy)")


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )

    # Run validation for last 3 years
    end_date = date_type.today()
    start_date = end_date - timedelta(days=3 * 365)

    logger.info(f"Walk-forward validation: {start_date} to {end_date}")
    logger.info(f"Train window: {TRAIN_WINDOW_DAYS} days | Test window: {TEST_WINDOW_DAYS} days | Step: {STEP_DAYS} days")

    summaries = run_all_validations(start_date, end_date)
    print_validation_report(summaries)

    logger.info("\n✓ Validation complete. Review CSV files in /tmp/alphalens_regime_validation/")
