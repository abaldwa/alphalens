#!/usr/bin/env python3
"""
Analyze regime detection validation results and generate recommendations.

Reads walk-forward validation CSVs and produces:
1. Best algorithm per market cap
2. Comparative performance table
3. Configuration recommendations for B-027 R9 regime switching
"""

import logging
from pathlib import Path
from typing import Dict, Tuple
import sys

import pandas as pd

logger = logging.getLogger(__name__)

VALIDATION_DIR = Path('/tmp/alphalens_regime_validation')
OUTPUT_DIR = Path('/tmp/alphalens_regime_recommendations')


def load_validation_results() -> Dict[str, pd.DataFrame]:
    """Load all walk-forward validation CSV files."""
    if not VALIDATION_DIR.exists():
        raise FileNotFoundError(f"Validation results not found at {VALIDATION_DIR}")

    results = {}

    # Load combined results
    combined_file = VALIDATION_DIR / 'combined_walkforward_results.csv'
    if combined_file.exists():
        results['combined'] = pd.read_csv(combined_file)
    else:
        logger.warning(f"Combined results file not found: {combined_file}")

    # Load per-market-cap summary files
    for summary_file in VALIDATION_DIR.glob('*_summary.csv'):
        market_cap = summary_file.stem.replace('_summary', '')
        results[f'summary_{market_cap}'] = pd.read_csv(summary_file, index_col=0)

    return results


def analyze_by_market_cap(combined_df: pd.DataFrame) -> Dict[str, Dict]:
    """Analyze accuracy by market cap and algorithm."""
    analysis = {}

    for market_cap in combined_df['market_cap_band'].unique():
        market_cap_df = combined_df[combined_df['market_cap_band'] == market_cap]

        market_cap_analysis = {}
        for algo in market_cap_df['algorithm'].unique():
            algo_df = market_cap_df[market_cap_df['algorithm'] == algo]

            accuracies = algo_df['accuracy'].dropna()
            if len(accuracies) > 0:
                market_cap_analysis[algo] = {
                    'avg_accuracy': accuracies.mean(),
                    'std_accuracy': accuracies.std(),
                    'min_accuracy': accuracies.min(),
                    'max_accuracy': accuracies.max(),
                    'n_windows': len(accuracies),
                    'avg_predictions': algo_df['n_predictions'].mean(),
                    'avg_bull_accuracy': algo_df['bull_correct'].mean(),
                    'avg_bear_accuracy': algo_df['bear_correct'].mean(),
                }

        analysis[market_cap] = market_cap_analysis

    return analysis


def recommend_best_algorithms(analysis: Dict[str, Dict]) -> Dict[str, Tuple[str, float]]:
    """Recommend best algorithm per market cap."""
    recommendations = {}

    for market_cap, algos in analysis.items():
        if not algos:
            continue

        best_algo_name, best_algo_metrics = max(algos.items(), key=lambda x: x[1]['avg_accuracy'])
        recommendations[market_cap] = (best_algo_name, best_algo_metrics['avg_accuracy'])

    return recommendations


def generate_report(
    analysis: Dict[str, Dict],
    recommendations: Dict[str, Tuple[str, float]],
) -> str:
    """Generate readable markdown report."""
    report = []

    report.append("# Regime Detection Validation Report\n")
    report.append(f"Generated: {pd.Timestamp.now().isoformat()}\n")

    # Summary table
    report.append("## Summary: Best Algorithm per Market Cap\n")
    report.append("| Market Cap | Best Algorithm | Accuracy | Std Dev | Bull | Bear |\n")
    report.append("|------------|----------------|----------|---------|------|------|\n")

    for market_cap in sorted(analysis.keys()):
        if market_cap not in recommendations:
            continue

        algo_name, accuracy = recommendations[market_cap]
        algo_metrics = analysis[market_cap][algo_name]

        bull_acc = algo_metrics.get('avg_bull_accuracy', 0)
        bear_acc = algo_metrics.get('avg_bear_accuracy', 0)

        report.append(
            f"| {market_cap:20} | {algo_name:14} | {accuracy:.1%} | {algo_metrics['std_accuracy']:.1%} | "
            f"{bull_acc:.1%} | {bear_acc:.1%} |\n"
        )

    report.append("\n")

    # Detailed analysis per market cap
    report.append("## Detailed Analysis\n\n")

    for market_cap in sorted(analysis.keys()):
        report.append(f"### {market_cap}\n\n")

        algos = analysis[market_cap]
        algo_df = pd.DataFrame(algos).T
        algo_df = algo_df.sort_values('avg_accuracy', ascending=False)

        report.append("| Algorithm | Accuracy | Std Dev | Min | Max | Bull | Bear | n_windows |\n")
        report.append("|-----------|----------|---------|-----|-----|------|------|----------|\n")

        for algo_name, row in algo_df.iterrows():
            report.append(
                f"| {algo_name:12} | {row['avg_accuracy']:.1%} | {row['std_accuracy']:.1%} | "
                f"{row['min_accuracy']:.1%} | {row['max_accuracy']:.1%} | "
                f"{row['avg_bull_accuracy']:.1%} | {row['avg_bear_accuracy']:.1%} | "
                f"{int(row['n_windows'])} |\n"
            )

        best_algo = algo_df.index[0]
        best_accuracy = algo_df.iloc[0]['avg_accuracy']
        report.append(f"\n**Recommendation:** Use `{best_algo}` ({best_accuracy:.1%} accuracy)\n\n")

    # Configuration for B-027
    report.append("## B-027 Implementation Configuration\n\n")
    report.append("Once you approve the recommendations above, use these settings for each market cap:\n\n")

    for market_cap in sorted(recommendations.keys()):
        algo_name, accuracy = recommendations[market_cap]
        report.append(f"### {market_cap}\n")
        report.append("```json\n")
        report.append(f'{{"market_cap_band": "{market_cap}",\n')
        report.append(f' "regime_detector": "{algo_name}",\n')
        report.append(f' "accuracy_validated": {accuracy:.1%}}}\n')
        report.append("```\n\n")

    report.append("## Next Steps\n\n")
    report.append(
        "1. Review the recommendations above\n"
        "2. If satisfied, implement regime switching in R9 using the recommended algorithms\n"
        "3. Update backtest/strategy_id.py to use market-cap-specific regime detectors\n"
        "4. Run R9 with regime switching enabled\n"
        "5. Compare R9 Sharpe vs R0 to decide if R9 should be kept\n"
    )

    return '\n'.join(report)


def main():
    logging.basicConfig(level=logging.INFO)

    logger.info(f"Loading validation results from {VALIDATION_DIR}")

    try:
        results = load_validation_results()
    except FileNotFoundError as e:
        logger.error(str(e))
        logger.info("Run regime_walkforward_validator.py first to generate validation results")
        sys.exit(1)

    if 'combined' not in results:
        logger.error("Combined validation results not found")
        sys.exit(1)

    combined_df = results['combined']
    logger.info(f"Loaded {len(combined_df)} validation windows")

    # Analyze
    analysis = analyze_by_market_cap(combined_df)
    recommendations = recommend_best_algorithms(analysis)

    # Generate report
    report = generate_report(analysis, recommendations)

    # Save report
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    report_file = OUTPUT_DIR / 'regime_analysis_report.md'
    report_file.write_text(report)

    logger.info(f"✓ Report saved to {report_file}")

    # Print to console
    print(report)

    # Save recommendations as JSON for programmatic use
    recommendations_data = {
        market_cap: algo_name
        for market_cap, (algo_name, _) in recommendations.items()
    }

    import json
    recommendations_file = OUTPUT_DIR / 'recommended_algorithms.json'
    recommendations_file.write_text(json.dumps(recommendations_data, indent=2))
    logger.info(f"✓ Recommendations saved to {recommendations_file}")


if __name__ == '__main__':
    main()
