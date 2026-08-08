#!/usr/bin/env python3
"""
scripts/validate_ta_backtest_results.py

Post-run sanity checks over a completed TA comparison sweep. Answers the
only question that matters before anyone reads the leaderboard: are these
46 numbers trustworthy, or did some of them come from a degenerate run?

    python scripts/validate_ta_backtest_results.py --suffix 20260808_120000

Exits non-zero if any FAIL-level check trips. Checks are deliberately
about run VALIDITY (coverage, integrity flags, degenerate output), not
about whether a strategy performed well — a genuinely bad strategy is a
valid result, not a validation failure.
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

REPORTS_DIR = Path(__file__).resolve().parent.parent / "backtest" / "reports"


def validate(suffix: str, expected_jobs: int, reports_dir: Path = REPORTS_DIR) -> int:
    from systems.technical_analysis.screener.templates import TEMPLATE_MAP

    comparison_path = reports_dir / f"ta_comparison_{suffix}.json"
    if not comparison_path.exists():
        print(f"FAIL  comparison report missing: {comparison_path}")
        return 1
    comparison = json.loads(comparison_path.read_text())
    strategies = comparison["strategies"]

    failures, warnings = [], []

    expected = expected_jobs or len(TEMPLATE_MAP)
    if len(strategies) != expected:
        failures.append(f"collated {len(strategies)} strategies, expected {expected}")
    for bad in comparison.get("failed_reports", []):
        failures.append(f"report {bad['report']} failed to collate: {bad['error']}")

    seen = set()
    for row in strategies:
        name = row.get("template_name")
        if name in seen:
            failures.append(f"{name}: collated twice — job indices may overlap")
        seen.add(name)

        if row["closed_trades"] == 0:
            warnings.append(f"{name}: zero closed trades — template matched nothing, or never exited")
            continue
        if row["engine_metrics"].get("sharpe") is None:
            warnings.append(f"{name}: no Sharpe (degenerate return series)")
        if row["engine_metrics"].get("benchmark_cagr") is None:
            warnings.append(f"{name}: no benchmark CAGR — excess return unavailable")
        if (row.get("avg_holding_days") or 0) <= 0:
            failures.append(f"{name}: non-positive average holding period")
        years = {b["trading_year"] for b in row["yearly"]}
        if len(years) < 3:
            warnings.append(f"{name}: realized trades in only {len(years)} trading year(s)")
        tax = row["taxes"][comparison["tax_regime"]]
        if tax["post_tax_pnl_inr"] > tax["pre_tax_pnl_inr"] + 1e-6:
            failures.append(f"{name}: post-tax P&L exceeds pre-tax — tax computation is wrong")

    # integrity_passed is per-run, read from the raw orchestrator reports.
    for path in sorted(reports_dir.glob(f"orchestrator_{suffix}_job*.json")):
        report = json.loads(path.read_text())
        if report.get("integrity_passed") is False:
            failures.append(f"{path.name}: integrity_passed=False ({report.get('integrity_detail')})")

    for line in warnings:
        print(f"WARN  {line}")
    for line in failures:
        print(f"FAIL  {line}")
    print(
        f"\n{len(strategies)} strategies validated — {len(failures)} failure(s), {len(warnings)} warning(s)"
    )
    return 1 if failures else 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate a completed TA comparison sweep")
    parser.add_argument("--suffix", required=True)
    parser.add_argument("--expected-jobs", type=int, default=0, help="Default: the full template count")
    parser.add_argument("--reports-dir", default=str(REPORTS_DIR))
    args = parser.parse_args()
    sys.exit(validate(args.suffix, args.expected_jobs, Path(args.reports_dir)))


if __name__ == "__main__":
    main()
