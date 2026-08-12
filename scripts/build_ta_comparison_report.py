#!/usr/bin/env python3
"""
scripts/build_ta_comparison_report.py

Writes the Technical-channel comparison dataset that the dashboard reads.

    python scripts/build_ta_comparison_report.py \
        'backtest/reports/orchestrator_ta2009_combined_job*.json' \
        'backtest/reports/orchestrator_ta2009_taxfix_job*.json'

Output defaults to backtest/reports/ta_comparison_2009.json. Reads report files
only — no DuckDB access — so it is safe to run while a sweep or the API is busy.
"""

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backtest.ta_comparison_report import build  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

DEFAULT_OUT = Path("backtest/reports/ta_comparison_2009.json")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("globs", nargs="+", help="glob(s) matching orchestrator run reports")
    p.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = p.parse_args()

    data = build(args.globs)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(data, indent=1))

    print(f"wrote {args.out}  ({data['n_runs']} runs -> {data['n_strategies']} strategy/variant pairs)")

    # A quick leaderboard so a run of this script is self-checking rather than
    # only producing a file nobody looks at until the UI is wired.
    ranked = sorted(
        (r for r in data["strategies"] if r["lump"]),
        key=lambda r: -(r["lump"]["cagr_pct"] or 0),
    )[:10]
    bench = ranked[0]["lump"]["benchmark_cagr_pct"] if ranked else 0.0
    print(f"\nbenchmark CAGR {bench:.2f}%   top 10 by CAGR:")
    print(f"  {'tmpl':<6}{'variant':<15}{'CAGR%':>8}{'Sharpe':>8}{'maxDD%':>9}"
          f"{'net extracted':>16}  {'5y roll worst/med/best':>26}")
    for r in ranked:
        lump = r["lump"]
        ar = r["annual_reset"] or {}
        first = next(iter(ar.values()), {}) if ar else {}
        roll = (lump.get("rolling_returns") or {}).get("5y") or {}
        roll_s = (f"{roll['worst_pct']:.1f}/{roll['median_pct']:.1f}/{roll['best_pct']:.1f}"
                  if roll else "-")
        print(f"  {r['template']:<6}{r['exit_variant']:<15}{lump['cagr_pct']:>8.2f}"
              f"{(lump['sharpe'] or 0):>8.2f}{lump['max_drawdown_pct']:>9.1f}"
              f"{first.get('net_extracted', 0):>16,.0f}  {roll_s:>26}")


if __name__ == "__main__":
    main()
