#!/usr/bin/env python3
"""
scripts/generate_ta_backtest_queue.py

Generates the 5-year TA strategy comparison queue (46 templates × 1 exit variant = 46 jobs).
Each template runs with its style-specific exit parameters (stop/target/max_hold) from TEMPLATE_STYLE.

Usage:
    python scripts/generate_ta_backtest_queue.py --output backtest/queues/ta_5year_comparison.json
"""

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from systems.technical_analysis.screener.templates import TEMPLATE_MAP, TEMPLATE_STYLE, STYLE_EXIT_PARAMS


def build_queue(
    start_date: str = "2021-04-01",
    end_date: str = "2026-03-31",
    initial_capital: float = 1_000_000.0,
    output_path: str = "backtest/queues/ta_5year_comparison.json",
    include_categories: list = None,
    top_n: int = 200,
) -> dict:
    """
    Build a queue of 46 technical strategy backtests (one per template).

    Each job uses the template's style-specific exit parameters:
    - Momentum: 4% stop / 10% target / 21d max_hold
    - Trend Following: 5% stop / 12% target / 25d max_hold
    - Mean Reversion: 5% stop / 10% target / 21d max_hold
    - Volatility: 4.5% stop / 9% target / 20d max_hold
    - Regime: 6% stop / 15% target / 30d max_hold

    Entry: single date (start_date), buy ALL matching stocks (no top_n limit)
    Exit: apply per-template stop/target/max_hold rules on every trading day
    """

    # Filter templates if categories specified
    templates = list(TEMPLATE_MAP.values())
    if include_categories:
        templates = [t for t in templates if t.category in include_categories]

    # Sort by template name for consistent ordering
    templates.sort(key=lambda t: t.name)

    jobs = []
    for template in templates:
        style = TEMPLATE_STYLE.get(template.name, "Momentum")
        exit_params = STYLE_EXIT_PARAMS.get(style, STYLE_EXIT_PARAMS["Momentum"])

        job = {
            "kind": "orchestrator",
            "channel": "technical",
            "template_name": template.name,
            # "All matches, no limit" as far as the data allows: TechnicalAdapter
            # rejects top_n <= 0 and over-fetches limit=top_n*5 from the screener,
            # so top_n=200 screens 1000 candidates against a <=800-ticker universe —
            # every match a template produces survives the ranking cut.
            "top_n": top_n,
            "start_date": start_date,
            "end_date": end_date,
            "initial_capital": initial_capital,
            "capital_mode": "lump",
            "universe_spec": "curated",
            "max_tickers": 800,
            "min_history_days": 60,
            # baseline = PerTemplateExitPolicy, which reads each template's own
            # exit_stop_pct/exit_target_pct/exit_max_hold_days (assigned from
            # STYLE_EXIT_PARAMS at the bottom of templates.py). max_hold_days is
            # passed explicitly too because build_exit_model_for_variant leaves the
            # day-count barrier off unless it is given.
            "exit_variant": "baseline",
            "max_hold_days": int(exit_params["max_hold_days"]),
            "defer_db_writes": True,
        }
        jobs.append(job)

    queue_def = {
        "description": f"5-year TA strategy comparison ({len(jobs)} templates) - single entry on {start_date}, exit management per style",
        "start_date": start_date,
        "end_date": end_date,
        "initial_capital_per_strategy": initial_capital,
        "entry_mode": "single_date_all_matches",
        "exit_mode": "per_template_style_params",
        "jobs": jobs
    }

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(queue_def, f, indent=2)

    print(f"Generated queue with {len(jobs)} jobs at {output_path}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Capital per strategy: ₹{initial_capital:,.0f}")
    print(f"Categories: {', '.join(set(t.category for t in templates))}")
    print(f"Styles: {', '.join(set(TEMPLATE_STYLE.get(t.name) for t in templates))}")

    return queue_def


def main():
    parser = argparse.ArgumentParser(description="Generate TA 5-year backtest queue")
    parser.add_argument("--output", default="backtest/queues/ta_5year_comparison.json", help="Output queue JSON path")
    parser.add_argument("--start-date", default="2021-04-01", help="Backtest start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", default="2026-03-31", help="Backtest end date (YYYY-MM-DD)")
    parser.add_argument("--capital", type=float, default=1_000_000.0, help="Initial capital per strategy (INR)")
    parser.add_argument("--categories", nargs="*", help="Filter by category (A,B,C,D,E,F,S,R)")
    parser.add_argument("--top-n", type=int, default=200, help="Position cap per strategy (200 = effectively all matches)")

    args = parser.parse_args()

    build_queue(
        start_date=args.start_date,
        end_date=args.end_date,
        initial_capital=args.capital,
        output_path=args.output,
        include_categories=args.categories,
        top_n=args.top_n,
    )


if __name__ == "__main__":
    main()
