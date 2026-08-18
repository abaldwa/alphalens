"""
scripts/run_momentum_grace_comparison.py

Phase: FeatureBacklog.md ML38 — momentum strategy implementation
Owner: Platform / Backtest
Consumers: operator CLI

Reruns the full ML38 300-variant grid (5 market-cap rank bands x 4
lookbacks x 5 rebalance cadences x 3 portfolio sizes) at grace_cycles in
{0, 1} — grace=2 is the already-computed default grid in
backtest/reports/momentum/. Summary metrics only (no per-trade ledger, no
equity curve) since this is a sensitivity comparison, not a new primary
dataset — keeps output small enough to merge into the dashboard.
"""

import argparse
import json
import logging
from pathlib import Path
from typing import Dict, List

from config.timezone import now_ist

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

STARTING_CAPITAL = 1_000_000.0
INVESTABLE_PCT = 0.8
TOP_N_OPTIONS = [10, 15, 20]
REBALANCE_PERIODS = {"weekly": 5, "biweekly": 10, "monthly": 21, "bimonthly": 42, "quarterly": 63}

REPORTS_DIR = Path(__file__).resolve().parent.parent / "backtest" / "reports" / "momentum"


def _union_tickers(yearly_rankings) -> List[str]:
    tickers = set()
    for ranked in yearly_rankings.values():
        if not ranked.empty:
            tickers.update(ranked["ticker"].tolist())
    return sorted(tickers)


def run(years_back: int, grace_values: List[int]) -> Dict:
    # [H4, 2026-08-18] grace_cycles no longer exists on MomentumAdapter --
    # deprecated by the 2026-08-18 user decision that pure-play momentum is
    # a plain rank rotation (UnifiedGeneratorRefactorPlan.md §19). This
    # script's entire purpose was to compare grace_cycles values, so there
    # is nothing left to compute; every "variant" would silently be
    # identical, which would misrepresent a real sensitivity comparison.
    raise NotImplementedError(
        "grace_cycles no longer exists on MomentumAdapter (deprecated 2026-08-18, "
        "UnifiedGeneratorRefactorPlan.md §19) -- this comparison has no meaning to compute."
    )


def main():
    parser = argparse.ArgumentParser(description="ML38 grace-period sensitivity comparison (0/1 vs default 2)")
    parser.add_argument("--years-back", type=int, default=10)
    parser.add_argument("--grace-values", type=int, nargs="+", default=[0, 1])
    args = parser.parse_args()

    report = run(years_back=args.years_back, grace_values=args.grace_values)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = REPORTS_DIR / f"momentum_grace_comparison_{now_ist().strftime('%Y%m%d_%H%M%S')}.json"
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    logger.info("Wrote report to %s", out_path)


if __name__ == "__main__":
    main()
