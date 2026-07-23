"""
scripts/compute_strategy_confidence.py

Replaces scripts/compute_ta_recommendation_outcomes.py (the rejected
touch-based win/loss backfill). Re-runnable: evaluates TA screener signals
against the shared backtest/strategy_confidence.py framework and upserts
both per-signal detail (strategy_confidence_outcomes) and the tiered
per-strategy/per-regime summary (strategy_confidence_summary).

Fixes both operational defects the prior script hit in production:
1. Date-bounded (`--since`, default MAX_LOOKBACK_DAYS ago) instead of
   pulling each ticker's entire OHLCV history every run.
2. `--since` lets a partial/killed run be resumed cheaply by narrowing the
   window to just the affected dates, rather than re-evaluating everything.

Usage:
    python -m scripts.compute_strategy_confidence
    python -m scripts.compute_strategy_confidence --since 2026-06-23
"""
from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta

from systems.technical_analysis.screener.outcomes import compute_and_store_ta_confidence

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MAX_LOOKBACK_DAYS = 400


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--since", default=None,
        help=f"YYYY-MM-DD; only evaluate ta_signals rows on/after this date "
             f"(default: {MAX_LOOKBACK_DAYS} days ago)",
    )
    parser.add_argument("--horizon-days", type=int, default=5)
    parser.add_argument("--win-threshold-pct", type=float, default=0.0)
    parser.add_argument(
        "--chunk-size-dates", type=int, default=20,
        help="Trading dates evaluated+persisted per DB write/memory-release cycle "
             "(smaller = more frequent writes, lower peak memory, more DB round-trips)",
    )
    args = parser.parse_args()

    since = args.since or (date.today() - timedelta(days=MAX_LOOKBACK_DAYS)).isoformat()
    results = compute_and_store_ta_confidence(
        since=since, horizon_days=args.horizon_days, win_threshold_pct=args.win_threshold_pct,
        chunk_size_dates=args.chunk_size_dates,
    )
    logger.info("strategy confidence: %d strategies evaluated (since=%s)", len(results), since)
    for strategy_id, res in sorted(results.items()):
        logger.info(
            "  %-6s tier=%-18s n_dates=%-4d win_rate=%s baseline=%s",
            strategy_id, res.tier, res.n_independent_dates,
            f"{res.win_rate:.3f}" if res.win_rate is not None else "n/a",
            f"{res.baseline_win_rate:.3f}" if res.baseline_win_rate is not None else "n/a",
        )


if __name__ == "__main__":
    main()
