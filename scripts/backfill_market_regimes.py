"""
scripts/backfill_market_regimes.py

Computes and persists Bull/Bear/Sideways market-phase segments
(systems/regime/market_regime.py) into the market_regimes table for one
or more indices, from index_ohlcv's full history. Unlike
backfill_hmm_regime.py, this does NOT need a walk-forward refit-and-
replay: classify_regimes() is a deterministic rule over closing prices,
not a fitted model, so a single full-history pass produces the same
segments a caller would get running it fresh — no look-ahead bias to
walk around (see systems/regime/market_regime.py's PIT-safety note:
consumers gate on confirmed_date, not start_date, for that).

Idempotent — always does a full reclassification per index (DELETE +
re-insert, not a partial upsert). Only the trailing OPEN segment is ever
actually revised by new data (confirmed segments are historical fact once
confirmed_date has passed), but a full reclassification is simplest and
correct either way, and it's fast — a single pass over the index's daily
closes.

Usage:
    python -m scripts.backfill_market_regimes
    python -m scripts.backfill_market_regimes --index "Nifty 50" "Nifty 500"
    python -m scripts.backfill_market_regimes --threshold-pct 0.05 0.10 0.15 0.20

By default backfills all four thresholds used by the Backtest page's
"Market Regime Timeline" comparison (20% — the original default — plus
15%/10%/5%, added so strategies with regime-conditional exits can compare
sensitivity to the threshold choice before picking one).
"""
from __future__ import annotations

import argparse
import logging

import pandas as pd

from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from datastore.schema.create_normalised import create_schema
from systems.regime.market_regime import BULL_BEAR_THRESHOLD_PCT, classify_regimes, method_name
from systems.regime.regime_store import recompute_regime_segments

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_INDICES = ["Nifty 500"]
DEFAULT_THRESHOLDS = [0.05, 0.10, 0.15, BULL_BEAR_THRESHOLD_PCT]  # 5%, 10%, 15%, 20%


def backfill_index(conn, index_name: str, threshold_pct: float = BULL_BEAR_THRESHOLD_PCT) -> int:
    rows = conn.execute(
        "SELECT date, close FROM index_ohlcv WHERE index_name = ? ORDER BY date", [index_name]
    ).fetchall()
    if not rows:
        logger.warning(f"No index_ohlcv rows for '{index_name}' — skipping")
        return 0
    prices = pd.Series([r[1] for r in rows], index=pd.DatetimeIndex([r[0] for r in rows]))
    segments = classify_regimes(prices, threshold_pct=threshold_pct)
    method = method_name(threshold_pct)
    recompute_regime_segments(conn, index_name, segments, method=method)
    logger.info(f"{index_name} [{method}]: {len(segments)} segments from {len(rows)} price points")
    return len(segments)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index", nargs="+", default=DEFAULT_INDICES, help="Index name(s) in index_ohlcv")
    parser.add_argument(
        "--threshold-pct",
        nargs="+",
        type=float,
        default=DEFAULT_THRESHOLDS,
        help="Bull/Bear confirmation threshold(s) as fractions, e.g. 0.05 0.10 0.15 0.20 (default: all four)",
    )
    args = parser.parse_args()

    create_schema()
    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
        total = 0
        for index_name in args.index:
            for threshold_pct in args.threshold_pct:
                total += backfill_index(conn, index_name, threshold_pct=threshold_pct)
    logger.info(
        f"Done — {total} total segments across {len(args.index)} index(es) x {len(args.threshold_pct)} threshold(s)"
    )


if __name__ == "__main__":
    main()
