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
"""
from __future__ import annotations

import argparse
import logging

import pandas as pd

from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from datastore.schema.create_normalised import create_schema
from systems.regime.market_regime import classify_regimes
from systems.regime.regime_store import recompute_regime_segments

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_INDICES = ["Nifty 500"]


def backfill_index(conn, index_name: str) -> int:
    rows = conn.execute(
        "SELECT date, close FROM index_ohlcv WHERE index_name = ? ORDER BY date", [index_name]
    ).fetchall()
    if not rows:
        logger.warning(f"No index_ohlcv rows for '{index_name}' — skipping")
        return 0
    prices = pd.Series([r[1] for r in rows], index=pd.DatetimeIndex([r[0] for r in rows]))
    segments = classify_regimes(prices)
    recompute_regime_segments(conn, index_name, segments)
    logger.info(f"{index_name}: {len(segments)} segments from {len(rows)} price points")
    return len(segments)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index", nargs="+", default=DEFAULT_INDICES, help="Index name(s) in index_ohlcv")
    args = parser.parse_args()

    create_schema()
    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
        total = 0
        for index_name in args.index:
            total += backfill_index(conn, index_name)
    logger.info(f"Done — {total} total segments across {len(args.index)} index(es)")


if __name__ == "__main__":
    main()
