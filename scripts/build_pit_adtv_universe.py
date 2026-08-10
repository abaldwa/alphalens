#!/usr/bin/env python3
"""
scripts/build_pit_adtv_universe.py

Phase: Technical feature backfill / PIT backtest universe (2007-2026)
Owner: Platform / Features
Consumers: scripts/backfill_advanced_technical_top_n.py (--ticker-file),
           the Category-T backtest sweep

Point-in-time top-N-by-ADTV universe: for each year's first trading day,
the N most liquid tickers by trailing-6-month average daily traded value
AS OF THAT DAY — so 2007 screens 2007's liquid names and 2021 screens
2021's.

Why this exists
---------------
config/universe.py::get_top_adtv_tickers(n) ranks a STATIC snapshot of
today's universe CSV. Applied to a 2007-2026 backtest that is
survivorship bias in its purest form: it selects the companies that
became liquid and survived to today. Measured on this data, only 239 of
2007's real top-800 are in today's top-800 — a current-universe backtest
of 2007 screens the wrong ~70% of the market. Liquidity thresholds moved
too: the 800th name needed Rs 0.11 cr ADTV in 2007 vs Rs 2.70 cr in 2021.

Membership is fixed ANNUALLY (not daily): it mirrors the yearly-fixed
convention features/momentum_universe.py::RANK_BANDS already establishes,
and avoids a universe that churns mid-quarter for noise reasons.

The BACKFILL needs the union of every year's set (one ticker-major pass
must cover any ticker the backtest can ever screen); the BACKTEST needs
the per-year sets. Both are emitted: --out-union writes the flat ticker
list, --out-yearly writes {year: [tickers]} JSON.

Delisted tickers are included by construction — ranking reads
ohlcv_adjusted, which retains rows for securities that later delisted, so
a name that was genuinely liquid in 2009 is present for 2009 even if it
stopped trading in 2014. This is the same survivorship-bias fix
momentum_universe.py's include_delisted=True applies.

Usage
-----
    PYTHONPATH=$PWD .venv/bin/python scripts/build_pit_adtv_universe.py \
        --from-year 2007 --to-year 2026 --top-n 800 \
        --out-union logs/pit_universe_union.txt \
        --out-yearly logs/pit_universe_yearly.json
"""

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Trailing window used to rank liquidity as of each year's first trading
# day. 6 months is long enough that a single event-driven volume spike
# can't promote an otherwise-illiquid name, short enough to reflect the
# liquidity actually available to a trader at that moment.
LOOKBACK_MONTHS = 6
# A ticker must have traded at least this many days inside the lookback to
# be rankable — guards against a name that listed days before the cutoff
# showing a high average over 3 bars.
MIN_TRADING_DAYS = 40


def first_trading_day(conn, year: int) -> str:
    """First real equity trading day on/after 1 April of `year` (the Indian
    financial-year boundary this project's trading_year() already uses).
    Saturday special sessions are excluded via the >=50-ticker floor —
    those days trade a handful of names and are not a real session."""
    row = conn.execute(
        """
        SELECT date FROM (
            SELECT date, COUNT(DISTINCT ticker) n FROM ohlcv_adjusted
            WHERE date >= ? AND date < ?
            GROUP BY date
        ) WHERE n >= 50
        ORDER BY date LIMIT 1
        """,
        [f"{year}-04-01", f"{year + 1}-04-01"],
    ).fetchone()
    return row[0].isoformat() if row else None


def top_adtv_as_of(conn, as_of: str, top_n: int) -> list:
    """Top-`top_n` tickers by trailing-`LOOKBACK_MONTHS` ADTV as of `as_of`.

    ADTV is close*volume averaged over the lookback (crores). Uses only
    bars strictly BEFORE as_of — the ranking must not peek at the day it
    is used to select for.
    """
    rows = conn.execute(
        f"""
        SELECT ticker, AVG(close * volume) / 1e7 AS adtv_cr
        FROM ohlcv_adjusted
        WHERE date < ? AND date >= (CAST(? AS DATE) - INTERVAL {LOOKBACK_MONTHS} MONTH)
          AND close > 0 AND volume > 0
        GROUP BY ticker
        HAVING COUNT(*) >= {MIN_TRADING_DAYS}
        ORDER BY adtv_cr DESC
        LIMIT ?
        """,
        [as_of, as_of, top_n],
    ).fetchall()
    return [r[0] for r in rows]


def build(from_year: int, to_year: int, top_n: int) -> tuple:
    yearly = {}
    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        for year in range(from_year, to_year + 1):
            as_of = first_trading_day(conn, year)
            if as_of is None:
                logger.warning(f"{year}: no trading day found — skipped")
                continue
            tickers = top_adtv_as_of(conn, as_of, top_n)
            yearly[year] = {"as_of": as_of, "tickers": tickers}
            logger.info(f"{year}: as_of={as_of} -> {len(tickers)} tickers")

    union = sorted({t for y in yearly.values() for t in y["tickers"]})
    return yearly, union


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the PIT top-N-by-ADTV universe")
    parser.add_argument("--from-year", type=int, default=2007)
    parser.add_argument("--to-year", type=int, default=2026)
    parser.add_argument("--top-n", type=int, default=800)
    parser.add_argument("--out-union", default="logs/pit_universe_union.txt")
    parser.add_argument("--out-yearly", default="logs/pit_universe_yearly.json")
    args = parser.parse_args()

    yearly, union = build(args.from_year, args.to_year, args.top_n)
    if not union:
        raise SystemExit("empty universe — refusing to write")

    Path(args.out_union).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_union).write_text("\n".join(union) + "\n")
    Path(args.out_yearly).write_text(json.dumps(yearly, indent=2))

    # Overlap with today's static universe — the size of the survivorship
    # bias this script exists to remove.
    from config.universe import get_top_adtv_tickers

    current = set(get_top_adtv_tickers(args.top_n))
    first_year = min(yearly)
    first_set = set(yearly[first_year]["tickers"])
    logger.info(f"union across {args.from_year}-{args.to_year}: {len(union)} tickers -> {args.out_union}")
    logger.info(f"  {first_year} PIT set overlaps today's top-{args.top_n} by {len(first_set & current)}/{len(first_set)}")
    logger.info(f"  in union but NOT in today's top-{args.top_n}: {len(set(union) - current)}")


if __name__ == "__main__":
    main()
