"""
Sector Lookup Resolution — R10's ticker->sector mapping, resolved
directly from stock_master.sector.

Verified 2026-09-04 against datastore/normalised/alphalens.duckdb:
stock_master.sector is 100% populated (1,626/1,626 tickers) — better
coverage than the ~95% figure recalled going into this check.
stock_master.industry is NOT usable (empty in the data checked) — R10
uses sector only, not the finer industry granularity its name might
suggest.

This is a static current snapshot (stock_master has no per-date
history), not point-in-time — a ticker's sector as recorded today is
assumed to apply across the whole backtest window. Flagged, not hidden:
if a company's sector classification changed over 2009-2026, this will
misattribute its pre-change history. No PIT sector table exists in the
schema to do better.
"""

from typing import Any, Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


def load_sector_lookup(conn: Any, tickers: Optional[List[str]] = None) -> Dict[str, str]:
    """
    ticker -> sector, from stock_master. Pass `tickers` to scope the
    query to a specific universe (recommended — avoids loading all 1,626
    rows when only a band's ~75-800 tickers are needed); omit for the
    full table.

    Tickers with no stock_master row, or NULL sector, are OMITTED (not
    defaulted to "Unknown" here) — common/sector_ranking.py::rank_sectors()
    is what applies the "Unknown" grouping convention for missing data,
    to keep that policy in one place.
    """
    if tickers:
        placeholders = ",".join("?" for _ in tickers)
        query = f"SELECT ticker, sector FROM stock_master WHERE ticker IN ({placeholders}) AND sector IS NOT NULL"
        rows = conn.execute(query, tickers).fetchall()
    else:
        rows = conn.execute("SELECT ticker, sector FROM stock_master WHERE sector IS NOT NULL").fetchall()

    lookup = {ticker: sector for ticker, sector in rows}

    if tickers:
        missing = set(tickers) - lookup.keys()
        if missing:
            logger.debug(f"load_sector_lookup: {len(missing)}/{len(tickers)} tickers have no sector mapping")

    return lookup
