"""
systems/regime/regime_store.py

Persistence layer for systems/regime/market_regime.py's classify_regimes()
output -> the market_regimes DuckDB table (datastore/schema/
create_normalised.py). Mirrors backtest/core/run_store.py's shape (a
thin save/query module co-located with its consumer, not a generic ORM).
"""

import logging
from datetime import date as date_type
from typing import List, Optional

from systems.regime.market_regime import METHOD_NAME, RegimeSegment

logger = logging.getLogger(__name__)


def save_regime_segments(conn, index_name: str, segments: List[RegimeSegment], method: str = METHOD_NAME) -> None:
    """Upserts segments for (index_name, method) — (index_name, method,
    start_date) is the primary key, so re-running the classifier after new
    price data arrives (which can revise the trailing open segment, or
    supersede it with a newly confirmed one) overwrites cleanly rather than
    accumulating stale rows. `method` MUST be part of the key (not just a
    stored column) because different threshold_pct classifications of the
    SAME index are not guaranteed to have distinct start_dates — e.g. every
    method's very first segment starts on the series' first date — so
    without `method` in the PK, saving a second threshold's segments would
    silently clobber the first threshold's opening segment. Does NOT delete
    segments no longer produced by a fresh classification (e.g. if start
    dates shift) — callers doing a full reclassification should DELETE FROM
    market_regimes WHERE index_name = ? AND method = ? first; see
    recompute_regime_segments() below for that flow.
    """
    for seg in segments:
        conn.execute(
            """
            INSERT INTO market_regimes
                (index_name, regime, start_date, end_date, confirmed_date, method, move_pct)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (index_name, method, start_date) DO UPDATE SET
                regime = excluded.regime,
                end_date = excluded.end_date,
                confirmed_date = excluded.confirmed_date,
                move_pct = excluded.move_pct,
                computed_at = now()
            """,
            [index_name, seg.regime, seg.start_date, seg.end_date, seg.confirmed_date, method, seg.move_pct],
        )
    logger.info(f"Saved {len(segments)} regime segments for {index_name} (method={method})")


def recompute_regime_segments(conn, index_name: str, segments: List[RegimeSegment], method: str = METHOD_NAME) -> None:
    """Full reclassification: replace every stored segment for
    (index_name, method) with the freshly computed set. Use this (not
    save_regime_segments) whenever segment START dates may have shifted —
    e.g. a backfill rerun after correcting the underlying index_ohlcv data,
    or recomputing at a different threshold_pct — since upsert alone would
    leave orphaned rows keyed on start dates the new run no longer
    produces. Scoped to `method` so recomputing one threshold never touches
    another threshold's already-stored segments for the same index."""
    conn.execute("DELETE FROM market_regimes WHERE index_name = ? AND method = ?", [index_name, method])
    save_regime_segments(conn, index_name, segments, method=method)


def list_regime_segments(
    conn,
    index_name: str,
    as_of: Optional[date_type] = None,
    start_date: Optional[date_type] = None,
    end_date: Optional[date_type] = None,
    method: Optional[str] = None,
) -> List[dict]:
    """Segments for index_name, ascending by start_date.

    as_of: PIT-safe filter — only segments CONFIRMED at or before this
    date are returned (confirmed_date <= as_of), so a caller reconstructing
    "what was known as of date X" doesn't see a regime that hadn't actually
    confirmed yet. Omit for the full, latest reclassification (fine for
    retrospective backtest performance attribution, which is not making a
    live decision as-of any particular past date).
    start_date/end_date: restrict to segments overlapping this date range
    (inclusive) — for slicing a specific backtest run's window.
    method: restrict to one classification method (e.g. "5pct_threshold_v1").
    Omit to get every method's segments for this index (e.g. all four
    thresholds interleaved, ascending by start_date) — callers wanting a
    single method's timeline should always pass this.
    """
    where = ["index_name = ?"]
    params: list = [index_name]
    if method is not None:
        where.append("method = ?")
        params.append(method)
    if as_of is not None:
        where.append("confirmed_date <= ?")
        params.append(as_of)
    if start_date is not None:
        where.append("end_date >= ?")
        params.append(start_date)
    if end_date is not None:
        where.append("start_date <= ?")
        params.append(end_date)
    rows = conn.execute(
        f"""
        SELECT index_name, regime, start_date, end_date, confirmed_date, method, move_pct
        FROM market_regimes WHERE {' AND '.join(where)} ORDER BY start_date
        """,
        params,
    ).fetchall()
    cols = ["index_name", "regime", "start_date", "end_date", "confirmed_date", "method", "move_pct"]
    return [dict(zip(cols, r)) for r in rows]
