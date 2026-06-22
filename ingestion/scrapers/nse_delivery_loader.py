"""
ingestion/scrapers/nse_delivery_loader.py

Phase: 0.5 (FYERS Historical Backfill)
Specs: SPEC-PIPE-001, SPEC-PIPE-005
Owner: Platform / Ingestion
Consumers: ingestion/backfill_runner, datastore/normalised

FYERS' history API returns OHLCV only — no delivery_qty/delivery_pct.
This module backfills those two columns for the same 5-year window by
replaying NSE's historical sec_bhavdata_full archives (the same source and
parsing logic as ingestion/scrapers/bhavcopy.py's day-to-day download),
and MERGING (UPDATE, never INSERT) delivery_qty/delivery_pct into
ohlcv_adjusted rows that FYERS backfill already created.

Reuses bhavcopy._fetch_bhavcopy_csv / _nse_session rather than
re-implementing NSE session handling — same archive endpoint, same format,
the only difference is this module loops over a date range instead of one
date, and only needs two of bhavcopy's columns.
"""

import logging
import time
from datetime import date as date_type
from datetime import timedelta
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from ingestion.scheduler.gap_detector import is_trading_day
from ingestion.scrapers import bhavcopy

logger = logging.getLogger(__name__)

# A multi-year backfill hits NSE's archive server for every trading day in
# range (~1,500 requests for 5 years) with no other rate limit in place —
# this throttle is a good-citizen safeguard against tripping NSE's
# anti-bot/rate-limiting defenses mid-run, which would corrupt an
# otherwise-clean multi-day backfill.
NSE_FETCH_THROTTLE_SECONDS = 0.5

_UPDATE_DELIVERY_FROM_DF = """
    UPDATE ohlcv_adjusted
    SET delivery_qty = _delivery_df.delivery_qty, delivery_pct = _delivery_df.delivery_pct
    FROM _delivery_df
    WHERE ohlcv_adjusted.ticker = _delivery_df.ticker
      AND ohlcv_adjusted.date = ?
"""


def download_delivery_for_date(trade_date: date_type) -> pd.DataFrame:
    """
    Fetch delivery_qty/delivery_pct for every EQ-series ticker on one date.

    Parameters
    ----------
    trade_date : date

    Returns
    -------
    pd.DataFrame
        Columns: ticker, delivery_qty, delivery_pct.

    Spec References
    ----------------
    SPEC-PIPE-001, SPEC-PIPE-005: delivery_pct in [0, 100].

    PIT Assumptions
    ----------------
    None — same-day NSE archive data, no PIT lag.

    Raises
    ------
    ConnectionError
        If the NSE archive fetch fails after retries (propagated from
        bhavcopy._fetch_bhavcopy_csv).
    """
    from datetime import datetime

    raw = bhavcopy._fetch_bhavcopy_csv(datetime.combine(trade_date, datetime.min.time()))
    raw.columns = [c.strip().upper() for c in raw.columns]
    for col in ("SYMBOL", "SERIES"):
        if raw[col].dtype == object:
            raw[col] = raw[col].str.strip()

    raw = raw[raw["SERIES"] == "EQ"].reset_index(drop=True)

    traded_qty = pd.to_numeric(raw["TTL_TRD_QNTY"], errors="coerce")
    delivery_qty = pd.to_numeric(raw["DELIV_QTY"], errors="coerce")
    delivery_pct = (delivery_qty / traded_qty * 100).where(traded_qty > 0)

    return pd.DataFrame(
        {"ticker": raw["SYMBOL"], "delivery_qty": delivery_qty, "delivery_pct": delivery_pct}
    )


def merge_delivery_into_ohlcv(conn, trade_date: date_type, delivery_df: pd.DataFrame) -> int:
    """
    UPDATE ohlcv_adjusted.delivery_qty/delivery_pct for one date's rows.

    Only updates rows that already exist (created by the FYERS OHLCV
    backfill or the daily bhavcopy pipeline) — never inserts new rows, so
    this can never desync ohlcv_adjusted's price columns from its
    delivery columns.

    Uses a single set-based UPDATE ... FROM against delivery_df (registered
    as a DuckDB view) rather than one UPDATE per ticker: besides being much
    faster, DuckDB's UPDATE result row reports the number of rows actually
    matched and changed, whereas executemany() over per-row UPDATEs has no
    way to distinguish "matched an existing ohlcv_adjusted row" from "no
    such ticker/date — silently a no-op" (DuckDB's executemany rowcount is
    always -1). The earlier per-row version returned len(rows) — i.e. the
    full NSE EQ-series count for the date (~1,700-2,000 tickers/day) —
    regardless of how many of those tickers actually existed in
    ohlcv_adjusted (only the ~170-502 FYERS-backfilled tickers do), wildly
    overstating progress in logs (e.g. reporting 2.7M "rows updated" for a
    5-year run against a 170-ticker universe, when only ~206K rows were
    truly touched).

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
    trade_date : date
    delivery_df : pd.DataFrame
        Output of download_delivery_for_date().

    Returns
    -------
    int
        Number of ohlcv_adjusted rows actually updated (matched on
        ticker + date) — not the number of tickers in delivery_df.

    Spec References
    ----------------
    SPEC-PIPE-001: "Merges delivery_qty and delivery_pct into existing
    ohlcv_adjusted rows."

    PIT Assumptions
    ----------------
    None.

    Raises
    ------
    None
    """
    if delivery_df.empty:
        return 0

    conn.register("_delivery_df", delivery_df)
    try:
        result = conn.execute(_UPDATE_DELIVERY_FROM_DF, [trade_date.isoformat()])
        return result.fetchall()[0][0]
    finally:
        conn.unregister("_delivery_df")


def load_delivery_history(
    from_date: str,
    to_date: str,
    conn=None,
    db_path: Optional[Path] = None,
    in_memory: bool = False,
) -> Dict[str, int]:
    """
    Backfill delivery_qty/delivery_pct for every NSE trading day in range.

    A single failed date (e.g. a very old archive NSE no longer serves) is
    logged and skipped — it must never abort the whole 5-year run.

    Parameters
    ----------
    from_date : str
        "YYYY-MM-DD".
    to_date : str
        "YYYY-MM-DD".
    conn : duckdb.DuckDBPyConnection, optional
        If provided, used directly (and NOT closed) — lets callers control
        the connection lifecycle (e.g. backfill_runner reusing one
        connection across both OHLCV and delivery backfill).
    db_path : Path, optional
        Used only when `conn` is None. Defaults to
        config.settings.DUCKDB_PATH.
    in_memory : bool
        Used only when `conn` is None — for tests.

    Returns
    -------
    dict
        "YYYY-MM-DD" -> tickers updated for that date (0 for skipped/
        failed dates).

    Spec References
    ----------------
    SPEC-PIPE-001: "Parses NSE historical bhavcopy archives for delivery
    data (5 years)."

    PIT Assumptions
    ----------------
    None.

    Raises
    ------
    None — per-date failures are caught and logged.
    """
    start = date_type.fromisoformat(from_date)
    end = date_type.fromisoformat(to_date)

    results: Dict[str, int] = {}

    def _run(active_conn) -> None:
        cursor = start
        while cursor <= end:
            if not is_trading_day(cursor):
                cursor += timedelta(days=1)
                continue
            try:
                delivery_df = download_delivery_for_date(cursor)
                count = merge_delivery_into_ohlcv(active_conn, cursor, delivery_df)
                results[cursor.isoformat()] = count
            except Exception as exc:
                logger.warning(f"Delivery backfill failed for {cursor}: {exc}")
                results[cursor.isoformat()] = 0
            time.sleep(NSE_FETCH_THROTTLE_SECONDS)
            cursor += timedelta(days=1)

    if conn is not None:
        _run(conn)
    else:
        from datastore.api.db import get_duckdb_connection
        from config.settings import DUCKDB_PATH

        resolved_path = None if in_memory else (db_path or DUCKDB_PATH)
        with get_duckdb_connection(resolved_path) as new_conn:
            _run(new_conn)

    total = sum(results.values())
    logger.info(f"Delivery backfill complete: {len(results)} trading days processed, {total} ticker-rows updated")
    return results
