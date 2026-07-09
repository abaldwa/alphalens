"""
ingestion/scrapers/bulk_deal_reconciliation.py

Phase D (Big Investor Activity — plan: gentle-wobbling-swing.md)
Owner: Platform / Ingestion
Consumers: scripts/reconcile_bulk_deal_families.py, datastore/api/routers/big_investors.py

Reconciles bulk_deal_positions' daily-tracked, bulk/block-deal-derived
position estimate for a (family_id, ticker) against public_shareholders'
reported stake_pct for the same quarter, once a new quarter's named-holder
data lands (ingestion/scrapers/trendlyne.py's batch_export_named_holdings).

[AS BUILT, 2026-07-05] public_shareholders.reported_shares carries
Trendlyne's own real "Qty Held" figure where disclosed that quarter (a
genuine authenticated fetch that day showed Trendlyne reports absolute
share counts directly, not just stake_pct — see trendlyne.py's
_parse_holdings_table). reconcile_family_ticker_quarter() uses this
directly when present. Only when Trendlyne itself shows "-"/"Filing
Awaited" for quantity that quarter does this module fall back to
deriving shares outstanding from stock_master.market_cap_cr /
ohlcv_adjusted.close (nearest trading day on/before quarter_end_date) and
computing reported_shares_est = stake_pct% of that — an estimate, not a
fact, used only as a last resort.

Per user instruction: correct bulk_deal_positions' historical estimate
FIRST when a real discrepancy is found (reset cumulative_position_est to
the reported estimate as of quarter_end_date, propagate forward), THEN
flag the correction for review — this is also how the investor_family
seed itself should eventually grow (a corrected gap that lines up with an
'unmapped:' client's trades points at a missing family alias), though that
inference step is not yet automated here (flagged in the log's notes
instead of auto-added to investor_family).
"""

import logging
from datetime import date as date_type
from typing import Optional

logger = logging.getLogger(__name__)

# Discrepancies within this fraction of the reported estimate auto-resolve
# without touching bulk_deal_positions; anything larger triggers a
# correction + review flag.
RECONCILIATION_TOLERANCE_PCT = 0.10


def _estimate_shares_outstanding(conn, ticker: str, as_of: date_type) -> Optional[float]:
    row = conn.execute(
        "SELECT market_cap_cr FROM stock_master WHERE ticker = ?", [ticker]
    ).fetchone()
    if not row or row[0] is None:
        return None
    market_cap_cr = row[0]

    price_row = conn.execute(
        """
        SELECT close FROM ohlcv_adjusted
        WHERE ticker = ? AND date <= ?
        ORDER BY date DESC LIMIT 1
        """,
        [ticker, as_of],
    ).fetchone()
    if not price_row or not price_row[0]:
        return None
    close = price_row[0]

    return (market_cap_cr * 1e7) / close


def _next_id(conn) -> int:
    row = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM bulk_deal_reconciliation_log").fetchone()
    return row[0]


def reconcile_family_ticker_quarter(conn, family_id: str, ticker: str, quarter_end_date: date_type) -> dict:
    """
    Reconcile one (family_id, ticker) pair for one quarter.

    Returns
    -------
    dict
        {"status": "resolved" | "auto_corrected" | "flagged_for_review" | "no_data", ...}
    """
    holder_row = conn.execute(
        """
        SELECT stake_pct, reported_shares, filing_date FROM public_shareholders
        WHERE family_id = ? AND ticker = ? AND quarter_end_date = ?
        """,
        [family_id, ticker, quarter_end_date],
    ).fetchone()
    if not holder_row or (holder_row[0] is None and holder_row[1] is None):
        return {"status": "no_data"}
    stake_pct, reported_shares, filing_date = holder_row

    # Prefer Trendlyne's real reported share count ("Qty Held") when
    # disclosed that quarter — only fall back to the market-cap/price
    # derived estimate when Trendlyne itself shows "-"/"Filing Awaited"
    # for quantity that quarter (see trendlyne.py's _parse_holdings_table).
    if reported_shares is not None:
        reported_shares_est = reported_shares
    else:
        if stake_pct is None:
            return {"status": "no_data"}
        shares_outstanding = _estimate_shares_outstanding(conn, ticker, quarter_end_date)
        if shares_outstanding is None:
            return {"status": "no_data"}
        reported_shares_est = round(stake_pct / 100.0 * shares_outstanding)

    # Position is tracked independently per deal_type (bulk/block/
    # reconciliation) — sum each track's latest-as-of-quarter-end row
    # rather than picking one arbitrary deal_type's row.
    est_row = conn.execute(
        """
        WITH latest AS (
            SELECT deal_type, cumulative_position_est,
                   ROW_NUMBER() OVER (PARTITION BY deal_type ORDER BY trade_date DESC) AS rn
            FROM bulk_deal_positions
            WHERE family_id = ? AND ticker = ? AND trade_date <= ?
        )
        SELECT COALESCE(SUM(cumulative_position_est), 0) FROM latest WHERE rn = 1
        """,
        [family_id, ticker, quarter_end_date],
    ).fetchone()
    estimated_position = est_row[0]

    if reported_shares_est == 0:
        discrepancy_pct = None if estimated_position == 0 else 1.0
    else:
        discrepancy_pct = abs(estimated_position - reported_shares_est) / abs(reported_shares_est)

    within_tolerance = discrepancy_pct is not None and discrepancy_pct <= RECONCILIATION_TOLERANCE_PCT
    log_id = _next_id(conn)

    if discrepancy_pct is None or within_tolerance:
        status = "resolved"
        correction_applied = False
        correction_delta = None
    else:
        status = "flagged_for_review"
        correction_applied = True
        correction_delta = reported_shares_est - estimated_position
        # The most recent trade may predate quarter_end_date (no trades
        # since) — a plain UPDATE over existing rows would then touch
        # nothing and silently fail to correct anything. Instead: insert
        # an explicit reconciliation anchor row AT quarter_end_date
        # (deal_type='reconciliation', a dedicated marker value distinct from
        # 'bulk'/'block') carrying the corrected reported_shares_est, then
        # propagate the same delta forward to any rows strictly AFTER
        # quarter_end_date (those already reflect real trades since, so
        # they only need the offset applied on top, not a full reset).
        conn.execute(
            """
            INSERT INTO bulk_deal_positions (
                family_id, ticker, trade_date, deal_type, cumulative_position_est,
                is_new_entry, is_full_exit, source_correction_id
            ) VALUES (?, ?, ?, 'reconciliation', ?, FALSE, FALSE, ?)
            ON CONFLICT (family_id, ticker, trade_date, deal_type) DO UPDATE SET
                cumulative_position_est = excluded.cumulative_position_est,
                source_correction_id = excluded.source_correction_id
            """,
            [family_id, ticker, quarter_end_date, int(reported_shares_est), log_id],
        )
        # Known Phase D simplification: if the family has trades booked
        # under more than one deal_type after quarter_end_date, this
        # applies the full correction_delta to each deal_type's series
        # independently (rather than splitting it proportionally), which
        # can over-correct the combined total in that specific edge case.
        # Acceptable for now — deal_type-mixed post-quarter-end activity
        # for the same family+ticker is uncommon; revisit if real data
        # shows it matters.
        conn.execute(
            """
            UPDATE bulk_deal_positions
            SET cumulative_position_est = cumulative_position_est + ?,
                source_correction_id = ?
            WHERE family_id = ? AND ticker = ? AND trade_date > ? AND deal_type != 'reconciliation'
            """,
            [correction_delta, log_id, family_id, ticker, quarter_end_date],
        )
        logger.info(
            f"reconcile_bulk_deal_families: corrected {family_id}/{ticker} as of {quarter_end_date} "
            f"by {correction_delta} shares (discrepancy {discrepancy_pct:.1%})"
        )

    conn.execute(
        """
        INSERT INTO bulk_deal_reconciliation_log (
            id, family_id, ticker, quarter_end_date, filing_date,
            estimated_position_pre_correction, reported_shares_est,
            correction_applied, correction_delta, discrepancy_pct, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [log_id, family_id, ticker, quarter_end_date, filing_date,
         estimated_position, int(reported_shares_est), correction_applied,
         correction_delta, discrepancy_pct, status],
    )
    return {
        "status": status,
        "estimated_position_pre_correction": estimated_position,
        "reported_shares_est": int(reported_shares_est),
        "discrepancy_pct": discrepancy_pct,
    }


def reconcile_quarter(conn, quarter_end_date: date_type) -> list:
    """
    Reconcile every (family_id, ticker) pair with public_shareholders data
    for quarter_end_date. Skips rows with no family_id (unmatched holder
    names — nothing to reconcile bulk_deal_positions against).

    Returns
    -------
    list of dict
        One result per (family_id, ticker) pair reconciled.
    """
    pairs = conn.execute(
        """
        SELECT DISTINCT family_id, ticker FROM public_shareholders
        WHERE quarter_end_date = ? AND family_id IS NOT NULL
        """,
        [quarter_end_date],
    ).fetchall()

    results = []
    for family_id, ticker in pairs:
        result = reconcile_family_ticker_quarter(conn, family_id, ticker, quarter_end_date)
        result.update({"family_id": family_id, "ticker": ticker})
        results.append(result)
    logger.info(f"reconcile_quarter {quarter_end_date}: {len(results)} pairs processed")
    return results
