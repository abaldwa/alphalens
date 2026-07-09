"""
ingestion/scrapers/bulk_deal_attribution.py

Phase B (Big Investor Activity — plan: gentle-wobbling-swing.md)
Owner: Platform / Ingestion
Consumers: ingestion/scheduler/daily_pipeline.py (step_attribute_bulk_deals)

Derives `bulk_deal_positions` from `large_deals` + `investor_family` for one
trade_date: nets same-day wash trades (a client both buying and selling a
near-matching quantity of the same ticker), attributes each client to a
known investor family (or 'unmapped:<normalized_name>' if not seeded yet),
and tracks a running position estimate per (family_id, ticker, deal_type).

[AS BUILT, Phase B simplification] Netting and position-tracking are scoped
per deal_type (bulk vs block) rather than combined, even though the same
client can in principle appear in both on the same day — this keeps the
`bulk_deal_positions` PRIMARY KEY (family_id, ticker, trade_date, deal_type)
simple and covers the common case (wash trades are typically both-bulk or
both-block). Revisit if real data shows cross-deal-type wash trades.
This table is a derived, rebuildable artifact — never a second source of
truth over large_deals + investor_family.
"""

import logging
import re
from datetime import date as date_type
from typing import Optional

import pandas as pd

from config.settings import INTRADAY_NETTING_QTY_TOLERANCE_PCT

logger = logging.getLogger(__name__)


def normalize_client_name(name: Optional[str]) -> str:
    """Upper-case, whitespace-collapsed form used as the investor_family join key."""
    if not name:
        return ""
    return re.sub(r"\s+", " ", name.strip().upper())


def _load_family_map(conn) -> dict:
    rows = conn.execute("SELECT entity_name, family_id FROM investor_family").fetchall()
    return dict(rows)


def _net_group(buy_qty: float, sell_qty: float, buy_value: float, sell_value: float) -> tuple:
    """
    Net BUY vs SELL quantity for one (family, ticker, deal_type, trade_date) group.

    The position-relevant number is always the directional residual
    (buy_qty - sell_qty): a client fully washing out (equal buy/sell) nets
    to zero and is dropped; a near-equal-but-not-exact pair (within
    config.settings.INTRADAY_NETTING_QTY_TOLERANCE_PCT) still resolves to
    the same small residual either way, so there is no separate "keep both
    sides" branch — only whether the wash was substantial enough to flag
    for audit (see the caller's log line).

    Returns (net_transaction_type, net_quantity, avg_price) — net_transaction_type
    is None and net_quantity is 0 if the group fully washes out.
    """
    net_qty_signed = buy_qty - sell_qty
    if net_qty_signed == 0:
        return None, 0, None
    if net_qty_signed > 0:
        avg_price = (buy_value / buy_qty) if buy_qty else None
        return "BUY", net_qty_signed, avg_price
    avg_price = (sell_value / sell_qty) if sell_qty else None
    return "SELL", abs(net_qty_signed), avg_price


def _is_substantial_wash(buy_qty: float, sell_qty: float) -> bool:
    """True if both sides are present and matched within the configured tolerance."""
    if buy_qty <= 0 or sell_qty <= 0:
        return False
    matched, larger = min(buy_qty, sell_qty), max(buy_qty, sell_qty)
    return (matched / larger) >= (1 - INTRADAY_NETTING_QTY_TOLERANCE_PCT)


def attribute_bulk_deals(conn, run_date: date_type) -> int:
    """
    Rebuild bulk_deal_positions rows for run_date from large_deals + investor_family.

    Idempotent for this date — deletes any existing bulk_deal_positions rows
    for run_date before inserting the freshly computed ones.

    Returns
    -------
    int
        Number of (family_id, ticker, deal_type) position rows written.
    """
    date_str = run_date.isoformat()
    rows = conn.execute(
        """
        SELECT exchange, deal_type, ticker, client_name, transaction_type, quantity, price
        FROM large_deals WHERE trade_date = ?
        """,
        [date_str],
    ).fetchall()
    if not rows:
        return 0

    df = pd.DataFrame(
        rows, columns=["exchange", "deal_type", "ticker", "client_name", "transaction_type", "quantity", "price"]
    )
    df["client_norm"] = df["client_name"].map(normalize_client_name)
    family_map = _load_family_map(conn)
    df["family_id"] = df["client_norm"].map(lambda c: family_map.get(c, f"unmapped:{c}"))
    df["value"] = df["quantity"].fillna(0) * df["price"].fillna(0)

    conn.execute("DELETE FROM bulk_deal_positions WHERE trade_date = ?", [date_str])

    written = 0
    for (family_id, ticker, deal_type), grp in df.groupby(["family_id", "ticker", "deal_type"]):
        # [AS BUILT, 2026-07-05] REAL BUG FOUND: large_deals.transaction_type
        # is normalised to "B"/"S" (ingestion/scrapers/large_deals.py's
        # _normalise_transaction_type), never the literal "BUY"/"SELL" this
        # used to filter on — silently produced 0 attributed rows against
        # any real data (undetected until large_deals itself had real rows
        # to attribute, since it was empty before this session's NSE
        # archive-CSV fix).
        buys = grp[grp["transaction_type"] == "B"]
        sells = grp[grp["transaction_type"] == "S"]
        buy_qty, sell_qty = buys["quantity"].sum(), sells["quantity"].sum()
        buy_value, sell_value = buys["value"].sum(), sells["value"].sum()

        net_type, net_qty, avg_price = _net_group(buy_qty, sell_qty, buy_value, sell_value)
        if _is_substantial_wash(buy_qty, sell_qty):
            logger.info(
                f"attribute_bulk_deals: {family_id}/{ticker}/{deal_type} substantial same-day "
                f"wash on {date_str} (buy={buy_qty}, sell={sell_qty}, net={net_qty or 0})"
            )
        if net_type is None:
            continue

        exchange = ",".join(sorted(grp["exchange"].dropna().unique()))
        prior = conn.execute(
            """
            SELECT cumulative_position_est FROM bulk_deal_positions
            WHERE family_id = ? AND ticker = ? AND deal_type = ? AND trade_date < ?
            ORDER BY trade_date DESC LIMIT 1
            """,
            [family_id, ticker, deal_type, date_str],
        ).fetchone()
        prior_pos = prior[0] if prior else 0
        delta = net_qty if net_type == "BUY" else -net_qty
        new_pos = prior_pos + delta
        is_new_entry = prior_pos == 0 and new_pos != 0
        is_full_exit = prior_pos > 0 and new_pos <= 0

        conn.execute(
            """
            INSERT INTO bulk_deal_positions (
                family_id, ticker, trade_date, deal_type, net_transaction_type,
                net_quantity, avg_price, exchange, cumulative_position_est,
                is_new_entry, is_full_exit
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [family_id, ticker, date_str, deal_type, net_type, int(net_qty), avg_price,
             exchange, int(new_pos), bool(is_new_entry), bool(is_full_exit)],
        )
        written += 1

    logger.info(f"attribute_bulk_deals: {written} family/ticker/deal_type positions for {date_str}")
    return written
