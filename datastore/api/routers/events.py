"""
datastore/api/routers/events.py

A72 (2026-07-13, partial) — cross-cutting Events endpoint: one ticker's
timeline of real, dated events for the chart overlay described in
FeatureBacklog.md A72.

Covers 3 of the originally-scoped 4 event types this pass:
  - corporate_action: real rows from `corporate_actions` (existing table,
    read-only reuse — no new schema).
  - bulk_deal: real rows from `bulk_deal_positions` (existing table,
    read-only reuse — no new schema).
  - recommendation_trigger: a ticker crossing INTO a "buy" signal_5d call
    (i.e. `signal_direction == 'buy'` on a date where the immediately
    preceding real `ml_signals` row for signal_5d was NOT 'buy', or there
    was no prior row) — sourced entirely from existing `ml_signals`
    history, no new table.

"forensic-flag date" (the 4th originally-scoped type) is intentionally
NOT included — `ml_forensic` only records composite scores/labels as of
each (infrequent, quarterly) scoring date, not a discrete "flag raised on
this date" event; turning that into a real "date this flag first
appeared" event would need its own dedup/definition pass. Logged as a
follow-up in FeatureBacklog.md rather than guessed here.
"""

import logging
from typing import List, Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

from config.settings import DUCKDB_PATH, SIGNALS_DUCKDB_PATH
from datastore.api.db import get_duckdb_connection

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/events", tags=["Events"])


class EventRow(BaseModel):
    date: str
    event_type: str
    description: str


@router.get("/{ticker}", response_model=List[EventRow])
async def get_events(
    ticker: str,
    from_date: Optional[str] = Query(None, description="YYYY-MM-DD, inclusive"),
    to_date: Optional[str] = Query(None, description="YYYY-MM-DD, inclusive"),
) -> List[EventRow]:
    """Real events for `ticker` across corporate actions, bulk/block
    deals, and buy-signal-crossing triggers, sorted by date ascending."""
    ticker = ticker.upper()
    events: List[EventRow] = []

    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=True) as conn:
        ca_where = "ticker = ?"
        ca_params = [ticker]
        if from_date is not None:
            ca_where += " AND ex_date >= ?"
            ca_params.append(from_date)
        if to_date is not None:
            ca_where += " AND ex_date <= ?"
            ca_params.append(to_date)
        ca_rows = conn.execute(
            f"SELECT ex_date, action_type, ratio, details FROM corporate_actions WHERE {ca_where} ORDER BY ex_date",
            ca_params,
        ).fetchall()
        for ex_date, action_type, ratio, details in ca_rows:
            desc = f"{action_type} (ratio {ratio})" if ratio else action_type
            if details:
                desc += f" — {details}"
            events.append(EventRow(date=str(ex_date), event_type="corporate_action", description=desc))

        bd_where = "ticker = ?"
        bd_params = [ticker]
        if from_date is not None:
            bd_where += " AND trade_date >= ?"
            bd_params.append(from_date)
        if to_date is not None:
            bd_where += " AND trade_date <= ?"
            bd_params.append(to_date)
        bd_rows = conn.execute(
            f"""
            SELECT trade_date, deal_type, net_transaction_type, net_quantity, is_new_entry, is_full_exit
            FROM bulk_deal_positions WHERE {bd_where} ORDER BY trade_date
            """,
            bd_params,
        ).fetchall()
        for trade_date, deal_type, net_txn_type, net_qty, is_new_entry, is_full_exit in bd_rows:
            tag = "new entry" if is_new_entry else ("full exit" if is_full_exit else (net_txn_type or ""))
            desc = f"{deal_type} deal — {tag}" + (f" ({fmtint(net_qty)} shares)" if net_qty else "")
            events.append(EventRow(date=str(trade_date), event_type="bulk_deal", description=desc))

    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
        sig_where = "ticker = ? AND model_name = 'signal_5d'"
        sig_params = [ticker]
        if from_date is not None:
            sig_where += " AND date >= ?"
            sig_params.append(from_date)
        if to_date is not None:
            sig_where += " AND date <= ?"
            sig_params.append(to_date)
        sig_rows = conn.execute(
            f"SELECT date, signal_direction FROM ml_signals WHERE {sig_where} ORDER BY date",
            sig_params,
        ).fetchall()
        prev_direction = None
        for sig_date, direction in sig_rows:
            if direction == "buy" and prev_direction != "buy":
                events.append(
                    EventRow(
                        date=str(sig_date), event_type="recommendation_trigger",
                        description="signal_5d crossed into a Buy recommendation",
                    )
                )
            prev_direction = direction

    events.sort(key=lambda e: e.date)
    return events


def fmtint(x) -> str:
    try:
        return f"{int(x):,}"
    except (TypeError, ValueError):
        return str(x)
