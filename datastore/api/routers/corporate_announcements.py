"""
datastore/api/routers/corporate_announcements.py

Phase: follow-up (Corporate Announcements feature, 2026-07-07)
Specs: SPEC-DS-001, SPEC-DS-002
Owner: Platform / DataStore
Consumers: dashboard/static/*, ingestion/scrapers/nse_corporate_announcements.py (write side)

Read endpoints over the corporate_announcements table (real NSE
Corporate Announcements feed, material-event categories only — see
ingestion/scrapers/nse_corporate_announcements.py's module docstring for
the source and category taxonomy). No write endpoint here: the scheduler
(ingestion/scheduler/daily_pipeline.py's step_download_macro_morning)
writes directly against DUCKDB_PATH via upsert_corporate_announcements,
same established precedent as corporate_actions.py.

[AS BUILT, SPEC-SCHED-013] persist=False — releases the write lock
immediately, same as every other read-only router in this package.
"""

import logging
from datetime import date as date_type, timedelta
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/corporate-announcements", tags=["Corporate Announcements"])

_COLUMNS = [
    "seq_id", "ticker", "company_name", "category", "subject",
    "announcement_text", "announced_at", "exchange_disseminated_at", "attachment_url",
]
_SELECT_COLS = ", ".join(_COLUMNS)

VALID_CATEGORIES = {
    "buyback", "qip", "board_change", "investigation", "insider",
    "credit_rating", "auditor_change", "ma",
}


class CorporateAnnouncementRow(BaseModel):
    seq_id: str
    ticker: str
    company_name: Optional[str] = None
    category: str
    subject: Optional[str] = None
    announcement_text: Optional[str] = None
    announced_at: str
    exchange_disseminated_at: Optional[str] = None
    attachment_url: Optional[str] = None


class CorporateAnnouncementResponse(BaseModel):
    data: List[CorporateAnnouncementRow]
    record_count: int


def _row_to_model(row: tuple) -> CorporateAnnouncementRow:
    d = dict(zip(_COLUMNS, row))
    d["announced_at"] = d["announced_at"].isoformat() if d["announced_at"] is not None else None
    d["exchange_disseminated_at"] = (
        d["exchange_disseminated_at"].isoformat() if d["exchange_disseminated_at"] is not None else None
    )
    return CorporateAnnouncementRow(**d)


@router.get("/recent", response_model=CorporateAnnouncementResponse)
async def get_recent_announcements(
    days: int = Query(5, ge=1, le=30, description="How many trailing calendar days to return"),
    category: Optional[str] = Query(None, description="Filter to one category, e.g. 'buyback'"),
) -> CorporateAnnouncementResponse:
    """Latest N trailing days of material corporate announcements, newest first (dashboard feed default view)."""
    if category and category not in VALID_CATEGORIES:
        raise HTTPException(status_code=400, detail=f"Unknown category '{category}'. Must be one of {sorted(VALID_CATEGORIES)}")

    cutoff = date_type.today() - timedelta(days=days)
    conditions = ["announced_at >= ?"]
    params = [cutoff]
    if category:
        conditions.append("category = ?")
        params.append(category)

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        rows = conn.execute(
            f"SELECT {_SELECT_COLS} FROM corporate_announcements "
            f"WHERE {' AND '.join(conditions)} ORDER BY announced_at DESC",
            params,
        ).fetchall()

    data = [_row_to_model(r) for r in rows]
    return CorporateAnnouncementResponse(data=data, record_count=len(data))


@router.get("/search", response_model=CorporateAnnouncementResponse)
async def search_announcements(
    ticker: Optional[str] = Query(None, description="Exact ticker match"),
    company: Optional[str] = Query(None, description="Case-insensitive substring match on company_name"),
    category: Optional[str] = Query(None),
    from_date: Optional[date_type] = Query(None, alias="from"),
    to_date: Optional[date_type] = Query(None, alias="to"),
    limit: int = Query(200, ge=1, le=2000),
) -> CorporateAnnouncementResponse:
    """
    Search corporate announcements by ticker, company name (substring), category,
    and/or date range. All filters are optional and combine with AND; at least
    one of ticker/company must be supplied to avoid an unbounded full-table scan.
    """
    if not ticker and not company:
        raise HTTPException(status_code=400, detail="Provide at least one of 'ticker' or 'company'")
    if category and category not in VALID_CATEGORIES:
        raise HTTPException(status_code=400, detail=f"Unknown category '{category}'. Must be one of {sorted(VALID_CATEGORIES)}")
    if from_date and to_date and from_date > to_date:
        raise HTTPException(status_code=400, detail="from must be <= to")

    conditions = []
    params = []
    if ticker:
        conditions.append("ticker = ?")
        params.append(ticker)
    if company:
        conditions.append("lower(company_name) LIKE ?")
        params.append(f"%{company.lower()}%")
    if category:
        conditions.append("category = ?")
        params.append(category)
    if from_date:
        conditions.append("announced_at >= ?")
        params.append(from_date)
    if to_date:
        conditions.append("announced_at < ?")
        params.append(to_date + timedelta(days=1))

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        rows = conn.execute(
            f"SELECT {_SELECT_COLS} FROM corporate_announcements "
            f"WHERE {' AND '.join(conditions)} ORDER BY announced_at DESC LIMIT ?",
            params + [limit],
        ).fetchall()

    data = [_row_to_model(r) for r in rows]
    return CorporateAnnouncementResponse(data=data, record_count=len(data))
