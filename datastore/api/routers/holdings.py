"""
datastore/api/routers/holdings.py

ML30 (2026-07-13) — MyHoldings: real DB-backed CRUD, replacing
dashboard/static/ml/holdings.html's prior browser-localStorage-only
storage (2026-07-11 exploration note in FeatureBacklog.md). Backs
`my_holdings` (datastore/schema/create_normalised.py, ticker/
purchase_date/qty/purchase_price/sale_date/sell_price/
purchase_rationale/sell_rationale/journal_entry).

Table creation is idempotent (`CREATE TABLE IF NOT EXISTS`, same
convention every other table in this schema uses) and happens lazily on
first use via `_ensure_table(conn)` — this session deliberately never ran
it against the real production `datastore/normalised/alphalens.duckdb`
(ML31/A26 jobs may hold its write lock; see FeatureBacklog.md ML30), but
it is safe to run whenever this router first receives a real request
against that file, exactly like every other lazily-created table in this
codebase.

Endpoints
---------
GET    /api/v1/holdings/            — list all holdings (optionally
                                       filtered to open positions only)
POST   /api/v1/holdings/            — create one holding
PUT    /api/v1/holdings/{id}        — update one holding (e.g. record a
                                       sale: sale_date/sell_price/
                                       sell_rationale)
DELETE /api/v1/holdings/{id}        — delete one holding
POST   /api/v1/holdings/upload-csv  — bulk-create from an uploaded CSV
                                       (columns: ticker, purchase_date,
                                       qty, purchase_price, and optionally
                                       sale_date, sell_price,
                                       purchase_rationale, sell_rationale,
                                       journal_entry)
"""

import csv
import io
import logging
from datetime import date as date_type
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from datastore.schema.create_normalised import _CREATE_MY_HOLDINGS

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/holdings", tags=["MyHoldings"])

_COLUMNS = [
    "id", "ticker", "purchase_date", "qty", "purchase_price",
    "sale_date", "sell_price", "purchase_rationale", "sell_rationale",
    "journal_entry",
]


def _ensure_table(conn) -> None:
    conn.execute(_CREATE_MY_HOLDINGS)


class HoldingCreate(BaseModel):
    ticker: str
    purchase_date: date_type
    qty: float
    purchase_price: Optional[float] = None
    sale_date: Optional[date_type] = None
    sell_price: Optional[float] = None
    purchase_rationale: Optional[str] = None
    sell_rationale: Optional[str] = None
    journal_entry: Optional[str] = None


class HoldingUpdate(BaseModel):
    """All fields optional — PUT only overwrites what's provided."""
    ticker: Optional[str] = None
    purchase_date: Optional[date_type] = None
    qty: Optional[float] = None
    purchase_price: Optional[float] = None
    sale_date: Optional[date_type] = None
    sell_price: Optional[float] = None
    purchase_rationale: Optional[str] = None
    sell_rationale: Optional[str] = None
    journal_entry: Optional[str] = None


class HoldingRow(BaseModel):
    id: int
    ticker: str
    purchase_date: date_type
    qty: float
    purchase_price: Optional[float] = None
    sale_date: Optional[date_type] = None
    sell_price: Optional[float] = None
    purchase_rationale: Optional[str] = None
    sell_rationale: Optional[str] = None
    journal_entry: Optional[str] = None


def _row_to_dict(row: tuple) -> dict:
    return dict(zip(_COLUMNS, row))


@router.get("/", response_model=List[HoldingRow])
async def list_holdings(open_only: bool = False) -> List[HoldingRow]:
    """All holdings, most recently purchased first. `open_only=True`
    restricts to positions with no sale_date recorded yet."""
    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
        _ensure_table(conn)
        where = "WHERE sale_date IS NULL" if open_only else ""
        rows = conn.execute(
            f"SELECT {', '.join(_COLUMNS)} FROM my_holdings {where} ORDER BY purchase_date DESC, id DESC"
        ).fetchall()
    return [HoldingRow(**_row_to_dict(r)) for r in rows]


@router.post("/", response_model=HoldingRow)
async def create_holding(holding: HoldingCreate) -> HoldingRow:
    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
        _ensure_table(conn)
        new_id = conn.execute(
            """
            INSERT INTO my_holdings
                (ticker, purchase_date, qty, purchase_price, sale_date, sell_price,
                 purchase_rationale, sell_rationale, journal_entry)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING id
            """,
            [
                holding.ticker.upper(), holding.purchase_date, holding.qty, holding.purchase_price,
                holding.sale_date, holding.sell_price, holding.purchase_rationale,
                holding.sell_rationale, holding.journal_entry,
            ],
        ).fetchone()[0]
        row = conn.execute(
            f"SELECT {', '.join(_COLUMNS)} FROM my_holdings WHERE id = ?", [new_id]
        ).fetchone()
    return HoldingRow(**_row_to_dict(row))


@router.put("/{holding_id}", response_model=HoldingRow)
async def update_holding(holding_id: int, update: HoldingUpdate) -> HoldingRow:
    fields = update.model_dump(exclude_unset=True)
    if not fields:
        raise HTTPException(status_code=400, detail="No fields provided to update")

    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
        _ensure_table(conn)
        existing = conn.execute("SELECT id FROM my_holdings WHERE id = ?", [holding_id]).fetchone()
        if existing is None:
            raise HTTPException(status_code=404, detail=f"Holding {holding_id} not found")

        set_clause = ", ".join(f"{col} = ?" for col in fields)
        params = list(fields.values()) + [holding_id]
        conn.execute(
            f"UPDATE my_holdings SET {set_clause}, updated_at = current_timestamp WHERE id = ?",
            params,
        )
        row = conn.execute(
            f"SELECT {', '.join(_COLUMNS)} FROM my_holdings WHERE id = ?", [holding_id]
        ).fetchone()
    return HoldingRow(**_row_to_dict(row))


@router.delete("/{holding_id}")
async def delete_holding(holding_id: int) -> dict:
    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
        _ensure_table(conn)
        existing = conn.execute("SELECT id FROM my_holdings WHERE id = ?", [holding_id]).fetchone()
        if existing is None:
            raise HTTPException(status_code=404, detail=f"Holding {holding_id} not found")
        conn.execute("DELETE FROM my_holdings WHERE id = ?", [holding_id])
    return {"deleted": True, "id": holding_id}


_CSV_REQUIRED_COLUMNS = {"ticker", "purchase_date", "qty"}
_CSV_OPTIONAL_COLUMNS = [
    "purchase_price", "sale_date", "sell_price",
    "purchase_rationale", "sell_rationale", "journal_entry",
]


@router.post("/upload-csv", response_model=List[HoldingRow])
async def upload_holdings_csv(request: Request) -> List[HoldingRow]:
    """
    Bulk-create holdings from an uploaded CSV, sent as the raw request
    body (dashboard/static/ml/js/holdings.js reads the chosen file with
    `<input type="file">`'s FileReader and POSTs its text content
    directly — deliberately not a `multipart/form-data` upload, which
    would need the `python-multipart` package this project doesn't
    currently depend on for anything else). Required columns: ticker,
    purchase_date, qty. Optional: purchase_price, sale_date, sell_price,
    purchase_rationale, sell_rationale, journal_entry — missing optional
    columns/blank cells become real NULLs (never a fabricated 0/"").

    Raises
    ------
    HTTPException (400)
        If the CSV is missing a required column, or has zero real rows.
    """
    raw = await request.body()
    text = raw.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        raise HTTPException(status_code=400, detail="CSV has no header row")
    missing = _CSV_REQUIRED_COLUMNS - set(reader.fieldnames)
    if missing:
        raise HTTPException(status_code=400, detail=f"CSV missing required column(s): {sorted(missing)}")

    created_ids: List[int] = []
    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
        _ensure_table(conn)
        for line_num, raw_row in enumerate(reader, start=2):
            ticker = (raw_row.get("ticker") or "").strip().upper()
            purchase_date = (raw_row.get("purchase_date") or "").strip()
            qty_str = (raw_row.get("qty") or "").strip()
            if not ticker or not purchase_date or not qty_str:
                logger.warning(f"upload_holdings_csv: skipping row {line_num} — missing required value(s)")
                continue
            optional_values = {}
            for col in _CSV_OPTIONAL_COLUMNS:
                val = (raw_row.get(col) or "").strip()
                optional_values[col] = val if val else None
            new_id = conn.execute(
                """
                INSERT INTO my_holdings
                    (ticker, purchase_date, qty, purchase_price, sale_date, sell_price,
                     purchase_rationale, sell_rationale, journal_entry)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING id
                """,
                [
                    ticker, purchase_date, float(qty_str),
                    float(optional_values["purchase_price"]) if optional_values["purchase_price"] else None,
                    optional_values["sale_date"],
                    float(optional_values["sell_price"]) if optional_values["sell_price"] else None,
                    optional_values["purchase_rationale"], optional_values["sell_rationale"],
                    optional_values["journal_entry"],
                ],
            ).fetchone()[0]
            created_ids.append(new_id)

        if not created_ids:
            raise HTTPException(status_code=400, detail="No valid rows found in CSV")

        placeholders = ",".join("?" for _ in created_ids)
        rows = conn.execute(
            f"SELECT {', '.join(_COLUMNS)} FROM my_holdings WHERE id IN ({placeholders}) ORDER BY id",
            created_ids,
        ).fetchall()
    return [HoldingRow(**_row_to_dict(r)) for r in rows]
