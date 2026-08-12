"""
datastore/api/routers/portfolios.py

Phase: FeatureBacklog.md ML38 — momentum strategy consolidation (2026-08-09)
Owner: Platform / Backend
Consumers: frontend/src/pages/portfolios.tsx,
    frontend/src/pages/momentum/StrategyDeployPage.tsx (portfolio_id picker)

Generic, cross-channel Portfolio module (not momentum-specific) — see
datastore/schema/create_normalised.py's `portfolios`/`portfolio_cash_flows`
tables for the schema rationale. Replaces the orphaned
`momentum_strategy_configs.portfolio_id` integer (added 2026-08-08, no
table behind it until now) with a real, working reference.

Endpoints
---------
GET    /api/v1/portfolios/                    — list all portfolios
POST   /api/v1/portfolios/                    — create a portfolio
GET    /api/v1/portfolios/{id}                — get one portfolio
PATCH  /api/v1/portfolios/{id}                — update name/description/is_active
GET    /api/v1/portfolios/{id}/cash_flows     — list cash flows
POST   /api/v1/portfolios/{id}/cash_flows     — record a deposit/withdrawal/SIP/etc
GET    /api/v1/portfolios/{id}/nav?as_of=     — NAV/cash/holdings/XIRR as of a date
GET    /api/v1/portfolios/{id}/trades         — momentum_trades tagged to this portfolio

Table creation is idempotent (`CREATE TABLE IF NOT EXISTS`, same
convention every other table in this schema uses), lazy via
`_ensure_tables(conn)` — matches datastore/api/routers/holdings.py.
"""

import logging
from datetime import date as date_type
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from config.settings import DUCKDB_PATH
from config.timezone import now_ist
from datastore.api.db import get_duckdb_connection
from datastore.api.portfolio_nav import compute_nav
from datastore.schema.create_normalised import _CREATE_PORTFOLIO_CASH_FLOWS, _CREATE_PORTFOLIOS

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/portfolios", tags=["Portfolios"])

_PORTFOLIO_COLUMNS = [
    "portfolio_id", "name", "description", "channel", "base_capital",
    "is_active", "created_at", "updated_at",
]
_CASH_FLOW_COLUMNS = ["id", "portfolio_id", "date", "amount", "kind", "note", "created_at"]
_VALID_CASH_FLOW_KINDS = {"initial", "sip", "withdrawal", "dividend", "tax", "fee"}


def _ensure_tables(conn) -> None:
    conn.execute(_CREATE_PORTFOLIOS)
    conn.execute(_CREATE_PORTFOLIO_CASH_FLOWS)


class PortfolioCreate(BaseModel):
    name: str
    description: Optional[str] = None
    channel: Optional[str] = None
    base_capital: float = 0.0


class PortfolioUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    channel: Optional[str] = None
    is_active: Optional[bool] = None


class PortfolioRow(BaseModel):
    portfolio_id: int
    name: str
    description: Optional[str] = None
    channel: Optional[str] = None
    base_capital: float
    is_active: bool
    created_at: str
    updated_at: str


class CashFlowCreate(BaseModel):
    date: date_type
    amount: float
    kind: str
    note: Optional[str] = None


class CashFlowRow(BaseModel):
    id: int
    portfolio_id: int
    date: date_type
    amount: float
    kind: str
    note: Optional[str] = None
    created_at: str


class NavResponse(BaseModel):
    portfolio_id: int
    as_of: str
    nav: float
    cash_balance: float
    holdings_value: float
    total_contributed: float
    total_withdrawn: float
    xirr: Optional[float] = None


def _portfolio_row_to_dict(row: tuple) -> dict:
    d = dict(zip(_PORTFOLIO_COLUMNS, row))
    d["created_at"] = str(d["created_at"])
    d["updated_at"] = str(d["updated_at"])
    return d


def _cash_flow_row_to_dict(row: tuple) -> dict:
    d = dict(zip(_CASH_FLOW_COLUMNS, row))
    d["created_at"] = str(d["created_at"])
    return d


def _get_portfolio_or_404(conn, portfolio_id: int) -> None:
    existing = conn.execute("SELECT portfolio_id FROM portfolios WHERE portfolio_id = ?", [portfolio_id]).fetchone()
    if existing is None:
        raise HTTPException(status_code=404, detail=f"Portfolio {portfolio_id} not found")


@router.get("/", response_model=List[PortfolioRow])
async def list_portfolios(active_only: bool = False) -> List[PortfolioRow]:
    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
        _ensure_tables(conn)
        where = "WHERE is_active = TRUE" if active_only else ""
        rows = conn.execute(
            f"SELECT {', '.join(_PORTFOLIO_COLUMNS)} FROM portfolios {where} ORDER BY name"
        ).fetchall()
    return [PortfolioRow(**_portfolio_row_to_dict(r)) for r in rows]


@router.post("/", response_model=PortfolioRow)
async def create_portfolio(portfolio: PortfolioCreate) -> PortfolioRow:
    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
        _ensure_tables(conn)
        existing = conn.execute("SELECT portfolio_id FROM portfolios WHERE name = ?", [portfolio.name]).fetchone()
        if existing is not None:
            raise HTTPException(status_code=400, detail=f"A portfolio named {portfolio.name!r} already exists")
        new_id = conn.execute(
            """
            INSERT INTO portfolios (name, description, channel, base_capital)
            VALUES (?, ?, ?, ?)
            RETURNING portfolio_id
            """,
            [portfolio.name, portfolio.description, portfolio.channel, portfolio.base_capital],
        ).fetchone()[0]
        if portfolio.base_capital:
            conn.execute(
                "INSERT INTO portfolio_cash_flows (portfolio_id, date, amount, kind, note) VALUES (?, ?, ?, ?, ?)",
                [new_id, now_ist().date(), portfolio.base_capital, "initial", "Initial capital at portfolio creation"],
            )
        row = conn.execute(
            f"SELECT {', '.join(_PORTFOLIO_COLUMNS)} FROM portfolios WHERE portfolio_id = ?", [new_id]
        ).fetchone()
    return PortfolioRow(**_portfolio_row_to_dict(row))


@router.get("/{portfolio_id}", response_model=PortfolioRow)
async def get_portfolio(portfolio_id: int) -> PortfolioRow:
    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
        _ensure_tables(conn)
        _get_portfolio_or_404(conn, portfolio_id)
        row = conn.execute(
            f"SELECT {', '.join(_PORTFOLIO_COLUMNS)} FROM portfolios WHERE portfolio_id = ?", [portfolio_id]
        ).fetchone()
    return PortfolioRow(**_portfolio_row_to_dict(row))


@router.patch("/{portfolio_id}", response_model=PortfolioRow)
async def update_portfolio(portfolio_id: int, update: PortfolioUpdate) -> PortfolioRow:
    fields = update.model_dump(exclude_unset=True)
    if not fields:
        raise HTTPException(status_code=400, detail="No fields provided to update")

    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
        _ensure_tables(conn)
        _get_portfolio_or_404(conn, portfolio_id)
        set_clause = ", ".join(f"{col} = ?" for col in fields)
        params = list(fields.values()) + [portfolio_id]
        conn.execute(
            f"UPDATE portfolios SET {set_clause}, updated_at = current_timestamp WHERE portfolio_id = ?",
            params,
        )
        row = conn.execute(
            f"SELECT {', '.join(_PORTFOLIO_COLUMNS)} FROM portfolios WHERE portfolio_id = ?", [portfolio_id]
        ).fetchone()
    return PortfolioRow(**_portfolio_row_to_dict(row))


@router.get("/{portfolio_id}/cash_flows", response_model=List[CashFlowRow])
async def list_cash_flows(portfolio_id: int) -> List[CashFlowRow]:
    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
        _ensure_tables(conn)
        _get_portfolio_or_404(conn, portfolio_id)
        rows = conn.execute(
            f"SELECT {', '.join(_CASH_FLOW_COLUMNS)} FROM portfolio_cash_flows "
            "WHERE portfolio_id = ? ORDER BY date DESC, id DESC",
            [portfolio_id],
        ).fetchall()
    return [CashFlowRow(**_cash_flow_row_to_dict(r)) for r in rows]


@router.post("/{portfolio_id}/cash_flows", response_model=CashFlowRow)
async def create_cash_flow(portfolio_id: int, cash_flow: CashFlowCreate) -> CashFlowRow:
    if cash_flow.kind not in _VALID_CASH_FLOW_KINDS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid kind {cash_flow.kind!r} — must be one of {sorted(_VALID_CASH_FLOW_KINDS)}",
        )
    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
        _ensure_tables(conn)
        _get_portfolio_or_404(conn, portfolio_id)
        new_id = conn.execute(
            """
            INSERT INTO portfolio_cash_flows (portfolio_id, date, amount, kind, note)
            VALUES (?, ?, ?, ?, ?)
            RETURNING id
            """,
            [portfolio_id, cash_flow.date, cash_flow.amount, cash_flow.kind, cash_flow.note],
        ).fetchone()[0]
        row = conn.execute(
            f"SELECT {', '.join(_CASH_FLOW_COLUMNS)} FROM portfolio_cash_flows WHERE id = ?", [new_id]
        ).fetchone()
    return CashFlowRow(**_cash_flow_row_to_dict(row))


@router.get("/{portfolio_id}/nav", response_model=NavResponse)
async def get_nav(portfolio_id: int, as_of: Optional[date_type] = None) -> NavResponse:
    as_of_date = str(as_of) if as_of is not None else str(now_ist().date())
    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
        _ensure_tables(conn)
        _get_portfolio_or_404(conn, portfolio_id)
        result = compute_nav(conn, portfolio_id, as_of_date)
    return NavResponse(portfolio_id=portfolio_id, as_of=as_of_date, **result)


@router.get("/{portfolio_id}/trades")
async def get_portfolio_trades(portfolio_id: int) -> List[dict]:
    """Aggregated trade history across every momentum_trades row tagged to
    this portfolio, regardless of which strategy_id placed it."""
    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
        _ensure_tables(conn)
        _get_portfolio_or_404(conn, portfolio_id)
        rows = conn.execute(
            """
            SELECT id, strategy_id, ticker, purchase_date, qty, purchase_price, sale_date, sell_price
            FROM momentum_trades WHERE portfolio_id = ? ORDER BY purchase_date DESC, id DESC
            """,
            [portfolio_id],
        ).fetchall()
    cols = ["id", "strategy_id", "ticker", "purchase_date", "qty", "purchase_price", "sale_date", "sell_price"]
    return [dict(zip(cols, r)) for r in rows]
