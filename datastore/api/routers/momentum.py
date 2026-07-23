"""
datastore/api/routers/momentum.py

ML38 (2026-07-14, extended 2026-07-15) — Momentum: live dashboard section
for the robustness-validated momentum strategy config (top 15 / 6-month
lookback / monthly rebalance / grace=2 rebalance cycles — see
features/momentum_live.py's module docstring for why this config was
chosen over higher-backtest-CAGR variants), run independently across all
5 market-cap rank bands (features.momentum_universe.RANK_BANDS) as 5
separate strategies, selected via `strategy_id` on every endpoint below
(2026-07-15 user request: pick the rank band from a dashboard dropdown,
and tag every recorded trade with which strategy it belongs to). Manual
paper-trading style: the user places trades themselves and logs the fill
here (mirrors datastore/api/routers/holdings.py's my_holdings CRUD almost
verbatim) — no broker integration.

Endpoints
---------
GET    /api/v1/momentum/strategies                   — the 5 selectable
                                                        strategies (id +
                                                        label) for dropdowns
GET    /api/v1/momentum/universe?strategy_id=         — ranked list for
                                                        that strategy
GET    /api/v1/momentum/rebalance/next?strategy_id=   — next rebalance date
GET    /api/v1/momentum/rebalance/suggestions?strategy_id=
POST   /api/v1/momentum/rebalance/suggestions/{id}/dismiss
GET    /api/v1/momentum/trades/?strategy_id=          — list trades
POST   /api/v1/momentum/trades/                       — record a buy
                                                        (strategy_id
                                                        required in body)
PUT    /api/v1/momentum/trades/{id}                   — record a sale / edit
DELETE /api/v1/momentum/trades/{id}
GET    /api/v1/momentum/contributions/?strategy_id=
POST   /api/v1/momentum/contributions/                — strategy_id
                                                        required in body
GET    /api/v1/momentum/summary?strategy_id=          — Holding Dashboard:
                                                        capital invested,
                                                        current value,
                                                        CAGR, XIRR, tax

Table creation is idempotent (lazy, `_ensure_tables(conn)`), matching
holdings.py's convention.
"""

import logging
from datetime import date as date_type
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backtest.momentum_metrics import cagr as compute_cagr
from backtest.momentum_metrics import xirr
from backtest.momentum_tax import compute_total_tax, post_tax_ending_value
from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from datastore.schema.create_normalised import (
    _CREATE_MOMENTUM_CONTRIBUTIONS,
    _CREATE_MOMENTUM_RANKINGS,
    _CREATE_MOMENTUM_REBALANCE_STATE,
    _CREATE_MOMENTUM_REBALANCE_SUGGESTIONS,
    _CREATE_MOMENTUM_TRADES,
)
from features import momentum_live
from features.momentum_signal import trailing_momentum

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/momentum", tags=["Momentum"])

DEFAULT_STRATEGY_ID = momentum_live.DEFAULT_STRATEGY_ID

_TRADE_COLUMNS = [
    "id", "strategy_id", "ticker", "purchase_date", "qty", "purchase_price",
    "sale_date", "sell_price", "entry_rank", "exit_rank", "suggestion_id",
    "grace_remaining", "purchase_rationale", "sell_rationale", "journal_entry",
]


def _ensure_tables(conn) -> None:
    for ddl in (
        _CREATE_MOMENTUM_TRADES, _CREATE_MOMENTUM_CONTRIBUTIONS, _CREATE_MOMENTUM_RANKINGS,
        _CREATE_MOMENTUM_REBALANCE_SUGGESTIONS, _CREATE_MOMENTUM_REBALANCE_STATE,
    ):
        conn.execute(ddl)


def _validate_strategy_id(strategy_id: str) -> str:
    try:
        momentum_live.get_strategy(strategy_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return strategy_id


def _row_to_dict(row: tuple, columns: List[str]) -> dict:
    return dict(zip(columns, row))


# --------------------------------------------------------------- strategies

class StrategyRow(BaseModel):
    strategy_id: str
    band_id: int
    rank_start: int
    rank_end: int
    label: str


@router.get("/strategies", response_model=List[StrategyRow])
async def list_strategies() -> List[StrategyRow]:
    """The 5 selectable rank-band strategies, for populating dashboard
    dropdowns — same top_n/lookback/rebalance/grace config, different
    market-cap rank band each (features.momentum_universe.RANK_BANDS)."""
    return [StrategyRow(**s) for s in momentum_live.STRATEGIES]


# ---------------------------------------------------------------- universe

class RankingRow(BaseModel):
    ticker: str
    company_name: Optional[str] = None
    momentum_return: float
    momentum_rank: int
    in_top_n: bool
    return_20d: Optional[float] = None
    price: Optional[float] = None
    sparkline: List[float] = []


# Trailing trading days shown in the Universe screen's sparkline — a short
# visual "shape of the recent move" cue, independent of (shorter than) the
# 6-month momentum lookback the ranking itself is computed over.
_SPARKLINE_TRADING_DAYS = 30

# A separate, shorter-horizon trailing return shown alongside the 6-month
# momentum ranking (2026-07-15 user request) — a "how has this name done
# very recently" cue, independent of (and not used for) the ranking itself.
_SHORT_TERM_RETURN_TRADING_DAYS = 20


def _enrich_with_price_data(conn, tickers: List[str], as_of_date: str) -> dict:
    """{ticker: {"company_name", "price", "sparkline"}} for the Universe
    screen — company name (stock_master), latest close on/before
    as_of_date (price), and the trailing _SPARKLINE_TRADING_DAYS closes
    (sparkline, chronological order). Missing data (no stock_master row,
    ticker not yet listed, etc.) leaves that ticker's fields as real
    None/[] — never fabricated."""
    if not tickers:
        return {}
    placeholders = ",".join("?" for _ in tickers)

    names = dict(conn.execute(
        f"SELECT ticker, company_name FROM stock_master WHERE ticker IN ({placeholders})",
        tickers,
    ).fetchall())

    price_rows = conn.execute(
        f"""
        SELECT ticker, date, close FROM (
            SELECT ticker, date, close,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
            FROM ohlcv_adjusted WHERE ticker IN ({placeholders}) AND date <= ?
        ) WHERE rn <= ?
        ORDER BY ticker, date
        """,
        tickers + [as_of_date, _SPARKLINE_TRADING_DAYS],
    ).fetchall()

    by_ticker: dict = {}
    for ticker, _date, close in price_rows:
        by_ticker.setdefault(ticker, []).append(close)

    return {
        t: {
            "company_name": names.get(t),
            "price": by_ticker[t][-1] if by_ticker.get(t) else None,
            "sparkline": by_ticker.get(t, []),
        }
        for t in tickers
    }


@router.get("/pillar_summary")
async def get_momentum_pillar_summary(strategy_id: str = DEFAULT_STRATEGY_ID) -> dict:
    """Home page pillar-outcome card, for one of the 5 rank-band
    strategies (defaults to DEFAULT_STRATEGY_ID). Uses the pipeline-
    written momentum_rankings snapshot only (no on-the-spot compute, unlike
    /universe's fallback) — a summary card shouldn't trigger a live ranking
    run. avg_expected_return_pct here is `momentum_return`, the trailing
    lookback return used to RANK stocks into the top-N, not a forward-
    looking forecast — labeled accordingly so it isn't confused with the
    ML pillar's forward q50_return. No win-rate/success-rate table exists
    for momentum (only the user's own manually-logged trades' CAGR does,
    via /summary) — top_strategy names the active strategy_id but its
    success-rate field is left null rather than depending on whether the
    user happens to have logged trades in this band."""
    _validate_strategy_id(strategy_id)
    target_date = date_type.today().isoformat()
    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=True) as conn:
        tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
        if "momentum_rankings" not in tables:
            return {"as_of_date": None, "available": False, "recommendation_count": 0,
                     "avg_expected_return_pct": None, "top_strategy": None, "top_strategy_success_rate_pct": None}
        row = conn.execute(
            "SELECT COUNT(*), AVG(momentum_return) FROM momentum_rankings "
            "WHERE date = ? AND strategy_id = ? AND in_top_n = TRUE",
            [target_date, strategy_id],
        ).fetchone()

    count, avg_return = row if row else (0, None)
    strategy_label = next((s["label"] for s in momentum_live.STRATEGIES if s["strategy_id"] == strategy_id), strategy_id)
    return {
        "as_of_date": target_date if count else None,
        "available": bool(count),
        "recommendation_count": int(count or 0),
        "avg_expected_return_pct": float(avg_return * 100) if avg_return is not None else None,
        "top_strategy": strategy_label,
        "top_strategy_success_rate_pct": None,
    }


@router.get("/universe", response_model=List[RankingRow])
async def get_universe(
    strategy_id: str = DEFAULT_STRATEGY_ID, as_of_date: Optional[date_type] = None
) -> List[RankingRow]:
    """strategy_id's (or as_of_date's) momentum ranking, with company
    name, latest price, and a trailing-30-day sparkline per ticker. Reads
    the pipeline-written momentum_rankings snapshot when available; falls
    back to computing it on the spot (e.g. dashboard hit before that
    day's step_compute_momentum has run yet)."""
    _validate_strategy_id(strategy_id)
    target_date = (as_of_date or date_type.today()).isoformat()
    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
        _ensure_tables(conn)
        rows = conn.execute(
            "SELECT ticker, momentum_return, momentum_rank, in_top_n FROM momentum_rankings "
            "WHERE date = ? AND strategy_id = ? ORDER BY momentum_rank",
            [target_date, strategy_id],
        ).fetchall()

        if not rows:
            ranking = momentum_live.compute_daily_ranking(conn, target_date, strategy_id=strategy_id)
            if ranking.empty:
                return []
            rows = [
                (r.ticker, float(r.momentum_return), int(r.momentum_rank), bool(r.in_top_n))
                for r in ranking.itertuples(index=False)
            ]

        tickers = [r[0] for r in rows]
        extra = _enrich_with_price_data(conn, tickers, target_date)
        returns_20d = trailing_momentum(conn, tickers, target_date, _SHORT_TERM_RETURN_TRADING_DAYS)

    return [
        RankingRow(
            ticker=ticker, momentum_return=momentum_return, momentum_rank=momentum_rank, in_top_n=in_top_n,
            company_name=extra.get(ticker, {}).get("company_name"),
            price=extra.get(ticker, {}).get("price"),
            sparkline=extra.get(ticker, {}).get("sparkline", []),
            return_20d=float(returns_20d[ticker]) if ticker in returns_20d.index else None,
        )
        for ticker, momentum_return, momentum_rank, in_top_n in rows
    ]


# ------------------------------------------------------------- rebalance

@router.get("/rebalance/next")
async def get_next_rebalance(strategy_id: str = DEFAULT_STRATEGY_ID) -> dict:
    _validate_strategy_id(strategy_id)
    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
        _ensure_tables(conn)
        row = conn.execute(
            "SELECT last_rebalance_date, next_rebalance_date FROM momentum_rebalance_state WHERE strategy_id = ?",
            [strategy_id],
        ).fetchone()
    if row is None:
        return {"last_rebalance_date": None, "next_rebalance_date": None}
    return {"last_rebalance_date": row[0], "next_rebalance_date": row[1]}


class SuggestionRow(BaseModel):
    id: int
    rebalance_date: date_type
    ticker: str
    action: str
    momentum_rank: Optional[int] = None
    grace_remaining: Optional[int] = None
    status: str


@router.get("/rebalance/suggestions", response_model=List[SuggestionRow])
async def get_rebalance_suggestions(
    strategy_id: str = DEFAULT_STRATEGY_ID, rebalance_date: Optional[date_type] = None
) -> List[SuggestionRow]:
    """Suggestions for strategy_id on rebalance_date, or (if omitted) the
    most recent rebalance_date that has any rows at all."""
    _validate_strategy_id(strategy_id)
    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
        _ensure_tables(conn)
        if rebalance_date is None:
            latest = conn.execute(
                "SELECT MAX(rebalance_date) FROM momentum_rebalance_suggestions WHERE strategy_id = ?",
                [strategy_id],
            ).fetchone()
            if latest is None or latest[0] is None:
                return []
            rebalance_date = latest[0]
        rows = conn.execute(
            "SELECT id, rebalance_date, ticker, action, momentum_rank, grace_remaining, status "
            "FROM momentum_rebalance_suggestions WHERE strategy_id = ? AND rebalance_date = ? "
            "ORDER BY action, ticker",
            [strategy_id, rebalance_date.isoformat() if isinstance(rebalance_date, date_type) else rebalance_date],
        ).fetchall()
    columns = ["id", "rebalance_date", "ticker", "action", "momentum_rank", "grace_remaining", "status"]
    return [SuggestionRow(**_row_to_dict(r, columns)) for r in rows]


@router.post("/rebalance/suggestions/{suggestion_id}/dismiss")
async def dismiss_suggestion(suggestion_id: int) -> dict:
    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
        _ensure_tables(conn)
        existing = conn.execute(
            "SELECT id FROM momentum_rebalance_suggestions WHERE id = ?", [suggestion_id]
        ).fetchone()
        if existing is None:
            raise HTTPException(status_code=404, detail=f"Suggestion {suggestion_id} not found")
        conn.execute(
            "UPDATE momentum_rebalance_suggestions SET status = 'dismissed' WHERE id = ?", [suggestion_id]
        )
    return {"dismissed": True, "id": suggestion_id}


# ------------------------------------------------------------------ trades

class TradeCreate(BaseModel):
    strategy_id: str
    ticker: str
    purchase_date: date_type
    qty: float
    purchase_price: Optional[float] = None
    entry_rank: Optional[int] = None
    suggestion_id: Optional[int] = None
    purchase_rationale: Optional[str] = None
    journal_entry: Optional[str] = None


class TradeUpdate(BaseModel):
    """All fields optional — PUT only overwrites what's provided (e.g.
    record a sale via sale_date/sell_price/exit_rank/sell_rationale)."""
    ticker: Optional[str] = None
    purchase_date: Optional[date_type] = None
    qty: Optional[float] = None
    purchase_price: Optional[float] = None
    sale_date: Optional[date_type] = None
    sell_price: Optional[float] = None
    exit_rank: Optional[int] = None
    purchase_rationale: Optional[str] = None
    sell_rationale: Optional[str] = None
    journal_entry: Optional[str] = None


class TradeRow(BaseModel):
    id: int
    strategy_id: str
    ticker: str
    purchase_date: date_type
    qty: float
    purchase_price: Optional[float] = None
    sale_date: Optional[date_type] = None
    sell_price: Optional[float] = None
    entry_rank: Optional[int] = None
    exit_rank: Optional[int] = None
    suggestion_id: Optional[int] = None
    grace_remaining: Optional[int] = None
    purchase_rationale: Optional[str] = None
    sell_rationale: Optional[str] = None
    journal_entry: Optional[str] = None


@router.get("/trades/", response_model=List[TradeRow])
async def list_trades(strategy_id: Optional[str] = None, open_only: bool = False) -> List[TradeRow]:
    """strategy_id omitted lists trades across every strategy (e.g. an
    "All Strategies" dropdown option on the Holding Dashboard)."""
    if strategy_id is not None:
        _validate_strategy_id(strategy_id)
    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
        _ensure_tables(conn)
        conditions = []
        params: List = []
        if strategy_id is not None:
            conditions.append("strategy_id = ?")
            params.append(strategy_id)
        if open_only:
            conditions.append("sale_date IS NULL")
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        rows = conn.execute(
            f"SELECT {', '.join(_TRADE_COLUMNS)} FROM momentum_trades {where} "
            "ORDER BY purchase_date DESC, id DESC",
            params,
        ).fetchall()
    return [TradeRow(**_row_to_dict(r, _TRADE_COLUMNS)) for r in rows]


@router.post("/trades/", response_model=TradeRow)
async def create_trade(trade: TradeCreate) -> TradeRow:
    _validate_strategy_id(trade.strategy_id)
    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
        _ensure_tables(conn)
        new_id = conn.execute(
            """
            INSERT INTO momentum_trades
                (strategy_id, ticker, purchase_date, qty, purchase_price, entry_rank, suggestion_id,
                 purchase_rationale, journal_entry)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING id
            """,
            [
                trade.strategy_id, trade.ticker.upper(), trade.purchase_date, trade.qty, trade.purchase_price,
                trade.entry_rank, trade.suggestion_id, trade.purchase_rationale, trade.journal_entry,
            ],
        ).fetchone()[0]
        if trade.suggestion_id is not None:
            conn.execute(
                "UPDATE momentum_rebalance_suggestions SET status = 'acted' WHERE id = ?",
                [trade.suggestion_id],
            )
        row = conn.execute(
            f"SELECT {', '.join(_TRADE_COLUMNS)} FROM momentum_trades WHERE id = ?", [new_id]
        ).fetchone()
    return TradeRow(**_row_to_dict(row, _TRADE_COLUMNS))


@router.put("/trades/{trade_id}", response_model=TradeRow)
async def update_trade(trade_id: int, update: TradeUpdate) -> TradeRow:
    fields = update.model_dump(exclude_unset=True)
    if not fields:
        raise HTTPException(status_code=400, detail="No fields provided to update")

    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
        _ensure_tables(conn)
        existing = conn.execute("SELECT id FROM momentum_trades WHERE id = ?", [trade_id]).fetchone()
        if existing is None:
            raise HTTPException(status_code=404, detail=f"Trade {trade_id} not found")

        set_clause = ", ".join(f"{col} = ?" for col in fields)
        params = list(fields.values()) + [trade_id]
        conn.execute(
            f"UPDATE momentum_trades SET {set_clause}, updated_at = now() WHERE id = ?", params,
        )
        row = conn.execute(
            f"SELECT {', '.join(_TRADE_COLUMNS)} FROM momentum_trades WHERE id = ?", [trade_id]
        ).fetchone()
    return TradeRow(**_row_to_dict(row, _TRADE_COLUMNS))


@router.delete("/trades/{trade_id}")
async def delete_trade(trade_id: int) -> dict:
    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
        _ensure_tables(conn)
        existing = conn.execute("SELECT id FROM momentum_trades WHERE id = ?", [trade_id]).fetchone()
        if existing is None:
            raise HTTPException(status_code=404, detail=f"Trade {trade_id} not found")
        conn.execute("DELETE FROM momentum_trades WHERE id = ?", [trade_id])
    return {"deleted": True, "id": trade_id}


# ------------------------------------------------------------ contributions

class ContributionCreate(BaseModel):
    strategy_id: str
    contribution_date: date_type
    amount: float
    note: Optional[str] = None


class ContributionRow(BaseModel):
    id: int
    strategy_id: str
    contribution_date: date_type
    amount: float
    note: Optional[str] = None


@router.get("/contributions/", response_model=List[ContributionRow])
async def list_contributions(strategy_id: str = DEFAULT_STRATEGY_ID) -> List[ContributionRow]:
    _validate_strategy_id(strategy_id)
    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
        _ensure_tables(conn)
        rows = conn.execute(
            "SELECT id, strategy_id, contribution_date, amount, note FROM momentum_contributions "
            "WHERE strategy_id = ? ORDER BY contribution_date",
            [strategy_id],
        ).fetchall()
    return [ContributionRow(id=r[0], strategy_id=r[1], contribution_date=r[2], amount=r[3], note=r[4]) for r in rows]


@router.post("/contributions/", response_model=ContributionRow)
async def create_contribution(contribution: ContributionCreate) -> ContributionRow:
    _validate_strategy_id(contribution.strategy_id)
    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
        _ensure_tables(conn)
        new_id = conn.execute(
            "INSERT INTO momentum_contributions (strategy_id, contribution_date, amount, note) "
            "VALUES (?, ?, ?, ?) RETURNING id",
            [contribution.strategy_id, contribution.contribution_date, contribution.amount, contribution.note],
        ).fetchone()[0]
        row = conn.execute(
            "SELECT id, strategy_id, contribution_date, amount, note FROM momentum_contributions WHERE id = ?",
            [new_id],
        ).fetchone()
    return ContributionRow(id=row[0], strategy_id=row[1], contribution_date=row[2], amount=row[3], note=row[4])


# ------------------------------------------------------------------ summary

@router.get("/summary")
async def get_summary(strategy_id: str = DEFAULT_STRATEGY_ID) -> dict:
    """Holding Dashboard numbers for strategy_id: capital invested,
    current holdings value, CAGR, XIRR (money-weighted, since
    contributions arrive at arbitrary dates), tax due, and post-tax
    value — computed from the real recorded momentum_trades +
    momentum_contributions ledger, fed through the same
    backtest.momentum_metrics.xirr/cagr / backtest.momentum_tax helpers
    the validated backtest itself uses (no separate live math)."""
    _validate_strategy_id(strategy_id)
    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=False) as conn:
        _ensure_tables(conn)
        contributions = conn.execute(
            "SELECT contribution_date, amount FROM momentum_contributions WHERE strategy_id = ?",
            [strategy_id],
        ).fetchall()
        trades = conn.execute(
            "SELECT ticker, purchase_date, qty, purchase_price, sale_date, sell_price "
            "FROM momentum_trades WHERE strategy_id = ?",
            [strategy_id],
        ).fetchall()

        open_tickers = sorted({t[0] for t in trades if t[4] is None})
        latest_close = {}
        if open_tickers:
            placeholders = ",".join("?" for _ in open_tickers)
            price_rows = conn.execute(
                f"""
                SELECT ticker, close FROM (
                    SELECT ticker, close, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
                    FROM ohlcv_adjusted WHERE ticker IN ({placeholders})
                ) WHERE rn = 1
                """,
                open_tickers,
            ).fetchall()
            latest_close = dict(price_rows)

    cash_flows = [(str(d), -amt) for d, amt in contributions]
    transactions = []
    today = date_type.today()
    current_holdings_value = 0.0
    total_deployed = 0.0   # every buy, open or closed — money that left idle cash
    total_recovered = 0.0  # every sell — money that returned to idle cash
    capital_invested = 0.0  # cost basis of currently-open positions only

    for ticker, purchase_date, qty, purchase_price, sale_date, sell_price in trades:
        if purchase_price is not None:
            cash_flows.append((str(purchase_date), -qty * purchase_price))
            total_deployed += qty * purchase_price
        if sale_date is not None and sell_price is not None:
            cash_flows.append((str(sale_date), qty * sell_price))
            total_recovered += qty * sell_price
            holding_days = (sale_date - purchase_date).days
            transactions.append({
                "buy_price": purchase_price, "sell_price": sell_price, "qty": qty, "holding_days": holding_days,
            })
        else:
            if purchase_price is not None:
                capital_invested += qty * purchase_price
            mark_price = latest_close.get(ticker)
            if mark_price is not None:
                current_holdings_value += qty * mark_price
                holding_days = (today - purchase_date).days
                transactions.append({
                    "buy_price": purchase_price, "sell_price": mark_price, "qty": qty, "holding_days": holding_days,
                })

    total_contributed = sum(amt for _, amt in contributions)
    # Contributed money never deployed into a buy (or already recovered from a
    # sell) is still real net worth sitting as cash — it must be part of the
    # terminal value fed to xirr()/post_tax_ending_value(), otherwise an
    # account that's mostly still in cash looks like it lost almost
    # everything (and can even make xirr() unbracketable).
    idle_cash = total_contributed - total_deployed + total_recovered
    total_net_worth = current_holdings_value + idle_cash

    if total_net_worth != 0:
        cash_flows.append((today.isoformat(), total_net_worth))

    money_weighted_return = xirr(cash_flows) if len(cash_flows) >= 2 else None
    total_tax = compute_total_tax(transactions)
    post_tax_value = post_tax_ending_value(total_net_worth, transactions)

    # CAGR alongside XIRR: unlike XIRR (which correctly handles contributions
    # landing on many different dates), CAGR here is a single-lump-sum-style
    # approximation — start value = total ever contributed, end value =
    # today's total net worth, over the span from the first cash flow to
    # today. Reported as a simpler, more familiar companion number, not a
    # replacement for XIRR's more precise money-weighted figure.
    first_flow_date = min((d for d, _ in contributions), default=None)
    cagr_value = None
    if first_flow_date is not None and total_contributed > 0:
        try:
            cagr_value = compute_cagr(total_contributed, total_net_worth, str(first_flow_date), today.isoformat())
        except ValueError:
            cagr_value = None  # same-day (years<=0) or non-positive input — not enough elapsed time to annualize

    return {
        "strategy_id": strategy_id,
        "as_of_date": today.isoformat(),
        "capital_invested": capital_invested,
        "current_holdings_value": current_holdings_value,
        "idle_cash": idle_cash,
        "total_net_worth": total_net_worth,
        "cagr": cagr_value,
        "xirr": money_weighted_return,
        "total_tax_due": total_tax,
        "post_tax_value": post_tax_value,
        "total_contributed": total_contributed,
    }
