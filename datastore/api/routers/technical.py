"""
datastore/api/routers/technical.py

Phase: 3.x (Technical Analysis API Scaffolding + Screener + Alerts)
Specs: SPEC-TA-004, SPEC-TA-005, SPEC-TA-006
Owner: Platform / DataStore
Consumers: dashboard/static/technical/{chart,compare,overview,screener,alerts}.html

features/technical.py (70 core indicators), features/advanced_technical.py
(18 advanced), and features/pattern_scores.py (6 chart-pattern probability
scores) write 94 real, daily-computed columns into the same Parquet store.

This router adds:
  - /screener/templates — list all 42 pre-built templates (SPEC-TA-005)
  - /screener/run/{template_name} — run a named template (SPEC-TA-005)
  - POST /screener/custom — run user-defined conditions (SPEC-TA-005)
  - /alerts/today — today's ta_signals rows from the signals DuckDB (SPEC-TA-006)
  - /alerts/{ticker} — all templates that matched a ticker (SPEC-TA-006)

Screener/alert routes are placed BEFORE the /{ticker}/... parametric routes
to prevent FastAPI from interpreting "screener" or "alerts" as a ticker name.
"""

import json
import logging
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import APIRouter, Body, HTTPException, Query

from config.settings import DUCKDB_PATH, SIGNALS_DUCKDB_PATH
from config.universe import load_universe_raw
from datastore.api.db import get_duckdb_connection
from datastore.api.schemas import (
    TAAlertResponse,
    TAAlertRow,
    TACheckTriggersRequest,
    TACompareResponse,
    TAConsensusResponse,
    TAConsensusRow,
    TACompareTickerRow,
    TAIndicatorsResponse,
    TAMarketOverviewResponse,
    TAPatternsResponse,
    TASectorBreadthRow,
    TAScreenerRequest,
    TAScreenerResponse,
    TAScreenerRow,
    TASignalWriteRequest,
    TATemplateInfo,
    TATemplateListResponse,
    TAUserAlertCreate,
    TAUserAlertResponse,
    TAUserAlertRow,
    TAWatchlistResponse,
    TAWatchlistRow,
)
from datastore.api.utils.feature_store import read_feature_row, resolve_date
from features.advanced_technical import ADVANCED_TECHNICAL_FEATURES
from features.pattern_scores import PATTERN_FEATURES
from features.technical import CORE_TECHNICAL_FEATURES
from systems.technical_analysis.alerts import alert_store
from systems.technical_analysis.screener.engine import ScreenerEngine
from systems.technical_analysis.screener.templates import TEMPLATE_MAP

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/ta", tags=["Technical Analysis"])

# Module-level engine instance — stateless, safe to reuse across requests
_screener = ScreenerEngine()


# ---------------------------------------------------------------------------
# Screener endpoints (SPEC-TA-005) — MUST be registered before /{ticker}/...
# routes so FastAPI does not match "screener" or "alerts" as ticker values.
# ---------------------------------------------------------------------------


@router.get("/screener/templates", response_model=TATemplateListResponse)
async def list_screener_templates() -> TATemplateListResponse:
    """List all 42 pre-built screener templates with condition counts.

    Spec References
    ---------------
    SPEC-TA-005: GET /api/v1/ta/screener/templates
    """
    infos = _screener.list_templates()
    return TATemplateListResponse(
        templates=[
            TATemplateInfo(
                name=i.name,
                category=i.category,
                description=i.description,
                condition_count=i.condition_count,
            )
            for i in infos
        ],
        count=len(infos),
    )


@router.get("/screener/run/{template_name}", response_model=TAScreenerResponse)
async def run_screener_template(
    template_name: str,
    date: Optional[str] = Query(None, description="YYYY-MM-DD; defaults to latest"),
    limit: int = Query(50, ge=1, le=500, description="Maximum results"),
) -> TAScreenerResponse:
    """Run a named screener template against the daily feature store.

    Parameters
    ----------
    template_name : str
        Template identifier, e.g. "A1", "E2", "S004".
    date : str, optional
        Feature date (YYYY-MM-DD). Defaults to the latest available day.
    limit : int, optional
        Maximum results to return (default 50, max 500).

    Returns
    -------
    TAScreenerResponse
        Rows sorted by score desc (full matches first), then by volume.

    Spec References
    ---------------
    SPEC-TA-005: GET /api/v1/ta/screener/run/{template_name}
    """
    try:
        # ML24 (2026-07-11): over-fetch by a buffer so the ADTV recommendation
        # floor doesn't shrink the effective result count below what the
        # caller asked for.
        results = _screener.screen(template_name, date=date, limit=limit * 5)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    if results:
        from config.training_universe import filter_recommendable

        results_df = pd.DataFrame({"ticker": [r.ticker for r in results]})
        recommendable_tickers = set(filter_recommendable(results_df)["ticker"])
        results = [r for r in results if r.ticker in recommendable_tickers][:limit]

    rows = [
        TAScreenerRow(
            ticker=r.ticker,
            date=r.date,
            template_name=r.template_name,
            matched_conditions=r.matched_conditions,
            total_conditions=r.total_conditions,
            score=r.score,
            key_values={k: (None if v != v else v) for k, v in r.key_values.items()},
        )
        for r in results
    ]
    return TAScreenerResponse(
        template_name=template_name,
        date=results[0].date if results else resolve_date(date),
        rows=rows,
        count=len(rows),
    )


@router.post("/screener/custom", response_model=TAScreenerResponse)
async def run_custom_screener(
    body: TAScreenerRequest = Body(...),
) -> TAScreenerResponse:
    """Run user-defined screener conditions against the daily feature store.

    Parameters
    ----------
    body : TAScreenerRequest
        JSON body with conditions list, optional date, and optional limit.
        Condition format: {"feature": "rsi_14", "op": "lt", "value": 30}

    Returns
    -------
    TAScreenerResponse
        Rows sorted by score desc.

    Spec References
    ---------------
    SPEC-TA-005: POST /api/v1/ta/screener/custom
    """
    raw_conditions = [
        {k: v for k, v in c.model_dump().items() if v is not None}
        for c in body.conditions
    ]
    results = _screener.screen_custom(raw_conditions, date=body.date, limit=body.limit)

    rows = [
        TAScreenerRow(
            ticker=r.ticker,
            date=r.date,
            template_name=r.template_name,
            matched_conditions=r.matched_conditions,
            total_conditions=r.total_conditions,
            score=r.score,
            key_values={k: (None if v != v else v) for k, v in r.key_values.items()},
        )
        for r in results
    ]
    return TAScreenerResponse(
        template_name="custom",
        date=results[0].date if results else resolve_date(body.date),
        rows=rows,
        count=len(rows),
    )


# ---------------------------------------------------------------------------
# Alerts endpoints (SPEC-TA-006)
# ---------------------------------------------------------------------------


def _parse_key_values(raw: Optional[str]) -> Dict[str, Optional[float]]:
    """Safely parse a JSON key_values string from ta_signals into a dict.

    Parameters
    ----------
    raw : str or None
        JSON string stored in the ta_signals.key_values column, or None.

    Returns
    -------
    dict
        Parsed key-value mapping, or empty dict on any parse error.

    Spec References
    ---------------
    SPEC-TA-008: key_values is stored as JSON in ta_signals
    """
    if not raw:
        return {}
    try:
        return {k: (None if v is None else float(v)) for k, v in json.loads(raw).items()}
    except (json.JSONDecodeError, TypeError, ValueError):
        return {}


@router.get("/alerts/today", response_model=TAAlertResponse)
async def get_alerts_today(
    date: Optional[str] = Query(None, description="YYYY-MM-DD; defaults to latest"),
    category: Optional[str] = Query(None, description="Filter by category letter, e.g. A"),
    limit: int = Query(200, ge=1, le=2000, description="Maximum rows"),
) -> TAAlertResponse:
    """Return today's ta_signals rows (all templates that fired today).

    Parameters
    ----------
    date : str, optional
        Feature date. Defaults to the most recent date in ta_signals.
    category : str, optional
        Filter to a specific category (A-F, S).
    limit : int, optional
        Maximum rows (default 200).

    Returns
    -------
    TAAlertResponse
        Rows from the ta_signals table, sorted by category then ticker.

    Spec References
    ---------------
    SPEC-TA-006: GET /api/v1/ta/alerts/today
    """
    SIGNALS_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
            # Check if table exists
            tables = [r[0] for r in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'ta_signals'"
            ).fetchall()]
            if not tables:
                return TAAlertResponse(count=0)

            if date:
                target_date = date
            else:
                row = conn.execute(
                    "SELECT MAX(date) FROM ta_signals"
                ).fetchone()
                if row is None or row[0] is None:
                    return TAAlertResponse(count=0)
                target_date = str(row[0])

            # ML24 (2026-07-11): over-fetch by a buffer, ADTV-gate below, then trim.
            buffer_limit = limit * 5
            if category:
                df = conn.execute(
                    """
                    SELECT date, ticker, template_name, category, score,
                           matched_conditions, total_conditions, key_values
                    FROM ta_signals
                    WHERE date = ? AND category = ?
                    ORDER BY category, ticker
                    LIMIT ?
                    """,
                    [target_date, category, buffer_limit],
                ).fetchdf()
            else:
                df = conn.execute(
                    """
                    SELECT date, ticker, template_name, category, score,
                           matched_conditions, total_conditions, key_values
                    FROM ta_signals
                    WHERE date = ?
                    ORDER BY category, ticker
                    LIMIT ?
                    """,
                    [target_date, buffer_limit],
                ).fetchdf()
    except Exception as exc:
        logger.warning("alerts/today DB query failed: %s", exc)
        return TAAlertResponse(count=0)

    if df.empty:
        return TAAlertResponse(as_of_date=target_date, count=0)

    from config.training_universe import filter_recommendable

    df = filter_recommendable(df).head(limit)
    if df.empty:
        return TAAlertResponse(as_of_date=target_date, count=0)

    rows = [
        TAAlertRow(
            date=str(row["date"]),
            ticker=str(row["ticker"]),
            template_name=str(row["template_name"]),
            category=str(row["category"]),
            score=float(row["score"]),
            matched_conditions=int(row["matched_conditions"]),
            total_conditions=int(row["total_conditions"]),
            key_values=_parse_key_values(row.get("key_values")),
        )
        for _, row in df.iterrows()
    ]
    return TAAlertResponse(as_of_date=target_date, rows=rows, count=len(rows))


@router.get("/alerts/{ticker}", response_model=TAAlertResponse)
async def get_alerts_for_ticker(
    ticker: str,
    date: Optional[str] = Query(None, description="YYYY-MM-DD; defaults to latest"),
) -> TAAlertResponse:
    """Return all templates that matched a specific ticker on the given date.

    Parameters
    ----------
    ticker : str
        NSE ticker symbol (case-insensitive; normalised to upper).
    date : str, optional
        Feature date. Defaults to the most recent date in ta_signals.

    Returns
    -------
    TAAlertResponse
        All ta_signals rows for the ticker on the given date.

    Spec References
    ---------------
    SPEC-TA-006: GET /api/v1/ta/alerts/{ticker}
    """
    ticker_upper = ticker.upper()
    SIGNALS_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
            tables = [r[0] for r in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'ta_signals'"
            ).fetchall()]
            if not tables:
                return TAAlertResponse(count=0)

            if date:
                target_date = date
            else:
                row = conn.execute(
                    "SELECT MAX(date) FROM ta_signals WHERE ticker = ?",
                    [ticker_upper],
                ).fetchone()
                if row is None or row[0] is None:
                    return TAAlertResponse(count=0)
                target_date = str(row[0])

            df = conn.execute(
                """
                SELECT date, ticker, template_name, category, score,
                       matched_conditions, total_conditions, key_values
                FROM ta_signals
                WHERE ticker = ? AND date = ?
                ORDER BY category, template_name
                """,
                [ticker_upper, target_date],
            ).fetchdf()
    except Exception as exc:
        logger.warning("alerts/%s DB query failed: %s", ticker, exc)
        return TAAlertResponse(count=0)

    if df.empty:
        return TAAlertResponse(as_of_date=target_date, count=0)

    rows = [
        TAAlertRow(
            date=str(row["date"]),
            ticker=str(row["ticker"]),
            template_name=str(row["template_name"]),
            category=str(row["category"]),
            score=float(row["score"]),
            matched_conditions=int(row["matched_conditions"]),
            total_conditions=int(row["total_conditions"]),
            key_values=_parse_key_values(row.get("key_values")),
        )
        for _, row in df.iterrows()
    ]
    return TAAlertResponse(as_of_date=target_date, rows=rows, count=len(rows))


@router.get("/watchlist/daily", response_model=TAWatchlistResponse)
async def get_ta_daily_watchlist(
    date: Optional[str] = Query(None, description="YYYY-MM-DD; defaults to latest ta_signals date"),
    limit: int = Query(20, ge=1, le=100, description="Maximum tickers"),
) -> TAWatchlistResponse:
    """Daily TA WatchList: best-scoring template match per ticker (ta_signals,
    SPEC-TA-006), with a plain-English rationale and the next resistance
    levels above the current price (rolling 20d/50d/252d swing highs plus
    classic floor-pivot R1/R2, computed from real OHLCV — SPEC-TA-004)."""
    SIGNALS_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
            tables = [r[0] for r in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'ta_signals'"
            ).fetchall()]
            if not tables:
                return TAWatchlistResponse(count=0)

            if date:
                target_date = date
            else:
                row = conn.execute("SELECT MAX(date) FROM ta_signals").fetchone()
                if row is None or row[0] is None:
                    return TAWatchlistResponse(count=0)
                target_date = str(row[0])

            df = conn.execute(
                """
                SELECT date, ticker, template_name, category, score,
                       matched_conditions, total_conditions, key_values
                FROM ta_signals
                WHERE date = ?
                QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY score DESC) = 1
                ORDER BY score DESC
                LIMIT ?
                """,
                # ML24 (2026-07-11): over-fetch, ADTV-gate below, then trim.
                [target_date, limit * 5],
            ).fetchdf()
    except Exception as exc:
        logger.warning("watchlist/daily TA query failed: %s", exc)
        return TAWatchlistResponse(count=0)

    if df.empty:
        return TAWatchlistResponse(date=target_date, count=0)

    universe = load_universe_raw()
    from config.training_universe import filter_recommendable

    df = filter_recommendable(df, universe=universe).head(limit)
    if df.empty:
        return TAWatchlistResponse(date=target_date, count=0)

    name_map = dict(zip(universe["ticker"], universe["company_name"].fillna("")))
    sector_map = dict(zip(universe["ticker"], universe["sector"].fillna("")))

    tickers = df["ticker"].tolist()
    placeholders = ",".join("?" * len(tickers))
    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as pconn:
        hist = pconn.execute(
            f"""
            SELECT ticker, date, high, low, close
            FROM ohlcv_adjusted
            WHERE ticker IN ({placeholders}) AND date <= ?
            ORDER BY ticker, date
            """,
            tickers + [target_date],
        ).fetchdf()

    rows: List[TAWatchlistRow] = []
    for _, r in df.iterrows():
        ticker = str(r["ticker"])
        tmpl = TEMPLATE_MAP.get(str(r["template_name"]))
        matched, total = int(r["matched_conditions"]), int(r["total_conditions"])
        rationale = (
            f"{tmpl.description} — {matched}/{total} conditions matched"
            if tmpl is not None else f"{matched}/{total} conditions matched"
        )

        g = hist[hist["ticker"] == ticker]
        current_price = float(g["close"].iloc[-1]) if not g.empty else None
        resistance_levels: List[float] = []
        support_levels: List[float] = []
        if not g.empty and current_price is not None:
            for window in (20, 50, 252):
                sub = g.tail(window)
                hi, lo = float(sub["high"].max()), float(sub["low"].min())
                if hi > current_price:
                    resistance_levels.append(round(hi, 2))
                if lo < current_price:
                    support_levels.append(round(lo, 2))
            last = g.iloc[-1]
            pivot = (float(last["high"]) + float(last["low"]) + float(last["close"])) / 3
            r1 = 2 * pivot - float(last["low"])
            r2 = pivot + (float(last["high"]) - float(last["low"]))
            for lvl in (r1, r2):
                if lvl > current_price:
                    resistance_levels.append(round(lvl, 2))
            resistance_levels = sorted(set(resistance_levels))[:3]
            support_levels = sorted(set(support_levels), reverse=True)[:3]

        rows.append(TAWatchlistRow(
            ticker=ticker,
            company_name=name_map.get(ticker) or None,
            sector=sector_map.get(ticker) or None,
            current_price=round(current_price, 2) if current_price is not None else None,
            template_name=str(r["template_name"]),
            category=str(r["category"]),
            score=float(r["score"]),
            rationale=rationale,
            matched_conditions=matched,
            total_conditions=total,
            resistance_levels=resistance_levels,
            support_levels=support_levels,
        ))

    return TAWatchlistResponse(date=target_date, rows=rows, count=len(rows))


@router.get("/consensus/daily", response_model=TAConsensusResponse)
async def get_ta_consensus(
    date: Optional[str] = Query(None, description="YYYY-MM-DD; defaults to latest ta_signals date"),
    limit: int = Query(20, ge=1, le=100, description="Maximum tickers"),
) -> TAConsensusResponse:
    """T11: Multi-strategy consensus — when the same ticker is recommended
    by multiple templates on the same date (ta_signals, SPEC-TA-006/T10),
    list every matching strategy and surface the ticker with the most
    concurrent strategy-recommendations first (ties broken by avg score)."""
    SIGNALS_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
            tables = [r[0] for r in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'ta_signals'"
            ).fetchall()]
            if not tables:
                return TAConsensusResponse(count=0)

            if date:
                target_date = date
            else:
                row = conn.execute("SELECT MAX(date) FROM ta_signals").fetchone()
                if row is None or row[0] is None:
                    return TAConsensusResponse(count=0)
                target_date = str(row[0])

            df = conn.execute(
                """
                SELECT ticker,
                       COUNT(DISTINCT template_name) AS strategy_count,
                       LIST(DISTINCT template_name) AS template_names,
                       LIST(DISTINCT category) AS categories,
                       AVG(score) AS avg_score
                FROM ta_signals
                WHERE date = ?
                GROUP BY ticker
                ORDER BY strategy_count DESC, avg_score DESC
                LIMIT ?
                """,
                [target_date, limit],
            ).fetchdf()
    except Exception as exc:
        logger.warning("consensus/daily TA query failed: %s", exc)
        return TAConsensusResponse(count=0)

    if df.empty:
        return TAConsensusResponse(date=target_date, count=0)

    universe = load_universe_raw()
    name_map = dict(zip(universe["ticker"], universe["company_name"].fillna("")))
    sector_map = dict(zip(universe["ticker"], universe["sector"].fillna("")))

    rows: List[TAConsensusRow] = []
    for _, r in df.iterrows():
        ticker = str(r["ticker"])
        rows.append(TAConsensusRow(
            ticker=ticker,
            company_name=name_map.get(ticker) or None,
            sector=sector_map.get(ticker) or None,
            strategy_count=int(r["strategy_count"]),
            template_names=sorted(str(t) for t in r["template_names"]),
            categories=sorted(set(str(c) for c in r["categories"])),
            avg_score=round(float(r["avg_score"]), 4),
        ))

    return TAConsensusResponse(date=target_date, rows=rows, count=len(rows))


# ---------------------------------------------------------------------------
# User-defined alerts (SPEC-TA-009) — placed before /{ticker}/... routes,
# same reasoning as screener/alerts above.
# ---------------------------------------------------------------------------


@router.get("/user-alerts", response_model=TAUserAlertResponse)
async def list_user_alerts(active_only: bool = Query(True, description="Only return active (non-deleted) alerts")) -> TAUserAlertResponse:
    """List user-created alerts, enriched with trigger state.

    Parameters
    ----------
    active_only : bool, optional
        If True (default), only active (non-deleted) alerts.

    Returns
    -------
    TAUserAlertResponse

    Spec References
    ----------------
    SPEC-TA-009: GET /api/v1/ta/user-alerts
    """
    defs = alert_store.list_alerts(active_only=active_only)
    rows = [
        TAUserAlertRow(
            alert_id=d.alert_id,
            ticker=d.ticker,
            template_name=d.template_name,
            category=d.category,
            active=d.active,
            last_triggered_date=d.last_triggered_date,
            triggered_today=d.triggered_today,
        )
        for d in defs
    ]
    return TAUserAlertResponse(rows=rows, count=len(rows))


@router.post("/user-alerts", response_model=TAUserAlertRow)
async def create_user_alert(body: TAUserAlertCreate) -> TAUserAlertRow:
    """Create a new user-defined alert watching (ticker, template_name).

    Parameters
    ----------
    body : TAUserAlertCreate

    Returns
    -------
    TAUserAlertRow
        The newly created alert (never triggered yet).

    Spec References
    ----------------
    SPEC-TA-009: POST /api/v1/ta/user-alerts

    Raises
    ------
    HTTPException
        400 if template_name is unknown.
    """
    if body.template_name not in TEMPLATE_MAP:
        raise HTTPException(status_code=400, detail=f"Unknown template_name '{body.template_name}'")
    alert_id = alert_store.create_alert(body.ticker, body.template_name)
    return TAUserAlertRow(
        alert_id=alert_id,
        ticker=body.ticker.upper(),
        template_name=body.template_name,
        category=TEMPLATE_MAP[body.template_name].category,
        active=True,
        last_triggered_date=None,
        triggered_today=False,
    )


@router.delete("/user-alerts/{alert_id}")
async def delete_user_alert(alert_id: int) -> Dict[str, bool]:
    """Deactivate a user-defined alert.

    Parameters
    ----------
    alert_id : int

    Returns
    -------
    dict
        {"deleted": True}

    Spec References
    ----------------
    SPEC-TA-009: DELETE /api/v1/ta/user-alerts/{alert_id}

    Raises
    ------
    HTTPException
        404 if alert_id doesn't exist or is already inactive.
    """
    ok = alert_store.delete_alert(alert_id)
    if not ok:
        raise HTTPException(status_code=404, detail=f"alert_id {alert_id} not found or already inactive")
    return {"deleted": True}


@router.post("/signals/write")
async def write_ta_signals(body: TASignalWriteRequest) -> Dict[str, int]:
    """Batch upsert ta_signals rows — the API-process write path for
    DailyAlertChecker.evaluate() results.

    Exists so ingestion/scheduler/daily_pipeline.py's check_ta_alerts step
    (running in the scheduler process, a different OS process than this
    API) never opens SIGNALS_DUCKDB_PATH directly. That file already has
    a long-lived connection cached by this API process (several other
    routers default to persist=True); a second process opening its own
    connection loses the race for DuckDB's single-writer-per-file lock —
    observed as a live "check_ta_alerts" Ops Monitor failure. Routing the
    write through this endpoint means only the API process ever touches
    the file directly, matching SPEC-DS-002 ("no other process touches
    signals.duckdb").

    Parameters
    ----------
    body : TASignalWriteRequest

    Returns
    -------
    dict
        {"written": <row count>}

    Spec References
    ----------------
    SPEC-TA-006, SPEC-TA-008: ta_signals schema/upsert
    """
    from systems.technical_analysis.alerts.daily_alert_checker import (
        _CREATE_TA_SIGNALS_SQL,
        _INSERT_SQL,
    )

    if not body.rows:
        return {"written": 0}

    rows = [
        (
            r.date, r.ticker, r.template_name, r.category, r.score,
            r.matched_conditions, r.total_conditions,
            json.dumps(r.key_values) if r.key_values else None,
        )
        for r in body.rows
    ]

    SIGNALS_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=False) as conn:
        conn.execute(_CREATE_TA_SIGNALS_SQL)
        conn.executemany(_INSERT_SQL, rows)
    return {"written": len(rows)}


@router.post("/user-alerts/check-triggers")
async def check_user_alert_triggers(body: TACheckTriggersRequest) -> Dict[str, Any]:
    """Run alert_store.check_alerts() inside the API process for the given date.

    Same cross-process-lock reasoning as write_ta_signals above: the
    scheduler calls this HTTP endpoint instead of importing alert_store
    and connecting to SIGNALS_DUCKDB_PATH itself.

    Parameters
    ----------
    body : TACheckTriggersRequest

    Returns
    -------
    dict
        {"newly_triggered": [alert_id, ...]}

    Spec References
    ----------------
    SPEC-TA-009: daily alert-trigger evaluation
    """
    from datetime import date as date_type

    run_date = date_type.fromisoformat(body.date)
    newly = alert_store.check_alerts(run_date)
    return {"newly_triggered": newly}


# ---------------------------------------------------------------------------
# Existing endpoints (SPEC-TA-004) — kept below screener/alerts routes
# ---------------------------------------------------------------------------


@router.get("/{ticker}/indicators", response_model=TAIndicatorsResponse)
async def get_ta_indicators(
    ticker: str,
    date: Optional[str] = Query(None, description="YYYY-MM-DD; defaults to the latest available day"),
) -> TAIndicatorsResponse:
    """All 70 core + 18 advanced technical indicator values for one ticker/day."""
    resolved_date = resolve_date(date)
    if resolved_date is None:
        return TAIndicatorsResponse(ticker=ticker, available=False)

    row = read_feature_row(ticker, resolved_date)
    if row is None:
        return TAIndicatorsResponse(ticker=ticker, date=resolved_date, available=False)

    cols = CORE_TECHNICAL_FEATURES + ADVANCED_TECHNICAL_FEATURES
    indicators = {c: (None if c not in row or pd.isna(row[c]) else float(row[c])) for c in cols}
    return TAIndicatorsResponse(ticker=ticker, date=resolved_date, available=True, indicators=indicators)


@router.get("/{ticker}/patterns", response_model=TAPatternsResponse)
async def get_ta_patterns(
    ticker: str,
    date: Optional[str] = Query(None, description="YYYY-MM-DD; defaults to the latest available day"),
) -> TAPatternsResponse:
    """The 6 real chart-pattern probability scores (features/pattern_scores.py)."""
    resolved_date = resolve_date(date)
    if resolved_date is None:
        return TAPatternsResponse(ticker=ticker, available=False)

    row = read_feature_row(ticker, resolved_date)
    if row is None:
        return TAPatternsResponse(ticker=ticker, date=resolved_date, available=False)

    patterns = {c: (None if c not in row or pd.isna(row[c]) else float(row[c])) for c in PATTERN_FEATURES}
    return TAPatternsResponse(ticker=ticker, date=resolved_date, available=True, patterns=patterns)


@router.get("/compare", response_model=TACompareResponse)
async def get_ta_compare(
    tickers: str = Query(..., description="Comma-separated tickers, e.g. RELIANCE,TCS,INFY"),
    days: int = Query(90, ge=10, le=500, description="Calendar days of OHLCV history for the correlation matrix"),
    date: Optional[str] = Query(None, description="YYYY-MM-DD; defaults to the latest available feature day"),
) -> TACompareResponse:
    """Real RS/beta/alpha (already-computed features) plus a real pairwise
    close-to-close return correlation matrix computed from OHLCV — both are
    aggregation over existing real data, not new feature engineering."""
    ticker_list = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    resolved_date = resolve_date(date)

    rows: List[TACompareTickerRow] = []
    if resolved_date is not None:
        for t in ticker_list:
            row = read_feature_row(t, resolved_date)
            if row is None:
                rows.append(TACompareTickerRow(ticker=t))
                continue
            rows.append(TACompareTickerRow(
                ticker=t,
                rs_vs_nifty500_21d=None if "rs_vs_nifty500_21d" not in row or pd.isna(row["rs_vs_nifty500_21d"]) else float(row["rs_vs_nifty500_21d"]),
                beta_63d=None if "beta_63d" not in row or pd.isna(row["beta_63d"]) else float(row["beta_63d"]),
                alpha_21d=None if "alpha_21d" not in row or pd.isna(row["alpha_21d"]) else float(row["alpha_21d"]),
            ))

    correlation: Dict[str, Dict[str, float]] = {}
    if len(ticker_list) >= 2:
        with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
            placeholders = ",".join("?" * len(ticker_list))
            df = conn.execute(
                f"""
                SELECT date, ticker, close FROM ohlcv_adjusted
                WHERE ticker IN ({placeholders}) AND date >= CURRENT_DATE - INTERVAL '{int(days)} days'
                ORDER BY date
                """,
                ticker_list,
            ).fetchdf()
        if not df.empty:
            pivot = df.pivot(index="date", columns="ticker", values="close")
            returns = pivot.pct_change().dropna(how="all")
            corr_matrix = returns.corr()
            for t1 in corr_matrix.columns:
                correlation[t1] = {t2: (None if pd.isna(v) else round(float(v), 4)) for t2, v in corr_matrix[t1].items()}

    return TACompareResponse(date=resolved_date, rows=rows, correlation=correlation)


@router.get("/market_overview", response_model=TAMarketOverviewResponse)
async def get_ta_market_overview() -> TAMarketOverviewResponse:
    """Sector breadth (advances/declines/avg % change) computed from the
    latest 2 trading days of real OHLCV, grouped by config/universe.py's
    real sector map — lightweight aggregation, no new feature computation."""
    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        dates = conn.execute(
            "SELECT DISTINCT date FROM ohlcv_adjusted ORDER BY date DESC LIMIT 2"
        ).fetchall()
        if len(dates) < 2:
            return TAMarketOverviewResponse(available=False)
        latest_date, prev_date = dates[0][0], dates[1][0]

        df = conn.execute(
            "SELECT ticker, date, close FROM ohlcv_adjusted WHERE date IN (?, ?)",
            [latest_date, prev_date],
        ).fetchdf()

    if df.empty:
        return TAMarketOverviewResponse(available=False)

    # DuckDB returns `date` as datetime.date but pandas pivots a
    # datetime64 column into Timestamp-typed columns — normalize both
    # sides to pandas.Timestamp before comparing/indexing.
    latest_date, prev_date = pd.Timestamp(latest_date), pd.Timestamp(prev_date)
    pivot = df.pivot_table(index="ticker", columns="date", values="close")
    if latest_date not in pivot.columns or prev_date not in pivot.columns:
        return TAMarketOverviewResponse(available=False)
    pivot = pivot.dropna(subset=[latest_date, prev_date])
    pivot["change_pct"] = (pivot[latest_date] - pivot[prev_date]) / pivot[prev_date]

    universe = load_universe_raw()
    sector_map = dict(zip(universe["ticker"], universe["sector"]))
    pivot["sector"] = pivot.index.map(lambda t: sector_map.get(t, "Unknown"))

    advances = int((pivot["change_pct"] > 0).sum())
    declines = int((pivot["change_pct"] < 0).sum())
    unchanged = int((pivot["change_pct"] == 0).sum())

    breadth: List[TASectorBreadthRow] = []
    for sector, g in pivot.groupby("sector"):
        breadth.append(TASectorBreadthRow(
            sector=sector,
            advances=int((g["change_pct"] > 0).sum()),
            declines=int((g["change_pct"] < 0).sum()),
            unchanged=int((g["change_pct"] == 0).sum()),
            avg_change_pct=float(g["change_pct"].mean()) if not g["change_pct"].empty else None,
        ))
    breadth.sort(key=lambda r: r.avg_change_pct or 0, reverse=True)

    return TAMarketOverviewResponse(
        date=str(latest_date),
        advances=advances,
        declines=declines,
        unchanged=unchanged,
        sector_breadth=breadth,
        available=True,
    )
