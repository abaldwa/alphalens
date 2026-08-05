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

import duckdb
import pandas as pd
import talib
from fastapi import APIRouter, Body, HTTPException, Query

from backtest import strategy_confidence
from config.settings import CONFIDENCE_MIN_INDEPENDENT_DATES, DUCKDB_PATH, SIGNALS_DUCKDB_PATH
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
    TAStrategyHistoryResponse,
    TAStrategyHistoryRow,
    TAStrategyWinRateResponse,
    TAStrategyWinRateRow,
    TASummaryResponse,
    TATemplateInfo,
    TATemplateListResponse,
    TATickerProfileResponse,
    TARecommendationResponse,
    TARecommendationRow,
    TAUserAlertCreate,
    TAUserAlertResponse,
    TAUserAlertRow,
    TAWatchlistResponse,
    TAWatchlistRow,
    TAWatchlistStrategyMatch,
)
from datastore.api.utils.feature_store import read_feature_row, resolve_date
from features.advanced_technical import ADVANCED_TECHNICAL_FEATURES
from features.pattern_scores import PATTERN_FEATURES
from features.technical import CORE_TECHNICAL_FEATURES, _supertrend
from systems.technical_analysis.alerts import alert_store
from systems.technical_analysis.screener.engine import ScreenerEngine
from systems.technical_analysis.screener.templates import STRATEGY_STYLES, TEMPLATE_MAP, TEMPLATE_STYLE

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
    except duckdb.Error as exc:  # REV12 (2026-07-21 review): narrowed from bare Exception —
        # only a real DuckDB-layer failure (missing table, malformed query, lock
        # conflict) should present as an empty "nothing happened today" response;
        # anything else is a genuine bug and must propagate to a 500, not hide.
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
    except duckdb.Error as exc:  # REV12 (2026-07-21 review): narrowed from bare Exception —
        # only a real DuckDB-layer failure (missing table, malformed query, lock
        # conflict) should present as an empty "nothing happened today" response;
        # anything else is a genuine bug and must propagate to a 500, not hide.
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


_FEATURE_LABEL_OVERRIDES = {
    "rsi": "RSI", "sma": "SMA", "ema": "EMA", "macd": "MACD", "bb": "BB",
    "atr": "ATR", "adx": "ADX", "roc": "ROC", "cci": "CCI", "mfi": "MFI",
    "vwap": "VWAP", "di": "DI", "rs": "RS", "cvi": "CVI", "obv": "OBV",
    "hh": "HH", "sar": "SAR",
}


def _humanize_feature(feature: str) -> str:
    """rsi_14 -> 'RSI 14', sma_200_ratio -> 'SMA 200 Ratio' — no Python-side
    label dict exists (only chart.js's CURATED_INDICATORS on the frontend,
    a curated 12-of-94 subset), so this covers the full feature set with a
    token-by-token relabel instead."""
    tokens = [_FEATURE_LABEL_OVERRIDES.get(t.lower(), t.capitalize()) for t in feature.split("_")]
    return " ".join(tokens)


def _describe_condition(cond: Dict[str, Any], key_values: Dict[str, Optional[float]]) -> str:
    """Render one screener condition dict (templates.py) against the actual
    fetched indicator value for that ticker/day, e.g. 'RSI 14 42.2 (in 40-60)'
    instead of just counting matched/total conditions."""
    feature = cond.get("feature", "")
    op = cond.get("op", "")
    value = cond.get("value")
    label = _humanize_feature(feature)
    actual = key_values.get(feature)
    actual_str = f"{actual:.2f}" if actual is not None else "—"

    if op == "between" and isinstance(value, (list, tuple)) and len(value) == 2:
        return f"{label} {actual_str} (in {value[0]}-{value[1]})"
    if op == "top_pct":
        return f"{label} {actual_str} (top {float(value) * 100:.0f}% of universe)"
    if op == "bottom_pct":
        return f"{label} {actual_str} (bottom {float(value) * 100:.0f}% of universe)"
    symbol = {"lt": "<", "gt": ">", "lte": "<=", "gte": ">=", "eq": "="}.get(op, op)
    return f"{label} {actual_str} ({symbol} {value})"


def _describe_rule(cond: Dict[str, Any]) -> str:
    """Render one screener condition dict as a strategy-level rule, with no
    per-ticker actual value (unlike `_describe_condition`, which answers
    "why did this fire for THIS ticker today") — e.g. 'SMA 50 Ratio > 1.0'.
    Used to build a general "what is this strategy" description straight
    from the template's own real condition definitions in templates.py,
    not a fabricated summary."""
    feature = cond.get("feature", "")
    op = cond.get("op", "")
    value = cond.get("value")
    label = _humanize_feature(feature)

    if op == "between" and isinstance(value, (list, tuple)) and len(value) == 2:
        return f"{label} in {value[0]}-{value[1]}"
    if op == "top_pct":
        return f"{label} in top {float(value) * 100:.0f}% of universe"
    if op == "bottom_pct":
        return f"{label} in bottom {float(value) * 100:.0f}% of universe"
    symbol = {"lt": "<", "gt": ">", "lte": "<=", "gte": ">=", "eq": "="}.get(op, op)
    return f"{label} {symbol} {value}"


def _template_strategy_description(tmpl: Any) -> Optional[str]:
    """One-line plain-English explanation of what a screener template
    actually requires, e.g. 'Minervini SEPA requires: SMA 50 Ratio > 1.0;
    SMA 200 Ratio > 1.0; ADX 14 > 20; ...' — derived from the template's own
    conditions list, not a separately maintained (and driftable) blurb."""
    if tmpl is None or not tmpl.conditions:
        return None
    rules = "; ".join(_describe_rule(c) for c in tmpl.conditions)
    return f"{tmpl.description} requires: {rules}"


@router.get("/watchlist/daily", response_model=TAWatchlistResponse)
async def get_ta_daily_watchlist(
    date: Optional[str] = Query(None, description="YYYY-MM-DD; defaults to latest ta_signals date"),
    limit: int = Query(20, ge=1, le=100, description="Maximum tickers"),
    lookback_days: int = Query(5, ge=1, le=20, description="Trading-day window to pool recommendations from"),
    templates: Optional[str] = Query(
        None, description="Comma-separated template names to restrict to; omit for all templates"
    ),
) -> TAWatchlistResponse:
    """Weekly TA WatchList: pools every screener-template recommendation
    (ta_signals, SPEC-TA-006) across the trailing `lookback_days` real
    trading days ending on the target date, keeping each ticker's single
    best (highest-score, most-recent-on-tie) match per template — so a
    ticker with several templates firing in the window shows every one of
    them (see `strategies`), not just whichever template happens to win a
    tie-break. Optionally restricted to a caller-selected subset of
    templates via `templates`. Reports a plain-English rationale per
    matched template, the price as of the ticker's most recent trigger in
    the window (`recommended_price`) next to today's price
    (`current_price`), and the next resistance levels above the current
    price (rolling 20d/50d/252d swing highs plus classic floor-pivot R1/R2,
    computed from real OHLCV — SPEC-TA-004)."""
    SIGNALS_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    template_filter = [t.strip() for t in templates.split(",") if t.strip()] if templates else None
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

            window_dates = [
                r[0] for r in conn.execute(
                    "SELECT DISTINCT date FROM ta_signals WHERE date <= ? ORDER BY date DESC LIMIT ?",
                    [target_date, lookback_days],
                ).fetchall()
            ]
            if not window_dates:
                return TAWatchlistResponse(count=0)
            window_placeholders = ",".join("?" * len(window_dates))

            template_clause = ""
            params: List[object] = list(window_dates)
            if template_filter:
                template_clause = f" AND template_name IN ({','.join('?' * len(template_filter))})"
                params += template_filter

            df = conn.execute(
                f"""
                SELECT date, ticker, template_name, category, score,
                       matched_conditions, total_conditions, key_values
                FROM ta_signals
                WHERE date IN ({window_placeholders}){template_clause}
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY ticker, template_name
                    ORDER BY score DESC, matched_conditions DESC, date DESC
                ) = 1
                ORDER BY ticker ASC, template_name ASC
                """,
                params,
            ).fetchdf()
    except duckdb.Error as exc:  # REV12 (2026-07-21 review): narrowed from bare Exception —
        # only a real DuckDB-layer failure (missing table, malformed query, lock
        # conflict) should present as an empty "nothing happened today" response;
        # anything else is a genuine bug and must propagate to a 500, not hide.
        logger.warning("watchlist/daily TA query failed: %s", exc)
        return TAWatchlistResponse(count=0)

    if df.empty:
        return TAWatchlistResponse(date=target_date, count=0)

    universe = load_universe_raw()
    from config.training_universe import filter_recommendable

    df = filter_recommendable(df, universe=universe)
    if df.empty:
        return TAWatchlistResponse(date=target_date, count=0)

    # Rank tickers and trim to `limit` tickers — not `limit` raw
    # (ticker, template) rows, since a ticker can now carry several matched
    # templates. With no template filter (the default "which stocks are
    # most corroborated" view), most-templates-matched wins ties. With an
    # explicit template_filter, the caller asked for "any ticker matching
    # ANY of these strategies" (OR semantics) — ranking by match_count
    # there would starve single-strategy matches out of the `limit` cutoff
    # whenever enough tickers happen to match several of the selected
    # strategies at once, which reads to a user picking N strategies as
    # "it's requiring ALL of them to match." Rank by recency instead so
    # every selected strategy gets a fair chance to surface its own ticker.
    agg = df.groupby("ticker").agg(match_count=("template_name", "count"), latest_date=("date", "max"))
    sort_keys = ["latest_date"] if template_filter else ["match_count", "latest_date"]
    ticker_rank = agg.sort_values(sort_keys, ascending=False)
    top_tickers = ticker_rank.index[:limit].tolist()
    df = df[df["ticker"].isin(top_tickers)].copy()
    if df.empty:
        return TAWatchlistResponse(date=target_date, count=0)

    name_map = dict(zip(universe["ticker"], universe["company_name"].fillna("")))
    sector_map = dict(zip(universe["ticker"], universe["sector"].fillna("")))
    mcap_map = dict(zip(universe["ticker"], universe["market_cap_cr"]))
    # market_cap_cr == 0 means "not yet sourced" for that ticker (see
    # config/universe.py's REQUIRED_COLUMNS docstring) — excluded from the
    # ranking rather than ranked as if it were a real (tiny) market cap.
    # Ranked over the full universe, not just this watchlist's filtered
    # subset, so "rank" means market-cap rank, not rank-among-20-rows.
    ranked = universe[universe["market_cap_cr"] > 0].copy()
    ranked["market_cap_rank"] = ranked["market_cap_cr"].rank(ascending=False, method="min").astype(int)
    mcap_rank_map = dict(zip(ranked["ticker"], ranked["market_cap_rank"]))

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
    hist["date"] = pd.to_datetime(hist["date"])

    rows: List[TAWatchlistRow] = []
    # Preserve the ticker_rank ordering (most templates matched, then most
    # recent) rather than df's ticker-ASC ordering from the SQL query.
    for ticker in top_tickers:
        tdf = df[df["ticker"] == ticker]
        if tdf.empty:
            continue
        ticker = str(ticker)

        strategies: List[TAWatchlistStrategyMatch] = []
        for _, r in tdf.iterrows():
            tmpl = TEMPLATE_MAP.get(str(r["template_name"]))
            matched, total = int(r["matched_conditions"]), int(r["total_conditions"])
            key_values = _parse_key_values(r.get("key_values"))
            if tmpl is not None and tmpl.conditions:
                detail = "; ".join(_describe_condition(c, key_values) for c in tmpl.conditions)
                rationale = f"{tmpl.description}: {detail}"
            elif tmpl is not None:
                rationale = f"{tmpl.description} — {matched}/{total} conditions matched"
            else:
                rationale = f"{matched}/{total} conditions matched"

            strategies.append(TAWatchlistStrategyMatch(
                template_name=str(r["template_name"]),
                template_description=tmpl.description if tmpl is not None else None,
                template_strategy_description=_template_strategy_description(tmpl),
                category=str(r["category"]),
                date=pd.Timestamp(r["date"]).strftime("%Y-%m-%d"),
                score=float(r["score"]),
                rationale=rationale,
                matched_conditions=matched,
                total_conditions=total,
                key_values=key_values,
            ))
        # Most recently fired template first.
        strategies.sort(key=lambda s: s.date, reverse=True)

        g = hist[hist["ticker"] == ticker]
        current_price = float(g["close"].iloc[-1]) if not g.empty else None

        rec_date = strategies[0].date
        g_asof_rec = g[g["date"] <= pd.Timestamp(rec_date)]
        recommended_price = float(g_asof_rec["close"].iloc[-1]) if not g_asof_rec.empty else None

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
            market_cap_cr=float(mcap_map[ticker]) if ticker in mcap_map and mcap_map[ticker] > 0 else None,
            market_cap_rank=int(mcap_rank_map[ticker]) if ticker in mcap_rank_map else None,
            recommendation_date=rec_date,
            recommended_price=round(recommended_price, 2) if recommended_price is not None else None,
            current_price=round(current_price, 2) if current_price is not None else None,
            strategies=strategies,
            resistance_levels=resistance_levels,
            support_levels=support_levels,
        ))

    return TAWatchlistResponse(date=target_date, rows=rows, count=len(rows))


@router.get("/{ticker}/profile", response_model=TATickerProfileResponse)
async def get_ta_ticker_profile(ticker: str) -> TATickerProfileResponse:
    """Bare company_name/sector lookup for a single ticker (e.g. for the
    Technical Chart page header) — no existing endpoint returned just
    these two fields, so this reuses the same real universe lookup every
    other TA endpoint already does rather than shipping the whole
    universe to the frontend."""
    ticker = ticker.upper()
    universe = load_universe_raw()
    row = universe[universe["ticker"] == ticker]
    if row.empty:
        return TATickerProfileResponse(ticker=ticker)
    r = row.iloc[0]
    return TATickerProfileResponse(
        ticker=ticker,
        company_name=(str(r["company_name"]) if pd.notna(r.get("company_name")) else None),
        sector=(str(r["sector"]) if pd.notna(r.get("sector")) else None),
    )


@router.get("/{ticker}/recommendations", response_model=TARecommendationResponse)
async def get_ta_ticker_recommendations(
    ticker: str,
    date: Optional[str] = Query(None, description="YYYY-MM-DD; defaults to this ticker's latest ta_signals date"),
) -> TARecommendationResponse:
    """ALL screener-template matches for one ticker on one date (not
    deduped to a single best match like /watchlist/daily) — backs the
    Technical Chart page's "recommendations for this day" panel."""
    ticker = ticker.upper()
    SIGNALS_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
            tables = [r[0] for r in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'ta_signals'"
            ).fetchall()]
            if not tables:
                return TARecommendationResponse(ticker=ticker, count=0)

            if date:
                target_date = date
            else:
                row = conn.execute("SELECT MAX(date) FROM ta_signals WHERE ticker = ?", [ticker]).fetchone()
                if row is None or row[0] is None:
                    return TARecommendationResponse(ticker=ticker, count=0)
                target_date = str(row[0])

            df = conn.execute(
                """
                SELECT date, ticker, template_name, category, score,
                       matched_conditions, total_conditions, key_values
                FROM ta_signals
                WHERE ticker = ? AND date = ?
                ORDER BY score DESC, matched_conditions DESC, template_name ASC
                """,
                [ticker, target_date],
            ).fetchdf()

            outcomes_tables = [r[0] for r in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'strategy_confidence_outcomes'"
            ).fetchall()]
            outcomes = pd.DataFrame()
            if outcomes_tables:
                outcomes = conn.execute(
                    """
                    SELECT strategy_id AS template_name, outcome, outcome_date, entry_price, exit_price, net_return_pct
                    FROM strategy_confidence_outcomes
                    WHERE ticker = ? AND date = ?
                    """,
                    [ticker, target_date],
                ).fetchdf()
    except duckdb.Error as exc:  # REV12 (2026-07-21 review): narrowed from bare Exception —
        # only a real DuckDB-layer failure (missing table, malformed query, lock
        # conflict) should present as an empty "nothing happened today" response;
        # anything else is a genuine bug and must propagate to a 500, not hide.
        logger.warning("ta/%s/recommendations query failed: %s", ticker, exc)
        return TARecommendationResponse(ticker=ticker, count=0)

    if df.empty:
        return TARecommendationResponse(date=target_date, ticker=ticker, count=0)

    outcome_map = {str(o["template_name"]): o for _, o in outcomes.iterrows()} if not outcomes.empty else {}

    rows: List[TARecommendationRow] = []
    for _, r in df.iterrows():
        tmpl_name = str(r["template_name"])
        tmpl = TEMPLATE_MAP.get(tmpl_name)
        matched, total = int(r["matched_conditions"]), int(r["total_conditions"])
        key_values = _parse_key_values(r.get("key_values"))
        if tmpl is not None and tmpl.conditions:
            detail = "; ".join(_describe_condition(c, key_values) for c in tmpl.conditions)
            rationale = f"{tmpl.description}: {detail}"
        elif tmpl is not None:
            rationale = f"{tmpl.description} — {matched}/{total} conditions matched"
        else:
            rationale = f"{matched}/{total} conditions matched"

        o = outcome_map.get(tmpl_name)
        rows.append(TARecommendationRow(
            date=pd.Timestamp(r["date"]).strftime("%Y-%m-%d"),
            ticker=ticker,
            template_name=tmpl_name,
            category=str(r["category"]),
            style=TEMPLATE_STYLE.get(tmpl_name),
            score=float(r["score"]),
            rationale=rationale,
            matched_conditions=matched,
            total_conditions=total,
            outcome=(str(o["outcome"]) if o is not None else None),
            outcome_date=(pd.Timestamp(o["outcome_date"]).strftime("%Y-%m-%d") if o is not None and pd.notna(o.get("outcome_date")) else None),
            entry_price=(float(o["entry_price"]) if o is not None and pd.notna(o.get("entry_price")) else None),
            exit_price=(float(o["exit_price"]) if o is not None and pd.notna(o.get("exit_price")) else None),
            net_return_pct=(float(o["net_return_pct"]) if o is not None and pd.notna(o.get("net_return_pct")) else None),
        ))

    return TARecommendationResponse(date=target_date, ticker=ticker, rows=rows, count=len(rows))


@router.get("/{ticker}/strategy_history", response_model=TAStrategyHistoryResponse)
async def get_ta_ticker_strategy_history(ticker: str) -> TAStrategyHistoryResponse:
    """Every template this ticker has ever been recommended under
    (ta_signals, full history), with that ticker's own cost-adjusted
    win/loss track record per template
    (strategy_confidence_outcomes — backtest/strategy_confidence.py).
    Note: win_rate/wilson bounds here are computed over just THIS ticker's
    own firings, so they'll usually be INSUFFICIENT_DATA (too few
    independent dates) even for a template that's VALIDATED in aggregate
    on /strategies/win_rates — that's expected, not a bug: a single
    ticker's history is a much smaller sample than the template's
    universe-wide history."""
    ticker = ticker.upper()
    SIGNALS_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
            tables = [r[0] for r in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'ta_signals'"
            ).fetchall()]
            if not tables:
                return TAStrategyHistoryResponse(ticker=ticker, count=0)

            outcomes_tables = [r[0] for r in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'strategy_confidence_outcomes'"
            ).fetchall()]
            outcomes_join = (
                """
                LEFT JOIN strategy_confidence_outcomes o
                    ON o.date = s.date AND o.ticker = s.ticker AND o.strategy_id = s.template_name
                """
                if outcomes_tables else ""
            )
            outcome_cols = (
                "SUM(CASE WHEN o.outcome = 'win' THEN 1 ELSE 0 END) AS wins,"
                "SUM(CASE WHEN o.outcome = 'loss' THEN 1 ELSE 0 END) AS losses,"
                "SUM(CASE WHEN o.outcome = 'pending' OR o.outcome IS NULL THEN 1 ELSE 0 END) AS pending,"
                "COUNT(DISTINCT CASE WHEN o.outcome IN ('win','loss') THEN s.date END) AS n_independent_dates"
                if outcomes_tables else
                "0 AS wins, 0 AS losses, COUNT(*) AS pending, 0 AS n_independent_dates"
            )

            df = conn.execute(
                f"""
                SELECT s.template_name, s.category, COUNT(*) AS times_recommended,
                       MAX(s.date) AS last_recommended_date, {outcome_cols}
                FROM ta_signals s
                {outcomes_join}
                WHERE s.ticker = ?
                GROUP BY s.template_name, s.category
                ORDER BY times_recommended DESC
                """,
                [ticker],
            ).fetchdf()
    except duckdb.Error as exc:  # REV12 (2026-07-21 review): narrowed from bare Exception —
        # only a real DuckDB-layer failure (missing table, malformed query, lock
        # conflict) should present as an empty "nothing happened today" response;
        # anything else is a genuine bug and must propagate to a 500, not hide.
        logger.warning("ta/%s/strategy_history query failed: %s", ticker, exc)
        return TAStrategyHistoryResponse(ticker=ticker, count=0)

    if df.empty:
        return TAStrategyHistoryResponse(ticker=ticker, count=0)

    rows: List[TAStrategyHistoryRow] = []
    for _, r in df.iterrows():
        wins, losses = int(r["wins"]), int(r["losses"])
        decided = wins + losses
        n_independent_dates = int(r["n_independent_dates"])
        wilson_lo, wilson_hi = strategy_confidence.wilson_interval(wins, decided) if decided > 0 else (None, None)
        tier = (
            strategy_confidence.TIER_INSUFFICIENT
            if n_independent_dates < CONFIDENCE_MIN_INDEPENDENT_DATES or decided == 0
            else strategy_confidence.TIER_PRELIMINARY
        )
        rows.append(TAStrategyHistoryRow(
            template_name=str(r["template_name"]),
            category=str(r["category"]),
            style=TEMPLATE_STYLE.get(str(r["template_name"])),
            times_recommended=int(r["times_recommended"]),
            wins=wins,
            losses=losses,
            pending=int(r["pending"]),
            win_rate=(wins / decided) if decided > 0 else None,
            wilson_lo=wilson_lo,
            wilson_hi=wilson_hi,
            tier=tier,
            last_recommended_date=pd.Timestamp(r["last_recommended_date"]).strftime("%Y-%m-%d"),
        ))

    return TAStrategyHistoryResponse(ticker=ticker, rows=rows, count=len(rows))


@router.get("/strategies/recent_outcomes")
async def get_ta_strategy_recent_outcomes(
    template: Optional[str] = Query(None, description="Filter by strategy_id/template_name"),
    ticker: Optional[str] = Query(None, description="Filter by ticker"),
    limit: int = Query(10, ge=1, le=50),
) -> Dict[str, Any]:
    """Individual firing-level rows from strategy_confidence_outcomes
    (backtest/strategy_confidence.py — the same table /{ticker}/
    strategy_history aggregates), most recent first. Powers two UI
    surfaces off one query: the Strategies page's per-template "latest
    Win/Loss/Open recommendations" drawer (filter by `template`) and Deep
    Dive's "last N strategies that hit this stock" card (filter by
    `ticker`). Exactly one of template/ticker should be passed. `outcome`
    is 'win'/'loss'/'pending' as persisted — the frontend labels 'pending'
    as "Open". Real per-firing entry/exit prices and dates, no synthetic
    numbers — if the table doesn't exist yet (no backfill run) or the
    filter matches nothing, `rows` is simply empty."""
    if not template and not ticker:
        raise HTTPException(status_code=400, detail="Provide template or ticker")

    SIGNALS_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
            tables = [r[0] for r in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'strategy_confidence_outcomes'"
            ).fetchall()]
            if not tables:
                return {"rows": [], "count": 0}

            where_col, where_val = ("strategy_id", template) if template else ("ticker", ticker.upper())
            df = conn.execute(
                f"""
                SELECT date, ticker, strategy_id, entry_price, exit_price, outcome, outcome_date, net_return_pct
                FROM strategy_confidence_outcomes
                WHERE {where_col} = ?
                ORDER BY date DESC
                LIMIT ?
                """,
                [where_val, limit],
            ).fetchdf()
    except duckdb.Error as exc:
        logger.warning("ta/strategies/recent_outcomes query failed: %s", exc)
        return {"rows": [], "count": 0}

    if df.empty:
        return {"rows": [], "count": 0}

    rows = [
        {
            "date": pd.Timestamp(r["date"]).strftime("%Y-%m-%d"),
            "ticker": str(r["ticker"]),
            "template_name": str(r["strategy_id"]),
            "entry_price": None if pd.isna(r["entry_price"]) else float(r["entry_price"]),
            "exit_price": None if pd.isna(r["exit_price"]) else float(r["exit_price"]),
            "outcome": "open" if str(r["outcome"]) == "pending" else str(r["outcome"]),
            "exit_date": None if pd.isna(r["outcome_date"]) else pd.Timestamp(r["outcome_date"]).strftime("%Y-%m-%d"),
            "return_pct": None if pd.isna(r["net_return_pct"]) else float(r["net_return_pct"]),
        }
        for _, r in df.iterrows()
    ]
    return {"rows": rows, "count": len(rows)}


@router.get("/pillar_summary")
async def get_ta_pillar_summary() -> Dict[str, Any]:
    """Home page pillar-outcome card: reuses /watchlist/daily (today's
    template recommendations) for recommendation_count + avg expected
    return (same nearest-resistance-vs-CMP arithmetic the frontend already
    does per-row on the Weekly WatchList page), and /strategies/win_rates
    (real, confidence-graded — INSUFFICIENT_DATA templates already
    excluded there) for the single best-performing template's win rate.
    Technical is the one pillar with a genuine strategy/win-rate table
    (strategy_confidence_summary) — the other 4 pillar_summary endpoints
    return null for top_strategy_success_rate_pct because no equivalent
    table exists for them."""
    # Calling get_ta_daily_watchlist directly (not through a real HTTP
    # request) bypasses FastAPI's dependency injection, so its Query(...)
    # parameter objects never resolve to their defaults — pass the same
    # literal defaults FastAPI would have used (date=None, limit=20,
    # lookback_days=5, templates=None) explicitly.
    watchlist = await get_ta_daily_watchlist(date=None, limit=20, lookback_days=5, templates=None)
    gains = []
    for row in watchlist.rows:
        if row.current_price and row.resistance_levels:
            target = row.resistance_levels[0]
            gains.append((target - row.current_price) / row.current_price * 100)
    avg_gain = sum(gains) / len(gains) if gains else None

    win_rates = await get_ta_strategy_win_rates()
    best_row = None
    for rows in win_rates.styles.values():
        for r in rows:
            if r.win_rate is not None and (best_row is None or r.win_rate > best_row.win_rate):
                best_row = r

    return {
        "as_of_date": watchlist.date,
        "available": watchlist.count > 0,
        "recommendation_count": watchlist.count,
        "avg_expected_return_pct": avg_gain,
        "top_strategy": best_row.template_name if best_row else None,
        "top_strategy_success_rate_pct": (best_row.win_rate * 100) if best_row and best_row.win_rate is not None else None,
    }


@router.get("/strategies/win_rates", response_model=TAStrategyWinRateResponse)
async def get_ta_strategy_win_rates() -> TAStrategyWinRateResponse:
    """Every one of the 42 screener templates, grouped by strategy style
    (Momentum / Trend Following / Mean Reversion / Volatility —
    systems/technical_analysis/screener/templates.py::TEMPLATE_STYLE),
    with its cost-adjusted forward-return confidence result pooled across
    every ticker/date it's fired on (strategy_confidence_summary, regime
    'ALL' rows — backtest/strategy_confidence.py). Templates tiered
    INSUFFICIENT_DATA are DELIBERATELY OMITTED from the response — the
    whole point of the confidence framework is not showing a win-rate
    number until it's earned enough independent history, multi-regime
    coverage, and survives multiple-comparison correction. An empty or
    short list here means exactly that: nothing has enough real history
    yet, not a bug."""
    SIGNALS_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    summaries: Dict[str, Dict] = {}
    try:
        with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
            summary_tables = [r[0] for r in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'strategy_confidence_summary'"
            ).fetchall()]
            if summary_tables:
                df = conn.execute(
                    """
                    SELECT strategy_id, wins, losses, pending, win_rate, wilson_lo, wilson_hi,
                           baseline_win_rate, delta_vs_baseline, deflated_sharpe, sortino, calmar, tier, reasons
                    FROM strategy_confidence_summary
                    WHERE regime = 'ALL'
                    """
                ).fetchdf()
                summaries = {str(r["strategy_id"]): r for _, r in df.iterrows()}
    except duckdb.Error as exc:  # REV12 (2026-07-21 review): narrowed from bare Exception —
        # only a real DuckDB-layer failure (missing table, malformed query, lock
        # conflict) should present as an empty "nothing happened today" response;
        # anything else is a genuine bug and must propagate to a 500, not hide.
        logger.warning("ta/strategies/win_rates query failed: %s", exc)

    styles: Dict[str, List[TAStrategyWinRateRow]] = {s: [] for s in STRATEGY_STYLES}
    for name, tmpl in TEMPLATE_MAP.items():
        s = summaries.get(name)
        tier = str(s["tier"]) if s is not None else strategy_confidence.TIER_INSUFFICIENT
        if tier == strategy_confidence.TIER_INSUFFICIENT:
            continue  # don't show a number until it's earned one

        style = TEMPLATE_STYLE.get(name, "Momentum")
        styles.setdefault(style, []).append(TAStrategyWinRateRow(
            template_name=name,
            category=tmpl.category,
            description=tmpl.description,
            style=style,
            times_recommended=int(s["wins"]) + int(s["losses"]) + int(s["pending"]),
            wins=int(s["wins"]),
            losses=int(s["losses"]),
            pending=int(s["pending"]),
            win_rate=(float(s["win_rate"]) if pd.notna(s.get("win_rate")) else None),
            wilson_lo=(float(s["wilson_lo"]) if pd.notna(s.get("wilson_lo")) else None),
            wilson_hi=(float(s["wilson_hi"]) if pd.notna(s.get("wilson_hi")) else None),
            baseline_win_rate=(float(s["baseline_win_rate"]) if pd.notna(s.get("baseline_win_rate")) else None),
            delta_vs_baseline=(float(s["delta_vs_baseline"]) if pd.notna(s.get("delta_vs_baseline")) else None),
            deflated_sharpe=(float(s["deflated_sharpe"]) if pd.notna(s.get("deflated_sharpe")) else None),
            sortino=(float(s["sortino"]) if pd.notna(s.get("sortino")) else None),
            calmar=(float(s["calmar"]) if pd.notna(s.get("calmar")) else None),
            tier=tier,
            reasons=str(s["reasons"]).split("; ") if s.get("reasons") else [],
        ))
    # VALIDATED rows rank above PRELIMINARY ones regardless of raw win_rate —
    # a lucky small-sample PRELIMINARY win rate shouldn't outrank a row that
    # has actually earned the VALIDATED tier (sample size, multi-regime, DSR,
    # baseline checks). win_rate desc is only the tie-break within a tier.
    _tier_rank = {strategy_confidence.TIER_VALIDATED: 0, strategy_confidence.TIER_PRELIMINARY: 1}
    for s in styles:
        styles[s].sort(
            key=lambda r: (_tier_rank.get(r.tier, 2), r.win_rate is None, -(r.win_rate or 0))
        )

    return TAStrategyWinRateResponse(styles=styles)


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
    except duckdb.Error as exc:  # REV12 (2026-07-21 review): narrowed from bare Exception —
        # only a real DuckDB-layer failure (missing table, malformed query, lock
        # conflict) should present as an empty "nothing happened today" response;
        # anything else is a genuine bug and must propagate to a 500, not hide.
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
        _BULK_UPSERT_SQL,
        _CREATE_TA_SIGNALS_SQL,
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
    batch_df = pd.DataFrame(rows, columns=[
        "date", "ticker", "template_name", "category", "score",
        "matched_conditions", "total_conditions", "key_values",
    ])

    SIGNALS_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=False) as conn:
        conn.execute(_CREATE_TA_SIGNALS_SQL)
        conn.register("_ta_signals_upsert_batch", batch_df)
        try:
            conn.execute(_BULK_UPSERT_SQL)
        finally:
            conn.unregister("_ta_signals_upsert_batch")
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


def _last_or_none(series: "pd.Series") -> Optional[float]:
    if series is None or len(series) == 0 or pd.isna(series.iloc[-1]):
        return None
    return float(series.iloc[-1])


@router.get("/{ticker}/summary", response_model=TASummaryResponse)
async def get_ta_summary(
    ticker: str,
    date: Optional[str] = Query(None, description="YYYY-MM-DD; defaults to the latest available day"),
) -> TASummaryResponse:
    """Raw display-scale values (CMP, 52wk hi/lo, raw SMA/EMA/MACD/VWAP) for
    the Technical Deep Dive page, computed directly from OHLCV — most of
    these aren't stored in features/technical.py, which keeps only ratios.
    """
    resolved_date = resolve_date(date)
    if resolved_date is None:
        return TASummaryResponse(ticker=ticker, available=False)

    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT date, high, low, close, volume, delivery_pct
            FROM ohlcv_adjusted
            WHERE ticker = ? AND date <= ?
            ORDER BY date ASC
            """,
            [ticker, resolved_date],
        ).fetchall()

    if not rows:
        return TASummaryResponse(ticker=ticker, date=resolved_date, available=False)

    df = pd.DataFrame(rows, columns=["date", "high", "low", "close", "volume", "delivery_pct"])
    if str(df["date"].iloc[-1]) != resolved_date:
        return TASummaryResponse(ticker=ticker, date=resolved_date, available=False)

    close, high, low = df["close"], df["high"], df["low"]
    cmp_ = float(close.iloc[-1])

    win252 = df.tail(252)
    week52_high = float(win252["high"].max())
    week52_low = float(win252["low"].min())

    sma_20 = _last_or_none(close.rolling(20).mean())
    sma_50 = _last_or_none(close.rolling(50).mean())
    sma_100 = _last_or_none(close.rolling(100).mean())
    sma_200 = _last_or_none(close.rolling(200).mean())
    ema_9 = _last_or_none(pd.Series(talib.EMA(close.values, timeperiod=9)))
    ema_21 = _last_or_none(pd.Series(talib.EMA(close.values, timeperiod=21)))
    rsi_14 = _last_or_none(pd.Series(talib.RSI(close.values, timeperiod=14)))

    st_dir, _st_signal = _supertrend(high.values, low.values, close.values)
    supertrend_dir = _last_or_none(pd.Series(st_dir))
    hl2 = (high + low) / 2
    atr = pd.Series(talib.ATR(high.values, low.values, close.values, timeperiod=10))
    supertrend_value = _last_or_none(hl2 - (supertrend_dir or 0) * 3 * atr) if supertrend_dir is not None else None

    macd_line, macd_signal_line, macd_hist = talib.MACD(close.values, fastperiod=12, slowperiod=26, signalperiod=9)
    macd = _last_or_none(pd.Series(macd_line))
    macd_signal = _last_or_none(pd.Series(macd_signal_line))
    macd_hist_val = _last_or_none(pd.Series(macd_hist))

    vwap_win = df.tail(20)
    vwap_typical = (vwap_win["high"] + vwap_win["low"] + vwap_win["close"]) / 3
    vwap_denom = vwap_win["volume"].sum()
    vwap_20d = float((vwap_typical * vwap_win["volume"]).sum() / vwap_denom) if vwap_denom else None

    dist_from_52w_high = (cmp_ / week52_high - 1) if week52_high else None
    dist_from_52w_low = (cmp_ / week52_low - 1) if week52_low else None
    sma_50_200_ratio = (sma_50 / sma_200) if sma_50 and sma_200 else None

    delivery_pct = _last_or_none(df["delivery_pct"])
    delivery_win = df["delivery_pct"].tail(21).dropna()
    avg_delivery_pct_21d = float(delivery_win.mean()) if len(delivery_win) else None
    delivery_std = delivery_win.std()
    delivery_pct_zscore_21d = (
        float((delivery_pct - avg_delivery_pct_21d) / delivery_std)
        if delivery_pct is not None and avg_delivery_pct_21d is not None and delivery_std
        else None
    )

    return TASummaryResponse(
        ticker=ticker, date=resolved_date, available=True,
        cmp=cmp_, week52_high=week52_high, week52_low=week52_low,
        sma_20=sma_20, sma_50=sma_50, sma_100=sma_100, sma_200=sma_200,
        ema_9=ema_9, ema_21=ema_21, rsi_14=rsi_14,
        supertrend_value=supertrend_value, supertrend_dir=supertrend_dir,
        macd=macd, macd_signal=macd_signal, macd_hist=macd_hist_val,
        vwap_20d=vwap_20d,
        dist_from_52w_high=dist_from_52w_high, dist_from_52w_low=dist_from_52w_low,
        sma_50_200_ratio=sma_50_200_ratio,
        delivery_pct=delivery_pct, avg_delivery_pct_21d=avg_delivery_pct_21d,
        delivery_pct_zscore_21d=delivery_pct_zscore_21d,
    )


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
