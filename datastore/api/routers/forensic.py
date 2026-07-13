"""
datastore/api/routers/forensic.py

Phase: 2.6 (Phase 2 Data Source Integration)
Specs: SPEC-DS-002, SPEC-DS-004, SPEC-MODEL-009, SPEC-MODEL-010
Owner: Platform / DataStore
Consumers: dashboard, systems/ml_signal_engine, backtest

GET /api/v1/signals/ml/forensic/{ticker}?as_of= and
POST /api/v1/signals/ml/forensic/write — against the ml_forensic DuckDB
table (Store 4, datastore/schema/create_signals.py). One row per
(date, ticker), written by systems/ml_signal_engine/inference/
score_forensic.py (M-09/M-10).

[AS BUILT, P2.6] No further PIT filtering on read: unlike fundamentals/
shareholding (raw disclosures with their own separate filing/announcement
date), ml_forensic rows are already "as of" their own `date` column — the
scoring script that wrote a row already used PIT-filtered inputs to
compute it (same reasoning datastore/api/routers/signals.py's ml_signals
GET endpoints already document/apply: no enforce_pit_* call on this kind
of model-output table). GET returns the most recent row at-or-before
`as_of` (default: now) — a "current forensic risk score" read, not a
specific-date lookup the way signals.py's /ml/{ticker}/{date} is (no date
path param in the build prompt's literal endpoint signature).

[AS BUILT, AF-1] All call sites below now pass persist=False explicitly
(plus an explicit read_only=) — see signals.py's module docstring for why
the earlier "sole writer, no persist=False needed" reasoning didn't
actually hold (BuildLog.md "Fix check_ta_alerts cross-process DuckDB lock
race", commit 8147579).
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, Optional

from fastapi import APIRouter, HTTPException, Query

from config.settings import SIGNALS_DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from datastore.api.schemas import (
    ForensicFlaggedResponse,
    ForensicFlaggedRow,
    ForensicRow,
    ForensicSummaryResponse,
    ForensicWrite,
    ForensicWriteResult,
)
from datastore.api.utils.pdf import build_pdf_response

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/signals/ml/forensic", tags=["Forensic"])

_COLUMNS = [
    "date", "ticker", "beneish_m", "altman_z", "piotroski_f", "ohlson_o", "dechow_f",
    "sloan_accrual", "benford_mad", "benford_detail_json", "forensic_composite", "forensic_flag",
    "forensic_flag_label", "forensic_ml_prob", "shap_top5_json", "pattern_match",
]
_SELECT_COLS = ", ".join(_COLUMNS)


# [AS BUILT, P2.6-dashboard] /summary MUST be registered before /{ticker}: same
# route-ordering discipline as signals.py's /ml/top_buys/{date}-before-/ml/{ticker}/{date}.
@router.get("/summary", response_model=ForensicSummaryResponse)
async def get_forensic_summary() -> ForensicSummaryResponse:
    """
    Universe-wide forensic flag counts for the most recent scored date.
    red_count = red + black labels (critical); amber_count = orange + yellow (elevated).
    Returns available=False when ml_forensic has never been written.
    """
    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
        latest = conn.execute("SELECT MAX(date) FROM ml_forensic").fetchone()
        latest_date = latest[0] if latest else None
        if latest_date is None:
            return ForensicSummaryResponse()

        rows = conn.execute(
            "SELECT forensic_flag_label FROM ml_forensic WHERE date = ?",
            [latest_date],
        ).fetchall()

    labels = [r[0] for r in rows if r[0] is not None]
    red = sum(1 for lbl in labels if lbl in ("red", "black"))
    amber = sum(1 for lbl in labels if lbl in ("orange", "yellow"))
    green = sum(1 for lbl in labels if lbl == "green")
    return ForensicSummaryResponse(
        as_of_date=latest_date,
        red_count=red,
        amber_count=amber,
        green_count=green,
        total_scored=len(labels),
        available=True,
    )


# [AS BUILT, P3.x] /flagged MUST also be registered before /{ticker} (same
# route-ordering discipline as /summary above).
@router.get("/flagged", response_model=ForensicFlaggedResponse)
async def get_forensic_flagged(
    flag: str = Query("red,amber", description="Comma-separated: red, amber, green"),
) -> ForensicFlaggedResponse:
    """All tickers carrying any of the requested flag group(s) on the most recent scored date."""
    groups = {f.strip().lower() for f in flag.split(",") if f.strip()}
    labels: set = set()
    if "red" in groups:
        labels |= {"red", "black"}
    if "amber" in groups:
        labels |= {"orange", "yellow"}
    if "green" in groups:
        labels |= {"green"}
    if not labels:
        raise HTTPException(status_code=400, detail="flag must include at least one of: red, amber, green")

    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
        latest = conn.execute("SELECT MAX(date) FROM ml_forensic").fetchone()
        latest_date = latest[0] if latest else None
        if latest_date is None:
            return ForensicFlaggedResponse()

        placeholders = ", ".join("?" for _ in labels)
        rows = conn.execute(
            f"""
            SELECT ticker, date, forensic_composite, forensic_flag_label FROM ml_forensic
            WHERE date = ? AND forensic_flag_label IN ({placeholders})
            ORDER BY forensic_composite DESC
            """,
            [latest_date, *labels],
        ).fetchall()

    return ForensicFlaggedResponse(
        as_of_date=latest_date,
        rows=[
            ForensicFlaggedRow(ticker=r[0], date=r[1], forensic_composite=r[2], forensic_flag_label=r[3])
            for r in rows
        ],
    )


@router.get("/{ticker}", response_model=Optional[ForensicRow])
async def get_forensic_score(
    ticker: str,
    as_of: Optional[datetime] = Query(None, description="Most recent row at or before this date (default: now)"),
) -> Optional[ForensicRow]:
    """Most recent forensic scoring row for a ticker at or before `as_of`, or None if none exists."""
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")
    pit_reference = as_of or datetime.utcnow()

    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
        row = conn.execute(
            f"""
            SELECT {_SELECT_COLS} FROM ml_forensic
            WHERE ticker = ? AND date <= ?
            ORDER BY date DESC LIMIT 1
            """,
            [ticker, pit_reference.date()],
        ).fetchone()

    return ForensicRow(**dict(zip(_COLUMNS, row))) if row else None


@router.get("/{ticker}/report/pdf")
async def get_forensic_report_pdf(
    ticker: str,
    as_of: Optional[datetime] = Query(None, description="Most recent row at or before this date (default: now)"),
):
    """
    FO6 — server-side PDF export of the Investigation Report screen: same
    real ForensicRow fields report.js templates (Beneish M, Altman Z,
    Piotroski F, Sloan accrual, Benford MAD, ML fraud probability, pattern
    match, blocked/not-blocked recommendation), rendered as an actual PDF
    document via reportlab rather than report.js's previous window.print().
    """
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")
    ticker = ticker.upper()
    pit_reference = as_of or datetime.utcnow()

    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
        row = conn.execute(
            f"""
            SELECT {_SELECT_COLS} FROM ml_forensic
            WHERE ticker = ? AND date <= ?
            ORDER BY date DESC LIMIT 1
            """,
            [ticker, pit_reference.date()],
        ).fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail=f"No forensic score for {ticker}")

    r = dict(zip(_COLUMNS, row))

    lines = []
    composite = r.get("forensic_composite")
    label = (r.get("forensic_flag_label") or "unscored").upper()
    lines.append(f"Forensic Composite: {composite:.0f}/100 ({label})" if composite is not None else f"Forensic Composite: — ({label})")
    if r.get("beneish_m") is not None:
        lines.append(f"Beneish M-Score: {r['beneish_m']:.2f} (manipulator threshold: -1.78)")
    if r.get("altman_z") is not None:
        lines.append(f"Altman Z-Score: {r['altman_z']:.2f} (distress: <1.81, safe: >2.99)")
    if r.get("piotroski_f") is not None:
        lines.append(f"Piotroski F-Score: {r['piotroski_f']:.0f} (weak: <=2)")
    if r.get("sloan_accrual") is not None:
        lines.append(f"Sloan Accrual Ratio: {r['sloan_accrual']:.3f} (high-accrual: >0.10)")
    if r.get("benford_mad") is not None:
        lines.append(f"Benford MAD: {r['benford_mad']:.4f} (non-conforming: >0.015)")
    if r.get("forensic_ml_prob") is not None:
        lines.append(f"ML Fraud Probability: {r['forensic_ml_prob'] * 100:.1f}%")
    if r.get("pattern_match"):
        lines.append(f"Historical Pattern Match: {r['pattern_match']}")

    recommendation = "BLOCKED FROM BUY RECOMMENDATIONS" if r.get("forensic_flag") else "Not currently blocked"

    return build_pdf_response(
        filename=f"{ticker}_investigation_report.pdf",
        title=f"Investigation Report — {ticker}",
        subtitle=f"Scored {r.get('date')} | Recommendation: {recommendation}",
        sections=[("Findings", lines)],
    )


@router.post("/write", response_model=ForensicWriteResult)
async def write_forensic_score(record: ForensicWrite) -> ForensicWriteResult:
    """Upsert one forensic scoring row — SPEC-DS-004: same date+ticker replaces, never duplicates."""
    values = [getattr(record, col) if col != "date" else record.date.date() for col in _COLUMNS]
    placeholders = ", ".join("?" for _ in _COLUMNS)
    update_cols = [c for c in _COLUMNS if c not in ("date", "ticker")]
    update_clause = ", ".join(f"{c} = excluded.{c}" for c in update_cols)

    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=False) as conn:
        conn.execute(
            f"""
            INSERT INTO ml_forensic ({_SELECT_COLS}) VALUES ({placeholders})
            ON CONFLICT (date, ticker) DO UPDATE SET {update_clause}
            """,
            values,
        )

    logger.info(f"forensic.write: {record.ticker} {record.date.date()}")
    return ForensicWriteResult(date=record.date, ticker=record.ticker, written=True)


@router.post("/scan/run")
async def run_universe_scan(
    limit: int = Query(
        default=300, ge=1, le=2500,
        description="Max tickers to scan this call (bounded — same discipline as A28(c)'s "
        "chunking fix; the full ~2,300-ticker universe is scanned in repeated calls, "
        "never materialized/loaded at once).",
    ),
    tier: int = Query(
        default=None, ge=1, le=6,
        description="Optional: restrict the scan to universe tier<=tier (1=Nifty50 ... 6=broader NSE).",
    ),
) -> Dict[str, object]:
    """
    FO7 — on-demand trigger for the universe forensic scan.

    Wraps `score_forensic.py::score_universe` (the same real, per-ticker
    scoring loop the CLI uses — real fundamentals, real classical M-09/M-10
    models, real writes to ml_forensic) behind a dashboard button instead of
    only being reachable as a standalone CLI script. Bounded to `limit`
    tickers per call (default 300) so a single request can't hold the whole
    ~2,300-ticker universe's per-ticker DataFrames in memory or block the
    event loop for the 5-15 minutes a full-universe run takes — run
    repeatedly (or raise `limit`, capped at 2,500) to cover more of the
    universe. Runs in a worker thread (`asyncio.to_thread`) so the FastAPI
    event loop stays responsive to other requests while this executes.
    """
    from config.universe import get_tickers, load_universe_raw
    from systems.ml_signal_engine.inference.score_forensic import score_universe

    try:
        if tier is not None:
            univ = load_universe_raw()
            tickers = univ[univ["tier"] <= tier]["ticker"].dropna().tolist()
        else:
            tickers = get_tickers()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Universe load failed: {exc}")

    tickers = tickers[:limit]
    if not tickers:
        return {"scanned": 0, "succeeded": 0, "failed": 0, "tickers": []}

    try:
        results = await asyncio.to_thread(score_universe, tickers, write=True)
    except Exception as exc:
        logger.error(f"forensic.scan.run failed: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Universe scan failed: {exc}")

    succeeded = [t for t, ok in results.items() if ok]
    failed = [t for t, ok in results.items() if not ok]
    logger.info(f"forensic.scan.run: {len(succeeded)}/{len(tickers)} succeeded (limit={limit}, tier={tier})")
    return {
        "scanned": len(tickers),
        "succeeded": len(succeeded),
        "failed": len(failed),
        "failed_tickers": failed[:50],
    }
