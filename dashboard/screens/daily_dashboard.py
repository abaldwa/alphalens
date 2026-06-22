"""
dashboard/screens/daily_dashboard.py

Phase: 1.7 (DataStore API Full + Daily Pipeline + Dashboard)
Specs: SPEC-UI-001, SPEC-DS-002
Owner: Platform / Dashboard
Consumers: operator CLI (`python3 -m dashboard.screens.daily_dashboard`)

Phase 1 CLI dashboard (SPEC-UI-001, "Screen A"): market regime, top 5 buy
signals with probabilities + intervals, exit urgency for held positions,
P&D blocks/warnings, pipeline health. ALL data reads go through the
DataStore API via httpx (SPEC-DS-002: "Consumer systems use httpx to call
API, never import db modules") — this module never imports datastore.api.db
or opens a database connection itself.

No portfolio/positions endpoint exists yet (architecture doc's /portfolio/
group is out of this prompt's explicit router list) — "held positions" is
supplied by the operator via --held (comma-separated tickers) rather than
fabricated. "No complex UI needed in Phase 1 — clear terminal output is
sufficient" (build prompt): plain print() sections, no curses/rich/etc.
"""

import argparse
import logging
from datetime import date as date_type
from typing import Dict, List, Optional

import httpx

from config.settings import DATASTORE_API_BASE_URL
from config.timezone import now_ist

logger = logging.getLogger(__name__)

SECTION_WIDTH = 70


def _fetch(client: httpx.Client, api_base_url: str, path: str, params: Optional[dict] = None) -> Optional[dict]:
    try:
        response = client.get(f"{api_base_url}{path}", params=params, timeout=10.0)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError as exc:
        logger.warning(f"dashboard: GET {path} failed: {exc}")
        return None


def _header(title: str) -> str:
    return f"\n{'=' * SECTION_WIDTH}\n{title}\n{'=' * SECTION_WIDTH}"


def render_regime_section(regime: Optional[dict]) -> str:
    lines = [_header("MARKET REGIME")]
    if not regime or not regime.get("available"):
        lines.append("  No regime data available yet.")
        return "\n".join(lines)
    lines.append(f"  Regime:     {regime['hmm_regime'].upper()}")
    if regime.get("hmm_regime_prob") is not None:
        lines.append(f"  Confidence: {regime['hmm_regime_prob']:.1%}")
    lines.append(f"  As of:      {regime['date']}")
    return "\n".join(lines)


def render_top_buys_section(buys: Optional[List[dict]]) -> str:
    lines = [_header("TOP 5 BUY SIGNALS")]
    if not buys:
        lines.append("  No buy signals for this date.")
        return "\n".join(lines)
    for i, row in enumerate(buys, 1):
        interval = ""
        if row.get("q10_return") is not None and row.get("q90_return") is not None:
            interval = f"  [q10={row['q10_return']:+.1%}  q90={row['q90_return']:+.1%}]"
        lines.append(f"  {i}. {row['ticker']:<12} buy_prob={row['buy_prob']:.1%}{interval}")
    return "\n".join(lines)


def render_exit_section(exit_rows: Optional[List[dict]], held_tickers: List[str]) -> str:
    lines = [_header("HELD POSITIONS — EXIT SIGNALS")]
    if not held_tickers:
        lines.append("  No positions specified (use --held TICKER1,TICKER2,...).")
        return "\n".join(lines)
    if not exit_rows:
        lines.append("  No exit signals available for held positions yet.")
        return "\n".join(lines)
    for row in exit_rows:
        action = (
            "IMMEDIATE EXIT" if row["exit_urgency"] > 80
            else "REDUCE 50%" if row["exit_urgency"] > 60
            else "MONITOR" if row["exit_urgency"] > 40
            else "HOLD"
        )
        lines.append(
            f"  {row['ticker']:<12} urgency={row['exit_urgency']:.0f}  type={row['exit_type']}  -> {action}"
        )
    return "\n".join(lines)


def render_pnd_section(alerts: Optional[dict]) -> str:
    lines = [_header("P&D BLOCKS / WARNINGS")]
    pnd_alerts = [a for a in (alerts or {}).get("alerts", []) if a["alert_type"] in ("pnd_block", "pnd_flag")]
    if not pnd_alerts:
        lines.append("  No P&D alerts today.")
        return "\n".join(lines)
    for alert in pnd_alerts:
        lines.append(f"  [{alert['severity'].upper()}] {alert['message']}")
    return "\n".join(lines)


def render_health_section(health: Optional[dict]) -> str:
    lines = [_header("PIPELINE HEALTH")]
    if not health:
        lines.append("  Could not reach DataStore API.")
        return "\n".join(lines)
    lines.append(f"  API status:   {health['status']}")
    lines.append(f"  Stock count:  {health['stock_count']}")
    last_run = health.get("last_pipeline_run")
    if last_run:
        lines.append(f"  Last run:     {last_run['date']} ({last_run['status']})")
    else:
        lines.append("  Last run:     none recorded yet")
    drift = health.get("drift", {})
    if drift.get("worst_status") and drift["worst_status"] != "unknown":
        lines.append(f"  Drift status: {drift['worst_status']} (worst: {drift.get('worst_feature')})")
    else:
        lines.append("  Drift status: no PSI check recorded yet")
    return "\n".join(lines)


def render_dashboard(
    run_date: Optional[date_type] = None,
    api_base_url: Optional[str] = None,
    held_tickers: Optional[List[str]] = None,
    top_n: int = 5,
) -> str:
    """
    Build the full Phase 1 dashboard as a printable string (SPEC-UI-001).

    Parameters
    ----------
    run_date : date, optional
        Defaults to today (IST).
    api_base_url : str, optional
        Defaults to config.settings.DATASTORE_API_BASE_URL.
    held_tickers : list of str, optional
        Tickers currently held, for the exit-urgency section.
    top_n : int
        Number of buy signals to show (build prompt: top 5, default 5).

    Returns
    -------
    str
        The fully rendered dashboard text (also printed by main()).

    Spec References
    ----------------
    SPEC-UI-001 (Screen A), SPEC-DS-002 (API-only access).

    Raises
    ------
    None — every section degrades to an explicit "not available" message
    rather than raising, so one failing endpoint never blocks the rest of
    the dashboard from rendering.
    """
    run_date = run_date or now_ist().date()
    api_base_url = api_base_url or DATASTORE_API_BASE_URL
    held_tickers = held_tickers or []

    with httpx.Client() as client:
        regime = _fetch(client, api_base_url, "/api/v1/macro/regime")
        buys = _fetch(client, api_base_url, f"/api/v1/signals/ml/top_buys/{run_date.isoformat()}", {"n": top_n})
        alerts = _fetch(client, api_base_url, "/api/v1/alerts/today")
        health = _fetch(client, api_base_url, "/health")

        exit_rows: List[Dict] = []
        for ticker in held_tickers:
            rows = _fetch(client, api_base_url, f"/api/v1/signals/ml/{ticker}/{run_date.isoformat()}")
            for row in rows or []:
                if row["model_name"] == "exit_signal" and row.get("exit_urgency") is not None:
                    exit_rows.append(row)

    sections = [
        f"AlphaLens Daily Dashboard — {run_date.isoformat()} (generated {now_ist().strftime('%H:%M:%S IST')})",
        render_regime_section(regime),
        render_top_buys_section(buys),
        render_exit_section(exit_rows, held_tickers),
        render_pnd_section(alerts),
        render_health_section(health),
    ]
    return "\n".join(sections) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="AlphaLens Phase 1 daily CLI dashboard (SPEC-UI-001)")
    parser.add_argument("--date", type=str, default=None, help="YYYY-MM-DD (default: today, IST)")
    parser.add_argument(
        "--api", type=str, default=None, help=f"DataStore API base URL (default: {DATASTORE_API_BASE_URL})"
    )
    parser.add_argument("--held", type=str, default="", help="Comma-separated tickers currently held")
    parser.add_argument("--top", type=int, default=5, help="Number of buy signals to show (default: 5)")
    args = parser.parse_args()

    run_date = date_type.fromisoformat(args.date) if args.date else None
    held_tickers = [t.strip() for t in args.held.split(",") if t.strip()]

    print(render_dashboard(run_date=run_date, api_base_url=args.api, held_tickers=held_tickers, top_n=args.top))


if __name__ == "__main__":
    main()
