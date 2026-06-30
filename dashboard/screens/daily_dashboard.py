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

--log-trade: records a paper-trading entry decision via the existing
scripts/paper_trading_tracker.py's PaperTradingTracker — no real broker
order, just a written record of "I would have bought/sold this." Entry
only (no --log-exit yet): PaperTradingTracker.log_trade() is an
append-only CSV writer with no "find and update an open position"
mechanism, so closing a position is a separate, not-yet-built action,
not silently bolted on here.
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


def render_multibagger_section(watchlist: Optional[dict]) -> str:
    lines = [_header("MULTIBAGGER WATCHLIST (top 5, weekly refresh — SPEC-UI-003)")]
    if not watchlist or not watchlist.get("implemented"):
        lines.append("  Watchlist not available yet (score_multibagger.py has not run).")
        return "\n".join(lines)
    tickers = watchlist.get("tickers", [])[:5]
    if not tickers:
        lines.append("  No multibagger candidates this week.")
        return "\n".join(lines)
    for i, row in enumerate(tickers, 1):
        survival = ""
        if row.get("survival_12m") is not None:
            survival = f"  12m-surv={row['survival_12m']:.1%}"
        tier = f"  [{row['mb_tier'].upper()}]" if row.get("mb_tier") else ""
        archetype = f"  {row['mb_archetype']}" if row.get("mb_archetype") else ""
        lines.append(
            f"  {i}. {row['ticker']:<12} prob={row['mb_probability']:.1%}{survival}{tier}{archetype}"
        )
    notes = watchlist.get("notes", "")
    if notes:
        lines.append(f"  ({notes})")
    return "\n".join(lines)


def render_forensic_alerts_section(summary: Optional[dict]) -> str:
    lines = [_header("FORENSIC ALERTS (M-09/M-10 universe scan)")]
    if not summary or not summary.get("available"):
        lines.append("  Forensic scores not available yet (score_forensic.py has not run).")
        return "\n".join(lines)
    red = summary.get("red_count", 0)
    amber = summary.get("amber_count", 0)
    green = summary.get("green_count", 0)
    total = summary.get("total_scored", 0)
    as_of = summary.get("as_of_date", "")
    lines.append(f"  As of:     {as_of}")
    lines.append(f"  RED:       {red:>4}  (composite > 60 or black-flag — entry blocked)")
    lines.append(f"  AMBER:     {amber:>4}  (orange/yellow — elevated risk, monitor)")
    lines.append(f"  GREEN:     {green:>4}  (low forensic risk)")
    lines.append(f"  Total:     {total:>4}  stocks scored")
    return "\n".join(lines)


def render_signal63d_section(buys_63d: Optional[List[dict]]) -> str:
    lines = [_header("TOP 5 SIGNAL63D (long-horizon, 63-day horizon)")]
    if not buys_63d:
        lines.append("  No Signal63D scores available (signal_63d model has not run).")
        return "\n".join(lines)
    for i, row in enumerate(buys_63d, 1):
        interval = ""
        if row.get("q10_return") is not None and row.get("q90_return") is not None:
            interval = f"  [q10={row['q10_return']:+.1%}  q90={row['q90_return']:+.1%}]"
        lines.append(f"  {i}. {row['ticker']:<12} buy_prob={row['buy_prob']:.1%}{interval}")
    return "\n".join(lines)


def render_dashboard(
    run_date: Optional[date_type] = None,
    api_base_url: Optional[str] = None,
    held_tickers: Optional[List[str]] = None,
    top_n: int = 5,
) -> str:
    """
    Build the full Phase 2 dashboard as a printable string (SPEC-UI-001).

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
    SPEC-UI-001 (Screen A), SPEC-UI-003 (multibagger watchlist),
    SPEC-DS-002 (API-only access), SPEC-MODEL-009/010 (forensic alerts).

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
        buys_63d = _fetch(
            client, api_base_url,
            f"/api/v1/signals/ml/top_buys/{run_date.isoformat()}",
            {"n": top_n, "model_name": "signal_63d"},
        )
        alerts = _fetch(client, api_base_url, "/api/v1/alerts/today")
        health = _fetch(client, api_base_url, "/health")
        watchlist = _fetch(client, api_base_url, "/api/v1/watchlist/current")
        forensic_summary = _fetch(client, api_base_url, "/api/v1/signals/ml/forensic/summary")

        exit_rows: List[Dict] = []
        for ticker in held_tickers:
            rows = _fetch(client, api_base_url, f"/api/v1/signals/ml/{ticker}/{run_date.isoformat()}")
            for row in rows or []:
                if row["model_name"] == "exit_signal" and row.get("exit_urgency") is not None:
                    exit_rows.append(row)

    # buys_63d comes back as a list directly (same shape as buys)
    buys_63d_list: Optional[List[Dict]] = buys_63d if isinstance(buys_63d, list) else None

    sections = [
        f"AlphaLens Daily Dashboard — {run_date.isoformat()} (generated {now_ist().strftime('%H:%M:%S IST')})",
        render_regime_section(regime),
        render_top_buys_section(buys),
        render_signal63d_section(buys_63d_list),
        render_multibagger_section(watchlist),
        render_forensic_alerts_section(forensic_summary),
        render_exit_section(exit_rows, held_tickers),
        render_pnd_section(alerts),
        render_health_section(health),
    ]
    return "\n".join(sections) + "\n"


def log_paper_trade(
    ticker: str,
    side: str,
    price: float,
    quantity: int,
    entry_time: Optional[str] = None,
    run_date: Optional[date_type] = None,
) -> None:
    """
    Record a paper-trading entry decision (--log-trade) via
    scripts/paper_trading_tracker.py's PaperTradingTracker.

    Parameters
    ----------
    ticker : str
    side : str
        'BUY' | 'SELL' (signal_type in PaperTradingTracker's schema).
    price : float
        Entry price.
    quantity : int
        Share quantity.
    entry_time : str, optional
        "HH:MM:SS". Defaults to now (IST).
    run_date : date, optional
        Defaults to today (IST).

    Returns
    -------
    None
        Prints a one-line confirmation; the row is appended to
        paper_trading/executions/{date}.csv with exit_price/exit_time/
        pnl/pnl_pct left blank (this trade is still open).

    Spec References
    ----------------
    SPEC-OBS-004.

    Raises
    ------
    None
    """
    from scripts.paper_trading_tracker import PaperTradingTracker

    now = now_ist()
    trade_date = (run_date or now.date()).isoformat()
    resolved_time = entry_time or now.strftime("%H:%M:%S")

    tracker = PaperTradingTracker()
    tracker.log_trade(
        date=trade_date, ticker=ticker, signal_type=side, entry_price=price,
        quantity=quantity, entry_time=resolved_time,
    )
    print(f"Logged paper trade: {side} {quantity}x {ticker} @ {price:.2f} on {trade_date} {resolved_time}")


def main() -> None:
    parser = argparse.ArgumentParser(description="AlphaLens Phase 1 daily CLI dashboard (SPEC-UI-001)")
    parser.add_argument("--date", type=str, default=None, help="YYYY-MM-DD (default: today, IST)")
    parser.add_argument(
        "--api", type=str, default=None, help=f"DataStore API base URL (default: {DATASTORE_API_BASE_URL})"
    )
    parser.add_argument("--held", type=str, default="", help="Comma-separated tickers currently held")
    parser.add_argument("--top", type=int, default=5, help="Number of buy signals to show (default: 5)")
    parser.add_argument(
        "--log-trade", type=str, metavar="TICKER", default=None,
        help="Log a paper-trade entry for TICKER (requires --price and --qty) instead of rendering the dashboard",
    )
    parser.add_argument("--side", type=str, default="BUY", choices=["BUY", "SELL"], help="--log-trade only")
    parser.add_argument("--price", type=float, default=None, help="--log-trade only: entry price")
    parser.add_argument("--qty", type=int, default=None, help="--log-trade only: quantity")
    parser.add_argument("--time", type=str, default=None, help="--log-trade only: entry time HH:MM:SS (default: now)")
    args = parser.parse_args()

    run_date = date_type.fromisoformat(args.date) if args.date else None

    if args.log_trade:
        if args.price is None or args.qty is None:
            parser.error("--log-trade requires --price and --qty")
        log_paper_trade(args.log_trade, args.side, args.price, args.qty, args.time, run_date)
        return

    held_tickers = [t.strip() for t in args.held.split(",") if t.strip()]

    print(render_dashboard(run_date=run_date, api_base_url=args.api, held_tickers=held_tickers, top_n=args.top))


if __name__ == "__main__":
    main()
