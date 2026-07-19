"""
scripts/backfill_fundamentals_trendlyne.py

Phase: 3 (Fundamentals Backfill — Trendlyne source)
Specs: SPEC-PIPE-003 (CRITICAL — PIT), SPEC-SEC-001
Owner: Platform / Ingestion

Logs into Trendlyne and backfills fundamentals for all tickers using the
`get-fundamental_results-v2` JSON API (confirmed working 2026-06-25).

Data returned (all in INR Cr, consolidated where available):
  Quarterly  : Revenue, EBITDA, OPM%, PAT, EPS, Net Margin%, Depreciation
               Interest, Book Value per Share  (13 quarters, ~3 years)
  Annual     : ROE%, ROCE%, D/E, Interest Coverage, EBITDA margin,
               CFO, Cash & equivalents, Total Debt, Asset Turnover,
               Current Assets/Liabilities  (11 years, back to ~FY2015)

UPSERT strategy
---------------
  Quarterly fields  →  UPSERT on (ticker, fiscal_year, quarter)
                         COALESCE preserves existing Screener values;
                         Trendlyne fills only NULL slots.
  Annual fields     →  replicated across all 4 quarters of that FY
                         so every quarter row has ROE, ROCE etc.

PREREQUISITES
-------------
  - TRENDLYNE_USERNAME / TRENDLYNE_PASSWORD in .env
  - No API server required (writes directly to DuckDB)
  - Do NOT run simultaneously with the DataStore API (DuckDB single-writer)

Usage
-----
    # Full run (all DB tickers)
    nohup .venv/bin/python3 scripts/backfill_fundamentals_trendlyne.py \\
        > logs/trendlyne_backfill.log 2>&1 &
    tail -f logs/trendlyne_backfill.log

    # Universe-only (~2492 active tickers, ~60 min)
    .venv/bin/python3 scripts/backfill_fundamentals_trendlyne.py --universe-only

    # Dry-run (parse + print, no DB writes)
    .venv/bin/python3 scripts/backfill_fundamentals_trendlyne.py --dry-run --limit 5

Timing
------
  ~2 HTTP requests + 1.5 s sleep per ticker.
  2492 tickers → ~60 min.
  4110 tickers → ~3.5 hours.
"""

import argparse
import calendar
import gc
import logging
import re
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from features.fundamental_quality_gate import validate_and_annotate  # noqa: E402
from features.fundamental_source_priority import (  # noqa: E402
    SOURCE_PRIORITY,
    build_priority_update_clause,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)

BASE_URL = "https://trendlyne.com"
LOGIN_URL = f"{BASE_URL}/accounts/login/"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

SLEEP_BETWEEN_TICKERS = 1.5   # seconds — respect Trendlyne's servers
BATCH_SIZE = 100               # commit to DuckDB every N tickers

_MONTH_MAP = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

# ── Quarterly field mapping  (Trendlyne key → DB column) ─────────────────────
_Q_FIELDS: Dict[str, str] = {
    "SR_Q": "revenue",  # Operating Revenues Qtr (INR Cr)
    "EBIDT_Q": "ebitda",  # EBITDA Qtr (INR Cr)
    "OPMPCT_Q": "operating_margin",  # Operating Profit Margin %
    "NP_Q": "pat",  # Net Profit Qtr (INR Cr)
    "EPS_Q": "eps",  # Basic EPS Qtr (INR)
    "NETPCT_Q": "net_margin",  # Net Profit Margin %
    "BVSH_Q":     "book_value_per_share",
    "DEP_Q":      "depreciation",
}

# ── Annual field mapping  (Trendlyne key → DB column) ────────────────────────
_A_FIELDS: Dict[str, str] = {
    "ROE_A":                    "roe",
    "ROCE_A":                   "roce",
    "DEBT_CE_A":                "debt_to_equity",
    "IC_A":                     "interest_coverage",
    "EBIDTPCT_A":               "ebitda_margin",
    "CFO_A":                    "fcf",              # cash from ops as FCF proxy
    "CashAndCashEquivalents_A": "cash_and_equivalents",
    "ASETTO_A":                 "asset_turnover",
    "CA_A":                     "current_assets",
    "CL_A":                     "current_liabilities",
    "BVSH_A":                   "book_value_per_share",
}
# total_debt = LongTermBorrowings_A + ShortTermBorrowings_A (computed below)
# gross_profit = SR_Q - OEXPNS_Q + EBIDT_Q (not straightforward; skip)
# shares_outstanding: not directly available as a count in INR Cr units


# ── Date helpers ──────────────────────────────────────────────────────────────

def _parse_quarter_label(label: str) -> Tuple[str, int, int]:
    """
    'Mar 2026' → (quarter_end_date='2026-03-31', fiscal_year=2026, quarter=4)

    Indian FY: Apr-Jun=Q1, Jul-Sep=Q2, Oct-Dec=Q3, Jan-Mar=Q4.
    FY label = the year in which March falls (FY-end).
    """
    parts = label.strip().split()
    if len(parts) != 2:
        raise ValueError(f"Unexpected quarter label: {label!r}")
    mon_str, yr_str = parts
    month = _MONTH_MAP[mon_str]
    year = int(yr_str)
    last_day = calendar.monthrange(year, month)[1]
    qend = f"{year}-{month:02d}-{last_day:02d}"
    if month <= 3:
        fy, q = year, 4
    elif month <= 6:
        fy, q = year + 1, 1
    elif month <= 9:
        fy, q = year + 1, 2
    else:
        fy, q = year + 1, 3
    return qend, fy, q


def _announcement_date(qend: str) -> str:
    """Conservative PIT default: 45 days after quarter end (60 for Q4)."""
    d = date.fromisoformat(qend)
    days = 60 if d.month == 3 else 45
    ann = date(d.year, d.month, d.day)
    import datetime
    return (ann + datetime.timedelta(days=days)).isoformat()


def _safe(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
        return None if (f != f) else f   # NaN check
    except (TypeError, ValueError):
        return None


# ── Trendlyne session helpers ─────────────────────────────────────────────────

def _login() -> requests.Session:
    from config.settings import TRENDLYNE_USERNAME, TRENDLYNE_PASSWORD
    if not TRENDLYNE_USERNAME or not TRENDLYNE_PASSWORD:
        raise RuntimeError(
            "TRENDLYNE_USERNAME / TRENDLYNE_PASSWORD not set in .env"
        )
    session = requests.Session()
    session.headers.update(_HEADERS)

    page = session.get(LOGIN_URL, timeout=30)
    csrf_m = re.search(r'name="csrfmiddlewaretoken"\s+value="([^"]+)"', page.text)
    if not csrf_m:
        raise RuntimeError("Could not find CSRF token on Trendlyne login page")
    csrf = csrf_m.group(1)

    resp = session.post(
        LOGIN_URL,
        data={
            "csrfmiddlewaretoken": csrf,
            "login": TRENDLYNE_USERNAME,
            "password": TRENDLYNE_PASSWORD,
            "recaptcha_token": "",
            "recaptcha_action": "login",
            "remember": "on",
        },
        headers={"Referer": LOGIN_URL},
        timeout=30,
    )
    if resp.status_code >= 400 or "id_password" in resp.text:
        raise RuntimeError(
            f"Trendlyne login failed (status={resp.status_code}). "
            "Check TRENDLYNE_USERNAME/PASSWORD in .env"
        )
    logger.info("Trendlyne login successful → %s", resp.url)
    return session


def _fetch_ticker_data(session: requests.Session, ticker: str) -> Tuple[Optional[Dict], str]:
    """
    Fetch Trendlyne fundamental JSON for a single ticker.

    Returns
    -------
    (body, reason)
        body is the parsed dict on success, else None.
        reason is one of "ok", "404" (ticker genuinely not on Trendlyne),
        "405" (HTTP 405/blocked — see note below), "error" (network/parse
        failure).

    [Fix 2026-07-13] The two live backfill runs on record (2026-06-25,
    2026-06-30 — logs/trendlyne_backfill*.log) both collapsed to near-0%
    success after the first ~100-150 tickers, uniformly across large-cap
    names (ADANIPORTS, TCS-adjacent tickers, etc.) that are unquestionably
    on Trendlyne — a live re-check on 2026-07-13 confirmed these same
    tickers resolve fine (200, real data) with the exact same session/URL
    logic. The 405s were NOT a ticker-matching/URL bug — they are Trendlyne
    WAF/rate-limit responses that started appearing mid-run and then
    self-reinforced: the caller's old code treated 405 exactly like a
    genuine 404 "not on Trendlyne" and slept the SHORT 0.3x notfound delay
    before the next request, so once the WAF started blocking, every
    subsequent request fired even faster and never gave the block a chance
    to clear — a cascading near-100%-failure tail. This function now
    reports 405 distinctly so the caller can apply a full-length backoff
    (not the fast 404 skip) and a circuit-breaker pause + re-login on a
    run of consecutive 405s (see main()'s _consecutive_405 handling).

    Two HTTP requests on success:
      1. Company page  → extract data-tablesurl (session-specific hash)
      2. tablesurl     → JSON with quarterly + annual data
    """
    company_url = f"{BASE_URL}/equity/{ticker}/{ticker.lower()}/"
    try:
        r = session.get(company_url, timeout=30)
    except requests.RequestException as exc:
        logger.debug("Network error for %s: %s", ticker, exc)
        return None, "error"

    if r.status_code == 404:
        logger.debug("%s: 404 on Trendlyne (not listed)", ticker)
        return None, "404"
    if r.status_code in (405, 410):
        # Try the dash-slug fallback first (a genuine, if rare, ticker/slug
        # mismatch) before concluding this is a WAF block.
        slug = ticker.lower().replace("&", "-")
        alt_url = f"{BASE_URL}/equity/{ticker}/{slug}/"
        if alt_url != company_url:
            try:
                r2 = session.get(alt_url, timeout=30)
                if r2.status_code == 200:
                    r = r2
                else:
                    logger.debug("%s: not on Trendlyne (405 + alt %d)", ticker, r2.status_code)
                    return None, "405"
            except requests.RequestException:
                return None, "405"
        else:
            logger.debug("%s: not on Trendlyne (HTTP 405)", ticker)
            return None, "405"
    elif r.status_code == 403:
        logger.debug("%s: 403 — may need re-login", ticker)
        return None, "405"  # same "possible block" bucket as 405 — full backoff, not the fast 404 skip
    elif r.status_code != 200:
        logger.warning("%s: company page → HTTP %d", ticker, r.status_code)
        return None, "error"

    tablesurl_m = re.search(r'data-tablesurl=(https://[^\s>]+)', r.text)
    if not tablesurl_m:
        logger.debug("%s: data-tablesurl not found on company page", ticker)
        return None, "404"
    tablesurl = tablesurl_m.group(1)

    try:
        rj = session.get(
            tablesurl, timeout=30,
            headers={"X-Requested-With": "XMLHttpRequest", "Referer": company_url},
        )
    except requests.RequestException as exc:
        logger.debug("Network error fetching tablesurl for %s: %s", ticker, exc)
        return None, "error"

    if rj.status_code != 200 or not rj.text.strip():
        logger.debug("%s: tablesurl → HTTP %d (empty=%s)", ticker, rj.status_code, not rj.text.strip())
        return None, "405" if rj.status_code in (405, 403, 410) else "error"

    try:
        js = rj.json()
    except Exception:
        logger.debug("%s: tablesurl response is not JSON", ticker)
        return None, "error"

    if js.get("head", {}).get("status") != "0":
        logger.debug("%s: Trendlyne API returned non-success: %s", ticker, js.get("head"))
        return None, "404"

    return js.get("body"), "ok"


# ── Data extraction ───────────────────────────────────────────────────────────

def _extract_quarterly_rows(ticker: str, body: Dict) -> List[Dict]:
    """Build one fundamentals dict per quarter from quarterly data dump."""
    q_order = body.get("quarterlyOrder", [])
    # Prefer consolidated; fall back to standalone
    q_dump = body.get("quarterlyDataDump", {})
    q_data = q_dump.get("consolidated") or q_dump.get("standalone") or {}

    rows = []
    for label in q_order:
        period = q_data.get(label)
        if not period:
            continue
        try:
            qend, fy, q = _parse_quarter_label(label)
        except Exception:
            continue

        row: Dict[str, Any] = {
            "ticker": ticker,
            "fiscal_year": fy,
            "quarter": q,
            "quarter_end_date": qend,
            "announcement_date": _announcement_date(qend),
        }
        for tl_key, db_col in _Q_FIELDS.items():
            row[db_col] = _safe(period.get(tl_key))
        # Trendlyne's OPMPCT_Q/NETPCT_Q are already percent (e.g. 27.0 = 27%); the
        # fundamentals.operating_margin/net_margin columns are a fraction contract
        # (matches ingestion/scrapers/screener.py's operating_profit/revenue), and
        # the dashboard's fmtPct() multiplies by 100 at display time — storing
        # percent here made margins render 100x too high (e.g. "2700%").
        for pct_col in ("operating_margin", "net_margin"):
            if row.get(pct_col) is not None:
                row[pct_col] = row[pct_col] / 100

        # gross_profit: SR_Q - OEXPNS_Q (if available)
        sr = _safe(period.get("SR_Q"))
        opex = _safe(period.get("OEXPNS_Q"))
        row["gross_profit"] = (sr - opex) if (sr is not None and opex is not None) else None

        # interest_coverage (quarterly): OP_Q / INT_Q
        op_q = _safe(period.get("OP_Q"))
        int_q = _safe(period.get("INT_Q"))
        row["interest_coverage"] = (
            (op_q / int_q) if (op_q is not None and int_q is not None and int_q > 0) else None
        )

        # Annual fields defaulting to None (will be patched from annual data later)
        for db_col in _A_FIELDS.values():
            row.setdefault(db_col, None)
        row.setdefault("total_debt", None)
        row.setdefault("capex", None)
        row.setdefault("shares_outstanding", None)

        rows.append(row)
    return rows


def _extract_annual_patch(body: Dict) -> Dict[int, Dict]:
    """
    Build a map of fiscal_year → {db_col: value} from annual data.
    Annual FY label 'Mar YYYY' → fiscal_year = YYYY.
    """
    a_order = body.get("annualOrder", [])
    a_dump = body.get("annualDataDump", {})
    a_data = a_dump.get("consolidated") or a_dump.get("standalone") or {}

    patches: Dict[int, Dict] = {}
    for label in a_order:
        period = a_data.get(label)
        if not period:
            continue
        try:
            _, fy, _ = _parse_quarter_label(label)   # Mar YYYY → Q4 of FY
        except Exception:
            continue

        patch: Dict[str, Any] = {}
        for tl_key, db_col in _A_FIELDS.items():
            patch[db_col] = _safe(period.get(tl_key))

        # total_debt = long-term + short-term borrowings
        ltd = _safe(period.get("LongTermBorrowings_A"))
        std = _safe(period.get("ShortTermBorrowings_A"))
        if ltd is not None or std is not None:
            patch["total_debt"] = (ltd or 0.0) + (std or 0.0)
        else:
            patch["total_debt"] = None

        patches[fy] = patch
    return patches


def _merge_annual(q_rows: List[Dict], annual_patches: Dict[int, Dict]) -> List[Dict]:
    """
    Patch quarterly rows with annual data for the same fiscal year.
    COALESCE: quarterly value wins if already present.
    """
    for row in q_rows:
        patch = annual_patches.get(row["fiscal_year"], {})
        for col, val in patch.items():
            if row.get(col) is None:
                row[col] = val
    return q_rows


# ── DB writes ─────────────────────────────────────────────────────────────────

# [AS BUILT, A36 fix 2026-07-09] update clause is now built from
# features/fundamental_source_priority.py's shared
# build_priority_update_clause instead of a hand-written COALESCE
# direction — see that module's docstring for why (4 independently
# drifting writers was the A36 bug itself). Trendlyne's own priority
# (SOURCE_PRIORITY["trendlyne"] = 3) is written on every upsert via
# fundamentals_source/fundamentals_source_priority.
_UPSERT_DATA_COLS = [
    "revenue", "ebitda", "pat", "eps", "operating_margin", "ebitda_margin", "net_margin",
    "roe", "roce", "debt_to_equity", "interest_coverage", "fcf", "gross_profit",
    "total_debt", "cash_and_equivalents", "asset_turnover",
    "current_assets", "current_liabilities",
    "book_value_per_share", "depreciation",
    "quality_flag", "quality_flag_reason",
]
_UPSERT_SQL = f"""
INSERT INTO fundamentals (
    ticker, fiscal_year, quarter, quarter_end_date, announcement_date,
    {", ".join(_UPSERT_DATA_COLS)},
    fundamentals_source, fundamentals_source_priority, as_of_ingested
) VALUES (
    ?,?,?,?,?,
    {", ".join("?" for _ in _UPSERT_DATA_COLS)},
    ?,?,CURRENT_TIMESTAMP
)
ON CONFLICT (ticker, fiscal_year, quarter) DO UPDATE SET
    {build_priority_update_clause(_UPSERT_DATA_COLS)}
"""


def _write_rows(conn, rows: List[Dict]) -> int:
    written = 0
    for r in rows:
        # backlog #12/AF-5: flag (never silently write) out-of-range ratios,
        # e.g. a margin still in 0-100 scale that slipped past the /100
        # conversion above, before this row ever reaches the DB.
        r = validate_and_annotate(r)
        try:
            conn.execute(_UPSERT_SQL, [
                r["ticker"], r["fiscal_year"], r["quarter"],
                r["quarter_end_date"], r["announcement_date"],
                *[r.get(c) for c in _UPSERT_DATA_COLS],
                "trendlyne", SOURCE_PRIORITY["trendlyne"],
            ])
            written += 1
        except Exception as exc:
            logger.debug("Write error %s FY%s Q%s: %s",
                         r["ticker"], r["fiscal_year"], r["quarter"], exc)
    return written


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill fundamentals from Trendlyne")
    parser.add_argument("--universe-only", action="store_true",
                        help="Only process the ~2492-ticker active universe (default: all DB tickers)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and parse but do not write to DB")
    parser.add_argument("--limit", type=int, default=None,
                        help="Stop after N tickers (for testing)")
    parser.add_argument("--sleep", type=float, default=SLEEP_BETWEEN_TICKERS,
                        help=f"Seconds between tickers (default: {SLEEP_BETWEEN_TICKERS})")
    parser.add_argument("--publish-mode", choices=["direct", "staged"], default="staged",
                        help="'staged' (default as of the 2026-07-10 Pipeline & Monitoring "
                             "Remediation, A51): accumulate every row across the whole run, "
                             "merge against production with the same existing-wins COALESCE "
                             "policy (datastore/staging/merge.py::coalesce_merge), and publish "
                             "atomically once at the end — gives this backfill an N=7 rollback "
                             "point (A25) instead of bypassing it. 'direct': legacy per-batch "
                             "COALESCE upsert, no rollback snapshot; kept only as an escape hatch.")
    args = parser.parse_args()

    from config.settings import DUCKDB_PATH
    from datastore.api.db import get_duckdb_connection

    # Build ticker list
    if args.universe_only:
        from config.universe import get_tickers
        tickers = get_tickers()
    else:
        with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
            tickers = [r[0] for r in conn.execute(
                "SELECT DISTINCT ticker FROM ohlcv_adjusted ORDER BY ticker"
            ).fetchall()]
    if args.limit:
        tickers = tickers[:args.limit]
    logger.info("Trendlyne backfill: %d tickers, dry_run=%s", len(tickers), args.dry_run)

    session = _login()
    t_start = time.monotonic()
    ok = notfound = errors = total_rows = 0
    pending_rows: List[Dict] = []
    # [Fix 2026-07-13] Circuit breaker for Trendlyne WAF/rate-limit 405s —
    # see _fetch_ticker_data's docstring for why 405 must never use the
    # fast 404-skip sleep (that's exactly what turned a transient block
    # into a near-100%-failure cascade in the 2026-06-25/06-30 runs).
    consecutive_405 = 0
    _CONSECUTIVE_405_THRESHOLD = 5
    _BACKOFF_SECONDS = 60

    def _flush(conn):
        nonlocal total_rows
        if not pending_rows:
            return
        written = _write_rows(conn, pending_rows)
        total_rows += written
        pending_rows.clear()

    for i, ticker in enumerate(tickers, start=1):
        try:
            body, reason = _fetch_ticker_data(session, ticker)
        except Exception as exc:
            logger.warning("[%d/%d] %s: fetch error — %s", i, len(tickers), ticker, exc)
            errors += 1
            consecutive_405 = 0
            time.sleep(args.sleep)
            continue

        if reason == "405":
            consecutive_405 += 1
            notfound += 1
            if consecutive_405 >= _CONSECUTIVE_405_THRESHOLD:
                logger.warning(
                    "[%d/%d] %d consecutive HTTP 405s (likely Trendlyne WAF/rate-limit) — "
                    "backing off %ds and re-logging in before continuing",
                    i, len(tickers), consecutive_405, _BACKOFF_SECONDS,
                )
                time.sleep(_BACKOFF_SECONDS)
                try:
                    session = _login()
                except Exception as exc:
                    logger.warning("Re-login failed: %s — continuing with the existing session", exc)
                consecutive_405 = 0
            else:
                time.sleep(args.sleep)   # full sleep, not the fast 404 skip — do not feed the block
            continue

        if body is None:
            # Genuine 404 / not-on-Trendlyne / parse miss — safe to skip fast.
            notfound += 1
            consecutive_405 = 0
            time.sleep(args.sleep * 0.3)
            continue

        consecutive_405 = 0
        try:
            q_rows = _extract_quarterly_rows(ticker, body)
            a_patch = _extract_annual_patch(body)
            rows = _merge_annual(q_rows, a_patch)
        except Exception as exc:
            logger.warning("[%d/%d] %s: parse error — %s", i, len(tickers), ticker, exc)
            errors += 1
            time.sleep(args.sleep)
            continue

        ok += 1
        if not args.dry_run:
            pending_rows.extend(rows)
        else:
            logger.info("[%d/%d] %s: %d rows (dry-run)", i, len(tickers), ticker, len(rows))
            if rows:
                r0 = rows[0]
                logger.info("  Sample: FY%s Q%s | rev=%.0f ebitda=%.0f pat=%.0f roe=%s",
                            r0["fiscal_year"], r0["quarter"],
                            r0.get("revenue") or 0, r0.get("ebitda") or 0,
                            r0.get("pat") or 0, r0.get("roe"))

        # Flush every BATCH_SIZE tickers — direct mode only. Staged mode
        # (A25) accumulates every row for the whole run and merges +
        # publishes once at the end (see below), since a partial
        # mid-run publish would defeat the point of an atomic swap.
        if not args.dry_run and args.publish_mode == "direct" and len(pending_rows) >= BATCH_SIZE * 15:
            with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
                _flush(conn)
            gc.collect()

        if i % 100 == 0 or i == len(tickers):
            elapsed = time.monotonic() - t_start
            rate = i / elapsed if elapsed > 0 else 0
            eta = (len(tickers) - i) / rate / 60 if rate > 0 else 0
            logger.info("[%d/%d] ok=%d notfound=%d err=%d rows=%d  %.1f t/s  ETA~%.0f min",
                        i, len(tickers), ok, notfound, errors, total_rows, rate, eta)

        time.sleep(args.sleep)

    # Final flush
    if not args.dry_run and args.publish_mode == "direct" and pending_rows:
        with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
            _flush(conn)

    if not args.dry_run and args.publish_mode == "staged" and pending_rows:
        import pandas as pd

        from datastore.staging.gate import stage_dataframe
        from datastore.staging.merge import coalesce_merge
        from datastore.staging.publish import publish_run_lock, publish_table

        annotated_rows = [validate_and_annotate(r) for r in pending_rows]
        new_df = pd.DataFrame(annotated_rows)
        total_rows = len(new_df)

        with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
            existing_df = conn.execute("SELECT * FROM fundamentals").df()
            merged_df = coalesce_merge(
                existing_df, new_df, key_cols=["ticker", "fiscal_year", "quarter"],
                new_wins=False,  # trendlyne never overwrites an already-populated value
                force_new_wins_cols=["quality_flag", "quality_flag_reason"],
            )
            with publish_run_lock() as acquired:
                if not acquired:
                    logger.error("Another publish is in progress — staged trendlyne backfill NOT published.")
                else:
                    result = stage_dataframe(conn, "fundamentals", merged_df, validators=[])
                    if not result.ok:
                        logger.error("Staging gate rejected the entire batch — nothing published.")
                    else:
                        published_rows = publish_table(conn, "fundamentals")
                        logger.info(
                            "Staged publish: %d new rows merged, %d now in fundamentals",
                            total_rows, published_rows,
                        )

    # Summary
    if not args.dry_run:
        with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
            f = conn.execute("SELECT COUNT(*), COUNT(DISTINCT ticker) FROM fundamentals").fetchone()
            roe_pct = conn.execute(
                "SELECT COUNT(roe)*100.0/COUNT(*) FROM fundamentals"
            ).fetchone()[0]
            roce_pct = conn.execute(
                "SELECT COUNT(roce)*100.0/COUNT(*) FROM fundamentals"
            ).fetchone()[0]

    elapsed_min = (time.monotonic() - t_start) / 60
    logger.info("─" * 60)
    logger.info("Trendlyne backfill complete in %.1f min", elapsed_min)
    logger.info("  Tickers: %d ok, %d not-on-Trendlyne, %d errors", ok, notfound, errors)
    logger.info("  Total rows written: %d", total_rows)
    if not args.dry_run:
        logger.info("  fundamentals table: %d rows, %d tickers", f[0], f[1])
        logger.info("  ROE completeness : %.1f%%", roe_pct)
        logger.info("  ROCE completeness: %.1f%%", roce_pct)


if __name__ == "__main__":
    main()
