"""
ingestion/scrapers/nse_xbrl_financials.py

Phase: follow-up (NSE XBRL primary-source pipeline, 2026-07-07)
Specs: SPEC-PIPE-006, SPEC-PIPE-003 (CRITICAL)
Owner: Platform / Ingestion
Consumers: scripts/backfill_fundamentals_nse_xbrl.py

Real, free, regulator-authoritative source for quarterly financials —
live-discovered and verified 2026-07-07 (found by grepping NSE's own
loaded corporate-filings.js bundle, same technique that found
nse_pledge.py's endpoint):

    https://www.nseindia.com/api/integrated-filing-results?symbol=X
        -> list of {..., "ixbrl": "https://nsearchives.nseindia.com/.../
           INTEGRATED_FILING_INDAS_<seqId>_<timestamp>_iXBRL_WEB.html", ...}

Each `INTEGRATED_FILING_INDAS_*` URL is a real, standardized HTML
rendering of SEBI's mandatory "Integrated Filing — IndAS" quarterly
disclosure (the regulatory filing itself — every listed company's own
submission, not a third party's re-scrape/re-render of it). Despite the
"iXBRL" filename, live inspection found it is NOT actually inline-XBRL-
tagged (no `<ix:nonFraction>` elements) — it is plain, well-structured
HTML with consistent `<h3>`-delimited sections and `<tr><th>Label</th>
<td>Value</td></tr>` rows, which this module parses directly rather than
via an XBRL library.

Live-verified against RELIANCE's real 2026-03-31 (Q4 FY26) filing —
confirmed sections: "General information about company", "Financial
Results Ind-AS" (full P&L: revenue, expenses, PAT, EPS, debt-equity
ratio), "Statement of Asset and Liabilities" (full balance sheet:
goodwill=Rs.28,46,200 Lakh, CWIP, inventories, trade receivables/
payables, total liabilities, total equity — all real, non-fabricated),
and "Details of Impact of Audit Qualification" (a real structured
"Declaration of unmodified opinion" / qualified-opinion field).

Per explicit operator instruction (2026-07-07): this is now the
PREFERRED/primary fundamentals source — Screener/Trendlyne become
fallback for companies/quarters this regime doesn't yet cover (SEBI's
Integrated Filing regime only fully phased in from FY2023-24 on, so
older quarters may have no filing here).

Genuinely NOT structured in this source (verified live): "Disclosure of
notes on assets and liabilities" renders as freeform "Textual
Information", not a numeric field — contingent liabilities, subsidiary
count, related-party loan amounts remain unavailable here too (same gap
as Screener/Trendlyne, not resolved by this pipeline).
"""

import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

NSE_HOMEPAGE_URL = "https://www.nseindia.com"
NSE_INTEGRATED_FILINGS_URL = "https://www.nseindia.com/api/integrated-filing-results"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)
_TIMEOUT_S = 25
_MAX_RETRIES = 3

# Real 3-crore(x1e-2) unit conversion: NSE's IndAS filings report "Amount
# in (Lakhs)" (1 Lakh = 1e5 INR) — this codebase's fundamentals table
# stores everything in Crore (1 Cr = 1e7 INR) throughout (Screener/Trendlyne
# convention, verified via existing columns' docstrings, e.g. total_debt
# "already in crore, same unit Screener reports every balance-sheet/
# quarterly figure in"). 1 Lakh = 0.01 Cr.
_LAKH_TO_CRORE = 0.01


def _nse_session() -> requests.Session:
    """Create an NSE session with browser headers and homepage cookies (same pattern as corporate_actions.py)."""
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    session.get(NSE_HOMEPAGE_URL, timeout=_TIMEOUT_S)
    return session


def list_integrated_filings(ticker: str) -> List[Dict[str, Any]]:
    """
    Fetch the real list of NSE Integrated Filing disclosures for one ticker.

    Returns
    -------
    list of dict
        Each real NSE record (qe_Date, ixbrl URL, consolidated/standalone
        flag via URL naming, etc.) — filtered to only
        INTEGRATED_FILING_INDAS_* rows (the financial-statements filing;
        INTEGRATED_FILING_GOVERNANCE_* rows are a separate, not-yet-parsed
        disclosure — see module docstring).

    Raises
    ------
    ConnectionError
        If the fetch fails after retries.
    """
    last_exc = None
    for attempt in range(_MAX_RETRIES):
        try:
            session = _nse_session()
            resp = session.get(NSE_INTEGRATED_FILINGS_URL, params={"symbol": ticker}, timeout=_TIMEOUT_S)
            resp.raise_for_status()
            payload = resp.json()
            rows = payload.get("data", [])
            return [r for r in rows if "INTEGRATED_FILING_INDAS" in (r.get("ixbrl") or "")]
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            logger.warning(f"list_integrated_filings({ticker}) attempt {attempt + 1}/{_MAX_RETRIES} failed: {exc}")
    raise ConnectionError(f"Failed to list NSE integrated filings for {ticker} after {_MAX_RETRIES} attempts: {last_exc}")


def _extract_section(html: str, heading: str, next_headings: List[str]) -> str:
    """Return the HTML slice starting at `heading`'s <h3>/<h4> tag up to the next real section heading."""
    idx = html.find(heading)
    if idx == -1:
        return ""
    end = len(html)
    for nh in next_headings:
        nidx = html.find(nh, idx + len(heading))
        if nidx != -1:
            end = min(end, nidx)
    return html[idx:end]


def _table_rows(section_html: str) -> List[List[str]]:
    """Parse every <tr>...</tr> in a section into a list of cleaned cell-text lists."""
    # <tr[^>]*> (not bare <tr>): real subtotal rows ("Total current assets",
    # "Total current liabilities") render as <tr style="background-color:
    # lightgray;">, which a bare `<tr>` pattern silently fails to match —
    # a real bug caught during verification (those two rows were being
    # dropped entirely, not just parsed as None).
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", section_html, re.S)
    out = []
    for row in rows:
        cells = re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", row, re.S)
        cells = [re.sub(r"<[^>]+>", "", c).strip() for c in cells]
        if any(cells):
            out.append(cells)
    return out


def _parse_amount(value: str) -> Optional[float]:
    """'7,51,08,700.00' (Lakh, Indian digit grouping) -> float Crore, or None if blank/non-numeric."""
    if not value:
        return None
    cleaned = value.replace(",", "").strip()
    if not cleaned or cleaned in ("-", "—"):
        return None
    negative = cleaned.startswith("(") and cleaned.endswith(")")
    if negative:
        cleaned = cleaned[1:-1]
    try:
        amount_lakh = float(cleaned)
    except ValueError:
        return None
    amount_cr = amount_lakh * _LAKH_TO_CRORE
    return -amount_cr if negative else amount_cr


# Maps a real "Statement of Asset and Liabilities" row label (as rendered,
# NOT an XBRL tag name — this source isn't actually inline-XBRL-tagged, see
# module docstring) to this codebase's fundamentals column name. Matched via
# exact string equality on the cleaned label text (live-verified against
# RELIANCE's real filing — labels are consistent across the row set NSE's
# Integrated Filing template generates for every company).
_BALANCE_SHEET_LABEL_MAP = {
    "Goodwill": "goodwill",
    "Capital work-in-progress": "cwip",
    "Inventories": "inventories",
    "Trade receivables, current": "trade_receivables_current",
    "Total Trade payable": "trade_payables_current",  # last "Total Trade payable" row wins (current section)
    "Total liabilities": "total_liabilities",
    "Total equity and liabilites": "total_assets",  # balance-sheet identity: total assets == total equity+liabilities
    # 2026-07-07 (same-day follow-up, "add additional columns as necessary,
    # do not skip any datapoints") — the rest of the real, distinct line
    # items this same table exposes.
    "Property, plant and equipment": "property_plant_equipment",
    "Other intangible assets": "intangible_assets",
    "Non-current investments": "non_current_investments",
    "Trade receivables, non-current": "non_current_trade_receivables",
    "Deferred tax assets (net)": "deferred_tax_assets",
    "Current investments": "current_investments",
    "Current tax assets (net)": "current_tax_assets",
    "Borrowings, current": "borrowings_current",
    "Borrowings, non-current": "borrowings_noncurrent",
    "Deferred tax liabilities (net)": "deferred_tax_liabilities",
    "Provisions, current": "provisions_current",
    "Provisions, non-current": "provisions_noncurrent",
    "Equity share capital": "equity_share_capital",
    "Other equity": "other_equity",
    "Non controlling interest": "non_controlling_interest",
    "Total non-current liabilities": "non_current_liabilities",
    # current_assets/current_liabilities already exist in the schema
    # (Screener-sourced) — NSE's own real subtotal rows are at least as
    # authoritative; map them here too so this pipeline can supersede
    # Screener's for companies it covers.
    "Total current assets": "current_assets",
    "Total current liabilities": "current_liabilities",
    # cash_and_equivalents already exists in the schema (Screener-sourced) —
    # NSE's own real figure is at least as authoritative; map it here too so
    # this pipeline can supersede Screener's for companies it covers.
    "Cash and cash equivalents": "cash_and_equivalents",
    # [2026-08-08] Additional NSE XBRL raw line items for derived forensic/working capital columns
    "Contingent liabilities": "contingent_liabilities",
    "Number of subsidiaries": "subsidiary_count_raw",
    "Loans to related parties": "loans_to_related_parties",
    "Director remuneration": "director_remuneration",
}


def _parse_balance_sheet(html: str) -> Dict[str, Any]:
    """
    Parse the real 'Statement of Asset and Liabilities' section into
    fundamentals-column values (Crore), plus the section's own real
    'Date of end of reporting period' — the actual balance-sheet-as-of
    date for THIS filing, which is the correct quarter_end_date to key
    the fundamentals row on (NOT "General information"'s "Date of end of
    financial year", which is always the company's fiscal year-end
    regardless of which quarter this filing covers — a real bug caught
    by checking a non-Q4 filing during verification).
    """
    section = _extract_section(
        html, "Statement of Asset and Liabilities",
        ["Format for Reporting Segment", "Other Comprehensive Income", "Cash flow statement"],
    )
    result: Dict[str, Any] = {}
    for cells in _table_rows(section):
        if len(cells) < 2:
            continue
        label, value = cells[-2].strip(), cells[-1]
        if label == "Date of end of reporting period":
            result["quarter_end_date"] = value
            continue
        col = _BALANCE_SHEET_LABEL_MAP.get(label)
        if col is None:
            continue
        parsed = _parse_amount(value)
        if parsed is not None:
            result[col] = parsed  # last match wins (current-period section appears after non-current in the table)
    return result


def _parse_audit_qualification(html: str) -> Optional[bool]:
    """Parse the real 'Details of Impact of Audit Qualification' section -> True if qualified, False if unmodified."""
    section = _extract_section(html, "Details of Impact of Audit Qualification", [])
    for cells in _table_rows(section):
        if len(cells) >= 2 and "Declaration of unmodified opinion" in cells[0]:
            value = cells[1].strip().lower()
            if "unmodified" in value:
                return False
            if value:
                return True
    return None


def _parse_general_info(html: str) -> Dict[str, Any]:
    """
    Parse real filing-identity fields from 'General information about
    company' — consolidated/standalone only. Deliberately does NOT read
    "Date of end of financial year" here — that field is always the
    company's fiscal year-end regardless of which quarter this filing
    covers (a real bug caught during verification against a non-Q4
    filing); the correct per-filing quarter_end_date comes from the
    Statement of Asset and Liabilities section's own "Date of end of
    reporting period" instead — see _parse_balance_sheet.
    """
    section = _extract_section(html, "General information about company", ["Financial Results Ind-AS"])
    info: Dict[str, Any] = {}
    for cells in _table_rows(section):
        if len(cells) != 2:
            continue
        label, value = cells
        if label == "Nature of report standalone or consolidated":
            info["consolidated"] = value.strip().lower() == "consolidated"
    return info


def _parse_financial_results_period(html: str) -> Optional[str]:
    """
    Fallback quarter_end_date source: the 'Financial Results Ind-AS'
    section's own "Date of end of reporting period" row.

    2026-07-08 (real gap found in full-universe verification): many real
    filings have NO "Statement of Asset and Liabilities" section at all —
    not a template bug, but standard Indian accounting practice: SEBI LODR
    only mandates a full balance sheet at half-year/year-end, so Q1/Q3
    "results only" filings are real and legitimate (live-verified: a real
    BHARTIARTL 31-DEC-2025 filing has Financial Results but no balance
    sheet section). Without this fallback, every such filing was silently
    dropped as "unparseable" even though its P&L-adjacent fields
    (shares_outstanding, consolidated flag) and real quarter identity are
    still genuinely available.
    """
    section = _extract_section(html, "Financial Results Ind-AS", ["Statement of Asset and Liabilities"])
    for cells in _table_rows(section):
        cleaned = [c.strip() for c in cells]
        if "Date of end of reporting period" in cleaned:
            label_idx = cleaned.index("Date of end of reporting period")
            # This row has two value columns ("3 months/6 months ended" and
            # "Year to date") — the second is sometimes genuinely blank
            # (real gap found live: a filing with only the first period
            # populated, `cleaned[-1] == ''`). Take the first non-empty
            # value after the label instead of blindly trusting the last
            # cell.
            for value in cleaned[label_idx + 1:]:
                if value:
                    return value
    return None


def _parse_shares_outstanding(html: str) -> Optional[int]:
    """
    Derive real shares_outstanding from the 'Financial Results Ind-AS'
    section's "Paid-up equity share capital" and "Face value of equity
    share capital" rows — shares = paid-up capital (raw INR) / face value
    (raw INR per share). Live-verified against RELIANCE's real 2026-03-31
    filing: paid-up capital 13,53,200.00 Lakh, face value Rs.10 ->
    1,353,200,000 x 10 = 13,532,000,000 shares (1,353.2 Cr shares) —
    matches RELIANCE's real public share count.

    This resolves a real, pre-existing gap: `shares_outstanding` was
    sparsely populated on Screener-sourced rows (only ever derived
    transiently from a current-snapshot page header, then mostly
    discarded — see ingestion/scrapers/screener.py's
    _build_fundamentals_row), which left features/deep_forensic.py's
    altman_z permanently NaN even after working_capital/total_liabilities/
    retained_earnings all became real (2026-07-07, same session). This
    field, unlike a live/current headline number, comes from the same
    per-quarter regulatory filing as the rest of this module's data — a
    real, PIT-correct historical count, not a current snapshot misapplied
    to an old quarter.

    Unlike _parse_balance_sheet's rows (single value column), this
    section's rows have TWO value columns ("3 months/6 months ended" and
    "Year to date") — both cells hold the same share-capital figure since
    it's a point-in-time balance, not a flow; the last cell is used for
    consistency with the rest of this module's cells[-1] convention.
    """
    section = _extract_section(
        html, "Financial Results Ind-AS", ["Statement of Asset and Liabilities"],
    )
    paid_up_capital_raw: Optional[str] = None
    face_value: Optional[float] = None
    for cells in _table_rows(section):
        if len(cells) < 2:
            continue
        # Unlike _parse_balance_sheet's rows (single value column, label
        # always at cells[-2]), this section's rows have TWO value columns
        # ("3 months/6 months ended" and "Year to date"), so the label can
        # be anywhere before the trailing value cells — search all cells
        # rather than assuming a fixed position (a real indexing bug caught
        # during verification: cells[-2] here is a VALUE, not the label).
        cleaned = [c.strip() for c in cells]
        # Real gap found live: the second ("Year to date") value column is
        # sometimes genuinely blank — take the first non-empty value after
        # the label rather than always trusting the last cell.
        if "Paid-up equity share capital" in cleaned:
            idx = cleaned.index("Paid-up equity share capital")
            paid_up_capital_raw = next((v for v in cleaned[idx + 1:] if v), None)
        elif "Face value of equity share capital" in cleaned:
            idx = cleaned.index("Face value of equity share capital")
            value = next((v for v in cleaned[idx + 1:] if v), None)
            try:
                face_value = float(value) if value else None
            except ValueError:
                face_value = None
    if not paid_up_capital_raw or not face_value:
        return None

    # [FIXED 2026-07-08, second real gap found live] NSE's own filings are
    # genuinely inconsistent about whether "Paid-up equity share capital" is
    # Lakh-scaled (matching this section's "Amount in (Lakhs)" header, e.g.
    # RELIANCE's real "13,53,200.00" -> 1,353.2 Cr shares, correct) or a
    # plain raw-rupee absolute figure (e.g. AARON's real "20,94,64,780.00"
    # -> ~2.1 Cr shares if treated as raw, correct — but 2,094,647,800,000
    # "shares" if wrongly Lakh-scaled). Comma/decimal presence does NOT
    # distinguish these — AARON's value has commas too. Both companies'
    # numbers are real; the reporting convention itself varies per filing.
    # Resolved with a plausibility check instead of a formatting guess: try
    # both interpretations, keep whichever lands in a real-world-plausible
    # share-count range (loose bounds covering the smallest microcap to
    # every real NSE-listed company, including RELIANCE's ~1,353 Cr
    # shares); if both are plausible, prefer the Lakh-scaled interpretation
    # (confirmed the majority convention across real filings checked this
    # session); if neither is plausible, return None rather than guess.
    _MIN_PLAUSIBLE_SHARES = 10_000
    _MAX_PLAUSIBLE_SHARES = 50_000_000_000  # ~50 billion, well above RELIANCE's real ~13.5 billion

    lakh_scaled_cr = _parse_amount(paid_up_capital_raw)
    lakh_shares = int(round((lakh_scaled_cr * 1e7) / face_value)) if lakh_scaled_cr else None
    try:
        raw_rupees = float(paid_up_capital_raw.replace(",", ""))
        raw_shares = int(round(raw_rupees / face_value))
    except ValueError:
        raw_shares = None

    lakh_plausible = lakh_shares is not None and _MIN_PLAUSIBLE_SHARES <= lakh_shares <= _MAX_PLAUSIBLE_SHARES
    raw_plausible = raw_shares is not None and _MIN_PLAUSIBLE_SHARES <= raw_shares <= _MAX_PLAUSIBLE_SHARES

    if lakh_plausible:
        return lakh_shares
    if raw_plausible:
        return raw_shares
    return None


def _parse_indas_html(html: str) -> Dict[str, Any]:
    """Parse an already-fetched real IndAS filing HTML document into fundamentals fields."""
    result: Dict[str, Any] = {}
    result.update(_parse_general_info(html))
    result.update(_parse_balance_sheet(html))
    if result.get("quarter_end_date") is None:
        # Real Q1/Q3 "results only" filings have no balance sheet section at
        # all (SEBI LODR only mandates one at half-year/year-end) — fall
        # back to the Financial Results section's own reporting-period
        # field so these real filings aren't dropped just because a
        # non-mandatory section is genuinely absent.
        fallback_qe = _parse_financial_results_period(html)
        if fallback_qe is not None:
            result["quarter_end_date"] = fallback_qe
    audit_flag = _parse_audit_qualification(html)
    if audit_flag is not None:
        result["audit_qualified_flag"] = audit_flag
    shares_out = _parse_shares_outstanding(html)
    if shares_out is not None:
        result["shares_outstanding"] = shares_out
    return result


def fetch_indas_html(ixbrl_url: str, seq_id: Optional[str] = None, cache_dir: Optional["Path"] = None) -> str:
    """
    Fetch one real NSE Integrated Filing IndAS document's raw HTML, local-
    disk-cached by seq_id when `cache_dir` is given.

    2026-07-08 (per explicit operator instruction): a published regulatory
    filing's reported figures don't change, so once fetched, a filing never
    needs re-downloading — `cache_dir` lets repeat runs (the weekly
    scheduled scan) skip the network entirely for every already-seen
    filing, hitting NSE only for genuinely new ones.

    Parameters
    ----------
    ixbrl_url : str
        A real URL from list_integrated_filings()'s "ixbrl" field.
    seq_id : str, optional
        NSE's own filing sequence ID (list_integrated_filings()'s
        "seq_Id") — the cache key. Required if `cache_dir` is given.
    cache_dir : Path, optional
        Local directory to cache raw HTML in. If None, always fetches live
        (used by tests and by download_indas_filing's backward-compatible
        no-cache path).

    Returns
    -------
    str
        Raw HTML.

    Raises
    ------
    ConnectionError
        If the fetch fails after retries (only reached on a cache miss).
    """
    if cache_dir is not None and seq_id is not None:
        cache_path = cache_dir / f"{seq_id}.html"
        if cache_path.exists():
            return cache_path.read_text()

    last_exc = None
    for attempt in range(_MAX_RETRIES):
        try:
            session = _nse_session()
            resp = session.get(ixbrl_url, timeout=_TIMEOUT_S)
            resp.raise_for_status()
            html = resp.text
            if cache_dir is not None and seq_id is not None:
                cache_dir.mkdir(parents=True, exist_ok=True)
                (cache_dir / f"{seq_id}.html").write_text(html)
            return html
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            logger.warning(f"fetch_indas_html({ixbrl_url}) attempt {attempt + 1}/{_MAX_RETRIES} failed: {exc}")
    raise ConnectionError(f"Failed to download NSE IndAS filing {ixbrl_url} after {_MAX_RETRIES} attempts: {last_exc}")


def download_indas_filing(
    ixbrl_url: str, seq_id: Optional[str] = None, cache_dir: Optional["Path"] = None
) -> Dict[str, Any]:
    """
    Fetch (local-cached, see fetch_indas_html) and parse one real NSE
    Integrated Filing IndAS document.

    Parameters
    ----------
    ixbrl_url : str
        A real URL from list_integrated_filings()'s "ixbrl" field.
    seq_id, cache_dir : optional
        Forwarded to fetch_indas_html — see its docstring.

    Returns
    -------
    dict
        {'quarter_end_date': 'DD-MM-YYYY' or None, 'consolidated': bool or
        None, 'audit_qualified_flag': bool or None, **balance_sheet_fields}
        — only genuinely-parsed real values are present; a field NSE's
        template didn't render for this company/quarter is simply absent
        (never fabricated as 0/None-meaning-zero).

    Raises
    ------
    ConnectionError
        If the fetch fails after retries.
    """
    html = fetch_indas_html(ixbrl_url, seq_id=seq_id, cache_dir=cache_dir)
    return _parse_indas_html(html)


# ── Ingestion state tracking (SPEC-DS-007: SQLite for transactional state,
# never DuckDB) — 2026-07-08, per explicit operator instruction ──────────────

_CREATE_INGESTED_FILINGS_TABLE = """
    CREATE TABLE IF NOT EXISTS nse_xbrl_ingested_filings (
        seq_id VARCHAR NOT NULL PRIMARY KEY,
        ticker VARCHAR NOT NULL,
        fiscal_year INTEGER,
        quarter INTEGER,
        ingested_at TIMESTAMP NOT NULL
    )
"""


def ensure_ingested_filings_table(conn) -> None:
    """Idempotently create the SQLite state table tracking which real NSE seq_ids have been ingested."""
    conn.execute(_CREATE_INGESTED_FILINGS_TABLE)
    conn.commit()


def get_ingested_seq_ids(conn) -> "set":
    """Real seq_ids already ingested in a prior run — these are skipped entirely (no re-download, no re-parse)."""
    return {row[0] for row in conn.execute("SELECT seq_id FROM nse_xbrl_ingested_filings").fetchall()}


def mark_filings_ingested(conn, records: List[Dict[str, Any]]) -> None:
    """
    Record newly-ingested filings so future runs skip them.

    Parameters
    ----------
    records : list of dict
        Each with keys: seq_id, ticker, fiscal_year, quarter.
    """
    if not records:
        return
    now = datetime.now().isoformat()
    # OR REPLACE, not OR IGNORE: a caller may mark the same seq_id twice in
    # one call (e.g. once eagerly with fiscal_year/quarter still unknown,
    # then again once parsing succeeds and they're known) — REPLACE lets a
    # later, more-complete record win, since executemany processes this
    # list in order.
    conn.executemany(
        "INSERT OR REPLACE INTO nse_xbrl_ingested_filings (seq_id, ticker, fiscal_year, quarter, ingested_at) "
        "VALUES (?, ?, ?, ?, ?)",
        [(r["seq_id"], r["ticker"], r["fiscal_year"], r["quarter"], now) for r in records],
    )
    conn.commit()
