"""
ingestion/scrapers/fyers_symbol_master.py

Phase: 0.5 (FYERS Historical Backfill / Daily Cutover)
Specs: SPEC-PIPE-001
Owner: Platform / Ingestion
Consumers: scripts/fyers_staged_backfill.py, ingestion/scheduler/daily_pipeline.py
    (step_download_fyers_daily)

Downloads FYERS' own NSE Capital-Market symbol master
(https://public.fyers.in/sym_details/NSE_CM.csv — public, no auth
required) and extracts the set of bare tickers FYERS actually recognizes
under the "NSE:<TICKER>-EQ" symbol format this project's FYERSBackfill
uses.

Why this exists
----------------
[2026-08-04, live-confirmed] scripts/fyers_staged_backfill.py's live runs
hit a steady ~10-12% "Invalid symbol provided" (code -300) rate across
the universe — genuine NSE/FYERS symbol-mapping gaps (renames, delistings,
suspensions our own stock_master hasn't caught up to), not a bug. Rather
than discovering each one reactively via a failed history() call (wasted
API call + noisy warning log per ticker, every single run), this module
lets callers filter the ticker list against FYERS' own source of truth
BEFORE requesting history for any of them.

The CSV has no header row and its column order has changed before per
FYERS' own community forum — column POSITION is not hardcoded here for
that reason. Instead, the row is scanned for the one field matching the
"EXCHANGE:SYMBOL-SERIES" pattern this project's FYERSBackfill already
builds (ingestion/scrapers/fyers_backfill.py's EXCHANGE_PREFIX +
EXCHANGE_SEGMENT_SUFFIX), which is robust to column reordering.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Optional, Set


from config.settings import FYERS_RAW_DIR
from config.timezone import now_ist
from ingestion.scrapers.fyers_backfill import EXCHANGE_PREFIX, EXCHANGE_SEGMENT_SUFFIX

logger = logging.getLogger(__name__)

NSE_CM_SYMBOL_MASTER_URL = "https://public.fyers.in/sym_details/NSE_CM.csv"
SYMBOL_MASTER_CACHE_PATH = FYERS_RAW_DIR / "nse_cm_symbol_master.csv"
_REQUEST_TIMEOUT_SECONDS = 30

_SYMBOL_PATTERN = re.compile(
    rf"^{re.escape(EXCHANGE_PREFIX)}([A-Z0-9&\-]+){re.escape(EXCHANGE_SEGMENT_SUFFIX)}$"
)


def _extract_ticker(field: str) -> Optional[str]:
    """Return the bare ticker if `field` matches 'NSE:<TICKER>-EQ', else None."""
    match = _SYMBOL_PATTERN.match(field.strip())
    return match.group(1) if match else None


def fetch_valid_nse_eq_tickers(force_refresh: bool = False) -> Set[str]:
    """
    Return the set of bare NSE-EQ tickers FYERS' own symbol master
    recognizes — i.e. every ticker download_history() can succeed for
    under the "NSE:<ticker>-EQ" symbol format.

    Parameters
    ----------
    force_refresh : bool
        If True, re-download even if today's cache already exists.

    Returns
    -------
    Set[str]
        Bare tickers (e.g. "RELIANCE", not "NSE:RELIANCE-EQ"). Empty set
        if the download fails and no usable cache exists — callers must
        treat that as "could not filter" (fall back to attempting every
        ticker), never as "FYERS has zero symbols".

    Spec References
    ----------------
    SPEC-PIPE-001.

    Raises
    ------
    None — network/parse failures are logged and degrade to an empty
    set (or the last good cache, if any), never propagated.
    """
    if not force_refresh and _cache_is_fresh():
        return _load_from_cache()

    import requests

    try:
        response = requests.get(NSE_CM_SYMBOL_MASTER_URL, timeout=_REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except Exception as exc:
        logger.warning(f"fyers_symbol_master: download failed ({exc}) — falling back to cache")
        return _load_from_cache()

    SYMBOL_MASTER_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    SYMBOL_MASTER_CACHE_PATH.write_bytes(response.content)
    _write_cache_date_marker()

    tickers = _parse_tickers(response.text)
    logger.info(f"fyers_symbol_master: {len(tickers)} valid NSE-EQ tickers loaded from FYERS")
    return tickers


def _cache_date_marker_path() -> Path:
    return SYMBOL_MASTER_CACHE_PATH.with_suffix(".date")


def _cache_is_fresh() -> bool:
    marker = _cache_date_marker_path()
    if not SYMBOL_MASTER_CACHE_PATH.exists() or not marker.exists():
        return False
    return marker.read_text().strip() == now_ist().date().isoformat()


def _write_cache_date_marker() -> None:
    _cache_date_marker_path().write_text(now_ist().date().isoformat())


def _load_from_cache() -> Set[str]:
    if not SYMBOL_MASTER_CACHE_PATH.exists():
        logger.warning("fyers_symbol_master: no cache available — cannot filter by valid symbols")
        return set()
    return _parse_tickers(SYMBOL_MASTER_CACHE_PATH.read_text())


def _parse_tickers(csv_text: str) -> Set[str]:
    tickers: Set[str] = set()
    for line in csv_text.splitlines():
        for field in line.split(","):
            ticker = _extract_ticker(field)
            if ticker:
                tickers.add(ticker)
                break
    return tickers
