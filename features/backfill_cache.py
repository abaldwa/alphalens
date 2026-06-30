"""
features/backfill_cache.py

Provides BackfillDataCache — a one-time pre-loader for per-ticker data used
during historical feature backfills.

Problem it solves
-----------------
The standard build_feature_matrix path calls the DataStore API per-ticker AND
per-date for fundamentals, shareholding, corporate_actions, and F&O.  Over a
4780-date backfill of 500 tickers this produces ~15–20 million API calls.

With BackfillDataCache, each of these four data types is fetched ONCE per
ticker (2 000 calls total).  For every subsequent date the cache serves
PIT-filtered in-memory slices — no additional I/O.

Usage (feature_backfill.py)
---------------------------
    from features.backfill_cache import BackfillDataCache
    from datastore.client import DataStoreClient

    client = DataStoreClient()
    cache = BackfillDataCache(client, tickers, to_date=date.today())

    # then for each date:
    build_feature_matrix(date_str, tickers, client, data_cache=cache)
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# How far back to load fundamentals/shareholding/corp-actions at pre-load time.
# 25 years safely exceeds the longest lookback (4 years) even for dates back
# to 2007 (2007 - 4 = 2003 < 2001 = today - 25 years).
_MAX_LOOKBACK_YEARS = 25

# F&O data has no meaningful pre-load because:
#   * 250 eligible tickers × 20 years × ~50 contracts/day = 90 M+ rows
#   * F&O eligibility changes over time; old-date data is sparse anyway
# F&O calls remain per-date per-ticker (unchanged from baseline).


class BackfillDataCache:
    """
    Pre-loads fundamentals, shareholding, and corporate-action data for all
    tickers once, then serves PIT-filtered slices for each historical date
    without any further API calls.

    Parameters
    ----------
    client : DataStoreClient
        Live client used only during the pre-load phase.
    tickers : list of str
        Universe to pre-load (e.g. config.universe.get_tickers()).
    to_date : datetime
        Latest date in the backfill — pre-load fetches history up to here.
    """

    def __init__(self, client, tickers: List[str], to_date: datetime) -> None:
        self._fundamentals: Dict[str, List[Dict[str, Any]]] = {}
        self._shareholding: Dict[str, List[Dict[str, Any]]] = {}
        self._corp_actions: Dict[str, List[Dict[str, Any]]] = {}

        n = len(tickers)
        logger.info(
            "BackfillDataCache: pre-loading %d tickers up to %s ...",
            n, to_date.date().isoformat() if hasattr(to_date, 'date') else str(to_date),
        )

        for i, ticker in enumerate(tickers):
            if i % 100 == 0:
                logger.info("  BackfillDataCache pre-load %d/%d", i, n)

            try:
                self._fundamentals[ticker] = client.get_fundamentals_history(
                    ticker, to_date, lookback_years=_MAX_LOOKBACK_YEARS
                )
            except Exception as exc:
                logger.debug("BackfillDataCache fundamentals %s: %s", ticker, exc)
                self._fundamentals[ticker] = []

            try:
                self._shareholding[ticker] = client.get_shareholding_history(
                    ticker, to_date, lookback_years=_MAX_LOOKBACK_YEARS
                )
            except Exception as exc:
                logger.debug("BackfillDataCache shareholding %s: %s", ticker, exc)
                self._shareholding[ticker] = []

            try:
                self._corp_actions[ticker] = client.get_corporate_actions(ticker)
            except Exception as exc:
                logger.debug("BackfillDataCache corp_actions %s: %s", ticker, exc)
                self._corp_actions[ticker] = []

        logger.info("BackfillDataCache: pre-load complete for %d tickers.", n)

    # ── PIT-filtered accessors ─────────────────────────────────────────────────

    def get_fundamentals(self, ticker: str, as_of: datetime) -> List[Dict[str, Any]]:
        """
        Return fundamentals rows with announcement_date <= as_of.

        Mirrors the server-side PIT filter of get_fundamentals_history()
        (SPEC-PIPE-003) but applied in-memory instead of via HTTP.
        """
        rows = self._fundamentals.get(ticker, [])
        if not rows:
            return []
        as_of_ts = pd.Timestamp(as_of)
        return [
            r for r in rows
            if pd.notna(r.get("announcement_date"))
            and pd.Timestamp(r["announcement_date"]) <= as_of_ts
        ]

    def get_shareholding(self, ticker: str, as_of: datetime) -> List[Dict[str, Any]]:
        """
        Return shareholding rows with filing_date <= as_of.

        Mirrors the server-side PIT filter of get_shareholding_history()
        (SPEC-PIPE-003).
        """
        rows = self._shareholding.get(ticker, [])
        if not rows:
            return []
        as_of_ts = pd.Timestamp(as_of)
        return [
            r for r in rows
            if pd.notna(r.get("filing_date"))
            and pd.Timestamp(r["filing_date"]) <= as_of_ts
        ]

    def get_corp_actions(self, ticker: str) -> List[Dict[str, Any]]:
        """
        Return all corporate-action rows for a ticker.

        PIT filtering (announcement_date <= as_of) is applied inside
        compute_corporate_action_features via _pit_filter_actions, so the
        full pre-loaded list is returned here unchanged.
        """
        return self._corp_actions.get(ticker, [])
