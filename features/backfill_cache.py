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
from typing import TYPE_CHECKING, Any, Dict, List

import pandas as pd

if TYPE_CHECKING:  # import-time cycle: datastore.client is a runtime-only dep here
    from datastore.client import DataStoreClient

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

    def __init__(self, client: "DataStoreClient", tickers: List[str], to_date: datetime, n_workers: int = 1) -> None:
        """
        n_workers is accepted for backward compatibility with existing call
        sites but no longer used: the pre-load used to thread ~3 requests
        per ticker (fundamentals/shareholding/corp_actions, each opening its
        own DuckDB connection) across n_workers threads. That's now 3 bulk
        requests total regardless of universe size (GET .../bulk — see
        datastore/api/routers/fundamentals.py, shareholding.py,
        corporate_actions.py), so there's nothing left to parallelize.
        """
        n = len(tickers)
        logger.info(
            "BackfillDataCache: pre-loading %d tickers up to %s (bulk) ...",
            n, to_date.date().isoformat() if hasattr(to_date, 'date') else str(to_date),
        )

        try:
            fundamentals_bulk = client.get_fundamentals_history_bulk(
                tickers, to_date, lookback_years=_MAX_LOOKBACK_YEARS
            )
        except Exception as exc:
            logger.warning("BackfillDataCache: bulk fundamentals fetch failed (%s) — all tickers empty", exc)
            fundamentals_bulk = {}
        try:
            shareholding_bulk = client.get_shareholding_history_bulk(
                tickers, to_date, lookback_years=_MAX_LOOKBACK_YEARS
            )
        except Exception as exc:
            logger.warning("BackfillDataCache: bulk shareholding fetch failed (%s) — all tickers empty", exc)
            shareholding_bulk = {}
        try:
            corp_actions_bulk = client.get_corporate_actions_bulk(tickers)
        except Exception as exc:
            logger.warning("BackfillDataCache: bulk corp_actions fetch failed (%s) — all tickers empty", exc)
            corp_actions_bulk = {}

        self._fundamentals: Dict[str, List[Dict[str, Any]]] = {t: fundamentals_bulk.get(t, []) for t in tickers}
        self._shareholding: Dict[str, List[Dict[str, Any]]] = {t: shareholding_bulk.get(t, []) for t in tickers}
        self._corp_actions: Dict[str, List[Dict[str, Any]]] = {t: corp_actions_bulk.get(t, []) for t in tickers}

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
