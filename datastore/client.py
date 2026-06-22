"""
datastore/client.py

Phase: 0.2 (DataStore Schema & API Shell)
Specs: SPEC-DS-001, SPEC-DS-002, SPEC-DS-003, SPEC-SOLID-005
Owner: Platform / DataStore
Consumers: systems/ml_signal_engine, systems/technical_analysis,
           systems/damodaran_valuation, systems/fundamental_analysis,
           backtest, dashboard

Thin httpx client over the DataStore FastAPI layer. SPEC-DS-002: consumer
systems call the API exclusively via httpx — never import datastore.api.db
or open a DuckDB/SQLite connection directly. SPEC-SOLID-005 (Dependency
Inversion): consumer systems depend on this class, not on the API's
underlying storage engine, so the engine can change without touching
consumer code.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx

from config.settings import DATASTORE_API_BASE_URL

logger = logging.getLogger(__name__)


class DataStoreClient:
    """
    httpx client for the DataStore API (SPEC-DS-002).

    No method on this class touches DuckDB, SQLite, or Parquet directly —
    every call is an HTTP request. PIT enforcement (SPEC-DS-003) and schema
    validation happen once, centrally, at the API layer; this client never
    re-implements that logic locally.
    """

    def __init__(
        self, base_url: str = DATASTORE_API_BASE_URL, timeout: float = 30.0
    ) -> None:
        """
        Parameters
        ----------
        base_url : str
            DataStore API base URL. Defaults to
            config.settings.DATASTORE_API_BASE_URL (SPEC-QUALITY-003: no
            hardcoded hosts/paths elsewhere in the codebase).
        timeout : float
            Per-request timeout in seconds.

        Spec References
        ----------------
        SPEC-DS-002, SPEC-SOLID-005

        Raises
        ------
        None
        """
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    def get_ohlcv(
        self,
        ticker: str,
        from_date: datetime,
        to_date: datetime,
        as_of: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch adjusted OHLCV rows for a ticker over [from_date, to_date].

        Parameters
        ----------
        ticker : str
            Stock ticker, e.g. 'RELIANCE'.
        from_date : datetime
            Inclusive range start.
        to_date : datetime
            Inclusive range end.
        as_of : datetime, optional
            PIT reference date; the API defaults this to `to_date` when
            omitted.

        Returns
        -------
        list of dict
            Raw OHLCV rows from GET /api/v1/ohlcv/{ticker}.

        Spec References
        ----------------
        SPEC-DS-001: Normalised, corporate-action-adjusted price data.
        SPEC-DS-003: PIT correctness enforced server-side.

        PIT Assumptions
        ----------------
        This client performs no local date filtering — PIT filtering is the
        API's responsibility (datastore/api/pit.py).

        Raises
        ------
        httpx.HTTPStatusError
            If the API returns a non-2xx response.
        httpx.RequestError
            If the request could not be sent (connection failure, timeout).
        """
        # [AS BUILT, P1.7] datastore/api/routers/ohlcv.py's literal contract
        # (build prompt: "GET /api/v1/ohlcv/{ticker}?from=&to=&adjusted=true")
        # uses from/to, not start_date/end_date, and has no as_of param —
        # OHLCV is PITRule.NONE (always same-day knowable), so as_of was
        # never forwarded server-side even before this endpoint moved into
        # routers/. `as_of` stays in this method's signature for call-site
        # stability but is accepted-and-ignored here, same as it always was.
        params: Dict[str, Any] = {
            "from": from_date.date().isoformat(),
            "to": to_date.date().isoformat(),
        }

        response = self._get(f"/api/v1/ohlcv/{ticker}", params=params)
        return response.get("data", [])

    def get_universe_tickers(self, min_rows: int = 0) -> Dict[str, Any]:
        """
        Distinct tickers present in ohlcv_adjusted, with row counts.

        Parameters
        ----------
        min_rows : int
            Only include tickers with at least this many rows.

        Returns
        -------
        dict
            {'tickers': [...], 'row_counts': {ticker: count}} from
            GET /api/v1/ohlcv/_meta/tickers.

        Spec References
        ----------------
        SPEC-DS-002. Broader than config.universe.get_tickers() (which
        applies tier/ADTV/market-cap filters) — "every ticker this
        DataStore has ever observed data for", needed by callers that must
        distinguish the current investable universe from the full
        historical record (e.g. BacktestIntegrityChecker.check_04_survivorship).

        Raises
        ------
        httpx.HTTPStatusError
            If the API returns a non-2xx response.
        httpx.RequestError
            If the request could not be sent.
        """
        return self._get("/api/v1/ohlcv/_meta/tickers", params={"min_rows": min_rows})

    def get_fundamentals_pit(
        self,
        ticker: str,
        as_of: datetime,
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch the latest point-in-time-correct fundamental record for a ticker.

        Parameters
        ----------
        ticker : str
            Stock ticker.
        as_of : datetime
            Reference date. Only fundamentals with announcement_date <=
            as_of are eligible (SPEC-PIPE-003) — never quarter_end_date.

        Returns
        -------
        dict or None
            The most recent PIT-eligible fundamental record, or None if
            none exists yet for this ticker as of `as_of`.

        Spec References
        ----------------
        SPEC-DS-003: PIT enforcement at the API level.
        SPEC-PIPE-003: announcement_date is the PIT key for fundamentals.

        PIT Assumptions
        ----------------
        This client never reads quarter_end_date as a substitute PIT key —
        filtering by announcement_date <= as_of is done server-side only.

        Raises
        ------
        httpx.HTTPStatusError
            If the API returns a non-2xx response.
        httpx.RequestError
            If the request could not be sent.
        """
        params = {
            "start_date": as_of.isoformat(),
            "end_date": as_of.isoformat(),
            "as_of": as_of.isoformat(),
        }
        response = self._get(f"/api/v1/fundamentals/{ticker}", params=params)
        rows = response.get("data", [])
        return rows[-1] if rows else None

    def get_signals(self, ticker: str, date: datetime) -> List[Dict[str, Any]]:
        """
        Fetch all ML signals for a ticker on a specific date.

        Parameters
        ----------
        ticker : str
            Stock ticker.
        date : datetime
            Signal date.

        Returns
        -------
        list of dict
            Signals from GET /api/v1/signals/ml/{ticker}/{date}.

        Spec References
        ----------------
        SPEC-DS-004: Write-back/read-back protocol for system outputs.
        SPEC-DS-005: Cross-system signal fusion — any consumer may read.

        PIT Assumptions
        ----------------
        None — signals are already-computed daily outputs, not subject to
        announcement-date PIT filtering.

        Raises
        ------
        httpx.HTTPStatusError
            If the API returns a non-2xx response.
        httpx.RequestError
            If the request could not be sent.
        """
        date_str = date.strftime("%Y-%m-%d")
        return self._get(f"/api/v1/signals/ml/{ticker}/{date_str}")

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Issue a GET request against the DataStore API and return parsed JSON.

        Parameters
        ----------
        path : str
            URL path, relative to `base_url` (e.g. '/api/v1/ohlcv/RELIANCE').
        params : dict, optional
            Query string parameters.

        Returns
        -------
        Any
            Parsed JSON response body.

        Spec References
        ----------------
        SPEC-DS-002: Sole interface for consumer-system API access.

        Raises
        ------
        httpx.HTTPStatusError
            If the API returns a non-2xx response.
        httpx.RequestError
            If the request could not be sent.
        """
        url = f"{self._base_url}{path}"
        with httpx.Client(timeout=self._timeout) as client:
            response = client.get(url, params=params)
            response.raise_for_status()
            return response.json()
