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
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import httpx
import pandas as pd

from config.settings import DATASTORE_API_BASE_URL

logger = logging.getLogger(__name__)

# A 503 from the API means a transient DuckDB write-lock conflict
# (datastore/api/main.py's duckdb_lock_conflict_handler) — e.g. the
# ingestion scheduler holding a long-lived connection open. Caught live:
# a multi-hour backtest queue died outright on the FIRST such response,
# with no retry at all, even though the conflict was gone moments later.
# Retried here rather than left to blow up the whole caller.
_RETRYABLE_STATUS_CODES = {503}
_RETRY_ATTEMPTS = 6
_RETRY_BASE_DELAY_S = 2.0


def _get_with_retry(client: httpx.Client, url: str, params: Optional[Dict[str, Any]] = None) -> httpx.Response:
    """GET with backoff retry on a transient 503 (see module docstring
    above _RETRYABLE_STATUS_CODES). Does NOT call raise_for_status() —
    callers do that themselves on the final response, same as before."""
    last_response: Optional[httpx.Response] = None
    for attempt in range(_RETRY_ATTEMPTS):
        response = client.get(url, params=params)
        if response.status_code not in _RETRYABLE_STATUS_CODES:
            return response
        last_response = response
        if attempt == _RETRY_ATTEMPTS - 1:
            break
        delay = _RETRY_BASE_DELAY_S * (2**attempt)
        logger.warning(
            f"DataStoreClient: {response.status_code} from {url} (attempt {attempt + 1}/{_RETRY_ATTEMPTS}) "
            f"— retrying in {delay:.1f}s"
        )
        time.sleep(delay)
    return last_response  # exhausted retries — caller's raise_for_status() surfaces it


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

    def get_ohlcv_bulk(self, from_date: datetime, to_date: datetime) -> "pd.DataFrame":
        """
        Fetch OHLCV for ALL tickers in [from_date, to_date] in a single HTTP call.

        Replaces the per-ticker get_ohlcv() loop in _fetch_ohlcv_panel / step_compute_features.
        Hits GET /api/v1/ohlcv/_bulk which issues one DuckDB query instead of 500.

        Parameters
        ----------
        from_date, to_date : datetime
            Inclusive date range.

        Returns
        -------
        pd.DataFrame
            Columns: date (datetime64), ticker, open, high, low, close, volume,
            delivery_pct, adj_factor. Empty DataFrame with those columns if no data.

        Raises
        ------
        httpx.HTTPStatusError, httpx.RequestError
        """
        import json

        import pandas as pd

        params: Dict[str, Any] = {
            "from": from_date.date().isoformat(),
            "to": to_date.date().isoformat(),
        }
        url = f"{self._base_url}/api/v1/ohlcv/_bulk"
        # 2026-07-26: bumped 120s -> 300s (backtest-reviewer sign-off) — the
        # orchestrator backtest queue now calls this for a full ~2300-ticker,
        # 10-year universe pull (previously a per-ticker loop, see
        # run_orchestrator_backtest.py::_fetch_real_ohlcv), a materially
        # larger single payload than matrix_builder's existing call sites.
        with httpx.Client(timeout=300.0) as hclient:
            response = _get_with_retry(hclient, url, params=params)
        response.raise_for_status()

        payload = json.loads(response.text)
        if isinstance(payload, dict):
            payload = payload.get("data", payload)

        if not payload:
            return pd.DataFrame(
                columns=["date", "ticker", "open", "high", "low", "close", "volume", "delivery_pct", "adj_factor"]
            )

        if isinstance(payload, list):
            df = pd.DataFrame(payload)
        else:
            df = pd.DataFrame([payload])

        if df.empty:
            return pd.DataFrame(
                columns=["date", "ticker", "open", "high", "low", "close", "volume", "delivery_pct", "adj_factor"]
            )
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        return df

    def get_index_ohlcv(self, index_name: str, from_date: datetime, to_date: datetime) -> "pd.DataFrame":
        """
        Fetch real NSE index OHLC (index_ohlcv table,
        ingestion/scrapers/nse_indices.py) for one tracked index name over
        [from_date, to_date] — e.g. index_name="Nifty 500" for ML17a's real
        benchmark equity curve (backtest/engine.py).

        Parameters
        ----------
        index_name : str
            Must match ingestion/scrapers/nse_indices.py's TRACKED_INDICES
            exactly (e.g. "Nifty 500", "Nifty IT").
        from_date, to_date : datetime
            Inclusive date range.

        Returns
        -------
        pd.DataFrame
            Columns: date (datetime64), index_name, open, high, low, close,
            volume. Empty DataFrame with those columns if no data.

        Raises
        ------
        httpx.HTTPStatusError, httpx.RequestError
        """
        import json
        from urllib.parse import quote

        import pandas as pd

        params: Dict[str, Any] = {
            "from": from_date.date().isoformat(),
            "to": to_date.date().isoformat(),
        }
        # index_name can contain spaces/"&" (e.g. "Nifty Oil & Gas") — must
        # be percent-encoded since it's embedded directly in the URL path,
        # not passed as a query param.
        url = f"{self._base_url}/api/v1/ohlcv/index/{quote(index_name, safe='')}"
        with httpx.Client(timeout=60.0) as hclient:
            response = _get_with_retry(hclient, url, params=params)
        response.raise_for_status()

        payload = json.loads(response.text)
        columns = ["date", "index_name", "open", "high", "low", "close", "volume"]
        if not payload:
            return pd.DataFrame(columns=columns)

        df = pd.DataFrame(payload)
        if df.empty:
            return pd.DataFrame(columns=columns)
        df["date"] = pd.to_datetime(df["date"])
        return df

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

    def get_listing_dates(self) -> Dict[str, datetime]:
        """
        {ticker: listing_date} for every ticker with a real stock_master.listing_date,
        one bulk HTTP call (GET /stock-master/listing-dates — system.router has
        no /api/v1 prefix, same as /health) — see datastore/api/routers/system.py's
        get_listing_dates for the source.

        Returns
        -------
        dict
            ticker -> datetime (parsed from the API's ISO date strings).

        Raises
        ------
        httpx.HTTPStatusError, httpx.RequestError
        """
        raw = self._get("/stock-master/listing-dates")
        return {ticker: datetime.fromisoformat(date_str) for ticker, date_str in raw.items()}

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
        # Use a 5-year lookback on quarter_end_date so all recent quarters
        # are considered; as_of is the actual PIT gate (announcement_date <=
        # as_of enforced server-side). Passing start_date == end_date == as_of
        # would only match a quarter whose quarter_end_date equals as_of exactly,
        # which is almost never true.
        lookback_start = (as_of - timedelta(days=5 * 365)).isoformat()
        params = {
            "start_date": lookback_start,
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

    def get_fundamentals_history(
        self,
        ticker: str,
        as_of: datetime,
        lookback_years: int = 4,
    ) -> List[Dict[str, Any]]:
        """
        Fetch PIT-eligible quarterly fundamentals history for a ticker.

        Parameters
        ----------
        ticker : str
            Stock ticker.
        as_of : datetime
            PIT reference date — only rows with announcement_date <= as_of
            are returned (SPEC-PIPE-003).
        lookback_years : int
            How many years of quarter_end_date history to request — needs
            to cover at least 5 quarters for YoY growth and ~13 for the
            3-year revenue CAGR feature (features/fundamental.py).

        Returns
        -------
        list of dict
            Rows from GET /api/v1/fundamentals/{ticker}, ascending by
            announcement_date, already PIT-filtered server-side.

        Spec References
        ----------------
        SPEC-DS-003, SPEC-PIPE-003 (CRITICAL): PIT enforced server-side via
        announcement_date — this client never reads quarter_end_date as a
        substitute join key.

        PIT Assumptions
        ----------------
        Server-side filtering only; no local date logic here.

        Raises
        ------
        httpx.HTTPStatusError
            If the API returns a non-2xx response.
        httpx.RequestError
            If the request could not be sent.
        """
        start_date = as_of - timedelta(days=365 * lookback_years)
        params = {
            "start_date": start_date.date().isoformat(),
            "end_date": as_of.date().isoformat(),
            "as_of": as_of.date().isoformat(),
        }
        response = self._get(f"/api/v1/fundamentals/{ticker}", params=params)
        return response.get("data", [])

    def get_fundamentals_history_bulk(
        self,
        tickers: List[str],
        as_of: datetime,
        lookback_years: int = 4,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Same as get_fundamentals_history, for many tickers in one request —
        one DuckDB round trip instead of one per ticker (see
        features/backfill_cache.py's BackfillDataCache, which uses this to
        preload the whole universe instead of threading thousands of
        individual get_fundamentals_history calls).

        Returns
        -------
        dict
            ticker -> list of rows (same shape as get_fundamentals_history's
            return value), one entry per requested ticker (empty list if
            that ticker had no eligible rows).
        """
        start_date = as_of - timedelta(days=365 * lookback_years)
        params = {
            "tickers": tickers,
            "start_date": start_date.date().isoformat(),
            "end_date": as_of.date().isoformat(),
            "as_of": as_of.date().isoformat(),
        }
        response = self._get("/api/v1/fundamentals/bulk", params=params)
        return response.get("data", {})

    def write_fundamentals(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Upsert one quarterly fundamentals row via POST /api/v1/fundamentals/write.

        Parameters
        ----------
        record : dict
            Must match datastore.api.schemas.FundamentalsWrite's fields
            (ticker, fiscal_year, quarter, quarter_end_date,
            announcement_date, plus the financial-ratio columns).

        Returns
        -------
        dict
            Write confirmation from the API.

        Spec References
        ----------------
        SPEC-DS-004: upsert semantics — same (ticker, fiscal_year, quarter)
        replaces, never duplicates.
        SPEC-PIPE-003 (CRITICAL): ingestion writes flow through this API
        endpoint, never a direct DuckDB INSERT (P2.1 build prompt's
        explicit instruction, unlike ingestion/backfill_runner.py's direct
        DuckDB writes — see that module's docstring for the contrasting
        precedent).

        Raises
        ------
        httpx.HTTPStatusError
            If the API returns a non-2xx response (e.g. 400 on a
            SPEC-PIPE-003 violation: announcement_date <= quarter_end_date).
        httpx.RequestError
            If the request could not be sent.
        """
        return self._post("/api/v1/fundamentals/write", record)

    def write_fundamentals_batch(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Upsert many quarterly fundamentals rows in ONE request via
        POST /api/v1/fundamentals/write_batch — A35's "client-side
        batching" fix (see FeatureBacklog.md's A35 entry): lets a batch
        scraper like ingestion/scrapers/screener.py::batch_export
        accumulate records in memory and do one bulk upsert instead of one
        HTTP POST + one DuckDB write-lock acquisition per ticker.

        Parameters
        ----------
        records : list of dict
            Each must match datastore.api.schemas.FundamentalsWrite's
            fields, same shape as write_fundamentals's single-row payload.

        Returns
        -------
        dict
            Write confirmation from the API (count written).

        Spec References
        ----------------
        SPEC-DS-004, SPEC-PIPE-003 (CRITICAL) — same as write_fundamentals;
        batching is purely a transport/transaction optimization, not a
        semantics change.

        Raises
        ------
        httpx.HTTPStatusError
            If the API returns a non-2xx response.
        httpx.RequestError
            If the request could not be sent.
        """
        return self._post("/api/v1/fundamentals/write_batch", {"records": records})

    def get_shareholding_history(
        self,
        ticker: str,
        as_of: datetime,
        lookback_years: int = 2,
    ) -> List[Dict[str, Any]]:
        """
        Fetch PIT-eligible quarterly shareholding history for a ticker.

        Parameters
        ----------
        ticker : str
            Stock ticker.
        as_of : datetime
            PIT reference date — only rows with filing_date <= as_of are
            returned (SPEC-PIPE-003).
        lookback_years : int
            How many years of quarter_end_date history to request — needs
            to cover at least 5 quarters for features/governance.py's QoQ
            change features.

        Returns
        -------
        list of dict
            Rows from GET /api/v1/shareholding/{ticker}, ascending by
            filing_date, already PIT-filtered server-side.

        Spec References
        ----------------
        SPEC-DS-003, SPEC-PIPE-003 (CRITICAL): PIT key is filing_date,
        NEVER quarter_end_date.

        PIT Assumptions
        ----------------
        Server-side filtering only; no local date logic here.

        Raises
        ------
        httpx.HTTPStatusError
            If the API returns a non-2xx response.
        httpx.RequestError
            If the request could not be sent.
        """
        start_date = as_of - timedelta(days=365 * lookback_years)
        params = {
            "start_date": start_date.date().isoformat(),
            "end_date": as_of.date().isoformat(),
            "as_of": as_of.date().isoformat(),
        }
        response = self._get(f"/api/v1/shareholding/{ticker}", params=params)
        return response.get("data", [])

    def get_shareholding_history_bulk(
        self,
        tickers: List[str],
        as_of: datetime,
        lookback_years: int = 2,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Same as get_shareholding_history, for many tickers in one request —
        see get_fundamentals_history_bulk's docstring for the rationale.

        Returns
        -------
        dict
            ticker -> list of rows, one entry per requested ticker (empty
            list if that ticker had no eligible rows).
        """
        start_date = as_of - timedelta(days=365 * lookback_years)
        params = {
            "tickers": tickers,
            "start_date": start_date.date().isoformat(),
            "end_date": as_of.date().isoformat(),
            "as_of": as_of.date().isoformat(),
        }
        response = self._get("/api/v1/shareholding/bulk", params=params)
        return response.get("data", {})

    def write_shareholding(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Upsert one quarterly shareholding row via POST /api/v1/shareholding/write.

        Parameters
        ----------
        record : dict
            Must match datastore.api.schemas.ShareholdingWrite's fields
            (ticker, quarter_end_date, filing_date, plus the
            promoter/FII/DII/MF/retail percentage columns).

        Returns
        -------
        dict
            Write confirmation from the API.

        Spec References
        ----------------
        SPEC-DS-004: upsert semantics. SPEC-PIPE-003 (CRITICAL): filing_date
        is the PIT key, never quarter_end_date.

        Raises
        ------
        httpx.HTTPStatusError
            If the API returns a non-2xx response (e.g. 400 on a
            SPEC-PIPE-003 violation: filing_date <= quarter_end_date).
        httpx.RequestError
            If the request could not be sent.
        """
        return self._post("/api/v1/shareholding/write", record)

    def get_corporate_actions(
        self,
        ticker: str,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch corporate actions for a ticker over an optional [from, to] ex_date window.

        Parameters
        ----------
        ticker : str
        from_date : datetime, optional
            Inclusive ex_date range start. Omit for "everything up to to_date".
        to_date : datetime, optional
            Inclusive ex_date range end. Omit for "everything from from_date".

        Returns
        -------
        list of dict
            Rows from GET /api/v1/corporate_actions/{ticker}, ascending by ex_date.

        Spec References
        ----------------
        SPEC-DS-001, SPEC-DS-002, SPEC-PIPE-002.

        PIT Assumptions
        ----------------
        No server-side PIT filtering (corporate actions are PITRule.NONE
        at this endpoint) — a caller needing "known as of X" semantics
        filters by announcement_date itself, same as
        features/corporate_action_features.py does.

        Raises
        ------
        httpx.HTTPStatusError
            If the API returns a non-2xx response.
        httpx.RequestError
            If the request could not be sent.
        """
        params: Dict[str, Any] = {}
        if from_date:
            params["from"] = from_date.date().isoformat()
        if to_date:
            params["to"] = to_date.date().isoformat()
        response = self._get(f"/api/v1/corporate_actions/{ticker}", params=params)
        return response.get("data", [])

    def get_corporate_actions_bulk(
        self,
        tickers: List[str],
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Same as get_corporate_actions, for many tickers in one request —
        see get_fundamentals_history_bulk's docstring for the rationale.

        Returns
        -------
        dict
            ticker -> list of rows, one entry per requested ticker (empty
            list if that ticker had no rows).
        """
        params: Dict[str, Any] = {"tickers": tickers}
        if from_date:
            params["from"] = from_date.date().isoformat()
        if to_date:
            params["to"] = to_date.date().isoformat()
        response = self._get("/api/v1/corporate_actions/bulk", params=params)
        return response.get("data", {})

    def get_fno_chain(
        self,
        ticker: str,
        from_date: datetime,
        to_date: datetime,
    ) -> List[Dict[str, Any]]:
        """
        Fetch every F&O contract row (futures + options, all expiries/strikes)
        for a ticker over [from_date, to_date].

        Parameters
        ----------
        ticker : str
        from_date : datetime
            Inclusive trade_date range start.
        to_date : datetime
            Inclusive trade_date range end.

        Returns
        -------
        list of dict
            Raw F&O rows from GET /api/v1/fno/{ticker}, ascending by
            trade_date, expiry, strike. Empty list if the ticker has no
            F&O contracts in this window — features/fno_features.py
            treats this as "not F&O eligible", not an error.

        Spec References
        ----------------
        SPEC-DS-001, SPEC-DS-002, SPEC-PIPE-001.

        PIT Assumptions
        ----------------
        None — F&O bhavcopy is PITRule.NONE (same-day knowable, like OHLCV).

        Raises
        ------
        httpx.HTTPStatusError
            If the API returns a non-2xx response.
        httpx.RequestError
            If the request could not be sent.
        """
        params: Dict[str, Any] = {
            "from": from_date.date().isoformat(),
            "to": to_date.date().isoformat(),
        }
        response = self._get(f"/api/v1/fno/{ticker}", params=params)
        return response.get("data", [])

    # ===== Phase 2.6 additions =====
    def get_fundamentals_history_by_quarters(
        self, ticker: str, quarters: int = 8, as_of: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch the most recent `quarters` quarterly fundamentals rows for a
        ticker via GET /api/v1/fundamentals/{ticker}/history.

        Parameters
        ----------
        ticker : str
        quarters : int
            Number of most recent quarters to return (count-based, not a
            calendar-year lookback).
        as_of : datetime, optional
            PIT reference (default: now, server-side).

        Returns
        -------
        list of dict
            Ascending by announcement_date, already PIT-filtered server-side.

        Spec References
        ----------------
        SPEC-DS-003, SPEC-PIPE-003 (CRITICAL).

        PIT Assumptions
        ----------------
        Server-side filtering only; no local date logic here.

        Raises
        ------
        httpx.HTTPStatusError
            If the API returns a non-2xx response.
        httpx.RequestError
            If the request could not be sent.
        """
        params: Dict[str, Any] = {"quarters": quarters}
        if as_of:
            params["as_of"] = as_of.date().isoformat()
        response = self._get(f"/api/v1/fundamentals/{ticker}/history", params=params)
        return response.get("data", [])

    def get_governance(self, ticker: str, as_of: datetime) -> List[Dict[str, Any]]:
        """
        Fetch governance (shareholding + superstar-investor tracking)
        history for a ticker via GET /api/v1/governance/{ticker}.

        Parameters
        ----------
        ticker : str
        as_of : datetime
            PIT reference — only rows with filing_date <= as_of are
            returned (SPEC-PIPE-003).

        Returns
        -------
        list of dict
            Rows from GET /api/v1/governance/{ticker}, ascending by
            filing_date, already PIT-filtered server-side. `shareholding`
            IS this project's governance store (12_platform_architecture.md
            line 320) — same underlying table get_shareholding_history()
            reads, exposed under the governance path for callers that want
            superstar_flag/superstar_change alongside promoter/FII/DII.

        Spec References
        ----------------
        SPEC-DS-003, SPEC-PIPE-003 (CRITICAL).

        Raises
        ------
        httpx.HTTPStatusError
            If the API returns a non-2xx response.
        httpx.RequestError
            If the request could not be sent.
        """
        params = {
            "start_date": date(1990, 1, 1).isoformat(),
            "end_date": as_of.date().isoformat(),
            "as_of": as_of.date().isoformat(),
        }
        response = self._get(f"/api/v1/governance/{ticker}", params=params)
        return response.get("data", [])

    def get_forensic_score(self, ticker: str, as_of: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
        """
        Fetch the most recent forensic scoring row for a ticker via
        GET /api/v1/signals/ml/forensic/{ticker} (M-09/M-10).

        Parameters
        ----------
        ticker : str
        as_of : datetime, optional
            Most recent row at or before this date (default: now, server-side).

        Returns
        -------
        dict or None
            None if no forensic row exists yet for this ticker (e.g.
            systems/ml_signal_engine/inference/score_forensic.py has never
            run) — not an error.

        Spec References
        ----------------
        SPEC-MODEL-009, SPEC-MODEL-010.

        Raises
        ------
        httpx.HTTPStatusError
            If the API returns a non-2xx response.
        httpx.RequestError
            If the request could not be sent.
        """
        params: Dict[str, Any] = {}
        if as_of:
            params["as_of"] = as_of.date().isoformat()
        return self._get(f"/api/v1/signals/ml/forensic/{ticker}", params=params)

    def write_forensic_score(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Upsert one forensic scoring row via POST /api/v1/signals/ml/forensic/write.

        Parameters
        ----------
        record : dict
            Must match datastore.api.schemas.ForensicWrite's fields.

        Returns
        -------
        dict
            Write confirmation from the API.

        Spec References
        ----------------
        SPEC-DS-004: upsert semantics.

        Raises
        ------
        httpx.HTTPStatusError
            If the API returns a non-2xx response.
        httpx.RequestError
            If the request could not be sent.
        """
        return self._post("/api/v1/signals/ml/forensic/write", record)

    def get_multibagger_score(self, ticker: str, as_of: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
        """
        Fetch the most recent multibagger scoring row for a ticker via
        GET /api/v1/signals/ml/multibagger/{ticker} (M-08).

        Parameters
        ----------
        ticker : str
        as_of : datetime, optional
            Most recent row at or before this date (default: now, server-side).

        Returns
        -------
        dict or None
            None if no multibagger row exists yet for this ticker.

        Spec References
        ----------------
        SPEC-MODEL-001, SPEC-UI-003.

        Raises
        ------
        httpx.HTTPStatusError
            If the API returns a non-2xx response.
        httpx.RequestError
            If the request could not be sent.
        """
        params: Dict[str, Any] = {}
        if as_of:
            params["as_of"] = as_of.date().isoformat()
        return self._get(f"/api/v1/signals/ml/multibagger/{ticker}", params=params)

    def write_multibagger_score(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Upsert one multibagger scoring row via POST /api/v1/signals/ml/multibagger/write.

        Parameters
        ----------
        record : dict
            Must match datastore.api.schemas.MultibaggerWrite's fields.

        Returns
        -------
        dict
            Write confirmation from the API.

        Spec References
        ----------------
        SPEC-DS-004: upsert semantics.

        Raises
        ------
        httpx.HTTPStatusError
            If the API returns a non-2xx response.
        httpx.RequestError
            If the request could not be sent.
        """
        return self._post("/api/v1/signals/ml/multibagger/write", record)

    def get_multibagger_watchlist(self) -> Dict[str, Any]:
        """
        Fetch the current top-20 multibagger watchlist via
        GET /api/v1/watchlist/current (SPEC-UI-003).

        Returns
        -------
        dict
            {"tickers": [...], "implemented": bool, "notes": str} — see
            datastore.api.schemas.WatchlistResponse. `implemented` is
            False (tickers == []) if score_multibagger.py has never run.

        Spec References
        ----------------
        SPEC-UI-003.

        Raises
        ------
        httpx.HTTPStatusError
            If the API returns a non-2xx response.
        httpx.RequestError
            If the request could not be sent.
        """
        return self._get("/api/v1/watchlist/current")

    def get_forensic_summary(self) -> Dict[str, Any]:
        """
        Fetch universe-wide forensic flag counts for the most recent scored date
        via GET /api/v1/signals/ml/forensic/summary (M-09/M-10, dashboard).

        Returns
        -------
        dict
            {"as_of_date": str, "red_count": int, "amber_count": int,
             "green_count": int, "total_scored": int, "available": bool}.
            `available` is False when ml_forensic has never been written.

        Spec References
        ----------------
        SPEC-MODEL-009, SPEC-MODEL-010.

        Raises
        ------
        httpx.HTTPStatusError
            If the API returns a non-2xx response.
        httpx.RequestError
            If the request could not be sent.
        """
        return self._get("/api/v1/signals/ml/forensic/summary")

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
            response = _get_with_retry(client, url, params=params)
            response.raise_for_status()
            return response.json()

    def _post(self, path: str, body: Dict[str, Any]) -> Any:
        """
        Issue a POST request against the DataStore API and return parsed JSON.

        Parameters
        ----------
        path : str
            URL path, relative to `base_url` (e.g. '/api/v1/fundamentals/write').
        body : dict
            JSON request body. Any `datetime`/`date` values are converted
            to ISO-8601 strings first — Python's stdlib JSON encoder (which
            httpx uses internally) cannot serialize them directly.

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
        payload = {k: (v.isoformat() if isinstance(v, (datetime, date)) else v) for k, v in body.items()}
        url = f"{self._base_url}{path}"
        with httpx.Client(timeout=self._timeout) as client:
            response = client.post(url, json=payload)
            response.raise_for_status()
            return response.json()
