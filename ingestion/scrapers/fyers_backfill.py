"""
ingestion/scrapers/fyers_backfill.py

Phase: 0.5 (FYERS Historical Backfill)
Specs: SPEC-PIPE-001, SPEC-PIPE-002
Owner: Platform / Ingestion
Consumers: ingestion/backfill_runner

Downloads multi-year daily OHLCV history per ticker from the FYERS API v3
(fyers-apiv3), for the one-time historical backfill that NSE's daily
bhavcopy cannot provide (bhavcopy.py only ever has *today's* data).

NOTE on API_SPEC.md / task wording: "write to DuckDB ohlcv_adjusted table
via DataStore API" (see ingestion/backfill_runner.py) is read here as
"via the DataStore layer", i.e. a direct DuckDB write — the same pattern
already established by bhavcopy.py, macro.py, and price_adjuster.py.
datastore/client.py's DataStoreClient is intentionally read-only (SPEC-DS-002:
"No method on this class touches DuckDB ... every call is an HTTP request"),
used only by *consumer* systems (ml_signal_engine, backtest, dashboard).
Ingestion is explicitly not a consumer in SPEC-DS-002's own consumer list —
it is the writer — so this module (and backfill_runner.py) write to DuckDB
directly, consistent with every other ingestion module in this codebase.

Rate limiting: FYERS allows generous per-second throughput; this module
paces calls with a FYERS_RATE_LIMIT_SLEEP_SECONDS sleep between them.
[2026-08-01] There is deliberately no daily call-count cap — an earlier
FYERS_MAX_CALLS_PER_DAY budget was removed at explicit user request; it
was a project-chosen planning number, not a FYERS-documented rate limit.

OAuth2 login: get_access_token() drives an *interactive* input()-based flow
by default (SPEC-PIPE-001). That blocks forever in any environment where
stdin isn't truly interactive (e.g. a captured IDE output pane) — for those
cases, use the non-interactive two-step CLI instead, which never calls
input():
    python3 -m ingestion.scrapers.fyers_backfill login
    python3 -m ingestion.scrapers.fyers_backfill exchange "<redirected URL or auth_code>"
The second command caches a real access token to disk, which
get_access_token() then picks up automatically (no further login needed
for the rest of that day).
"""

import json
import logging
import sys
import threading
import time
from datetime import date as date_type
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import pandas as pd
from fyers_apiv3 import fyersModel

from config.settings import (
    FYERS_APP_ID,
    FYERS_HISTORY_MAX_DAYS_PER_CALL,
    FYERS_RATE_LIMIT_SLEEP_SECONDS,
    FYERS_RAW_DIR,
    FYERS_REDIRECT_URI,
    FYERS_SECRET_ID,
    FYERS_TOKEN_CACHE_PATH,
)
from config.timezone import now_ist

logger = logging.getLogger(__name__)


class _GlobalRateLimiter:
    """
    Thread-safe minimum-interval gate, shared across every FYERSBackfill
    instance/thread in this process.

    [2026-08-04, live-confirmed] FYERS' rate limit is per-app (account-
    wide), not per-object or per-thread — the OLD per-instance
    `time.sleep(FYERS_RATE_LIMIT_SLEEP_SECONDS)` in _throttle() paces
    each caller independently, which does nothing to stop N concurrent
    threads from each sleeping the same interval and then firing at
    nearly the same moment, bursting well past the true limit. Confirmed
    live: a one-shot burst of 8 concurrent calls succeeded cleanly, but a
    SUSTAINED stream of 6 concurrent workers hit widespread 429s within
    about a minute — consistent with FYERS' documented 200-req/minute cap
    (200/60 ≈ 3.3 req/sec sustained) being the real binding constraint
    for a long-running backfill, not the more forgiving ~10/sec
    instantaneous-burst figure. This class enforces one global minimum
    interval between calls REGARDLESS of how many threads are calling.
    """

    def __init__(self, min_interval_seconds: float) -> None:
        self._min_interval = min_interval_seconds
        self._lock = threading.Lock()
        self._next_allowed_at = 0.0

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait_seconds = max(0.0, self._next_allowed_at - now)
            self._next_allowed_at = max(now, self._next_allowed_at) + self._min_interval
        if wait_seconds > 0:
            time.sleep(wait_seconds)


# One limiter shared by every FYERSBackfill instance in this process —
# see _GlobalRateLimiter's docstring for why this must be global, not
# per-instance.
_RATE_LIMITER = _GlobalRateLimiter(FYERS_RATE_LIMIT_SLEEP_SECONDS)

# Retry budget for a 429 ("request limit reached") response specifically
# — distinct from every other error type, which still fails immediately
# (an invalid symbol will never succeed no matter how many times it's
# retried, so retrying THAT would just waste calls and delay the batch).
_MAX_429_RETRIES = 4
_429_BACKOFF_BASE_SECONDS = 2.0

# [2026-08-06, Issue2] Progress-telemetry cadence for batch_download when
# show_progress=False (the scheduler's path — ~2000 tickers at 0.35s each
# ≈ 12min of otherwise-silent output that reads as a hang). _PROGRESS_LOG_
# INTERVAL_S sets how often a cumulative progress line is emitted; _IDLE_
# WATCHDOG_S is a stall tripwire: if no ticker completes within that window
# (far longer than the rate-limiter's pacing), log a warning so an operator
# can distinguish "slow-but-progressing" from "genuinely stuck".
_PROGRESS_LOG_INTERVAL_S = 20.0
_IDLE_WATCHDOG_S = 180.0

EXCHANGE_SEGMENT_SUFFIX = "-EQ"
EXCHANGE_PREFIX = "NSE:"
RESOLUTION_DAILY = "D"
DATE_FORMAT_EPOCH = "0"  # FYERS history API: "0" = unix epoch, "1" = "yyyy-mm-dd"

# FYERS history response columns, in order: [epoch, open, high, low, close, volume]
CANDLE_COLUMNS = ["epoch", "open", "high", "low", "close", "volume"]
OUTPUT_COLUMNS = ["date", "ticker", "open", "high", "low", "close", "volume"]


class FYERSBackfill:
    """
    Thin wrapper over fyers_apiv3.fyersModel for multi-year OHLCV backfill.

    Spec References
    ----------------
    SPEC-PIPE-001: FYERS API as the historical-backfill OHLCV source.
    """

    def __init__(
        self,
        app_id: Optional[str] = None,
        secret_id: Optional[str] = None,
        redirect_uri: Optional[str] = None,
        access_token: Optional[str] = None,
        token_cache_path: Optional[Path] = None,
        non_interactive: bool = False,
    ) -> None:
        """
        Parameters
        ----------
        app_id : str, optional
            Defaults to config.settings.FYERS_APP_ID.
        secret_id : str, optional
            Defaults to config.settings.FYERS_SECRET_ID.
        redirect_uri : str, optional
            Defaults to config.settings.FYERS_REDIRECT_URI.
        access_token : str, optional
            If provided, skips both the cache and the OAuth flow entirely —
            used by tests and by callers that already hold a valid token.
        token_cache_path : Path, optional
            Defaults to config.settings.FYERS_TOKEN_CACHE_PATH.
        non_interactive : bool, optional
            2026-07-30: if True, get_access_token() raises RuntimeError
            instead of falling through to _run_oauth_flow() when no valid
            token is available from any non-interactive source (explicit
            access_token, cache, or FYERS_ACCESS_TOKEN env var). Every
            unattended caller (daily pipeline, A20 checks, cron scripts)
            must set this — _run_oauth_flow() calls input() and hangs
            forever with no connected stdin. A pre-check like "is there a
            cached token" is not sufficient on its own:
            get_access_token() re-derives the token from scratch on every
            call (its own cache lookup + validation, independent of any
            caller-side guard), so a transient validation blip or a token
            expiring mid-run can still fall through to the interactive
            flow even when a caller checked first. This flag is the only
            thing that closes that hole for every call site, not just the
            ones that remember to guard.

        Spec References
        ----------------
        SPEC-PIPE-001

        Raises
        ------
        None — credential validation is deferred to get_access_token(),
        so constructing this object never requires network access.
        """
        self._app_id = app_id or FYERS_APP_ID
        self._secret_id = secret_id or FYERS_SECRET_ID
        self._redirect_uri = redirect_uri or FYERS_REDIRECT_URI
        self._token_cache_path = token_cache_path or FYERS_TOKEN_CACHE_PATH
        self._access_token: Optional[str] = access_token
        self._non_interactive = non_interactive
        self._client: Optional[fyersModel.FyersModel] = None
        # [2026-08-04] Guards _get_client()'s lazy construction (and the
        # get_access_token() call chain it triggers) against a real,
        # live-confirmed race: scripts/fyers_staged_backfill.py's
        # parallel ticker fetch shares ONE FYERSBackfill instance across
        # worker threads. Without this lock, multiple threads independently
        # see self._client is None at once and each independently call
        # get_access_token() -> _validate_token() -> a duplicate
        # get_profile() probe per thread. One of those redundant probes
        # got 429-rate-limited in testing, which _validate_token then
        # (before its own 2026-08-04 fix) misread as "token invalid" and
        # deleted the still-good cached token file out from under every
        # other thread. The lock makes only the FIRST thread do the
        # token dance; the rest block briefly and then reuse self._client.
        self._client_lock = threading.Lock()

    @staticmethod
    def _extract_auth_code(raw_input_value: str) -> str:
        """Accept either a bare auth_code or a full redirected URL containing one."""
        if "auth_code=" not in raw_input_value:
            if raw_input_value.startswith(("http://", "https://")):
                # A bare redirect URL (e.g. 'https://127.0.0.1/') with no query
                # string at all is a common mistake — the browser's address bar
                # after the OAuth redirect has 'auth_code=...&state=...' appended;
                # failing clearly here beats sending the bare URL to FYERS as if
                # it were the code itself and getting back a cryptic
                # "{'code': -437, 'message': 'invalid auth code'}" instead.
                raise RuntimeError(
                    f"'{raw_input_value}' looks like a URL but has no "
                    "'?auth_code=...' query string. Copy the FULL URL your "
                    "browser was redirected to after logging in — it looks like "
                    "'https://127.0.0.1/?auth_code=XXXXXXXX&state=None' — not "
                    "just the base redirect URL."
                )
            return raw_input_value
        query = parse_qs(urlparse(raw_input_value).query)
        codes = query.get("auth_code")
        if not codes:
            raise RuntimeError(f"Could not find auth_code in: {raw_input_value}")
        return codes[0]

    def get_access_token(self) -> str:
        """
        Return a valid FYERS access token, obtaining one if necessary.

        Resolution order: (1) token passed to __init__ (trusted as-is — an
        explicit caller override), (2) a same-day cached token on disk,
        (3) FYERS_ACCESS_TOKEN from the environment, (4) an interactive
        OAuth2 authorization-code flow. Sources (2) and (3) are silently
        *picked up* rather than explicitly supplied by the caller, so each
        is validated with a lightweight live API call before being
        trusted — catching both an unedited `.env` placeholder and a
        genuinely expired token (FYERS tokens expire daily) up front,
        rather than discovering it only after every ticker in a batch has
        already failed with the same "Could not authenticate" error.

        Parameters
        ----------
        None

        Returns
        -------
        str
            FYERS access token.

        Spec References
        ----------------
        SPEC-PIPE-001: "get_access_token() — OAuth2 flow using FYERS_APP_ID
        and FYERS_SECRET_ID from .env."

        PIT Assumptions
        ----------------
        None.

        Raises
        ------
        RuntimeError
            If FYERS_APP_ID/FYERS_SECRET_ID are not configured, or if the
            token exchange fails.
        """
        if self._access_token:
            return self._access_token

        cached = self._load_cached_token()
        if cached and self._validate_token(cached):
            self._access_token = cached
            return self._access_token
        if cached:
            logger.warning("Cached FYERS token failed validation — discarding and re-authenticating")
            self._token_cache_path.unlink(missing_ok=True)

        from config.settings import FYERS_ACCESS_TOKEN as env_token

        if env_token and self._validate_token(env_token):
            self._access_token = env_token
            self._save_cached_token(env_token)
            return self._access_token
        if env_token:
            logger.warning(
                "FYERS_ACCESS_TOKEN in .env is invalid, expired, or still the "
                "placeholder value — falling back to interactive OAuth2 login"
            )

        if self._non_interactive:
            raise RuntimeError(
                "FYERS: no valid token available from access_token/cache/env "
                "and non_interactive=True — refusing to fall through to the "
                "interactive OAuth2 flow (_run_oauth_flow() hangs forever on "
                "input() with no connected stdin). Run "
                "`python3 -m ingestion.scrapers.fyers_backfill login` / "
                "`... exchange <code_or_url>` to refresh the cached token."
            )

        if not self._app_id or not self._secret_id:
            raise RuntimeError(
                "FYERS_APP_ID and FYERS_SECRET_ID must be set (.env) before "
                "running the interactive OAuth2 flow."
            )

        self._access_token = self._run_oauth_flow()
        self._save_cached_token(self._access_token)
        return self._access_token

    def _validate_token(self, token: str) -> bool:
        """
        Probe a token with a single lightweight, non-throttled API call.

        Parameters
        ----------
        token : str

        Returns
        -------
        bool
            True iff FYERS accepts the token (get_profile() returns
            {'s': 'ok', ...}), OR the probe itself was merely rate-limited
            (code 429) — a 429 says nothing about token validity, so it
            must not be treated as "invalid" (see 2026-08-04 fix note
            below). Only a genuine auth rejection returns False.

        Spec References
        ----------------
        SPEC-PIPE-001

        Raises
        ------
        None — any exception (network error, malformed response) is
        treated as "invalid token", not propagated.
        """
        try:
            probe = fyersModel.FyersModel(client_id=self._app_id, token=token, is_async=False)
            response = probe.get_profile()
            if isinstance(response, dict) and response.get("s") == "ok":
                return True
            # [2026-08-04, live-confirmed] A concurrent/high-throughput
            # caller (e.g. parallel ticker fetch) can 429 this validation
            # probe itself before ever getting far enough to check the
            # token. Treating that as "invalid" was a real, destructive
            # bug: get_access_token() would then unlink() the cache file
            # and refuse to proceed (non_interactive=True), discarding a
            # perfectly good token over what was actually just rate
            # limiting on the PROBE call, not a rejection of the token.
            if isinstance(response, dict) and response.get("code") == 429:
                logger.warning(
                    "FYERS token validation probe was rate-limited (429) — treating the "
                    "cached token as still valid rather than discarding it; a 429 on this "
                    "probe says nothing about the token's own validity."
                )
                return True
            return False
        except Exception as exc:
            logger.warning(f"FYERS token validation request failed: {exc}")
            return False

    def _build_session(self) -> fyersModel.SessionModel:
        return fyersModel.SessionModel(
            client_id=self._app_id,
            secret_key=self._secret_id,
            redirect_uri=self._redirect_uri,
            response_type="code",
            grant_type="authorization_code",
        )

    def get_authorization_url(self) -> str:
        """
        Build the FYERS OAuth2 login URL — no blocking I/O, no input().

        Parameters
        ----------
        None

        Returns
        -------
        str
            URL to open in a browser to log in and obtain an auth_code.

        Spec References
        ----------------
        SPEC-PIPE-001

        Raises
        ------
        None
        """
        return self._build_session().generate_authcode()

    def exchange_auth_code(self, raw_input_value: str) -> str:
        """
        Exchange a pasted auth_code (or full redirected URL) for an access
        token, with no input() / stdin dependency — safe to call from a
        non-interactive shell, a second short-lived process, or a script.

        Parameters
        ----------
        raw_input_value : str
            Either a bare auth_code or the full URL FYERS redirected to
            (e.g. 'https://127.0.0.1/?auth_code=XXXX&state=...').

        Returns
        -------
        str
            The new access token. Also cached to FYERS_TOKEN_CACHE_PATH so
            a subsequent, separate `FYERSBackfill()` instance (e.g. the
            backfill_runner process) picks it up automatically.

        Spec References
        ----------------
        SPEC-PIPE-001: "get_access_token() — OAuth2 flow using FYERS_APP_ID
        and FYERS_SECRET_ID from .env."

        PIT Assumptions
        ----------------
        None.

        Raises
        ------
        RuntimeError
            If the auth_code cannot be found in `raw_input_value`, or if
            FYERS rejects the token exchange.
        """
        auth_code = self._extract_auth_code(raw_input_value)
        session = self._build_session()
        session.set_token(auth_code)
        response = session.generate_token()
        if not isinstance(response, dict) or response.get("s") != "ok":
            raise RuntimeError(f"FYERS token exchange failed: {response}")

        token = response["access_token"]
        self._access_token = token
        self._save_cached_token(token)
        return token

    def _run_oauth_flow(self) -> str:
        """
        Interactive OAuth2 authorization-code exchange (SPEC-PIPE-001).

        Blocks on input() — only safe in a genuinely interactive terminal
        with a connected stdin. If that's not the case (e.g. a captured
        IDE output pane with no writable stdin), this hangs forever
        waiting for input that will never arrive; use
        `python3 -m ingestion.scrapers.fyers_backfill login` /
        `... exchange <code_or_url>` instead — those never call input().
        """
        auth_url = self.get_authorization_url()
        print(
            "FYERS OAuth2 login required.\n"
            f"1. Open this URL in a browser and log in:\n   {auth_url}\n"
            f"2. After login you'll be redirected to a URL starting with "
            f"{self._redirect_uri} containing 'auth_code=...'.\n"
            "3. Paste the FULL redirected URL (or just the auth_code) below.",
            flush=True,
        )
        sys.stdout.flush()
        raw_input_value = input("Redirected URL or auth_code: ").strip()
        return self.exchange_auth_code(raw_input_value)

    def _load_cached_token(self) -> Optional[str]:
        """Return today's cached token, or None if absent/stale (tokens expire daily)."""
        if not self._token_cache_path.exists():
            return None
        try:
            cache = json.loads(self._token_cache_path.read_text())
        except (json.JSONDecodeError, OSError):
            return None
        if cache.get("date") != now_ist().date().isoformat():
            return None
        return cache.get("token")

    def _save_cached_token(self, token: str) -> None:
        """Persist today's token so repeated runs in one day skip the OAuth prompt."""
        self._token_cache_path.parent.mkdir(parents=True, exist_ok=True)
        self._token_cache_path.write_text(
            json.dumps({"token": token, "date": now_ist().date().isoformat()})
        )

    def _get_client(self) -> fyersModel.FyersModel:
        """Lazily construct the FyersModel client once a token is available.

        Thread-safe (see self._client_lock's docstring at __init__) — the
        double-checked lock avoids serializing every call through the
        lock once self._client is already set, which matters when this
        is invoked from many parallel worker threads.
        """
        if self._client is None:
            with self._client_lock:
                if self._client is None:
                    self._client = fyersModel.FyersModel(
                        client_id=self._app_id, token=self.get_access_token(), is_async=False
                    )
        return self._client

    def _throttle(self) -> None:
        """
        Inter-call pacing gate — global across every thread/instance in
        this process (see _GlobalRateLimiter's docstring), not a plain
        per-call sleep.

        [2026-08-01] The FYERS_MAX_CALLS_PER_DAY daily-call-count cap was
        removed at explicit user request — it was a project-chosen planning
        number (documented in config/settings.py as "the task's explicit
        budget"), not a limit sourced from FYERS' own published API rate
        limits, and was blocking same-day resumption of an in-progress
        backfill for no confirmed real-world reason. The per-call pacing
        (FYERS_RATE_LIMIT_SLEEP_SECONDS) is kept — that one is a genuine
        inter-request throttle, not a daily budget.
        """
        _RATE_LIMITER.acquire()

    def download_history(
        self,
        ticker: str,
        from_date: str,
        to_date: str,
        timeframe: str = RESOLUTION_DAILY,
    ) -> pd.DataFrame:
        """
        Download OHLCV history for one ticker over [from_date, to_date].

        Automatically chunks the request into windows no larger than
        FYERS_HISTORY_MAX_DAYS_PER_CALL, since FYERS' history API rejects
        date ranges spanning more than ~1 year per call.

        Parameters
        ----------
        ticker : str
            Bare NSE symbol, e.g. 'RELIANCE' (not 'NSE:RELIANCE-EQ').
        from_date : str
            "YYYY-MM-DD", inclusive.
        to_date : str
            "YYYY-MM-DD", inclusive.
        timeframe : str
            FYERS resolution code; 'D' for daily (the only mode this
            module's downstream consumers — ohlcv_adjusted — use).

        Returns
        -------
        pd.DataFrame
            Columns: date, ticker, open, high, low, close, volume. Empty
            DataFrame (same columns) if FYERS returns no candles at all.

        Spec References
        ----------------
        SPEC-PIPE-001: FYERS historical backfill OHLCV source.

        PIT Assumptions
        ----------------
        None — this is raw historical price magnitude, not yet
        corporate-action-adjusted (that happens downstream in
        ingestion/adjust/price_adjuster.py, applied uniformly to all
        ohlcv_adjusted rows regardless of source).

        Raises
        ------
        RuntimeError
            If the daily call budget is exhausted mid-download, or if
            FYERS returns an error response ('s' != 'ok' and != 'no_data').
        """
        symbol = f"{EXCHANGE_PREFIX}{ticker}{EXCHANGE_SEGMENT_SUFFIX}"
        start = datetime.strptime(from_date, "%Y-%m-%d").date()
        end = datetime.strptime(to_date, "%Y-%m-%d").date()

        chunks: List[pd.DataFrame] = []
        window_start = start
        while window_start <= end:
            window_end = min(
                window_start + timedelta(days=FYERS_HISTORY_MAX_DAYS_PER_CALL - 1), end
            )
            chunk = self._download_chunk(symbol, window_start, window_end, timeframe)
            if not chunk.empty:
                chunks.append(chunk)
            window_start = window_end + timedelta(days=1)

        if not chunks:
            return pd.DataFrame(columns=OUTPUT_COLUMNS)

        df = pd.concat(chunks, ignore_index=True)
        df["ticker"] = ticker
        df = df.drop_duplicates(subset="date").sort_values("date").reset_index(drop=True)
        return df[OUTPUT_COLUMNS]

    def _download_chunk(
        self, symbol: str, window_start: date_type, window_end: date_type, timeframe: str
    ) -> pd.DataFrame:
        """
        Fetch one <= FYERS_HISTORY_MAX_DAYS_PER_CALL-day window of candles.

        [2026-08-04, live-confirmed] A 429 ("request limit reached") is
        retried with exponential backoff (_MAX_429_RETRIES attempts) — it
        is transient rate limiting, NOT a real error about this specific
        symbol/window, and must never be treated the same as a genuine
        rejection (e.g. "Invalid symbol provided", which retrying can
        never fix and should fail immediately). Before this fix, a
        sustained multi-threaded backfill run silently dropped valid,
        liquid tickers (RELIANCE-tier names) purely because they happened
        to be rate-limited at the moment of their one and only attempt.
        """
        response = None
        for attempt in range(_MAX_429_RETRIES + 1):
            self._throttle()
            client = self._get_client()
            response = client.history(
                data={
                    "symbol": symbol,
                    "resolution": timeframe,
                    "date_format": DATE_FORMAT_EPOCH,
                    "range_from": str(int(datetime.combine(window_start, datetime.min.time()).timestamp())),
                    "range_to": str(int(datetime.combine(window_end, datetime.min.time()).timestamp())),
                    "cont_flag": "1",
                }
            )

            if not isinstance(response, dict):
                raise RuntimeError(f"Unexpected FYERS response for {symbol}: {response!r}")

            if response.get("code") == 429 and attempt < _MAX_429_RETRIES:
                backoff_seconds = _429_BACKOFF_BASE_SECONDS * (2 ** attempt)
                logger.warning(
                    f"{symbol}: 429 rate-limited (attempt {attempt + 1}/{_MAX_429_RETRIES + 1}), "
                    f"retrying in {backoff_seconds:.1f}s"
                )
                time.sleep(backoff_seconds)
                continue
            break

        assert isinstance(response, dict), f"response must be dict but got {type(response)}"
        status = response.get("s")
        if status == "no_data":
            logger.info(f"{symbol}: no FYERS data for {window_start}..{window_end}")
            return pd.DataFrame(columns=CANDLE_COLUMNS)
        if status != "ok":
            raise RuntimeError(f"FYERS history error for {symbol}: {response}")

        candles = response.get("candles", [])
        if not candles:
            return pd.DataFrame(columns=CANDLE_COLUMNS)

        df = pd.DataFrame(candles, columns=CANDLE_COLUMNS)
        df["date"] = pd.to_datetime(df["epoch"], unit="s", utc=True).dt.tz_convert(
            "Asia/Kolkata"
        ).dt.date
        return df.drop(columns="epoch")

    def batch_download(
        self,
        tickers: List[str],
        from_date: str,
        to_date: str,
        timeframe: str = RESOLUTION_DAILY,
        save: bool = True,
        show_progress: bool = True,
    ) -> Dict[str, pd.DataFrame]:
        """
        Download history for many tickers, with a progress bar and raw save.

        Parameters
        ----------
        tickers : list of str
        from_date : str
            "YYYY-MM-DD".
        to_date : str
            "YYYY-MM-DD".
        timeframe : str
        save : bool
            If True, persist each ticker's result to FYERS_RAW_DIR as
            Parquet (SPEC-PIPE-001 raw retention).
        show_progress : bool
            If True, render a tqdm progress bar over tickers.

        Returns
        -------
        dict
            ticker -> DataFrame (download_history's output schema). A
            ticker whose download failed maps to an empty DataFrame with
            the same columns; the failure is logged, not raised, so one
            bad ticker never aborts the whole batch.

        Spec References
        ----------------
        SPEC-PIPE-001: "batch_download(...) downloads all with progress
        bar (tqdm)."

        PIT Assumptions
        ----------------
        None.

        Raises
        ------
        None — per-ticker failures are caught and logged.
        """
        from tqdm import tqdm

        results: Dict[str, pd.DataFrame] = {}
        n_total = len(tickers)
        n_done = 0
        n_failed = 0
        start_ts = time.monotonic()
        last_progress_ts = start_ts
        last_complete_ts = start_ts

        def _log_progress(force: bool = False) -> None:
            nonlocal last_progress_ts
            now = time.monotonic()
            if not force and now - last_progress_ts < _PROGRESS_LOG_INTERVAL_S:
                return
            last_progress_ts = now
            elapsed = now - start_ts
            if n_done > 0:
                eta = elapsed / n_done * (n_total - n_done)
            else:
                eta = float("nan")
            logger.info(
                "FYERS backfill: %d/%d tickers (elapsed=%.0fs, ETA=%.0fs, %d failed)",
                n_done, n_total, elapsed, eta, n_failed,
            )

        iterator = tqdm(tickers, desc="FYERS backfill", unit="ticker") if show_progress else tickers

        for ticker in iterator:
            try:
                df = self.download_history(ticker, from_date, to_date, timeframe)
                ok = not df.empty
            except Exception as exc:
                logger.error(f"{ticker}: FYERS backfill failed: {exc}")
                df = pd.DataFrame(columns=OUTPUT_COLUMNS)
                ok = False

            n_done += 1
            if not ok:
                n_failed += 1
            else:
                last_complete_ts = time.monotonic()

            if save and not df.empty:
                self._save_parquet(ticker, from_date, to_date, df)

            results[ticker] = df

            # [2026-08-06, Issue2] Telemetry on the silent (show_progress=False)
            # scheduler path: emit cumulative progress ~every 20s, and trip the
            # idle watchdog if nothing has completed for _IDLE_WATCHDOG_S.
            if not show_progress:
                _log_progress()
                idle_s = time.monotonic() - last_complete_ts
                if idle_s >= _IDLE_WATCHDOG_S:
                    logger.warning(
                        "FYERS backfill: no ticker completed in %.0fs — possibly stalled "
                        "on a hung network call (only %d/%d done). Inspect before aborting.",
                        idle_s, n_done, n_total,
                    )
                    last_complete_ts = time.monotonic()  # avoid spamming every iteration

        if not show_progress:
            _log_progress(force=True)

        return results

    @staticmethod
    def _save_parquet(ticker: str, from_date: str, to_date: str, df: pd.DataFrame) -> None:
        """Persist one ticker's raw FYERS download (SPEC-PIPE-001 raw retention)."""
        FYERS_RAW_DIR.mkdir(parents=True, exist_ok=True)
        path = FYERS_RAW_DIR / f"{ticker}_{from_date}_{to_date}.parquet"
        df.to_parquet(path, index=False)


def _cli() -> None:
    """
    `python3 -m ingestion.scrapers.fyers_backfill {login|exchange}` — a
    non-interactive, two-step alternative to FYERSBackfill's built-in
    input()-based OAuth2 flow (SPEC-PIPE-001), for environments where a
    blocking input() call never receives input (e.g. a captured IDE output
    pane with no connected stdin). Neither subcommand ever calls input().
    """
    import argparse

    parser = argparse.ArgumentParser(description="FYERS OAuth2 login helper (non-interactive)")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("login", help="Print the FYERS login URL and exit immediately.")
    exchange_parser = subparsers.add_parser(
        "exchange", help="Exchange a pasted auth_code (or full redirected URL) for an access token."
    )
    exchange_parser.add_argument("code_or_url")

    args = parser.parse_args()
    fb = FYERSBackfill()

    if args.command == "login":
        print(fb.get_authorization_url(), flush=True)
    else:
        fb.exchange_auth_code(args.code_or_url)
        print(f"Access token obtained and cached to {fb._token_cache_path}", flush=True)


if __name__ == "__main__":
    _cli()
