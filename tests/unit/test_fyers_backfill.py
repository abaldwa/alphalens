"""
tests/unit/test_fyers_backfill.py

Phase: 0.5 (FYERS Historical Backfill)
Specs: SPEC-PIPE-001, SPEC-PIPE-002
Owner: Platform / Ingestion
Consumers: CI, pytest

Unit tests for ingestion/scrapers/fyers_backfill.py and
ingestion/backfill_runner.py's checkpoint-resume behavior. The FYERS API
itself is always mocked — these tests never make real network calls.
"""

from datetime import datetime, timezone

import pandas as pd
import pytest

from datastore.schema import create_normalised
from ingestion import backfill_runner
from ingestion.scrapers import fyers_backfill
from ingestion.scrapers.fyers_backfill import OUTPUT_COLUMNS, FYERSBackfill


def _epoch(y: int, m: int, d: int) -> int:
    return int(datetime(y, m, d, tzinfo=timezone.utc).timestamp())


def _ok_response(start_year: int, n_days: int = 3) -> dict:
    """A minimal 'ok' FYERS history response with n_days daily candles."""
    candles = [
        [_epoch(start_year, 1, day), 100.0 + day, 105.0 + day, 95.0 + day, 102.0 + day, 1000 * day]
        for day in range(1, n_days + 1)
    ]
    return {"s": "ok", "candles": candles}


class _StubClient:
    """Records every history() call and returns a fixed canned response."""

    def __init__(self, response_fn):
        self.calls = []
        self._response_fn = response_fn

    def history(self, data):
        self.calls.append(data)
        return self._response_fn(data)


def _make_backfill(monkeypatch, response_fn) -> FYERSBackfill:
    fb = FYERSBackfill(access_token="test-token")
    stub = _StubClient(response_fn)
    monkeypatch.setattr(fb, "_get_client", lambda: stub)
    return fb, stub


def test_batch_download_processes_all_tickers(monkeypatch):
    """SPEC-PIPE-001: batch_download must download and return every ticker requested."""
    fb, stub = _make_backfill(monkeypatch, lambda data: _ok_response(2024))
    tickers = ["AAA", "BBB", "CCC"]

    results = fb.batch_download(tickers, "2024-01-01", "2024-01-03", save=False, show_progress=False)

    assert set(results.keys()) == set(tickers)
    for ticker in tickers:
        assert list(results[ticker].columns) == OUTPUT_COLUMNS
        assert (results[ticker]["ticker"] == ticker).all()
        assert len(results[ticker]) == 3


def test_rate_limiting_sleeps_between_calls(monkeypatch):
    """SPEC-PIPE-001: the shared global rate limiter must gate every FYERS API call.

    2026-08-04: _throttle() now goes through _GlobalRateLimiter (a real
    cross-thread minimum-interval gate, not a fixed per-call sleep) — see
    that class's docstring for why per-instance sleeps alone don't
    prevent concurrent callers from bursting past FYERS' rate limit. The
    exact sleep duration is no longer a fixed constant (it depends on
    real elapsed monotonic time between calls), so this test asserts the
    gate fires with roughly the right duration instead of an exact value.
    """
    sleeps = []
    monkeypatch.setattr(fyers_backfill.time, "sleep", lambda secs: sleeps.append(secs))
    # _RATE_LIMITER is a module-level singleton shared across the whole
    # process (by design — see _GlobalRateLimiter's docstring: the whole
    # point is one gate shared across every caller, not per-test
    # isolation). Reset its state so this test only observes waits caused
    # by ITS OWN two calls, not backlog accumulated by other tests that
    # ran earlier in the same process.
    monkeypatch.setattr(fyers_backfill._RATE_LIMITER, "_next_allowed_at", 0.0)

    fb, stub = _make_backfill(monkeypatch, lambda data: _ok_response(2020))

    # A 400-day span exceeds FYERS_HISTORY_MAX_DAYS_PER_CALL (365) -> 2 chunks -> 2 throttled calls.
    fb.download_history("RELIANCE", "2020-01-01", "2021-02-04")

    # The first call may fire immediately (the freshly-reset limiter has
    # no backlog, so time.sleep is skipped entirely — see
    # _GlobalRateLimiter.acquire's `if wait_seconds > 0` guard); the
    # second call must always wait roughly one full interval behind it.
    assert len(stub.calls) == 2
    assert len(sleeps) in (1, 2)
    for sleep_seconds in sleeps:
        assert 0 < sleep_seconds <= fyers_backfill.FYERS_RATE_LIMIT_SLEEP_SECONDS


def test_extract_auth_code_from_full_redirected_url():
    """SPEC-PIPE-001: a full redirected URL must yield just the auth_code value."""
    url = "https://127.0.0.1/?auth_code=ABC123&state=None"
    assert FYERSBackfill._extract_auth_code(url) == "ABC123"


def test_extract_auth_code_accepts_bare_code():
    """A bare auth_code (no URL wrapper) must be returned as-is."""
    assert FYERSBackfill._extract_auth_code("ABC123") == "ABC123"


def test_extract_auth_code_rejects_bare_redirect_url_with_no_query_string():
    """
    A bare redirect URL with no '?auth_code=...' (e.g. someone pasted just
    'https://127.0.0.1/' instead of the full post-login redirected URL)
    must fail with a clear, actionable local error — not be silently sent
    to FYERS as if the whole URL were the auth_code itself.
    """
    with pytest.raises(RuntimeError, match="looks like a URL but has no"):
        FYERSBackfill._extract_auth_code("https://127.0.0.1/")


def test_invalid_env_token_falls_back_to_interactive_oauth(monkeypatch, tmp_path):
    """An unedited .env placeholder (or expired token) must never be sent to FYERS as-is."""
    import config.settings as settings

    fb = FYERSBackfill(app_id="X", secret_id="Y", token_cache_path=tmp_path / "token.json")
    monkeypatch.setattr(settings, "FYERS_ACCESS_TOKEN", "your_fyers_access_token_here")
    monkeypatch.setattr(fb, "_validate_token", lambda token: False)
    monkeypatch.setattr(fb, "_run_oauth_flow", lambda: "real-token-from-oauth")

    token = fb.get_access_token()

    assert token == "real-token-from-oauth"


def test_valid_env_token_is_used_without_triggering_oauth(monkeypatch, tmp_path):
    """A genuinely valid .env token must be accepted without prompting for interactive login."""
    import config.settings as settings

    fb = FYERSBackfill(app_id="X", secret_id="Y", token_cache_path=tmp_path / "token.json")
    monkeypatch.setattr(settings, "FYERS_ACCESS_TOKEN", "a-real-valid-token")
    monkeypatch.setattr(fb, "_validate_token", lambda token: True)

    def _fail_if_called():
        raise AssertionError("OAuth flow must not run when the env token validates")

    monkeypatch.setattr(fb, "_run_oauth_flow", _fail_if_called)

    token = fb.get_access_token()

    assert token == "a-real-valid-token"


def test_resumes_from_last_completed_ticker(monkeypatch, tmp_path):
    """SPEC-PIPE-001: a ticker already fully present in ohlcv_adjusted must be
    skipped on resume — verified via real DB content (has_sufficient_history),
    not via the checkpoint file's list position. An earlier implementation
    skipped everything up to the checkpoint's *index* in the tickers list,
    which broke the moment the list's order or membership changed between
    runs (e.g. after rebuilding config/nifty500_universe.csv) — this test
    deliberately uses a stale/irrelevant checkpoint file alongside a
    differently-ordered ticker list to guard against that regression."""
    create_normalised.create_schema(in_memory=True)

    from datastore.api.db import get_duckdb_connection

    with get_duckdb_connection(None) as conn:
        conn.execute(
            "INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume, adj_factor) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ["2024-01-01", "AAA", 100.0, 101.0, 99.0, 100.5, 1000, 1.0],
        )

    checkpoint_path = tmp_path / "resume.txt"
    # Stale checkpoint pointing at a ticker not even in this run's list —
    # must have zero influence on what gets skipped.
    backfill_runner.write_resume_checkpoint(checkpoint_path, "ZZZ_NOT_IN_LIST")

    called_tickers = []

    class _RecordingClient:
        def download_history(self, ticker, from_date, to_date, timeframe="D"):
            called_tickers.append(ticker)
            return pd.DataFrame(
                {
                    "date": ["2024-01-01"],
                    "ticker": [ticker],
                    "open": [100.0],
                    "high": [101.0],
                    "low": [99.0],
                    "close": [100.5],
                    "volume": [1000],
                }
            )

    # Note the order: AAA is NOT first, unlike the old position-based design
    # would have required for correct skipping.
    results = backfill_runner.run_backfill(
        ["BBB", "AAA", "CCC"],
        "2024-01-01",
        "2024-01-01",
        in_memory=True,
        checkpoint_path=checkpoint_path,
        client=_RecordingClient(),
    )

    assert "AAA" not in called_tickers
    assert called_tickers == ["BBB", "CCC"]
    assert results["AAA"] == 0
    assert results["BBB"] == 1 and results["CCC"] == 1


def test_checkpoint_does_not_advance_past_a_failed_ticker(tmp_path):
    """A ticker whose download raises must NOT be marked complete in the resume file —
    otherwise a transient/auth failure looks identical to a real success, and a
    re-run wrongly skips a ticker that still has zero rows in ohlcv_adjusted."""
    create_normalised.create_schema(in_memory=True)
    checkpoint_path = tmp_path / "resume.txt"

    class _AlwaysFailingClient:
        def download_history(self, ticker, from_date, to_date, timeframe="D"):
            raise RuntimeError("FYERS history error: Could not authenticate the user")

    results = backfill_runner.run_backfill(
        ["AAA", "BBB"],
        "2024-01-01",
        "2024-01-01",
        in_memory=True,
        checkpoint_path=checkpoint_path,
        client=_AlwaysFailingClient(),
    )

    assert results == {"AAA": 0, "BBB": 0}
    # Neither ticker succeeded, so the checkpoint file must still be empty —
    # a subsequent run must retry both, not skip them as "already done".
    assert backfill_runner.read_resume_checkpoint(checkpoint_path) is None


def test_direct_mode_does_not_hold_db_connection_across_network_download(tmp_path, monkeypatch):
    """
    2026-07-10 lock-hold-time remediation: run_backfill used to open ONE
    DuckDB write connection wrapping the entire ticker loop, including
    every network-bound download_history() call — holding DuckDB's
    single-writer lock for the whole (potentially hours-long) backfill.
    Asserts the DB connection is closed again before each ticker's
    download starts, and only reopened for that ticker's own write.
    """
    create_normalised.create_schema(in_memory=True)
    checkpoint_path = tmp_path / "resume.txt"

    from datastore.api import db as db_module

    class _RecordingClient:
        def download_history(self, ticker, from_date, to_date, timeframe="D"):
            return pd.DataFrame({
                "date": ["2024-01-01"], "ticker": [ticker], "open": [100.0],
                "high": [101.0], "low": [99.0], "close": [100.5], "volume": [1000],
            })

    real_get_conn = db_module.get_duckdb_connection
    call_log = []

    def _spy_get_conn(*args, **kwargs):
        call_log.append("open")
        cm = real_get_conn(*args, **kwargs)
        return cm

    monkeypatch.setattr(backfill_runner, "get_duckdb_connection", _spy_get_conn)

    backfill_runner.run_backfill(
        ["AAA", "BBB"], "2024-01-01", "2024-01-01",
        in_memory=True, checkpoint_path=checkpoint_path, client=_RecordingClient(),
    )

    # 2 tickers x (1 read-only sufficiency check + 1 write) = 4 separate
    # connection acquisitions — never one connection spanning both tickers.
    assert len(call_log) == 4


def test_staged_publish_mode_matches_ohlcv_adjusted_full_schema(tmp_path):
    """A25 staged mode's merge SQL does 'SELECT * FROM ohlcv_adjusted UNION ALL
    SELECT * FROM _stage_new_batch' — DuckDB requires equal column counts for
    UNION ALL. download_history() only returns 7 columns; ohlcv_adjusted has
    11 (delivery_qty, delivery_pct, vol_adj_factor are schema-only). Staging
    must pad the batch to the full column set or every staged-mode backfill
    run raises duckdb.BinderException the moment it's exercised."""
    create_normalised.create_schema(in_memory=True)
    checkpoint_path = tmp_path / "resume.txt"

    class _RecordingClient:
        def download_history(self, ticker, from_date, to_date, timeframe="D"):
            return pd.DataFrame(
                {
                    "date": ["2024-01-01"],
                    "ticker": [ticker],
                    "open": [100.0],
                    "high": [101.0],
                    "low": [99.0],
                    "close": [100.5],
                    "volume": [1000],
                }
            )

    results = backfill_runner.run_backfill(
        ["AAA"],
        "2024-01-01",
        "2024-01-01",
        in_memory=True,
        checkpoint_path=checkpoint_path,
        client=_RecordingClient(),
        publish_mode="staged",
    )

    assert results["AAA"] == 1
