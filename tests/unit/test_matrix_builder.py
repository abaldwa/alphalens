"""
tests/unit/test_matrix_builder.py

Phase: 1.2 (Core Feature Computation)
Specs: SPEC-SOLID-005, SPEC-DS-005, SPEC-PIPE-005, SPEC-FEAT-001
Owner: Platform / QA
Consumers: CI, pytest

Unit tests for features/matrix_builder.py's assembly logic. Uses a fake
DataStoreClient (SPEC-SOLID-005: tests inject mock implementations via
interfaces) so this suite never touches the network or DuckDB, and never
runs the expensive HMM fit (compute_hmm=False everywhere here — HMM
fitting is covered separately and thoroughly by tests/unit/test_hmm.py).
"""

import numpy as np
import pandas as pd
import pytest

from features.calendar import CALENDAR_FEATURES
from features.intraday import INTRADAY_FEATURES
from features.macro_features import MACRO_FEATURES
from features.matrix_builder import ALL_FEATURE_COLUMNS, build_feature_matrix
from features.technical import BENCHMARK_TICKERS, CORE_TECHNICAL_FEATURES


def _make_ohlcv_rows(n_days, seed, start="2024-01-01"):
    dates = pd.bdate_range(start=start, periods=n_days)
    rng = np.random.default_rng(seed)
    base_price = 100 + rng.uniform(0, 900)
    rets = rng.normal(0.0003, 0.02, n_days)
    close = base_price * np.cumprod(1 + rets)
    open_ = close * (1 + rng.normal(0, 0.005, n_days))
    high = np.maximum(open_, close) * (1 + np.abs(rng.normal(0, 0.005, n_days)))
    low = np.minimum(open_, close) * (1 - np.abs(rng.normal(0, 0.005, n_days)))
    volume = rng.integers(100_000, 5_000_000, n_days)
    delivery_pct = rng.uniform(10, 90, n_days)
    return [
        {
            "date": d.strftime("%Y-%m-%d"),
            "ticker": None,  # filled by caller
            "open": float(o),
            "high": float(h),
            "low": float(low_),
            "close": float(c),
            "volume": int(v),
            "delivery_pct": float(dp),
        }
        for d, o, h, low_, c, v, dp in zip(dates, open_, high, low, close, volume, delivery_pct)
    ]


class _FakeDataStoreClient:
    """In-memory stand-in for DataStoreClient.get_ohlcv (SPEC-SOLID-005 DI)."""

    def __init__(self, rows_by_ticker, missing_tickers=()):
        self._rows_by_ticker = rows_by_ticker
        self._missing = set(missing_tickers)

    def get_ohlcv(self, ticker, from_date, to_date):
        if ticker in self._missing:
            return []
        rows = self._rows_by_ticker.get(ticker, [])
        for r in rows:
            r["ticker"] = ticker
        return rows


@pytest.fixture
def fake_client():
    rows_a = _make_ohlcv_rows(300, seed=1)
    rows_b = _make_ohlcv_rows(300, seed=2)
    bm_rows = {name: _make_ohlcv_rows(300, seed=hash(name) % 1000) for name in BENCHMARK_TICKERS.values()}
    return _FakeDataStoreClient({"AAA": rows_a, "BBB": rows_b, **bm_rows})


@pytest.fixture(autouse=True)
def _reset_matrix_builder_process_singletons():
    """[BUG FIX, 2026-07-28 model-review item 5] build_feature_matrix now
    caches get_listing_dates() (and the pre-existing F&O/fundamental-cache
    singletons) once per process, on purpose — but each test here installs
    its own fake_client with different listing_dates behavior, so the cache
    must be reset between tests or a later test would silently see an
    earlier test's cached result instead of calling its own fake client."""
    import features.matrix_builder as mb

    mb._listing_dates_cache = None
    mb._ever_fno_eligible_tickers = None
    mb._fundamental_raw_cache = None
    yield
    mb._listing_dates_cache = None
    mb._ever_fno_eligible_tickers = None
    mb._fundamental_raw_cache = None


class TestBuildFeatureMatrix:
    def test_empty_tickers_raises(self, fake_client):
        with pytest.raises(ValueError):
            build_feature_matrix("2024-12-20", [], client=fake_client, save=False, compute_hmm=False)

    def test_shape_matches_all_feature_columns(self, fake_client):
        target_date = pd.bdate_range(start="2024-01-01", periods=300)[-1].strftime("%Y-%m-%d")
        mat = build_feature_matrix(
            target_date, ["AAA", "BBB"], client=fake_client, save=False, compute_hmm=False
        )
        assert mat.shape == (2, 2 + len(ALL_FEATURE_COLUMNS))
        assert list(mat.columns) == ["date", "ticker"] + ALL_FEATURE_COLUMNS

    def test_ticker_with_no_data_gets_all_nan_row_not_dropped(self, fake_client):
        """Ticker-specific columns (technical/intraday/HMM) must be NaN for a ticker whose
        OHLCV fetch failed — but market-wide columns (calendar/macro) are still broadcast,
        since they don't depend on any individual ticker's data."""
        fake_client._missing.add("ZZZ")
        target_date = pd.bdate_range(start="2024-01-01", periods=300)[-1].strftime("%Y-%m-%d")
        mat = build_feature_matrix(
            target_date, ["AAA", "ZZZ"], client=fake_client, save=False, compute_hmm=False
        )
        assert set(mat["ticker"]) == {"AAA", "ZZZ"}
        zzz_row = mat[mat["ticker"] == "ZZZ"]
        per_ticker_cols = CORE_TECHNICAL_FEATURES + INTRADAY_FEATURES
        assert zzz_row[per_ticker_cols].isna().all(axis=None)
        # Broadcast columns are present (not dropped) regardless of per-ticker fetch outcome.
        for col in CALENDAR_FEATURES:
            assert zzz_row[col].notna().all()

    def test_all_tickers_missing_raises_instead_of_all_nan_matrix(self, fake_client):
        """2026-07-07 incident regression: when OHLCV is unavailable for every ticker
        (e.g. the DataStore API is down), this must hard-fail rather than silently
        write an all-NaN feature matrix that gets checkpointed 'success' and fed to
        every downstream model. A handful of individually-missing tickers (covered by
        test_ticker_with_no_data_gets_all_nan_row_not_dropped) is a distinct, tolerated
        case — zero-of-N is never legitimate."""
        fake_client._missing.update({"AAA", "BBB"})
        target_date = pd.bdate_range(start="2024-01-01", periods=300)[-1].strftime("%Y-%m-%d")
        with pytest.raises(RuntimeError, match="No OHLCV data returned"):
            build_feature_matrix(
                target_date, ["AAA", "BBB"], client=fake_client, save=False, compute_hmm=False
            )

    def test_macro_columns_present_even_without_macro_indicators(self, fake_client):
        """No macro_indicators seeded for this date -> macro columns NaN, but still present, not dropped."""
        target_date = pd.bdate_range(start="2024-01-01", periods=300)[-1].strftime("%Y-%m-%d")
        mat = build_feature_matrix(
            target_date, ["AAA"], client=fake_client, save=False, compute_hmm=False
        )
        for col in MACRO_FEATURES:
            assert col in mat.columns
        assert mat.loc[0, "rl_regime_label"] == 0.0  # Phase 1 stub, always populated

    def test_missing_date_column_in_feature_panel_is_tolerated(self, fake_client, monkeypatch):
        import features.matrix_builder as mb

        def _bad_technical_panel(*args, **kwargs):
            return pd.DataFrame(columns=["ticker", "close"])

        monkeypatch.setattr(mb, "compute_technical_features", _bad_technical_panel)
        target_date = pd.bdate_range(start="2024-01-01", periods=300)[-1].strftime("%Y-%m-%d")

        mat = build_feature_matrix(target_date, ["AAA"], client=fake_client, save=False, compute_hmm=False)

        assert mat.shape[0] == 1
        assert list(mat.columns[:2]) == ["date", "ticker"]
        assert mat.loc[0, "ticker"] == "AAA"

    def test_advance_decline_ratio_outside_price_ratio_range_is_not_flagged(self, fake_client, caplog):
        """Regression guard for the false-positive found in P1.2: advance_decline_ratio
        legitimately falls outside [0.1, 10.0] for small/lopsided universes and must not
        trip the price-ratio range warning meant for sma/ema ratios."""
        target_date = pd.bdate_range(start="2024-01-01", periods=300)[-1].strftime("%Y-%m-%d")
        with caplog.at_level("WARNING"):
            build_feature_matrix(target_date, ["AAA", "BBB"], client=fake_client, save=False, compute_hmm=False)
        assert not any("advance_decline_ratio" in r.message for r in caplog.records)

    def test_does_not_save_when_save_false(self, fake_client, tmp_path, monkeypatch):
        import features.matrix_builder as mb

        monkeypatch.setattr(mb, "FEATURES_DAILY_DIR", tmp_path)
        target_date = pd.bdate_range(start="2024-01-01", periods=300)[-1].strftime("%Y-%m-%d")
        build_feature_matrix(target_date, ["AAA"], client=fake_client, save=False, compute_hmm=False)
        assert list(tmp_path.iterdir()) == []

    def test_saves_parquet_when_save_true(self, fake_client, tmp_path, monkeypatch):
        import features.matrix_builder as mb

        monkeypatch.setattr(mb, "FEATURES_DAILY_DIR", tmp_path)
        target_date_ts = pd.bdate_range(start="2024-01-01", periods=300)[-1]
        build_feature_matrix(
            target_date_ts.strftime("%Y-%m-%d"), ["AAA"], client=fake_client, save=True, compute_hmm=False
        )
        expected = tmp_path / f"{target_date_ts.date().isoformat()}.parquet"
        assert expected.exists()
        roundtrip = pd.read_parquet(expected)
        assert roundtrip.shape[0] == 1


class TestNotYetListedTickerFiltering:
    """2026-07-28 perf fix: a ticker whose listing_date is confirmed AFTER
    the backfill's as_of date must be excluded from every per-ticker panel
    call entirely (not just get a NaN row via a failed fetch) — profiling
    found ~37% of a real historical sample were exactly this case (today's
    universe includes tickers that hadn't IPO'd yet on older backtest
    dates), each wastefully triggering a live per-ticker fallback API call
    just to confirm 'no data, as expected'."""

    def test_not_yet_listed_ticker_gets_nan_row_without_being_fetched(self, fake_client):
        fake_client.get_listing_dates = lambda: {"AAA": pd.Timestamp("2030-01-01")}
        target_date = pd.bdate_range(start="2024-01-01", periods=300)[-1].strftime("%Y-%m-%d")

        calls = {"n": 0}
        orig_get_ohlcv = fake_client.get_ohlcv

        def counting_get_ohlcv(ticker, *a, **kw):
            if ticker == "AAA":
                calls["n"] += 1
            return orig_get_ohlcv(ticker, *a, **kw)

        fake_client.get_ohlcv = counting_get_ohlcv

        mat = build_feature_matrix(
            target_date, ["AAA", "BBB"], client=fake_client, save=False, compute_hmm=False
        )
        assert set(mat["ticker"]) == {"AAA", "BBB"}
        aaa_row = mat[mat["ticker"] == "AAA"]
        assert aaa_row[CORE_TECHNICAL_FEATURES].isna().all(axis=None)
        assert calls["n"] == 0  # never fetched — filtered out before any per-ticker call

    def test_unknown_listing_date_is_not_treated_as_unlisted(self, fake_client):
        """Missing from listing_dates entirely (e.g. pre-2012 IPO, per
        stock_master's real coverage gap) must NOT be excluded — same
        conservative missing-data convention as everywhere else here."""
        fake_client.get_listing_dates = lambda: {}
        target_date = pd.bdate_range(start="2024-01-01", periods=300)[-1].strftime("%Y-%m-%d")
        mat = build_feature_matrix(
            target_date, ["AAA", "BBB"], client=fake_client, save=False, compute_hmm=False
        )
        assert mat[mat["ticker"] == "AAA"][CORE_TECHNICAL_FEATURES].notna().any(axis=None)

    def test_listing_date_before_as_of_is_kept(self, fake_client):
        fake_client.get_listing_dates = lambda: {"AAA": pd.Timestamp("2020-01-01")}
        target_date = pd.bdate_range(start="2024-01-01", periods=300)[-1].strftime("%Y-%m-%d")
        mat = build_feature_matrix(
            target_date, ["AAA", "BBB"], client=fake_client, save=False, compute_hmm=False
        )
        assert mat[mat["ticker"] == "AAA"][CORE_TECHNICAL_FEATURES].notna().any(axis=None)

    def test_get_listing_dates_failure_falls_back_to_no_filtering(self, fake_client):
        def raising_get_listing_dates():
            raise ConnectionError("API down")

        fake_client.get_listing_dates = raising_get_listing_dates
        target_date = pd.bdate_range(start="2024-01-01", periods=300)[-1].strftime("%Y-%m-%d")
        mat = build_feature_matrix(
            target_date, ["AAA", "BBB"], client=fake_client, save=False, compute_hmm=False
        )
        assert set(mat["ticker"]) == {"AAA", "BBB"}

    def test_get_listing_dates_failure_logs_loudly_at_error_level(self, fake_client, caplog):
        """[BUG FIX, 2026-07-28 model-review] The get_listing_dates()
        fail-open (listing_dates={}) silently reintroduces the
        not-yet-listed-ticker bug on any API hiccup — this must be a LOUD
        (ERROR-level) log, not indistinguishable from a normal build."""
        def raising_get_listing_dates():
            raise ConnectionError("API down")

        fake_client.get_listing_dates = raising_get_listing_dates
        target_date = pd.bdate_range(start="2024-01-01", periods=300)[-1].strftime("%Y-%m-%d")
        with caplog.at_level("ERROR"):
            build_feature_matrix(target_date, ["AAA", "BBB"], client=fake_client, save=False, compute_hmm=False)
        assert any("FEATURE_BUILD_DEGRADED" in r.message for r in caplog.records if r.levelname == "ERROR")

    def test_listing_dates_is_only_fetched_once_across_multiple_calls(self, fake_client):
        """[BUG FIX, 2026-07-28 model-review item 5] get_listing_dates() is
        PIT-agnostic, same singleton-caching pattern already applied to the
        F&O-eligibility set — it must not be re-fetched from the API on
        every build_feature_matrix call in a multi-thousand-date backfill."""
        calls = {"n": 0}

        def counting_get_listing_dates():
            calls["n"] += 1
            return {"AAA": pd.Timestamp("2020-01-01")}

        fake_client.get_listing_dates = counting_get_listing_dates
        target_date = pd.bdate_range(start="2024-01-01", periods=300)[-1].strftime("%Y-%m-%d")
        build_feature_matrix(target_date, ["AAA", "BBB"], client=fake_client, save=False, compute_hmm=False)
        build_feature_matrix(target_date, ["AAA", "BBB"], client=fake_client, save=False, compute_hmm=False)
        assert calls["n"] == 1

    def test_listing_dates_failure_is_not_permanently_cached(self, fake_client):
        """A transient get_listing_dates() failure must be retried on the
        next call, not permanently memoized as "confirmed no listing
        dates" for the rest of the process's life."""
        calls = {"n": 0}

        def flaky_get_listing_dates():
            calls["n"] += 1
            if calls["n"] == 1:
                raise ConnectionError("API down")
            return {"AAA": pd.Timestamp("2020-01-01")}

        fake_client.get_listing_dates = flaky_get_listing_dates
        target_date = pd.bdate_range(start="2024-01-01", periods=300)[-1].strftime("%Y-%m-%d")
        build_feature_matrix(target_date, ["AAA", "BBB"], client=fake_client, save=False, compute_hmm=False)
        build_feature_matrix(target_date, ["AAA", "BBB"], client=fake_client, save=False, compute_hmm=False)
        assert calls["n"] == 2  # first call failed and was not cached; second call retried it


class TestSaveFeatureMatrixAtomicWrite:
    """[BUG FIX, 2026-07-28 model-review item 2] A kill mid-write must never
    leave a corrupt/truncated parquet at the final path — write-then-rename
    is atomic on the same filesystem."""

    def test_write_failure_does_not_leave_a_file_at_the_final_path(self, monkeypatch, tmp_path):
        import features.matrix_builder as mb

        monkeypatch.setattr(mb, "FEATURES_DAILY_DIR", tmp_path)
        matrix = pd.DataFrame({"ticker": ["AAA"], "close": [100.0]})

        def raising_to_parquet(self, path, index=False):
            raise OSError("simulated kill mid-write")

        monkeypatch.setattr(pd.DataFrame, "to_parquet", raising_to_parquet)

        target_date = pd.Timestamp("2024-06-30")
        with pytest.raises(OSError):
            mb._save_feature_matrix(matrix, target_date)

        final_path = tmp_path / "2024-06-30.parquet"
        tmp_leftover = tmp_path / "2024-06-30.parquet.tmp"
        assert not final_path.exists()
        assert not tmp_leftover.exists()  # temp file is cleaned up on failure too

    def test_successful_write_lands_at_the_final_path_only(self, tmp_path):
        import features.matrix_builder as mb

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(mb, "FEATURES_DAILY_DIR", tmp_path)
            matrix = pd.DataFrame({"ticker": ["AAA"], "close": [100.0]})
            target_date = pd.Timestamp("2024-06-30")
            out_path = mb._save_feature_matrix(matrix, target_date)

        assert out_path == tmp_path / "2024-06-30.parquet"
        assert out_path.exists()
        assert not (tmp_path / "2024-06-30.parquet.tmp").exists()


class TestChunkedComputationMatchesUnchunked:
    """A47 (2026-07-10): the critical regression test — chunking
    technical/intraday/hmm/pnd/adv_tech/patterns must produce numerically
    identical output to computing them in one full-universe pass. This is
    the test that proves chunking is safe: any future change that makes
    chunk boundaries leak into a per-ticker computation (e.g. a rolling
    window accidentally spanning a chunk boundary) would fail this."""

    @pytest.fixture
    def multi_ticker_client(self):
        rows = {
            t: _make_ohlcv_rows(300, seed=i)
            for i, t in enumerate(["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"])
        }
        bm_rows = {name: _make_ohlcv_rows(300, seed=hash(name) % 1000) for name in BENCHMARK_TICKERS.values()}
        return _FakeDataStoreClient({**rows, **bm_rows})

    def test_small_chunk_size_matches_single_chunk_result(self, multi_ticker_client, monkeypatch):
        import config.settings as settings

        target_date = pd.bdate_range(start="2024-01-01", periods=300)[-1].strftime("%Y-%m-%d")
        tickers = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"]

        # Single chunk (chunk size >= universe size) — everything in one pass.
        monkeypatch.setattr(settings, "SCREENER_BATCH_EXPORT_CHUNK_SIZE", 100)
        mat_unchunked = build_feature_matrix(
            target_date, tickers, client=multi_ticker_client, save=False, compute_hmm=False
        )

        # Force many small chunks (chunk size 2 -> 3 chunks for 6 tickers).
        monkeypatch.setattr(settings, "SCREENER_BATCH_EXPORT_CHUNK_SIZE", 2)
        mat_chunked = build_feature_matrix(
            target_date, tickers, client=multi_ticker_client, save=False, compute_hmm=False
        )

        mat_unchunked = mat_unchunked.sort_values("ticker").reset_index(drop=True)
        mat_chunked = mat_chunked.sort_values("ticker").reset_index(drop=True)
        pd.testing.assert_frame_equal(mat_unchunked, mat_chunked)


    def test_single_ticker_chunks_match_full_pass(self, multi_ticker_client, monkeypatch):
        """Most extreme case: chunk size 1 -- every ticker computed alone."""
        import config.settings as settings

        target_date = pd.bdate_range(start="2024-01-01", periods=300)[-1].strftime("%Y-%m-%d")
        tickers = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"]

        monkeypatch.setattr(settings, "SCREENER_BATCH_EXPORT_CHUNK_SIZE", 100)
        mat_unchunked = build_feature_matrix(
            target_date, tickers, client=multi_ticker_client, save=False, compute_hmm=False
        )

        monkeypatch.setattr(settings, "SCREENER_BATCH_EXPORT_CHUNK_SIZE", 1)
        mat_chunked = build_feature_matrix(
            target_date, tickers, client=multi_ticker_client, save=False, compute_hmm=False
        )

        mat_unchunked = mat_unchunked.sort_values("ticker").reset_index(drop=True)
        mat_chunked = mat_chunked.sort_values("ticker").reset_index(drop=True)
        pd.testing.assert_frame_equal(mat_unchunked, mat_chunked)


class TestPanelWorkersParallelization:
    """2026-07-29 perf fix: parallelizing
    _compute_chunked_ticker_independent_panels across ticker chunks via a
    spawn-context multiprocessing.Pool (mirroring
    compute_hmm_regime_features's n_workers pattern) must not change any
    computed feature value vs. the original sequential (panel_workers=1)
    path — this is the correctness bar that matters most for this change."""

    @pytest.fixture
    def multi_ticker_client(self):
        rows = {
            t: _make_ohlcv_rows(300, seed=i)
            for i, t in enumerate(["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"])
        }
        bm_rows = {name: _make_ohlcv_rows(300, seed=hash(name) % 1000) for name in BENCHMARK_TICKERS.values()}
        return _FakeDataStoreClient({**rows, **bm_rows})

    def test_panel_workers_two_matches_sequential_default(self, multi_ticker_client, monkeypatch):
        """panel_workers=2 (real spawn-context Pool, small chunks so >1 chunk
        exists to dispatch) produces byte-identical output to panel_workers=1
        for the same input — correctness under parallelization, not just
        'it runs without crashing'."""
        import config.settings as settings

        target_date = pd.bdate_range(start="2024-01-01", periods=300)[-1].strftime("%Y-%m-%d")
        tickers = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"]

        # Small chunk size so 6 tickers become 3 chunks -> pool actually
        # gets multiple tasks to dispatch.
        monkeypatch.setattr(settings, "SCREENER_BATCH_EXPORT_CHUNK_SIZE", 2)

        mat_sequential = build_feature_matrix(
            target_date, tickers, client=multi_ticker_client, save=False, compute_hmm=False,
            panel_workers=1,
        )
        mat_parallel = build_feature_matrix(
            target_date, tickers, client=multi_ticker_client, save=False, compute_hmm=False,
            panel_workers=2,
        )

        mat_sequential = mat_sequential.sort_values("ticker").reset_index(drop=True)
        mat_parallel = mat_parallel.sort_values("ticker").reset_index(drop=True)
        pd.testing.assert_frame_equal(mat_sequential, mat_parallel)

    def test_panel_workers_restores_blas_env_vars_after_pool_exits(self, multi_ticker_client, monkeypatch):
        """BLAS thread-count env vars are capped to '1' before Pool creation
        and must be restored (or left unset, matching pre-call state)
        afterwards, so this doesn't leak into unrelated code running later
        in the same process — same safeguard as compute_hmm_regime_features."""
        import os

        import config.settings as settings

        monkeypatch.setattr(settings, "SCREENER_BATCH_EXPORT_CHUNK_SIZE", 2)
        monkeypatch.delenv("OMP_NUM_THREADS", raising=False)
        os.environ["OPENBLAS_NUM_THREADS"] = "7"  # a pre-existing, non-"1" value to check restoration

        target_date = pd.bdate_range(start="2024-01-01", periods=300)[-1].strftime("%Y-%m-%d")
        tickers = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"]
        try:
            build_feature_matrix(
                target_date, tickers, client=multi_ticker_client, save=False, compute_hmm=False,
                panel_workers=2,
            )
            assert "OMP_NUM_THREADS" not in os.environ
            assert os.environ["OPENBLAS_NUM_THREADS"] == "7"
        finally:
            os.environ.pop("OPENBLAS_NUM_THREADS", None)
