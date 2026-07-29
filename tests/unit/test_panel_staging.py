"""
tests/unit/test_panel_staging.py

Phase: 3 (feature-backfill performance)
Specs: SPEC-DS-005, SPEC-FEAT-001
Owner: Platform / QA
Consumers: CI, pytest

The critical regression test for features/panel_staging.py: batch-staged
technical/intraday/pnd/advanced_technical/pattern_scores output must be
numerically IDENTICAL, date by date, to what the original per-date
sequential path (build_feature_matrix with staged_panel=None) produces
for the exact same dates — this is the correctness bar the whole
batch-staging redesign exists to meet (see that module's docstring).

Uses a fake DataStoreClient (no network/real DuckDB — a dedicated
tmp_path-scoped staging DB file is used instead of the real
config.settings.FEATURE_PANEL_STAGING_DB_PATH, per this project's
no-synthetic-writes-to-the-real-DB policy).
"""

import numpy as np
import pandas as pd
import pytest

from features.advanced_technical import ADVANCED_TECHNICAL_FEATURES
from features.intraday import INTRADAY_FEATURES
from features.matrix_builder import build_feature_matrix
from features.pattern_scores import PATTERN_FEATURES
from features.pnd_features import PND_FEATURES
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
            "ticker": None,
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
    """Same fixture pattern as tests/unit/test_matrix_builder.py — ignores
    from_date/to_date (returns the full stored history), which is fine
    here since both the per-date path and the batch-staging path go
    through this same client and therefore see identical raw data."""

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
def multi_ticker_client():
    tickers = ["AAA", "BBB", "CCC"]
    rows = {t: _make_ohlcv_rows(320, seed=i) for i, t in enumerate(tickers)}
    bm_rows = {name: _make_ohlcv_rows(320, seed=hash(name) % 1000) for name in BENCHMARK_TICKERS.values()}
    return _FakeDataStoreClient({**rows, **bm_rows}), tickers


@pytest.fixture(autouse=True)
def _reset_matrix_builder_process_singletons():
    import features.matrix_builder as mb

    mb._listing_dates_cache = None
    mb._ever_fno_eligible_tickers = None
    mb._fundamental_raw_cache = None
    yield
    mb._listing_dates_cache = None
    mb._ever_fno_eligible_tickers = None
    mb._fundamental_raw_cache = None


_BATCHED_CATEGORY_COLUMNS = (
    CORE_TECHNICAL_FEATURES + INTRADAY_FEATURES + PND_FEATURES + ADVANCED_TECHNICAL_FEATURES + PATTERN_FEATURES
)


class TestBatchStagingMatchesPerDateSequential:
    def test_staged_panel_byte_identical_to_sequential_for_each_date(
        self, multi_ticker_client, monkeypatch, tmp_path
    ):
        from features import panel_staging

        client, tickers = multi_ticker_client
        staging_db_path = tmp_path / "panel_staging_test.duckdb"
        monkeypatch.setattr(panel_staging, "FEATURE_PANEL_STAGING_DB_PATH", staging_db_path)

        all_dates = pd.bdate_range(start="2024-01-01", periods=320)
        # 7 consecutive trading days near the end of the available history
        # (fully warmed-up rolling windows) — the correctness bar from the
        # brief ("5-10 consecutive trading days").
        target_dates = list(all_dates[-7:])

        # --- Baseline: current per-date sequential path -----------------
        baseline_by_date = {}
        for d in target_dates:
            mat = build_feature_matrix(
                d.strftime("%Y-%m-%d"), tickers, client=client, save=False, compute_hmm=False,
            )
            baseline_by_date[d] = mat[["ticker"] + _BATCHED_CATEGORY_COLUMNS].sort_values("ticker").reset_index(drop=True)

        # --- New batch-staging path --------------------------------------
        run_id = "test_run"
        staged_count = panel_staging.stage_batch_panels(client, tickers, target_dates, run_id=run_id)
        assert staged_count > 0

        for d in target_dates:
            staged = panel_staging.load_staged_panel_for_date(run_id, d)
            assert staged is not None, f"no staged rows for {d}"

            mat_staged = build_feature_matrix(
                d.strftime("%Y-%m-%d"), tickers, client=client, save=False, compute_hmm=False,
                staged_panel=staged,
            )
            actual = mat_staged[["ticker"] + _BATCHED_CATEGORY_COLUMNS].sort_values("ticker").reset_index(drop=True)
            expected = baseline_by_date[d]

            pd.testing.assert_frame_equal(actual, expected, check_dtype=False)

        panel_staging.drop_staging_run(run_id)

    def test_early_warmup_dates_keep_nan_rolling_features(self, multi_ticker_client, monkeypatch, tmp_path):
        """A ticker's first ~760 days of history should still produce
        incomplete/NaN rolling-window values in the batch path exactly as
        in the per-date path — the batch panel spans years of FUTURE dates
        too, so this guards against future context leaking backward into
        an early date's row."""
        from features import panel_staging

        client, tickers = multi_ticker_client
        staging_db_path = tmp_path / "panel_staging_test2.duckdb"
        monkeypatch.setattr(panel_staging, "FEATURE_PANEL_STAGING_DB_PATH", staging_db_path)

        all_dates = pd.bdate_range(start="2024-01-01", periods=320)
        # Pick an EARLY date (only ~10 trading days of real history behind
        # it) so 252-day rolling features (e.g. sma_200_ratio-style columns)
        # are still legitimately NaN.
        target_dates = [all_dates[10]]

        mat_baseline = build_feature_matrix(
            target_dates[0].strftime("%Y-%m-%d"), tickers, client=client, save=False, compute_hmm=False,
        )

        run_id = "test_run_warmup"
        panel_staging.stage_batch_panels(client, tickers, target_dates, run_id=run_id)
        staged = panel_staging.load_staged_panel_for_date(run_id, target_dates[0])
        assert staged is not None

        mat_staged = build_feature_matrix(
            target_dates[0].strftime("%Y-%m-%d"), tickers, client=client, save=False, compute_hmm=False,
            staged_panel=staged,
        )

        baseline_sorted = mat_baseline[["ticker"] + _BATCHED_CATEGORY_COLUMNS].sort_values("ticker").reset_index(drop=True)
        staged_sorted = mat_staged[["ticker"] + _BATCHED_CATEGORY_COLUMNS].sort_values("ticker").reset_index(drop=True)
        pd.testing.assert_frame_equal(staged_sorted, baseline_sorted, check_dtype=False)

        panel_staging.drop_staging_run(run_id)


class TestResumeSkipsAlreadyStagedTickers:
    def test_resume_does_not_recompute_already_staged_tickers(
        self, monkeypatch, tmp_path
    ):
        """Simulates an interrupted first attempt (only some tickers ever
        got staged before the process died) followed by a restart with the
        SAME run_id and the FULL ticker list. The already-staged tickers
        must not be recomputed (proven by making the compute function
        raise if invoked for them), the missing tickers must get staged,
        and load_staged_panel_for_date must return complete data for every
        ticker afterward."""
        from features import panel_staging

        tickers = ["AAA", "BBB", "CCC", "DDD"]
        rows = {t: _make_ohlcv_rows(320, seed=i) for i, t in enumerate(tickers)}
        bm_rows = {name: _make_ohlcv_rows(320, seed=hash(name) % 1000) for name in BENCHMARK_TICKERS.values()}
        client = _FakeDataStoreClient({**rows, **bm_rows})

        staging_db_path = tmp_path / "panel_staging_resume_test.duckdb"
        monkeypatch.setattr(panel_staging, "FEATURE_PANEL_STAGING_DB_PATH", staging_db_path)

        # Force each chunk to contain exactly one ticker, so per-ticker
        # skip behavior is easy to reason about/assert on.
        monkeypatch.setattr(panel_staging, "_BATCH_CHUNK_SIZE_DIVISOR", 1)
        import ingestion.scheduler.resource_guard as resource_guard
        monkeypatch.setattr(resource_guard, "adaptive_chunk_size", lambda *a, **k: 1)

        all_dates = pd.bdate_range(start="2024-01-01", periods=320)
        target_dates = list(all_dates[-5:])
        run_id = "test_resume_run"

        # --- "Interrupted first attempt": stage only AAA, BBB -----------
        first_attempt_tickers = ["AAA", "BBB"]
        staged_first = panel_staging.stage_batch_panels(
            client, first_attempt_tickers, target_dates, run_id=run_id
        )
        assert staged_first > 0
        for t in first_attempt_tickers:
            for d in target_dates:
                loaded = panel_staging.load_staged_panel_for_date(run_id, d)
                assert t in set(loaded["ticker"]), f"{t} should be staged after first attempt"

        # --- Restart with SAME run_id, FULL ticker list -----------------
        # Wrap compute_full_range_chunk_panels so it raises if invoked on a
        # chunk containing an already-staged ticker (AAA/BBB) — proves
        # resume genuinely skips recomputation, not just re-staging
        # identical results.
        import features.matrix_builder as mb

        real_compute = mb.compute_full_range_chunk_panels
        calls = []

        def _guarded_compute(chunk_panel, benchmark_wide):
            chunk_tickers = set(chunk_panel["ticker"].unique())
            calls.append(chunk_tickers)
            if chunk_tickers & set(first_attempt_tickers):
                raise AssertionError(
                    f"compute_full_range_chunk_panels invoked for already-staged "
                    f"ticker(s) {chunk_tickers & set(first_attempt_tickers)} — resume "
                    f"did not skip them"
                )
            return real_compute(chunk_panel, benchmark_wide)

        monkeypatch.setattr(panel_staging, "compute_full_range_chunk_panels", _guarded_compute)

        staged_second = panel_staging.stage_batch_panels(
            client, tickers, target_dates, run_id=run_id
        )
        assert staged_second > 0

        computed_tickers = set().union(*calls) if calls else set()
        assert computed_tickers == {"CCC", "DDD"}, (
            f"expected only the previously-missing tickers to be (re)computed, got {computed_tickers}"
        )

        # --- All 4 tickers now present for every requested date ---------
        for d in target_dates:
            loaded = panel_staging.load_staged_panel_for_date(run_id, d)
            assert loaded is not None
            assert set(loaded["ticker"]) == set(tickers), (
                f"expected all 4 tickers staged for {d}, got {set(loaded['ticker'])}"
            )

        panel_staging.drop_staging_run(run_id)
