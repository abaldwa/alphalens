"""
tests/unit/test_fundamental_raw_cache.py

Phase: Fundamental feature backfill performance (2026-07-28)
Owner: Platform / QA
Consumers: CI, pytest

Tests the event-driven raw-fundamental cache: features/fundamental_cache.py's
persistence layer, and compute_fundamental_features_panel's raw_cache
opt-in path in features/fundamental.py — using a fake DataStoreClient
(SPEC-SOLID-005 — no real HTTP call) and a temp DuckDB file (no real
fundamental_raw_cache.duckdb touched).
"""

from datetime import datetime
from unittest.mock import MagicMock

import numpy as np
import pytest

from features.fundamental import (
    CACHEABLE_RATIO_FEATURES,
    FUNDAMENTAL_FEATURES,
    PRICE_DEPENDENT_FEATURES,
    RATIO_FEATURES,
    compute_fundamental_features,
    compute_fundamental_features_panel,
)
from features.fundamental_cache import load_fundamental_raw_cache, save_fundamental_raw_cache_entries

_ALL_FUNDAMENTALS_FIELDS = [
    "ebitda", "pat", "eps", "operating_margin", "ebitda_margin", "net_margin", "roe", "roce",
    "debt_to_equity", "interest_coverage", "fcf", "asset_turnover", "inventory_days",
    "receivable_days", "payable_days", "book_value_per_share", "shares_outstanding",
    "gross_profit", "capex", "current_assets", "current_liabilities", "total_debt", "cash_and_equivalents",
]


def _quarter(fy, q, qed, ann, revenue=100.0, **kwargs):
    row = {
        "ticker": "TEST", "fiscal_year": fy, "quarter": q,
        "quarter_end_date": qed, "announcement_date": ann, "revenue": revenue,
    }
    row.update({field: None for field in _ALL_FUNDAMENTALS_FIELDS})
    row.update(kwargs)
    return row


def _one_quarter_history():
    return [
        _quarter(
            2025, 1, "2025-03-31", "2025-04-30",
            revenue=100.0, ebit=20.0, ebitda=25.0, eps=5.0, pat=15.0,
            book_value_per_share=50.0, shares_outstanding=1_000_000.0,
            total_debt=10.0, cash_and_equivalents=5.0, fcf=8.0,
        ),
    ]


class TestPriceDependentSplit:
    def test_cacheable_and_priced_partition_all_ratio_features(self):
        assert set(CACHEABLE_RATIO_FEATURES) | set(PRICE_DEPENDENT_FEATURES) == set(RATIO_FEATURES)
        assert set(CACHEABLE_RATIO_FEATURES) & set(PRICE_DEPENDENT_FEATURES) == set()

    def test_seven_price_dependent_features(self):
        assert len(PRICE_DEPENDENT_FEATURES) == 7


class TestPanelCacheCorrectness:
    def test_cache_hit_matches_uncached_computation(self):
        """The whole point: a cached day's raw entry must hold the exact
        same CACHEABLE_RATIO_FEATURES values a fresh, uncached computation
        would produce (compared pre-sector-z-score, which is orthogonal to
        what this test is checking — z-scoring is exercised separately by
        test_fundamental_features.py)."""
        rows = _one_quarter_history()
        client = MagicMock()
        client.get_ohlcv.return_value = [{"date": "2025-06-01", "close": 100.0}]

        uncached = compute_fundamental_features(client, "TEST", datetime(2025, 6, 1), pre_loaded_rows=rows)

        raw_cache = {}
        compute_fundamental_features_panel(
            client, ["TEST"], datetime(2025, 6, 1), {"TEST": "IT"},
            data_cache=_FakeDataCache(rows), raw_cache=raw_cache,
        )
        cached_ratios = raw_cache[("TEST", 2025, 1)]["ratios"]
        for f in CACHEABLE_RATIO_FEATURES:
            if np.isnan(uncached[f]):
                assert np.isnan(cached_ratios[f])
            else:
                assert cached_ratios[f] == pytest.approx(uncached[f])

        # Second call, same quarter -> served from cache -> raw_cache untouched.
        raw_cache_before = dict(raw_cache)
        compute_fundamental_features_panel(
            client, ["TEST"], datetime(2025, 6, 2), {"TEST": "IT"},
            data_cache=_FakeDataCache(rows), raw_cache=raw_cache,
        )
        assert raw_cache == raw_cache_before

    def test_price_dependent_features_still_move_with_price_on_a_cache_hit(self):
        # 2 tickers per sector so sector z-scoring doesn't collapse to NaN
        # (a single-element sector group has no meaningful std) — this test
        # is about market_cap tracking price, not about z-scoring itself.
        rows = _one_quarter_history()
        other_rows = [dict(rows[0], ticker="OTHER")]
        client = MagicMock()
        raw_cache = {}

        client.get_ohlcv.return_value = [{"date": "2025-06-01", "close": 100.0}]
        panel_day1 = compute_fundamental_features_panel(
            client, ["TEST", "OTHER"], datetime(2025, 6, 1), {"TEST": "IT", "OTHER": "IT"},
            data_cache=_FakeDataCache(rows, other_rows=other_rows), raw_cache=raw_cache,
        )
        assert len(raw_cache) == 2  # cache miss on day 1 for both -> populated

        client.get_ohlcv.return_value = [{"date": "2025-06-02", "close": 200.0}]  # price doubled
        panel_day2 = compute_fundamental_features_panel(
            client, ["TEST", "OTHER"], datetime(2025, 6, 2), {"TEST": "IT", "OTHER": "IT"},
            data_cache=_FakeDataCache(rows, other_rows=other_rows), raw_cache=raw_cache,
        )
        assert len(raw_cache) == 2  # still cache hits, no new entries

        # market_cap itself (pre-z-score) is what should double; recomputed
        # directly from the cached priced_inputs to sidestep sector z-scoring.
        from features.fundamental import _compute_priced_features
        priced_inputs = raw_cache[("TEST", 2025, 1)]["priced_inputs"]
        mc1 = _compute_priced_features(priced_inputs, 100.0)["market_cap"]
        mc2 = _compute_priced_features(priced_inputs, 200.0)["market_cap"]
        assert mc2 == pytest.approx(mc1 * 2)
        assert not panel_day1.empty and not panel_day2.empty  # sanity: both calls actually ran

    def test_new_quarter_is_a_cache_miss_and_updates_the_key(self):
        client = MagicMock()
        client.get_ohlcv.return_value = [{"date": "2025-06-01", "close": 100.0}]
        raw_cache = {}

        q1_rows = _one_quarter_history()
        compute_fundamental_features_panel(
            client, ["TEST"], datetime(2025, 6, 1), {"TEST": "IT"},
            data_cache=_FakeDataCache(q1_rows), raw_cache=raw_cache,
        )
        assert ("TEST", 2025, 1) in raw_cache

        q2_rows = q1_rows + [
            _quarter(2025, 2, "2025-06-30", "2025-07-31", revenue=110.0, ebit=22.0, ebitda=27.0, eps=5.5, pat=16.0,
                     book_value_per_share=52.0, shares_outstanding=1_000_000.0, total_debt=10.0,
                     cash_and_equivalents=5.0, fcf=9.0),
        ]
        compute_fundamental_features_panel(
            client, ["TEST"], datetime(2025, 8, 1), {"TEST": "IT"},
            data_cache=_FakeDataCache(q2_rows), raw_cache=raw_cache,
        )
        assert ("TEST", 2025, 2) in raw_cache
        assert len(raw_cache) == 2  # old quarter's entry kept, not evicted

    def test_cache_misses_out_only_contains_new_entries(self):
        client = MagicMock()
        client.get_ohlcv.return_value = [{"date": "2025-06-01", "close": 100.0}]
        rows = _one_quarter_history()
        raw_cache = {("TEST", 2025, 1): {
            "ratios": {f: np.nan for f in CACHEABLE_RATIO_FEATURES},
            "priced_inputs": {"shares": np.nan, "total_debt": np.nan, "cash": np.nan, "eps": np.nan,
                               "book_value_per_share": np.nan, "ebitda": np.nan, "ebit": np.nan,
                               "fcf": np.nan, "equity": np.nan},
            "announcement_date": "2025-04-30T00:00:00",
        }}
        misses = {}
        compute_fundamental_features_panel(
            client, ["TEST", "OTHER"], datetime(2025, 6, 1), {"TEST": "IT", "OTHER": "IT"},
            data_cache=_FakeDataCache(rows, other_rows=rows), raw_cache=raw_cache, cache_misses_out=misses,
        )
        # TEST was already cached -> not in misses. OTHER wasn't -> is.
        assert ("TEST", 2025, 1) not in misses
        assert ("OTHER", 2025, 1) in misses

    def test_no_history_ticker_is_not_a_crash_and_not_cached(self):
        client = MagicMock()
        client.get_ohlcv.return_value = []
        raw_cache = {}
        panel = compute_fundamental_features_panel(
            client, ["NODATA"], datetime(2025, 6, 1), {"NODATA": "IT"},
            data_cache=_FakeDataCache([]), raw_cache=raw_cache,
        )
        assert panel.set_index("ticker").loc["NODATA"]["results_pending_flag"] == 1
        assert raw_cache == {}

    def test_raw_cache_none_preserves_original_behavior(self):
        """The other real caller (retrain_phase2.py) never passes raw_cache —
        confirm the default is untouched."""
        client = MagicMock()
        client.get_ohlcv.return_value = [{"date": "2025-06-01", "close": 100.0}]
        rows = _one_quarter_history()
        panel = compute_fundamental_features_panel(
            client, ["TEST"], datetime(2025, 6, 1), {"TEST": "IT"}, data_cache=_FakeDataCache(rows),
        )
        assert set(panel.columns) == {"ticker"} | set(FUNDAMENTAL_FEATURES)


class _FakeDataCache:
    """Mimics features/backfill_cache.py's BackfillDataCache.get_fundamentals interface."""

    def __init__(self, rows, other_rows=None):
        self._rows = rows
        self._other_rows = other_rows if other_rows is not None else rows

    def get_fundamentals(self, ticker, as_of):
        return self._rows if ticker == "TEST" else self._other_rows


class TestPersistentCacheRoundTrip:
    def test_save_then_load_round_trips(self, tmp_path):
        db_path = tmp_path / "fundamental_raw_cache_test.duckdb"
        entries = {
            ("RELIANCE", 2025, 1): {
                "ratios": {"roe": 0.15, "roce": np.nan},
                "priced_inputs": {"shares": 1000.0, "total_debt": np.nan},
                "announcement_date": "2025-04-30T00:00:00",
            },
        }
        save_fundamental_raw_cache_entries(entries, db_path=db_path)
        loaded = load_fundamental_raw_cache(db_path=db_path)
        assert loaded[("RELIANCE", 2025, 1)]["ratios"]["roe"] == pytest.approx(0.15)
        assert np.isnan(loaded[("RELIANCE", 2025, 1)]["ratios"]["roce"])
        assert loaded[("RELIANCE", 2025, 1)]["announcement_date"] == "2025-04-30T00:00:00"

    def test_upsert_overwrites_existing_key(self, tmp_path):
        db_path = tmp_path / "fundamental_raw_cache_test.duckdb"
        key = ("TCS", 2025, 1)
        save_fundamental_raw_cache_entries(
            {key: {"ratios": {"roe": 0.10}, "priced_inputs": {}, "announcement_date": "2025-01-01T00:00:00"}},
            db_path=db_path,
        )
        save_fundamental_raw_cache_entries(
            {key: {"ratios": {"roe": 0.20}, "priced_inputs": {}, "announcement_date": "2025-01-01T00:00:00"}},
            db_path=db_path,
        )
        loaded = load_fundamental_raw_cache(db_path=db_path)
        assert len(loaded) == 1
        assert loaded[key]["ratios"]["roe"] == pytest.approx(0.20)

    def test_missing_db_file_returns_empty_dict(self, tmp_path):
        assert load_fundamental_raw_cache(db_path=tmp_path / "does_not_exist.duckdb") == {}

    def test_empty_entries_is_a_noop(self, tmp_path):
        db_path = tmp_path / "fundamental_raw_cache_test.duckdb"
        save_fundamental_raw_cache_entries({}, db_path=db_path)
        assert not db_path.exists()
