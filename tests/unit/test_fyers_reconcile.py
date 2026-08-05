"""
tests/unit/test_fyers_reconcile.py

Phase: 0.5 (FYERS Historical Backfill / Daily Cutover)
Specs: SPEC-PIPE-001
Owner: Platform / Ingestion
Consumers: CI, pytest

Unit tests for config.universe.{clip_to_listing_window, get_listing_windows}
and ingestion.reconcile.fyers_diff.
"""

from datetime import date

import pandas as pd
import pytest

from config.universe import clip_to_listing_window, get_listing_windows
from datastore.api.db import close_all_connections, get_duckdb_connection
from datastore.schema import create_normalised
from ingestion.reconcile.fyers_diff import diff_fyers_vs_prod, recompute_targets


# ===== clip_to_listing_window (pure function) =====


def test_clip_unbounded_both_sides_returns_full_range():
    result = clip_to_listing_window(None, None, date(2020, 1, 1), date(2020, 12, 31))
    assert result == (date(2020, 1, 1), date(2020, 12, 31))


def test_clip_ipo_mid_year_clips_start():
    result = clip_to_listing_window(date(2020, 6, 15), None, date(2020, 1, 1), date(2020, 12, 31))
    assert result == (date(2020, 6, 15), date(2020, 12, 31))


def test_clip_delisted_mid_year_clips_end():
    result = clip_to_listing_window(None, date(2020, 6, 15), date(2020, 1, 1), date(2020, 12, 31))
    assert result == (date(2020, 1, 1), date(2020, 6, 15))


def test_clip_listed_and_delisted_entirely_before_range_returns_none():
    result = clip_to_listing_window(date(2010, 1, 1), date(2010, 12, 31), date(2020, 1, 1), date(2020, 12, 31))
    assert result is None


def test_clip_listed_entirely_after_range_returns_none():
    result = clip_to_listing_window(date(2025, 1, 1), None, date(2020, 1, 1), date(2020, 12, 31))
    assert result is None


# ===== get_listing_windows (real seeded DuckDB) =====


@pytest.fixture
def db_path(tmp_path):
    p = tmp_path / "normalised_test.duckdb"
    create_normalised.create_schema(db_path=p)
    close_all_connections()
    return p


def test_get_listing_windows_returns_none_for_ticker_missing_from_stock_master(db_path):
    with get_duckdb_connection(db_path, persist=False) as conn:
        windows = get_listing_windows(conn, ["NOSUCHTICKER"])
    assert windows == {"NOSUCHTICKER": (None, None)}


def test_get_listing_windows_reads_real_dates(db_path):
    with get_duckdb_connection(db_path, persist=False) as conn:
        conn.execute(
            "INSERT INTO stock_master (ticker, company_name, nse_series, listing_date) "
            "VALUES ('AAA', 'AAA Ltd', 'EQ', '2020-06-15'), "
            "('BBB', 'BBB Ltd', 'EQ', '2010-01-01')"
        )
        conn.execute(
            "INSERT INTO delisted_companies (ticker, delisting_date) VALUES ('BBB', '2022-03-31')"
        )
        windows = get_listing_windows(conn, ["AAA", "BBB"])
    assert windows["AAA"] == (date(2020, 6, 15), None)
    assert windows["BBB"] == (date(2010, 1, 1), date(2022, 3, 31))


# ===== diff_fyers_vs_prod / recompute_targets =====


def _fyers_row(ticker, d, close, volume=1000):
    return {"ticker": ticker, "date": d, "open": close, "high": close, "low": close, "close": close, "volume": volume}


def test_diff_classifies_new_row_when_prod_has_nothing(db_path):
    fyers_df = pd.DataFrame([_fyers_row("AAA", date(2024, 1, 2), 100.0)])
    with get_duckdb_connection(db_path, persist=False) as conn:
        diff_df = diff_fyers_vs_prod(conn, fyers_df, ["AAA"], date(2024, 1, 1), date(2024, 1, 31))
    assert diff_df.iloc[0]["change_type"] == "new"


def test_diff_classifies_unchanged_when_close_and_volume_match(db_path):
    with get_duckdb_connection(db_path, persist=False) as conn:
        conn.execute(
            "INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume) "
            "VALUES ('2024-01-02', 'AAA', 100.0, 100.0, 100.0, 100.0, 1000)"
        )
        fyers_df = pd.DataFrame([_fyers_row("AAA", date(2024, 1, 2), 100.0, 1000)])
        diff_df = diff_fyers_vs_prod(conn, fyers_df, ["AAA"], date(2024, 1, 1), date(2024, 1, 31))
    assert diff_df.iloc[0]["change_type"] == "unchanged"


def test_diff_classifies_changed_when_close_differs_beyond_tolerance(db_path):
    with get_duckdb_connection(db_path, persist=False) as conn:
        conn.execute(
            "INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume) "
            "VALUES ('2024-01-02', 'AAA', 100.0, 100.0, 100.0, 100.0, 1000)"
        )
        # 5% off — the classic backward-adjustment-corruption case this
        # diff exists to catch (project_ohlcv_adjfactor_discontinuities_20260802).
        fyers_df = pd.DataFrame([_fyers_row("AAA", date(2024, 1, 2), 105.0, 1000)])
        diff_df = diff_fyers_vs_prod(conn, fyers_df, ["AAA"], date(2024, 1, 1), date(2024, 1, 31))
    assert diff_df.iloc[0]["change_type"] == "changed"


def test_diff_tiny_float_noise_within_tolerance_is_unchanged(db_path):
    with get_duckdb_connection(db_path, persist=False) as conn:
        conn.execute(
            "INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume) "
            "VALUES ('2024-01-02', 'AAA', 100.0, 100.0, 100.0, 100.00001, 1000)"
        )
        fyers_df = pd.DataFrame([_fyers_row("AAA", date(2024, 1, 2), 100.0, 1000)])
        diff_df = diff_fyers_vs_prod(conn, fyers_df, ["AAA"], date(2024, 1, 1), date(2024, 1, 31))
    assert diff_df.iloc[0]["change_type"] == "unchanged"


def test_diff_empty_fyers_df_returns_empty():
    diff_df = diff_fyers_vs_prod(None, pd.DataFrame(), ["AAA"], date(2024, 1, 1), date(2024, 1, 31))
    assert diff_df.empty


def test_recompute_targets_excludes_unchanged():
    diff_df = pd.DataFrame(
        [
            {"ticker": "AAA", "date": date(2024, 1, 2), "change_type": "new"},
            {"ticker": "AAA", "date": date(2024, 1, 3), "change_type": "changed"},
            {"ticker": "AAA", "date": date(2024, 1, 4), "change_type": "unchanged"},
        ]
    )
    targets = recompute_targets(diff_df)
    assert set(targets["date"]) == {date(2024, 1, 2), date(2024, 1, 3)}


def test_recompute_targets_empty_diff_returns_empty():
    targets = recompute_targets(pd.DataFrame(columns=["ticker", "date", "change_type"]))
    assert targets.empty
