"""
tests/unit/test_universe.py

Phase: 0
Specs: SPEC-SYS-001, SPEC-SYS-011, SPEC-DS-001
Owner: Platform / DataStore
Consumers: CI, pytest

Unit tests for config/universe.py.
"""

from datetime import date, datetime

import pandas as pd
import pytest

import config.universe as universe_mod
from datastore.api.db import close_all_connections, get_duckdb_connection
from features.fundamental_source_priority import append_fundamentals_history
from datastore.schema import create_normalised


def _write_csv(path, rows):
    """rows: list of dicts with universe_mod.REQUIRED_COLUMNS keys."""
    pd.DataFrame(rows).to_csv(path, index=False)


def _row(ticker, tier=1, market_cap_cr=1000, adtv_cr=10.0, is_nifty500=True):
    return {
        "ticker": ticker,
        "company_name": f"{ticker} Ltd.",
        "sector": "Test",
        "tier": tier,
        "market_cap_cr": market_cap_cr,
        "adtv_cr": adtv_cr,
        "is_fno_eligible": False,
        "is_nifty500": is_nifty500,
        "isin": f"INE{hash(ticker) % 1000000000:09d}",
    }


@pytest.fixture(autouse=True)
def _isolated_filters(monkeypatch):
    """Pin the filter thresholds so tests don't depend on the active UNIVERSE_PROFILE."""
    monkeypatch.setattr(universe_mod, "TIER_THRESHOLD", 2)
    monkeypatch.setattr(universe_mod, "MIN_ADTV_CR", 5.0)
    monkeypatch.setattr(universe_mod, "MIN_MCAP_CR", 500)


# ===== load_universe_raw =====


def test_load_universe_raw_raises_file_not_found_when_csv_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(universe_mod, "UNIVERSE_CSV_PATH", tmp_path / "does-not-exist.csv")

    with pytest.raises(FileNotFoundError, match="Universe CSV not found"):
        universe_mod.load_universe_raw()


def test_load_universe_raw_raises_value_error_on_missing_columns(monkeypatch, tmp_path):
    csv_path = tmp_path / "universe.csv"
    pd.DataFrame([{"ticker": "AAA"}]).to_csv(csv_path, index=False)
    monkeypatch.setattr(universe_mod, "UNIVERSE_CSV_PATH", csv_path)

    with pytest.raises(ValueError, match="missing required columns"):
        universe_mod.load_universe_raw()


def test_load_universe_raw_returns_all_rows_unfiltered(monkeypatch, tmp_path):
    csv_path = tmp_path / "universe.csv"
    _write_csv(csv_path, [_row("AAA", tier=1), _row("BBB", tier=5)])
    monkeypatch.setattr(universe_mod, "UNIVERSE_CSV_PATH", csv_path)

    df = universe_mod.load_universe_raw()

    assert len(df) == 2
    assert set(df["ticker"]) == {"AAA", "BBB"}


# ===== load_universe: tier filter =====


def test_load_universe_filters_by_tier_threshold(monkeypatch, tmp_path):
    csv_path = tmp_path / "universe.csv"
    _write_csv(csv_path, [_row("AAA", tier=1), _row("BBB", tier=2), _row("CCC", tier=3)])
    monkeypatch.setattr(universe_mod, "UNIVERSE_CSV_PATH", csv_path)

    df = universe_mod.load_universe()

    assert set(df["ticker"]) == {"AAA", "BBB"}  # tier<=2 only


# ===== load_universe: adtv/mcap known-vs-unsourced relaxation =====


def test_load_universe_excludes_known_insufficient_adtv(monkeypatch, tmp_path):
    csv_path = tmp_path / "universe.csv"
    _write_csv(csv_path, [_row("AAA", adtv_cr=10.0), _row("BBB", adtv_cr=1.0)])  # BBB < MIN_ADTV_CR=5.0
    monkeypatch.setattr(universe_mod, "UNIVERSE_CSV_PATH", csv_path)

    df = universe_mod.load_universe()

    assert set(df["ticker"]) == {"AAA"}


def test_load_universe_excludes_known_insufficient_mcap(monkeypatch, tmp_path):
    csv_path = tmp_path / "universe.csv"
    _write_csv(csv_path, [_row("AAA", market_cap_cr=1000), _row("BBB", market_cap_cr=100)])  # BBB < 500
    monkeypatch.setattr(universe_mod, "UNIVERSE_CSV_PATH", csv_path)

    df = universe_mod.load_universe()

    assert set(df["ticker"]) == {"AAA"}


def test_load_universe_treats_zero_adtv_as_unsourced_not_excluded(monkeypatch, tmp_path):
    """adtv_cr == 0 means 'not yet backfilled', not 'below threshold' -- must pass, not be excluded."""
    csv_path = tmp_path / "universe.csv"
    _write_csv(csv_path, [_row("AAA", adtv_cr=0.0)])
    monkeypatch.setattr(universe_mod, "UNIVERSE_CSV_PATH", csv_path)

    df = universe_mod.load_universe()

    assert set(df["ticker"]) == {"AAA"}


def test_load_universe_treats_zero_mcap_as_unsourced_not_excluded(monkeypatch, tmp_path):
    """market_cap_cr == 0 means 'not yet sourced', not 'below threshold' -- must pass, not be excluded."""
    csv_path = tmp_path / "universe.csv"
    _write_csv(csv_path, [_row("AAA", market_cap_cr=0)])
    monkeypatch.setattr(universe_mod, "UNIVERSE_CSV_PATH", csv_path)

    df = universe_mod.load_universe()

    assert set(df["ticker"]) == {"AAA"}


def test_load_universe_combines_all_three_filters(monkeypatch, tmp_path):
    csv_path = tmp_path / "universe.csv"
    _write_csv(
        csv_path,
        [
            _row("PASS", tier=2, market_cap_cr=1000, adtv_cr=10.0),
            _row("BAD_TIER", tier=3, market_cap_cr=1000, adtv_cr=10.0),
            _row("BAD_MCAP", tier=2, market_cap_cr=100, adtv_cr=10.0),
            _row("BAD_ADTV", tier=2, market_cap_cr=1000, adtv_cr=1.0),
            _row("UNSOURCED", tier=2, market_cap_cr=0, adtv_cr=0.0),
        ],
    )
    monkeypatch.setattr(universe_mod, "UNIVERSE_CSV_PATH", csv_path)

    df = universe_mod.load_universe()

    assert set(df["ticker"]) == {"PASS", "UNSOURCED"}


# ===== get_tickers =====


def test_get_tickers_returns_flat_list(monkeypatch, tmp_path):
    csv_path = tmp_path / "universe.csv"
    _write_csv(csv_path, [_row("AAA", tier=1), _row("BBB", tier=2), _row("CCC", tier=5)])
    monkeypatch.setattr(universe_mod, "UNIVERSE_CSV_PATH", csv_path)

    tickers = universe_mod.get_tickers()

    assert tickers == ["AAA", "BBB"]


def test_get_tickers_empty_when_nothing_passes(monkeypatch, tmp_path):
    csv_path = tmp_path / "universe.csv"
    _write_csv(csv_path, [_row("AAA", tier=5)])
    monkeypatch.setattr(universe_mod, "UNIVERSE_CSV_PATH", csv_path)

    assert universe_mod.get_tickers() == []


# ===== get_tickers_for_feature_engineering =====
# [2026-08-04] ~500/2300 universe rows are ETFs (trade under NSE's EQ
# series so they pass tier/adtv/mcap filters, but have no fundamentals/
# shareholding/corp-actions to compute features from) — excluded from the
# feature-engineering entry points (daily_pipeline.py's
# step_compute_features, scripts/feature_backfill*.py) but NOT from
# get_tickers() itself, since backtest/screener/ML-scoring callers are
# out of scope for this change.


def test_excludes_known_etf_tickers(monkeypatch, tmp_path):
    csv_path = tmp_path / "universe.csv"
    _write_csv(
        csv_path,
        [_row("REALSTOCK", tier=1), _row("SOMEETF", tier=1)],
    )
    monkeypatch.setattr(universe_mod, "UNIVERSE_CSV_PATH", csv_path)
    monkeypatch.setattr("config.etf_exclusions.ETF_TICKERS", frozenset({"SOMEETF"}))

    assert universe_mod.get_tickers_for_feature_engineering() == ["REALSTOCK"]


def test_no_etfs_in_universe_returns_everything(monkeypatch, tmp_path):
    csv_path = tmp_path / "universe.csv"
    _write_csv(csv_path, [_row("AAA", tier=1), _row("BBB", tier=1)])
    monkeypatch.setattr(universe_mod, "UNIVERSE_CSV_PATH", csv_path)
    monkeypatch.setattr("config.etf_exclusions.ETF_TICKERS", frozenset())

    assert universe_mod.get_tickers_for_feature_engineering() == ["AAA", "BBB"]


# ===== get_market_cap_rank_map_as_of =====
#
# Real seeded DuckDB schema (create_normalised.create_schema), no mocks over
# the DB layer — same pattern as tests/unit/test_fundamentals_history.py.


@pytest.fixture
def db_path(tmp_path):
    p = tmp_path / "normalised_test.duckdb"
    create_normalised.create_schema(db_path=p)
    close_all_connections()
    return p


def _insert_fundamentals(conn, ticker, shares_outstanding, announcement_date, fiscal_year=2025, quarter=1, recorded_at=None):
    """recorded_at: append_fundamentals_history always stamps the REAL
    wall-clock time (see test_fundamentals_history.py's
    test_restatement_recorded_after_as_of_does_not_leak), which in this
    test environment is ~2026 — later than every as_of used below. Tests
    that need PIT data to actually be visible as of a historical as_of
    date must backdate recorded_at afterwards, exactly like that test
    does, or get_fundamentals_pit's `recorded_at <= as_of` filter would
    exclude every row regardless of announcement_date."""
    conn.execute(
        """
        INSERT INTO fundamentals
            (ticker, fiscal_year, quarter, quarter_end_date, announcement_date, shares_outstanding)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (ticker, fiscal_year, quarter) DO UPDATE SET
            shares_outstanding = excluded.shares_outstanding,
            announcement_date = excluded.announcement_date
        """,
        [ticker, fiscal_year, quarter, "2025-03-31", announcement_date, shares_outstanding],
    )
    append_fundamentals_history(conn, ticker, fiscal_year, quarter)
    if recorded_at is not None:
        conn.execute(
            "UPDATE fundamentals_history SET recorded_at = ? WHERE ticker = ? AND fiscal_year = ? AND quarter = ?",
            [recorded_at, ticker, fiscal_year, quarter],
        )


def _insert_price(conn, ticker, as_of, close):
    conn.execute(
        """
        INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [as_of, ticker, close, close, close, close, 1000],
    )


class TestGetMarketCapRankMapAsOf:
    def test_ranks_by_shares_outstanding_times_close_descending(self, db_path):
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            # BIG: 100 shares * 10 = 1000; SMALL: 10 shares * 10 = 100
            _insert_fundamentals(conn, "BIG", 100, date(2025, 1, 1), recorded_at=date(2025, 1, 2))
            _insert_fundamentals(conn, "SMALL", 10, date(2025, 1, 1), recorded_at=date(2025, 1, 2))
            _insert_price(conn, "BIG", date(2025, 6, 1), 10.0)
            _insert_price(conn, "SMALL", date(2025, 6, 1), 10.0)

            ranks = universe_mod.get_market_cap_rank_map_as_of(
                conn, ["BIG", "SMALL"], datetime(2025, 6, 15),
            )

        assert ranks == {"BIG": 1, "SMALL": 2}

    def test_no_lookahead_shares_outstanding_announced_after_as_of_is_ignored(self, db_path):
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            # Old quarter known well before as_of.
            _insert_fundamentals(conn, "AAA", 50, date(2024, 1, 1), fiscal_year=2024, quarter=1, recorded_at=date(2024, 1, 2))
            # A much bigger share count, but announced AFTER as_of — must not leak in.
            _insert_fundamentals(conn, "AAA", 5000, date(2025, 12, 1), fiscal_year=2025, quarter=3)
            _insert_price(conn, "AAA", date(2025, 6, 1), 10.0)
            _insert_fundamentals(conn, "BBB", 40, date(2024, 1, 1), recorded_at=date(2024, 1, 2))
            _insert_price(conn, "BBB", date(2025, 6, 1), 10.0)

            ranks = universe_mod.get_market_cap_rank_map_as_of(
                conn, ["AAA", "BBB"], datetime(2025, 6, 15),
            )

        # AAA's PIT market cap must use shares_outstanding=50 (known as of
        # 2025-06-15), not 5000 (announced 2025-12-01, in the future
        # relative to as_of) — 50*10=500 > 40*10=400, so AAA still ranks 1,
        # but for the RIGHT reason (not because 5000 leaked in).
        assert ranks == {"AAA": 1, "BBB": 2}

    def test_ticker_with_no_pit_fundamentals_gets_no_rank(self, db_path):
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            _insert_fundamentals(conn, "HAS_DATA", 100, date(2025, 1, 1), recorded_at=date(2025, 1, 2))
            _insert_price(conn, "HAS_DATA", date(2025, 6, 1), 10.0)
            # NO_FUNDAMENTALS has a price but never any fundamentals row.
            _insert_price(conn, "NO_FUNDAMENTALS", date(2025, 6, 1), 10.0)

            ranks = universe_mod.get_market_cap_rank_map_as_of(
                conn, ["HAS_DATA", "NO_FUNDAMENTALS"], datetime(2025, 6, 15),
            )

        assert ranks == {"HAS_DATA": 1}
        assert "NO_FUNDAMENTALS" not in ranks

    def test_ticker_with_no_price_as_of_date_gets_no_rank(self, db_path):
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            _insert_fundamentals(conn, "HAS_DATA", 100, date(2025, 1, 1), recorded_at=date(2025, 1, 2))
            _insert_price(conn, "HAS_DATA", date(2025, 6, 1), 10.0)
            # NO_PRICE has fundamentals but no ohlcv_adjusted row at all.
            _insert_fundamentals(conn, "NO_PRICE", 100, date(2025, 1, 1), recorded_at=date(2025, 1, 2))

            ranks = universe_mod.get_market_cap_rank_map_as_of(
                conn, ["HAS_DATA", "NO_PRICE"], datetime(2025, 6, 15),
            )

        assert ranks == {"HAS_DATA": 1}
        assert "NO_PRICE" not in ranks

    def test_empty_ticker_list_returns_empty_dict(self, db_path):
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            ranks = universe_mod.get_market_cap_rank_map_as_of(conn, [], datetime(2025, 6, 15))
        assert ranks == {}

    def test_uses_nearest_prior_trading_day_price_not_only_exact_match(self, db_path):
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            _insert_fundamentals(conn, "AAA", 100, date(2025, 1, 1), recorded_at=date(2025, 1, 2))
            # Last traded price is from a Friday; as_of is the following Monday
            # (no ohlcv_adjusted row exists exactly on that date).
            _insert_price(conn, "AAA", date(2025, 6, 6), 20.0)

            ranks = universe_mod.get_market_cap_rank_map_as_of(
                conn, ["AAA"], datetime(2025, 6, 9),
            )

        assert ranks == {"AAA": 1}
