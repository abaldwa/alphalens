"""
tests/unit/test_build_universe_fno_eligible.py

2026-07-21 full-codebase-review REV14: config/build_universe.py's
build_full_nse_universe_from_db previously hardcoded is_fno_eligible=False
for every ticker (the standalone NSE fo_mktlots.csv lot-size list this
project's docstrings once relied on serves a PDF, not a CSV, and is
network-unreachable from this environment anyway). Real fix: derive
is_fno_eligible directly from fno_data (real F&O bhavcopy, already
ingested by ingestion/scrapers/fno.py) — any ticker with real stock-
option/stock-future (STO/STF) rows in the trailing window is F&O
eligible. Real seeded DuckDB (ohlcv_adjusted + a companion fno_data file
via fno_db_path_for), no mocked business logic.
"""

from datetime import date, timedelta

import duckdb
import pytest

from config.build_universe import build_full_nse_universe_from_db
from datastore.api.db import fno_db_path_for, get_duckdb_connection
from datastore.schema import create_normalised
from datastore.schema.create_normalised import _CREATE_FNO_DATA


@pytest.fixture
def db_path(tmp_path):
    p = tmp_path / "normalised_test.duckdb"
    create_normalised.create_schema(db_path=p)
    return p


def _seed_ohlcv(db_path, tickers, days=100):
    today = date.today()
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        for t in tickers:
            for i in range(days):
                d = today - timedelta(days=i)
                conn.execute(
                    """
                    INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT DO NOTHING
                    """,
                    [d.isoformat(), t, 100.0, 101.0, 99.0, 100.0, 50_000.0],
                )


def _seed_fno(db_path, ticker_instruments, days_ago=5):
    """ticker_instruments: list of (ticker, instrument) tuples to write one real-shaped row for."""
    fno_path = fno_db_path_for(str(db_path))
    conn = duckdb.connect(str(fno_path))
    conn.execute(_CREATE_FNO_DATA)
    trade_date = (date.today() - timedelta(days=days_ago)).isoformat()
    expiry = (date.today() + timedelta(days=25)).isoformat()
    for ticker, instrument in ticker_instruments:
        conn.execute(
            """
            INSERT INTO fno_data
                (trade_date, ticker, instrument, expiry, strike, option_type,
                 oi, oi_change, volume, settle_price, close_price, underlying_price)
            VALUES (?, ?, ?, ?, NULL, NULL, 1000, 10, 5000, 100.0, 100.0, 100.0)
            """,
            [trade_date, ticker, instrument, expiry],
        )
    conn.close()


class TestIsFnoEligibleFromRealFnoData:
    def test_ticker_with_stock_option_activity_is_eligible(self, tmp_path, db_path):
        _seed_ohlcv(db_path, ["FNOCO", "NOFNOCO"])
        _seed_fno(db_path, [("FNOCO", "STO")])

        out = tmp_path / "universe.csv"
        df = build_full_nse_universe_from_db(output_path=out, db_path=db_path, active_days=90)

        row = df.set_index("ticker").loc["FNOCO"]
        assert bool(row["is_fno_eligible"]) is True

    def test_ticker_with_stock_future_activity_is_eligible(self, tmp_path, db_path):
        _seed_ohlcv(db_path, ["FUTCO"])
        _seed_fno(db_path, [("FUTCO", "STF")])

        out = tmp_path / "universe.csv"
        df = build_full_nse_universe_from_db(output_path=out, db_path=db_path, active_days=90)

        assert bool(df.set_index("ticker").loc["FUTCO", "is_fno_eligible"]) is True

    def test_ticker_with_no_fno_activity_is_not_eligible(self, tmp_path, db_path):
        _seed_ohlcv(db_path, ["NOFNOCO"])
        _seed_fno(db_path, [("SOMEOTHERCO", "STO")])  # different ticker

        out = tmp_path / "universe.csv"
        df = build_full_nse_universe_from_db(output_path=out, db_path=db_path, active_days=90)

        assert bool(df.set_index("ticker").loc["NOFNOCO", "is_fno_eligible"]) is False

    def test_index_derivatives_do_not_make_a_ticker_eligible(self, tmp_path, db_path):
        """IDO/IDF (index options/futures) are not per-ticker F&O eligibility
        signals — only STO/STF (stock options/futures) count."""
        _seed_ohlcv(db_path, ["IDXCO"])
        _seed_fno(db_path, [("IDXCO", "IDO")])

        out = tmp_path / "universe.csv"
        df = build_full_nse_universe_from_db(output_path=out, db_path=db_path, active_days=90)

        assert bool(df.set_index("ticker").loc["IDXCO", "is_fno_eligible"]) is False

    def test_stale_fno_activity_outside_active_days_window_is_not_eligible(self, tmp_path, db_path):
        _seed_ohlcv(db_path, ["STALECO"])
        _seed_fno(db_path, [("STALECO", "STO")], days_ago=200)

        out = tmp_path / "universe.csv"
        df = build_full_nse_universe_from_db(output_path=out, db_path=db_path, active_days=90)

        assert bool(df.set_index("ticker").loc["STALECO", "is_fno_eligible"]) is False

    def test_missing_fno_data_file_defaults_to_not_eligible_not_a_crash(self, tmp_path, db_path):
        _seed_ohlcv(db_path, ["NOCOMPANIONDB"])
        # No _seed_fno call at all — companion fno_data file never created.

        out = tmp_path / "universe.csv"
        df = build_full_nse_universe_from_db(output_path=out, db_path=db_path, active_days=90)

        assert bool(df.set_index("ticker").loc["NOCOMPANIONDB", "is_fno_eligible"]) is False
