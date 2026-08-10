"""
tests/unit/test_daily_pipeline.py

Phase: 0.6 (Laptop-Only Daily Operation)
Specs: SPEC-SCHED-001, SPEC-SCHED-005, SPEC-SCHED-009, SPEC-PIPE-001,
       SPEC-PIPE-002, SPEC-PIPE-005, SPEC-PIPE-006
Owner: Platform / Scheduler
Consumers: CI, pytest

Unit tests for ingestion/scheduler/daily_pipeline.py's concrete step_*
dispatch functions. All scraper calls are mocked — these tests never make
real network calls.
"""

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from datastore.api.db import get_duckdb_connection, get_sqlite_connection
from datastore.schema import create_normalised
from ingestion.scheduler import daily_pipeline


def _bhavcopy_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ticker": ["AAA", "BBB"],
            "open": [100.0, 200.0],
            "high": [101.0, 201.0],
            "low": [99.0, 199.0],
            "close": [100.5, 200.5],
            "volume": [1000, 2000],
            "traded_qty": [1000, 2000],
            "delivery_qty": [500, 1200],
            "series": ["EQ", "EQ"],
        }
    )


class TestStepDownloadBhavcopy:
    def test_writes_ohlcv_and_computed_delivery_pct(self, monkeypatch):
        """SPEC-PIPE-001, SPEC-PIPE-005: bhavcopy's own delivery_qty/traded_qty
        columns are sufficient to compute delivery_pct directly — no separate
        NSE fetch (nse_delivery_loader is backfill-only, see module docstring)."""
        create_normalised.create_schema(in_memory=True)
        from ingestion.scrapers import bhavcopy

        monkeypatch.setattr(bhavcopy, "download_bhavcopy", lambda date_str: _bhavcopy_df())

        with get_duckdb_connection(None) as conn:
            monkeypatch.setattr(
                daily_pipeline, "get_duckdb_connection", lambda path, persist=True: _FixedConn(conn)
            )
            daily_pipeline.step_download_bhavcopy(date(2026, 1, 5))

            row = conn.execute(
                "SELECT open, volume, delivery_qty, delivery_pct FROM ohlcv_adjusted WHERE ticker = 'AAA'"
            ).fetchone()
            assert row == (100.0, 1000, 500, 50.0)


class _FixedConn:
    """Context manager wrapper so a single real in-memory connection can
    stand in for get_duckdb_connection(path) without reopening a fresh
    (and therefore empty) in-memory database on every call."""

    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, *exc_info):
        return False


class TestStepDownloadFno:
    def test_defers_without_raising_when_today_before_cutoff(self, monkeypatch):
        """A56 follow-up (2026-07-30): a live attempt for today, before
        FNO_MIN_ATTEMPT_TIME, must be a clean no-op — no scrape attempted,
        no raise, nothing written — not a 'failed' checkpoint. NSE simply
        hasn't published the bhavcopy yet at this hour most days; that's
        not a real outage."""
        from datetime import datetime as dt_cls

        import config.timezone as timezone_mod
        from ingestion.scrapers import fno

        today = date(2026, 7, 30)
        monkeypatch.setattr(timezone_mod, "now_ist", lambda: dt_cls(2026, 7, 30, 18, 0))

        called = []
        monkeypatch.setattr(fno, "download_fno_bhavcopy", lambda date_str: called.append(date_str))

        daily_pipeline.step_download_fno(today)  # must not raise

        assert called == []

    def test_attempts_for_real_when_today_after_cutoff(self, monkeypatch):
        """Past FNO_MIN_ATTEMPT_TIME (default 21:00), a live attempt for
        today proceeds normally — this is the one real attempt each day,
        made by schedule_fno_late_catchup."""
        from datetime import datetime as dt_cls

        import config.timezone as timezone_mod
        from ingestion.scrapers import fno

        today = date(2026, 7, 30)
        monkeypatch.setattr(timezone_mod, "now_ist", lambda: dt_cls(2026, 7, 30, 21, 0))

        def _raise(date_str):
            raise ConnectionError("F&O bhavcopy unavailable")

        monkeypatch.setattr(fno, "download_fno_bhavcopy", _raise)

        with pytest.raises(ConnectionError):
            daily_pipeline.step_download_fno(today)

    def test_backfill_for_a_past_date_is_never_deferred(self, monkeypatch):
        """The cutoff only applies to a live attempt for TODAY — a
        backfill/catch-up call for a past date must always attempt for
        real, regardless of current wall-clock time."""
        from datetime import datetime as dt_cls

        import config.timezone as timezone_mod
        from ingestion.scrapers import fno

        create_normalised.create_schema(in_memory=True)
        past_date = date(2026, 7, 20)
        monkeypatch.setattr(timezone_mod, "now_ist", lambda: dt_cls(2026, 7, 30, 9, 0))

        one_row_df = pd.DataFrame(
            {
                "ticker": ["AAA"], "instrument": ["STF"], "expiry": pd.to_datetime(["2026-07-29"]),
                "strike": [None], "option_type": [None], "oi": [1000], "oi_change": [50],
                "volume": [200], "settle_price": [102.0], "close_price": [102.0], "underlying_price": [101.5],
            }
        )
        called = []

        def _fake(date_str):
            called.append(date_str)
            return one_row_df

        monkeypatch.setattr(fno, "download_fno_bhavcopy", _fake)

        with get_duckdb_connection(None) as conn:
            monkeypatch.setattr(
                daily_pipeline, "get_duckdb_connection", lambda path, persist=True: _FixedConn(conn)
            )
            daily_pipeline.step_download_fno(past_date)

        assert called == ["2026-07-20"]

    def test_scraper_failure_propagates(self, monkeypatch):
        """2026-07-29: promoted to critical — a scrape failure must now
        raise so the checkpoint is honestly marked 'failed' and picked up
        by gap-backfill retry, instead of always recording 'success' even
        when nothing was written (the root cause of 6 silently-missed
        trading days, see step_download_fno's docstring)."""
        from ingestion.scrapers import fno

        def _raise(date_str):
            raise ConnectionError("F&O bhavcopy unavailable")

        monkeypatch.setattr(fno, "download_fno_bhavcopy", _raise)

        with pytest.raises(ConnectionError):
            daily_pipeline.step_download_fno(date(2026, 1, 5))

    def test_success_is_persisted_to_fno_data(self, monkeypatch):
        """P2.3: fno_data now exists — a successful fetch is written there,
        not just logged (see ingestion/scrapers/fno.py's module docstring
        for the real UDiFF endpoint fix this replaced)."""
        create_normalised.create_schema(in_memory=True)
        from ingestion.scrapers import fno

        df = pd.DataFrame(
            {
                "ticker": ["AAA", "AAA"],
                "instrument": ["STF", "STO"],
                "expiry": pd.to_datetime(["2026-01-29", "2026-01-29"]),
                "strike": [None, 100.0],
                "option_type": [None, "CE"],
                "oi": [1000, 500],
                "oi_change": [50, -10],
                "volume": [200, 80],
                "settle_price": [102.0, 5.5],
                "close_price": [102.0, 5.5],
                "underlying_price": [101.5, 101.5],
            }
        )
        monkeypatch.setattr(fno, "download_fno_bhavcopy", lambda date_str: df)

        with get_duckdb_connection(None) as conn:
            monkeypatch.setattr(
                daily_pipeline, "get_duckdb_connection", lambda path, persist=True: _FixedConn(conn)
            )
            daily_pipeline.step_download_fno(date(2026, 1, 5))

            rows = conn.execute("SELECT ticker, instrument, oi FROM fno_data ORDER BY instrument").fetchall()
            assert rows == [("AAA", "STF", 1000), ("AAA", "STO", 500)]

    def test_db_write_failure_propagates(self, monkeypatch):
        """2026-07-29: now critical (see class docstring above) — a
        cross-process DuckDB lock conflict (SPEC-SCHED-013) on the write
        must raise same as a scraper failure, so the checkpoint is marked
        'failed' and retried rather than silently recorded as 'success'."""
        from ingestion.scrapers import fno

        df = pd.DataFrame(
            {
                "ticker": ["AAA"], "instrument": ["STF"], "expiry": pd.to_datetime(["2026-01-29"]),
                "strike": [None], "option_type": [None], "oi": [1000], "oi_change": [50],
                "volume": [200], "settle_price": [102.0], "close_price": [102.0], "underlying_price": [101.5],
            }
        )
        monkeypatch.setattr(fno, "download_fno_bhavcopy", lambda date_str: df)

        def _raise_lock_conflict(path, persist=True):
            raise RuntimeError('IO Error: Could not set lock on file "alphalens.duckdb"')

        monkeypatch.setattr(daily_pipeline, "get_duckdb_connection", _raise_lock_conflict)

        with pytest.raises(RuntimeError):
            daily_pipeline.step_download_fno(date(2026, 1, 5))

    def test_rerun_for_same_date_replaces_not_duplicates(self, monkeypatch):
        """Delete-then-insert per trade_date (see datastore/schema/create_normalised.py's
        _CREATE_FNO_DATA comment) — a retry must not duplicate rows."""
        create_normalised.create_schema(in_memory=True)
        from ingestion.scrapers import fno

        df = pd.DataFrame(
            {
                "ticker": ["AAA"], "instrument": ["STF"], "expiry": pd.to_datetime(["2026-01-29"]),
                "strike": [None], "option_type": [None], "oi": [1000], "oi_change": [50],
                "volume": [200], "settle_price": [102.0], "close_price": [102.0], "underlying_price": [101.5],
            }
        )
        monkeypatch.setattr(fno, "download_fno_bhavcopy", lambda date_str: df)

        with get_duckdb_connection(None) as conn:
            monkeypatch.setattr(
                daily_pipeline, "get_duckdb_connection", lambda path, persist=True: _FixedConn(conn)
            )
            daily_pipeline.step_download_fno(date(2026, 1, 5))
            daily_pipeline.step_download_fno(date(2026, 1, 5))

            count = conn.execute("SELECT COUNT(*) FROM fno_data WHERE trade_date = '2026-01-05'").fetchone()[0]
            assert count == 1


class TestStepDownloadCorporateActions:
    def test_scraper_failure_propagates(self, monkeypatch):
        """2026-07-30: promoted to critical, same fix as step_download_fno
        (2026-07-29) — a scrape failure must now raise so the checkpoint is
        honestly marked 'failed' and picked up by gap-backfill retry,
        instead of always recording 'success' with zero rows written."""
        from ingestion.scrapers import corporate_actions

        def _raise(date_str):
            raise ConnectionError("NSE corporate actions API unavailable")

        monkeypatch.setattr(corporate_actions, "download_corporate_actions", _raise)

        with pytest.raises(ConnectionError):
            daily_pipeline.step_download_corporate_actions(date(2026, 1, 5))

    def test_success_is_persisted(self, monkeypatch):
        create_normalised.create_schema(in_memory=True)
        from ingestion.scrapers import corporate_actions

        df = pd.DataFrame(
            {
                "ticker": ["AAA"],
                "ex_date": [date(2026, 1, 5)],
                "action_type": ["DIVIDEND"],
                "ratio": [2.0],
                "announcement_date": [None],
                "record_date": [date(2026, 1, 6)],
                "details": ["Dividend - Rs 2 Per Share"],
            }
        )
        monkeypatch.setattr(corporate_actions, "download_corporate_actions", lambda date_str: df)

        with get_duckdb_connection(None) as conn:
            monkeypatch.setattr(
                daily_pipeline, "get_duckdb_connection", lambda path, persist=True: _FixedConn(conn)
            )
            daily_pipeline.step_download_corporate_actions(date(2026, 1, 5))

            rows = conn.execute(
                "SELECT ticker, action_type FROM corporate_actions WHERE ex_date = '2026-01-05'"
            ).fetchall()
            assert rows == [("AAA", "DIVIDEND")]

    def test_db_write_failure_propagates(self, monkeypatch):
        """2026-07-30: a cross-process DuckDB lock conflict on the write
        must raise same as a scraper failure, so the checkpoint is marked
        'failed' and retried rather than silently recorded as 'success'."""
        from ingestion.scrapers import corporate_actions

        df = pd.DataFrame(
            {
                "ticker": ["AAA"], "ex_date": [date(2026, 1, 5)], "action_type": ["DIVIDEND"],
                "ratio": [2.0], "announcement_date": [None], "record_date": [date(2026, 1, 6)],
                "details": ["Dividend - Rs 2 Per Share"],
            }
        )
        monkeypatch.setattr(corporate_actions, "download_corporate_actions", lambda date_str: df)

        def _raise_lock_conflict(path, persist=True):
            raise RuntimeError('IO Error: Could not set lock on file "alphalens.duckdb"')

        monkeypatch.setattr(daily_pipeline, "get_duckdb_connection", _raise_lock_conflict)

        with pytest.raises(RuntimeError):
            daily_pipeline.step_download_corporate_actions(date(2026, 1, 5))


class TestStepDownloadIndexOhlcv:
    def test_scraper_failure_is_caught_and_non_fatal(self, monkeypatch):
        """Non-critical (sector-rotation report + backtest benchmark only,
        neither on the critical path) — a scraper-side failure must never
        raise."""
        from ingestion.scrapers import nse_indices

        def _raise(date_str):
            raise ConnectionError("indices-close CSV unavailable")

        monkeypatch.setattr(nse_indices, "download_index_ohlcv", _raise)

        daily_pipeline.step_download_index_ohlcv(date(2026, 1, 5))  # must not raise

    def test_db_write_failure_is_caught_and_non_fatal(self, monkeypatch):
        """A31: the DB write (missing-table Catalog Error, cross-process
        DuckDB lock conflict — both observed live during a 2026-07 backfill)
        previously escaped the try/except that only wrapped the scraper
        fetch, failing the whole step despite it being documented as
        always-non-critical. Must be caught same as a scraper failure."""
        from ingestion.scrapers import nse_indices

        df = pd.DataFrame(
            {
                "index_name": ["Nifty 50"],
                "open": [100.0], "high": [101.0], "low": [99.0],
                "close": [100.5], "volume": [1000],
            }
        )
        monkeypatch.setattr(nse_indices, "download_index_ohlcv", lambda date_str: df)

        def _raise_lock_conflict(path, persist=True):
            raise RuntimeError('IO Error: Could not set lock on file "alphalens.duckdb"')

        monkeypatch.setattr(daily_pipeline, "get_duckdb_connection", _raise_lock_conflict)

        daily_pipeline.step_download_index_ohlcv(date(2026, 1, 5))  # must not raise

    def test_success_is_persisted_to_index_ohlcv(self, monkeypatch):
        create_normalised.create_schema(in_memory=True)
        from ingestion.scrapers import nse_indices

        df = pd.DataFrame(
            {
                "index_name": ["Nifty 50", "Nifty Bank"],
                "open": [100.0, 200.0], "high": [101.0, 201.0], "low": [99.0, 199.0],
                "close": [100.5, 200.5], "volume": [1000, 2000],
            }
        )
        monkeypatch.setattr(nse_indices, "download_index_ohlcv", lambda date_str: df)

        with get_duckdb_connection(None) as conn:
            monkeypatch.setattr(
                daily_pipeline, "get_duckdb_connection", lambda path, persist=True: _FixedConn(conn)
            )
            daily_pipeline.step_download_index_ohlcv(date(2026, 1, 5))

            rows = conn.execute(
                "SELECT index_name, close FROM index_ohlcv ORDER BY index_name"
            ).fetchall()
            assert rows == [("Nifty 50", 100.5), ("Nifty Bank", 200.5)]

    def test_rerun_for_same_date_upserts_not_duplicates(self, monkeypatch):
        create_normalised.create_schema(in_memory=True)
        from ingestion.scrapers import nse_indices

        df = pd.DataFrame(
            {
                "index_name": ["Nifty 50"],
                "open": [100.0], "high": [101.0], "low": [99.0],
                "close": [100.5], "volume": [1000],
            }
        )
        monkeypatch.setattr(nse_indices, "download_index_ohlcv", lambda date_str: df)

        with get_duckdb_connection(None) as conn:
            monkeypatch.setattr(
                daily_pipeline, "get_duckdb_connection", lambda path, persist=True: _FixedConn(conn)
            )
            daily_pipeline.step_download_index_ohlcv(date(2026, 1, 5))
            daily_pipeline.step_download_index_ohlcv(date(2026, 1, 5))

            count = conn.execute(
                "SELECT COUNT(*) FROM index_ohlcv WHERE date = '2026-01-05'"
            ).fetchone()[0]
            assert count == 1


class TestStepDownloadMacro:
    """2026-07 (backlog #1/#2/#3, Sub-task C): step_download_macro is now a
    no-op placeholder — VIX/FII-DII/USD-INR/global indices moved to
    step_download_macro_morning (07:30 IST), see TestStepDownloadMacroMorning."""

    def test_is_a_noop_and_never_raises(self):
        create_normalised.create_schema(in_memory=True)
        daily_pipeline.step_download_macro(date(2026, 1, 5))  # must not raise


def _patch_global_indices(monkeypatch, macro, value_map=None):
    """Stub out the six new global-index fetches (5 indices + DXY) so unit tests never hit
    the network — each is independently caught in step_download_macro_morning
    the same way VIX/FII-DII/USD-INR are (SPEC-PIPE-006)."""
    value_map = value_map or {
        "download_nasdaq": ("nasdaq_composite", 18000.0),
        "download_dow": ("dow_jones", 42000.0),
        "download_sp500": ("sp500", 6000.0),
        "download_nikkei": ("nikkei_225", 39000.0),
        "download_hangseng": ("hang_seng", 19000.0),
        "download_dxy": ("dxy", 101.5),
        "download_crude_oil": ("crude_oil_price", 82.3),
        "download_gold": ("gold_price", 2350.0),
    }
    for fn_name, (key, value) in value_map.items():
        monkeypatch.setattr(macro, fn_name, lambda d, db_path=None, k=key, v=value: {k: v})


class TestStepDownloadMacroMorning:
    """07:30 IST morning-catchup macro capture (2026-07, backlog #1/#2/#3,
    Sub-tasks B/C) — the successor to the old step_download_macro."""

    def test_writes_all_nine_indicators(self, monkeypatch):
        create_normalised.create_schema(in_memory=True)
        from ingestion.scrapers import macro

        monkeypatch.setattr(macro, "download_vix", lambda d, db_path=None: 14.5)
        monkeypatch.setattr(
            macro,
            "download_fiidii",
            lambda d, db_path=None: {
                "fii_net_cr": -120.5,
                "dii_net_cr": 340.2,
                "is_stale": False,
            },
        )
        monkeypatch.setattr(macro, "download_fx", lambda d, db_path=None: {"usd_inr": 83.2})
        _patch_global_indices(monkeypatch, macro)
        monkeypatch.setattr(
            macro, "download_bond_yields", lambda d, db_path=None: {"yield_10yr": 7.02, "yield_3m": 5.39}
        )
        from ingestion.scrapers import macro_real_economy
        monkeypatch.setattr(macro_real_economy, "upsert_macro_real_economy_parquet", lambda d: 0)
        import pandas as pd
        from ingestion.scrapers import nse_corporate_announcements
        monkeypatch.setattr(
            nse_corporate_announcements, "download_corporate_announcements", lambda f, t: pd.DataFrame()
        )

        with get_duckdb_connection(None) as conn:
            monkeypatch.setattr(
                daily_pipeline, "get_duckdb_connection", lambda path, persist=True: _FixedConn(conn)
            )
            daily_pipeline.step_download_macro_morning(date(2026, 1, 5))

            rows = dict(
                conn.execute(
                    "SELECT indicator, value FROM macro_indicators WHERE date = '2026-01-05'"
                ).fetchall()
            )
            assert rows == {
                "INDIA_VIX": 14.5,
                "FII_NET_CR": -120.5,
                "DII_NET_CR": 340.2,
                "USD_INR": 83.2,
                "NASDAQ_COMPOSITE": 18000.0,
                "DOW_JONES": 42000.0,
                "SP500": 6000.0,
                "NIKKEI_225": 39000.0,
                "HANG_SENG": 19000.0,
                "DXY": 101.5,
                "CRUDE_OIL": 82.3,
                "GOLD": 2350.0,
                "YIELD_10YR": 7.02,
                "YIELD_3M": 5.39,
            }

    def test_one_indicator_failing_does_not_block_the_others(self, monkeypatch):
        """SPEC-PIPE-006: each macro source fails independently — VIX
        outage must not prevent FII/DII or USD/INR from being written."""
        create_normalised.create_schema(in_memory=True)
        from ingestion.scrapers import macro

        def _raise_vix(d, db_path=None):
            raise ConnectionError("VIX unavailable")

        monkeypatch.setattr(macro, "download_vix", _raise_vix)
        monkeypatch.setattr(
            macro,
            "download_fiidii",
            lambda d, db_path=None: {"fii_net_cr": 10.0, "dii_net_cr": 20.0, "is_stale": False},
        )
        monkeypatch.setattr(macro, "download_fx", lambda d, db_path=None: {"usd_inr": 83.0})
        _patch_global_indices(monkeypatch, macro)
        monkeypatch.setattr(
            macro, "download_bond_yields", lambda d, db_path=None: {"yield_10yr": 7.0, "yield_3m": 5.3}
        )
        from ingestion.scrapers import macro_real_economy
        monkeypatch.setattr(macro_real_economy, "upsert_macro_real_economy_parquet", lambda d: 0)
        import pandas as pd
        from ingestion.scrapers import nse_corporate_announcements
        monkeypatch.setattr(
            nse_corporate_announcements, "download_corporate_announcements", lambda f, t: pd.DataFrame()
        )

        with get_duckdb_connection(None) as conn:
            monkeypatch.setattr(
                daily_pipeline, "get_duckdb_connection", lambda path, persist=True: _FixedConn(conn)
            )
            daily_pipeline.step_download_macro_morning(date(2026, 1, 5))  # must not raise

            rows = dict(
                conn.execute(
                    "SELECT indicator, value FROM macro_indicators WHERE date = '2026-01-05'"
                ).fetchall()
            )
            assert "INDIA_VIX" not in rows
            assert rows["FII_NET_CR"] == 10.0
            assert rows["USD_INR"] == 83.0

    def test_all_sources_failing_does_not_raise(self, monkeypatch):
        create_normalised.create_schema(in_memory=True)
        from ingestion.scrapers import macro

        def _raise(d, db_path=None):
            raise ConnectionError("unavailable")

        monkeypatch.setattr(macro, "download_vix", _raise)
        monkeypatch.setattr(macro, "download_fiidii", _raise)
        monkeypatch.setattr(macro, "download_fx", _raise)
        monkeypatch.setattr(macro, "download_nasdaq", _raise)
        monkeypatch.setattr(macro, "download_dow", _raise)
        monkeypatch.setattr(macro, "download_sp500", _raise)
        monkeypatch.setattr(macro, "download_nikkei", _raise)
        monkeypatch.setattr(macro, "download_hangseng", _raise)
        monkeypatch.setattr(macro, "download_dxy", _raise)
        monkeypatch.setattr(macro, "download_crude_oil", _raise)
        monkeypatch.setattr(macro, "download_gold", _raise)
        monkeypatch.setattr(macro, "download_bond_yields", _raise)
        from ingestion.scrapers import macro_real_economy
        monkeypatch.setattr(macro_real_economy, "upsert_macro_real_economy_parquet", _raise)
        from ingestion.scrapers import nse_corporate_announcements

        def _raise_announcements(from_date, to_date):
            raise ConnectionError("unavailable")

        monkeypatch.setattr(
            nse_corporate_announcements, "download_corporate_announcements", _raise_announcements
        )

        daily_pipeline.step_download_macro_morning(date(2026, 1, 5))  # must not raise


class TestStepAdjustPrices:
    def test_calls_adjust_for_corporate_actions_only_for_tickers_with_actions(self, monkeypatch):
        """
        2026-07-10 lock-hold-time remediation: step_adjust_prices now
        pre-filters to tickers that actually have a corporate_actions row
        before opening the write connection, instead of holding the write
        lock for a full-universe loop where most tickers are a no-op. CCC
        has no corporate_actions row and must be skipped entirely.
        """
        create_normalised.create_schema(in_memory=True)
        from config import universe
        from ingestion.adjust import price_adjuster

        monkeypatch.setattr(universe, "get_tickers", lambda: ["AAA", "BBB", "CCC"])
        calls = []
        monkeypatch.setattr(
            price_adjuster, "adjust_for_corporate_actions", lambda conn, ticker: calls.append(ticker)
        )

        with get_duckdb_connection(None) as conn:
            conn.execute(
                "INSERT INTO corporate_actions (ticker, ex_date, action_type, ratio) VALUES "
                "('AAA', '2026-01-01', 'split', 2.0), ('BBB', '2026-01-02', 'bonus', 1.5)"
            )
            monkeypatch.setattr(
                daily_pipeline, "get_duckdb_connection",
                lambda path, persist=True, read_only=False: _FixedConn(conn),
            )
            daily_pipeline.step_adjust_prices(date(2026, 1, 5))

        assert calls == ["AAA", "BBB"]


class TestStepComputeFeatures:
    """[AS BUILT, P1.7] step_compute_features wired to features/matrix_builder.py + features/pnd_features.py."""

    def test_builds_both_matrices_and_saves_pnd_parquet(self, monkeypatch, tmp_path):
        import numpy as np
        import features.matrix_builder as matrix_builder_mod
        from config import settings
        from config import universe as universe_mod
        from features.pnd_features import PND_FEATURES

        monkeypatch.setattr(universe_mod, "get_tickers_for_feature_engineering", lambda: ["AAA", "BBB"])

        # build_feature_matrix now includes PND columns; step_compute_features
        # extracts them from the matrix instead of making a second bulk call.
        pnd_row = {col: np.nan for col in PND_FEATURES}
        mock_matrix = pd.DataFrame([
            {"date": pd.Timestamp("2026-01-05"), "ticker": "AAA", **pnd_row},
            {"date": pd.Timestamp("2026-01-05"), "ticker": "BBB", **pnd_row},
        ])
        monkeypatch.setattr(
            matrix_builder_mod, "build_feature_matrix", lambda *a, **k: mock_matrix
        )

        pnd_dir = tmp_path / "daily_pnd"
        monkeypatch.setattr(settings, "FEATURES_PND_DAILY_DIR", pnd_dir)

        daily_pipeline.step_compute_features(date(2026, 1, 5))

        saved = pd.read_parquet(pnd_dir / "2026-01-05.parquet")
        assert sorted(saved["ticker"].to_list()) == ["AAA", "BBB"]

    def test_advanced_technical_computed_daily_except_fracdiff(self, monkeypatch, tmp_path):
        """[2026-08-10] Default flipped: the live daily pipeline now computes
        the advanced_technical block (used_only=False) but skips the 3
        fracdiff columns (skip_fracdiff=True).

        The 2026-08-04 default of used_only=True rested on "only
        hurst_exp_21d is used downstream", which Category-T templates
        falsified — they read hurst_exp_63d, the wavelet and entropy
        columns, rqa_rec_rate, lyapunov_exponent_proxy,
        time_series_complexity and nonlinear_trend_strength by name.
        Profiling also showed the cost was never spread across those 17:
        _optimal_fracdiff_d alone is 98 pct of the module (0.502s of a
        0.507s bar on a 21-year panel), so the expensive part is excluded
        specifically instead of the whole block."""
        import features.matrix_builder as matrix_builder_mod
        from config import settings
        from config import universe as universe_mod
        from features.pnd_features import PND_FEATURES

        monkeypatch.setattr(universe_mod, "get_tickers_for_feature_engineering", lambda: ["AAA"])

        captured_kwargs = {}
        pnd_row = {col: None for col in PND_FEATURES}
        mock_matrix = pd.DataFrame([{"date": pd.Timestamp("2026-01-05"), "ticker": "AAA", **pnd_row}])

        def _fake_build_feature_matrix(*args, **kwargs):
            captured_kwargs.update(kwargs)
            return mock_matrix

        monkeypatch.setattr(matrix_builder_mod, "build_feature_matrix", _fake_build_feature_matrix)
        monkeypatch.setattr(settings, "FEATURES_PND_DAILY_DIR", tmp_path / "daily_pnd")

        daily_pipeline.step_compute_features(date(2026, 1, 5))

        assert captured_kwargs["advanced_technical_used_only"] is False
        assert captured_kwargs["advanced_technical_skip_fracdiff"] is True

    def test_advanced_technical_used_only_can_be_overridden_false(self, monkeypatch, tmp_path):
        import features.matrix_builder as matrix_builder_mod
        from config import settings
        from config import universe as universe_mod
        from features.pnd_features import PND_FEATURES

        monkeypatch.setattr(universe_mod, "get_tickers_for_feature_engineering", lambda: ["AAA"])

        captured_kwargs = {}
        pnd_row = {col: None for col in PND_FEATURES}
        mock_matrix = pd.DataFrame([{"date": pd.Timestamp("2026-01-05"), "ticker": "AAA", **pnd_row}])

        def _fake_build_feature_matrix(*args, **kwargs):
            captured_kwargs.update(kwargs)
            return mock_matrix

        monkeypatch.setattr(matrix_builder_mod, "build_feature_matrix", _fake_build_feature_matrix)
        monkeypatch.setattr(settings, "FEATURES_PND_DAILY_DIR", tmp_path / "daily_pnd")

        daily_pipeline.step_compute_features(date(2026, 1, 5), advanced_technical_used_only=False)

        assert captured_kwargs["advanced_technical_used_only"] is False


class TestStepRunModels:
    """[AS BUILT, P1.7] step_run_models wired to systems/ml_signal_engine/inference/daily_inference.py."""

    def test_raises_when_daily_inference_halts(self, monkeypatch, tmp_path):
        from config import settings
        import systems.ml_signal_engine.inference.daily_inference as di_mod
        from datastore import client as client_mod

        feat_dir, pnd_dir, logs_dir = tmp_path / "daily", tmp_path / "daily_pnd", tmp_path / "logs"
        feat_dir.mkdir()
        pnd_dir.mkdir()
        pd.DataFrame({"ticker": ["AAA"]}).to_parquet(feat_dir / "2026-01-05.parquet")
        pd.DataFrame({"ticker": ["AAA"]}).to_parquet(pnd_dir / "2026-01-05.parquet")
        monkeypatch.setattr(settings, "FEATURES_DAILY_DIR", feat_dir)
        monkeypatch.setattr(settings, "FEATURES_PND_DAILY_DIR", pnd_dir)
        monkeypatch.setattr(settings, "LOGS_DIR", logs_dir)

        monkeypatch.setattr(client_mod.DataStoreClient, "get_ohlcv", lambda self, ticker, f, t: [])
        monkeypatch.setattr(
            di_mod, "run_daily_inference",
            lambda **kwargs: {
                "halted": True, "halt_reason": "PSI drift halt: test", "tickers_scored": 0, "pnd_blocked": [],
            },
        )

        with pytest.raises(RuntimeError, match="halted"):
            daily_pipeline.step_run_models(date(2026, 1, 5))

        with open(logs_dir / "daily_inference" / "2026-01-05.json") as f:
            import json
            assert json.load(f)["halted"] is True

    def test_does_not_raise_when_run_succeeds(self, monkeypatch, tmp_path):
        from config import settings
        import systems.ml_signal_engine.inference.daily_inference as di_mod
        from datastore import client as client_mod

        feat_dir, pnd_dir, logs_dir = tmp_path / "daily", tmp_path / "daily_pnd", tmp_path / "logs"
        feat_dir.mkdir()
        pnd_dir.mkdir()
        pd.DataFrame({"ticker": ["AAA"]}).to_parquet(feat_dir / "2026-01-05.parquet")
        pd.DataFrame({"ticker": ["AAA"]}).to_parquet(pnd_dir / "2026-01-05.parquet")
        monkeypatch.setattr(settings, "FEATURES_DAILY_DIR", feat_dir)
        monkeypatch.setattr(settings, "FEATURES_PND_DAILY_DIR", pnd_dir)
        monkeypatch.setattr(settings, "LOGS_DIR", logs_dir)

        monkeypatch.setattr(client_mod.DataStoreClient, "get_ohlcv", lambda self, ticker, f, t: [])
        monkeypatch.setattr(
            di_mod, "run_daily_inference",
            lambda **kwargs: {
                "halted": False, "halt_reason": None, "tickers_scored": 1, "pnd_blocked": [],
            },
        )

        daily_pipeline.step_run_models(date(2026, 1, 5))  # must not raise


class TestStepWriteSignals:
    """[AS BUILT, P1.7] step_write_signals verifies step_run_models' result file."""

    def test_raises_if_upstream_run_halted(self, monkeypatch, tmp_path):
        from config import settings

        logs_dir = tmp_path / "logs"
        result_dir = logs_dir / "daily_inference"
        result_dir.mkdir(parents=True)
        with open(result_dir / "2026-01-05.json", "w") as f:
            import json
            json.dump({"halted": True, "halt_reason": "PSI drift halt: test", "tickers_scored": 0}, f)
        monkeypatch.setattr(settings, "LOGS_DIR", logs_dir)

        with pytest.raises(RuntimeError, match="halted"):
            daily_pipeline.step_write_signals(date(2026, 1, 5))

    def test_succeeds_if_upstream_run_completed(self, monkeypatch, tmp_path):
        from config import settings

        logs_dir = tmp_path / "logs"
        result_dir = logs_dir / "daily_inference"
        result_dir.mkdir(parents=True)
        with open(result_dir / "2026-01-05.json", "w") as f:
            import json
            json.dump({"halted": False, "halt_reason": None, "tickers_scored": 5}, f)
        monkeypatch.setattr(settings, "LOGS_DIR", logs_dir)

        daily_pipeline.step_write_signals(date(2026, 1, 5))  # must not raise

    def test_raises_file_not_found_if_run_models_never_ran(self, tmp_path, monkeypatch):
        from config import settings

        monkeypatch.setattr(settings, "LOGS_DIR", tmp_path / "logs_never_written")
        with pytest.raises(FileNotFoundError):
            daily_pipeline.step_write_signals(date(2099, 1, 1))


class TestStepRunnerDispatch:
    def test_dispatches_to_the_correct_step_function(self, monkeypatch):
        calls = []
        monkeypatch.setitem(
            daily_pipeline._STEP_DISPATCH, "download_bhavcopy", lambda run_date: calls.append(run_date)
        )
        daily_pipeline.step_runner(date(2026, 1, 5), "download_bhavcopy")
        assert calls == [date(2026, 1, 5)]

    def test_unknown_step_name_raises_key_error(self):
        with pytest.raises(KeyError):
            daily_pipeline.step_runner(date(2026, 1, 5), "not_a_real_step")


class TestRunDailyPipelineOnceRecordsPipelineRuns:
    def test_success_is_recorded_to_pipeline_runs(self, monkeypatch, tmp_path):
        """SPEC-SCHED-005: previously nothing ever wrote to pipeline_runs,
        so gap_detector's get_last_successful_run_date() could never see
        any history — the startup catch-up would never trigger. This
        verifies run_daily_pipeline_once -> run_startup_sequence now
        records a 'success' row once every step succeeds (step_runner is
        stubbed here purely to isolate this recording behavior from the
        real step dispatch, which is covered by the tests above)."""
        sqlite_path = tmp_path / "pipeline_log.db"
        from datastore.schema.create_signals import create_pipeline_runs_schema

        create_pipeline_runs_schema(db_path=sqlite_path)

        import config.settings as settings

        monkeypatch.setattr(settings, "PIPELINE_LOG_DB_PATH", sqlite_path)

        from ingestion.scheduler import pipeline_scheduler

        monkeypatch.setattr(
            pipeline_scheduler, "detect_gaps", lambda today=None, db_path=None: []
        )
        monkeypatch.setattr(pipeline_scheduler, "is_trading_day", lambda d: True)

        def _all_succeed(run_date, step_name):
            return None

        monkeypatch.setattr(daily_pipeline, "step_runner", _all_succeed)

        ok = daily_pipeline.run_daily_pipeline_once(today=date(2026, 1, 5))

        assert ok is True

        with get_sqlite_connection(sqlite_path) as conn:
            row = conn.execute(
                "SELECT status FROM pipeline_runs WHERE date = '2026-01-05'"
            ).fetchone()
        assert row == ("success",)


class TestSanityKnownSparseColumns:
    """A54 (2026-07-10): capex_to_assets/noncash_assets_ratio/
    intangibles_growth/audit_qualification_flag/goodwill_ratio were
    incorrectly believed unsourceable (stale FO8/A26 claim, predating
    ingestion/scrapers/nse_xbrl_financials.py's real structured parser) --
    they read real, populated NSE XBRL columns and must NOT be exempted
    from step_sanity_check's all-NaN floor, so a future regression in their
    computation is still caught. Genuinely-unsourceable columns (no schema
    column at all, or freeform-text-only disclosures) remain exempted."""

    def test_now_real_columns_are_not_exempted(self):
        assert "capex_to_assets" not in daily_pipeline._SANITY_KNOWN_SPARSE_COLUMNS
        assert "noncash_assets_ratio" not in daily_pipeline._SANITY_KNOWN_SPARSE_COLUMNS
        assert "intangibles_growth" not in daily_pipeline._SANITY_KNOWN_SPARSE_COLUMNS
        assert "audit_qualification_flag" not in daily_pipeline._SANITY_KNOWN_SPARSE_COLUMNS
        assert "goodwill_ratio" not in daily_pipeline._SANITY_KNOWN_SPARSE_COLUMNS

    def test_genuinely_unsourceable_columns_are_still_exempted(self):
        assert "contingent_liability_ratio" in daily_pipeline._SANITY_KNOWN_SPARSE_COLUMNS
        assert "subsidiary_count" in daily_pipeline._SANITY_KNOWN_SPARSE_COLUMNS
        assert "board_independence" in daily_pipeline._SANITY_KNOWN_SPARSE_COLUMNS
        # [2026-08-08] benford_mad was exempted as a known-sparse/unsourceable
        # column, but it is now 90.9% populated in feature parquets, so it must
        # no longer be exempted from the all-NaN hard-floor.
        assert "benford_mad" not in daily_pipeline._SANITY_KNOWN_SPARSE_COLUMNS

    def test_raw_fundamentals_source_columns_are_exempted(self):
        """[2026-08-10] The raw fundamentals columns behind the exempted
        derived ratios must be exempted too. datastore/integrity/checks.py::
        check_null_sweep imports this same set but scans the *fundamentals
        table* rather than feature parquets, so exempting only the derived
        ratios left these 4 raising critical findings — which failed
        data_integrity_check and cascade-skipped compute_features,
        run_models, write_signals and paper_trade for a whole day.

        Measured 2026-08-10: 0 non-null of 53,182 rows for each, all history.
        """
        for col in (
            "contingent_liabilities",
            "subsidiary_count_raw",
            "loans_to_related_parties",
            "director_remuneration",
        ):
            assert col in daily_pipeline._SANITY_KNOWN_SPARSE_COLUMNS, (
                f"{col} is 100% NaN in the fundamentals table; de-exempting it "
                "makes check_null_sweep fail data_integrity_check every day"
            )


class TestFyersAdjFactorInvariant:
    """[2026-08-10, user decision] Every FYERS write path must set
    adj_factor/vol_adj_factor to the literal 1.0 — no exceptions.

    FYERS serves prices already adjusted as-of-fetch-time, so a FYERS row is
    by definition unadjusted-by-us. Passing the factors through from staging
    made 1.0 merely implicit (via ADD COLUMN IF NOT EXISTS ... DEFAULT 1.0,
    a no-op when the staged frame already has the column), and on the UPSERT
    path let a stale non-1.0 factor survive a FYERS price overwrite — which
    a later scripts/run_price_adjuster.py pass would double-adjust.
    """

    def _sql_of(self, path):
        return Path(path).read_text()

    def test_daily_fyers_upsert_writes_literal_one(self):
        sql = self._sql_of("ingestion/scheduler/daily_pipeline.py")
        # The FYERS publish statement, identified by its SELECT ... FROM
        # staging line (the file's first INSERT INTO ohlcv_adjusted is the
        # bhavcopy upsert, and the staging table name also appears in
        # earlier ALTER statements).
        assert (
            "SELECT date, ticker, open, high, low, close, volume, 1.0, 1.0, 'fyers'\n"
            "                    FROM staging.ohlcv_fyers_daily"
        ) in sql, (
            "step_download_fyers_daily must INSERT literal 1.0 factors, "
            "not pass staging's adj_factor through"
        )
        conflict = sql.split("FROM staging.ohlcv_fyers_daily\n")[1].split('"""')[0]
        assert "adj_factor     = 1.0" in conflict
        assert "vol_adj_factor = 1.0" in conflict
        assert "excluded.adj_factor" not in conflict

    def test_bhavcopy_upsert_also_writes_literal_one(self):
        """Bhavcopy stores raw NSE prices, so its factors are 1.0 too — and
        it must never downgrade a row FYERS already owns."""
        sql = self._sql_of("ingestion/scheduler/daily_pipeline.py")
        insert = sql.split("INSERT INTO ohlcv_adjusted")[1].split('"""')[0]
        assert "1.0, 1.0, 'bhavcopy'" in insert
        assert "adj_factor     = 1.0" in insert
        assert "WHEN ohlcv_adjusted.source = 'fyers'" in insert

    def test_staged_backfill_publish_writes_literal_one(self):
        sql = self._sql_of("scripts/fyers_staged_backfill.py")
        insert = sql.split("INSERT INTO ohlcv_adjusted")[1].split('"""')[0]
        assert "volume, 1.0, 1.0, 'fyers'" in insert
        assert "excluded.adj_factor" not in insert


class TestStepSanityCheck:
    """[AS BUILT, A26] step_sanity_check's Check 3 (no all-NaN feature
    columns) must not fire on _SANITY_KNOWN_SPARSE_COLUMNS, and must still
    fire on a genuinely-broken column that isn't in that exemption list."""

    def _seed_signals(self, db_path, run_date, n_rows, regime="bullish"):
        from datastore.api.db import close_all_connections
        from datastore.schema.create_signals import create_signal_tables_schema

        create_signal_tables_schema(db_path=db_path)
        close_all_connections()  # release the cached persist=True schema connection
        date_str = run_date.isoformat()
        with get_duckdb_connection(db_path, persist=False) as conn:
            for i in range(n_rows):
                conn.execute(
                    "INSERT INTO ml_signals (date, ticker, model_name, model_version, buy_prob) "
                    "VALUES (?, ?, 'signal_5d', 'v1', 0.5)",
                    [date_str, f"T{i}"],
                )
            conn.execute(
                "INSERT INTO ml_signals (date, ticker, model_name, model_version, hmm_regime) "
                "VALUES (?, 'MARKET', 'hmm_market', 'v1', ?)",
                [date_str, regime],
            )

    def _patch_settings(self, monkeypatch, tmp_path, db_path, min_stocks=10):
        import config.settings as settings

        features_dir = tmp_path / "features_daily"
        features_dir.mkdir()
        monkeypatch.setattr(settings, "SIGNALS_DUCKDB_PATH", db_path)
        monkeypatch.setattr(settings, "FEATURES_DAILY_DIR", features_dir)
        monkeypatch.setattr(settings, "MIN_STOCKS_FOR_INFERENCE", min_stocks)
        return features_dir

    def test_passes_when_only_exempted_columns_are_all_nan(self, monkeypatch, tmp_path):
        run_date = date(2026, 1, 5)
        db_path = tmp_path / "signals.duckdb"
        self._seed_signals(db_path, run_date, n_rows=10)
        features_dir = self._patch_settings(monkeypatch, tmp_path, db_path, min_stocks=10)

        df = pd.DataFrame(
            {
                "ticker": ["AAA", "BBB"],
                "contingent_liability_ratio": [float("nan"), float("nan")],
                "board_independence": [float("nan"), float("nan")],
                "some_real_feature": [1.0, 2.0],
            }
        )
        df.to_parquet(features_dir / f"{run_date.isoformat()}.parquet")

        daily_pipeline.step_sanity_check(run_date)  # must not raise

    def test_raises_when_a_non_exempted_column_is_all_nan(self, monkeypatch, tmp_path):
        run_date = date(2026, 1, 5)
        db_path = tmp_path / "signals.duckdb"
        self._seed_signals(db_path, run_date, n_rows=10)
        features_dir = self._patch_settings(monkeypatch, tmp_path, db_path, min_stocks=10)

        df = pd.DataFrame(
            {
                "ticker": ["AAA", "BBB"],
                "some_broken_feature": [float("nan"), float("nan")],
                "some_real_feature": [1.0, 2.0],
            }
        )
        df.to_parquet(features_dir / f"{run_date.isoformat()}.parquet")

        with pytest.raises(RuntimeError, match="all-NaN"):
            daily_pipeline.step_sanity_check(run_date)


class TestWaitForDatastoreApi:
    """A44 regression coverage for _wait_for_datastore_api's cold-start
    race: on a laptop restart, alphalens-scheduler.service can start before
    the DataStore API's own systemd unit has bound its port, and without
    this health-gate daily_pipeline.main() would immediately fall through
    to matrix_builder's unbounded per-ticker OHLCV fallback against a
    not-yet-listening API (the 2026-07-10 OOM). These tests don't touch a
    real network/process — httpx.get is monkeypatched to simulate the API
    being down-then-up, and time.sleep/time.monotonic are patched so the
    test doesn't actually block for the real poll interval."""

    def test_returns_immediately_when_api_already_up(self, monkeypatch):
        import httpx

        calls = []

        class _FakeResponse:
            status_code = 200

        def _fake_get(url, timeout):
            calls.append(url)
            return _FakeResponse()

        monkeypatch.setattr(httpx, "get", _fake_get)
        sleep_calls = []
        monkeypatch.setattr(daily_pipeline.time, "sleep", lambda s: sleep_calls.append(s))

        daily_pipeline._wait_for_datastore_api(max_wait_seconds=30, poll_interval_seconds=1)

        assert len(calls) == 1
        assert sleep_calls == []  # no polling needed at all

    def test_retries_until_api_comes_up(self, monkeypatch):
        import httpx

        attempts = {"n": 0}

        class _FakeResponse:
            status_code = 200

        def _fake_get(url, timeout):
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise httpx.RequestError("connection refused", request=None)
            return _FakeResponse()

        monkeypatch.setattr(httpx, "get", _fake_get)
        sleep_calls = []
        monkeypatch.setattr(daily_pipeline.time, "sleep", lambda s: sleep_calls.append(s))

        daily_pipeline._wait_for_datastore_api(max_wait_seconds=30, poll_interval_seconds=1)

        assert attempts["n"] == 3  # two failed cold-start attempts, third succeeded
        assert len(sleep_calls) == 2  # slept once after each failed attempt

    def test_gives_up_after_max_wait_without_raising(self, monkeypatch):
        """SPEC-PIPE-006: an API that never comes up must not crash the
        pipeline — steps needing it fail cleanly and retry on the next run."""
        import httpx

        def _always_down(url, timeout):
            raise httpx.RequestError("connection refused", request=None)

        monkeypatch.setattr(httpx, "get", _always_down)
        monkeypatch.setattr(daily_pipeline.time, "sleep", lambda s: None)

        # Deterministic deadline: no real wall-clock wait, just verify the
        # function returns (doesn't raise) once monotonic() has advanced
        # past its deadline.
        clock = {"t": 0.0}

        def _fake_monotonic():
            clock["t"] += 5.0
            return clock["t"]

        monkeypatch.setattr(daily_pipeline.time, "monotonic", _fake_monotonic)

        daily_pipeline._wait_for_datastore_api(max_wait_seconds=10, poll_interval_seconds=1)
        # No exception raised is the assertion — proceeding anyway is correct.
