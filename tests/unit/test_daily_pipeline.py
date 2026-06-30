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
    def test_failure_is_caught_and_non_fatal(self, monkeypatch):
        """SPEC-PIPE-001: F&O bhavcopy is non-critical for Phase 1 (NSE's
        archive endpoint is currently broken) — a failure here must never
        raise, so it can never block download_macro/adjust_prices."""
        from ingestion.scrapers import fno

        def _raise(date_str):
            raise ConnectionError("F&O bhavcopy unavailable")

        monkeypatch.setattr(fno, "download_fno_bhavcopy", _raise)

        daily_pipeline.step_download_fno(date(2026, 1, 5))  # must not raise

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


class TestStepDownloadMacro:
    def test_writes_all_three_indicators(self, monkeypatch):
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

        with get_duckdb_connection(None) as conn:
            monkeypatch.setattr(
                daily_pipeline, "get_duckdb_connection", lambda path, persist=True: _FixedConn(conn)
            )
            daily_pipeline.step_download_macro(date(2026, 1, 5))

            rows = dict(
                conn.execute(
                    "SELECT indicator, value FROM macro_indicators WHERE date = '2026-01-05'"
                ).fetchall()
            )
            assert rows == {"INDIA_VIX": 14.5, "FII_NET_CR": -120.5, "DII_NET_CR": 340.2, "USD_INR": 83.2}

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

        with get_duckdb_connection(None) as conn:
            monkeypatch.setattr(
                daily_pipeline, "get_duckdb_connection", lambda path, persist=True: _FixedConn(conn)
            )
            daily_pipeline.step_download_macro(date(2026, 1, 5))  # must not raise

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

        daily_pipeline.step_download_macro(date(2026, 1, 5))  # must not raise


class TestStepAdjustPrices:
    def test_calls_adjust_for_corporate_actions_per_universe_ticker(self, monkeypatch):
        create_normalised.create_schema(in_memory=True)
        from config import universe
        from ingestion.adjust import price_adjuster

        monkeypatch.setattr(universe, "get_tickers", lambda: ["AAA", "BBB"])
        calls = []
        monkeypatch.setattr(
            price_adjuster, "adjust_for_corporate_actions", lambda conn, ticker: calls.append(ticker)
        )

        with get_duckdb_connection(None) as conn:
            monkeypatch.setattr(
                daily_pipeline, "get_duckdb_connection", lambda path, persist=True: _FixedConn(conn)
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

        monkeypatch.setattr(universe_mod, "get_tickers", lambda: ["AAA", "BBB"])

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
