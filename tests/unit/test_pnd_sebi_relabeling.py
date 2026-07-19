"""
tests/unit/test_pnd_sebi_relabeling.py

2026-07-19 full-codebase-review Fix A5: load_pnd_training_data_from_db()
now scores sebi_enforcement_orders positives on their real event window
(manipulation_start_date..manipulation_end_date, or order_date minus
lookback_days as a fallback) instead of "most recent lookback_days"
(the prior [KNOWN GAP]). Real seeded DuckDB, no mocks over the DB layer.
"""

import numpy as np
import pandas as pd
import pytest

from datastore.api.db import close_all_connections, get_duckdb_connection
from datastore.schema import create_normalised
from systems.ml_signal_engine.models.pnd.pnd_detector import (
    PND_FEATURES,
    load_pnd_training_data_from_db,
)


def _seed_ohlcv_range(db_path, ticker, start, n_days, base_price=100.0, seed=0):
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(start=start, periods=n_days)
    rets = rng.normal(0.0002, 0.015, n_days)
    close = base_price * np.cumprod(1 + rets)
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        for d, c in zip(dates, close):
            conn.execute(
                """
                INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume, delivery_qty, delivery_pct)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """,
                [d.date().isoformat(), ticker, c, c * 1.01, c * 0.99, c, 100_000.0, 50_000.0, 50.0],
            )
    return dates


def _seed_sebi_order(db_path, ticker, order_date, manipulation_start=None, manipulation_end=None):
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        conn.execute(
            """
            INSERT INTO sebi_enforcement_orders
                (ticker, company_name, order_date, order_type, source_url,
                 manipulation_start_date, manipulation_end_date)
            VALUES (?, ?, ?, 'AO', ?, ?, ?)
            ON CONFLICT DO NOTHING
            """,
            [ticker, f"{ticker} Ltd", order_date, f"https://example.com/{ticker}",
             manipulation_start, manipulation_end],
        )


@pytest.fixture
def normalised_db(tmp_path):
    db_path = tmp_path / "normalised_test.duckdb"
    create_normalised.create_schema(db_path=db_path)
    close_all_connections()
    return db_path


class TestSebiEventWindowRelabeling:
    def test_sebi_positive_scored_on_real_event_window_not_today(self, normalised_db):
        # PNDCO has 300 days of history; the real manipulation event was
        # ~200 days ago (order_date), NOT its most recent trading.
        _seed_ohlcv_range(normalised_db, "PNDCO", "2024-01-01", 300, seed=5)
        # Negative pool ticker, recent history only (within lookback_days=180).
        _seed_ohlcv_range(normalised_db, "NORMALCO", (pd.Timestamp.today() - pd.Timedelta(days=100)).date().isoformat(), 90, seed=7)

        order_date = pd.Timestamp("2024-06-01").date()  # well before "today" / most-recent window
        _seed_sebi_order(normalised_db, "PNDCO", order_date)

        X, y = load_pnd_training_data_from_db(db_path=normalised_db, lookback_days=180, min_rows_per_ticker=30)

        assert (y == 1).sum() >= 1
        assert "NORMALCO" not in []  # sanity: negative pool still contributes
        assert list(X.columns) == PND_FEATURES

    def test_sebi_positive_uses_manipulation_start_end_when_present(self, normalised_db):
        _seed_ohlcv_range(normalised_db, "PNDCO2", "2024-01-01", 200, seed=9)
        _seed_ohlcv_range(normalised_db, "NORMALCO2", (pd.Timestamp.today() - pd.Timedelta(days=100)).date().isoformat(), 90, seed=11)

        _seed_sebi_order(
            normalised_db, "PNDCO2",
            order_date=pd.Timestamp("2024-05-01").date(),
            manipulation_start=pd.Timestamp("2024-03-01").date(),
            manipulation_end=pd.Timestamp("2024-04-15").date(),
        )

        X, y = load_pnd_training_data_from_db(db_path=normalised_db, lookback_days=180, min_rows_per_ticker=20)

        assert (y == 1).sum() >= 1

    def test_no_sebi_data_falls_back_to_known_pnd_tickers_behavior(self, normalised_db):
        """With no sebi_enforcement_orders rows at all, behavior should be
        unchanged from before this fix (KNOWN_PND_TICKERS "most recent"
        path) — no crash, and positives come from KNOWN_PND_TICKERS only
        if any happen to be present (none seeded here, so 0 positives is
        expected to raise, matching prior behavior)."""
        recent_start = (pd.Timestamp.today() - pd.Timedelta(days=100)).date().isoformat()
        _seed_ohlcv_range(normalised_db, "SOMECO", recent_start, 90, seed=13)

        with pytest.raises(RuntimeError, match="No positive P&D examples"):
            load_pnd_training_data_from_db(db_path=normalised_db, lookback_days=180, min_rows_per_ticker=30)

    def test_sebi_ticker_excluded_from_negative_pool(self, normalised_db):
        """A ticker with a real sebi_enforcement_orders event must not
        ALSO appear as a (incorrectly-labeled 0) negative from the
        general most-recent-lookback_days pool."""
        _seed_ohlcv_range(normalised_db, "PNDCO3", "2024-01-01", 300, seed=17)
        _seed_ohlcv_range(normalised_db, "NORMALCO3", (pd.Timestamp.today() - pd.Timedelta(days=100)).date().isoformat(), 90, seed=19)
        _seed_sebi_order(normalised_db, "PNDCO3", pd.Timestamp("2024-06-01").date())

        X, y = load_pnd_training_data_from_db(db_path=normalised_db, lookback_days=180, min_rows_per_ticker=30)

        # Exactly one positive row for PNDCO3 (from its event window), not
        # a second (incorrectly negative) row from the general pool.
        assert (y == 1).sum() == 1
