"""
tests/unit/test_backfill_hmm_regime.py

Tests for scripts/backfill_hmm_regime.py's walk-forward regime decoder —
specifically that it does not leak future data into a historical fit
(the reason it exists instead of one all-history HMM fit, see that
module's docstring), and that persistence upserts correctly.
"""
from datetime import date

import numpy as np
import pandas as pd

from datastore.api.db import get_duckdb_connection
from scripts.backfill_hmm_regime import _persist, _walk_forward_decode


def _make_nifty_obs(n_days=400, seed=0, start="2020-01-01"):
    """Synthetic NIFTYBEES-shaped OHLCV -> real compute_hmm_observables output."""
    from systems.ml_signal_engine.models.hmm.regime_detector import compute_hmm_observables

    dates = pd.bdate_range(start=start, periods=n_days)
    rng = np.random.default_rng(seed)
    rets = rng.normal(0.0003, 0.012, n_days)
    close = 100 * np.cumprod(1 + rets)
    df = pd.DataFrame({
        "ticker": "NIFTYBEES", "date": dates,
        "open": close, "high": close * 1.005, "low": close * 0.995, "close": close,
        "volume": rng.integers(1_000_000, 5_000_000, n_days).astype(float),
    })
    return compute_hmm_observables(df)


class TestWalkForwardDecode:
    def test_decodes_every_target_date(self):
        obs = _make_nifty_obs(n_days=400)
        target_start = obs["date"].iloc[200].date().isoformat()
        target_end = obs["date"].iloc[-1].date().isoformat()

        decoded = _walk_forward_decode(obs, target_start, target_end, refit_interval_days=28, lookback_days=1260)

        target_dates = set(pd.to_datetime(obs[
            (obs["date"] >= pd.Timestamp(target_start)) & (obs["date"] <= pd.Timestamp(target_end))
        ]["date"]).dt.date)
        decoded_dates = set(decoded["date"])
        # Some early dates may be undecodable (NaN observables during warmup);
        # the vast majority of a 200-day window should decode successfully.
        assert len(decoded_dates & target_dates) > 0.9 * len(target_dates)

    def test_refit_does_not_use_data_after_the_refit_date(self):
        """The whole point of walking forward instead of one all-history fit:
        a model used to decode an early block must be fit ONLY on data on/
        before its own refit date, never on later history."""
        obs = _make_nifty_obs(n_days=400)
        target_start = obs["date"].iloc[100].date().isoformat()
        target_end = obs["date"].iloc[150].date().isoformat()

        # Corrupt observations strictly AFTER target_end with an extreme
        # regime shift. If the decoder were (wrongly) fit on the full
        # dataset, this future corruption would change the state means, and
        # therefore the regime labels assigned to the untouched early block.
        obs_clean = obs.copy()
        obs_corrupted = obs.copy()
        after_mask = obs_corrupted["date"] > pd.Timestamp(target_end) + pd.Timedelta(days=30)
        obs_corrupted.loc[after_mask, "daily_return"] = 5.0
        obs_corrupted.loc[after_mask, "log_return"] = 1.5

        decoded_clean = _walk_forward_decode(obs_clean, target_start, target_end, refit_interval_days=28, lookback_days=1260)
        decoded_corrupted = _walk_forward_decode(obs_corrupted, target_start, target_end, refit_interval_days=28, lookback_days=1260)

        merged = decoded_clean.merge(decoded_corrupted, on="date", suffixes=("_clean", "_corrupted"))
        assert not merged.empty
        assert (merged["hmm_regime_clean"] == merged["hmm_regime_corrupted"]).all()

    def test_on_block_decoded_fires_per_refit_block_not_just_at_the_end(self, tmp_path):
        """The whole point of this callback: a caller persists each block as
        it's decoded, so a kill mid-run loses only the in-flight block, not
        the entire multi-year decode (the bug this replaced — see the
        module docstring's 'holding every block in memory' note)."""
        obs = _make_nifty_obs(n_days=400)
        target_start = obs["date"].iloc[100].date().isoformat()
        target_end = obs["date"].iloc[-1].date().isoformat()

        calls = []
        decoded = _walk_forward_decode(
            obs, target_start, target_end, refit_interval_days=28, lookback_days=1260,
            on_block_decoded=lambda i, n, df: calls.append((i, n, len(df))),
        )

        assert len(calls) > 1  # multiple refit blocks, not one giant block
        assert sum(c[2] for c in calls) == len(decoded)
        assert all(c[0] <= c[1] for c in calls)

    def test_empty_range_returns_empty_dataframe(self):
        obs = _make_nifty_obs(n_days=100)
        far_future_start = (obs["date"].iloc[-1] + pd.Timedelta(days=365)).date().isoformat()
        far_future_end = (obs["date"].iloc[-1] + pd.Timedelta(days=400)).date().isoformat()
        decoded = _walk_forward_decode(obs, far_future_start, far_future_end, refit_interval_days=28, lookback_days=1260)
        assert decoded.empty


class TestPersist:
    def test_bulk_upsert_round_trip(self, tmp_path):
        db_path = tmp_path / "hmm_test.duckdb"
        batch = pd.DataFrame([
            {"date": date(2020, 1, 1), "ticker": "MARKET", "model_name": "hmm_market",
             "model_version": "backfill-1.0", "hmm_regime": "bullish", "hmm_regime_prob": 0.9, "hmm_stability": 0.9},
            {"date": date(2020, 1, 2), "ticker": "MARKET", "model_name": "hmm_market",
             "model_version": "backfill-1.0", "hmm_regime": "bearish", "hmm_regime_prob": 0.8, "hmm_stability": 0.8},
        ])
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            n = _persist(conn, batch)
        assert n == 2

        with get_duckdb_connection(db_path, persist=False, read_only=True) as conn:
            count = conn.execute("SELECT COUNT(*) FROM ml_signals").fetchone()[0]
            regimes = conn.execute("SELECT hmm_regime FROM ml_signals ORDER BY date").fetchall()
        assert count == 2
        assert [r[0] for r in regimes] == ["bullish", "bearish"]

    def test_upsert_does_not_duplicate_on_rerun(self, tmp_path):
        db_path = tmp_path / "hmm_test2.duckdb"
        batch = pd.DataFrame([
            {"date": date(2020, 1, 1), "ticker": "MARKET", "model_name": "hmm_market",
             "model_version": "backfill-1.0", "hmm_regime": "bullish", "hmm_regime_prob": 0.9, "hmm_stability": 0.9},
        ])
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            _persist(conn, batch)
            _persist(conn, batch)

        with get_duckdb_connection(db_path, persist=False, read_only=True) as conn:
            count = conn.execute("SELECT COUNT(*) FROM ml_signals").fetchone()[0]
        assert count == 1

    def test_empty_batch_is_a_noop(self, tmp_path):
        db_path = tmp_path / "hmm_test3.duckdb"
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            n = _persist(conn, pd.DataFrame())
        assert n == 0
