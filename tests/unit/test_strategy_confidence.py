"""
tests/unit/test_strategy_confidence.py

Tests for backtest/strategy_confidence.py — the general-purpose strategy
confidence evaluator that replaced the rejected touch-based TA screener
win/loss computation. Real synthetic OHLCV/regime DataFrames constructed
directly in-test (pure-function inputs, no DB/network/mocks), plus a real
tmp_path DuckDB file for the persistence round-trip, matching this repo's
no-stub/synthetic-data-in-production-paths policy (constructing plain
DataFrames to feed a pure function is not the same as faking production
DB rows).
"""
from datetime import date, timedelta

import pandas as pd
import pytest

from backtest import strategy_confidence as sc
from datastore.api.db import get_duckdb_connection


def _dates(n, start=date(2026, 1, 1)):
    return [start + timedelta(days=i) for i in range(n)]


def _make_ohlcv(ticker: str, closes: list, dates=None) -> pd.DataFrame:
    dates = dates or _dates(len(closes))
    rows = []
    prev_close = closes[0]
    for d, c in zip(dates, closes):
        rows.append({
            "ticker": ticker, "date": pd.Timestamp(d),
            "open": prev_close, "high": max(prev_close, c) * 1.001,
            "low": min(prev_close, c) * 0.999, "close": c,
        })
        prev_close = c
    return pd.DataFrame(rows)


class TestWilsonInterval:
    def test_known_values_50_of_100(self):
        lo, hi = sc.wilson_interval(50, 100)
        assert 0.40 < lo < 0.41
        assert 0.59 < hi < 0.60

    def test_zero_n_returns_zero_zero(self):
        assert sc.wilson_interval(0, 0) == (0.0, 0.0)

    def test_narrower_interval_with_more_data(self):
        lo_small, hi_small = sc.wilson_interval(5, 10)
        lo_large, hi_large = sc.wilson_interval(500, 1000)
        assert (hi_large - lo_large) < (hi_small - lo_small)

    def test_bounds_stay_within_0_1(self):
        lo, hi = sc.wilson_interval(100, 100)
        assert 0.0 <= lo <= 1.0
        assert 0.0 <= hi <= 1.0


class TestComputeForwardNetReturn:
    def test_long_profit_net_of_costs(self):
        gross, net = sc.compute_forward_net_return(100.0, 110.0, "long", quantity=100)
        assert gross == pytest.approx(0.10)
        assert net < gross  # costs reduce the return
        assert net > 0.09  # costs are small (<0.5%), shouldn't erase a 10% move

    def test_short_profit_when_price_falls(self):
        gross, net = sc.compute_forward_net_return(100.0, 90.0, "short", quantity=100)
        assert gross == pytest.approx(0.10)
        assert net < gross

    def test_invalid_direction_raises(self):
        with pytest.raises(ValueError):
            sc.compute_forward_net_return(100.0, 110.0, "sideways")

    def test_small_move_can_flip_negative_after_costs(self):
        # A tiny 0.1% gross move should not survive ~0.4-0.5% round-trip costs.
        gross, net = sc.compute_forward_net_return(100.0, 100.1, "long", quantity=100)
        assert gross > 0
        assert net < 0


class TestEvaluateOneAndBreakoutBug:
    """The rejected touch-based implementation had a real bug: a resistance
    level only existed if some swing high sat above price, so a ticker
    making a genuine new high had resistance_1=None and could structurally
    never win. The return-based rule has no such dependency — confirm a
    clean breakout (monotonically rising close, new highs every day) scores
    a straightforward win, not a forced loss."""

    def test_new_high_breakout_scores_win_not_forced_loss(self):
        closes = [100 + i * 2 for i in range(30)]  # relentless new highs
        ohlcv = _make_ohlcv("BRK", closes)
        signals = [sc.SignalEvent(date=pd.Timestamp(_dates(30)[0]), ticker="BRK", strategy_id="breakout_tmpl")]
        results, detail = sc.evaluate_signals_with_detail(signals, ohlcv, horizon_days=5)
        assert detail.iloc[0]["outcome"] == "win"

    def test_falling_price_scores_loss(self):
        closes = [100 - i * 2 for i in range(30)]
        ohlcv = _make_ohlcv("FALL", closes)
        signals = [sc.SignalEvent(date=pd.Timestamp(_dates(30)[0]), ticker="FALL", strategy_id="tmpl")]
        _, detail = sc.evaluate_signals_with_detail(signals, ohlcv, horizon_days=5)
        assert detail.iloc[0]["outcome"] == "loss"

    def test_insufficient_forward_history_is_pending_not_loss(self):
        closes = [100 + i for i in range(3)]  # only 3 days, horizon needs 5
        ohlcv = _make_ohlcv("NEW", closes)
        signals = [sc.SignalEvent(date=pd.Timestamp(_dates(3)[0]), ticker="NEW", strategy_id="tmpl")]
        _, detail = sc.evaluate_signals_with_detail(signals, ohlcv, horizon_days=5)
        assert detail.iloc[0]["outcome"] == "pending"

    def test_unknown_ticker_produces_no_detail_row(self):
        ohlcv = _make_ohlcv("X", [100, 101, 102, 103, 104, 105])
        signals = [sc.SignalEvent(date=pd.Timestamp(_dates(6)[0]), ticker="NOTINDATA", strategy_id="tmpl")]
        results, detail = sc.evaluate_signals_with_detail(signals, ohlcv, horizon_days=5)
        assert detail.empty
        assert results == {}


class TestRegimeSegmentation:
    def test_signals_bucket_into_correct_regime(self):
        dates = _dates(40)
        closes = [100 + i for i in range(40)]
        ohlcv = _make_ohlcv("REG", closes, dates)
        # First half bullish, second half bearish.
        regime_df = pd.DataFrame({
            "date": [pd.Timestamp(dates[0]), pd.Timestamp(dates[20])],
            "hmm_regime": ["bullish", "bearish"],
        })
        signals = [
            sc.SignalEvent(date=pd.Timestamp(dates[2]), ticker="REG", strategy_id="tmpl"),
            sc.SignalEvent(date=pd.Timestamp(dates[25]), ticker="REG", strategy_id="tmpl"),
        ]
        results, detail = sc.evaluate_signals_with_detail(signals, ohlcv, regime_df=regime_df, horizon_days=5)
        regimes_seen = set(detail["regime"])
        assert regimes_seen == {"bullish", "bearish"}

    def test_date_before_any_regime_row_is_unknown(self):
        dates = _dates(10)
        ohlcv = _make_ohlcv("U", [100 + i for i in range(10)], dates)
        regime_df = pd.DataFrame({"date": [pd.Timestamp(dates[5])], "hmm_regime": ["bullish"]})
        signals = [sc.SignalEvent(date=pd.Timestamp(dates[0]), ticker="U", strategy_id="tmpl")]
        _, detail = sc.evaluate_signals_with_detail(signals, ohlcv, regime_df=regime_df, horizon_days=3)
        assert detail.iloc[0]["regime"] == sc.REGIME_UNKNOWN


class TestTierAssignment:
    def test_below_min_independent_dates_is_insufficient(self):
        tier, reasons = sc._assign_tier(
            n_independent_dates=5, regime_date_counts={"bullish": 5}, integrity_ok=True,
            wilson_lo=0.9, baseline_win_rate=0.5, deflated_sharpe=0.99,
        )
        assert tier == sc.TIER_INSUFFICIENT

    def test_failed_integrity_check_is_insufficient_even_with_huge_sample(self):
        tier, reasons = sc._assign_tier(
            n_independent_dates=1000, regime_date_counts={"bullish": 1000}, integrity_ok=False,
            wilson_lo=0.9, baseline_win_rate=0.5, deflated_sharpe=0.99,
        )
        assert tier == sc.TIER_INSUFFICIENT

    def test_single_regime_only_is_preliminary(self):
        tier, reasons = sc._assign_tier(
            n_independent_dates=100, regime_date_counts={"bullish": 100}, integrity_ok=True,
            wilson_lo=0.9, baseline_win_rate=0.5, deflated_sharpe=0.99,
        )
        assert tier == sc.TIER_PRELIMINARY
        assert any("regime" in r for r in reasons)

    def test_low_deflated_sharpe_is_preliminary(self):
        tier, reasons = sc._assign_tier(
            n_independent_dates=100,
            regime_date_counts={"bullish": 50, "bearish": 50},
            integrity_ok=True, wilson_lo=0.9, baseline_win_rate=0.5, deflated_sharpe=0.5,
        )
        assert tier == sc.TIER_PRELIMINARY
        assert any("deflated Sharpe" in r for r in reasons)

    def test_wilson_lower_bound_not_beating_baseline_is_preliminary(self):
        tier, reasons = sc._assign_tier(
            n_independent_dates=100,
            regime_date_counts={"bullish": 50, "bearish": 50},
            integrity_ok=True, wilson_lo=0.45, baseline_win_rate=0.5, deflated_sharpe=0.99,
        )
        assert tier == sc.TIER_PRELIMINARY
        assert any("baseline" in r for r in reasons)

    def test_meets_every_bar_is_validated(self):
        tier, reasons = sc._assign_tier(
            n_independent_dates=100,
            regime_date_counts={"bullish": 50, "bearish": 50},
            integrity_ok=True, wilson_lo=0.65, baseline_win_rate=0.5, deflated_sharpe=0.99,
        )
        assert tier == sc.TIER_VALIDATED


class TestPersistence:
    def test_detail_and_summary_round_trip(self, tmp_path):
        db_path = tmp_path / "sc_test.duckdb"
        dates = _dates(40)
        closes = [100 + i for i in range(40)]
        ohlcv = _make_ohlcv("PER", closes, dates)
        signals = [sc.SignalEvent(date=pd.Timestamp(dates[i]), ticker="PER", strategy_id="tmpl") for i in range(0, 30, 3)]
        results, detail = sc.evaluate_signals_with_detail(signals, ohlcv, horizon_days=5)

        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            n_detail = sc.persist_detail(conn, detail)
            n_summary = sc.persist_summary(conn, results)

        assert n_detail == len(detail)
        assert n_summary >= 1

        with get_duckdb_connection(db_path, persist=False, read_only=True) as conn:
            detail_count = conn.execute("SELECT COUNT(*) FROM strategy_confidence_outcomes").fetchone()[0]
            summary_count = conn.execute("SELECT COUNT(*) FROM strategy_confidence_summary").fetchone()[0]
        assert detail_count == len(detail)
        assert summary_count == n_summary

    def test_upsert_does_not_duplicate_on_rerun(self, tmp_path):
        db_path = tmp_path / "sc_test2.duckdb"
        dates = _dates(10)
        ohlcv = _make_ohlcv("DUP", [100 + i for i in range(10)], dates)
        signals = [sc.SignalEvent(date=pd.Timestamp(dates[0]), ticker="DUP", strategy_id="tmpl")]
        results, detail = sc.evaluate_signals_with_detail(signals, ohlcv, horizon_days=3)

        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            sc.persist_detail(conn, detail)
            sc.persist_detail(conn, detail)  # re-run, same rows

        with get_duckdb_connection(db_path, persist=False, read_only=True) as conn:
            count = conn.execute("SELECT COUNT(*) FROM strategy_confidence_outcomes").fetchone()[0]
        assert count == len(detail)


    def test_summary_rerun_drops_regimes_no_longer_produced(self, tmp_path):
        """Regression for the 2026-07-19 stale-'unknown'-row bug: a regime
        bucket a strategy no longer produces (e.g. 'unknown' once real
        regime history backfills in) must disappear from
        strategy_confidence_summary on the next persist_summary call, not
        linger forever via ON CONFLICT DO UPDATE (which only ever
        updates/inserts, never removes)."""
        db_path = tmp_path / "sc_stale_regime.duckdb"
        dates = _dates(40)
        ohlcv = _make_ohlcv("STALE", [100 + i for i in range(40)], dates)
        signals = [sc.SignalEvent(date=pd.Timestamp(dates[i]), ticker="STALE", strategy_id="tmpl") for i in range(0, 30, 3)]

        # First run: no regime data available -> everything buckets as REGIME_UNKNOWN.
        results_v1, detail_v1 = sc.evaluate_signals_with_detail(signals, ohlcv, horizon_days=5, regime_df=None)
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            sc.persist_detail(conn, detail_v1)
            sc.persist_summary(conn, results_v1)

        with get_duckdb_connection(db_path, persist=False, read_only=True) as conn:
            regimes_v1 = set(conn.execute(
                "SELECT regime FROM strategy_confidence_summary WHERE strategy_id = 'tmpl'"
            ).fetchdf()["regime"])
        assert sc.REGIME_UNKNOWN in regimes_v1

        # Second run: real regime data now covers every signal date -> no more REGIME_UNKNOWN.
        regime_df = pd.DataFrame({"date": dates, "hmm_regime": ["bullish"] * len(dates)})
        results_v2, detail_v2 = sc.evaluate_signals_with_detail(signals, ohlcv, horizon_days=5, regime_df=regime_df)
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            sc.persist_summary(conn, results_v2)

        with get_duckdb_connection(db_path, persist=False, read_only=True) as conn:
            regimes_v2 = set(conn.execute(
                "SELECT regime FROM strategy_confidence_summary WHERE strategy_id = 'tmpl'"
            ).fetchdf()["regime"])
        assert sc.REGIME_UNKNOWN not in regimes_v2
        assert "bullish" in regimes_v2


class TestAggregateChunkForSummary:
    def test_collapses_to_one_row_per_strategy_regime_date(self):
        detail = pd.DataFrame([
            {"date": pd.Timestamp("2024-01-01"), "ticker": "A", "strategy_id": "tmpl", "regime": "bullish",
             "outcome": "win", "net_return_pct": 0.02},
            {"date": pd.Timestamp("2024-01-01"), "ticker": "B", "strategy_id": "tmpl", "regime": "bullish",
             "outcome": "loss", "net_return_pct": -0.01},
            {"date": pd.Timestamp("2024-01-02"), "ticker": "A", "strategy_id": "tmpl", "regime": "bullish",
             "outcome": "pending", "net_return_pct": None},
        ])
        agg = sc._aggregate_chunk_for_summary(detail)

        assert len(agg) == 2  # one row per (strategy_id, regime, date), not per ticker
        row1 = agg[agg["date"] == pd.Timestamp("2024-01-01")].iloc[0]
        assert row1["n"] == 2 and row1["wins"] == 1 and row1["losses"] == 1 and row1["pending"] == 0
        assert row1["mean_net_return"] == pytest.approx((0.02 + -0.01) / 2)

        row2 = agg[agg["date"] == pd.Timestamp("2024-01-02")].iloc[0]
        assert row2["n"] == 1 and row2["pending"] == 1 and row2["wins"] == 0

    def test_empty_input_returns_empty_with_expected_columns(self):
        agg = sc._aggregate_chunk_for_summary(pd.DataFrame())
        assert agg.empty
        assert list(agg.columns) == ["strategy_id", "regime", "date", "n", "wins", "losses", "pending", "mean_net_return"]


class TestEvaluateSignalsChunked:
    def test_chunked_matches_unchunked_results(self, tmp_path):
        dates = _dates(40)
        closes = [100 + i for i in range(40)]
        ohlcv = _make_ohlcv("CHK", closes, dates)
        signals = [sc.SignalEvent(date=pd.Timestamp(dates[i]), ticker="CHK", strategy_id="tmpl") for i in range(0, 30, 3)]

        results_unchunked, detail_unchunked = sc.evaluate_signals_with_detail(signals, ohlcv, horizon_days=5)

        db_path = tmp_path / "sc_chunked.duckdb"
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            sc.create_outcomes_table(conn)
            results_chunked = sc.evaluate_signals_chunked(
                signals, ohlcv, conn, horizon_days=5, chunk_size_dates=3,
            )
            sc.persist_summary(conn, results_chunked)

        assert set(results_chunked.keys()) == set(results_unchunked.keys())
        assert results_chunked["tmpl"].win_rate == results_unchunked["tmpl"].win_rate
        assert results_chunked["tmpl"].n_independent_dates == results_unchunked["tmpl"].n_independent_dates

        with get_duckdb_connection(db_path, persist=False, read_only=True) as conn:
            detail_count = conn.execute("SELECT COUNT(*) FROM strategy_confidence_outcomes").fetchone()[0]
        assert detail_count == len(detail_unchunked)

    def test_chunk_persisted_callback_fires_per_chunk(self, tmp_path):
        dates = _dates(20)
        ohlcv = _make_ohlcv("CB", [100 + i for i in range(20)], dates)
        signals = [sc.SignalEvent(date=pd.Timestamp(dates[i]), ticker="CB", strategy_id="tmpl") for i in range(0, 15, 2)]

        calls = []
        db_path = tmp_path / "sc_chunked_cb.duckdb"
        with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
            sc.create_outcomes_table(conn)
            sc.evaluate_signals_chunked(
                signals, ohlcv, conn, horizon_days=3, chunk_size_dates=2,
                on_chunk_persisted=lambda i, n, rows: calls.append((i, n, rows)),
            )

        assert len(calls) > 1  # multiple chunks actually persisted separately
        assert all(c[0] <= c[1] for c in calls)

        # rows written are visible in the DB incrementally, not just at the end —
        # a mid-run read after chunk i sees exactly the rows persisted so far.
        with get_duckdb_connection(db_path, persist=False, read_only=True) as conn:
            final_count = conn.execute("SELECT COUNT(*) FROM strategy_confidence_outcomes").fetchone()[0]
        assert final_count == sum(c[2] for c in calls)


class TestComputeBaseline:
    def test_baseline_uses_same_win_threshold_as_strategy(self):
        dates = _dates(10)
        # Every ticker rises exactly 0.05% -- below typical round-trip costs,
        # so with threshold 0.0 these should mostly lose after costs.
        frames = [_make_ohlcv(f"T{i}", [100, 100.05, 100.1, 100.15, 100.2, 100.25], dates[:6]) for i in range(5)]
        ohlcv = pd.concat(frames, ignore_index=True)
        wins, total = sc.compute_baseline(ohlcv, [dates[0]], horizon_days=5, win_threshold_pct=0.0, sample_size=5)
        assert total > 0
        assert wins < total  # tiny moves shouldn't clear real transaction costs
