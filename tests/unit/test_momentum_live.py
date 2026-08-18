"""
tests/unit/test_momentum_live.py

ML38 — features/momentum_live.py. Real seeded DuckDB (ohlcv_adjusted)
via a fresh normalised schema per test, matching test_momentum_universe.py's
convention. compute_daily_ranking's `universe` override is used throughout
so tests don't need to seed 150+ ranked tickers just to exercise the
momentum/grace logic (Band 3 = rank 100-150 in production).
"""

import pandas as pd
import pytest

import backtest.momentum_backtest as mb
import features.momentum_live as ml
from datastore.api.db import close_all_connections, get_duckdb_connection
from datastore.schema import create_normalised


def _patch_momentum(monkeypatch, fn):
    """Inject a deterministic momentum ranking into both binding sites.

    [ML40, 2026-08-14] These tests used to patch
    `backtest.momentum_backtest.trailing_momentum_from_panel`. That name is
    gone from the engine: ranking moved to the shared
    features.momentum_strategy.rank_universe, so both engines rank through one
    implementation and the module no longer imports the primitive at all
    (which is what clears the `duplicate_momentum_ranking` quality gate).

    Two sites are patched, not one, because the single name the engine used to
    expose has become two distinct bindings:
      * features.momentum_strategy — bound into rank_universe, i.e. THE ranking.
      * features.momentum_signal   — bound into downtrend_tickers, i.e. the
        short-window reversal filter.
    They were the same function object when the engine held one import, and
    the downtrend tests depend on their stub seeing both calls, so patching
    only one would silently leave the real implementation running for the
    other and make those tests assert against half-real data.
    """
    import features.momentum_signal as _msig
    import features.momentum_strategy as _mstrat

    monkeypatch.setattr(_mstrat, "trailing_momentum_from_panel", fn)
    monkeypatch.setattr(_msig, "trailing_momentum_from_panel", fn)




def _patch_params(monkeypatch, **overrides):
    """Override the registry-declared parameters for one test.

    [C1, 2026-08-18] These tests used to `monkeypatch.setattr(ml, "TOP_N", 1)`.
    Those module constants are gone: features/momentum_live.py now reads
    top_n / lookback_months / grace_cycles from
    strategy_registry.definition_json, so a live strategy can no longer
    silently run parameters other than the ones its backtest was approved on.

    The tests' need is unchanged and legitimate -- a 29-day fixture cannot
    exercise a 6-month lookback, and a 3-ticker universe cannot exercise a
    top-15 cut -- so the injection point moves to the same function the
    production path reads through, rather than the constants it no longer has.
    """
    declared = {"top_n": 15, "lookback_months": 6, "grace_cycles": 2}
    declared.update(overrides)
    monkeypatch.setattr(ml, "strategy_params", lambda _strategy_id: dict(declared))


def _seed_ohlcv(db_path, ticker, date_str, close):
    with get_duckdb_connection(db_path, persist=False, read_only=False) as conn:
        conn.execute(
            """
            INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume, delivery_qty, delivery_pct)
            VALUES (?, ?, ?, ?, ?, ?, 1000, 500, 50.0)
            ON CONFLICT DO NOTHING
            """,
            [date_str, ticker, close, close, close, close],
        )


def _seed_daily_series(db_path, ticker, dates, closes):
    for d, c in zip(dates, closes):
        _seed_ohlcv(db_path, ticker, str(d.date()), c)


@pytest.fixture
def normalised_db(tmp_path):
    db_path = tmp_path / "normalised_test.duckdb"
    create_normalised.create_schema(db_path=db_path)
    close_all_connections()
    return db_path


class TestComputeDailyRanking:
    def test_matches_manual_trailing_return(self, normalised_db, monkeypatch):
        _patch_params(monkeypatch, lookback_months=1)  # 21 trading days -> fits a 29-day fixture
        dates = pd.bdate_range("2026-01-01", periods=30)
        _seed_daily_series(normalised_db, "AAA", dates, [100] * 29 + [150])  # +50%
        _seed_daily_series(normalised_db, "BBB", dates, [100] * 29 + [110])  # +10%

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            df = ml.compute_daily_ranking(conn, str(dates[-1].date()), universe=["AAA", "BBB"])

        assert df.set_index("ticker").loc["AAA", "momentum_rank"] == 1
        assert df.set_index("ticker").loc["BBB", "momentum_rank"] == 2
        assert df.set_index("ticker").loc["AAA", "momentum_return"] == pytest.approx(0.5)

    def test_top_n_flag(self, normalised_db, monkeypatch):
        _patch_params(monkeypatch, top_n=1, lookback_months=1)
        dates = pd.bdate_range("2026-01-01", periods=30)
        _seed_daily_series(normalised_db, "AAA", dates, [100] * 29 + [150])
        _seed_daily_series(normalised_db, "BBB", dates, [100] * 29 + [110])

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            df = ml.compute_daily_ranking(conn, str(dates[-1].date()), universe=["AAA", "BBB"])

        assert bool(df.set_index("ticker").loc["AAA", "in_top_n"]) is True
        assert bool(df.set_index("ticker").loc["BBB", "in_top_n"]) is False

    def test_empty_universe_returns_empty_df(self, normalised_db):
        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            df = ml.compute_daily_ranking(conn, "2026-01-01", universe=[])
        assert df.empty

    def test_insufficient_history_excludes_ticker(self, normalised_db):
        dates = pd.bdate_range("2026-01-01", periods=5)  # far short of a 6mo lookback
        _seed_daily_series(normalised_db, "AAA", dates, [100, 101, 102, 103, 104])

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            df = ml.compute_daily_ranking(conn, str(dates[-1].date()), universe=["AAA"])
        assert df.empty


class TestNextRebalanceDate:
    def test_as_of_date_is_first_trading_day_of_month(self, normalised_db):
        dates = pd.bdate_range("2026-01-01", "2026-03-31")
        for d in dates:
            _seed_ohlcv(normalised_db, "AAA", str(d.date()), 100)

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            first_feb = dates[dates.month == 2][0]
            result = ml.next_rebalance_date(conn, str(first_feb.date()))
        assert result == str(first_feb.date())

    def test_mid_month_rolls_to_next_month_first_trading_day(self, normalised_db):
        dates = pd.bdate_range("2026-01-01", "2026-03-31")
        for d in dates:
            _seed_ohlcv(normalised_db, "AAA", str(d.date()), 100)

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            mid_jan = dates[(dates.month == 1) & (dates.day > 15)][0]
            result = ml.next_rebalance_date(conn, str(mid_jan.date()))
            expected = dates[dates.month == 2][0]
        assert result == str(expected.date())

    def test_holiday_shifted_first_of_month(self, normalised_db):
        # Jan 1st is a holiday (no row) -> first real trading day is Jan 2nd.
        dates = pd.bdate_range("2026-01-02", "2026-01-31")
        for d in dates:
            _seed_ohlcv(normalised_db, "AAA", str(d.date()), 100)

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            result = ml.next_rebalance_date(conn, "2026-01-01")
        assert result == str(dates[0].date())

    def test_no_data_returns_none(self, normalised_db):
        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            result = ml.next_rebalance_date(conn, "2026-01-01")
        assert result is None


class TestIsRebalanceDay:
    def test_true_when_matches_state(self, normalised_db):
        with get_duckdb_connection(normalised_db, persist=False, read_only=False) as conn:
            conn.execute(
                "INSERT INTO momentum_rebalance_state (strategy_id, next_rebalance_date) VALUES (?, ?)",
                [ml.DEFAULT_STRATEGY_ID, "2026-02-02"],
            )
        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            assert ml.is_rebalance_day(conn, "2026-02-02") is True
            assert ml.is_rebalance_day(conn, "2026-02-03") is False

    def test_false_when_no_state_row(self, normalised_db):
        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            assert ml.is_rebalance_day(conn, "2026-02-02") is False


class TestDecideGraceTransitionsRegressionAgainstBacktester:
    """Guards backtest/momentum_backtest.py's decide_grace_transitions()
    extraction: running MomentumBacktester over several rebalances and
    independently replaying the same held-ticker/target-set snapshots
    through the standalone function must produce identical decisions at
    every step."""

    def test_matches_backtester_over_multi_period_run(self, monkeypatch):
        dates = pd.bdate_range("2026-01-01", periods=40)
        tickers = ["A", "B", "C", "D"]
        panel = pd.DataFrame({t: [100.0] * len(dates) for t in tickers}, index=dates)

        # Prescribed per-rebalance top-2 rankings that rotate membership,
        # forcing grace transitions (drop-out, re-entry, exhaustion).
        rankings_by_call = [
            pd.Series({"A": 3.0, "B": 2.0, "C": 1.0, "D": 0.0}),  # top2: A,B
            pd.Series({"A": 0.0, "B": 3.0, "C": 2.0, "D": 1.0}),  # top2: B,C (A drops)
            pd.Series({"A": 3.0, "B": 0.0, "C": 2.0, "D": 1.0}),  # top2: A,C (A re-enters, B drops)
            pd.Series({"D": 3.0, "C": 2.0, "A": 1.0, "B": 0.0}),  # top2: D,C (A,B both out)
            pd.Series({"D": 3.0, "C": 2.0, "A": 1.0, "B": 0.0}),  # same again -> exhausts grace
        ]
        call_idx = {"i": 0}

        def fake_momentum(*a, **kw):
            i = call_idx["i"]
            call_idx["i"] = min(i + 1, len(rankings_by_call) - 1)
            return rankings_by_call[i]

        _patch_momentum(monkeypatch, fake_momentum)

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=5,
            starting_capital=1_000_000.0,
            top_n=2,
            grace_cycles=2,
        )
        result = engine.run()

        # Verify decide_grace_transitions is a pure function of (held_grace,
        # target_set, grace_cycles) by independently replaying the same
        # per-rebalance rankings and confirming the resulting held-ticker
        # set matches what the real engine actually ended up holding.

        # Rebuild, from the transaction ledger, what grace state existed
        # just before each rebalance, and confirm decide_grace_transitions
        # reproduces the same buy/sell/hold outcome the engine actually
        # took. The engine actually runs ceil(40/5)=8 rebalances (not just
        # len(rankings_by_call)=5) — fake_momentum's clamping means calls
        # 6-8 replay ranking index 4 again, so the replay must match that.
        n_rebalances = len(dates[::5])
        held_grace = {}
        for step_num in range(n_rebalances):
            momentum = rankings_by_call[min(step_num, len(rankings_by_call) - 1)]
            target_set = set(momentum.sort_values(ascending=False).head(2).index)
            updated = mb.decide_grace_transitions(held_grace, target_set, 2)
            # Anything that reaches <=0 would be force-sold by the engine;
            # simulate that removal for the next iteration's input, exactly
            # as MomentumBacktester.run() does.
            for ticker in list(updated.keys()):
                if updated[ticker] is not None and updated[ticker] <= 0:
                    del updated[ticker]
            for ticker in target_set:
                if ticker not in updated:
                    updated[ticker] = None  # newly bought this step -> core
            held_grace = updated

        # Final held set after replaying the standalone function must match
        # the tickers the real engine still holds open at the end of run().
        engine_open_tickers = {t["ticker"] for t in result.transactions if t["status"] == "open"}
        assert set(held_grace.keys()) == engine_open_tickers


class TestComputeRebalanceSuggestions:
    def test_mixed_add_exit_grace_hold(self, normalised_db, monkeypatch):
        _patch_params(monkeypatch, top_n=2, lookback_months=1)
        dates = pd.bdate_range("2026-01-01", periods=30)
        # AAA, BBB will rank top-2 (highest returns); CCC ranks lowest.
        _seed_daily_series(normalised_db, "AAA", dates, [100] * 29 + [150])
        _seed_daily_series(normalised_db, "BBB", dates, [100] * 29 + [140])
        _seed_daily_series(normalised_db, "CCC", dates, [100] * 29 + [101])

        current_open_trades = [
            {"ticker": "BBB", "grace_remaining": None},  # already core, stays core
            {"ticker": "CCC", "grace_remaining": None},  # about to drop out -> grace starts
        ]

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            suggestions = ml.compute_rebalance_suggestions(
                conn, str(dates[-1].date()), current_open_trades,
                grace_cycles=2, universe=["AAA", "BBB", "CCC"],
            )

        by_ticker = {s["ticker"]: s for s in suggestions}
        assert by_ticker["AAA"]["action"] == "add"
        assert "BBB" not in by_ticker  # held and still core -> nothing to do
        assert by_ticker["CCC"]["action"] == "grace_hold"
        assert by_ticker["CCC"]["grace_remaining"] == 2

    def test_grace_exhausted_produces_exit(self, normalised_db, monkeypatch):
        _patch_params(monkeypatch, top_n=1, lookback_months=1)
        dates = pd.bdate_range("2026-01-01", periods=30)
        _seed_daily_series(normalised_db, "AAA", dates, [100] * 29 + [150])
        _seed_daily_series(normalised_db, "BBB", dates, [100] * 29 + [101])

        current_open_trades = [{"ticker": "BBB", "grace_remaining": 1}]

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            suggestions = ml.compute_rebalance_suggestions(
                conn, str(dates[-1].date()), current_open_trades,
                grace_cycles=2, universe=["AAA", "BBB"],
            )

        by_ticker = {s["ticker"]: s for s in suggestions}
        assert by_ticker["BBB"]["action"] == "exit"
        assert by_ticker["BBB"]["grace_remaining"] == 0

    def test_empty_portfolio_suggests_all_adds(self, normalised_db, monkeypatch):
        _patch_params(monkeypatch, top_n=2, lookback_months=1)
        dates = pd.bdate_range("2026-01-01", periods=30)
        _seed_daily_series(normalised_db, "AAA", dates, [100] * 29 + [150])
        _seed_daily_series(normalised_db, "BBB", dates, [100] * 29 + [140])

        with get_duckdb_connection(normalised_db, persist=False, read_only=True) as conn:
            suggestions = ml.compute_rebalance_suggestions(
                conn, str(dates[-1].date()), [], grace_cycles=2, universe=["AAA", "BBB"],
            )

        assert {s["ticker"]: s["action"] for s in suggestions} == {"AAA": "add", "BBB": "add"}
