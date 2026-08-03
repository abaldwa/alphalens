"""
tests/unit/test_momentum_backtest.py

ML38 — backtest/momentum_backtest.py. `trailing_momentum_from_panel` is
monkeypatched to a prescribed per-date ranking so grace-period and
buffer-exhaustion portfolio mechanics can be tested in isolation from
momentum-computation correctness (already covered by
test_momentum_signal.py).
"""

import logging

import pandas as pd
import pytest

import backtest.momentum_backtest as mb


def _flat_price_panel(tickers, n_days, price=100.0):
    dates = pd.date_range("2026-01-01", periods=n_days, freq="D")
    return pd.DataFrame({t: [price] * n_days for t in tickers}, index=dates)


class TestTradeCagr:
    """Regression for the OverflowError that crashed 66/311 jobs in the
    2026-08-02 Technical sweep: a short holding period (e.g. a 1-day
    circuit-limit move or a corrupted OHLCV bar) annualizes to an exponent
    large enough to overflow a float, which used to propagate up through
    export_trade_book and fail the whole orchestrator job even though the
    backtest run itself had already saved successfully."""

    def test_extreme_short_holding_ratio_returns_none_not_overflow(self):
        assert mb.trade_cagr(100.0, 5000.0, 1) is None

    def test_normal_case_unaffected(self):
        result = mb.trade_cagr(100.0, 110.0, 30)
        assert result == pytest.approx(2.19121409718457)

    def test_same_day_round_trip_returns_none(self):
        assert mb.trade_cagr(100.0, 110.0, 0) is None

    def test_open_position_returns_none(self):
        assert mb.trade_cagr(100.0, None, None) is None

    def test_overflow_is_logged_to_data_quality_anomaly_log(self, tmp_path, monkeypatch):
        # Redirect the module's anomaly file handler to an isolated tmp file
        # so this test doesn't append to the real logs/data_quality_anomalies.log.
        log_path = tmp_path / "anomalies.log"
        handler = logging.FileHandler(log_path, mode="a")
        handler.setFormatter(logging.Formatter("%(message)s"))
        monkeypatch.setattr(mb._anomaly_logger, "handlers", [handler])

        result = mb.trade_cagr(
            1.7616, 52.85, 1,
            ticker="ANMOL", buy_date="2023-07-17", sell_date="2023-07-18", run_id="orch_test_123",
        )
        handler.close()

        assert result is None
        logged = log_path.read_text()
        assert "ANMOL" in logged
        assert "orch_test_123" in logged
        assert "2023-07-17" in logged and "2023-07-18" in logged


class TestSIP:
    def test_monthly_contributions_applied_and_tracked(self, monkeypatch):
        # 5 calendar months of daily trading days -> 4 SIP injections
        # after the first (starting_capital covers month 1).
        dates = pd.date_range("2026-01-01", "2026-05-15", freq="B")
        panel = _flat_price_panel(["A"], len(dates))
        panel.index = dates

        monkeypatch.setattr(mb, "trailing_momentum_from_panel", lambda *a, **kw: pd.Series({"A": 1.0}))

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): ["A"]},
            lookback_days=1,
            rebalance_every_n_trading_days=5,
            starting_capital=1_000_000.0,
            top_n=1,
            sip_amount=50_000.0,
        )
        result = engine.run()

        # cash_flows: 1 initial + 4 monthly SIP contributions (Feb, Mar, Apr, May).
        assert len(result.cash_flows) == 5
        assert result.cash_flows[0] == {"date": "2026-01-01", "amount": -1_000_000.0}
        assert all(cf["amount"] == -50_000.0 for cf in result.cash_flows[1:])
        assert result.total_contributed == pytest.approx(1_000_000.0 + 4 * 50_000.0)
        # Flat price, no costs modeled to zero here — ending value should
        # be close to total contributed (not exact due to txn costs).
        assert result.ending_value == pytest.approx(result.total_contributed, rel=0.02)

    def test_no_sip_leaves_cash_flows_as_single_contribution(self, monkeypatch):
        dates = pd.date_range("2026-01-01", periods=10, freq="D")
        panel = _flat_price_panel(["A"], len(dates))
        monkeypatch.setattr(mb, "trailing_momentum_from_panel", lambda *a, **kw: pd.Series({"A": 1.0}))

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): ["A"]},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            top_n=1,
        )
        result = engine.run()
        assert result.cash_flows == [{"date": "2026-01-01", "amount": -1_000_000.0}]
        assert result.total_contributed == 1_000_000.0


class TestMinMomentumFilter:
    """2026-07-14 win-rate-improvement experiment: min_momentum excludes a
    top-ranked but non-positive-momentum name rather than padding the
    portfolio with it."""

    def test_excludes_ticker_below_floor_even_if_top_ranked(self, monkeypatch):
        tickers = ["A", "B"]
        panel = _flat_price_panel(tickers, 2)
        dates = panel.index
        # A ranks #1 by momentum but its momentum is negative; B is #2 but positive.
        monkeypatch.setattr(
            mb, "trailing_momentum_from_panel", lambda *a, **kw: pd.Series({"A": -0.05, "B": 0.02})
        )

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            top_n=2,
            min_momentum=0.0,
        )
        result = engine.run()
        assert result.rebalance_events[0]["n_bought"] == 1  # only B, not A
        assert "A" not in engine.positions
        assert "B" in engine.positions

    def test_none_preserves_original_fill_top_n_behavior(self, monkeypatch):
        tickers = ["A", "B"]
        panel = _flat_price_panel(tickers, 2)
        dates = panel.index
        monkeypatch.setattr(
            mb, "trailing_momentum_from_panel", lambda *a, **kw: pd.Series({"A": -0.05, "B": 0.02})
        )

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            top_n=2,
            min_momentum=None,
        )
        result = engine.run()
        assert result.rebalance_events[0]["n_bought"] == 2  # both, regardless of sign


class TestDowntrendFilter:
    """2026-07-15 comparison request: downtrend_filter_pct excludes a
    top-ranked (by the main lookback) ticker that has already dropped
    >= the threshold over the short-term (default 20-day) window, even
    though its long-lookback momentum still ranks it highly."""

    def test_excludes_ticker_with_sharp_recent_drop(self, monkeypatch):
        tickers = ["A", "B"]
        panel = _flat_price_panel(tickers, 2)
        dates = panel.index

        def fake_momentum(price_panel, universe_tickers, as_of_date, lookback_days):
            if lookback_days == 20:  # short-term downtrend check
                return pd.Series({"A": -0.08, "B": -0.01})
            return pd.Series({"A": 0.30, "B": 0.10})  # main lookback: A ranks #1

        monkeypatch.setattr(mb, "trailing_momentum_from_panel", fake_momentum)

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=63,
            rebalance_every_n_trading_days=1,
            top_n=2,
            downtrend_filter_pct=0.05,
            downtrend_lookback_days=20,
        )
        result = engine.run()
        assert result.rebalance_events[0]["n_bought"] == 1  # only B, A excluded
        assert "A" not in engine.positions
        assert "B" in engine.positions

    def test_mild_dip_under_threshold_stays_eligible(self, monkeypatch):
        tickers = ["A", "B"]
        panel = _flat_price_panel(tickers, 2)
        dates = panel.index

        def fake_momentum(price_panel, universe_tickers, as_of_date, lookback_days):
            if lookback_days == 20:
                return pd.Series({"A": -0.02, "B": -0.01})  # both under the 5% threshold
            return pd.Series({"A": 0.30, "B": 0.10})

        monkeypatch.setattr(mb, "trailing_momentum_from_panel", fake_momentum)

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=63,
            rebalance_every_n_trading_days=1,
            top_n=2,
            downtrend_filter_pct=0.05,
        )
        result = engine.run()
        assert result.rebalance_events[0]["n_bought"] == 2  # neither excluded

    def test_none_preserves_original_behavior(self, monkeypatch):
        tickers = ["A", "B"]
        panel = _flat_price_panel(tickers, 2)
        dates = panel.index
        monkeypatch.setattr(
            mb, "trailing_momentum_from_panel", lambda *a, **kw: pd.Series({"A": 0.30, "B": 0.10})
        )

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=63,
            rebalance_every_n_trading_days=1,
            top_n=2,
            downtrend_filter_pct=None,
        )
        result = engine.run()
        assert result.rebalance_events[0]["n_bought"] == 2


class TestTransactionLedger:
    def test_closed_transaction_has_dates_prices_ranks_and_holding_period(self, monkeypatch):
        tickers = ["A", "B", "C"]
        panel = _flat_price_panel(tickers, 4)
        dates = panel.index
        panel.loc[dates[3], "A"] = 150.0  # sell price differs from buy price

        rankings_by_date = {
            dates[0]: {"A": 1.0, "B": 0.5, "C": 0.1},  # A rank 1, buy A
            dates[1]: {"A": 1.0, "B": 0.5, "C": 0.1},
            dates[2]: {"A": 0.1, "B": 0.5, "C": 1.0},  # A drops to rank 3
            dates[3]: {"A": 0.1, "B": 0.5, "C": 1.0},
        }

        def fake_momentum(price_panel, tickers_arg, as_of_date, lookback_days):
            return pd.Series(rankings_by_date[pd.Timestamp(as_of_date)])

        monkeypatch.setattr(mb, "trailing_momentum_from_panel", fake_momentum)

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            investable_pct=0.3,  # cash headroom so buffer exhaustion (which
            # would otherwise force-sell A a cycle early to fund C's buy —
            # see TestBufferExhaustion) doesn't confound this test's
            # isolated check of the grace-cycle countdown timing.
            top_n=1,
            grace_cycles=1,  # A drops at date2, force-sold at date3
        )
        result = engine.run()

        closed = [t for t in result.transactions if t["ticker"] == "A" and t["status"] == "closed"]
        assert len(closed) == 1
        txn = closed[0]
        assert txn["buy_date"] == str(dates[0].date())
        assert txn["sell_date"] == str(dates[3].date())
        assert txn["buy_price"] == pytest.approx(100.0)
        assert txn["sell_price"] == pytest.approx(150.0)
        assert txn["holding_days"] == (dates[3] - dates[0]).days
        assert txn["buy_momentum_rank"] == 1
        assert txn["sell_momentum_rank"] == 3

    def test_still_open_position_surfaced_with_null_sell_fields(self, monkeypatch):
        tickers = ["A"]
        panel = _flat_price_panel(tickers, 3)
        dates = panel.index

        monkeypatch.setattr(
            mb, "trailing_momentum_from_panel", lambda *a, **kw: pd.Series({"A": 1.0})
        )

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            top_n=1,
        )
        result = engine.run()
        open_txns = [t for t in result.transactions if t["status"] == "open"]
        assert len(open_txns) == 1
        assert open_txns[0]["ticker"] == "A"
        assert open_txns[0]["sell_date"] is None
        assert open_txns[0]["sell_momentum_rank"] is None
        assert open_txns[0]["buy_momentum_rank"] == 1


class TestPriceRowNotBrokenByStaggeredListings:
    """Regression test for the bug caught on the first real 10-year run:
    a wide multi-ticker panel where different tickers start trading on
    different dates (real staggered listing history) produced an all-NaN
    equity curve end to end, because _price_row used DataFrame.asof(),
    which requires an ENTIRE row to be simultaneously non-null across
    every column by default — with 90+ staggered tickers that's nearly
    never true. Fixed to use .loc[date] directly (2026-07-14)."""

    def test_price_row_returns_real_values_with_staggered_tickers(self, monkeypatch):
        dates = pd.date_range("2026-01-01", periods=5, freq="D")
        panel = pd.DataFrame(index=dates)
        panel["EARLY"] = [100.0, 101.0, 102.0, 103.0, 104.0]
        # LATE only starts trading partway through — its own real listing
        # history, not a data gap to be guessed around.
        panel["LATE"] = [None, None, None, 50.0, 51.0]

        monkeypatch.setattr(mb, "trailing_momentum_from_panel", lambda *a, **kw: pd.Series(dtype=float))

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): ["EARLY", "LATE"]},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            top_n=2,
        )
        prices = engine._price_row(dates[2])
        assert prices["EARLY"] == pytest.approx(102.0)
        assert pd.isna(prices["LATE"])  # not yet listed on this date — real gap, not a bug

        prices_later = engine._price_row(dates[4])
        assert prices_later["EARLY"] == pytest.approx(104.0)
        assert prices_later["LATE"] == pytest.approx(51.0)


class TestGracePeriod:
    def test_dropped_ticker_held_for_grace_cycles_then_sold(self, monkeypatch):
        tickers = ["A", "B", "C"]
        panel = _flat_price_panel(tickers, 5)
        dates = panel.index

        rankings_by_date = {
            dates[0]: {"A": 1.0, "B": 0.5, "C": 0.1},
            dates[1]: {"A": 0.1, "B": 0.5, "C": 1.0},
            dates[2]: {"A": 0.1, "B": 0.5, "C": 1.0},
            dates[3]: {"A": 0.1, "B": 0.5, "C": 1.0},
            dates[4]: {"A": 0.1, "B": 0.5, "C": 1.0},
        }

        def fake_momentum(price_panel, tickers_arg, as_of_date, lookback_days):
            return pd.Series(rankings_by_date[pd.Timestamp(as_of_date)])

        monkeypatch.setattr(mb, "trailing_momentum_from_panel", fake_momentum)

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            starting_capital=1_000_000.0,
            investable_pct=0.3,  # generous cash headroom so buffer exhaustion
            # (tested separately in TestBufferExhaustion) doesn't confound
            # this test's isolated check of the grace-cycle countdown.
            top_n=2,
            grace_cycles=2,
        )
        result = engine.run()
        events = {e["date"]: e for e in result.rebalance_events}

        # NOTE: engine.run() executes every rebalance date in one call, so
        # engine.positions below reflects the FINAL (post-D4) state, not a
        # mid-run snapshot — the per-date bought/sold counts in
        # result.rebalance_events are the only way to check what happened
        # at each individual rebalance.
        d0, d1, d2, d3, d4 = [str(d.date()) for d in dates]
        assert events[d0]["n_bought"] == 2  # A, B bought
        assert events[d0]["n_sold"] == 0

        assert events[d1]["n_bought"] == 1  # C bought; A drops but held (grace)
        assert events[d1]["n_sold"] == 0

        assert events[d2]["n_bought"] == 0
        assert events[d2]["n_sold"] == 0

        assert events[d3]["n_sold"] == 1  # grace expired (2 cycles held), A force-sold
        assert events[d3]["n_bought"] == 0

        assert events[d4]["n_bought"] == 0
        assert events[d4]["n_sold"] == 0

        assert "A" not in engine.positions  # confirmed sold by end of run
        assert "B" in engine.positions and "C" in engine.positions

    def test_reentry_resets_grace(self, monkeypatch):
        tickers = ["A", "B"]
        panel = _flat_price_panel(tickers, 3)
        dates = panel.index

        rankings_by_date = {
            dates[0]: {"A": 1.0, "B": 0.1},
            dates[1]: {"A": 0.1, "B": 1.0},  # A drops -> grace
            dates[2]: {"A": 1.0, "B": 0.1},  # A back on top -> core again
        }

        def fake_momentum(price_panel, tickers_arg, as_of_date, lookback_days):
            return pd.Series(rankings_by_date[pd.Timestamp(as_of_date)])

        monkeypatch.setattr(mb, "trailing_momentum_from_panel", fake_momentum)

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            starting_capital=1_000_000.0,
            top_n=1,
            grace_cycles=2,
        )
        engine.run()
        assert engine.positions["A"].grace_remaining is None  # reset to core, not sold/rebought


class TestBufferExhaustion:
    def test_forces_sell_of_grace_holding_when_cash_runs_out(self, monkeypatch):
        tickers = ["A", "B"]
        panel = _flat_price_panel(tickers, 2, price=100.0)
        dates = panel.index

        rankings_by_date = {
            dates[0]: {"A": 1.0, "B": 0.1},
            dates[1]: {"A": 0.1, "B": 1.0},  # A drops (grace won't expire this run); B needs buying
        }

        def fake_momentum(price_panel, tickers_arg, as_of_date, lookback_days):
            return pd.Series(rankings_by_date[pd.Timestamp(as_of_date)])

        monkeypatch.setattr(mb, "trailing_momentum_from_panel", fake_momentum)

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            starting_capital=1_000.0,
            investable_pct=0.8,
            top_n=1,
            grace_cycles=5,  # long enough that grace alone wouldn't force the sell
        )
        result = engine.run()
        events = {e["date"]: e for e in result.rebalance_events}
        d0, d1 = [str(d.date()) for d in dates]

        assert events[d0]["n_bought"] == 1  # A bought
        # B needs funding but cash is tied up in A; buffer exhaustion forces A's sale.
        assert events[d1]["n_sold"] == 1
        assert events[d1]["n_bought"] == 1
        assert "A" not in engine.positions
        assert "B" in engine.positions


class TestLiquidityAndCircuitFilters:
    """2026-07-19 full-codebase-review Fix 1: ADTV liquidity floor, ADTV
    position-size cap, and circuit-lock deferral — all opt-in via None
    defaults, so existing behavior is unchanged unless explicitly set."""

    def test_ticker_below_min_adtv_excluded_from_buys(self, monkeypatch):
        tickers = ["A", "B"]
        panel = _flat_price_panel(tickers, 3)
        dates = panel.index
        # A has real volume -> liquid; B has zero volume -> illiquid, excluded.
        volume = pd.DataFrame({"A": [1_000_000] * 3, "B": [0] * 3}, index=dates)
        monkeypatch.setattr(
            mb, "trailing_momentum_from_panel", lambda *a, **kw: pd.Series({"A": 0.02, "B": 0.05})
        )

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            top_n=2,
            volume_panel=volume,
            min_adtv_cr=0.01,  # 100 price * 1,000,000 vol / 1e7 = 10 cr for A; 0 for B
        )
        engine.run()
        assert "A" in engine.positions
        assert "B" not in engine.positions

    def test_no_volume_panel_leaves_min_adtv_a_noop(self, monkeypatch):
        tickers = ["A"]
        panel = _flat_price_panel(tickers, 2)
        dates = panel.index
        monkeypatch.setattr(mb, "trailing_momentum_from_panel", lambda *a, **kw: pd.Series({"A": 1.0}))

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            top_n=1,
            min_adtv_cr=100.0,  # would exclude everything if enforced without volume data
        )
        engine.run()
        assert "A" in engine.positions

    def test_max_pct_of_adtv_caps_order_size(self, monkeypatch):
        tickers = ["A"]
        panel = _flat_price_panel(tickers, 2, price=100.0)
        dates = panel.index
        # ADTV = 100 * 100 / 1e7 = 0.001 cr. 5% of that = 5e-5 cr = 500 INR
        # notional -> max_qty_by_adtv = 500/100 = 5 shares, far below what
        # investable_per_slot alone would buy with a 1,000,000 starting capital.
        volume = pd.DataFrame({"A": [100] * 2}, index=dates)
        monkeypatch.setattr(mb, "trailing_momentum_from_panel", lambda *a, **kw: pd.Series({"A": 1.0}))

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            top_n=1,
            volume_panel=volume,
            max_pct_of_adtv=0.05,
        )
        engine.run()
        assert "A" in engine.positions
        assert engine.positions["A"].qty <= 5

    def test_circuit_locked_day_defers_force_sell(self, monkeypatch):
        tickers = ["A"]
        dates = pd.date_range("2026-01-01", periods=3, freq="D")
        # Day 0: 100 (buy, enters grace since target_set is always empty).
        # Day 1: 130 (+30% vs day0, "circuit-locked" by a 20% band, grace
        # hits 0 this day -> force-sell attempted but deferred).
        # Day 2: 130 (no move vs day1, not locked -> sold).
        panel = pd.DataFrame({"A": [100.0, 130.0, 130.0]}, index=dates)
        monkeypatch.setattr(mb, "trailing_momentum_from_panel", lambda *a, **kw: pd.Series(dtype=float))

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            top_n=1,
            grace_cycles=1,
            circuit_band_pct=0.20,
        )
        # Seed a held core position directly (target_set is always empty
        # since trailing_momentum_from_panel returns nothing) to isolate
        # the force-sell circuit-lock path from buy-side selection.
        engine.positions["A"] = mb.Position(qty=10, entry_price=100.0, entry_date="2026-01-01", entry_rank=1, grace_remaining=None)
        result = engine.run()

        events = {e["date"]: e for e in result.rebalance_events}
        d0, d1, d2 = [str(d.date()) for d in dates]
        assert events[d1]["n_sold"] == 0  # locked on day1 -> deferred, not sold
        assert events[d2]["n_sold"] == 1  # not locked on day2 -> sold
        assert "A" not in engine.positions  # confirmed sold by end of run

    def test_exclude_approximated_mcap_removes_ticker_from_selection(self, monkeypatch):
        tickers = ["A", "B"]
        panel = _flat_price_panel(tickers, 2)
        dates = panel.index
        monkeypatch.setattr(
            mb, "trailing_momentum_from_panel", lambda *a, **kw: pd.Series({"A": 0.05, "B": 0.02})
        )

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            top_n=2,
            approximation_flags={str(dates[0].date()): {"A": True, "B": False}},
            exclude_approximated_mcap=True,
        )
        engine.run()
        assert "A" not in engine.positions
        assert "B" in engine.positions

    def test_exclude_approximated_mcap_off_by_default(self, monkeypatch):
        tickers = ["A"]
        panel = _flat_price_panel(tickers, 2)
        dates = panel.index
        monkeypatch.setattr(mb, "trailing_momentum_from_panel", lambda *a, **kw: pd.Series({"A": 1.0}))

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            top_n=1,
            approximation_flags={str(dates[0].date()): {"A": True}},
            # exclude_approximated_mcap left at default False
        )
        engine.run()
        assert "A" in engine.positions


class TestVolumeWeightedMomentum:
    """2026-07-19 full-codebase-review Fix B1: volume-weighted position
    sizing among a rebalance's target_set, opt-in via volume_weighted."""

    def test_higher_volume_ticker_gets_larger_allocation(self, monkeypatch):
        tickers = ["A", "B"]
        panel = _flat_price_panel(tickers, 2, price=100.0)
        dates = panel.index
        # A has 10x the volume of B -> A should get proportionally more capital.
        volume = pd.DataFrame({"A": [100_000] * 2, "B": [10_000] * 2}, index=dates)
        monkeypatch.setattr(
            mb, "trailing_momentum_from_panel", lambda *a, **kw: pd.Series({"A": 0.05, "B": 0.03})
        )

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            top_n=2,
            starting_capital=1_000_000.0,
            volume_panel=volume,
            volume_weighted=True,
        )
        engine.run()

        assert "A" in engine.positions and "B" in engine.positions
        assert engine.positions["A"].qty > engine.positions["B"].qty

    def test_volume_weighted_off_by_default_gives_equal_allocation(self, monkeypatch):
        tickers = ["A", "B"]
        panel = _flat_price_panel(tickers, 2, price=100.0)
        dates = panel.index
        volume = pd.DataFrame({"A": [100_000] * 2, "B": [10_000] * 2}, index=dates)
        monkeypatch.setattr(
            mb, "trailing_momentum_from_panel", lambda *a, **kw: pd.Series({"A": 0.05, "B": 0.03})
        )

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            top_n=2,
            starting_capital=1_000_000.0,
            volume_panel=volume,
            # volume_weighted left at default False
        )
        engine.run()

        assert engine.positions["A"].qty == engine.positions["B"].qty

    def test_volume_weighted_without_volume_panel_falls_back_to_equal(self, monkeypatch):
        tickers = ["A", "B"]
        panel = _flat_price_panel(tickers, 2, price=100.0)
        dates = panel.index
        monkeypatch.setattr(
            mb, "trailing_momentum_from_panel", lambda *a, **kw: pd.Series({"A": 0.05, "B": 0.03})
        )

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            top_n=2,
            starting_capital=1_000_000.0,
            volume_weighted=True,
            # volume_panel omitted entirely
        )
        engine.run()

        assert engine.positions["A"].qty == engine.positions["B"].qty


class TestRegimeConditioning:
    """2026-07-19 full-codebase-review Fix B2: regime_series/disable_in_regimes
    skip new buys (not force-liquidate) during a disabled regime."""

    def test_disabled_regime_skips_new_buys(self, monkeypatch):
        tickers = ["A"]
        panel = _flat_price_panel(tickers, 3)
        dates = panel.index
        monkeypatch.setattr(mb, "trailing_momentum_from_panel", lambda *a, **kw: pd.Series({"A": 1.0}))

        regime_series = pd.Series(["high_vol", "high_vol", "high_vol"], index=dates)

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            top_n=1,
            regime_series=regime_series,
            disable_in_regimes={"high_vol"},
        )
        result = engine.run()

        assert "A" not in engine.positions
        assert all(e["n_bought"] == 0 for e in result.rebalance_events)

    def test_normal_regime_allows_buys(self, monkeypatch):
        tickers = ["A"]
        panel = _flat_price_panel(tickers, 2)
        dates = panel.index
        monkeypatch.setattr(mb, "trailing_momentum_from_panel", lambda *a, **kw: pd.Series({"A": 1.0}))

        regime_series = pd.Series(["normal", "normal"], index=dates)

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            top_n=1,
            regime_series=regime_series,
            disable_in_regimes={"high_vol"},
        )
        engine.run()

        assert "A" in engine.positions

    def test_no_regime_series_preserves_original_behavior(self, monkeypatch):
        tickers = ["A"]
        panel = _flat_price_panel(tickers, 2)
        dates = panel.index
        monkeypatch.setattr(mb, "trailing_momentum_from_panel", lambda *a, **kw: pd.Series({"A": 1.0}))

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            top_n=1,
            # regime_series/disable_in_regimes left at defaults
        )
        engine.run()

        assert "A" in engine.positions

    def test_missing_regime_entry_never_excludes(self, monkeypatch):
        tickers = ["A"]
        panel = _flat_price_panel(tickers, 2)
        dates = panel.index
        monkeypatch.setattr(mb, "trailing_momentum_from_panel", lambda *a, **kw: pd.Series({"A": 1.0}))

        # regime_series exists but has no entries on/before the rebalance
        # dates (e.g. real benchmark history starts later than the
        # backtest window) -> never treated as disabled.
        regime_series = pd.Series(["high_vol"], index=[dates[-1] + pd.Timedelta(days=100)])

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            top_n=1,
            regime_series=regime_series,
            disable_in_regimes={"high_vol"},
        )
        engine.run()

        assert "A" in engine.positions


class TestFactorOrthogonalization:
    """2026-07-19 full-codebase-review Fix B3: orthogonalize_vs_size_beta
    ranks/selects on residual momentum instead of raw."""

    def test_off_by_default_preserves_original_ranking(self, monkeypatch):
        tickers = ["A", "B"]
        panel = _flat_price_panel(tickers, 2)
        dates = panel.index
        monkeypatch.setattr(
            mb, "trailing_momentum_from_panel", lambda *a, **kw: pd.Series({"A": 0.10, "B": 0.05})
        )

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            top_n=1,
            # orthogonalize_vs_size_beta left at default False
        )
        engine.run()

        assert "A" in engine.positions  # highest raw momentum picked, unchanged

    def test_orthogonalize_without_market_cap_panel_is_a_noop(self, monkeypatch):
        tickers = ["A", "B"]
        panel = _flat_price_panel(tickers, 2)
        dates = panel.index
        monkeypatch.setattr(
            mb, "trailing_momentum_from_panel", lambda *a, **kw: pd.Series({"A": 0.10, "B": 0.05})
        )

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            top_n=1,
            orthogonalize_vs_size_beta=True,
            # market_cap_panel omitted -> silently no-ops
        )
        engine.run()

        assert "A" in engine.positions


class TestQualityGatedMomentum:
    """2026-07-19 full-codebase-review Fix B5: quality_scores/quality_gate
    excludes momentum candidates failing a Piotroski/Beneish threshold."""

    def test_low_f_score_ticker_excluded(self, monkeypatch):
        tickers = ["A", "B"]
        panel = _flat_price_panel(tickers, 2)
        dates = panel.index
        monkeypatch.setattr(
            mb, "trailing_momentum_from_panel", lambda *a, **kw: pd.Series({"A": 0.10, "B": 0.05})
        )

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            top_n=2,
            quality_scores={"A": {"f_score": 2}, "B": {"f_score": 7}},
            quality_gate={"min_f_score": 4},
        )
        engine.run()

        assert "A" not in engine.positions
        assert "B" in engine.positions

    def test_high_m_score_manipulator_excluded(self, monkeypatch):
        tickers = ["A", "B"]
        panel = _flat_price_panel(tickers, 2)
        dates = panel.index
        monkeypatch.setattr(
            mb, "trailing_momentum_from_panel", lambda *a, **kw: pd.Series({"A": 0.10, "B": 0.05})
        )

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            top_n=2,
            quality_scores={"A": {"m_score": -1.0}, "B": {"m_score": -3.0}},
            quality_gate={"max_m_score": -1.78},
        )
        engine.run()

        assert "A" not in engine.positions  # -1.0 > -1.78 threshold -> flagged
        assert "B" in engine.positions

    def test_ticker_missing_from_quality_scores_never_excluded(self, monkeypatch):
        tickers = ["A"]
        panel = _flat_price_panel(tickers, 2)
        dates = panel.index
        monkeypatch.setattr(mb, "trailing_momentum_from_panel", lambda *a, **kw: pd.Series({"A": 0.10}))

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            top_n=1,
            quality_scores={},  # no data for A at all
            quality_gate={"min_f_score": 4},
        )
        engine.run()

        assert "A" in engine.positions

    def test_no_quality_gate_preserves_original_behavior(self, monkeypatch):
        tickers = ["A"]
        panel = _flat_price_panel(tickers, 2)
        dates = panel.index
        monkeypatch.setattr(mb, "trailing_momentum_from_panel", lambda *a, **kw: pd.Series({"A": 0.10}))

        engine = mb.MomentumBacktester(
            price_panel=panel,
            yearly_universes={str(dates[0].date()): tickers},
            lookback_days=1,
            rebalance_every_n_trading_days=1,
            top_n=1,
            quality_scores={"A": {"f_score": 0}},
            # quality_gate left at default (empty) -> no filtering
        )
        engine.run()

        assert "A" in engine.positions
