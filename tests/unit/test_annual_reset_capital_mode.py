"""
Tests for capital_mode="annual_reset" (backtest/core/portfolio.py).

This is the user's third performance measure (2026-08-12): start each Indian FY
on Rs 10,00,000, withdraw booked profit after tax at the FY boundary, and top the
base back up after a losing year.

The cases that matter are the ones where the naive reading of "take all profits
out and restart at 10L" is IMPOSSIBLE — a near-fully-invested portfolio has most
of its gain unrealised and very little cash, so it cannot withdraw down to the
base without selling positions the strategy never signalled an exit for. The
agreed rule lets the base drift upward instead, and these tests pin that down so
nobody later "fixes" it into a forced liquidation.
"""

from datetime import date

import pandas as pd
import pytest

from backtest.core.horizon import HorizonBucket
from backtest.core.portfolio import AnnualResetConfig, StrategyPortfolio


def _pf(annual_reset=None, capital=1_000_000.0):
    return StrategyPortfolio(
        initial_capital=capital,
        horizon_bucket=list(HorizonBucket)[0],
        annual_reset=annual_reset,
    )


def _trading_days(start="2015-01-01", end="2018-12-31"):
    # Weekdays are a good enough stand-in for the NSE calendar here; the FY
    # boundary logic only cares about the first available day on/after 1 Apr.
    return pd.bdate_range(start, end)


class TestScheduleConstruction:
    def test_fy_start_dates_skip_the_opening_year(self):
        pf = _pf(AnnualResetConfig())
        pf.prime_annual_reset_schedule(_trading_days("2015-01-01", "2018-12-31"))
        got = [str(d.date()) for d in pf._annual_reset_dates]
        # Run starts inside FY2014-15; that year opens on initial_capital and
        # must NOT be adjusted. First reset is 1 Apr 2015.
        assert got == ["2015-04-01", "2016-04-01", "2017-04-03", "2018-04-02"], got

    def test_first_of_april_on_a_weekend_rolls_to_the_next_trading_day(self):
        # 1 Apr 2017 is a Saturday -> first trading day is Monday 3 Apr.
        pf = _pf(AnnualResetConfig())
        pf.prime_annual_reset_schedule(_trading_days("2017-01-01", "2017-12-31"))
        assert [str(d.date()) for d in pf._annual_reset_dates] == ["2017-04-03"]


class TestInertWhenNotEnabled:
    """lump/sip must be untouched — this is the regression gate."""

    def test_no_schedule_no_ledger_no_cash_change(self):
        pf = _pf(annual_reset=None)
        pf.prime_annual_reset_schedule(_trading_days())
        pf.apply_due_annual_reset("2015-04-01", {})
        assert pf._annual_reset_dates is None
        assert pf.fy_ledger == []
        assert pf.cash == 1_000_000.0
        assert pf.total_withdrawn == 0.0


class TestLosingYearIsToppedUp:
    def test_top_up_restores_the_base_exactly(self):
        pf = _pf(AnnualResetConfig(base_capital=1_000_000.0))
        pf.prime_annual_reset_schedule(_trading_days("2015-01-01", "2016-12-31"))
        pf.cash = 850_000.0  # lost 1.5L, all in cash, no open positions

        pf.apply_due_annual_reset("2015-04-01", {})

        assert pf.cash == pytest.approx(1_000_000.0)
        row = pf.fy_ledger[0]
        assert row["topped_up"] == pytest.approx(150_000.0)
        assert row["withdrawn"] == 0.0
        # A loss year genuinely restarts on the base.
        assert row["opening_capital_next"] == pytest.approx(1_000_000.0)
        assert row["opened_above_base"] is False


class TestWithdrawalIsCappedByLiquidity:
    """The normal case: gains are real but sitting in open positions."""

    def test_cash_binds_when_booked_profit_exceeds_liquidity(self):
        """The case the whole design hinges on: the year booked more profit than
        is sitting in cash, because the money went straight back into positions.
        Withdrawal must clamp to cash and let the base drift, never sell."""
        pf = _pf(AnnualResetConfig(base_capital=1_000_000.0))
        pf.prime_annual_reset_schedule(_trading_days("2015-01-01", "2016-12-31"))

        # Equity 18L: only 5L liquid, 13L tied up in an open position.
        pf.cash = 500_000.0
        pf.positions = {"ACME": _open_position("ACME", qty=1000, entry=1_000.0)}
        prices = {"ACME": 1_300.0}
        # 8L gross booked, held < 1yr -> STCG 20% -> 1.6L tax -> 6.4L after tax,
        # which is MORE than the 5L of cash on hand.
        pf.trades = [_closed_trade("OLDCO", buy=date(2014, 5, 1), sell=date(2015, 2, 1),
                                   buy_px=100.0, sell_px=900.0, qty=1000)]

        pf.apply_due_annual_reset("2015-04-01", prices)

        row = pf.fy_ledger[0]
        assert row["realised_after_tax"] == pytest.approx(640_000.0)
        # Clamped to available cash, NOT to realised-after-tax.
        assert row["withdrawn"] == pytest.approx(500_000.0)
        assert row["realised_after_tax"] > row["withdrawn"]
        assert pf.cash == pytest.approx(0.0)
        # Base drifted well above 10L — this MUST be visible.
        assert row["opened_above_base"] is True
        assert row["opening_capital_next"] == pytest.approx(1_300_000.0)

    def test_realised_profit_binds_when_cash_is_ample(self):
        """Mirror image: plenty of cash, but only part of the gain was booked.
        Only the booked-and-taxed part may leave."""
        pf = _pf(AnnualResetConfig(base_capital=1_000_000.0))
        pf.prime_annual_reset_schedule(_trading_days("2015-01-01", "2016-12-31"))

        pf.cash = 500_000.0
        pf.positions = {"ACME": _open_position("ACME", qty=1000, entry=1_000.0)}
        # 2L gross booked -> STCG 20% -> 40k tax -> 1.6L after tax < 5L cash.
        pf.trades = [_closed_trade("OLDCO", buy=date(2014, 5, 1), sell=date(2015, 2, 1),
                                   buy_px=100.0, sell_px=300.0, qty=1000)]

        pf.apply_due_annual_reset("2015-04-01", {"ACME": 1_300.0})

        row = pf.fy_ledger[0]
        assert row["tax"] == pytest.approx(40_000.0)
        assert row["withdrawn"] == pytest.approx(160_000.0)
        assert pf.cash == pytest.approx(340_000.0)

    def test_positions_are_never_liquidated_to_reach_the_base(self):
        pf = _pf(AnnualResetConfig(base_capital=1_000_000.0))
        pf.prime_annual_reset_schedule(_trading_days("2015-01-01", "2016-12-31"))
        pf.cash = 500_000.0
        pf.positions = {"ACME": _open_position("ACME", qty=1000, entry=1_000.0)}
        pf.trades = [_closed_trade("OLDCO", buy=date(2014, 5, 1), sell=date(2015, 2, 1),
                                   buy_px=100.0, sell_px=300.0, qty=1000)]

        pf.apply_due_annual_reset("2015-04-01", {"ACME": 1_300.0})

        assert "ACME" in pf.positions
        assert pf.positions["ACME"].quantity == 1000


class TestWithdrawalIsCappedByRealisedProfit:
    def test_unbooked_gains_are_not_withdrawn_even_when_liquid(self):
        pf = _pf(AnnualResetConfig(base_capital=1_000_000.0))
        pf.prime_annual_reset_schedule(_trading_days("2015-01-01", "2016-12-31"))
        # 12L all in cash, but nothing was ever realised -> nothing to take out.
        pf.cash = 1_200_000.0
        pf.trades = []

        pf.apply_due_annual_reset("2015-04-01", {})

        row = pf.fy_ledger[0]
        assert row["withdrawn"] == 0.0
        assert pf.cash == pytest.approx(1_200_000.0)


class TestNeverGoesNegative:
    def test_withdrawal_cannot_overdraw_cash(self):
        pf = _pf(AnnualResetConfig(base_capital=1_000_000.0))
        pf.prime_annual_reset_schedule(_trading_days("2015-01-01", "2016-12-31"))
        pf.cash = 10_000.0
        pf.positions = {"ACME": _open_position("ACME", qty=1000, entry=1_000.0)}
        pf.trades = [_closed_trade("OLDCO", buy=date(2014, 5, 1), sell=date(2015, 2, 1),
                                   buy_px=100.0, sell_px=900.0, qty=1000)]

        pf.apply_due_annual_reset("2015-04-01", {"ACME": 1_500.0})

        assert pf.cash >= 0.0


class TestReturnIsMeasuredOnActualOpeningCapital:
    def test_return_uses_the_capital_the_year_started_with(self):
        pf = _pf(AnnualResetConfig(base_capital=1_000_000.0))
        pf.prime_annual_reset_schedule(_trading_days("2015-01-01", "2017-12-31"))
        pf.cash = 1_200_000.0
        pf.apply_due_annual_reset("2015-04-01", {})
        assert pf.fy_ledger[0]["return_on_opening_pct"] == pytest.approx(20.0)

        # Year 2 opens on 12L (drifted), so a 13.2L close is +10%, not +32%.
        pf.cash = 1_320_000.0
        pf.apply_due_annual_reset("2016-04-01", {})
        row2 = pf.fy_ledger[1]
        assert row2["opening_capital"] == pytest.approx(1_200_000.0)
        assert row2["return_on_opening_pct"] == pytest.approx(10.0)


# --------------------------------------------------------------------------
# helpers


def _open_position(ticker, qty, entry):
    from backtest.core.portfolio import Position

    return Position(
        ticker=ticker, quantity=qty, entry_price=entry,
        entry_date=date(2015, 1, 5), sector="IT",
    )


def _closed_trade(ticker, buy, sell, buy_px, sell_px, qty):
    from backtest.core.portfolio import Trade

    gross = (sell_px - buy_px) * qty
    return Trade(
        ticker=ticker, entry_date=buy, exit_date=sell,
        entry_price=buy_px, exit_price=sell_px, quantity=qty,
        pnl_inr=gross, pnl_pct=(sell_px / buy_px - 1) * 100, cost_inr=0.0,
        exit_reason="target",
    )


class TestFinancialYearLabelling:
    """Regression: the FY label drives which realised trades are taxed and
    withdrawn, so mislabelling it silently withdraws the wrong amount.

    Caught 2026-08-12 by the pre-sweep smoke test — a 17-year ledger came out
    with 2013/2019/2024 duplicated and 2012/2017/2023 missing, because the old
    code derived the closed FY from `reset_date - 1 day`. When 1 April is a
    weekend or holiday the first trading day is 2-3 April, so that subtraction
    stays inside the NEW FY and names the wrong year.
    """

    def test_one_row_per_fy_no_duplicates_no_gaps(self):
        pf = _pf(AnnualResetConfig(base_capital=1_000_000.0))
        days = _trading_days("2009-04-01", "2026-03-31")
        pf.prime_annual_reset_schedule(days)
        for d in pf._annual_reset_dates:
            pf.cash = 1_000_000.0
            pf.apply_due_annual_reset(d, {})

        labels = [r["fy_end"] for r in pf.fy_ledger]
        assert len(labels) == len(set(labels)), f"duplicate FY labels: {labels}"
        years = sorted(int(label[:4]) for label in labels)
        assert years == list(range(years[0], years[-1] + 1)), f"gap in FY sequence: {years}"

    def test_label_is_correct_when_april_first_is_not_a_trading_day(self):
        # 1 Apr 2012 = Sunday, 1 Apr 2017 = Saturday, 1 Apr 2018 = Sunday.
        pf = _pf(AnnualResetConfig(base_capital=1_000_000.0))
        pf.prime_annual_reset_schedule(_trading_days("2011-06-01", "2012-12-31"))
        reset = pf._annual_reset_dates[0]
        assert str(reset.date()) == "2012-04-02"  # Monday
        pf.apply_due_annual_reset(reset, {})
        # The FY that just closed is 2011-12 -> ends 2012-03-31, NOT 2013-03-31.
        assert pf.fy_ledger[0]["fy_end"] == "2012-03-31"
