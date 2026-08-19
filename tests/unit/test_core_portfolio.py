"""tests/unit/test_core_portfolio.py — backtest/core/portfolio.py."""

from datetime import date

import pandas as pd
import pytest

from backtest.core.horizon import HorizonBucket, sizing_for
from backtest.core.portfolio import SipConfig, StrategyPortfolio
from config.settings import MIN_ADT_INR


def _portfolio(**overrides):
    defaults = dict(initial_capital=1_000_000.0, horizon_bucket=HorizonBucket.D21)
    defaults.update(overrides)
    return StrategyPortfolio(**defaults)


class TestInitialization:
    def test_rejects_non_positive_capital(self):
        with pytest.raises(ValueError):
            _portfolio(initial_capital=0.0)

    def test_initial_cash_flow_recorded_with_none_date_until_primed(self):
        p = _portfolio()
        # `kind` tags what each flow IS (initial / sip / tax / withdrawal /
        # topup) so engine._finalize can build the XIRR series from the flows
        # the investor actually made — tax is an expense of the book, not a
        # receipt of theirs, and used to be counted as one.
        assert p.cash_flows == [
            {"date": None, "amount": -1_000_000.0, "kind": "initial"}
        ]

    def test_sizing_pulled_from_horizon_bucket(self):
        p = _portfolio(horizon_bucket=HorizonBucket.D5)
        expected = sizing_for(HorizonBucket.D5)
        assert p.sizing.max_position_pct == expected.max_position_pct


class TestPositionSizing:
    def test_caps_at_horizon_bucket_max_position_pct(self):
        p = _portfolio(horizon_bucket=HorizonBucket.D5, n_target_positions=1)  # would otherwise be 100% equal-weight
        qty = p.position_size(price=100.0, portfolio_value=1_000_000.0)
        # D5 max_position_pct = 0.02 -> 20,000 INR budget -> 200 shares
        assert qty == 200

    def test_adtv_cap_restricts_size_when_binding(self):
        p = _portfolio(horizon_bucket=HorizonBucket.Y1, adtv_cap_fraction=0.10)  # Y1 max_position_pct=0.05 -> 50,000 budget
        # adtv_cr = 0.001 crore = 10,000 INR ADTV -> cap = 10% of 10,000 = 1,000 INR budget, well below the 50,000 position cap
        qty = p.position_size(price=100.0, portfolio_value=1_000_000.0, adtv_cr=0.001)
        assert qty == 10

    def test_rejects_non_positive_price(self):
        p = _portfolio()
        with pytest.raises(ValueError):
            p.position_size(price=0.0, portfolio_value=1_000_000.0)

    def test_weight_multiplier_defaults_to_todays_behavior(self):
        """2026-08-05 (Momentum volume-weighted sizing): the new optional
        param must be a strict no-op for every existing caller."""
        p = _portfolio(horizon_bucket=HorizonBucket.Y1, n_target_positions=10)
        assert p.position_size(price=100.0, portfolio_value=1_000_000.0) == p.position_size(
            price=100.0, portfolio_value=1_000_000.0, weight_multiplier=1.0,
        )

    def test_weight_multiplier_scales_the_equal_weight_slot(self):
        # n=25 -> 4,000 equal-weight slot, below Y1's 5,000 max_position cap,
        # so the multiplier (not a cap) is what binds.
        p = _portfolio(horizon_bucket=HorizonBucket.Y1, n_target_positions=25)
        assert p.position_size(price=100.0, portfolio_value=100_000.0) == 40
        assert p.position_size(price=100.0, portfolio_value=100_000.0, weight_multiplier=0.5) == 20

    def test_weight_multiplier_never_overrides_the_max_position_cap(self):
        p = _portfolio(horizon_bucket=HorizonBucket.Y1, n_target_positions=10)
        # 2x the 100,000 slot is 200,000, but max_position_pct=0.05 caps at 50,000
        assert p.position_size(price=100.0, portfolio_value=1_000_000.0, weight_multiplier=2.0) == 500

    def test_rejects_non_positive_weight_multiplier(self):
        p = _portfolio()
        with pytest.raises(ValueError):
            p.position_size(price=100.0, portfolio_value=1_000_000.0, weight_multiplier=0.0)


class TestBuySell:
    def test_buy_reduces_cash_and_opens_position(self):
        p = _portfolio(horizon_bucket=HorizonBucket.D5, n_target_positions=1)
        pos = p.buy("RELIANCE", "Energy", 100.0, date(2020, 1, 1), {"RELIANCE": 100.0})
        assert pos is not None
        assert pos.quantity == 200
        assert p.cash == pytest.approx(1_000_000.0 - 200 * 100.0)

    def test_buy_rejected_if_already_held(self):
        p = _portfolio(horizon_bucket=HorizonBucket.D5, n_target_positions=1)
        p.buy("RELIANCE", "Energy", 100.0, date(2020, 1, 1), {"RELIANCE": 100.0})
        second = p.buy("RELIANCE", "Energy", 100.0, date(2020, 1, 2), {"RELIANCE": 100.0})
        assert second is None

    def test_buy_hard_rejected_below_min_adt_floor_not_just_resized(self):
        """[BUG FIX, 6th fundamental-strategies review, item 3] a ticker
        whose real ADTV is below MIN_ADT_INR must be excluded from trading
        entirely, not merely sized down by adtv_cap_fraction."""
        p = _portfolio(horizon_bucket=HorizonBucket.D5, n_target_positions=1)
        sub_floor_adtv_cr = (MIN_ADT_INR / 1e7) * 0.5  # half the floor, in crore
        pos = p.buy(
            "ILLIQUIDCO", "Energy", 100.0, date(2020, 1, 1), {"ILLIQUIDCO": 100.0}, adtv_cr=sub_floor_adtv_cr,
        )
        assert pos is None
        assert p.can_buy("ILLIQUIDCO", "Energy", 100.0, {"ILLIQUIDCO": 100.0}, adtv_cr=sub_floor_adtv_cr) is False

    def test_buy_allowed_at_or_above_min_adt_floor(self):
        p = _portfolio(horizon_bucket=HorizonBucket.D5, n_target_positions=1)
        above_floor_adtv_cr = (MIN_ADT_INR / 1e7) * 5.0
        pos = p.buy(
            "LIQUIDCO", "Energy", 100.0, date(2020, 1, 1), {"LIQUIDCO": 100.0}, adtv_cr=above_floor_adtv_cr,
        )
        assert pos is not None

    def test_buy_with_no_adtv_data_is_not_rejected_by_the_liquidity_floor(self):
        """None (no ADTV data) is the separate, already-tracked
        'no_adtv_data_position_sized_uncapped' case — must remain allowed
        (uncapped), not conflated with a known sub-floor rejection."""
        p = _portfolio(horizon_bucket=HorizonBucket.D5, n_target_positions=1)
        pos = p.buy("NODATACO", "Energy", 100.0, date(2020, 1, 1), {"NODATACO": 100.0}, adtv_cr=None)
        assert pos is not None

    def test_sell_before_min_holding_days_is_a_no_op(self):
        p = _portfolio(horizon_bucket=HorizonBucket.D21, n_target_positions=1)  # min_holding_days=5
        p.buy("RELIANCE", "Energy", 100.0, date(2020, 1, 1), {"RELIANCE": 100.0})
        trade = p.sell("RELIANCE", 110.0, date(2020, 1, 2), reason="signal")  # only 1 day held
        assert trade is None
        assert "RELIANCE" in p.positions

    def test_sell_after_min_holding_days_closes_position(self):
        p = _portfolio(horizon_bucket=HorizonBucket.D21, n_target_positions=1)  # min_holding_days=5
        p.buy("RELIANCE", "Energy", 100.0, date(2020, 1, 1), {"RELIANCE": 100.0})
        trade = p.sell("RELIANCE", 110.0, date(2020, 1, 10), reason="signal")
        assert trade is not None
        assert "RELIANCE" not in p.positions

    def test_forced_close_bypasses_min_holding_days(self):
        p = _portfolio(horizon_bucket=HorizonBucket.D21, n_target_positions=1)
        p.buy("RELIANCE", "Energy", 100.0, date(2020, 1, 1), {"RELIANCE": 100.0})
        trade = p.force_close("RELIANCE", 90.0, date(2020, 1, 2), reason="forced_close")
        assert trade is not None
        assert "RELIANCE" not in p.positions

    def test_force_close_on_unheld_ticker_returns_none(self):
        p = _portfolio()
        assert p.force_close("TCS", 100.0, date(2020, 1, 1)) is None


class TestSipInjection:
    def test_prime_sip_schedule_stamps_first_cash_flow_date(self):
        p = _portfolio(sip=SipConfig(amount=100_000.0))
        trading_days = pd.date_range("2020-01-01", periods=100, freq="B")
        p.prime_sip_schedule(trading_days)
        assert p.cash_flows[0]["date"] == str(trading_days[0].date())

    def test_no_sip_injections_due_in_first_month(self):
        p = _portfolio(sip=SipConfig(amount=100_000.0))
        trading_days = pd.date_range("2020-01-01", periods=20, freq="B")
        p.prime_sip_schedule(trading_days)
        p.apply_due_sip_injections(trading_days[10])
        assert p.cash == 1_000_000.0
        assert p.total_contributed == 1_000_000.0

    def test_sip_injection_applied_once_second_month_begins(self):
        p = _portfolio(sip=SipConfig(amount=100_000.0))
        trading_days = pd.date_range("2020-01-01", periods=60, freq="B")  # spans Jan-Mar 2020
        p.prime_sip_schedule(trading_days)
        p.apply_due_sip_injections(date(2020, 2, 15))
        assert p.cash == pytest.approx(1_100_000.0)
        assert p.total_contributed == pytest.approx(1_100_000.0)
        assert len(p.cash_flows) == 2  # initial + 1 SIP contribution

    def test_no_sip_config_means_no_injections_ever(self):
        p = _portfolio(sip=None)
        trading_days = pd.date_range("2020-01-01", periods=60, freq="B")
        p.prime_sip_schedule(trading_days)
        p.apply_due_sip_injections(date(2020, 3, 1))
        assert p.cash == 1_000_000.0

    def test_sip_config_rejects_non_monthly_cadence(self):
        with pytest.raises(ValueError):
            SipConfig(amount=100_000.0, cadence="weekly")

    def test_sip_config_rejects_non_positive_amount(self):
        with pytest.raises(ValueError):
            SipConfig(amount=0.0)


class TestEquityAndCashPositionSeries:
    def test_record_equity_populates_both_series(self):
        p = _portfolio()
        p.record_equity(date(2020, 1, 1), {})
        assert len(p.equity_curve) == 1
        assert p.cash_position_series[0]["cash"] == 1_000_000.0

    def test_equity_curve_reflects_open_position_mark_to_market(self):
        p = _portfolio(horizon_bucket=HorizonBucket.D5, n_target_positions=1)
        p.buy("RELIANCE", "Energy", 100.0, date(2020, 1, 1), {"RELIANCE": 100.0})
        p.record_equity(date(2020, 1, 2), {"RELIANCE": 120.0})
        # cash after buy: 1,000,000 - 200*100 = 980,000; +200*120 mark-to-market = 1,004,000
        assert p.equity_curve.iloc[-1] == pytest.approx(1_004_000.0)


class TestTaxTransactions:
    def test_closed_trade_converts_to_tax_transaction(self):
        p = _portfolio(horizon_bucket=HorizonBucket.D5, n_target_positions=1)
        p.buy("RELIANCE", "Energy", 100.0, date(2020, 1, 1), {"RELIANCE": 100.0})
        p.sell("RELIANCE", 150.0, date(2020, 1, 10), reason="signal")
        txns = p.tax_transactions()
        assert len(txns) == 1
        assert txns[0].ticker == "RELIANCE"
        assert txns[0].buy_date == date(2020, 1, 1)
        assert txns[0].sell_date == date(2020, 1, 10)

    def test_open_position_produces_no_tax_transaction(self):
        p = _portfolio(horizon_bucket=HorizonBucket.D5, n_target_positions=1)
        p.buy("RELIANCE", "Energy", 100.0, date(2020, 1, 1), {"RELIANCE": 100.0})
        assert p.tax_transactions() == []
