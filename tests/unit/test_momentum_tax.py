"""tests/unit/test_momentum_tax.py — ML38 backtest/momentum_tax.py."""

import pytest

from backtest.momentum_tax import compute_total_tax, compute_transaction_tax, post_tax_ending_value


def _txn(buy_price, sell_price, qty, holding_days):
    return {"buy_price": buy_price, "sell_price": sell_price, "qty": qty, "holding_days": holding_days}


class TestComputeTransactionTax:
    def test_stcg_rate_under_365_days(self):
        txn = _txn(100.0, 150.0, 10, 364)
        # gain = 500, STCG 20% = 100
        assert compute_transaction_tax(txn) == pytest.approx(100.0)

    def test_ltcg_rate_at_365_days_and_above(self):
        txn = _txn(100.0, 150.0, 10, 365)
        # gain = 500, LTCG 12.5% = 62.5
        assert compute_transaction_tax(txn) == pytest.approx(62.5)

    def test_no_tax_on_a_loss(self):
        txn = _txn(150.0, 100.0, 10, 400)
        assert compute_transaction_tax(txn) == 0.0

    def test_no_tax_when_still_open_without_a_mark_price(self):
        txn = _txn(100.0, None, 10, 10)
        assert compute_transaction_tax(txn) == 0.0


class TestComputeTotalTax:
    def test_sums_across_transactions(self):
        txns = [_txn(100.0, 150.0, 10, 364), _txn(100.0, 150.0, 10, 365)]
        assert compute_total_tax(txns) == pytest.approx(100.0 + 62.5)


class TestPostTaxEndingValue:
    def test_subtracts_total_tax(self):
        txns = [_txn(100.0, 150.0, 10, 364)]
        assert post_tax_ending_value(1_000_000.0, txns) == pytest.approx(1_000_000.0 - 100.0)
