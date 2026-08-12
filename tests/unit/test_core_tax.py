"""tests/unit/test_core_tax.py — backtest/core/tax.py (FY-netted tax engine)."""

from datetime import date

import pytest

from backtest.core.tax import (
    LTCG_RATE, STCG_RATE, Transaction, financial_year_end, fy_net_tax,
    fy_tax_cash_flows, group_by_financial_year, post_tax_ending_value, total_tax,
)


def _txn(ticker, buy_date, sell_date, buy_price, sell_price, qty=10):
    return Transaction(
        ticker=ticker, buy_date=buy_date, sell_date=sell_date,
        buy_price=buy_price, sell_price=sell_price, quantity=qty,
    )


class TestFinancialYearEnd:
    def test_date_after_april_1_belongs_to_next_march_31(self):
        assert financial_year_end(date(2015, 6, 1)) == date(2016, 3, 31)

    def test_date_before_april_1_belongs_to_same_year_march_31(self):
        assert financial_year_end(date(2015, 3, 15)) == date(2015, 3, 31)

    def test_march_31_itself_closes_its_own_fy(self):
        assert financial_year_end(date(2016, 3, 31)) == date(2016, 3, 31)

    def test_april_1_itself_belongs_to_the_new_fy(self):
        assert financial_year_end(date(2016, 4, 1)) == date(2017, 3, 31)


class TestHoldingClassification:
    def test_under_365_days_is_short_term(self):
        txn = _txn("X", date(2020, 1, 1), date(2020, 12, 30), 100.0, 150.0)
        assert txn.holding_days == 364
        assert not txn.is_long_term

    def test_365_days_and_above_is_long_term(self):
        txn = _txn("X", date(2020, 1, 1), date(2020, 12, 31), 100.0, 150.0)
        assert txn.holding_days == 365
        assert txn.is_long_term


class TestFyNetTax:
    def test_single_stcg_winner(self):
        txn = _txn("X", date(2020, 1, 1), date(2020, 6, 1), 100.0, 150.0, qty=10)
        # gain = 500, STCG 20% = 100
        assert fy_net_tax([txn]) == pytest.approx(500 * STCG_RATE)

    def test_single_ltcg_winner(self):
        txn = _txn("X", date(2019, 1, 1), date(2020, 6, 1), 100.0, 150.0, qty=10)
        assert fy_net_tax([txn]) == pytest.approx(500 * LTCG_RATE)

    def test_stcg_loss_offsets_stcg_gain_within_fy(self):
        winner = _txn("A", date(2020, 5, 1), date(2020, 8, 1), 100.0, 150.0, qty=10)  # +500 STCG
        loser = _txn("B", date(2020, 5, 1), date(2020, 9, 1), 100.0, 80.0, qty=10)  # -200 STCG
        # net STCG = 300, tax = 300 * 0.20 = 60 (not 100, since the loss offsets the gain)
        assert fy_net_tax([winner, loser]) == pytest.approx(300 * STCG_RATE)

    def test_net_loss_in_a_bucket_pays_zero_tax_for_that_bucket(self):
        loser = _txn("B", date(2020, 5, 1), date(2020, 9, 1), 100.0, 80.0, qty=10)
        assert fy_net_tax([loser]) == 0.0

    def test_short_term_loss_is_set_off_against_long_term_gain(self):
        """[UPDATED 2026-08-12] This test previously asserted the OPPOSITE — that
        the two buckets never interact — and so locked in a real bug rather than
        catching it. Under Income-tax Act s.70/s.74 a short-term capital loss may
        be set off against long-term gains; only a long-term loss is confined to
        its own bucket. The old expectation (500 * LTCG_RATE) overstated the tax.
        Measured cost on the 2009-2026 technical sweep: 34 annual-reset runs
        wrong enough to need re-running. See tests/unit/test_tax_setoff_rules.py
        for the full rule, including the asymmetry this must NOT break."""
        stcg_loser = _txn("A", date(2020, 5, 1), date(2020, 8, 1), 100.0, 80.0, qty=10)  # -200 STCG
        ltcg_winner = _txn("B", date(2019, 1, 1), date(2020, 6, 1), 100.0, 150.0, qty=10)  # +500 LTCG
        # -200 short-term loss shelters 200 of the long-term gain -> 300 taxable.
        assert fy_net_tax([stcg_loser, ltcg_winner]) == pytest.approx(300 * LTCG_RATE)

    def test_long_term_loss_does_not_shelter_short_term_gain(self):
        """The asymmetry — guards against 'fixing' the above by pooling buckets."""
        stcg_winner = _txn("A", date(2020, 5, 1), date(2020, 8, 1), 100.0, 150.0, qty=10)  # +500 STCG
        ltcg_loser = _txn("B", date(2019, 1, 1), date(2020, 6, 1), 100.0, 60.0, qty=10)  # -400 LTCG
        assert fy_net_tax([stcg_winner, ltcg_loser]) == pytest.approx(500 * STCG_RATE)


class TestGroupByFinancialYear:
    def test_groups_transactions_into_correct_fy_buckets(self):
        early = _txn("A", date(2020, 1, 1), date(2020, 3, 15), 100.0, 110.0)  # FY2019-20, closes 2020-03-31
        later = _txn("B", date(2020, 5, 1), date(2020, 8, 1), 100.0, 110.0)  # FY2020-21, closes 2021-03-31
        grouped = group_by_financial_year([early, later])
        assert set(grouped.keys()) == {date(2020, 3, 31), date(2021, 3, 31)}
        assert grouped[date(2020, 3, 31)] == [early]
        assert grouped[date(2021, 3, 31)] == [later]


class TestFyTaxCashFlows:
    def test_produces_one_negative_flow_per_fy_with_tax_owed(self):
        txn_fy1 = _txn("A", date(2020, 1, 1), date(2020, 3, 15), 100.0, 150.0, qty=10)  # +500 STCG, FY ends 2020-03-31
        txn_fy2 = _txn("B", date(2020, 5, 1), date(2020, 8, 1), 100.0, 150.0, qty=10)  # +500 STCG, FY ends 2021-03-31
        flows = fy_tax_cash_flows([txn_fy1, txn_fy2])
        assert flows == [
            (date(2020, 3, 31), pytest.approx(-500 * STCG_RATE)),
            (date(2021, 3, 31), pytest.approx(-500 * STCG_RATE)),
        ]

    def test_fy_with_net_loss_produces_no_cash_flow_event(self):
        loser = _txn("A", date(2020, 5, 1), date(2020, 9, 1), 100.0, 80.0, qty=10)
        assert fy_tax_cash_flows([loser]) == []


class TestTotalTaxAndPostTax:
    def test_total_tax_sums_across_fys(self):
        txn_fy1 = _txn("A", date(2020, 1, 1), date(2020, 3, 15), 100.0, 150.0, qty=10)
        txn_fy2 = _txn("B", date(2020, 5, 1), date(2020, 8, 1), 100.0, 150.0, qty=10)
        assert total_tax([txn_fy1, txn_fy2]) == pytest.approx(2 * 500 * STCG_RATE)

    def test_post_tax_ending_value_subtracts_total_tax(self):
        txn = _txn("A", date(2020, 1, 1), date(2020, 3, 15), 100.0, 150.0, qty=10)
        expected_tax = 500 * STCG_RATE
        assert post_tax_ending_value(1_000_000.0, [txn]) == pytest.approx(1_000_000.0 - expected_tax)
