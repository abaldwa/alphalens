"""
Tests for the Indian capital-gains set-off rules in backtest/core/tax.py.

Regression for a bug found 2026-08-12 by scripts/validate_fy_ledger.py on the
2009-2026 technical sweep: fy_net_tax and fy_net_tax_with_regime netted gains
strictly within the STCG and LTCG buckets and never applied the short-term-loss
set-off against long-term gains, while their docstrings claimed to follow "real
Indian set-off rules". 69 of 390 runs were affected, tax overstated by ~Rs 31.25
lakh in aggregate.

The rule is ASYMMETRIC, and that asymmetry is the whole point of this file:
    short-term loss  -> may offset BOTH short-term and long-term gains
    long-term  loss  -> may offset ONLY long-term gains
So a fix that simply pools the two buckets would pass the first case below and
silently break the second. Both directions are pinned here.
"""

from datetime import date

import pytest

from backtest.core.tax import (
    LTCG_RATE,
    STCG_RATE,
    Transaction,
    fy_net_tax,
    fy_net_tax_with_regime,
    net_buckets_after_setoff,
)


def _txn(gain: float, long_term: bool, ticker: str = "ACME") -> Transaction:
    """One closed trade with the requested gain and holding-period bucket."""
    buy = date(2020, 1, 1)
    sell = date(2021, 6, 1) if long_term else date(2020, 3, 1)
    # quantity 1 keeps gain == sell_price - buy_price.
    return Transaction(
        ticker=ticker, buy_date=buy, sell_date=sell,
        buy_price=1000.0, sell_price=1000.0 + gain, quantity=1,
    )


class TestShortTermLossOffsetsLongTermGain:
    def test_short_term_loss_fully_shelters_a_smaller_long_term_gain(self):
        """The exact shape that was mis-taxed: C6 FY2022-23."""
        txns = [_txn(-1_010_928, long_term=False), _txn(505_480, long_term=True)]
        net_stcg, net_ltcg = net_buckets_after_setoff(txns)
        assert net_ltcg == pytest.approx(0.0)
        assert net_stcg == pytest.approx(-505_448)  # loss remaining, unused
        assert fy_net_tax(txns) == pytest.approx(0.0)
        # And under the regime the sweep actually ran.
        assert fy_net_tax_with_regime(txns, ltcg_rate=0.10, ltcg_exemption=100_000) == pytest.approx(0.0)

    def test_partial_shelter_taxes_only_the_surviving_long_term_gain(self):
        txns = [_txn(-200_000, long_term=False), _txn(500_000, long_term=True)]
        net_stcg, net_ltcg = net_buckets_after_setoff(txns)
        assert net_stcg == pytest.approx(0.0)
        assert net_ltcg == pytest.approx(300_000)
        assert fy_net_tax(txns) == pytest.approx(300_000 * LTCG_RATE)

    def test_exemption_applies_after_setoff_not_before(self):
        """Order matters: exempting first would under-tax."""
        txns = [_txn(-200_000, long_term=False), _txn(500_000, long_term=True)]
        # Post-set-off LTCG is 3L; minus the 1L exemption -> 2L taxable.
        got = fy_net_tax_with_regime(txns, ltcg_rate=0.10, ltcg_exemption=100_000)
        assert got == pytest.approx(200_000 * 0.10)


class TestLongTermLossMayNotOffsetShortTermGain:
    """The asymmetry. A naive 'just pool the buckets' fix breaks exactly here."""

    def test_long_term_loss_leaves_short_term_gain_fully_taxable(self):
        txns = [_txn(500_000, long_term=False), _txn(-400_000, long_term=True)]
        net_stcg, net_ltcg = net_buckets_after_setoff(txns)
        assert net_stcg == pytest.approx(500_000), "LTCG loss must not shelter STCG gain"
        assert net_ltcg == pytest.approx(-400_000)
        assert fy_net_tax(txns) == pytest.approx(500_000 * STCG_RATE)

    def test_a_losing_year_overall_can_still_owe_short_term_tax(self):
        """Net P&L is negative, yet tax is genuinely due — this is correct, and
        is why validate_fy_ledger.py cannot simply assert 'no tax on a losing
        year' without qualification."""
        txns = [_txn(300_000, long_term=False), _txn(-900_000, long_term=True)]
        assert sum(t.gain for t in txns) < 0
        assert fy_net_tax(txns) == pytest.approx(300_000 * STCG_RATE)


class TestUnchangedBehaviour:
    """Cases with no cross-bucket loss must be byte-identical to the old code."""

    def test_within_bucket_netting_is_untouched(self):
        txns = [_txn(300_000, long_term=False), _txn(-100_000, long_term=False),
                _txn(400_000, long_term=True), _txn(-150_000, long_term=True)]
        assert net_buckets_after_setoff(txns) == (pytest.approx(200_000), pytest.approx(250_000))
        assert fy_net_tax(txns) == pytest.approx(200_000 * STCG_RATE + 250_000 * LTCG_RATE)

    def test_both_buckets_in_loss_pays_nothing(self):
        txns = [_txn(-100_000, long_term=False), _txn(-200_000, long_term=True)]
        assert fy_net_tax(txns) == pytest.approx(0.0)

    def test_no_transactions(self):
        assert net_buckets_after_setoff([]) == (0.0, 0.0)
        assert fy_net_tax([]) == pytest.approx(0.0)

    def test_regime_defaults_still_reduce_to_fy_net_tax(self):
        """The contract fy_net_tax_with_regime was written to preserve."""
        for txns in (
            [_txn(300_000, long_term=False), _txn(400_000, long_term=True)],
            [_txn(-500_000, long_term=False), _txn(200_000, long_term=True)],
            [_txn(500_000, long_term=False), _txn(-400_000, long_term=True)],
        ):
            assert fy_net_tax_with_regime(txns) == pytest.approx(fy_net_tax(txns))
