"""
tests/quality/test_property_invariants.py

Property-based invariants for AlphaLens' core financial logic, using
Hypothesis to explore the input space far beyond hand-written examples.

The first suite targets `backtest/core/tax.py`'s Indian capital-gains
netting rules, because those rules are asymmetric and easy to get subtly
wrong (see the 2026-08-12 bug where the short-term-loss set-off was
omitted entirely). A property test is the right tool here: the set-off
rule is a pure function of (net_stcg, net_ltcg), so we can assert
invariants that must hold for EVERY combination, not just the handful a
human would write.

Invariants asserted (each is a real property of the Income-tax Act
s.70/s.74 set-off as implemented in `net_buckets_after_setoff`):

1.  **No tax on a net loss.** If both buckets are <= 0, tax is exactly 0.
2.  **Long-term loss never shelters short-term gain.** If net_ltcg < 0,
    net_stcg is returned unchanged (the asymmetry that a naive pooling
    fix breaks).
3.  **Short-term loss shelters long-term gain, up to the loss.** The
    long-term bucket is reduced by at most the short-term loss; the
    short-term bucket never goes below 0 when it was positive.
4.  **Conservation of total gain.** The sum of the two buckets is
    unchanged by set-off (set-off only reclassifies, never destroys or
    creates gain).
5.  **Tax is monotone non-decreasing in each bucket.** More gain never
    means less tax, holding the other bucket fixed.
6.  **Regime exemption never increases tax.** With a positive exemption,
    tax is <= the no-exemption tax for the same netting.
7.  **Zero transactions => zero tax.** The empty FY owes nothing.

These are pure-function tests: no DB, no network, no fixtures. They run
fast and are deterministic given the Hypothesis seed.
"""

from datetime import date

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from backtest.core.tax import (
    LTCG_RATE,
    STCG_RATE,
    Transaction,
    apply_stcg_loss_setoff,
    fy_net_tax,
    fy_net_tax_with_regime,
    net_buckets_after_setoff,
)

# A gain/loss magnitude that keeps arithmetic exact enough for approx
# comparisons while still exercising large values (lakhs of rupees).
_GAIN = st.floats(min_value=-5_000_000.0, max_value=5_000_000.0, allow_nan=False, allow_infinity=False)


def _txn(gain: float, long_term: bool) -> Transaction:
    """One closed trade with the requested gain and holding-period bucket."""
    buy = date(2020, 1, 1)
    sell = date(2021, 6, 1) if long_term else date(2020, 3, 1)
    return Transaction(
        ticker="ACME", buy_date=buy, sell_date=sell,
        buy_price=1000.0, sell_price=1000.0 + gain, quantity=1,
    )


@settings(max_examples=200, deadline=None)
@given(stcg=_GAIN, ltcg=_GAIN)
def test_setoff_conserves_total_gain(stcg: float, ltcg: float) -> None:
    """Set-off reclassifies gain between buckets but never changes the total."""
    net_stcg, net_ltcg = apply_stcg_loss_setoff(stcg, ltcg)
    assert net_stcg + net_ltcg == stcg + ltcg


@settings(max_examples=200, deadline=None)
@given(stcg=_GAIN, ltcg=_GAIN)
def test_long_term_loss_never_shelters_short_term_gain(stcg: float, ltcg: float) -> None:
    """The asymmetry: a net LTCG loss must leave the STCG bucket untouched."""
    if ltcg < 0:
        net_stcg, _ = apply_stcg_loss_setoff(stcg, ltcg)
        assert net_stcg == stcg


@settings(max_examples=200, deadline=None)
@given(stcg=_GAIN, ltcg=_GAIN)
def test_short_term_loss_shelters_long_term_gain_bounded(stcg: float, ltcg: float) -> None:
    """STCG loss reduces LTCG by at most the loss; STCG never goes below 0."""
    net_stcg, net_ltcg = apply_stcg_loss_setoff(stcg, ltcg)
    if stcg < 0 and ltcg > 0:
        # The long-term gain is reduced by exactly the loss (or fully absorbed).
        assert net_ltcg == max(0.0, ltcg + stcg)
        assert net_stcg == min(0.0, stcg + ltcg)
    else:
        # No set-off applies; both buckets unchanged.
        assert net_stcg == stcg
        assert net_ltcg == ltcg


@settings(max_examples=200, deadline=None)
@given(stcg=_GAIN, ltcg=_GAIN)
def test_no_tax_on_net_loss(stcg: float, ltcg: float) -> None:
    """A year with no net gain in either bucket owes zero tax."""
    txns = []
    if stcg != 0:
        txns.append(_txn(stcg, long_term=False))
    if ltcg != 0:
        txns.append(_txn(ltcg, long_term=True))
    if stcg <= 0 and ltcg <= 0:
        assert fy_net_tax(txns) == 0.0


@settings(max_examples=200, deadline=None)
@given(stcg=_GAIN, ltcg=_GAIN)
def test_tax_monotone_in_each_bucket(stcg: float, ltcg: float) -> None:
    """More gain never means less tax, holding the other bucket fixed."""
    base = fy_net_tax([_txn(stcg, False), _txn(ltcg, True)])
    bumped_stcg = fy_net_tax([_txn(stcg + 1000.0, False), _txn(ltcg, True)])
    bumped_ltcg = fy_net_tax([_txn(stcg, False), _txn(ltcg + 1000.0, True)])
    assert bumped_stcg >= base - 1e-6
    assert bumped_ltcg >= base - 1e-6


@settings(max_examples=200, deadline=None)
@given(stcg=_GAIN, ltcg=_GAIN)
def test_regime_exemption_never_increases_tax(stcg: float, ltcg: float) -> None:
    """A positive LTCG exemption can only reduce (or leave) the tax."""
    txns = [_txn(stcg, False), _txn(ltcg, True)]
    no_exemption = fy_net_tax_with_regime(txns, ltcg_rate=LTCG_RATE, ltcg_exemption=0.0)
    with_exemption = fy_net_tax_with_regime(txns, ltcg_rate=LTCG_RATE, ltcg_exemption=125_000.0)
    assert with_exemption <= no_exemption + 1e-6


def test_empty_fy_owes_zero_tax() -> None:
    """No transactions => no tax, regardless of regime."""
    assert fy_net_tax([]) == 0.0
    assert fy_net_tax_with_regime([]) == 0.0


@settings(max_examples=200, deadline=None)
@given(stcg=_GAIN, ltcg=_GAIN)
def test_net_buckets_after_setoff_matches_apply(stcg: float, ltcg: float) -> None:
    """The Transaction-level netting delegates to the shared set-off rule."""
    txns = [_txn(stcg, False), _txn(ltcg, True)]
    net_stcg, net_ltcg = net_buckets_after_setoff(txns)
    expected_stcg, expected_ltcg = apply_stcg_loss_setoff(stcg, ltcg)
    assert net_stcg == pytest.approx(expected_stcg)
    assert net_ltcg == pytest.approx(expected_ltcg)


@settings(max_examples=200, deadline=None)
@given(stcg=_GAIN, ltcg=_GAIN)
def test_tax_rate_applied_to_positive_bucket_only(stcg: float, ltcg: float) -> None:
    """Tax equals rate * positive net bucket, with no cross-bucket leakage."""
    net_stcg, net_ltcg = net_buckets_after_setoff([_txn(stcg, False), _txn(ltcg, True)])
    expected = 0.0
    if net_ltcg > 0:
        expected += net_ltcg * LTCG_RATE
    if net_stcg > 0:
        expected += net_stcg * STCG_RATE
    assert fy_net_tax([_txn(stcg, False), _txn(ltcg, True)]) == expected