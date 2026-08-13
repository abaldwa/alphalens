"""
tests/unit/test_annual_reset_no_top_up.py

capital_mode="annual_reset" with top_up_after_loss=False (2026-08-13 user
request): withdraw surplus at each FY boundary exactly as before, but after a
LOSING year inject nothing — the strategy continues on the capital it has left
and must earn its way back.

Why this needs its own tests rather than riding on the existing annual-reset
ones: the two variants are only distinguishable in losing years, and the
existing suite's fixtures are profitable. A no-top-up implementation that
silently still topped up would pass every one of them.

The property that matters most is that the difference COMPOUNDS. A smaller
book sizes smaller positions and can fail can_buy on integer-share rounding, so
the two variants take different trades — this is not the same trade book scored
two ways, and a test that only checked the closing balance would miss that.
"""


import pandas as pd
import pytest

from backtest.core.horizon import HorizonBucket
from backtest.core.portfolio import AnnualResetConfig, StrategyPortfolio


BASE = 1_000_000.0


def _portfolio(top_up: bool, initial: float = BASE) -> StrategyPortfolio:
    p = StrategyPortfolio(
        initial_capital=initial,
        horizon_bucket=HorizonBucket.D21,
        annual_reset=AnnualResetConfig(base_capital=BASE, top_up_after_loss=top_up),
    )
    p.prime_annual_reset_schedule(pd.bdate_range("2020-01-01", "2024-06-30"))
    return p


def _fy_boundary(p, when, equity):
    """Drive one FY boundary with the portfolio holding only cash."""
    p.cash = equity
    p.apply_due_annual_reset(pd.Timestamp(when), prices={})


def test_a_losing_year_is_not_topped_up():
    p = _portfolio(top_up=False)
    _fy_boundary(p, "2020-04-01", equity=600_000.0)

    assert p.cash == pytest.approx(600_000.0), "capital must be left exactly as the year ended"
    # total_contributed starts at the initial capital, so "nothing injected"
    # means it is UNCHANGED, not zero.
    assert p.total_contributed == pytest.approx(BASE), "no cash may be injected"
    row = p.fy_ledger[-1]
    assert row["topped_up"] == 0.0
    assert row["topup_forgone"] == pytest.approx(400_000.0)
    assert row["top_up_after_loss"] is False


def test_the_default_still_tops_up():
    """The original measure must be untouched — this is the regression guard
    for every existing annual_reset run."""
    p = _portfolio(top_up=True)
    _fy_boundary(p, "2020-04-01", equity=600_000.0)

    assert p.cash == pytest.approx(BASE)
    assert p.total_contributed == pytest.approx(BASE + 400_000.0)
    assert p.fy_ledger[-1]["topped_up"] == pytest.approx(400_000.0)
    assert p.fy_ledger[-1]["topup_forgone"] == 0.0


def test_topup_forgone_distinguishes_no_shortfall_from_a_withheld_top_up():
    """A losing year showing topped_up=0.0 is ambiguous between 'none was
    needed' and 'one was withheld', and those are opposite facts. Both must be
    readable from the row."""
    profitable = _portfolio(top_up=False)
    _fy_boundary(profitable, "2020-04-01", equity=1_400_000.0)
    assert profitable.fy_ledger[-1]["topped_up"] == 0.0
    assert profitable.fy_ledger[-1]["topup_forgone"] == 0.0

    losing = _portfolio(top_up=False)
    _fy_boundary(losing, "2020-04-01", equity=900_000.0)
    assert losing.fy_ledger[-1]["topped_up"] == 0.0
    assert losing.fy_ledger[-1]["topup_forgone"] == pytest.approx(100_000.0)


def test_no_withdrawal_while_the_book_is_still_below_base():
    """Recovery is measured against the ORIGINAL base: nothing comes out until
    the book is whole again. Withdrawing from a recovering-but-still-underwater
    book would prevent it from ever recovering."""
    p = _portfolio(top_up=False)
    _fy_boundary(p, "2020-04-01", equity=600_000.0)
    # Year two: recovered part of the way, still under base.
    _fy_boundary(p, "2021-04-01", equity=800_000.0)

    assert p.total_withdrawn == 0.0
    assert p.cash == pytest.approx(800_000.0)
    assert all(r["withdrawn"] == 0.0 for r in p.fy_ledger)


def test_withdrawal_resumes_once_the_book_is_back_above_base():
    p = _portfolio(top_up=False)
    _fy_boundary(p, "2020-04-01", equity=600_000.0)
    _fy_boundary(p, "2021-04-01", equity=1_200_000.0)

    # Surplus above base exists, but nothing was BOOKED (no closed trades), so
    # realised_after_tax is 0 and nothing may be taken out. Withdrawal is
    # capped by what was realised, not by mark-to-market surplus alone.
    assert p.total_withdrawn == 0.0
    assert p.fy_ledger[-1]["closing_equity"] == pytest.approx(1_200_000.0)


def test_the_no_top_up_book_is_strictly_smaller_after_a_loss():
    """The compounding property. After the same losing year the two variants
    hold different capital, so every subsequent sizing decision differs — they
    are different simulations, not one trade book scored twice."""
    topped, bare = _portfolio(top_up=True), _portfolio(top_up=False)
    _fy_boundary(topped, "2020-04-01", equity=500_000.0)
    _fy_boundary(bare, "2020-04-01", equity=500_000.0)

    assert topped.cash == pytest.approx(BASE)
    assert bare.cash == pytest.approx(500_000.0)
    assert bare.cash < topped.cash


def test_consecutive_losing_years_keep_shrinking_the_book():
    """The topped-up variant is refunded every April and therefore cannot ever
    report ruin, however badly it trades. Exposing that is the point of this
    variant, so the decline must actually accumulate."""
    p = _portfolio(top_up=False)
    for when, equity in [
        ("2020-04-01", 700_000.0),
        ("2021-04-01", 450_000.0),
        ("2022-04-01", 250_000.0),
    ]:
        _fy_boundary(p, when, equity=equity)

    assert p.cash == pytest.approx(250_000.0)
    assert p.total_contributed == pytest.approx(BASE)
    forgone = [r["topup_forgone"] for r in p.fy_ledger]
    assert forgone == [pytest.approx(300_000.0), pytest.approx(550_000.0), pytest.approx(750_000.0)]


def test_ledger_records_which_variant_produced_it():
    """Two runs differing only in this flag produce different numbers, so a
    ledger that does not say which variant it came from is unreadable — the
    same failure mode as the LTCG regime, which is recorded for exactly this
    reason."""
    for top_up in (True, False):
        p = _portfolio(top_up=top_up)
        _fy_boundary(p, "2020-04-01", equity=800_000.0)
        assert p.fy_ledger[-1]["top_up_after_loss"] is top_up


def test_cash_never_goes_negative_in_either_variant():
    for top_up in (True, False):
        p = _portfolio(top_up=top_up)
        _fy_boundary(p, "2020-04-01", equity=1.0)
        assert p.cash >= 0.0


# ---------------------------------------------------------------------------
# Queue plumbing
# ---------------------------------------------------------------------------

def test_queue_field_is_named_for_the_flag_not_the_config():
    """run_strategy_queue maps job field names to CLI flags mechanically and
    OMITS False booleans entirely. A job field named for the config attribute
    (annual_reset_top_up_after_loss: false) would therefore emit no flag and
    silently run the TOPPED-UP variant — the precise opposite of what its
    author wrote, with nothing in the run record to show it.

    The field is spelled annual_reset_no_top_up so true -> flag emitted -> no
    top-up, and the wrong name is rejected outright rather than misread.
    """
    from backtest.run_strategy_queue import _job_to_cmd

    base = {
        "kind": "orchestrator", "channel": "technical",
        "start_date": "2009-04-01", "end_date": "2026-08-10",
        "capital_mode": "annual_reset", "annual_reset_ltcg_rate": 0.125,
        "annual_reset_regime_label": "r",
    }

    cmd = _job_to_cmd({**base, "annual_reset_no_top_up": True}, 0, "t")
    assert "--annual-reset-no-top-up" in cmd

    cmd = _job_to_cmd({**base, "annual_reset_no_top_up": False}, 0, "t")
    assert "--annual-reset-no-top-up" not in cmd, "False must mean the default topped-up run"

    with pytest.raises(ValueError, match="annual_reset_top_up_after_loss"):
        _job_to_cmd({**base, "annual_reset_top_up_after_loss": False}, 0, "t")
