"""
tests/unit/test_income_mode_variants.py

A88: the two "regular returns" variants the user asked for.

    top_up_after_loss=True   a losing year is refunded to base capital
    top_up_after_loss=False  the strategy carries on with what it has left and
                             must earn its way back

They answer different questions and neither rescores the other. The topped-up
variant measures return on a maintained base. The no-top-up variant is the
only one that CAN report ruin, because a run refunded every April cannot go
broke however badly it trades.
"""

from __future__ import annotations

from backtest.core.report import _income_from_ledger, from_run_result


def _ledger(top_up: bool):
    """Three years: a gain, a loss, a gain."""
    return [
        {"fy_end": "2021-03-31", "withdrawn": 50_000.0, "topped_up": 0.0,
         "topup_forgone": 0.0, "top_up_after_loss": top_up},
        {"fy_end": "2022-03-31", "withdrawn": 0.0,
         "topped_up": 80_000.0 if top_up else 0.0,
         "topup_forgone": 0.0 if top_up else 80_000.0,
         "top_up_after_loss": top_up},
        {"fy_end": "2023-03-31", "withdrawn": 30_000.0, "topped_up": 0.0,
         "topup_forgone": 0.0, "top_up_after_loss": top_up},
    ]


def test_the_topped_up_variant_reports_what_it_injected():
    income = _income_from_ledger(_ledger(top_up=True))
    assert income.total_injected == 80_000.0
    assert income.total_withdrawn == 80_000.0
    assert income.top_up_after_loss is True


def test_the_no_top_up_variant_injects_nothing():
    """The point of the variant: no external money arrives, so the strategy
    has to earn its way back from the loss."""
    income = _income_from_ledger(_ledger(top_up=False))
    assert income.total_injected == 0.0
    assert income.top_up_after_loss is False


def test_a_losing_year_is_not_counted_as_survived_in_either_variant():
    """A year that needed a top-up and a year that went without one are both
    losing years. Counting the second as 'survived' because no cash moved
    would make the harsher variant look better than the gentler one."""
    assert _income_from_ledger(_ledger(top_up=True)).years_survived_pct == 2 / 3
    assert _income_from_ledger(_ledger(top_up=False)).years_survived_pct == 2 / 3


def test_a_lump_sum_run_reports_no_income_block():
    """Reporting zeros for a run with no withdrawal behaviour would make it
    look like an income strategy that never paid out."""
    assert _income_from_ledger(None) is None
    assert _income_from_ledger([]) is None


class _Run:
    channel = "momentum"
    start_date = "2020-04-01"
    end_date = "2023-03-31"
    capital_mode = "annual_reset"
    initial_capital = 1_000_000.0


class _Result:
    def __init__(self, ledger):
        self.metrics = {"cagr": 0.12, "tax_basis": "post_tax"}
        self.run = _Run()
        self.fy_ledger = ledger
        self.equity_curve = []
        self.benchmark_curve = []
        self.exit_policy_variant = None
        self.strategy_key = "momentum:balanced_b1"


def test_income_reaches_the_shared_report():
    r = from_run_result(_Result(_ledger(top_up=False)))
    assert r.income is not None
    assert r.income.n_years == 3
    assert r.income.top_up_after_loss is False


def test_report_income_is_none_for_a_lump_run():
    r = from_run_result(_Result([]))
    assert r.income is None
