"""
tests/unit/test_tax_bases.py

A86: one run reports both tax bases.

Momentum previously needed two full executions (withhold_fy_tax=True re-ran
the engine) and Technical computed tax post-hoc on the trade book, so the two
channels' "post-tax" numbers were not the same measure — which made the
post-tax column across channels a comparison of two different things.
"""

from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd

from backtest.core.metrics import compute_metrics, reconstruct_pre_tax_curve
from backtest.core.report import from_run_result


def _curve():
    idx = pd.date_range("2015-04-01", "2026-03-31", freq="B")
    return pd.Series(100_000 * (1.0004 ** np.arange(len(idx))), index=idx)


def _metrics(**kw):
    return compute_metrics(
        equity_curve=_curve(),
        cash_flows=[("2015-04-01", -100_000.0)],
        trade_pnls=[100.0],
        trade_values=[1000.0],
        distinct_tickers=["A"],
        start_date=dt.date(2015, 4, 1),
        end_date=dt.date(2026, 3, 31),
        total_contributed=100_000.0,
        **kw,
    )


LEDGER = [
    {"fy_end": "2020-03-31", "paid": 5000.0},
    {"fy_end": "2023-03-31", "paid": 8000.0},
]


def test_a_run_states_which_basis_its_cagr_is_on():
    """Reading a CAGR without knowing the basis is how a post-tax figure gets
    compared with a pre-tax one and the difference is read as skill."""
    assert _metrics(tax_ledger=LEDGER, deduct_tax_annually=True).tax_basis == "post_tax"
    assert _metrics().tax_basis == "pre_tax"


def test_both_bases_come_from_one_execution():
    m = _metrics(tax_ledger=LEDGER, deduct_tax_annually=True)
    assert m.cagr is not None and m.cagr_other_basis is not None
    # Adding tax back can only raise the return.
    assert m.cagr_other_basis > m.cagr
    assert m.total_tax_paid == 13_000.0


def test_a_run_that_paid_no_tax_reports_no_second_basis():
    """Fabricating a post-tax figure for a run that never modelled tax would
    be a guess presented as a measurement."""
    m = _metrics()
    assert m.cagr_other_basis is None
    assert m.total_tax_paid is None


def test_reconstruction_adds_each_payment_from_its_own_date():
    """Tax leaves as a dated cash outflow, so a payment must only affect the
    curve from the date it was actually paid — adding the total everywhere
    would inflate the early years, which is where compounding does most work."""
    curve = _curve()
    pre = reconstruct_pre_tax_curve(curve, LEDGER)
    before_first = pd.Timestamp("2019-01-01")
    between = pd.Timestamp("2021-01-01")
    assert pre.loc[before_first] == curve.loc[before_first]
    assert abs((pre.loc[between] - curve.loc[between]) - 5000.0) < 1e-6
    assert abs((pre.iloc[-1] - curve.iloc[-1]) - 13_000.0) < 1e-6


def test_reconstruction_is_none_without_a_ledger():
    assert reconstruct_pre_tax_curve(_curve(), None) is None
    assert reconstruct_pre_tax_curve(_curve(), []) is None
    assert reconstruct_pre_tax_curve(_curve(), [{"fy_end": "2020-03-31", "paid": 0.0}]) is None


class _Run:
    channel = "technical"
    start_date = "2015-04-01"
    end_date = "2026-03-31"
    capital_mode = "lump"
    initial_capital = 100_000.0


class _Result:
    def __init__(self, metrics):
        self.metrics = metrics
        self.run = _Run()
        self.equity_curve = []
        self.benchmark_curve = []
        self.exit_policy_variant = None


def test_the_report_assigns_each_basis_to_the_right_field():
    """The assignment follows the recorded basis rather than assuming, so a
    post-tax run's headline number never lands in the pre-tax column."""
    r = from_run_result(
        _Result({"cagr": 0.10996, "cagr_other_basis": 0.11405, "tax_basis": "post_tax"}),
        strategy_key="technical:A1",
    )
    assert r.returns.cagr_post_tax == 0.10996
    assert r.returns.cagr_pre_tax == 0.11405
    assert "returns.cagr_post_tax" not in r.pending


def test_a_pre_tax_run_reports_no_post_tax_figure():
    r = from_run_result(
        _Result({"cagr": 0.11405, "tax_basis": "pre_tax"}), strategy_key="technical:A1"
    )
    assert r.returns.cagr_pre_tax == 0.11405
    assert r.returns.cagr_post_tax is None
    assert r.pending["returns.cagr_post_tax"].backlog_id == "A86"
