"""
tests/unit/test_technical_backtest_schemas.py

T14: the Technical backtest endpoints return typed responses.

They previously returned bare Dict[str, Any] read straight off disk, so the
frontend's TypeScript interfaces were the only written-down description of the
shape and nothing checked the writer still matched. A renamed field surfaced
as a blank column rather than an error — six columns once rendered empty for a
week that way (commit 2c430777).

These tests validate the models against the shapes the reports actually
contain, including the awkward parts: dates arrive as date objects from
DuckDB, and older reports legitimately lack newer fields.
"""

from __future__ import annotations

import datetime as dt

import pytest
from pydantic import ValidationError

from datastore.api.schemas import (
    TAComparisonReportOut,
    TARollingWindowOut,
    TATradeBookOut,
)


def test_older_reports_missing_newer_fields_still_validate():
    """A required field would make a historical report unreadable rather than
    merely incomplete."""
    report = TAComparisonReportOut.model_validate(
        {
            "n_runs": 2,
            "n_strategies": 1,
            "strategies": [{"template": "A1", "lump": {"cagr_pct": 12.5}}],
        }
    )
    assert report.strategies[0].lump.cagr_pct == 12.5
    assert report.strategies[0].lump.benchmark_index_name is None
    assert report.strategies[0].lump.rolling_returns == {}


def test_rolling_windows_are_carried_as_annualised_percentages():
    """These are ALREADY rates. The model documents that so a consumer
    converts units only — re-annualising understates by roughly the window
    length, which is a defect this project has already shipped once."""
    w = TARollingWindowOut.model_validate(
        {"best_pct": 41.2, "median_pct": 33.1, "worst_pct": -3.0,
         "positive_windows": 12, "n_windows": 14}
    )
    assert w.median_pct == 33.1
    assert w.n_windows == 14


def test_trade_dates_may_arrive_as_date_objects():
    """DuckDB hands back real dates, not strings. Declaring these as str
    rejected every trade book outright."""
    book = TATradeBookOut.model_validate(
        {
            "run_id": "orch_technical_x",
            "total": 1,
            "trades": [
                {
                    "ticker": "INDOTECH",
                    "buy_date": dt.date(2009, 4, 1),
                    "sale_date": dt.date(2009, 4, 24),
                    "pnl_pct": -0.056,
                }
            ],
        }
    )
    assert book.trades[0].buy_date == dt.date(2009, 4, 1)


def test_pnl_pct_stays_a_fraction():
    """pnl_pct is a trade outcome, not a rate — it must never be annualised,
    and the flag saying it is a fraction travels with the payload."""
    book = TATradeBookOut.model_validate({"run_id": "r", "trades": []})
    assert book.pnl_pct_is_fraction is True


def test_a_strategy_without_a_template_is_rejected():
    """template is the strategy's identity in this report; a row without one
    cannot be linked to anything and would render as an unnamed line."""
    with pytest.raises(ValidationError):
        TAComparisonReportOut.model_validate({"strategies": [{"exit_variant": "risk_managed"}]})


def test_annual_reset_defaults_to_unverified():
    """The income figures are provisional: FY tax is reported but not debited,
    so equity compounds tax-free. Defaulting `unverified` to True means a
    report that omits the flag is still treated as provisional rather than
    silently promoted to final."""
    report = TAComparisonReportOut.model_validate(
        {"strategies": [{"template": "A1", "annual_reset": {"ltcg_12_5pct": {}}}]}
    )
    assert report.strategies[0].annual_reset["ltcg_12_5pct"].unverified is True
