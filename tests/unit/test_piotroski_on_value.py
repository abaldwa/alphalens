"""
tests/unit/test_piotroski_on_value.py

Pure-logic tests for systems/fundamental_analysis/quality/piotroski_on_value.py.
Exercises _build_financials() and compute_piotroski_on_value()'s gating logic
directly with explicit pd.Series inputs — no DB writes (per this project's
no-synthetic-DB-writes convention), no network.
"""

from datetime import datetime
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from systems.fundamental_analysis.quality.piotroski_on_value import (
    CHEAP_ZSCORE_THRESHOLD,
    PIOTROSKI_STRONG_GATE,
    _build_financials,
    compute_piotroski_on_value,
)

STRONG_ROW = pd.Series({
    "pat": 100.0, "total_assets": 1000.0,
    "current_assets": 500.0, "current_liabilities": 200.0,
    "fcf": 80.0, "capex": 20.0,
    "borrowings_noncurrent": 100.0,
    "shares_outstanding": 1_000_000,
    "gross_profit": 400.0, "revenue": 1000.0,
    "asset_turnover": 1.2,
})

WEAK_PRIOR_ROW = pd.Series({
    "pat": 50.0, "total_assets": 900.0,
    "current_assets": 400.0, "current_liabilities": 250.0,
    "fcf": 30.0, "capex": 10.0,
    "borrowings_noncurrent": 150.0,
    "shares_outstanding": 1_100_000,
    "gross_profit": 300.0, "revenue": 900.0,
    "asset_turnover": 1.0,
})


class TestBuildFinancials:
    def test_maps_columns_to_piotroski_keys(self):
        financials = _build_financials(STRONG_ROW, WEAK_PRIOR_ROW)
        assert financials["ni"] == 100.0
        assert financials["ta"] == 1000.0
        assert financials["cfo"] == pytest.approx(100.0)  # fcf + capex
        assert financials["roa"] == pytest.approx(0.1)
        assert financials["current_ratio"] == pytest.approx(2.5)
        assert financials["ni_yoy"] == 50.0
        assert financials["current_ratio_yoy"] == pytest.approx(1.6)

    def test_missing_prior_row_yields_nan_yoy(self):
        financials = _build_financials(STRONG_ROW, None)
        assert np.isnan(financials["ni_yoy"])
        assert np.isnan(financials["roa_yoy"])


class TestComputePiotroskiOnValue:
    def _history(self):
        return pd.DataFrame([
            {**WEAK_PRIOR_ROW.to_dict(), "fiscal_year": 2025, "quarter": 1,
             "quarter_end_date": pd.Timestamp("2025-03-31")},
            {**STRONG_ROW.to_dict(), "fiscal_year": 2026, "quarter": 1,
             "quarter_end_date": pd.Timestamp("2026-03-31")},
        ])

    def test_empty_history_fails_conservatively(self):
        with patch(
            "systems.fundamental_analysis.quality.piotroski_on_value.get_fundamentals_pit",
            return_value=pd.DataFrame(),
        ):
            result = compute_piotroski_on_value(conn=object(), ticker="X", as_of=datetime(2026, 4, 1))
        assert np.isnan(result["f_score"])
        assert result["is_cheap"] is None
        assert result["passes"] is False

    def test_strong_fscore_and_cheap_passes(self):
        with patch(
            "systems.fundamental_analysis.quality.piotroski_on_value.get_fundamentals_pit",
            return_value=self._history(),
        ), patch(
            "systems.fundamental_analysis.quality.piotroski_on_value.read_feature_day",
            return_value=pd.DataFrame({
                "ticker": ["X"], "ev_ebit_yield": [CHEAP_ZSCORE_THRESHOLD + 0.1], "book_to_market": [0.0],
            }),
        ):
            result = compute_piotroski_on_value(conn=object(), ticker="X", as_of=datetime(2026, 4, 1))
        assert result["is_cheap"] is True
        assert not np.isnan(result["f_score"])
        assert result["passes"] == (result["f_score"] >= PIOTROSKI_STRONG_GATE)

    def test_not_cheap_fails_even_with_strong_fscore(self):
        with patch(
            "systems.fundamental_analysis.quality.piotroski_on_value.get_fundamentals_pit",
            return_value=self._history(),
        ), patch(
            "systems.fundamental_analysis.quality.piotroski_on_value.read_feature_day",
            return_value=pd.DataFrame({
                "ticker": ["X"], "ev_ebit_yield": [0.0], "book_to_market": [0.0],
            }),
        ):
            result = compute_piotroski_on_value(conn=object(), ticker="X", as_of=datetime(2026, 4, 1))
        assert result["is_cheap"] is False
        assert result["passes"] is False

    def test_no_feature_panel_leaves_is_cheap_none_and_fails(self):
        with patch(
            "systems.fundamental_analysis.quality.piotroski_on_value.get_fundamentals_pit",
            return_value=self._history(),
        ), patch(
            "systems.fundamental_analysis.quality.piotroski_on_value.read_feature_day",
            return_value=None,
        ):
            result = compute_piotroski_on_value(conn=object(), ticker="X", as_of=datetime(2026, 4, 1))
        assert result["is_cheap"] is None
        assert result["passes"] is False
