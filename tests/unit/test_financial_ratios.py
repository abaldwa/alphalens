"""
tests/unit/test_financial_ratios.py

Tests features/financial_ratios.py — pure ratio derivation from raw
fundamentals line items. All tests are offline (no DB, no HTTP).
"""

import pytest

from features.financial_ratios import (
    compute_asset_turnover,
    compute_capex_intensity,
    compute_debt_to_ebitda,
    compute_debt_to_equity,
    compute_ebit,
    compute_equity,
    compute_fcf_margin,
    compute_net_debt,
    compute_roce,
    compute_roe,
    compute_shares_outstanding,
    derive_all_ratios,
)


class TestComputeEbit:
    def test_basic_subtraction(self):
        assert compute_ebit(ebitda=100.0, depreciation=20.0) == 80.0

    def test_none_ebitda_returns_none(self):
        assert compute_ebit(ebitda=None, depreciation=20.0) is None

    def test_none_depreciation_returns_none(self):
        assert compute_ebit(ebitda=100.0, depreciation=None) is None


class TestComputeNetDebt:
    def test_basic(self):
        assert compute_net_debt(total_debt=500.0, cash_and_equivalents=100.0) == 400.0

    def test_missing_input_returns_none(self):
        assert compute_net_debt(total_debt=None, cash_and_equivalents=100.0) is None


class TestComputeDebtToEbitda:
    def test_basic(self):
        assert compute_debt_to_ebitda(total_debt=300.0, ebitda=100.0) == 3.0

    def test_zero_ebitda_returns_none(self):
        assert compute_debt_to_ebitda(total_debt=300.0, ebitda=0.0) is None

    def test_negative_ebitda_returns_none(self):
        # A negative EBITDA makes the leverage ratio undefined, not a huge negative number.
        assert compute_debt_to_ebitda(total_debt=300.0, ebitda=-50.0) is None

    def test_missing_total_debt_returns_none(self):
        assert compute_debt_to_ebitda(total_debt=None, ebitda=100.0) is None


class TestComputeSharesOutstanding:
    def test_basic(self):
        assert compute_shares_outstanding(pat=100.0, eps=2.0) == 50.0

    def test_zero_eps_returns_none(self):
        assert compute_shares_outstanding(pat=100.0, eps=0.0) is None

    def test_none_eps_returns_none(self):
        assert compute_shares_outstanding(pat=100.0, eps=None) is None


class TestComputeEquity:
    def test_basic(self):
        assert compute_equity(book_value_per_share=50.0, shares_outstanding=1000.0) == 50000.0

    def test_missing_bvps_returns_none(self):
        assert compute_equity(book_value_per_share=None, shares_outstanding=1000.0) is None


class TestComputeRoe:
    def test_basic(self):
        assert compute_roe(pat=100.0, equity=1000.0) == pytest.approx(0.1)

    def test_zero_equity_returns_none(self):
        assert compute_roe(pat=100.0, equity=0.0) is None

    def test_negative_equity_returns_none(self):
        # Negative net worth makes ROE undefined/meaningless, not a fabricated negative ratio.
        assert compute_roe(pat=100.0, equity=-500.0) is None

    def test_missing_equity_returns_none(self):
        assert compute_roe(pat=100.0, equity=None) is None


class TestComputeRoce:
    def test_basic(self):
        # EBIT=150, capital employed = total_debt(300) + equity(700) = 1000
        assert compute_roce(ebit=150.0, total_debt=300.0, equity=700.0) == pytest.approx(0.15)

    def test_zero_capital_employed_returns_none(self):
        assert compute_roce(ebit=150.0, total_debt=-700.0, equity=700.0) is None

    def test_missing_ebit_returns_none(self):
        assert compute_roce(ebit=None, total_debt=300.0, equity=700.0) is None


class TestComputeDebtToEquity:
    def test_basic(self):
        assert compute_debt_to_equity(total_debt=400.0, equity=800.0) == pytest.approx(0.5)

    def test_zero_equity_returns_none(self):
        assert compute_debt_to_equity(total_debt=400.0, equity=0.0) is None


class TestComputeAssetTurnover:
    def test_basic(self):
        assert compute_asset_turnover(revenue=500.0, current_assets=250.0) == 2.0

    def test_missing_current_assets_returns_none(self):
        assert compute_asset_turnover(revenue=500.0, current_assets=None) is None


class TestComputeFcfMargin:
    def test_basic(self):
        assert compute_fcf_margin(fcf=50.0, revenue=500.0) == pytest.approx(0.1)


class TestComputeCapexIntensity:
    def test_basic(self):
        assert compute_capex_intensity(capex=25.0, revenue=500.0) == pytest.approx(0.05)


class TestDeriveAllRatios:
    def test_full_row_with_equity_inputs(self):
        row = {
            "ebitda": 100.0,
            "depreciation": 20.0,
            "total_debt": 300.0,
            "cash_and_equivalents": 50.0,
            "pat": 60.0,
            "eps": 3.0,
            "book_value_per_share": 40.0,
            "shares_outstanding": None,
            "revenue": 1000.0,
            "current_assets": 400.0,
            "fcf": 70.0,
            "capex": 30.0,
        }
        result = derive_all_ratios(row)
        assert result["ebit"] == 80.0
        assert result["net_debt"] == 250.0
        assert result["debt_to_ebitda"] == 3.0
        # shares derived from pat/eps = 60/3 = 20; equity = bvps*shares = 40*20 = 800
        assert result["roe"] == pytest.approx(60.0 / 800.0)
        assert result["debt_to_equity"] == pytest.approx(300.0 / 800.0)
        assert result["asset_turnover"] == pytest.approx(1000.0 / 400.0)
        assert result["fcf_margin"] == pytest.approx(0.07)
        assert result["capex_intensity"] == pytest.approx(0.03)

    def test_sparse_row_only_populates_what_raw_inputs_allow(self):
        # No equity-related inputs at all (book_value_per_share missing) —
        # roe/roce/debt_to_equity must stay honestly None, not estimated.
        row = {
            "ebitda": 100.0,
            "depreciation": 20.0,
            "total_debt": 300.0,
            "cash_and_equivalents": None,
            "pat": 60.0,
            "eps": None,
            "book_value_per_share": None,
            "shares_outstanding": None,
            "revenue": 1000.0,
            "current_assets": None,
            "fcf": None,
            "capex": None,
        }
        result = derive_all_ratios(row)
        assert result["ebit"] == 80.0
        assert result["debt_to_ebitda"] == 3.0
        assert result["roe"] is None
        assert result["roce"] is None
        assert result["debt_to_equity"] is None
        assert result["asset_turnover"] is None
        assert result["net_debt"] is None
        assert result["fcf_margin"] is None
        assert result["capex_intensity"] is None

    def test_prefers_scraped_shares_outstanding_when_present(self):
        row = {
            "ebitda": 100.0, "depreciation": 20.0, "total_debt": 300.0,
            "cash_and_equivalents": 50.0, "pat": 60.0, "eps": 3.0,
            "book_value_per_share": 40.0,
            "shares_outstanding": 25.0,  # scraped value, differs from pat/eps=20
            "revenue": 1000.0, "current_assets": 400.0, "fcf": 70.0, "capex": 30.0,
        }
        result = derive_all_ratios(row)
        # equity = 40 * 25 (scraped shares wins) = 1000, not 40*20=800
        assert result["roe"] == pytest.approx(60.0 / 1000.0)

    def test_prefers_total_equity_over_bvps_derivation(self):
        # total_equity (from Screener's #balance-sheet Equity Capital +
        # Reserves) should win over the book_value_per_share * shares
        # back-derivation entirely, even when both are present.
        row = {
            "ebitda": 100.0, "depreciation": 20.0, "total_debt": 300.0,
            "cash_and_equivalents": 50.0, "pat": 60.0, "eps": 3.0,
            "book_value_per_share": 40.0, "shares_outstanding": 25.0,
            "total_equity": 1500.0,
            "revenue": 1000.0, "current_assets": 400.0, "fcf": 70.0, "capex": 30.0,
        }
        result = derive_all_ratios(row)
        assert result["roe"] == pytest.approx(60.0 / 1500.0)
        assert result["debt_to_equity"] == pytest.approx(300.0 / 1500.0)
        assert result["roce"] == pytest.approx(80.0 / (300.0 + 1500.0))

    def test_total_equity_alone_lights_up_equity_ratios(self):
        # No book_value_per_share/shares_outstanding/eps at all — total_equity
        # alone should be enough for roe/roce/debt_to_equity to populate.
        row = {
            "ebitda": 100.0, "depreciation": 20.0, "total_debt": 300.0,
            "cash_and_equivalents": None, "pat": 60.0, "eps": None,
            "book_value_per_share": None, "shares_outstanding": None,
            "total_equity": 1000.0,
            "revenue": 1000.0, "current_assets": None, "fcf": None, "capex": None,
        }
        result = derive_all_ratios(row)
        assert result["roe"] == pytest.approx(0.06)
        assert result["debt_to_equity"] == pytest.approx(0.3)
