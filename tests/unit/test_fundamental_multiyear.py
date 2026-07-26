"""
tests/unit/test_fundamental_multiyear.py

Pure-logic tests for the multi-year rolling stats and 1-year deltas added
to features/fundamental.py's compute_fundamental_features() (QGLP, Moat,
Longevity, Turnaround, Earnings Re-rating, Contrarian Recovery, Capital
Allocation Quality, Sector-Leader Compounders, Small-Cap Compounders,
SMILE). Uses a mocked DataStoreClient with 6 years of synthetic quarterly
history, no DB/network.
"""

from datetime import datetime
from unittest.mock import MagicMock

import numpy as np
import pytest

from features.fundamental import compute_fundamental_features

_MONTHS = [3, 6, 9, 12]


def _quarter(i: int, fy0: int = 2020, **overrides):
    fy = fy0 + i // 4
    qn = i % 4 + 1
    month = _MONTHS[qn - 1]
    ann = f"{fy}-{month:02d}-28"
    row = {
        "ticker": "X", "fiscal_year": fy, "quarter": qn,
        "quarter_end_date": ann, "announcement_date": ann,
        "revenue": 100.0 + i, "ebit": 20.0 + i, "pat": 10.0 + i, "fcf": 8.0 + i, "capex": 2.0,
        "total_debt": 50.0, "cash_and_equivalents": 10.0,
        "current_assets": 60.0, "current_liabilities": 30.0,
        "property_plant_equipment": 40.0, "cwip": 5.0,
        "book_value_per_share": 20.0, "shares_outstanding": 1000,
        "roce": 0.15 + i * 0.001, "ebitda_margin": 0.20 + i * 0.001,
        "eps": 5.0 + i * 0.01, "total_assets": 200.0, "borrowings_noncurrent": 30.0,
        "retained_earnings": 80.0, "ebitda": 30.0 + i,
        "receivable_days": 40.0, "inventory_days": 20.0, "gross_profit": 60.0,
    }
    row.update(overrides)
    return row


def _client(n_quarters=24, **kw):
    client = MagicMock()
    client.get_fundamentals_history.return_value = [_quarter(i, **kw) for i in range(n_quarters)]
    client.get_ohlcv.return_value = [{"date": "2026-01-01", "close": 100.0}]
    return client


class TestMultiYearRollingStats:
    def test_avg_roce_5y_is_mean_over_5yr_window(self):
        feats = compute_fundamental_features(_client(), "X", datetime(2026, 1, 10))
        # Window is inclusive of the 20-quarters-back base quarter itself
        # (i=3, FY2020 Q4) through the latest (i=23) -> 21 quarters, i in [3, 23].
        expected = np.mean([0.15 + i * 0.001 for i in range(3, 24)])
        assert feats["avg_roce_5y"] == pytest.approx(expected)

    def test_margin_stability_5y_is_negative_stdev(self):
        feats = compute_fundamental_features(_client(), "X", datetime(2026, 1, 10))
        assert feats["margin_stability_5y"] < 0  # any variation in ebitda_margin -> negative stability score

    def test_sales_cagr_5y_matches_5yr_cagr_formula(self):
        feats = compute_fundamental_features(_client(), "X", datetime(2026, 1, 10))
        # revenue_t (i=23) = 123.0, revenue 20 quarters back (i=3) = 103.0
        expected = (123.0 / 103.0) ** (1.0 / 5.0) - 1.0
        assert feats["sales_cagr_5y"] == pytest.approx(expected)

    def test_delta_roce_3y_is_roce_now_minus_roce_3y_ago(self):
        feats = compute_fundamental_features(_client(), "X", datetime(2026, 1, 10))
        # roce_t (i=23) - roce 12 quarters back (i=11)
        expected = (0.15 + 23 * 0.001) - (0.15 + 11 * 0.001)
        assert feats["delta_roce_3y"] == pytest.approx(expected)

    def test_insufficient_history_yields_nan_not_crash(self):
        feats = compute_fundamental_features(_client(n_quarters=3), "X", datetime(2026, 1, 10))
        assert not np.isnan(feats["avg_roce_5y"])  # mean over whatever's present, not NaN
        assert np.isnan(feats["sales_cagr_5y"])  # no quarter 20-back exists yet
        assert np.isnan(feats["delta_roce_3y"])  # no quarter 12-back exists yet


class Test1YearDeltas:
    def test_eps_acceleration_is_second_derivative(self):
        feats = compute_fundamental_features(_client(), "X", datetime(2026, 1, 10))

        def eps(i):
            return 5.0 + i * 0.01

        growth_now = (eps(23) - eps(19)) / abs(eps(19))
        growth_prior = (eps(19) - eps(15)) / abs(eps(15))
        assert feats["eps_acceleration"] == pytest.approx(growth_now - growth_prior)

    def test_margin_expansion_is_yoy_ebitda_margin_delta(self):
        feats = compute_fundamental_features(_client(), "X", datetime(2026, 1, 10))

        def margin(i):
            return 0.20 + i * 0.001

        assert feats["margin_expansion"] == pytest.approx(margin(23) - margin(19))

    def test_delta_operating_cash_flow_1y_is_yoy_growth(self):
        feats = compute_fundamental_features(_client(), "X", datetime(2026, 1, 10))

        def cfo(i):
            return (8.0 + i) + 2.0  # fcf + capex

        assert feats["delta_operating_cash_flow_1y"] == pytest.approx((cfo(23) - cfo(19)) / abs(cfo(19)))

    def test_flat_history_yields_zero_deltas(self):
        # Identical financials every quarter -> every delta should be exactly 0, not NaN.
        client = _client()
        client.get_fundamentals_history.return_value = [
            _quarter(i, roce=0.15, ebitda_margin=0.20, eps=5.0, pat=10.0, revenue=100.0, ebit=20.0, fcf=8.0)
            for i in range(24)
        ]
        feats = compute_fundamental_features(client, "X", datetime(2026, 1, 10))
        assert feats["delta_roa_1y"] == pytest.approx(0.0)
        assert feats["delta_current_ratio_1y"] == pytest.approx(0.0)
        assert feats["margin_expansion"] == pytest.approx(0.0)


class TestSizeAgeAndCapitalAllocation:
    def test_company_age_years_uses_listing_date(self):
        client = _client()
        as_of = datetime(2026, 1, 10)
        feats = compute_fundamental_features(client, "X", as_of, listing_date=datetime(2016, 1, 10))
        assert feats["company_age_years"] == pytest.approx(10.0, abs=0.01)

    def test_company_age_years_nan_without_listing_date(self):
        feats = compute_fundamental_features(_client(), "X", datetime(2026, 1, 10))
        assert np.isnan(feats["company_age_years"])

    def test_dilution_3y_flat_shares_is_zero(self):
        feats = compute_fundamental_features(_client(), "X", datetime(2026, 1, 10))
        assert feats["dilution_3y"] == pytest.approx(0.0)

    def test_reinvestment_rate_is_capex_over_cfo_proxy(self):
        feats = compute_fundamental_features(_client(), "X", datetime(2026, 1, 10))
        cfo_proxy = (8.0 + 23) + 2.0  # fcf(i=23) + capex
        assert feats["reinvestment_rate"] == pytest.approx(2.0 / cfo_proxy)

    def test_capital_allocation_efficiency_uses_3yr_retained_earnings(self):
        feats = compute_fundamental_features(_client(), "X", datetime(2026, 1, 10))
        delta_ebit = (20.0 + 23) - (20.0 + 19)
        assert feats["capital_allocation_efficiency"] == pytest.approx(delta_ebit / 80.0)

    def test_market_cap_present(self):
        feats = compute_fundamental_features(_client(), "X", datetime(2026, 1, 10))
        assert feats["market_cap"] == pytest.approx((100.0 * 1000) / 1e7)
