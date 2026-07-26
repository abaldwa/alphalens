"""
tests/unit/test_fundamental_features.py

Phase: 2.1 (Fundamental Data Ingestion + PIT Validation)
Specs: SPEC-FEAT-002, SPEC-PIPE-003 (CRITICAL), SPEC-PIPE-004
Owner: Platform / QA
Consumers: CI, pytest

Tests features/fundamental.py's growth/profitability/leverage math and
SPEC-FEAT-002's sector-relative z-score normalization, using a fake
DataStoreClient (SPEC-SOLID-005 — no real HTTP call).
"""

from datetime import datetime
from unittest.mock import MagicMock

import numpy as np
import pytest

from features.fundamental import (
    FUNDAMENTAL_FEATURES,
    RATIO_FEATURES,
    STALENESS_FEATURES,
    compute_fundamental_features,
    compute_fundamental_features_panel,
    compute_staleness,
)


_ALL_FUNDAMENTALS_FIELDS = [
    "ebitda", "pat", "eps", "operating_margin", "ebitda_margin", "net_margin", "roe", "roce",
    "debt_to_equity", "interest_coverage", "fcf", "asset_turnover", "inventory_days",
    "receivable_days", "payable_days", "book_value_per_share", "shares_outstanding",
    "gross_profit", "capex", "current_assets", "current_liabilities", "total_debt", "cash_and_equivalents",
]


def _quarter(fy, q, qed, ann, revenue, **kwargs):
    """
    Builds a row matching the real shape of every dict
    DataStoreClient.get_fundamentals_history() returns in production —
    every FundamentalsWrite field always present (None default), never a
    column missing entirely. A FundamentalsWrite-mirroring dict, not just
    the handful of fields a given test cares about.
    """
    row = {
        "ticker": "TEST", "fiscal_year": fy, "quarter": q,
        "quarter_end_date": qed, "announcement_date": ann, "revenue": revenue,
    }
    row.update({field: None for field in _ALL_FUNDAMENTALS_FIELDS})
    row.update(kwargs)
    return row


class TestFundamentalFeatureCount:
    def test_fifty_three_features_total(self):
        # 30 original P2.1 features + 5 value/quality features (Piotroski-
        # on-Value/Magic Formula/Quality-Value/FCF+LowDebt/GARP) + 18
        # multi-year/delta/size/capital-allocation features (QGLP, Moat,
        # Longevity, Turnaround, Earnings Re-rating, Contrarian Recovery,
        # Capital Allocation Quality, Sector-Leader Compounders,
        # Small-Cap Compounders, SMILE, etc.).
        assert len(FUNDAMENTAL_FEATURES) == 54
        assert len(RATIO_FEATURES) == 51
        assert len(STALENESS_FEATURES) == 3
        assert set(RATIO_FEATURES) | set(STALENESS_FEATURES) == set(FUNDAMENTAL_FEATURES)


class TestComputeFundamentalFeatures:
    def test_revenue_growth_yoy_and_qoq(self):
        client = MagicMock()
        client.get_fundamentals_history.return_value = [
            _quarter(2024, 2, "2024-06-30", "2024-08-10", revenue=100.0),
            _quarter(2024, 3, "2024-09-30", "2024-11-12", revenue=105.0),
            _quarter(2024, 4, "2024-12-31", "2025-02-14", revenue=110.0),
            _quarter(2025, 1, "2025-03-31", "2025-05-15", revenue=120.0),
            _quarter(2025, 2, "2025-06-30", "2025-08-14", revenue=130.0),  # latest
        ]
        client.get_ohlcv.return_value = []

        feats = compute_fundamental_features(client, "TEST", as_of=datetime(2025, 9, 1))

        # QoQ: latest (130, Q2 FY25) vs immediately preceding quarter (120, Q1 FY25)
        assert feats["revenue_growth_qoq"] == pytest.approx((130 - 120) / 120)
        # YoY: latest (130, Q2 FY25) vs same quarter prior year (100, Q2 FY24)
        assert feats["revenue_growth_yoy"] == pytest.approx((130 - 100) / 100)

    def test_no_history_returns_all_nan_and_pending(self):
        client = MagicMock()
        client.get_fundamentals_history.return_value = []

        feats = compute_fundamental_features(client, "NEWCO", as_of=datetime(2025, 9, 1))

        assert feats["results_pending_flag"] == 1
        assert np.isnan(feats["revenue_growth_yoy"])
        assert set(feats.keys()) == set(FUNDAMENTAL_FEATURES)

    def test_staleness_uses_latest_quarter_announcement_date(self):
        client = MagicMock()
        client.get_fundamentals_history.return_value = [
            _quarter(2025, 1, "2025-03-31", "2025-05-15", revenue=100.0),
        ]
        client.get_ohlcv.return_value = []

        feats = compute_fundamental_features(client, "TEST", as_of=datetime(2025, 6, 19))

        assert feats["days_since_results"] == 35.0

    def test_pe_pb_use_pit_safe_close_not_unconstrained_latest(self):
        """SPEC-PIPE-003: valuation ratios must use the close <= as_of, never an unconstrained latest price."""
        client = MagicMock()
        client.get_fundamentals_history.return_value = [
            _quarter(
                2025, 1, "2025-03-31", "2025-05-15", revenue=1000.0,
                eps=10.0, book_value_per_share=50.0, shares_outstanding=1000,
            ),
        ]
        client.get_ohlcv.return_value = [
            {"date": "2025-06-01", "close": 100.0},
            {"date": "2025-06-10", "close": 120.0},  # would be wrong if "latest unconstrained" were used
        ]

        feats = compute_fundamental_features(client, "TEST", as_of=datetime(2025, 6, 10))

        assert feats["pe_ratio"] == pytest.approx(120.0 / 10.0)
        assert feats["pb_ratio"] == pytest.approx(120.0 / 50.0)
        # Confirms the call was bounded by as_of, not fetching all history
        _, kwargs = client.get_ohlcv.call_args
        assert kwargs["to_date"] == datetime(2025, 6, 10)


class TestValueQualityFeatures:
    """New features added for Piotroski-on-Value/Magic Formula/Quality-Value/
    FCF+LowDebt/GARP: ev_ebit_yield, fcf_ev_yield, magic_formula_roc,
    book_to_market, cfo_to_pat — all pure arithmetic over already-stored columns."""

    def _client(self):
        client = MagicMock()
        client.get_fundamentals_history.return_value = [
            _quarter(
                2025, 1, "2025-03-31", "2025-05-15", revenue=1000.0,
                ebit=200.0, pat=100.0, fcf=80.0, capex=20.0,
                total_debt=300.0, cash_and_equivalents=100.0,
                current_assets=500.0, current_liabilities=200.0,
                property_plant_equipment=150.0, cwip=50.0,
                book_value_per_share=50.0, shares_outstanding=1000,
            ),
        ]
        client.get_ohlcv.return_value = [{"date": "2025-06-01", "close": 100.0}]
        return client

    def test_ev_ebit_yield_and_fcf_ev_yield(self):
        # market_cap = 100 * 1000 / 1e7 = 0.01 (crore); EV = 0.01 + 300 - 100 = 200.01
        feats = compute_fundamental_features(self._client(), "TEST", as_of=datetime(2025, 6, 10))
        ev = 0.01 + 300.0 - 100.0
        assert feats["ev_ebit_yield"] == pytest.approx(200.0 / ev)
        assert feats["fcf_ev_yield"] == pytest.approx(80.0 / ev)

    def test_magic_formula_roc(self):
        # NWC = 500 - 200 = 300; net_fixed_assets = 150 + 50 = 200
        feats = compute_fundamental_features(self._client(), "TEST", as_of=datetime(2025, 6, 10))
        assert feats["magic_formula_roc"] == pytest.approx(200.0 / (300.0 + 200.0))

    def test_book_to_market(self):
        # book_value_equity = 50 * 1000 / 1e7 = 0.005 (crore); market_cap = 0.01
        feats = compute_fundamental_features(self._client(), "TEST", as_of=datetime(2025, 6, 10))
        assert feats["book_to_market"] == pytest.approx(0.005 / 0.01)

    def test_cfo_to_pat(self):
        # cfo_proxy = fcf + capex = 100.0
        feats = compute_fundamental_features(self._client(), "TEST", as_of=datetime(2025, 6, 10))
        assert feats["cfo_to_pat"] == pytest.approx(100.0 / 100.0)

    def test_missing_inputs_yield_nan_not_crash(self):
        client = MagicMock()
        client.get_fundamentals_history.return_value = [
            _quarter(2025, 1, "2025-03-31", "2025-05-15", revenue=1000.0),
        ]
        client.get_ohlcv.return_value = []
        feats = compute_fundamental_features(client, "TEST", as_of=datetime(2025, 6, 10))
        assert np.isnan(feats["ev_ebit_yield"])
        assert np.isnan(feats["magic_formula_roc"])
        assert np.isnan(feats["book_to_market"])
        assert np.isnan(feats["cfo_to_pat"])


class TestComputeStaleness:
    def test_formula_matches_spec(self):
        result = compute_staleness(datetime(2025, 1, 1), datetime(2025, 1, 31))
        assert result["days_since_results"] == 30.0
        assert result["quarter_age_pct"] == pytest.approx(30.0 / 63.0)
        assert result["results_pending_flag"] == 0


class TestSectorRelativeZScore:
    def test_ratio_features_are_zscored_within_sector(self):
        client = MagicMock()

        def fake_history(ticker, as_of, lookback_years=4):
            base_revenue = {"A": 100.0, "B": 200.0, "C": 50.0}[ticker]
            return [_quarter(2025, 1, "2025-03-31", "2025-05-15", revenue=base_revenue, roe=10.0)]

        client.get_fundamentals_history.side_effect = fake_history
        client.get_ohlcv.return_value = []

        panel = compute_fundamental_features_panel(
            client, ["A", "B", "C"], as_of=datetime(2025, 6, 1),
            sector_map={"A": "IT", "B": "IT", "C": "PHARMA"},
        )

        # IT sector (A, B) has 2 members — roe identical (10.0) -> std=0 -> z=0 for both
        it_rows = panel[panel["ticker"].isin(["A", "B"])]
        assert it_rows["roe"].abs().max() < 1e-6
        # PHARMA has only 1 member — sector std is undefined (NaN) for a
        # single-element group, so z is honestly NaN (no relative-to-sector
        # information exists), not fabricated as 0 — same "let NaN flow"
        # philosophy used everywhere else in this project (e.g. F&O
        # features for non-eligible stocks, SPEC-FEAT-004).
        pharma_row = panel[panel["ticker"] == "C"].iloc[0]
        assert np.isnan(pharma_row["roe"])

    def test_staleness_features_are_never_zscored(self):
        client = MagicMock()
        client.get_fundamentals_history.return_value = [
            _quarter(2025, 1, "2025-03-31", "2025-05-15", revenue=100.0),
        ]
        client.get_ohlcv.return_value = []

        panel = compute_fundamental_features_panel(
            client, ["A"], as_of=datetime(2025, 6, 19), sector_map={"A": "IT"},
        )
        # days_since_results should be the raw value (35), not a z-score near 0
        assert panel.iloc[0]["days_since_results"] == 35.0
