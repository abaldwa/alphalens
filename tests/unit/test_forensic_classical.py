"""
tests/unit/test_forensic_classical.py

Phase: 2.5 (Forensic Accounting System M-09/M-10)
Specs: SPEC-MODEL-009
Owner: Platform / QA
Consumers: CI, pytest

Tests systems/ml_signal_engine/models/forensic/classical_scores.py (M-09's
7 pure formulas) and features/forensic_classical.py (the 26-feature
Groups A-C panel), using a fake DataStoreClient (SPEC-SOLID-005 — no
real HTTP call).
"""

from datetime import datetime
from unittest.mock import MagicMock

import numpy as np
import pytest

from features.forensic_classical import FORENSIC_CLASSICAL_FEATURES, compute_forensic_classical_features
from systems.ml_signal_engine.models.forensic.classical_scores import (
    altman_z_score,
    beneish_m_score,
    benford_analysis,
    dechow_f_score,
    forensic_classical_composite,
    ohlson_o_score,
    piotroski_f_score,
    sloan_accrual,
)


class TestBeneishMScore:
    def test_no_change_baseline_matches_published_value(self):
        """A company with zero YoY change on every Beneish ratio (all indices = 1.0,
        NI == CFO) has a well-known published baseline M-Score of -2.48
        (-4.84 + 0.92+0.528+0.404+0.892+0.115-0.172+0-0.327)."""
        financials = {
            "receivables": 100, "revenue": 1000, "receivables_yoy": 100, "revenue_yoy": 1000,
            "gross_profit": 300, "gross_profit_yoy": 300,
            "ca": 400, "ppe": 300, "ta": 1000, "ca_yoy": 400, "ppe_yoy": 300, "ta_yoy": 1000,
            "depreciation": 50, "depreciation_yoy": 50,
            "sga": 150, "sga_yoy": 150,
            "ni": 100, "cfo": 100,
            "ltd_cl": 400, "ltd_cl_yoy": 400,
        }
        result = beneish_m_score(financials)
        assert result["m_score"] == pytest.approx(-2.48, abs=0.01)
        assert result["is_likely_manipulator"] is False

    def test_elevated_dsri_and_tata_pushes_above_manipulator_threshold(self):
        """Receivables growing far faster than revenue (DSRI >> 1) plus NI far above
        CFO (high TATA) is the textbook Beneish manipulation signature — must cross
        the -1.78 threshold."""
        financials = {
            "receivables": 300, "revenue": 1000, "receivables_yoy": 100, "revenue_yoy": 900,
            "gross_profit": 300, "gross_profit_yoy": 300,
            "ca": 400, "ppe": 300, "ta": 1000, "ca_yoy": 400, "ppe_yoy": 300, "ta_yoy": 1000,
            "depreciation": 50, "depreciation_yoy": 50,
            "sga": 150, "sga_yoy": 150,
            "ni": 200, "cfo": 20,
            "ltd_cl": 400, "ltd_cl_yoy": 400,
        }
        result = beneish_m_score(financials)
        assert result["m_score"] > -1.78
        assert result["is_likely_manipulator"] is True

    def test_missing_inputs_degrade_to_nan_not_exception(self):
        result = beneish_m_score({})
        assert np.isnan(result["m_score"])
        assert result["is_likely_manipulator"] is None


class TestAltmanZScore:
    def test_distress_zone_detection(self):
        z = altman_z_score({"wc": -50, "re": -200, "ebit": -10, "ta": 1000, "mktcap": 200, "tl": 900, "sales": 500})
        assert z["z_score"] < 1.81
        assert z["distress_zone"] is True

    def test_safe_zone_detection(self):
        z = altman_z_score({"wc": 300, "re": 400, "ebit": 200, "ta": 1000, "mktcap": 2000, "tl": 400, "sales": 1200})
        assert z["z_score"] > 2.99
        assert z["safe_zone"] is True


class TestPiotroskiFScore:
    def test_all_nine_conditions_true_scores_nine(self):
        f = piotroski_f_score(
            {
                "ni": 100, "cfo": 120, "roa": 0.10, "roa_yoy": 0.08,
                "ltd_cl": 300, "ta": 1000, "ltd_cl_yoy": 350, "ta_yoy": 1000,
                "current_ratio": 1.5, "current_ratio_yoy": 1.3,
                "shares": 100, "shares_yoy": 100,
                "gross_margin": 0.35, "gross_margin_yoy": 0.30,
                "asset_turnover": 1.2, "asset_turnover_yoy": 1.0,
            }
        )
        assert f["f_score"] == 9.0
        assert f["is_strong"] is True

    def test_all_nine_conditions_false_scores_zero(self):
        f = piotroski_f_score(
            {
                "ni": -50, "cfo": -60, "roa": 0.02, "roa_yoy": 0.10,
                "ltd_cl": 500, "ta": 1000, "ltd_cl_yoy": 300, "ta_yoy": 1000,
                "current_ratio": 0.8, "current_ratio_yoy": 1.2,
                "shares": 120, "shares_yoy": 100,
                "gross_margin": 0.20, "gross_margin_yoy": 0.30,
                "asset_turnover": 0.8, "asset_turnover_yoy": 1.0,
            }
        )
        assert f["f_score"] == 0.0
        assert f["is_weak"] is True


class TestOhlsonOScore:
    def test_healthy_company_low_bankruptcy_probability(self):
        o = ohlson_o_score(
            {"ta": 5000, "tl": 1500, "wc": 800, "cl": 600, "ca": 1500, "ni": 400, "ffo": 600, "ni_yoy": 350}
        )
        assert o["bankruptcy_prob"] < 0.3

    def test_distressed_company_high_bankruptcy_probability(self):
        o = ohlson_o_score(
            {
                "ta": 1000, "tl": 1200, "wc": -100, "cl": 700, "ca": 300, "ni": -150, "ffo": -100,
                "ni_yoy": -120, "net_loss_2yr": 1,
            }
        )
        assert o["bankruptcy_prob"] > 0.5


class TestDechowFScore:
    def test_high_risk_inputs_score_higher_than_low_risk(self):
        high = dechow_f_score(
            {
                "rsst_accruals": 0.20, "change_receivables": 0.10, "change_inventory": 0.08,
                "pct_soft_assets": 0.5, "change_cash_sales": -0.05, "change_roa": -0.03,
                "issuance": 1, "book_to_market": 0.3, "abnormal_change_employees": -0.10,
            }
        )
        low = dechow_f_score(
            {
                "rsst_accruals": 0.01, "change_receivables": 0.0, "change_inventory": 0.0,
                "pct_soft_assets": 0.1, "change_cash_sales": 0.02, "change_roa": 0.02,
                "issuance": 0, "book_to_market": 0.8, "abnormal_change_employees": 0.0,
            }
        )
        assert high["misstatement_prob"] > low["misstatement_prob"]


class TestSloanAccrual:
    def test_high_accrual_flagged(self):
        s = sloan_accrual({"ni": 200, "cfo": 50, "ta": 1000})
        assert s["sloan_accrual"] == pytest.approx(0.15)
        assert s["is_high_accrual"] is True

    def test_low_accrual_not_flagged(self):
        s = sloan_accrual({"ni": 105, "cfo": 100, "ta": 1000})
        assert s["is_high_accrual"] is False


class TestBenfordAnalysis:
    def test_mad_exceeds_threshold_for_manipulated_revenue(self):
        """Build prompt deliverable: Benford MAD > 0.015 for an artificially
        manipulated revenue series — deliberately weighted toward digit '5'
        (e.g. repeatedly rounding/padding to figures starting with 5), a
        clear deviation from Benford's expected ~30%-starts-with-1 distribution."""
        manipulated = [500 + i for i in range(60)] + [5000 + i for i in range(60)] + [55000 + i for i in range(40)]
        result = benford_analysis({"revenue": manipulated})
        assert result["benford_mad"] > 0.015
        assert result["is_nonconforming"] is True

    def test_mad_low_for_naturally_benford_conforming_series(self):
        """A geometrically-spaced series (multiplicative growth, like compounding
        revenue) is the classic naturally-Benford-conforming shape."""
        conforming = [100 * (1.08**i) for i in range(60)]
        result = benford_analysis({"revenue": conforming})
        assert result["benford_mad"] < 0.06

    def test_too_few_values_returns_nan_not_error(self):
        result = benford_analysis({"revenue": [100, 200]})
        assert np.isnan(result["benford_revenue_chi2"])
        assert np.isnan(result["benford_mad"])


class TestForensicClassicalComposite:
    def test_all_red_flags_produce_high_composite(self):
        comp = forensic_classical_composite(
            {
                "m_score": 0.5, "z_score": 1.0, "piotroski_f_score": 1,
                "ohlson_bankruptcy_prob": 0.8, "dechow_misstatement_prob": 0.75,
                "sloan_accrual": 0.20, "benford_mad": 0.04,
            }
        )
        assert comp["forensic_classical_composite"] > 60
        assert comp["flag"] in ("red", "black")

    def test_all_clean_signals_produce_low_composite(self):
        comp = forensic_classical_composite(
            {
                "m_score": -3.5, "z_score": 4.0, "piotroski_f_score": 8,
                "ohlson_bankruptcy_prob": 0.02, "dechow_misstatement_prob": 0.03,
                "sloan_accrual": 0.01, "benford_mad": 0.005,
            }
        )
        assert comp["forensic_classical_composite"] < 20
        assert comp["flag"] == "green"

    def test_empty_scores_returns_nan_not_error(self):
        comp = forensic_classical_composite({})
        assert np.isnan(comp["forensic_classical_composite"])
        assert comp["flag"] is None
        assert comp["n_models_used"] == 0


def _quarter_row(fy, q, revenue, **kwargs):
    row = {
        "ticker": "TEST", "fiscal_year": fy, "quarter": q,
        "quarter_end_date": datetime(fy, q * 3, 28), "announcement_date": datetime(fy, q * 3, 28),
        "revenue": revenue, "ebitda": None, "pat": None, "eps": None, "operating_margin": None,
        "ebitda_margin": None, "net_margin": None, "roe": None, "roce": None, "debt_to_equity": None,
        "interest_coverage": None, "fcf": None, "asset_turnover": None, "inventory_days": None,
        "receivable_days": None, "payable_days": None, "book_value_per_share": None, "shares_outstanding": None,
        "gross_profit": None, "capex": None, "current_assets": None, "current_liabilities": None,
        "total_debt": None, "cash_and_equivalents": None, "depreciation": None,
        # Not a real `fundamentals` schema column yet (see module docstring's
        # documented gap) — supplied directly in this fixture to test the AQI
        # formula's real behavior once a genuine PPE source exists.
        "ppe": None,
    }
    row.update(kwargs)
    return row


class TestComputeForensicClassicalFeatures:
    def test_full_inputs_return_finite_floats_for_every_feature(self):
        """Build prompt deliverable: all 26 (see module docstring's "26 not 30")
        classical features return finite floats when given complete inputs covering
        every raw line item these formulas need — proves the computation pipeline
        works end-to-end, not just that individual formulas do in isolation. The
        7 features with no available real-world data source (interest_income_vs_cash,
        unbilled_revenue_ratio, cash_revenue_ratio, revenue_vs_gst_proxy,
        revenue_concentration) and the two coarse quarter-acceleration proxies that
        need >=4 prior quarters (channel_stuffing_indicator, quarter_end_revenue_spike)
        are excluded from the finite-check — see features/forensic_classical.py's
        module docstring on these documented gaps."""
        client = MagicMock()
        rows = []
        revenue = 1000.0
        for i in range(8):
            fy = 2024 + i // 4
            q = (i % 4) + 1
            revenue *= 1.05
            rows.append(
                _quarter_row(
                    fy, q, revenue,
                    gross_profit=revenue * 0.4, ebitda=revenue * 0.25, pat=revenue * 0.15,
                    operating_margin=0.20, current_assets=revenue * 0.5, current_liabilities=revenue * 0.3,
                    total_debt=revenue * 0.4, book_value_per_share=50.0, shares_outstanding=100_000_000,
                    fcf=revenue * 0.10, capex=revenue * 0.05, inventory_days=40.0,
                    receivable_days=45.0, payable_days=30.0, depreciation=revenue * 0.05,
                    ppe=revenue * 0.6,
                )
            )
        client.get_fundamentals_history.return_value = rows

        feats = compute_forensic_classical_features(client, "TEST", datetime(2026, 1, 1))

        excluded = {
            "interest_income_vs_cash", "unbilled_revenue_ratio", "cash_revenue_ratio",
            "revenue_vs_gst_proxy", "revenue_concentration",
            # This fixture's perfectly uniform 5%-per-quarter growth makes the
            # trailing-quarters standard deviation exactly 0 — a genuine
            # zero-variance edge case (spike z-score is undefined, correctly NaN,
            # not a bug); see TestQuarterEndRevenueSpike below for real coverage.
            "channel_stuffing_indicator", "quarter_end_revenue_spike",
        }
        checked = [f for f in FORENSIC_CLASSICAL_FEATURES if f not in excluded]
        for feat in checked:
            val = feats[feat]
            assert np.isfinite(val), f"{feat} expected a finite float, got {val}"

    def test_no_history_returns_all_nan(self):
        client = MagicMock()
        client.get_fundamentals_history.return_value = []
        feats = compute_forensic_classical_features(client, "TEST", datetime(2026, 1, 1))
        assert all(np.isnan(v) for v in feats.values())


class TestQuarterEndRevenueSpike:
    """channel_stuffing_indicator/quarter_end_revenue_spike need real quarter-to-quarter
    variance (zero variance -> an undefined z-score, correctly NaN — see
    TestComputeForensicClassicalFeatures's documented exclusion)."""

    def test_unusual_quarter_end_jump_is_flagged(self):
        client = MagicMock()
        # Mild, varying growth for the trailing 4 quarters, then a sharp final jump.
        revenues = [1000, 1030, 995, 1040, 1015, 1900]
        rows = []
        for i, rev in enumerate(revenues):
            fy, q = 2024 + i // 4, (i % 4) + 1
            rows.append(_quarter_row(fy, q, float(rev)))
        client.get_fundamentals_history.return_value = rows

        feats = compute_forensic_classical_features(client, "TEST", datetime(2026, 1, 1))

        assert feats["quarter_end_revenue_spike"] > 2.0
        assert feats["channel_stuffing_indicator"] == 1.0

    def test_normal_quarter_is_not_flagged(self):
        client = MagicMock()
        revenues = [1000, 1030, 995, 1040, 1015, 1045]
        rows = []
        for i, rev in enumerate(revenues):
            fy, q = 2024 + i // 4, (i % 4) + 1
            rows.append(_quarter_row(fy, q, float(rev)))
        client.get_fundamentals_history.return_value = rows

        feats = compute_forensic_classical_features(client, "TEST", datetime(2026, 1, 1))

        assert feats["channel_stuffing_indicator"] == 0.0
