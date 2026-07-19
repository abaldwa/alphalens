"""
tests/unit/test_damodaran.py

Phase: 3
Specs: SPEC-VAL-001, SPEC-VAL-002, SPEC-VAL-003, SPEC-VAL-005
Owner: Platform / QA

Unit tests for Damodaran valuation backend.

Test policy (SPEC-QUALITY-003 / no-stub policy):
  - Production paths NEVER receive synthetic data.
  - All fixtures below are clearly labeled TEST FIXTURE and only used in
    test functions, never injected into production code paths.
  - ValuationEngine.value_stock is tested for its error-handling contract
    (RuntimeError on insufficient data) without touching the production DB.
"""

from __future__ import annotations

import math
from typing import Any, Dict
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from systems.damodaran_valuation.dcf.models import (
    CommodityNormalizedModel,
    ExcessReturnModel,
    FCFFInputs,
    FCFFThreeStageModel,
    FCFFTwoStageModel,
)
from systems.damodaran_valuation.dcf.wacc import WACCCalculator, WACCInputs
from systems.damodaran_valuation.lifecycle.classifier import LifecycleClassifier, LifecycleStage
from systems.damodaran_valuation.relative.pe_regression import RelativePERegression
from systems.damodaran_valuation.scenarios.monte_carlo import MonteCarloDCF


# ---------------------------------------------------------------------------
# Shared TEST FIXTURES
# ---------------------------------------------------------------------------

def _make_fcff_inputs(**overrides) -> FCFFInputs:
    """TEST FIXTURE — representative mid-cap FMCG company."""
    base: Dict[str, Any] = dict(
        ebit=500.0,         # INR crore
        tax_rate=0.25,
        depreciation=80.0,
        capex=120.0,
        change_in_nwc=30.0,
        wacc=0.12,
        high_growth_rate=0.15,
        revenue=3_000.0,
        terminal_growth_rate=0.05,
        high_growth_years=5,
        shares_outstanding=100.0,  # crore shares
        total_debt=400.0,
        cash=150.0,
    )
    base.update(overrides)
    return FCFFInputs(**base)


# ---------------------------------------------------------------------------
# LifecycleClassifier tests
# ---------------------------------------------------------------------------

class TestLifecycleClassifier:
    """SPEC-VAL-001: lifecycle stage classification rules."""

    def setup_method(self) -> None:
        self.clf = LifecycleClassifier()

    def test_financial_services_banking(self) -> None:
        """Banking sector must always classify as FINANCIAL_SERVICES.

        2026-07-10: updated to use the real NSE sector string
        ("Financial Services" — see config/nifty500_universe.csv and
        classifier.py's _FINANCIAL_SERVICES_SECTORS comment, fixed
        2026-07-04). NSE's official taxonomy has no separate "Banking"/
        "NBFC"/"Insurance" sector string — real banks/NBFCs/insurers are
        all tagged "Financial Services" — so the old per-subsector test
        values never matched any real data even before this fix."""
        result = self.clf.classify({"sector": "Financial Services"})
        assert result == LifecycleStage.FINANCIAL_SERVICES

    def test_financial_services_nbfc(self) -> None:
        """See test_financial_services_banking — NBFCs share the same
        real "Financial Services" sector string, no separate NBFC tag."""
        result = self.clf.classify({"sector": "Financial Services"})
        assert result == LifecycleStage.FINANCIAL_SERVICES

    def test_financial_services_insurance(self) -> None:
        """See test_financial_services_banking — insurers share the same
        real "Financial Services" sector string, no separate Insurance tag."""
        result = self.clf.classify({"sector": "Financial Services"})
        assert result == LifecycleStage.FINANCIAL_SERVICES

    def test_distressed_negative_net_margin(self) -> None:
        """Negative net margin → DISTRESSED (SPEC-VAL-001)."""
        result = self.clf.classify({"net_margin": -0.05, "sector": "IT Services"})
        assert result == LifecycleStage.DISTRESSED

    def test_distressed_low_interest_coverage(self) -> None:
        """interest_coverage < 1.5 → DISTRESSED (SPEC-VAL-001)."""
        result = self.clf.classify({
            "interest_coverage": 1.0,
            "net_margin": 0.02,
            "sector": "Chemicals",
        })
        assert result == LifecycleStage.DISTRESSED

    def test_distressed_boundary_coverage_above(self) -> None:
        """interest_coverage = 1.5 → NOT distressed on that criterion alone."""
        result = self.clf.classify({
            "interest_coverage": 1.5,
            "net_margin": 0.05,
            "revenue_cagr_3y": 0.10,
            "payout_ratio": 0.20,
            "sector": "Chemicals",
        })
        assert result != LifecycleStage.DISTRESSED

    def test_distressed_low_altman_z(self) -> None:
        """Altman Z'' < 1.1 → DISTRESSED (Z'' non-manufacturing threshold,
        matching the Z'' weights valuation_engine._altman_z actually computes
        — not the original manufacturing Z-score's 1.81)."""
        result = self.clf.classify({
            "altman_z": 1.0,
            "net_margin": 0.03,
            "interest_coverage": 2.5,
            "sector": "Metals",
        })
        assert result == LifecycleStage.DISTRESSED

    def test_not_distressed_altman_z_in_greyzone(self) -> None:
        """Altman Z'' = 1.5 is in the 1.1-2.6 grey zone, not distressed."""
        result = self.clf.classify({
            "altman_z": 1.5,
            "net_margin": 0.03,
            "interest_coverage": 2.5,
            "revenue_cagr_3y": 0.10,
            "payout_ratio": 0.20,
            "sector": "Metals",
        })
        assert result != LifecycleStage.DISTRESSED

    def test_young_growth(self) -> None:
        """High CAGR + low margin → YOUNG_GROWTH."""
        result = self.clf.classify({
            "revenue_cagr_3y": 0.40,
            "net_margin": 0.05,
            "sector": "IT Services",
        })
        assert result == LifecycleStage.YOUNG_GROWTH

    def test_high_growth(self) -> None:
        """Fast growth + profitable + strong ROE → HIGH_GROWTH."""
        result = self.clf.classify({
            "revenue_cagr_3y": 0.20,
            "net_margin": 0.15,
            "roe": 0.20,
            "sector": "FMCG",
        })
        assert result == LifecycleStage.HIGH_GROWTH

    def test_mature_growth(self) -> None:
        """Moderate growth + dividends → MATURE_GROWTH."""
        result = self.clf.classify({
            "revenue_cagr_3y": 0.10,
            "net_margin": 0.12,
            "roe": 0.15,
            "payout_ratio": 0.25,
            "sector": "FMCG",
        })
        assert result == LifecycleStage.MATURE_GROWTH

    def test_mature_stable(self) -> None:
        """Low growth + established payout → MATURE_STABLE."""
        result = self.clf.classify({
            "revenue_cagr_3y": 0.04,
            "net_margin": 0.10,
            "payout_ratio": 0.50,
            "sector": "FMCG",
        })
        assert result == LifecycleStage.MATURE_STABLE

    def test_declining(self) -> None:
        """Near-zero CAGR → DECLINING."""
        result = self.clf.classify({
            "revenue_cagr_3y": 0.01,
            "net_margin": 0.08,
            "sector": "Telecom",
        })
        assert result == LifecycleStage.DECLINING

    def test_declining_via_sector_median_margin(self) -> None:
        """Non-trivial CAGR but margin well below sector median -> DECLINING (rule 7b)."""
        result = self.clf.classify({
            "revenue_cagr_3y": 0.03,
            "net_margin": 0.03,
            "payout_ratio": 0.0,
            "roe": 0.0,
            "sector_median_margin": 0.10,
            "sector": "Chemicals",
        })
        assert result == LifecycleStage.DECLINING

    def test_default_fallback_mature_stable(self) -> None:
        """No rule matches -> falls through to the MATURE_STABLE default."""
        result = self.clf.classify({
            "revenue_cagr_3y": 0.06,
            "net_margin": 0.10,
            "payout_ratio": 0.05,
            "roe": 0.05,
            "sector": "Chemicals",
        })
        assert result == LifecycleStage.MATURE_STABLE


# ---------------------------------------------------------------------------
# WACCCalculator tests
# ---------------------------------------------------------------------------

class TestWACCCalculator:
    """SPEC-VAL-003: India WACC computation."""

    def setup_method(self) -> None:
        self.calc = WACCCalculator()

    def test_synthetic_rating_aaa(self) -> None:
        """ICR > 12.5 → AAA spread 0.0063."""
        assert WACCCalculator.synthetic_rating_spread(15.0) == pytest.approx(0.0063)

    def test_synthetic_rating_aa(self) -> None:
        """ICR 9.5-12.5 → AA spread 0.0080."""
        assert WACCCalculator.synthetic_rating_spread(10.0) == pytest.approx(0.0080)

    def test_synthetic_rating_a_plus(self) -> None:
        """ICR 7.5-9.5 → A+ spread 0.0098 (SPEC-VAL-003 table)."""
        assert WACCCalculator.synthetic_rating_spread(8.0) == pytest.approx(0.0098)

    def test_synthetic_rating_bbb(self) -> None:
        assert WACCCalculator.synthetic_rating_spread(4.0) == pytest.approx(0.0156)

    def test_synthetic_rating_d(self) -> None:
        """ICR <= 0.5 → D spread 0.1200."""
        assert WACCCalculator.synthetic_rating_spread(0.3) == pytest.approx(0.1200)

    def test_synthetic_rating_negative_icr(self) -> None:
        """Negative ICR (EBIT < 0) → D spread."""
        assert WACCCalculator.synthetic_rating_spread(-5.0) == pytest.approx(0.1200)

    def test_compute_wacc_structure(self) -> None:
        """WACC output should have sensible structure and values."""
        inputs = WACCInputs(
            india_10y_yield=0.068,
            beta_unlevered=0.85,
            debt_to_equity=0.30,
            cost_of_debt=0.090,
            debt_weight=0.23,
            equity_weight=0.77,
        )
        result = self.calc.compute(inputs)
        assert 0.08 < result.wacc < 0.25, f"WACC {result.wacc} out of plausible range"
        assert result.cost_of_equity > result.after_tax_cost_of_debt
        assert result.beta_adjusted > 0
        assert result.risk_free_rate > 0
        assert result.equity_risk_premium > 0

    def test_blume_adjustment(self) -> None:
        """Blume-adjusted beta should be closer to 1 than raw levered beta."""
        inputs = WACCInputs(
            india_10y_yield=0.068,
            beta_unlevered=1.5,
            debt_to_equity=0.0,
            cost_of_debt=0.08,
            debt_weight=0.0,
            equity_weight=1.0,
        )
        result = self.calc.compute(inputs)
        # Blume: 0.67 * 1.5 + 0.33 = 1.335
        assert result.beta_adjusted == pytest.approx(0.67 * result.beta_levered + 0.33, rel=1e-6)

    def test_wacc_increases_with_leverage(self) -> None:
        """More leverage (higher D/E) should increase cost of equity via Hamada."""
        base_inputs = dict(
            india_10y_yield=0.068,
            beta_unlevered=0.85,
            cost_of_debt=0.090,
            debt_weight=0.20,
            equity_weight=0.80,
        )
        low_lev = self.calc.compute(WACCInputs(debt_to_equity=0.10, **base_inputs))
        high_lev = self.calc.compute(WACCInputs(debt_to_equity=1.00, **base_inputs))
        assert high_lev.cost_of_equity > low_lev.cost_of_equity

    def test_synthetic_rating_a(self) -> None:
        """ICR 6.0-7.5 -> A spread 0.0108."""
        assert WACCCalculator.synthetic_rating_spread(7.0) == pytest.approx(0.0108)

    def test_synthetic_rating_a_minus(self) -> None:
        """ICR 4.5-6.0 -> A- spread 0.0122."""
        assert WACCCalculator.synthetic_rating_spread(5.0) == pytest.approx(0.0122)

    def test_synthetic_rating_bb_plus(self) -> None:
        """ICR 2.5-3.5 -> BB+ spread 0.0200."""
        assert WACCCalculator.synthetic_rating_spread(3.0) == pytest.approx(0.0200)

    def test_synthetic_rating_bb(self) -> None:
        """ICR 2.0-2.5 -> BB spread 0.0240."""
        assert WACCCalculator.synthetic_rating_spread(2.2) == pytest.approx(0.0240)

    def test_synthetic_rating_b_plus(self) -> None:
        """ICR 1.5-2.0 -> B+ spread 0.0340."""
        assert WACCCalculator.synthetic_rating_spread(1.7) == pytest.approx(0.0340)

    def test_synthetic_rating_b(self) -> None:
        """ICR 1.25-1.5 -> B spread 0.0450."""
        assert WACCCalculator.synthetic_rating_spread(1.3) == pytest.approx(0.0450)

    def test_synthetic_rating_ccc(self) -> None:
        """ICR 0.5-0.8 -> CCC spread 0.0725."""
        assert WACCCalculator.synthetic_rating_spread(0.6) == pytest.approx(0.0725)

    def test_from_fundamentals_known_sector(self) -> None:
        """from_fundamentals should look up sector beta and derive weights (SPEC-VAL-003)."""
        calc, inputs = WACCCalculator.from_fundamentals(
            india_10y_yield=0.068,
            sector="Information Technology",
            debt_to_equity=0.10,
            interest_coverage=20.0,
            total_debt=100.0,
            market_cap=900.0,
        )
        assert isinstance(calc, WACCCalculator)
        assert inputs.beta_unlevered == pytest.approx(0.85)
        assert inputs.debt_weight == pytest.approx(100.0 / 1000.0)
        assert inputs.equity_weight == pytest.approx(900.0 / 1000.0)
        result = calc.compute(inputs)
        assert math.isfinite(result.wacc)

    def test_from_fundamentals_unknown_sector_uses_default(self) -> None:
        """Unknown sector should fall back to the 'Default' unlevered beta (0.90)."""
        _, inputs = WACCCalculator.from_fundamentals(
            india_10y_yield=0.068,
            sector="Not A Real Sector",
            debt_to_equity=0.10,
            interest_coverage=20.0,
            total_debt=0.0,
            market_cap=1000.0,
        )
        assert inputs.beta_unlevered == pytest.approx(0.90)
        assert inputs.debt_weight == 0.0
        assert inputs.equity_weight == 1.0

    def test_from_fundamentals_zero_total_value_defaults_debt_weight_zero(self) -> None:
        """market_cap + total_debt == 0 should not raise a ZeroDivisionError."""
        _, inputs = WACCCalculator.from_fundamentals(
            india_10y_yield=0.068,
            sector="Default",
            debt_to_equity=0.0,
            interest_coverage=10.0,
            total_debt=0.0,
            market_cap=0.0,
        )
        assert inputs.debt_weight == 0.0
        assert inputs.equity_weight == 1.0


# ---------------------------------------------------------------------------
# FCFFTwoStageModel tests
# ---------------------------------------------------------------------------

class TestFCFFTwoStageModel:
    """SPEC-VAL-002 Models 1/2: two-stage FCFF DCF."""

    def setup_method(self) -> None:
        self.model = FCFFTwoStageModel()

    def test_positive_finite_ev(self) -> None:
        """Standard profitable company → finite positive EV."""
        inputs = _make_fcff_inputs()
        result = self.model.value(inputs)
        assert math.isfinite(result.enterprise_value)
        assert result.enterprise_value > 0

    def test_intrinsic_value_per_share_positive(self) -> None:
        inputs = _make_fcff_inputs()
        result = self.model.value(inputs)
        assert result.intrinsic_value > 0

    def test_terminal_value_pct_bounded(self) -> None:
        """Terminal value must be between 0 and 100 % of EV."""
        inputs = _make_fcff_inputs()
        result = self.model.value(inputs)
        assert 0.0 <= result.terminal_value_pct <= 1.0

    def test_wacc_exceeds_terminal_growth_raises(self) -> None:
        """WACC <= terminal_growth_rate → ValueError."""
        inputs = _make_fcff_inputs(wacc=0.04, terminal_growth_rate=0.05)
        with pytest.raises(ValueError, match="must exceed terminal growth"):
            self.model.value(inputs)

    def test_equity_value_accounts_debt_cash(self) -> None:
        """equity_value = EV − debt + cash."""
        inputs = _make_fcff_inputs(total_debt=400.0, cash=150.0)
        result = self.model.value(inputs)
        expected_equity = result.enterprise_value - 400.0 + 150.0
        assert result.equity_value == pytest.approx(expected_equity, rel=1e-6)

    def test_equity_value_accounts_minority_interest(self) -> None:
        """2026-07-19 full-codebase-review Fix B6: equity_value = EV -
        debt - minority_interest + cash. A company with a material
        non-controlling interest should have a LOWER equity value (and
        intrinsic value per share) than an otherwise-identical company
        with none, since consolidated EV includes 100% of a
        partially-owned subsidiary's value."""
        inputs_no_mi = _make_fcff_inputs(total_debt=400.0, cash=150.0, minority_interest=0.0)
        inputs_with_mi = _make_fcff_inputs(total_debt=400.0, cash=150.0, minority_interest=200.0)

        result_no_mi = self.model.value(inputs_no_mi)
        result_with_mi = self.model.value(inputs_with_mi)

        expected_equity = result_with_mi.enterprise_value - 400.0 - 200.0 + 150.0
        assert result_with_mi.equity_value == pytest.approx(expected_equity, rel=1e-6)
        assert result_with_mi.equity_value < result_no_mi.equity_value
        assert result_with_mi.intrinsic_value < result_no_mi.intrinsic_value

    def test_minority_interest_defaults_to_zero_no_change(self) -> None:
        """Omitting minority_interest preserves prior behavior exactly."""
        explicit_zero = self.model.value(_make_fcff_inputs(minority_interest=0.0))
        default = self.model.value(_make_fcff_inputs())
        assert explicit_zero.equity_value == pytest.approx(default.equity_value, rel=1e-9)

    def test_higher_growth_gives_higher_ev(self) -> None:
        low = self.model.value(_make_fcff_inputs(high_growth_rate=0.05))
        high = self.model.value(_make_fcff_inputs(high_growth_rate=0.25))
        assert high.enterprise_value > low.enterprise_value

    def test_model_name(self) -> None:
        result = self.model.value(_make_fcff_inputs())
        assert "FCFFTwoStage" in result.model_used


# ---------------------------------------------------------------------------
# FCFFThreeStageModel tests
# ---------------------------------------------------------------------------

class TestFCFFThreeStageModel:
    """SPEC-VAL-002 Model 3: three-stage revenue-based model."""

    def test_positive_ev(self) -> None:
        inputs = _make_fcff_inputs(target_margin=0.14)
        result = FCFFThreeStageModel().value(inputs)
        assert result.enterprise_value > 0

    def test_model_name(self) -> None:
        inputs = _make_fcff_inputs()
        result = FCFFThreeStageModel().value(inputs)
        assert "FCFFThreeStage" in result.model_used

    def test_wacc_below_terminal_growth_raises(self) -> None:
        inputs = _make_fcff_inputs(wacc=0.04, terminal_growth_rate=0.05)
        with pytest.raises(ValueError, match="must exceed"):
            FCFFThreeStageModel().value(inputs)


# ---------------------------------------------------------------------------
# CommodityNormalizedModel tests (SPEC-VAL-002 Model 7)
# ---------------------------------------------------------------------------

class TestCommodityNormalizedModel:
    def test_positive_intrinsic_value(self) -> None:
        result = CommodityNormalizedModel().value(
            revenue=1000.0,
            normalized_margin=0.15,
            tax_rate=0.25,
            wacc=0.12,
            depreciation=50.0,
            capex=60.0,
            change_in_nwc=10.0,
            shares_outstanding=100.0,
            total_debt=200.0,
            cash=50.0,
        )
        assert result.intrinsic_value > 0
        assert result.model_used == "CommodityNormalized"
        assert result.assumptions["normalized_margin"] == pytest.approx(0.15)

    def test_delegates_to_two_stage_model(self) -> None:
        """CommodityNormalizedModel should reuse FCFFTwoStageModel's EV math."""
        commodity_result = CommodityNormalizedModel().value(
            revenue=1000.0, normalized_margin=0.10, tax_rate=0.25, wacc=0.12,
        )
        equiv_inputs = FCFFInputs(
            ebit=1000.0 * 0.10, tax_rate=0.25, depreciation=0.0, capex=0.0,
            change_in_nwc=0.0, wacc=0.12, high_growth_rate=0.04, revenue=1000.0,
            terminal_growth_rate=0.03, high_growth_years=5,
        )
        two_stage_result = FCFFTwoStageModel().value(equiv_inputs)
        assert commodity_result.enterprise_value == pytest.approx(two_stage_result.enterprise_value)


# ---------------------------------------------------------------------------
# ExcessReturnModel tests
# ---------------------------------------------------------------------------

class TestExcessReturnModel:
    """SPEC-VAL-002 Model 4: excess return for financial services."""

    def test_positive_excess_returns(self) -> None:
        """ROE > CoE → equity value > book value."""
        result = ExcessReturnModel().value(
            book_value=1_000.0,
            roe=0.18,
            cost_of_equity=0.12,
            terminal_growth=0.05,
            shares_outstanding=100.0,
        )
        assert result.equity_value > 1_000.0  # excess returns add value

    def test_zero_excess_returns(self) -> None:
        """ROE == CoE → equity value ≈ book value (no excess returns)."""
        coe = 0.12
        result = ExcessReturnModel().value(
            book_value=1_000.0,
            roe=coe,
            cost_of_equity=coe,
            terminal_growth=0.05,
            shares_outstanding=100.0,
        )
        assert result.equity_value == pytest.approx(1_000.0, rel=0.01)

    def test_coe_le_growth_raises(self) -> None:
        with pytest.raises(ValueError, match="must exceed terminal growth"):
            ExcessReturnModel().value(
                book_value=1_000.0, roe=0.12, cost_of_equity=0.04, terminal_growth=0.05
            )


# ---------------------------------------------------------------------------
# MonteCarloDCF tests
# ---------------------------------------------------------------------------

class TestMonteCarloDCF:
    """SPEC-VAL-005: Monte Carlo uncertainty quantification."""

    def setup_method(self) -> None:
        self.mc = MonteCarloDCF(seed=42)

    def test_probability_undervalued_bounds(self) -> None:
        """probability_undervalued must be in [0, 1] (SPEC-VAL-005)."""
        inputs = _make_fcff_inputs()
        result = self.mc.simulate(inputs, current_price=500.0, n_simulations=100)
        assert 0.0 <= result.probability_undervalued <= 1.0

    def test_percentile_ordering(self) -> None:
        """p10 <= median <= p90."""
        inputs = _make_fcff_inputs()
        result = self.mc.simulate(inputs, current_price=500.0, n_simulations=100)
        assert result.p10 <= result.median_value <= result.p90

    def test_valid_monte_carlo_result(self) -> None:
        """All result fields should be finite."""
        inputs = _make_fcff_inputs()
        result = self.mc.simulate(inputs, current_price=500.0, n_simulations=100)
        assert math.isfinite(result.median_value)
        assert math.isfinite(result.p10)
        assert math.isfinite(result.p90)
        assert math.isfinite(result.mc_skew)
        assert result.n_simulations >= 50  # at least half must succeed

    def test_n_simulations_recorded(self) -> None:
        inputs = _make_fcff_inputs()
        result = self.mc.simulate(inputs, n_simulations=100)
        # At least 50 % must succeed for a well-formed input
        assert result.n_simulations >= 50

    def test_zero_current_price_skips_probability(self) -> None:
        """current_price=0 → probability_undervalued may be nan (price unknown)."""
        inputs = _make_fcff_inputs()
        result = self.mc.simulate(inputs, current_price=0.0, n_simulations=100)
        # Either nan or meaningful float — just check it doesn't raise
        assert result is not None

    def test_too_many_failed_draws_raises_runtime_error(self) -> None:
        """If every simulated draw fails/produces a non-finite value, simulate()
        must raise rather than silently return a garbage result (SPEC-VAL-005)."""
        inputs = _make_fcff_inputs()
        with patch(
            "systems.damodaran_valuation.scenarios.monte_carlo.FCFFTwoStageModel.value",
            side_effect=ValueError("forced failure for every draw"),
        ):
            with pytest.raises(RuntimeError, match="produced finite positive"):
                self.mc.simulate(inputs, current_price=500.0, n_simulations=20)


# ---------------------------------------------------------------------------
# RelativePERegression tests
# ---------------------------------------------------------------------------

class TestRelativePERegression:
    """SPEC-VAL-002 Model 5: cross-sectional P/E regression."""

    @staticmethod
    def _make_peer_df(n: int = 20) -> pd.DataFrame:
        """TEST FIXTURE — synthetic peer dataset for regression fitting."""
        rng = np.random.default_rng(0)
        eps_g = rng.uniform(0.05, 0.30, n)
        payout = rng.uniform(0.10, 0.60, n)
        beta = rng.uniform(0.60, 1.40, n)
        # Roughly PE = 5 + 50*eps_g + 10*payout - 3*beta + noise
        pe = 5 + 50 * eps_g + 10 * payout - 3 * beta + rng.normal(0, 2, n)
        return pd.DataFrame({"pe_ratio": pe, "eps_growth_3y": eps_g,
                             "payout_ratio": payout, "beta": beta})

    def test_fit_and_predict(self) -> None:
        reg = RelativePERegression()
        peer_df = self._make_peer_df()
        reg.fit(peer_df)
        result = reg.value_gap({
            "pe_ratio": 25.0, "eps_growth_3y": 0.15, "payout_ratio": 0.30, "beta": 1.0
        })
        assert math.isfinite(result.gap_pct)
        assert math.isfinite(result.predicted_pe)
        assert result.n_peers == len(peer_df)

    def test_r_squared_positive(self) -> None:
        reg = RelativePERegression()
        peer_df = self._make_peer_df(30)
        reg.fit(peer_df)
        result = reg.value_gap({"pe_ratio": 20.0, "eps_growth_3y": 0.10,
                                "payout_ratio": 0.25, "beta": 0.9})
        # With our clean synthetic data R² should be fairly high
        assert result.r_squared > 0.5

    def test_no_fit_raises(self) -> None:
        reg = RelativePERegression()
        with pytest.raises(RuntimeError, match="Call fit\\(\\)"):
            reg.value_gap({"pe_ratio": 20.0})

    def test_insufficient_peers_raises(self) -> None:
        reg = RelativePERegression(min_peers=5)
        tiny_df = self._make_peer_df(3)
        with pytest.raises(ValueError, match="at least 5"):
            reg.fit(tiny_df)

    def test_lstsq_failure_wrapped_as_value_error(self) -> None:
        """A np.linalg.LinAlgError from lstsq must be re-raised as a ValueError,
        not an unhandled LinAlgError (SPEC-VAL-002 Model 5 error contract)."""
        reg = RelativePERegression()
        peer_df = self._make_peer_df()
        with patch(
            "numpy.linalg.lstsq",
            side_effect=np.linalg.LinAlgError("forced SVD failure"),
        ):
            with pytest.raises(ValueError, match="OLS fit failed"):
                reg.fit(peer_df)


# ---------------------------------------------------------------------------
# ValuationEngine.value_stock error contract
# ---------------------------------------------------------------------------

class TestValuationEngineErrorContract:
    """
    SPEC-QUALITY-003 / no-stub policy:
    Test only the RuntimeError contract — do NOT inject synthetic data into
    the production DB.  We mock _load_fundamentals at the module level to
    simulate an insufficient-data scenario without touching the real DuckDB.
    """

    def test_raises_on_insufficient_fundamentals(self) -> None:
        """< 4 quarters → RuntimeError with backfill pointer."""
        from systems.damodaran_valuation import valuation_engine

        # Return a 2-row DataFrame to simulate 2 quarters of data
        stub_df = pd.DataFrame([{"revenue": 100.0}] * 2)

        with patch.object(valuation_engine, "_load_fundamentals", return_value=stub_df):
            from systems.damodaran_valuation.valuation_engine import ValuationEngine
            engine = ValuationEngine(write_signals=False)
            with pytest.raises(RuntimeError, match="Insufficient fundamentals"):
                engine.value_stock("FAKEINC", as_of_date="2026-07-01")

    def test_value_universe_swallows_error(self) -> None:
        """value_universe must never propagate exceptions from individual tickers."""
        from systems.damodaran_valuation import valuation_engine

        stub_df = pd.DataFrame()  # 0 rows → triggers RuntimeError

        with patch.object(valuation_engine, "_load_fundamentals", return_value=stub_df):
            from systems.damodaran_valuation.valuation_engine import ValuationEngine
            engine = ValuationEngine(write_signals=False)
            results = engine.value_universe(["FAKEINC", "FAKEINC2"], n_workers=1)
        # Should return a list of 2 ValuationResult objects with error set
        assert len(results) == 2
        for r in results:
            assert r is not None
            assert r.error is not None
