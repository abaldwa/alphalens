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
        """Banking sector must always classify as FINANCIAL_SERVICES."""
        result = self.clf.classify({"sector": "Banking"})
        assert result == LifecycleStage.FINANCIAL_SERVICES

    def test_financial_services_nbfc(self) -> None:
        result = self.clf.classify({"sector": "NBFC"})
        assert result == LifecycleStage.FINANCIAL_SERVICES

    def test_financial_services_insurance(self) -> None:
        result = self.clf.classify({"sector": "Insurance"})
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
        """Altman Z < 1.81 → DISTRESSED."""
        result = self.clf.classify({
            "altman_z": 1.5,
            "net_margin": 0.03,
            "interest_coverage": 2.5,
            "sector": "Metals",
        })
        assert result == LifecycleStage.DISTRESSED

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
