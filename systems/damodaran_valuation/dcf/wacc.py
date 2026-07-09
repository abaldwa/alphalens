"""
systems/damodaran_valuation/dcf/wacc.py

Phase: 3
Specs: SPEC-VAL-003
Owner: Platform / Valuation
Consumers: systems/damodaran_valuation/valuation_engine.py

India-specific WACC computation following Damodaran's 2025 methodology:
  - Risk-free rate = G-Sec 10Y yield − India sovereign default spread
  - Equity risk premium = mature market ERP + lambda × country risk premium
  - Beta levered from sector unlevered beta via Hamada; Blume-adjusted
  - Cost of debt from synthetic rating (interest-coverage → Damodaran spread table)
  - After-tax WACC weighted by market-value weights

Reference: Damodaran, A. (2025). Equity Risk Premiums (ERP): Determinants,
Estimation and Implications. NYU Stern Working Paper, January 2025.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict


# ---------------------------------------------------------------------------
# Damodaran 2025 — Sector unlevered betas (India emerging market context)
# See scripts/download_damodaran_datasets.py for annual refresh procedure.
# ---------------------------------------------------------------------------
# Keys must match config/nifty500_universe.csv's real `sector` taxonomy
# exactly (NSE's official sector classification) — an earlier version used
# ad-hoc labels ("Banking", "IT Services", "Auto", ...) that never matched
# any real sector string, so every stock silently fell through to "Default"
# regardless of sector. See BuildLog.md 2026-07-04 for the fix.
SECTOR_UNLEVERED_BETAS: Dict[str, float] = {
    "Financial Services": 0.45,
    "Information Technology": 0.85,
    "Healthcare": 0.75,
    "Fast Moving Consumer Goods": 0.55,
    "Automobile and Auto Components": 0.90,
    "Metals & Mining": 1.10,
    "Chemicals": 0.85,
    "Realty": 0.95,
    "Power": 0.70,
    "Construction": 0.85,
    "Construction Materials": 0.85,
    "Telecommunication": 0.75,
    "Capital Goods": 0.90,
    "Consumer Durables": 0.80,
    "Consumer Services": 0.85,
    "Media Entertainment & Publication": 0.85,
    "Oil Gas & Consumable Fuels": 0.80,
    "Services": 0.85,
    "Textiles": 0.85,
    "Diversified": 0.90,
    "Default": 0.90,
}


@dataclass
class WACCInputs:
    """
    Inputs required for India-specific WACC computation (SPEC-VAL-003).

    All rates are expressed as decimals (e.g. 0.063 for 6.3 %).

    Attributes
    ----------
    india_10y_yield : float
        Current India G-Sec 10-year yield (from macro_indicators table).
    beta_unlevered : float
        Damodaran sector unlevered beta (from SECTOR_UNLEVERED_BETAS).
    debt_to_equity : float
        Book D/E ratio (total_debt / total_equity).
    cost_of_debt : float
        Pre-tax cost of debt; typically derived from
        ``WACCCalculator.synthetic_rating_spread``.
    debt_weight : float
        Debt / (Debt + Equity) at market value.
    equity_weight : float
        Equity / (Debt + Equity) at market value.
    india_default_spread : float
        Damodaran 2025 India sovereign default spread (default 2.16 %).
    mature_market_erp : float
        S&P 500 base equity risk premium (default 4.2 %).
    india_country_risk_premium : float
        Damodaran 2025 India country risk premium (default 2.3 %).
    tax_rate : float
        Effective corporate tax rate; India standard = 25 %.
    domestic_revenue_pct : float
        Fraction of revenue from India (lambda scaling factor).
    """

    india_10y_yield: float
    beta_unlevered: float
    debt_to_equity: float
    cost_of_debt: float
    debt_weight: float
    equity_weight: float
    india_default_spread: float = 0.0216
    mature_market_erp: float = 0.042
    india_country_risk_premium: float = 0.023
    tax_rate: float = 0.25
    domestic_revenue_pct: float = 1.0


@dataclass
class WACCResult:
    """
    WACC computation output (SPEC-VAL-003).

    Attributes
    ----------
    wacc : float
        Weighted-average cost of capital (decimal).
    cost_of_equity : float
        CAPM-derived cost of equity (decimal).
    after_tax_cost_of_debt : float
        After-tax cost of debt (decimal).
    beta_levered : float
        Hamada re-levered beta.
    beta_adjusted : float
        Blume-adjusted beta (0.67×levered + 0.33).
    risk_free_rate : float
        G-Sec yield net of sovereign default spread (decimal).
    equity_risk_premium : float
        Blended ERP including country risk premium (decimal).
    assumptions : dict
        All intermediate scalars for audit trail.
    """

    wacc: float
    cost_of_equity: float
    after_tax_cost_of_debt: float
    beta_levered: float
    beta_adjusted: float
    risk_free_rate: float
    equity_risk_premium: float
    assumptions: Dict[str, float] = field(default_factory=dict)


class WACCCalculator:
    """
    India-specific WACC calculator (SPEC-VAL-003).

    Usage
    -----
    >>> inputs = WACCInputs(
    ...     india_10y_yield=0.0680,
    ...     beta_unlevered=0.85,
    ...     debt_to_equity=0.30,
    ...     cost_of_debt=0.090,
    ...     debt_weight=0.23,
    ...     equity_weight=0.77,
    ... )
    >>> result = WACCCalculator().compute(inputs)
    >>> result.wacc  # doctest: +SKIP
    0.1123
    """

    def compute(self, inputs: WACCInputs) -> WACCResult:
        """
        Compute WACC using Damodaran's India methodology (SPEC-VAL-003).

        Parameters
        ----------
        inputs : WACCInputs
            Full set of market and company inputs.

        Returns
        -------
        WACCResult
            WACC and all intermediate components.

        Notes
        -----
        Formula chain:
          risk_free_rate      = india_10y_yield − india_default_spread
          lambda              = domestic_revenue_pct / 0.80
          equity_risk_premium = mature_market_erp + lambda × country_risk_premium
          beta_levered        = beta_unlevered × (1 + (1 − t) × D/E)
          beta_adjusted       = 0.67 × beta_levered + 0.33     [Blume]
          cost_of_equity      = risk_free_rate + beta_adjusted × equity_risk_premium
          after_tax_cod       = cost_of_debt × (1 − tax_rate)
          wacc                = equity_weight × CoE + debt_weight × after_tax_cod
        """
        rfr = inputs.india_10y_yield - inputs.india_default_spread
        # Lambda = firm's domestic revenue share / average domestic revenue share (80 %)
        lam = inputs.domestic_revenue_pct / 0.80
        erp = inputs.mature_market_erp + lam * inputs.india_country_risk_premium

        beta_lev = inputs.beta_unlevered * (
            1.0 + (1.0 - inputs.tax_rate) * inputs.debt_to_equity
        )
        beta_adj = 0.67 * beta_lev + 0.33  # Blume adjustment toward 1.0

        coe = rfr + beta_adj * erp
        atcod = inputs.cost_of_debt * (1.0 - inputs.tax_rate)
        wacc = inputs.equity_weight * coe + inputs.debt_weight * atcod

        return WACCResult(
            wacc=wacc,
            cost_of_equity=coe,
            after_tax_cost_of_debt=atcod,
            beta_levered=beta_lev,
            beta_adjusted=beta_adj,
            risk_free_rate=rfr,
            equity_risk_premium=erp,
            assumptions={
                "india_10y_yield": inputs.india_10y_yield,
                "india_default_spread": inputs.india_default_spread,
                "mature_market_erp": inputs.mature_market_erp,
                "india_country_risk_premium": inputs.india_country_risk_premium,
                "lambda": lam,
                "beta_unlevered": inputs.beta_unlevered,
                "debt_to_equity": inputs.debt_to_equity,
                "tax_rate": inputs.tax_rate,
                "domestic_revenue_pct": inputs.domestic_revenue_pct,
            },
        )

    @staticmethod
    def synthetic_rating_spread(interest_coverage: float) -> float:
        """
        Map interest-coverage ratio to Damodaran synthetic rating spread (SPEC-VAL-003).

        Uses Damodaran's ICR → credit rating → default spread table (2025 data).
        Returned spread should be added to the risk-free rate to derive pre-tax
        cost of debt.

        Parameters
        ----------
        interest_coverage : float
            EBIT / Interest expense.  Negative values → 'D' rating spread.

        Returns
        -------
        float
            Annual default spread as a decimal (e.g. 0.0063 for AAA).

        Examples
        --------
        >>> WACCCalculator.synthetic_rating_spread(15.0)
        0.0063
        >>> WACCCalculator.synthetic_rating_spread(8.0)
        0.0098
        >>> WACCCalculator.synthetic_rating_spread(1.0)
        0.055
        """
        icr = interest_coverage
        if icr > 12.5:
            return 0.0063   # AAA
        elif icr > 9.5:
            return 0.0080   # AA
        elif icr > 7.5:
            return 0.0098   # A+
        elif icr > 6.0:
            return 0.0108   # A
        elif icr > 4.5:
            return 0.0122   # A-
        elif icr > 3.5:
            return 0.0156   # BBB
        elif icr > 2.5:
            return 0.0200   # BB+
        elif icr > 2.0:
            return 0.0240   # BB
        elif icr > 1.5:
            return 0.0340   # B+
        elif icr > 1.25:
            return 0.0450   # B
        elif icr > 0.8:
            return 0.0550   # B-
        elif icr > 0.5:
            return 0.0725   # CCC
        else:
            return 0.1200   # D

    @classmethod
    def from_fundamentals(
        cls,
        *,
        india_10y_yield: float,
        sector: str,
        debt_to_equity: float,
        interest_coverage: float,
        total_debt: float,
        market_cap: float,
        domestic_revenue_pct: float = 1.0,
        tax_rate: float = 0.25,
    ) -> "tuple[WACCCalculator, WACCInputs]":
        """
        Convenience factory: build WACCInputs from raw fundamentals.

        Parameters
        ----------
        india_10y_yield : float
            Current G-Sec 10Y yield from macro_indicators.
        sector : str
            Company sector (looked up in SECTOR_UNLEVERED_BETAS).
        debt_to_equity : float
            Book D/E ratio.
        interest_coverage : float
            EBIT / Interest.
        total_debt : float
            Total book debt (INR crore).
        market_cap : float
            Market capitalisation (INR crore).
        domestic_revenue_pct : float
            Fraction of revenue from India.
        tax_rate : float
            Effective corporate tax rate.

        Returns
        -------
        tuple[WACCCalculator, WACCInputs]
            Ready-to-call calculator and inputs object.
        """
        beta_unlevered = SECTOR_UNLEVERED_BETAS.get(sector, SECTOR_UNLEVERED_BETAS["Default"])
        spread = cls.synthetic_rating_spread(interest_coverage)
        cost_of_debt = india_10y_yield + spread

        total_value = market_cap + total_debt
        debt_weight = total_debt / total_value if total_value > 0 else 0.0
        equity_weight = 1.0 - debt_weight

        inputs = WACCInputs(
            india_10y_yield=india_10y_yield,
            beta_unlevered=beta_unlevered,
            debt_to_equity=debt_to_equity,
            cost_of_debt=cost_of_debt,
            debt_weight=debt_weight,
            equity_weight=equity_weight,
            domestic_revenue_pct=domestic_revenue_pct,
            tax_rate=tax_rate,
        )
        return cls(), inputs
