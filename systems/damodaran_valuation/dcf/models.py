"""
systems/damodaran_valuation/dcf/models.py

Phase: 3
Specs: SPEC-VAL-002
Owner: Platform / Valuation
Consumers: systems/damodaran_valuation/valuation_engine.py,
           systems/damodaran_valuation/scenarios/monte_carlo.py

Four primary DCF model implementations (SPEC-VAL-002):
  - FCFFTwoStageModel   — classic high-growth + terminal (Models 1/2/3 variants)
  - FCFFThreeStageModel — revenue-driven with margin ramp (Model 3, three stages)
  - ExcessReturnModel   — for banks/NBFCs/financial services (Model 4)
  - CommodityNormalizedModel — for metals/commodities (Model 7)

All models return a DCFResult dataclass.  EV → equity value conversion requires
the caller to pass shares_outstanding, total_debt, and cash.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class FCFFInputs:
    """
    Inputs for FCFF-based DCF models (SPEC-VAL-002).

    Attributes
    ----------
    ebit : float
        Earnings before interest and tax (INR crore).
    tax_rate : float
        Effective tax rate (decimal, e.g. 0.25).
    depreciation : float
        D&A for the base year (INR crore).
    capex : float
        Capital expenditure for the base year (INR crore).
    change_in_nwc : float
        Change in net working capital for the base year (INR crore).
    wacc : float
        WACC (decimal).
    high_growth_rate : float
        Revenue / FCFF growth rate during the high-growth phase (decimal).
    revenue : float
        Base-year revenue (INR crore); used in three-stage model.
    terminal_growth_rate : float
        Perpetuity growth rate; India-standard default = 5 % (nominal GDP).
    high_growth_years : int
        Number of years in the high-growth phase (default 5).
    shares_outstanding : float
        Diluted shares outstanding (crore); used to derive per-share value.
    total_debt : float
        Book debt (INR crore); deducted from EV to get equity value.
    cash : float
        Cash & equivalents (INR crore); added back after debt deduction.
    minority_interest : float
        Non-controlling/minority interest (INR crore); deducted from EV
        alongside total_debt, since a consolidated EV includes 100% of a
        partially-owned subsidiary's value even though shareholders only
        have a claim on the parent's stake (2026-07-19 full-codebase-
        review Fix B6 — previously omitted entirely from the equity
        bridge, overstating intrinsic value per share for any company
        with a material listed/consolidated subsidiary). Defaults to 0.0
        so existing valuations are unchanged unless a real
        non_controlling_interest figure is supplied.
    target_margin : float, optional
        Target EBIT margin for the three-stage model; if None uses current margin.
    """

    ebit: float
    tax_rate: float
    depreciation: float
    capex: float
    change_in_nwc: float
    wacc: float
    high_growth_rate: float
    revenue: float
    terminal_growth_rate: float = 0.05
    high_growth_years: int = 5
    shares_outstanding: float = 1.0
    total_debt: float = 0.0
    cash: float = 0.0
    minority_interest: float = 0.0
    target_margin: Optional[float] = None


@dataclass
class DCFResult:
    """
    DCF model output (SPEC-VAL-002).

    Attributes
    ----------
    intrinsic_value : float
        Per-share intrinsic value (INR).
    enterprise_value : float
        Aggregate enterprise value (INR crore).
    equity_value : float
        Equity value = EV − debt + cash (INR crore).
    terminal_value : float
        Present value of the terminal (Gordon-growth) cash flow (INR crore).
    terminal_value_pct : float
        Terminal value as fraction of EV (0–1).
    implied_terminal_ev_nopat : float
        Implied EV/NOPAT multiple at the terminal year (terminal_fcff /
        (wacc - g) / NOPAT). NOTE: this is NOT a P/E ratio — NOPAT is
        unlevered (pre-interest) earnings, so for any company carrying
        debt this multiple will be lower than a true trailing/forward P/E
        (which divides by levered net income). Previously mislabeled
        ``implied_terminal_pe``; renamed rather than changed numerically —
        computing a true P/E would require a net-income projection this
        stateless model doesn't have.
    wacc : float
        WACC used (decimal).
    model_used : str
        Human-readable model name.
    assumptions : dict
        Full audit-trail of inputs and intermediate values.
    """

    intrinsic_value: float
    enterprise_value: float
    equity_value: float
    terminal_value: float
    terminal_value_pct: float
    implied_terminal_ev_nopat: float
    wacc: float
    model_used: str
    assumptions: Dict[str, float] = field(default_factory=dict)


def _base_fcff(inputs: FCFFInputs) -> float:
    """NOPAT-based FCFF for base year."""
    return (
        inputs.ebit * (1.0 - inputs.tax_rate)
        + inputs.depreciation
        - inputs.capex
        - inputs.change_in_nwc
    )


def _ev_to_result(
    pv_phase1: float,
    terminal_value_pv: float,
    inputs: FCFFInputs,
    terminal_fcff: float,
    model_name: str,
    extra_assumptions: Optional[Dict[str, float]] = None,
) -> DCFResult:
    """Shared EV → DCFResult converter."""
    ev = pv_phase1 + terminal_value_pv
    equity_value = ev - inputs.total_debt - inputs.minority_interest + inputs.cash
    shares = max(inputs.shares_outstanding, 1e-9)
    intrinsic = equity_value / shares  # both in INR crore units — cancels to INR/share

    tv_pct = terminal_value_pv / ev if ev > 0 else 0.0

    # Implied terminal P/E = terminal FCFF / (WACC − g) × (1 / NOPAT)
    nopat = inputs.ebit * (1.0 - inputs.tax_rate)
    implied_pe = (
        (terminal_fcff / max(inputs.wacc - inputs.terminal_growth_rate, 1e-6))
        / max(nopat, 1e-6)
        if terminal_fcff > 0 and nopat > 0
        else float("nan")
    )

    assumptions: Dict[str, float] = {
        "wacc": inputs.wacc,
        "terminal_growth_rate": inputs.terminal_growth_rate,
        "high_growth_rate": inputs.high_growth_rate,
        "high_growth_years": float(inputs.high_growth_years),
        "base_ebit": inputs.ebit,
        "tax_rate": inputs.tax_rate,
    }
    if extra_assumptions:
        assumptions.update(extra_assumptions)

    return DCFResult(
        intrinsic_value=intrinsic,
        enterprise_value=ev,
        equity_value=equity_value,
        terminal_value=terminal_value_pv,
        terminal_value_pct=tv_pct,
        implied_terminal_ev_nopat=implied_pe,
        wacc=inputs.wacc,
        model_used=model_name,
        assumptions=assumptions,
    )


class FCFFTwoStageModel:
    """
    Classic two-stage FCFF DCF model (SPEC-VAL-002 Models 1/2).

    Phase 1 — explicit FCFF cash flows grown at `high_growth_rate` for
    `high_growth_years` years, discounted at WACC.
    Phase 2 — terminal value via Gordon Growth Model applied at year N+1.

    Parameters
    ----------
    None — stateless; all inputs passed to ``value()``.
    """

    def value(self, inputs: FCFFInputs) -> DCFResult:
        """
        Compute enterprise value using two-stage FCFF model (SPEC-VAL-002).

        Parameters
        ----------
        inputs : FCFFInputs
            All valuation inputs (growth rates, WACC, FCFF components, etc.).

        Returns
        -------
        DCFResult
            Intrinsic value per share plus full result decomposition.

        Raises
        ------
        ValueError
            If WACC <= terminal_growth_rate (model ill-defined).

        Notes
        -----
        TV = FCFF_{N+1} / (WACC − g)
        EV = sum(PV_i for i in 1..N) + PV(TV)
        """
        if inputs.wacc <= inputs.terminal_growth_rate:
            raise ValueError(
                f"WACC ({inputs.wacc:.4f}) must exceed terminal growth rate "
                f"({inputs.terminal_growth_rate:.4f})."
            )

        base_fcff = _base_fcff(inputs)
        wacc = inputs.wacc
        g_high = inputs.high_growth_rate
        g_term = inputs.terminal_growth_rate
        n = inputs.high_growth_years

        pv_phase1 = 0.0
        fcff_t = base_fcff
        for t in range(1, n + 1):
            fcff_t *= 1.0 + g_high
            pv_phase1 += fcff_t / (1.0 + wacc) ** t

        # Terminal value at end of year N
        terminal_fcff = fcff_t * (1.0 + g_term)
        tv = terminal_fcff / (wacc - g_term)
        tv_pv = tv / (1.0 + wacc) ** n

        return _ev_to_result(pv_phase1, tv_pv, inputs, terminal_fcff, "FCFFTwoStage")


class FCFFThreeStageModel:
    """
    Revenue-driven three-stage FCFF DCF model (SPEC-VAL-002 Model 3).

    Stage 1 — high growth at ``high_growth_rate`` for ``high_growth_years``.
    Stage 2 — linear growth deceleration + margin ramp for ``transition_years``.
    Stage 3 — perpetuity at terminal growth with ``stable_margin``.

    Parameters
    ----------
    None — stateless.
    """

    def value(
        self,
        inputs: FCFFInputs,
        transition_years: int = 5,
        stable_margin: float = 0.12,
    ) -> DCFResult:
        """
        Compute enterprise value using three-stage revenue-based model (SPEC-VAL-002).

        Parameters
        ----------
        inputs : FCFFInputs
            Base-year inputs; ``revenue`` and ``target_margin`` must be set.
        transition_years : int
            Length of the transition phase (default 5).
        stable_margin : float
            EBIT margin in the stable/terminal phase (default 12 %).

        Returns
        -------
        DCFResult
            Intrinsic value per share plus full result decomposition.

        Notes
        -----
        Capex and D&A are kept at base-year ratios to revenue throughout.
        NWC change grows with revenue at its base-year ratio.
        """
        if inputs.wacc <= inputs.terminal_growth_rate:
            raise ValueError(
                f"WACC ({inputs.wacc:.4f}) must exceed terminal growth rate "
                f"({inputs.terminal_growth_rate:.4f})."
            )

        # Base-year ratios (as fraction of revenue)
        base_rev = max(inputs.revenue, 1e-9)
        base_margin = inputs.ebit / base_rev
        target_margin = (
            inputs.target_margin if inputs.target_margin is not None else stable_margin
        )
        dep_ratio = inputs.depreciation / base_rev
        capex_ratio = inputs.capex / base_rev
        nwc_ratio = inputs.change_in_nwc / base_rev

        wacc = inputs.wacc
        g_high = inputs.high_growth_rate
        g_term = inputs.terminal_growth_rate
        n1 = inputs.high_growth_years
        n2 = transition_years
        total_years = n1 + n2

        pv_phase1 = 0.0
        rev_t = base_rev
        last_fcff = None

        for t in range(1, total_years + 1):
            if t <= n1:
                g = g_high
                m = base_margin
            else:
                # Linear transition: growth decelerates to g_term, margin ramps to target
                frac = (t - n1) / n2
                g = g_high + frac * (g_term - g_high)
                m = base_margin + frac * (target_margin - base_margin)

            rev_t *= 1.0 + g
            ebit_t = rev_t * m
            dep_t = rev_t * dep_ratio
            capex_t = rev_t * capex_ratio
            nwc_t = rev_t * nwc_ratio
            fcff_t = ebit_t * (1.0 - inputs.tax_rate) + dep_t - capex_t - nwc_t
            pv_phase1 += fcff_t / (1.0 + wacc) ** t
            last_fcff = fcff_t

        # Terminal
        terminal_fcff = (last_fcff or 0.0) * (1.0 + g_term)
        tv = terminal_fcff / (wacc - g_term)
        tv_pv = tv / (1.0 + wacc) ** total_years

        extra = {
            "transition_years": float(transition_years),
            "stable_margin": stable_margin,
            "base_margin": base_margin,
        }
        return _ev_to_result(pv_phase1, tv_pv, inputs, terminal_fcff, "FCFFThreeStage", extra)


class ExcessReturnModel:
    """
    Excess-return valuation model for banks/NBFCs (SPEC-VAL-002 Model 4).

    Value = BV + PV(excess returns over terminal horizon) where
    excess return per period = (ROE − cost_of_equity) × BV.

    Parameters
    ----------
    None — stateless.
    """

    def value(
        self,
        book_value: float,
        roe: float,
        cost_of_equity: float,
        terminal_growth: float = 0.05,
        projection_years: int = 10,
        shares_outstanding: float = 1.0,
        total_debt: float = 0.0,
        cash: float = 0.0,
    ) -> DCFResult:
        """
        Compute equity value via excess-return model (SPEC-VAL-002 Model 4).

        Parameters
        ----------
        book_value : float
            Current book value of equity (INR crore).
        roe : float
            Return on equity (decimal).
        cost_of_equity : float
            Required return on equity (CoE from WACC module).
        terminal_growth : float
            Perpetuity growth rate for excess returns (default 5 %).
        projection_years : int
            Explicit projection horizon (default 10).
        shares_outstanding : float
            Diluted shares outstanding (crore).
        total_debt : float
            Book debt (INR crore); not applicable for banks but kept for API consistency.
        cash : float
            Cash & equivalents (INR crore).

        Returns
        -------
        DCFResult
            Intrinsic equity value per share.

        Notes
        -----
        Equity value = BV + PV(excess returns, years 1..N) + PV(terminal excess return)
        Excess return_t = (ROE − CoE) × BV_t
        BV grows at: BV_{t+1} = BV_t × (1 + ROE × reinvestment_rate)
        where reinvestment_rate is implied by terminal growth and ROE.
        """
        if cost_of_equity <= terminal_growth:
            raise ValueError(
                f"Cost of equity ({cost_of_equity:.4f}) must exceed "
                f"terminal growth ({terminal_growth:.4f})."
            )

        reinvestment_rate = terminal_growth / max(roe, 1e-6)
        reinvestment_rate = min(reinvestment_rate, 1.0)  # cap at 100 %

        pv_excess = 0.0
        bv_t = book_value
        for t in range(1, projection_years + 1):
            excess_return = (roe - cost_of_equity) * bv_t
            pv_excess += excess_return / (1.0 + cost_of_equity) ** t
            bv_t *= 1.0 + roe * reinvestment_rate

        # Terminal excess return (Gordon Growth on excess returns)
        terminal_excess = (roe - cost_of_equity) * bv_t
        tv_excess = terminal_excess / (cost_of_equity - terminal_growth)
        tv_pv = tv_excess / (1.0 + cost_of_equity) ** projection_years

        equity_value = book_value + pv_excess + tv_pv
        shares = max(shares_outstanding, 1e-9)
        intrinsic = equity_value / shares  # both in INR crore units — cancels to INR/share

        tv_pct = tv_pv / equity_value if equity_value > 0 else 0.0

        return DCFResult(
            intrinsic_value=intrinsic,
            enterprise_value=equity_value,   # for banks EV ≈ equity value
            equity_value=equity_value,
            terminal_value=tv_pv,
            terminal_value_pct=tv_pct,
            implied_terminal_ev_nopat=float("nan"),
            wacc=cost_of_equity,
            model_used="ExcessReturn",
            assumptions={
                "book_value": book_value,
                "roe": roe,
                "cost_of_equity": cost_of_equity,
                "terminal_growth": terminal_growth,
                "reinvestment_rate": reinvestment_rate,
                "projection_years": float(projection_years),
            },
        )


class CommodityNormalizedModel:
    """
    Normalized DCF for commodity/metals companies (SPEC-VAL-002 Model 7).

    Uses a through-cycle average margin instead of the current (potentially
    peak or trough) margin to avoid extrapolating commodity price extremes.

    Parameters
    ----------
    None — stateless.
    """

    def value(
        self,
        revenue: float,
        normalized_margin: float,
        tax_rate: float,
        wacc: float,
        depreciation: float = 0.0,
        capex: float = 0.0,
        change_in_nwc: float = 0.0,
        high_growth_rate: float = 0.04,
        terminal_growth_rate: float = 0.03,
        high_growth_years: int = 5,
        shares_outstanding: float = 1.0,
        total_debt: float = 0.0,
        cash: float = 0.0,
    ) -> DCFResult:
        """
        Compute EV using normalized (through-cycle) margins (SPEC-VAL-002 Model 7).

        Parameters
        ----------
        revenue : float
            Current-year revenue (INR crore).
        normalized_margin : float
            10-year average EBIT margin (decimal).
        tax_rate : float
            Effective tax rate (decimal).
        wacc : float
            WACC (decimal).
        depreciation : float
            D&A (INR crore); defaulted to zero if not provided.
        capex : float
            Capex (INR crore).
        change_in_nwc : float
            Change in NWC (INR crore).
        high_growth_rate : float
            Expected commodity revenue growth (default 4 %).
        terminal_growth_rate : float
            Long-run growth (default 3 % for commodity sector).
        high_growth_years : int
            Explicit projection horizon (default 5).
        shares_outstanding : float
            Diluted shares (crore).
        total_debt : float
            Book debt (INR crore).
        cash : float
            Cash (INR crore).

        Returns
        -------
        DCFResult
            Intrinsic value per share.
        """
        normalized_ebit = revenue * normalized_margin
        inputs = FCFFInputs(
            ebit=normalized_ebit,
            tax_rate=tax_rate,
            depreciation=depreciation,
            capex=capex,
            change_in_nwc=change_in_nwc,
            wacc=wacc,
            high_growth_rate=high_growth_rate,
            revenue=revenue,
            terminal_growth_rate=terminal_growth_rate,
            high_growth_years=high_growth_years,
            shares_outstanding=shares_outstanding,
            total_debt=total_debt,
            cash=cash,
        )
        result = FCFFTwoStageModel().value(inputs)
        # Override model name and add normalization info
        result.model_used = "CommodityNormalized"
        result.assumptions["normalized_margin"] = normalized_margin
        return result
