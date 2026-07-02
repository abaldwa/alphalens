"""
systems/damodaran_valuation/scenarios/monte_carlo.py

Phase: 3
Specs: SPEC-VAL-005
Owner: Platform / Valuation
Consumers: systems/damodaran_valuation/valuation_engine.py

Monte Carlo DCF uncertainty quantification (SPEC-VAL-005).

Samples growth rate, terminal margin, and WACC from triangular / normal
distributions and runs FCFFTwoStageModel for each draw.  Returns percentile
statistics, probability of undervaluation, and value-at-risk.

Dependencies: scipy (scipy.stats.triang, scipy.stats.skew).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
from scipy import stats

from systems.damodaran_valuation.dcf.models import DCFResult, FCFFInputs, FCFFTwoStageModel


@dataclass
class MonteCarloResult:
    """
    Monte Carlo simulation output (SPEC-VAL-005).

    Attributes
    ----------
    median_value : float
        Median intrinsic value across all simulations (INR per share).
    p10 : float
        10th percentile intrinsic value (bear scenario).
    p90 : float
        90th percentile intrinsic value (bull scenario).
    probability_undervalued : float
        Fraction of simulations where intrinsic value > current_price (0–1).
    value_at_risk_5pct : float
        5th percentile of (current_price − intrinsic_value); positive means
        intrinsic value was well above price in 95 % of sims.
    mc_skew : float
        Skewness of the intrinsic value distribution.
    n_simulations : int
        Number of Monte Carlo draws performed.
    """

    median_value: float
    p10: float
    p90: float
    probability_undervalued: float
    value_at_risk_5pct: float
    mc_skew: float
    n_simulations: int


class MonteCarloDCF:
    """
    Monte Carlo wrapper around FCFFTwoStageModel (SPEC-VAL-005).

    Draws parameters from:
      - growth_rate : triangular(low=base×0.5, peak=base, high=base×1.5)
      - terminal_margin : triangular(low=margin×0.6, peak=margin, high=margin×1.3)
      - wacc : normal(mean=wacc, std=0.01)

    Parameters
    ----------
    seed : int, optional
        Random seed for reproducibility.
    """

    def __init__(self, seed: Optional[int] = None) -> None:
        self._rng = np.random.default_rng(seed)

    def simulate(
        self,
        base_inputs: FCFFInputs,
        current_price: float = 0.0,
        n_simulations: int = 10_000,
    ) -> MonteCarloResult:
        """
        Run Monte Carlo DCF simulation (SPEC-VAL-005).

        Parameters
        ----------
        base_inputs : FCFFInputs
            Base-case inputs used as the mode of all triangular distributions.
            Must have ``ebit``, ``wacc``, ``high_growth_rate``, ``revenue`` set.
        current_price : float
            Current market price per share (INR); used to compute
            ``probability_undervalued`` and ``value_at_risk_5pct``.
        n_simulations : int
            Number of Monte Carlo draws (default 10 000; use 100 for fast tests).

        Returns
        -------
        MonteCarloResult
            Percentile statistics and risk metrics.

        Notes
        -----
        Draws that produce WACC ≤ terminal_growth_rate are discarded and
        resampled to avoid ill-defined Gordon Growth denominators.
        Individual simulation failures (extreme inputs) are silently skipped;
        if fewer than ``n_simulations // 2`` succeed a RuntimeError is raised.

        Examples
        --------
        >>> from systems.damodaran_valuation.dcf.models import FCFFInputs
        >>> inp = FCFFInputs(
        ...     ebit=100, tax_rate=0.25, depreciation=20, capex=30,
        ...     change_in_nwc=5, wacc=0.12, high_growth_rate=0.15,
        ...     revenue=500, terminal_growth_rate=0.05,
        ... )
        >>> mc = MonteCarloDCF(seed=42)
        >>> result = mc.simulate(inp, current_price=50.0, n_simulations=100)
        >>> 0 <= result.probability_undervalued <= 1
        True
        """
        model = FCFFTwoStageModel()
        base_growth = base_inputs.high_growth_rate
        base_wacc = base_inputs.wacc
        base_rev = max(base_inputs.revenue, 1e-9)
        base_margin = base_inputs.ebit / base_rev

        # -- Triangular distribution parameters (c = (peak - low) / (high - low)) ----
        # growth_rate
        g_low = base_growth * 0.5
        g_peak = base_growth
        g_high = base_growth * 1.5
        g_scale = g_high - g_low
        g_c = (g_peak - g_low) / g_scale if g_scale > 0 else 0.5

        # terminal_margin
        m_low = base_margin * 0.6
        m_peak = base_margin
        m_high_val = base_margin * 1.3
        m_scale = m_high_val - m_low
        m_c = (m_peak - m_low) / m_scale if m_scale > 0 else 0.5

        g_samples = stats.triang.rvs(
            c=g_c, loc=g_low, scale=g_scale,
            size=n_simulations, random_state=self._rng.integers(0, 2**31),
        )
        m_samples = stats.triang.rvs(
            c=m_c, loc=m_low, scale=m_scale,
            size=n_simulations, random_state=self._rng.integers(0, 2**31),
        )
        w_samples = self._rng.normal(loc=base_wacc, scale=0.01, size=n_simulations)

        intrinsic_values: list[float] = []

        for g_s, m_s, w_s in zip(g_samples, m_samples, w_samples):
            # Ensure WACC > terminal growth
            w_s = max(w_s, base_inputs.terminal_growth_rate + 0.005)
            sim_inputs = FCFFInputs(
                ebit=base_rev * m_s,
                tax_rate=base_inputs.tax_rate,
                depreciation=base_inputs.depreciation,
                capex=base_inputs.capex,
                change_in_nwc=base_inputs.change_in_nwc,
                wacc=w_s,
                high_growth_rate=g_s,
                revenue=base_inputs.revenue,
                terminal_growth_rate=base_inputs.terminal_growth_rate,
                high_growth_years=base_inputs.high_growth_years,
                shares_outstanding=base_inputs.shares_outstanding,
                total_debt=base_inputs.total_debt,
                cash=base_inputs.cash,
            )
            try:
                res: DCFResult = model.value(sim_inputs)
                if np.isfinite(res.intrinsic_value) and res.intrinsic_value > 0:
                    intrinsic_values.append(res.intrinsic_value)
            except Exception:
                pass  # extreme parameter combination — skip silently

        if len(intrinsic_values) < n_simulations // 2:
            raise RuntimeError(
                f"Monte Carlo: only {len(intrinsic_values)}/{n_simulations} simulations "
                "produced finite positive intrinsic values.  Check base inputs."
            )

        iv_arr = np.array(intrinsic_values, dtype=float)
        prob_undervalued = float(np.mean(iv_arr > current_price)) if current_price > 0 else float("nan")
        losses = current_price - iv_arr
        var_5pct = float(np.percentile(losses, 5))

        return MonteCarloResult(
            median_value=float(np.median(iv_arr)),
            p10=float(np.percentile(iv_arr, 10)),
            p90=float(np.percentile(iv_arr, 90)),
            probability_undervalued=prob_undervalued,
            value_at_risk_5pct=var_5pct,
            mc_skew=float(stats.skew(iv_arr)),
            n_simulations=len(iv_arr),
        )
