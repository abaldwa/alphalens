"""
systems/damodaran_valuation/lifecycle/classifier.py

Phase: 3
Specs: SPEC-VAL-001
Owner: Platform / Valuation
Consumers: systems/damodaran_valuation/valuation_engine.py

Classify stocks into Damodaran's lifecycle stages.  The classifier is
stateless — callers prepare a fundamentals dict and call `classify`.

Lifecycle stages (SPEC-VAL-001):
    FINANCIAL_SERVICES  — banks, NBFCs, insurance, AMCs (separate model path)
    DISTRESSED          — negative margin, weak coverage, or Altman Z < 1.81
    YOUNG_GROWTH        — fast-growing but still margin-negative
    HIGH_GROWTH         — fast growth + profitable + good ROE
    MATURE_GROWTH       — moderate growth + starting to pay dividends
    MATURE_STABLE       — slow growth + established payout
    DECLINING           — stagnant/declining revenue or well-below-sector margins
"""

from __future__ import annotations

from enum import Enum
from typing import Dict, Optional, Set


class LifecycleStage(str, Enum):
    """Damodaran lifecycle taxonomy (SPEC-VAL-001)."""

    YOUNG_GROWTH = "young_growth"
    HIGH_GROWTH = "high_growth"
    MATURE_GROWTH = "mature_growth"
    MATURE_STABLE = "mature_stable"
    DECLINING = "declining"
    DISTRESSED = "distressed"
    FINANCIAL_SERVICES = "financial_services"


# Must match config/nifty500_universe.csv's real `sector` taxonomy exactly
# (NSE's official classification only has "Financial Services" — there is
# no separate "Banking"/"NBFC"/"Insurance" sector string in the data). The
# previous set never matched any real sector, so no stock ever took the
# FINANCIAL_SERVICES lifecycle path. See BuildLog.md 2026-07-04.
_FINANCIAL_SERVICES_SECTORS: Set[str] = {
    "Financial Services",
}


class LifecycleClassifier:
    """
    Rule-based Damodaran lifecycle stage classifier (SPEC-VAL-001).

    Rules are evaluated in priority order:
      1. Financial services sector → FINANCIAL_SERVICES (separate model needed)
      2. Distress signals → DISTRESSED
      3. High-revenue-growth + low margin → YOUNG_GROWTH
      4. High-growth + profitable + strong ROE → HIGH_GROWTH
      5. Moderate growth + dividend payout → MATURE_GROWTH
      6. Low growth + established payout → MATURE_STABLE
      7. Stagnant / margin below sector median → DECLINING
      8. Default fallback → MATURE_STABLE

    Parameters
    ----------
    None — stateless; all configuration embedded as constants.
    """

    # -----------------------------------------------------------------------
    # Thresholds (SPEC-VAL-001 §3.2)
    # -----------------------------------------------------------------------
    _DISTRESS_MIN_INTEREST_COV: float = 1.5
    _DISTRESS_MIN_ALTMAN_Z: float = 1.81

    _YOUNG_GROWTH_MIN_CAGR: float = 0.30
    _YOUNG_GROWTH_MAX_MARGIN: float = 0.10

    _HIGH_GROWTH_MIN_CAGR: float = 0.15
    _HIGH_GROWTH_MIN_MARGIN: float = 0.08
    _HIGH_GROWTH_MIN_ROE: float = 0.12

    _MATURE_GROWTH_MIN_CAGR: float = 0.05
    _MATURE_GROWTH_MIN_PAYOUT: float = 0.15

    _MATURE_STABLE_MAX_CAGR: float = 0.08
    _MATURE_STABLE_MIN_PAYOUT: float = 0.30

    _DECLINING_MAX_CAGR: float = 0.02
    _DECLINING_SECTOR_MARGIN_RATIO: float = 0.50  # < 50 % of sector median

    def classify(self, fundamentals: Dict) -> LifecycleStage:
        """
        Classify a stock into its Damodaran lifecycle stage.

        Parameters
        ----------
        fundamentals : dict
            Keys expected (all optional — missing values treated as 0 / None):

            revenue_cagr_3y      : float  3-year revenue CAGR (e.g. 0.15 for 15 %)
            revenue_cagr_5y      : float  5-year revenue CAGR
            operating_margin     : float  EBIT margin
            net_margin           : float  PAT / Revenue
            payout_ratio         : float  DPS / EPS
            roe                  : float  Return on equity
            reinvestment_rate    : float  1 - payout_ratio (if provided, used as-is)
            interest_coverage    : float  EBIT / Interest expense
            altman_z             : float  Altman Z-score (pre-computed by caller)
            sector               : str    Sector name (e.g. "Banking")
            sector_median_margin : float  Sector median net margin (for declining test)

        Returns
        -------
        LifecycleStage
            One of the seven lifecycle stages defined in SPEC-VAL-001.

        Notes
        -----
        Priority is strictly top-down; the first matching rule wins.

        Examples
        --------
        >>> c = LifecycleClassifier()
        >>> c.classify({"sector": "Banking"})
        <LifecycleStage.FINANCIAL_SERVICES: 'financial_services'>
        >>> c.classify({"interest_coverage": 1.0})
        <LifecycleStage.DISTRESSED: 'distressed'>
        """
        sector: str = fundamentals.get("sector") or ""
        revenue_cagr_3y: float = fundamentals.get("revenue_cagr_3y") or 0.0
        net_margin: float = fundamentals.get("net_margin") or 0.0
        payout_ratio: float = fundamentals.get("payout_ratio") or 0.0
        roe: float = fundamentals.get("roe") or 0.0
        interest_coverage: Optional[float] = fundamentals.get("interest_coverage")
        altman_z: Optional[float] = fundamentals.get("altman_z")
        sector_median_margin: Optional[float] = fundamentals.get("sector_median_margin")

        # ------------------------------------------------------------------
        # Rule 1: Financial services — separate DCF model required
        # ------------------------------------------------------------------
        if sector in _FINANCIAL_SERVICES_SECTORS:
            return LifecycleStage.FINANCIAL_SERVICES

        # ------------------------------------------------------------------
        # Rule 2: Distress
        # ------------------------------------------------------------------
        if net_margin < 0:
            return LifecycleStage.DISTRESSED
        if interest_coverage is not None and interest_coverage < self._DISTRESS_MIN_INTEREST_COV:
            return LifecycleStage.DISTRESSED
        if altman_z is not None and altman_z < self._DISTRESS_MIN_ALTMAN_Z:
            return LifecycleStage.DISTRESSED

        # ------------------------------------------------------------------
        # Rule 3: Young growth — fast-growing but not yet profitable enough
        # ------------------------------------------------------------------
        if (revenue_cagr_3y > self._YOUNG_GROWTH_MIN_CAGR
                and net_margin < self._YOUNG_GROWTH_MAX_MARGIN):
            return LifecycleStage.YOUNG_GROWTH

        # ------------------------------------------------------------------
        # Rule 4: High growth — fast-growing, profitable, strong ROE
        # ------------------------------------------------------------------
        if (revenue_cagr_3y > self._HIGH_GROWTH_MIN_CAGR
                and net_margin > self._HIGH_GROWTH_MIN_MARGIN
                and roe > self._HIGH_GROWTH_MIN_ROE):
            return LifecycleStage.HIGH_GROWTH

        # ------------------------------------------------------------------
        # Rule 5: Mature growth — moderate growth + dividends starting
        # ------------------------------------------------------------------
        if (revenue_cagr_3y > self._MATURE_GROWTH_MIN_CAGR
                and payout_ratio > self._MATURE_GROWTH_MIN_PAYOUT):
            return LifecycleStage.MATURE_GROWTH

        # ------------------------------------------------------------------
        # Rule 6: Mature stable — slow growth + established payout
        # ------------------------------------------------------------------
        if (revenue_cagr_3y < self._MATURE_STABLE_MAX_CAGR
                and payout_ratio > self._MATURE_STABLE_MIN_PAYOUT):
            return LifecycleStage.MATURE_STABLE

        # ------------------------------------------------------------------
        # Rule 7: Declining — near-zero growth or well-below-peer margin
        # ------------------------------------------------------------------
        if revenue_cagr_3y < self._DECLINING_MAX_CAGR:
            return LifecycleStage.DECLINING
        if (sector_median_margin is not None
                and net_margin < sector_median_margin * self._DECLINING_SECTOR_MARGIN_RATIO):
            return LifecycleStage.DECLINING

        # ------------------------------------------------------------------
        # Default fallback
        # ------------------------------------------------------------------
        return LifecycleStage.MATURE_STABLE
