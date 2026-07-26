"""
systems/fundamental_analysis/growth/longevity.py

Longevity Compounders: durability over raw growth speed — 5yr avg ROCE,
5yr sales growth, low earnings volatility, low leverage.
"""

from typing import Dict, Optional

from systems.fundamental_analysis.scoring_utils import weighted_zscore_composite

LONGEVITY_COMPOUNDER_WEIGHTS = {
    "avg_roce_5y": 0.35,
    "sales_cagr_5y": 0.25,
    "earnings_volatility_5y": -0.20,
    "debt_to_equity": -0.20,
}


def longevity_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: sector-relative 5yr avg ROCE + sales growth, vs. volatility/leverage (lower=better)."""
    return weighted_zscore_composite(ratios, LONGEVITY_COMPOUNDER_WEIGHTS)
