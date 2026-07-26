"""
systems/fundamental_analysis/quality/capital_allocation.py

Capital Allocation Quality: whether retained capital is creating value
(delta EBIT vs. prior 3yr retained earnings), penalizing dilution and
leverage while rewarding interest coverage.
"""

from typing import Dict, Optional

from systems.fundamental_analysis.scoring_utils import weighted_zscore_composite

CAPITAL_ALLOCATION_WEIGHTS = {
    "capital_allocation_efficiency": 0.50,
    "dilution_3y": -0.20,
    "debt_to_equity": -0.15,
    "interest_coverage": 0.15,
}


def capital_allocation_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: sector-relative capital-allocation efficiency, vs. dilution/leverage (lower=better)."""
    return weighted_zscore_composite(ratios, CAPITAL_ALLOCATION_WEIGHTS)
