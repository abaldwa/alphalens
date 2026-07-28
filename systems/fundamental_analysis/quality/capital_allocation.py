"""
systems/fundamental_analysis/quality/capital_allocation.py

Capital Allocation Quality: whether retained capital is creating value
(delta EBIT vs. prior 3yr retained earnings), penalizing dilution and
leverage while rewarding interest coverage.
"""

from typing import Dict, Optional

from systems.fundamental_analysis.scoring_utils import weighted_zscore_composite

# NOTE [2026-07-28 second model-review, item 12]: debt_to_equity (-0.15)
# and interest_coverage (0.15) together are the same leverage/solvency
# family already flagged as multicollinear across Moat/Sector-Leader/
# Longevity, Owner Earnings, and Small-Cap Compounders — this strategy's
# capital_allocation_efficiency leg (50%) is its own distinctive signal,
# but the 30% leverage component overlaps with that broader family. By-
# inspection finding, not a measured correlation-matrix backtest (tracked
# outside this fix, same as Moat's own note in systems/fundamental_
# analysis/quality/moat.py).
CAPITAL_ALLOCATION_WEIGHTS = {
    "capital_allocation_efficiency": 0.50,
    "dilution_3y": -0.20,
    "debt_to_equity": -0.15,
    "interest_coverage": 0.15,
}


def capital_allocation_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: sector-relative capital-allocation efficiency, vs. dilution/leverage (lower=better)."""
    return weighted_zscore_composite(ratios, CAPITAL_ALLOCATION_WEIGHTS)
