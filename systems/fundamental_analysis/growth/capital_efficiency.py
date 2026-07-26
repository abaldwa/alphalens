"""
systems/fundamental_analysis/growth/capital_efficiency.py

Capital-Efficiency Growth: businesses that grow without balance-sheet
stress — sales growth, ROCE, asset turns, working-capital discipline.
"""

from typing import Dict, Optional

from systems.fundamental_analysis.scoring_utils import weighted_zscore_composite

CAPITAL_EFFICIENCY_GROWTH_WEIGHTS = {
    "revenue_cagr_3yr": 0.35,
    "roce": 0.30,
    "asset_turnover": 0.20,
    "receivable_days_change": -0.15,
}


def capital_efficiency_growth_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: sector-relative sales growth + ROCE + asset turns, vs. working-capital deterioration (lower=better)."""
    return weighted_zscore_composite(ratios, CAPITAL_EFFICIENCY_GROWTH_WEIGHTS)
