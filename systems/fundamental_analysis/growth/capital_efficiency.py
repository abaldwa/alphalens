"""
systems/fundamental_analysis/growth/capital_efficiency.py

Capital-Efficiency Growth: businesses that grow without balance-sheet
stress — sales growth, ROCE, asset turns, working-capital discipline.
"""

from typing import Dict, Optional

from systems.fundamental_analysis.scoring_utils import weighted_zscore_composite

# NOTE [2026-07-28 second model-review, item 12]: roce (0.30) is the same
# dominant feature already flagged as multicollinear across Moat/Sector-
# Leader/Longevity, QGLP, and Owner Earnings — a ticker's ROCE profile is
# a shared driver across this whole family, not unique signal here. By-
# inspection finding, not a measured correlation-matrix backtest (tracked
# outside this fix, same as Moat's own note in systems/fundamental_
# analysis/quality/moat.py).
CAPITAL_EFFICIENCY_GROWTH_WEIGHTS = {
    "revenue_cagr_3yr": 0.35,
    "roce": 0.30,
    "asset_turnover": 0.20,
    "receivable_days_change": -0.15,
}


def capital_efficiency_growth_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: sector-relative sales growth + ROCE + asset turns, vs. working-capital deterioration (lower=better)."""
    return weighted_zscore_composite(ratios, CAPITAL_EFFICIENCY_GROWTH_WEIGHTS)
