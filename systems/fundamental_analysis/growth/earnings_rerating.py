"""
systems/fundamental_analysis/growth/earnings_rerating.py

Earnings Re-rating Candidates: fundamentals inflecting before valuation
fully catches up — EPS growth accelerating, margins expanding, ROCE
improving, still cheap on EBIT/EV.
"""

from typing import Dict, Optional

from systems.fundamental_analysis.scoring_utils import weighted_zscore_composite

EARNINGS_RERATING_WEIGHTS = {
    "eps_acceleration": 0.35,
    "margin_expansion": 0.25,
    "delta_roce_3y": 0.20,
    "ev_ebit_yield": 0.20,
}


def earnings_rerating_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: sector-relative EPS acceleration + margin expansion + ROCE improvement + cheapness."""
    return weighted_zscore_composite(ratios, EARNINGS_RERATING_WEIGHTS)
