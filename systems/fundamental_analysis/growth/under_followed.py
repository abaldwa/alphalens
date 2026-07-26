"""
systems/fundamental_analysis/growth/under_followed.py

Dolly Khanna-style Under-followed Growth Improvers: re-rating often starts
in smaller, under-owned companies with improving numbers.

institutional_ownership_pct (features/governance.py) is deliberately NOT
sector-z-scored (same convention as promoter_pledge/management_quality_score
— it's a bounded percentage, not a ratio with a meaningful sector mean), so
it can't go through weighted_zscore_composite alongside the z-scored
growth ratios. Scored separately with an explicit, documented linear scale
instead — same shape as management_quality_score's raw-field handling.
"""

from typing import Dict, Optional

import numpy as np

from systems.fundamental_analysis.scoring_utils import combine_subscores, weighted_zscore_composite

GROWTH_IMPROVEMENT_WEIGHTS = {"eps_acceleration": 0.4, "margin_expansion": 0.3, "delta_roce_3y": 0.3}
UNDER_FOLLOWED_LEG_WEIGHTS = {"under_followed": 0.25, "growth_improvement": 0.75}
# Ad-hoc scale: combined FII+DII+MF holding of 0% -> fully under-followed
# (100), 50%+ -> treated as fully "followed" (0). 50% is a documented
# assumption, not a backtested threshold — same standing as
# management_quality_score's -0.5-point-per-1%-pledge scale.
FULLY_FOLLOWED_THRESHOLD_PCT = 50.0


def _under_followed_proxy(institutional_ownership_pct: float) -> float:
    if institutional_ownership_pct is None or (isinstance(institutional_ownership_pct, float) and np.isnan(institutional_ownership_pct)):
        return np.nan
    return float(np.clip(100.0 - (institutional_ownership_pct / FULLY_FOLLOWED_THRESHOLD_PCT) * 100.0, 0.0, 100.0))


def under_followed_growth_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: blended under-followed proxy (from raw institutional_ownership_pct) + growth improvement."""
    legs = {
        "under_followed": _under_followed_proxy(ratios.get("institutional_ownership_pct")),
        "growth_improvement": weighted_zscore_composite(ratios, GROWTH_IMPROVEMENT_WEIGHTS),
    }
    return combine_subscores(legs, UNDER_FOLLOWED_LEG_WEIGHTS)
