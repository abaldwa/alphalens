"""
systems/fundamental_analysis/management/governance_quality_growth.py

Sunil Singhania-style Governance-Aware Quality Growth: business quality
paired with governance discipline. Blends a z-scored business-quality leg
with a raw-governance leg — same split as under_followed.py, since
promoter/pledge fields are deliberately not sector-z-scored.
"""

from typing import Dict, Optional


from systems.fundamental_analysis.scoring_utils import combine_subscores, weighted_zscore_composite

BUSINESS_QUALITY_WEIGHTS = {"roce": 0.4, "fcf_ev_yield": 0.3, "revenue_cagr_3yr": 0.3}
GOV_LEG_WEIGHTS = {"business_quality": 0.70, "governance": 0.30}


def _governance_score(ratios: Dict[str, float]) -> Optional[float]:
    """Raw-field governance score, same shape/scale as management_quality_score
    (features/fundamental_composites.py) — reuses that function's fields
    directly rather than duplicating the pledge/conviction logic."""
    from features.fundamental_composites import management_quality_score

    return management_quality_score(ratios)


def governance_quality_growth_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: blended business quality (z-scored) + governance quality (raw)."""
    legs = {
        "business_quality": weighted_zscore_composite(ratios, BUSINESS_QUALITY_WEIGHTS),
        "governance": _governance_score(ratios),
    }
    return combine_subscores(legs, GOV_LEG_WEIGHTS)
