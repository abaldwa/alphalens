"""
systems/fundamental_analysis/quality/sector_leader.py

Sector-Leader Compounders: industry leaders defend margins and compound
longer — approximated via 5yr ROCE persistence, margin stability, 5yr
sales growth, and a gross-margin/asset-turnover leadership proxy.
"""

from typing import Dict, Optional

from systems.fundamental_analysis.scoring_utils import weighted_zscore_composite

SECTOR_LEADER_WEIGHTS = {
    "avg_roce_5y": 0.35,
    "margin_stability_5y": 0.25,
    "sales_cagr_5y": 0.20,
    "gross_margin": 0.10,
    "asset_turnover": 0.10,
}


def sector_leader_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: sector-relative ROCE persistence, margin stability, growth, and leadership proxy."""
    return weighted_zscore_composite(ratios, SECTOR_LEADER_WEIGHTS)
