"""
systems/fundamental_analysis/quality/sector_leader.py

Sector-Leader Compounders: industry leaders defend margins and compound
longer — approximated via 5yr ROCE persistence, margin stability, 5yr
sales growth, and a gross-margin/asset-turnover leadership proxy.
"""

from typing import Dict, Optional

from systems.fundamental_analysis.scoring_utils import weighted_zscore_composite

# NOTE [2026-07-28 model-review]: heavily correlated with Moat
# (systems/fundamental_analysis/quality/moat.py) and Longevity
# (systems/fundamental_analysis/growth/longevity.py) — all three are
# dominated by avg_roce_5y + margin_stability_5y (0.60 of this formula's
# weight alone), with only the smaller secondary legs (sales_cagr_5y/
# gross_margin/asset_turnover here) differing. See moat.py's identical
# note for the full comparison; a full correlation-matrix backtest to
# quantify the actual overlap is a separate, larger effort (tracked
# outside this fix) — this is a by-inspection finding, not a measured one.
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
