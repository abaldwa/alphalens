"""
systems/fundamental_analysis/growth/small_cap_compounders.py

Vijay Kedia/Dolly Khanna-style Small-Cap Compounders: small size combined
with business quality/growth and risk control. `market_cap` is sector-
z-scored like every other ratio, so a negative weight here means
"smaller than sector peers" (the doc's sector_percentile_asc(market_cap)).
"""

from typing import Dict, Optional

from systems.fundamental_analysis.scoring_utils import combine_subscores, weighted_zscore_composite

# NOTE [2026-07-28 second model-review, item 12]: roce (0.35 of the 50%-
# weighted quality_growth leg) and debt_to_equity (-0.5 of the 30%-weighted
# risk_control leg) are the same dominant ROE/ROCE/leverage features
# already flagged as multicollinear across Moat/Sector-Leader/Longevity,
# QGLP, Owner Earnings, Capital Efficiency, and Governance-Aware Quality
# Growth — a ticker's ROCE/leverage profile is a shared driver across this
# whole family, on top of (not instead of) the size leg that's actually
# distinctive to this strategy. By-inspection finding, not a measured
# correlation-matrix backtest (tracked outside this fix, same as Moat's
# own note in systems/fundamental_analysis/quality/moat.py).
QUALITY_GROWTH_WEIGHTS = {"roce": 0.35, "eps_growth_yoy": 0.35, "revenue_cagr_3yr": 0.30}
RISK_CONTROL_WEIGHTS = {"debt_to_equity": -0.5, "cfo_to_pat": 0.5}
SIZE_WEIGHT = {"market_cap": -1.0}
SMALL_CAP_LEG_WEIGHTS = {"size": 0.20, "quality_growth": 0.50, "risk_control": 0.30}


def small_cap_compounder_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: small size + quality/growth + risk control, blended."""
    legs = {
        "size": weighted_zscore_composite(ratios, SIZE_WEIGHT),
        "quality_growth": weighted_zscore_composite(ratios, QUALITY_GROWTH_WEIGHTS),
        "risk_control": weighted_zscore_composite(ratios, RISK_CONTROL_WEIGHTS),
    }
    return combine_subscores(legs, SMALL_CAP_LEG_WEIGHTS)
