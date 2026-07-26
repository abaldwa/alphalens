"""
systems/fundamental_analysis/growth/garp.py

Growth At a Reasonable Price: growth blended with valuation discipline and
margin stability. Now fills the margin-stability leg an earlier session
deferred (it needed a multi-year EBITDA-margin stddev feature, which now
exists as margin_stability_5y — 5-year rather than the doc's 3-year
window, close enough to not warrant a second near-duplicate feature).

The GARP `SCREENER_PRESETS` entry (features/fundamental_composites.py) is
a separate, simpler binary threshold and is unaffected by this change —
only this composite `garp_score` function gains the stability leg.
"""

from typing import Dict, Optional

from systems.fundamental_analysis.scoring_utils import combine_subscores, weighted_zscore_composite

GROWTH_WEIGHTS = {"revenue_cagr_3yr": 0.5, "eps_growth_yoy": 0.5}
PRICE_REASONABLENESS_WEIGHT = {"pe_ratio": -1.0}
STABILITY_WEIGHTS = {"margin_stability_5y": 0.7, "cfo_to_pat": 0.3}
GARP_LEG_WEIGHTS = {"growth": 0.45, "price": 0.30, "stability": 0.25}


def garp_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: blended growth + valuation-discipline + margin-stability legs."""
    legs = {
        "growth": weighted_zscore_composite(ratios, GROWTH_WEIGHTS),
        "price": weighted_zscore_composite(ratios, PRICE_REASONABLENESS_WEIGHT),
        "stability": weighted_zscore_composite(ratios, STABILITY_WEIGHTS),
    }
    return combine_subscores(legs, GARP_LEG_WEIGHTS)
