"""
systems/fundamental_analysis/management/promoter_aligned.py

Promoter-Aligned Compounders: an overlay on QGLP, not a standalone edge
(per the source doc's own framing — "use only as an overlay"). Promoter
alignment uses raw governance fields (promoter_pct, promoter_pledge,
dilution_3y — the last one IS sector-z-scored, unlike the other two, so
it's blended in via weighted_zscore_composite while the raw pair get an
explicit linear scale, same split as management_quality_score).
"""

from typing import Dict, Optional

import numpy as np

from systems.fundamental_analysis.growth.qglp import qglp_score
from systems.fundamental_analysis.scoring_utils import combine_subscores, weighted_zscore_composite

PROMOTER_ALIGNED_LEG_WEIGHTS = {"qglp": 0.75, "promoter_alignment": 0.25}
DILUTION_WEIGHT = {"dilution_3y": -1.0}


def _promoter_alignment_score(ratios: Dict[str, float]) -> Optional[float]:
    promoter_pct = ratios.get("promoter_pct")
    promoter_pledge = ratios.get("promoter_pledge")
    dilution_leg = weighted_zscore_composite(ratios, DILUTION_WEIGHT)

    raw_score = None
    if promoter_pct is not None and not (isinstance(promoter_pct, float) and np.isnan(promoter_pct)):
        promoter_pct = float(np.clip(promoter_pct, 0.0, 100.0))
        if promoter_pledge is not None and not (isinstance(promoter_pledge, float) and np.isnan(promoter_pledge)):
            # [2026-07-25 model-review fix] promoter_pledge is disclosed as
            # % OF the promoter's own holding (standard Indian disclosure
            # convention, same as management_quality_score's -0.5-pt-per-1%
            # scale), not a same-unit amount to subtract directly from
            # promoter_pct. The prior version did `clip(promoter_pct -
            # promoter_pledge, 0, 100)` — once promoter_pct was already
            # low, any pledge above that value floored at 0 identically,
            # collapsing the difference between a 30%-pledged and a
            # 95%-pledged promoter (skeptic-tester's top domain finding).
            # Scaling promoter_pct by the UNPLEDGED fraction instead keeps
            # severity distinguishable across the full 0-100% pledge range
            # regardless of how large promoter_pct itself is.
            pledge_frac = float(np.clip(promoter_pledge, 0.0, 100.0)) / 100.0
            raw_score = promoter_pct * (1.0 - pledge_frac)
        else:
            raw_score = promoter_pct

    return combine_subscores({"raw": raw_score, "dilution": dilution_leg}, {"raw": 0.7, "dilution": 0.3})


def promoter_aligned_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: 0.75 x QGLP + 0.25 x promoter-alignment overlay."""
    legs = {"qglp": qglp_score(ratios), "promoter_alignment": _promoter_alignment_score(ratios)}
    return combine_subscores(legs, PROMOTER_ALIGNED_LEG_WEIGHTS)
