"""
systems/fundamental_analysis/contrarian/recovery.py

Michael Burry/Seth Klarman-style Contrarian Recovery Value: cheap stocks
where perception is worse than improving fundamentals.
"""

from typing import Dict, Optional

from systems.fundamental_analysis.scoring_utils import combine_subscores, weighted_zscore_composite

CHEAPNESS_WEIGHTS = {"book_to_market": 0.5, "ev_ebit_yield": 0.5}
RECOVERY_WEIGHTS = {
    "delta_operating_cash_flow_1y": 0.35,
    "net_debt_to_ebitda": -0.35,
    "margin_expansion": 0.30,
}
RECOVERY_LEG_WEIGHTS = {"cheapness": 0.45, "recovery": 0.55}


def contrarian_recovery_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: blended cheapness + fundamental-recovery signals."""
    legs = {
        "cheapness": weighted_zscore_composite(ratios, CHEAPNESS_WEIGHTS),
        "recovery": weighted_zscore_composite(ratios, RECOVERY_WEIGHTS),
    }
    return combine_subscores(legs, RECOVERY_LEG_WEIGHTS)
