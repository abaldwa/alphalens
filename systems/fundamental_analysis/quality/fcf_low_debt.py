"""
systems/fundamental_analysis/quality/fcf_low_debt.py

FCF Yield + Low Debt: cash generation (FCF/EV) rewarded, leverage
(debt_to_ebitda) penalized, interest_coverage rewarded. Same
weighted-sector-z-score convention as quality_value.py.
"""

from typing import Dict, Optional

from systems.fundamental_analysis.scoring_utils import weighted_zscore_composite as _weighted_zscore_composite

FCF_LOW_DEBT_WEIGHTS = {
    "fcf_ev_yield": 0.50,
    "debt_to_ebitda": -0.30,
    "interest_coverage": 0.20,
}


def fcf_low_debt_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: sector-relative FCF/EV yield (higher=better) vs. leverage (lower=better) and coverage (higher=better)."""
    return _weighted_zscore_composite(ratios, FCF_LOW_DEBT_WEIGHTS)
