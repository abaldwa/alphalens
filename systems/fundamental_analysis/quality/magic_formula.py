"""
systems/fundamental_analysis/quality/magic_formula.py

Greenblatt Magic Formula: earnings yield (EBIT/EV) + return on capital
(EBIT/(NWC+net fixed assets)), equal-weighted. Sector-relative ranking
(the codebase's existing convention for every other preset) substitutes
for Greenblatt's classic Financials/Utilities exclusion — comparing a
bank only to other banks makes that blanket exclusion unnecessary.
"""

from typing import Dict, Optional

from systems.fundamental_analysis.scoring_utils import weighted_zscore_composite as _weighted_zscore_composite

MAGIC_FORMULA_WEIGHTS = {
    "ev_ebit_yield": 0.50,
    "magic_formula_roc": 0.50,
}


def magic_formula_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: sector-relative earnings yield + return on capital, equal-weighted."""
    return _weighted_zscore_composite(ratios, MAGIC_FORMULA_WEIGHTS)
