"""
systems/fundamental_analysis/quality/magic_formula.py

Greenblatt Magic Formula: earnings yield (EBIT/EV) + return on capital
(EBIT/(NWC+net fixed assets)), equal-weighted.

[BUG FIX, 2026-07-28 second model-review, item 10] This docstring used to
claim sector-relative ranking substitutes for Greenblatt's classic
Financials/Utilities exclusion, making it "unnecessary" — that was wrong
and directly contradicted by features/fundamental_composites.py's
PRESET_EXCLUDED_SECTORS["magic_formula"] = {"Financial Services"}, added
by the 2026-07-25 model-review fix. Sector-relative z-scoring does NOT fix
EV/EBIT and NWC-based ROC being structurally meaningless concepts for a
bank/NBFC/insurer (EBIT isn't coherent when "revenue" is net interest
income; "current liabilities" for a bank includes customer deposits, not
a working-capital drag) — comparing a bank's equally-nonsensical EV/EBIT-
ROC only to other banks' equally-nonsensical EV/EBIT-ROC still ranks on
noise, not signal. The Financial Services exclusion is real, enforced,
and load-bearing — see PRESET_EXCLUDED_SECTORS' own comment for the full
argument.
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
