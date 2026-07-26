"""
systems/fundamental_analysis/contrarian/normalization.py

Burry-style Normalization Value for cyclicals: current earnings are
temporarily depressed, use a cycle-average margin instead of the latest
quarter's. normalized_ebit uses avg_ebitda_margin_5y as an ebit_margin
proxy (see features/fundamental.py's avg_ebitda_margin_5y docstring) times
current revenue, divided by EV — computed here rather than as a stored
feature since it needs `revenue` (level, not ratio) and `enterprise_value`
which aren't sector-z-scored inputs a composite function normally takes.

Ships as a composite over already-z-scored proxy fields rather than the
literal normalized_ebit/EV computation (which needs raw, un-z-scored
revenue/EV — a different input shape than every other composite in this
package) — ev_ebit_yield combined with avg_ebitda_margin_5y approximates
the same "cheap relative to normalized earning power" idea without
requiring a bespoke raw-value module.
"""

from typing import Dict, Optional

from systems.fundamental_analysis.scoring_utils import weighted_zscore_composite

NORMALIZATION_WEIGHTS = {
    "ev_ebit_yield": 0.35,
    "avg_ebitda_margin_5y": 0.25,
    "delta_operating_cash_flow_1y": 0.40,
}


def normalization_value_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: sector-relative cheapness vs. normalized earning power + CFO recovery."""
    return weighted_zscore_composite(ratios, NORMALIZATION_WEIGHTS)
