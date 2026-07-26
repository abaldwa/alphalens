"""
systems/fundamental_analysis/quality/quality_value.py

Quality-Value Composite: valuation (EV/EBIT yield, book-to-market) blended
with quality (ROCE, ROE). Same 0-100 weighted-sector-z-score display
convention as features/fundamental_composites.py's quality_score/
growth_score — reuses that module's _weighted_zscore_composite directly
rather than reimplementing the arithmetic.
"""

from typing import Dict, Optional

from systems.fundamental_analysis.scoring_utils import weighted_zscore_composite as _weighted_zscore_composite

QUALITY_VALUE_WEIGHTS = {
    "ev_ebit_yield": 0.30,
    "book_to_market": 0.20,
    "roce": 0.30,
    "roe": 0.20,
}


def quality_value_composite(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: sector-relative earnings yield/book-to-market (cheap=better) blended with ROCE/ROE (higher=better)."""
    return _weighted_zscore_composite(ratios, QUALITY_VALUE_WEIGHTS)
