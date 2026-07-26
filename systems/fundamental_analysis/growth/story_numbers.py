"""
systems/fundamental_analysis/growth/story_numbers.py

Story + Numbers Confirmation (Lynch-style): narrative only matters if the
financials confirm it — growth backed by cash conversion and disciplined
working capital, not just a rising share price.
"""

from typing import Dict, Optional

from systems.fundamental_analysis.scoring_utils import weighted_zscore_composite

STORY_NUMBERS_WEIGHTS = {
    "revenue_cagr_3yr": 0.30,
    "ebitda_growth_yoy": 0.25,
    "cfo_to_pat": 0.20,
    "receivable_days_change": -0.15,
    "inventory_days_change": -0.10,
}


def story_numbers_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: sector-relative growth + cash conversion, vs. working-capital deterioration (lower=better)."""
    return weighted_zscore_composite(ratios, STORY_NUMBERS_WEIGHTS)
