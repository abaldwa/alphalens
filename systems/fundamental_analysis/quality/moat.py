"""
systems/fundamental_analysis/quality/moat.py

Moat Compounders: persistence/stability proxies for a durable competitive
advantage (a moat itself isn't codable directly) — 5-year average ROCE,
5-year margin stability, and low leverage.
"""

from typing import Dict, Optional

from systems.fundamental_analysis.scoring_utils import weighted_zscore_composite

MOAT_WEIGHTS = {
    "avg_roce_5y": 0.45,
    "margin_stability_5y": 0.30,
    "debt_to_equity": -0.25,
}


def moat_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: sector-relative 5yr avg ROCE + margin stability, vs. leverage (lower=better)."""
    return weighted_zscore_composite(ratios, MOAT_WEIGHTS)
