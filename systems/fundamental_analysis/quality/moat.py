"""
systems/fundamental_analysis/quality/moat.py

Moat Compounders: persistence/stability proxies for a durable competitive
advantage (a moat itself isn't codable directly) — 5-year average ROCE,
5-year margin stability, and low leverage.
"""

from typing import Dict, Optional

from systems.fundamental_analysis.scoring_utils import weighted_zscore_composite

# NOTE [2026-07-28 model-review]: heavily correlated with Sector-Leader
# (systems/fundamental_analysis/quality/sector_leader.py) and Longevity
# (systems/fundamental_analysis/growth/longevity.py) — all three are
# dominated by avg_roce_5y + a margin/earnings-stability term, differing
# only in their smaller secondary legs (this one: debt_to_equity;
# Sector-Leader: sales_cagr_5y + gross_margin/asset_turnover; Longevity:
# sales_cagr_5y + earnings_volatility_5y + debt_to_equity). A full
# correlation-matrix backtest to quantify the overlap is a separate,
# larger effort (tracked outside this fix) — this is a by-inspection
# finding, not a measured one, left here so a future reviewer doesn't
# re-litigate "are these three actually different strategies?" from
# scratch.
MOAT_WEIGHTS = {
    "avg_roce_5y": 0.45,
    "margin_stability_5y": 0.30,
    "debt_to_equity": -0.25,
}


def moat_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: sector-relative 5yr avg ROCE + margin stability, vs. leverage (lower=better)."""
    return weighted_zscore_composite(ratios, MOAT_WEIGHTS)
