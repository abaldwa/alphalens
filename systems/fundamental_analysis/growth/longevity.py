"""
systems/fundamental_analysis/growth/longevity.py

Longevity Compounders: durability over raw growth speed — 5yr avg ROCE,
5yr sales growth, low earnings volatility, low leverage.
"""

from typing import Dict, Optional

from systems.fundamental_analysis.scoring_utils import weighted_zscore_composite

# NOTE [2026-07-28 model-review]: heavily correlated with Moat
# (systems/fundamental_analysis/quality/moat.py) and Sector-Leader
# (systems/fundamental_analysis/quality/sector_leader.py) — all three are
# dominated by avg_roce_5y + a stability term (margin_stability_5y for
# Moat/Sector-Leader, earnings_volatility_5y here) plus debt_to_equity,
# differing mainly in sales_cagr_5y's presence/weight. See moat.py's
# identical note for the full comparison; a full correlation-matrix
# backtest to quantify the actual overlap is a separate, larger effort
# (tracked outside this fix) — this is a by-inspection finding, not a
# measured one.
LONGEVITY_COMPOUNDER_WEIGHTS = {
    "avg_roce_5y": 0.35,
    "sales_cagr_5y": 0.25,
    "earnings_volatility_5y": -0.20,
    "debt_to_equity": -0.20,
}


def longevity_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: sector-relative 5yr avg ROCE + sales growth, vs. volatility/leverage (lower=better)."""
    return weighted_zscore_composite(ratios, LONGEVITY_COMPOUNDER_WEIGHTS)
