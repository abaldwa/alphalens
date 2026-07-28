"""
systems/fundamental_analysis/growth/qglp.py

Raamdeo Agrawal's QGLP framework: Quality + Growth + Longevity + Price.
Each leg is itself a weighted_zscore_composite (0-100), then the 4 legs
are blended via combine_subscores.

Proxy substitutions where the doc's exact field doesn't exist in this
codebase's feature set (documented, not silent):
- GROWTH's eps_cagr_3y -> eps_growth_yoy (no separate 3yr EPS CAGR feature
  is computed; revenue_cagr_3yr already covers the sales leg).
- LONGEVITY's stdev(revenue_growth,5y)/max_drawdown_earnings ->
  margin_stability_5y/earnings_volatility_5y (this codebase's closest
  existing 5yr stability/volatility measures).
- PRICE's -peg -> -pe_ratio (no PEG feature is computed; GARP's own PEG
  leg has the same substitution, see growth/garp.py).
"""

from typing import Dict, Optional

from systems.fundamental_analysis.scoring_utils import combine_subscores, weighted_zscore_composite

# NOTE [2026-07-28 second model-review, item 12]: shares its dominant
# ROE/ROCE-family feature with Moat/Sector-Leader/Longevity (systems/
# fundamental_analysis/quality/moat.py's own multicollinearity note) and
# with owner_earnings/capital_efficiency/governance_quality_growth/
# small_cap_compounders/capital_allocation below — roce/roe appear in the
# QUALITY leg (0.4/0.3), avg_roce_5y in the LONGEVITY leg (0.4), and
# delta_roce_3y in the GROWTH leg (0.2), so a ticker's overall ROCE
# profile drives 3 of QGLP's 4 legs, not just an incidental one. By-
# inspection finding, not a measured correlation-matrix backtest (tracked
# outside this fix, same as Moat's own note) — left here so a future
# reviewer doesn't re-litigate "is QGLP actually distinct from the
# ROCE-heavy strategies?" from scratch.
QUALITY_WEIGHTS = {"roce": 0.4, "roe": 0.3, "cfo_to_pat": 0.3}
GROWTH_WEIGHTS = {"revenue_cagr_3yr": 0.4, "eps_growth_yoy": 0.4, "delta_roce_3y": 0.2}
LONGEVITY_WEIGHTS = {"avg_roce_5y": 0.4, "margin_stability_5y": 0.3, "earnings_volatility_5y": -0.3}
PRICE_WEIGHTS = {"ev_ebit_yield": 0.4, "book_to_market": 0.3, "pe_ratio": -0.3}
QGLP_LEG_WEIGHTS = {"quality": 0.30, "growth": 0.25, "longevity": 0.25, "price": 0.20}


def qglp_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: blended Quality + Growth + Longevity + Price legs."""
    legs = {
        "quality": weighted_zscore_composite(ratios, QUALITY_WEIGHTS),
        "growth": weighted_zscore_composite(ratios, GROWTH_WEIGHTS),
        "longevity": weighted_zscore_composite(ratios, LONGEVITY_WEIGHTS),
        "price": weighted_zscore_composite(ratios, PRICE_WEIGHTS),
    }
    return combine_subscores(legs, QGLP_LEG_WEIGHTS)
