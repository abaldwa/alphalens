"""
systems/fundamental_analysis/growth/smile.py

Vijay Kedia's SMILE framework (Small in size, Medium in experience, Large
in aspiration, Extra-large in market potential), mapped onto proxy
factors.

Two legs are simplified because their doc-specified fields don't exist
anywhere in this codebase (confirmed during planning — no raw data, not
a computation gap): `industry_growth_rate` and `r_and_d_to_sales`. Rather
than block SMILE entirely, ASPIRATION and MARKET_POTENTIAL each drop to
their one available leg, re-weighted to 1.0.
"""

from typing import Dict, Optional

from systems.fundamental_analysis.scoring_utils import combine_subscores, weighted_zscore_composite

SMALL_SIZE_WEIGHT = {"market_cap": -1.0}
EXPERIENCE_WEIGHT = {"company_age_years": 1.0}
# ASPIRATION_PROXY: doc is 0.5*capex_to_sales + 0.5*r_and_d_to_sales;
# r_and_d_to_sales doesn't exist -> capex_intensity carries full weight.
ASPIRATION_WEIGHT = {"capex_intensity": 1.0}
# MARKET_POTENTIAL_PROXY: doc is 0.5*industry_growth_rate + 0.5*sales_cagr_3y;
# industry_growth_rate doesn't exist -> revenue_cagr_3yr carries full weight.
MARKET_POTENTIAL_WEIGHT = {"revenue_cagr_3yr": 1.0}
SMILE_LEG_WEIGHTS = {"small_size": 0.25, "experience": 0.20, "aspiration": 0.25, "market_potential": 0.30}


def smile_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: blended small-size + experience + aspiration + market-potential proxies (simplified, see module docstring)."""
    legs = {
        "small_size": weighted_zscore_composite(ratios, SMALL_SIZE_WEIGHT),
        "experience": weighted_zscore_composite(ratios, EXPERIENCE_WEIGHT),
        "aspiration": weighted_zscore_composite(ratios, ASPIRATION_WEIGHT),
        "market_potential": weighted_zscore_composite(ratios, MARKET_POTENTIAL_WEIGHT),
    }
    return combine_subscores(legs, SMILE_LEG_WEIGHTS)
