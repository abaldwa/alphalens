"""
systems/fundamental_analysis/quality/owner_earnings.py

Buffett-style Owner Earnings Compounders. OWNER_EARNINGS_PROXY (=
operating_cash_flow - capex) reuses the existing `fcf` column as its
proxy, since this codebase's `cfo_proxy` was already defined as `fcf +
capex` — so OWNER_EARNINGS_YIELD is just fcf_ev_yield, reused rather than
recomputed. [2026-07-25 correction] This is an approximation, not an
algebraic identity: `fcf` is a raw value reported by the upstream data
source (Trendlyne/NSE XBRL — see ingestion/scrapers/screener.py, which
does NOT compute it itself), not derived in this codebase as `cfo -
capex`. If the source computed its "FCF" using a different capex figure,
period, or definition than this codebase's own `capex` column,
`cfo_proxy = fcf + capex` will diverge from the company's actually
reported operating cash flow. Treat `cfo_proxy`-derived features
(reinvestment_rate here; also used in Piotroski F2/F4 tests via
quality/piotroski_on_value.py) as a documented approximation, not ground truth.
reinvestment_rate (capex/cfo_proxy) stands in for the doc's
REINVESTMENT_QUALITY = roce * reinvestment_rate cross-term — this
codebase's composite convention is a linear weighted sum of z-scores, not
a product of two z-scores, so the two legs are weighted separately instead.
"""

from typing import Dict, Optional

from systems.fundamental_analysis.scoring_utils import weighted_zscore_composite

OWNER_EARNINGS_WEIGHTS = {
    "fcf_ev_yield": 0.50,
    "roce": 0.30,
    "reinvestment_rate": 0.20,
}


def owner_earnings_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: sector-relative owner-earnings yield + ROCE + reinvestment rate."""
    return weighted_zscore_composite(ratios, OWNER_EARNINGS_WEIGHTS)
