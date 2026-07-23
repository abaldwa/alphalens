"""
features/fundamental_composites.py

Phase: 3.x (Fundamental Analysis API Scaffolding)
Specs: SPEC-FA-008
Owner: Platform / Features
Consumers: datastore/api/routers/fundamentals.py

The 30 raw fundamental ratios (27 sector-relative z-scored per SPEC-FEAT-002,
features/fundamental.py) and 12 governance features (features/governance.py)
are already computed daily and merged into the same feature Parquet
features/matrix_builder.py writes (config.settings.FEATURES_DAILY_DIR) — see
datastore/api/routers/technical.py's docstring for the equivalent TA story.

What's genuinely missing — confirmed during 2026-07-01 planning — is the
small set of composite scores (quality/growth/management) and peer-ranking
logic that was never built in a dedicated module. These functions are intentionally
small (combine already-computed values, no new raw data ingestion) and are
called at API-request time, not persisted as new feature columns — there is
no new ground-truth data here, just documented arithmetic over real inputs.

quality_score/growth_score operate on the sector-relative z-scored ratios
already in the feature Parquet (the matrix has no raw, un-z-scored ratio
columns — see features/fundamental.py's compute_fundamental_features_panel),
so a positive score means "better than sector peers," not an absolute
threshold like "ROE > 15%". management_quality_score operates on raw
governance fields (not z-scored) since features/governance.py never
z-scores them.
"""

from typing import Dict, List, Optional

import numpy as np
import pandas as pd

# Weights are documented, not tuned/backtested — same standing as the
# documented-but-not-backtested weights already in this codebase's other
# composite scores (e.g. forensic_classical.py's 20/40/20/20 split,
# justified in that file's own docstring as a starting point subject to
# revision once enough labeled outcomes exist).
QUALITY_WEIGHTS = {"roe": 0.30, "roce": 0.30, "net_margin": 0.20, "debt_to_equity": -0.20}
GROWTH_WEIGHTS = {"revenue_growth_yoy": 0.30, "eps_growth_yoy": 0.30, "revenue_cagr_3yr": 0.40}


def _weighted_zscore_composite(ratios: Dict[str, float], weights: Dict[str, float]) -> Optional[float]:
    """
    Weighted sum of sector-relative z-scores, renormalized over whichever
    inputs are actually non-NaN, mapped onto a 0-100 display scale via
    `50 + 10 * weighted_z` (z is clipped to [-5, 5] upstream by
    features.fundamental.Z_SCORE_CLIP, so this lands in roughly [0, 100]
    before the final clip) — same display-scale convention
    forensic_classical.py already uses for its 0-100 composite.

    Returns None if every weighted input is NaN (e.g. a brand-new listing
    with no PIT-eligible fundamentals yet).
    """
    total_weight = 0.0
    weighted_sum = 0.0
    for col, w in weights.items():
        v = ratios.get(col)
        if v is None or (isinstance(v, float) and np.isnan(v)):
            continue
        weighted_sum += w * v
        total_weight += abs(w)
    if total_weight == 0:
        return None
    weighted_z = weighted_sum / total_weight
    return float(np.clip(50 + 10 * weighted_z, 0, 100))


def quality_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: sector-relative ROE/ROCE/margin (higher=better) vs. leverage (lower=better)."""
    return _weighted_zscore_composite(ratios, QUALITY_WEIGHTS)


def growth_score(ratios: Dict[str, float]) -> Optional[float]:
    """0-100: sector-relative revenue/EPS growth and 3yr CAGR."""
    return _weighted_zscore_composite(ratios, GROWTH_WEIGHTS)


def management_quality_score(governance: Dict[str, float]) -> Optional[float]:
    """
    0-100, built from raw (non-z-scored) governance fields:
    start at 50 (neutral), -0.5 point per 1% promoter pledge (lower pledge
    is better), -20 if promoter_pledge_spiral_flag is set, +15 if
    institutional_conviction_flag is set (FII+DII+MF all increasing qoq —
    see features/governance.py). Returns None if promoter_pledge is
    entirely missing (no shareholding data yet).
    """
    pledge = governance.get("promoter_pledge")
    if pledge is None or (isinstance(pledge, float) and np.isnan(pledge)):
        return None
    score = 50.0 - 0.5 * pledge
    if governance.get("promoter_pledge_spiral_flag"):
        score -= 20.0
    if governance.get("institutional_conviction_flag"):
        score += 15.0
    return float(np.clip(score, 0, 100))


def select_peers(
    ticker: str, panel: pd.DataFrame, sector_map: Dict[str, str], mcap_map: Dict[str, float], k: int = 5
) -> List[str]:
    """
    Real peer-selection logic (was previously unimplemented): same sector,
    ranked by closeness in log(market_cap), top k excluding the ticker
    itself. `panel` is the day's fundamental feature rows (ticker column
    must be present) — only tickers that actually have a row in the panel
    can be peers, so the result only ever names tickers with real data.

    [2026-07-02 fix] config/build_universe.py currently hardcodes
    market_cap_cr=0 for the entire universe (NSE's free archives don't
    publish bulk market cap, and no other source is wired in yet — see
    that module's docstring). The original version of this function
    required own_mcap > 0, which meant peers could never be returned for
    any ticker while that gap exists. Falls back to sector-only selection
    (alphabetical, for determinism — no fabricated market-cap ranking)
    whenever market cap is unavailable for the ticker or its candidates.
    """
    sector = sector_map.get(ticker)
    if sector is None:
        return []
    own_mcap = mcap_map.get(ticker)
    have_mcap = own_mcap is not None and own_mcap > 0
    candidates = [
        t for t in panel["ticker"]
        if t != ticker and sector_map.get(t) == sector
    ]
    if not candidates:
        return []
    if have_mcap:
        mcap_candidates = [t for t in candidates if mcap_map.get(t, 0) > 0]
        if mcap_candidates:
            own_log_mcap = np.log(own_mcap)
            mcap_candidates.sort(key=lambda t: abs(np.log(mcap_map[t]) - own_log_mcap))
            return mcap_candidates[:k]
    # No usable market-cap data (own or peers') — fall back to a
    # deterministic sector-only ordering rather than returning nothing.
    return sorted(candidates)[:k]


# Screener presets operate on sector-relative z-scores (the only ratio
# representation the feature Parquet carries) — "quality compounder" means
# "above sector peers on these dimensions," not an absolute % threshold.
SCREENER_PRESETS = {
    "quality_compounder": {"roe": 1.0, "roce": 1.0, "debt_to_equity": -0.5},  # min z-score per column (sign-adjusted)
    "garp": {"revenue_growth_yoy": 0.5, "pe_ratio": -0.5},  # growth above peers, valuation below peers
    "turnaround": {"revenue_growth_yoy": 1.0, "eps_growth_yoy": 1.0},  # strong recent acceleration vs peers
}


def matches_screener_preset(ratios: Dict[str, float], preset: str) -> bool:
    """True if every z-scored ratio in SCREENER_PRESETS[preset] clears its
    threshold (thresholds already sign-adjusted so 'pass' always means
    `value >= threshold`, e.g. pe_ratio's -0.5 means 'at least half a
    sector-std cheaper than peers'). Missing inputs fail the screen
    (conservative — never include a ticker on incomplete data)."""
    if preset not in SCREENER_PRESETS:
        raise ValueError(f"Unknown screener preset: {preset}")
    for col, threshold in SCREENER_PRESETS[preset].items():
        v = ratios.get(col)
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return False
        signed_v = -v if threshold < 0 else v
        signed_threshold = abs(threshold)
        if signed_v < signed_threshold:
            return False
    return True
