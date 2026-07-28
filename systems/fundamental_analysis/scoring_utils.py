"""
systems/fundamental_analysis/scoring_utils.py

Shared weighted-z-score composite helper, used by both
features/fundamental_composites.py's original quality_score/growth_score
and this package's new quality_value/fcf_low_debt/magic_formula/garp
scores. Lives here (not in fundamental_composites.py) so that module can
import the new scores without an import cycle.
"""

from typing import Dict, Optional

import numpy as np


MIN_COVERAGE = 0.5
# [2026-07-25 fundamental-strategy-catalog model-review fix] Both
# weighted_zscore_composite and combine_subscores used to renormalize over
# whatever inputs were non-NaN with no floor — a 4-factor composite with 3
# of 4 inputs missing would return a full-precision 0-100 score built off
# 1 factor, indistinguishable from a score backed by complete data (ml-
# rigor-reviewer and skeptic-tester both flagged this as the top risk in
# the 26-strategy model review). MIN_COVERAGE requires at least half the
# total absolute weight to be backed by real data before returning a
# score at all; below that, returns None (same "no score" signal already
# used for "every input missing") rather than a misleadingly precise number.


def weighted_zscore_composite(ratios: Dict[str, float], weights: Dict[str, float]) -> Optional[float]:
    """
    Weighted sum of sector-relative z-scores, renormalized over whichever
    inputs are actually non-NaN, mapped onto a 0-100 display scale via
    `50 + 10 * weighted_z` (z is clipped to [-5, 5] upstream by
    features.fundamental.Z_SCORE_CLIP, so this lands in roughly [0, 100]
    before the final clip) — same display-scale convention
    forensic_classical.py already uses for its 0-100 composite.

    Returns None if every weighted input is NaN (e.g. a brand-new listing
    with no PIT-eligible fundamentals yet) OR if fewer than MIN_COVERAGE
    (50%) of the total absolute weight is backed by non-NaN data — a
    sparse-data ticker should show as "no score," not a full-confidence
    number derived from a small minority of its intended inputs.
    """
    total_weight = sum(abs(w) for w in weights.values())
    covered_weight = 0.0
    weighted_sum = 0.0
    for col, w in weights.items():
        v = ratios.get(col)
        if v is None or (isinstance(v, float) and np.isnan(v)):
            continue
        weighted_sum += w * v
        covered_weight += abs(w)
    # [BUG FIX, 4th fundamental-strategies review, item 5] strict `<` let a
    # leg at EXACTLY 50% coverage through — for the majority-shape 2-factor
    # equal-weighted composites in this catalog (garp.py, magic_formula.py,
    # promoter_aligned.py, recovery.py, etc.), that means 1-of-2 factors
    # present clears the floor and produces a full-confidence score
    # indistinguishable from a fully-covered one, defeating MIN_COVERAGE's
    # intent for exactly the shape it's meant to protect. `<=` makes exact-
    # 50% insufficient uniformly; a 3+-factor leg only lands on exactly 50%
    # for specific weight combinations (e.g. 2-of-4 equal-weighted), which
    # is a narrower, less common case than the 2-factor 1-of-2 pattern this
    # fix targets — not worth a per-factor-count carve-out.
    if total_weight == 0 or covered_weight == 0 or (covered_weight / total_weight) <= MIN_COVERAGE:
        return None
    weighted_z = weighted_sum / covered_weight
    return float(np.clip(50 + 10 * weighted_z, 0, 100))


def combine_subscores(scores: Dict[str, Optional[float]], weights: Dict[str, float]) -> Optional[float]:
    """
    Weighted average of already-0-100 sub-scores (e.g. QGLP's Quality/
    Growth/Longevity/Price legs, each itself a weighted_zscore_composite
    result) — renormalized over whichever legs are non-None, same
    missing-input handling as weighted_zscore_composite but without the
    z-score display-scale transform (inputs are already 0-100).

    Returns None if every weighted leg is None, OR if fewer than
    MIN_COVERAGE (50%) of the total absolute weight is backed by a real
    (non-None) leg — same rationale as weighted_zscore_composite.
    """
    total_weight = sum(abs(w) for w in weights.values())
    covered_weight = 0.0
    weighted_sum = 0.0
    for key, w in weights.items():
        v = scores.get(key)
        if v is None or (isinstance(v, float) and np.isnan(v)):
            continue
        weighted_sum += w * v
        covered_weight += abs(w)
    # [BUG FIX, 4th fundamental-strategies review, item 5] strict `<` let a
    # leg at EXACTLY 50% coverage through — for the majority-shape 2-factor
    # equal-weighted composites in this catalog (garp.py, magic_formula.py,
    # promoter_aligned.py, recovery.py, etc.), that means 1-of-2 factors
    # present clears the floor and produces a full-confidence score
    # indistinguishable from a fully-covered one, defeating MIN_COVERAGE's
    # intent for exactly the shape it's meant to protect. `<=` makes exact-
    # 50% insufficient uniformly; a 3+-factor leg only lands on exactly 50%
    # for specific weight combinations (e.g. 2-of-4 equal-weighted), which
    # is a narrower, less common case than the 2-factor 1-of-2 pattern this
    # fix targets — not worth a per-factor-count carve-out.
    if total_weight == 0 or covered_weight == 0 or (covered_weight / total_weight) <= MIN_COVERAGE:
        return None
    return float(np.clip(weighted_sum / covered_weight, 0, 100))
