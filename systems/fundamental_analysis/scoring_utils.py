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
#
# [BUG FIX, 5th fundamental-strategies review, item 7] the 4th review's
# `<=` fix (below) correctly closed the 2-factor/50%-coverage gap it
# targeted, but as a strict inequality it applies uniformly to EVERY leg
# shape — silently flipping previously-scoring 4-factor composites
# (quality_score/growth_score in features/fundamental_composites.py, both
# confirmed real consumers of this shared helper) at exactly 50% coverage
# (2-of-4 populated) to None too, contradicting this module's own
# documented "at least half" (inclusive) intent above. Coverage-floor
# strictness is made factor-count-aware instead: a 2-factor leg can only
# ever be at "half" via its single non-degenerate split (1-of-2), which is
# mathematically indistinguishable from "less than half" of the leg's
# informative content — `<=` (exact-50%-insufficient) is correct there.
# A 3+-factor leg's exact-50% split (e.g. 2-of-4) genuinely IS "at least
# half" of the leg's content in the sense the module's docstring promises,
# so it keeps the original inclusive `<` behavior.
_MIN_FACTORS_FOR_STRICT_50PCT = 2


def _coverage_insufficient(covered_weight: float, total_weight: float, n_factors: int) -> bool:
    if total_weight == 0 or covered_weight == 0:
        return True
    coverage = covered_weight / total_weight
    if n_factors <= _MIN_FACTORS_FOR_STRICT_50PCT:
        return coverage <= MIN_COVERAGE
    return coverage < MIN_COVERAGE


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
    # [BUG FIX, 5th fundamental-strategies review, item 7] factor-count-
    # aware coverage floor — see module-level note above _coverage_
    # insufficient for why a 2-factor leg's exact-50% split is treated
    # differently from a 3+-factor leg's.
    if _coverage_insufficient(covered_weight, total_weight, len(weights)):
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
    # [BUG FIX, 5th fundamental-strategies review, item 7] see the matching
    # note in weighted_zscore_composite / the module-level docstring above
    # _coverage_insufficient.
    if _coverage_insufficient(covered_weight, total_weight, len(weights)):
        return None
    return float(np.clip(weighted_sum / covered_weight, 0, 100))
