"""
scripts/run_band_best_hmm_regime_sweep.py

Quick sweep: add per-ticker HMM regime filter (exclude bearish) to each
band's current best variant and measure impact on negative FYs vs overall CAGR.
"""

import logging
import sys
from typing import Any, Dict, List
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from backtest.core.metrics import cagr

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# [2026-08-19] STALE BAND NUMBERING -- deliberately NOT renumbered.
# RANK_BANDS was renumbered to a contiguous 1-7 on this date, but the configs
# below were selected by the 2026-08-05 sweep, which ran against the OLD
# tables: ids 8/6/7 there meant ranks 201-250 / 251-500 / 501-800. Only
# 501-800 survives unchanged (it is band 7 today); 201-250 and 251-500 were
# replaced by the 201-300 / 301-500 partition, so there is no id that means
# the same universe those two rows were measured on. Renumbering the keys
# would silently attach an empirical finding to a universe it was never
# measured against. The inline `_bands` literal below is kept for the same
# reason: it is what these results correspond to. Re-run the band sweep on
# the current RANK_BANDS before trusting these for bands 5-7.
# Current band-best configs (from the 2026-08-05 report)
BAND_BEST = {
    1: dict(strategy="max_defensive", lookback_months=9, rebalance="bimonthly", top_n=15),
    2: dict(strategy="all_risk", lookback_months=3, rebalance="quarterly", top_n=15),
    3: dict(strategy="balanced", lookback_months=6, rebalance="bimonthly", top_n=10),
    4: dict(strategy="all_risk", lookback_months=3, rebalance="quarterly", top_n=15),
    6: dict(strategy="risk_managed", lookback_months=9, rebalance="quarterly", top_n=20),
    7: dict(strategy="max_defensive", lookback_months=6, rebalance="bimonthly", top_n=10),
    8: dict(strategy="balanced", lookback_months=9, rebalance="quarterly", top_n=10),
}

# HMM regime disable options to test
# 0.0 = bearish, 1.0 = sideways, 2.0 = bullish
HMM_DISABLE_OPTIONS = [
    {0.0},           # bearish only
    {0.0, 1.0},      # bearish + sideways
]


def _fy_cagrs(equity_curve: Any) -> Dict[str, float]:
    buckets: Dict[str, List[Any]] = {}
    for pt in equity_curve:
        d = pd.Timestamp(pt["date"])
        fy_year = d.year if d.month >= 4 else d.year - 1
        fy = f"FY{fy_year}-{str(fy_year + 1)[-2:]}"
        buckets.setdefault(fy, []).append(pt)
    out = {}
    for fy, pts in buckets.items():
        if len(pts) >= 2:
            first, last = pts[0], pts[-1]
            out[fy] = cagr(first["total_value"], last["total_value"], first["date"], last["date"])
    return out


# [H4, 2026-08-18] _run_variant deleted: it built a MomentumBacktester with
# per_ticker_hmm_regime, deprecated by §19, and main() below now raises
# before ever calling it -- see main()'s docstring/comment.


def main() -> None:
    # [H4, 2026-08-18] Per-ticker HMM regime (per_ticker_hmm_regime/
    # disable_hmm_regimes) no longer exists on MomentumAdapter -- deprecated
    # by the 2026-08-18 user decision (§19: pure-play momentum is a plain
    # rank rotation). This script's entire purpose was to measure that
    # filter's impact, so there is nothing left to compute.
    raise NotImplementedError(
        "per_ticker_hmm_regime/disable_hmm_regimes no longer exist on MomentumAdapter "
        "(deprecated 2026-08-18, UnifiedGeneratorRefactorPlan.md §19) -- this sweep has no "
        "meaning to compute."
    )


if __name__ == "__main__":
    main()
