"""
strategies/momentum_identity.py

Phase: ML40-2.4 (momentum identity from the registry)
Owner: Platform / Architecture
Consumers: backtest/run_orchestrator_backtest.py::_momentum_descriptor

Resolves a momentum RUN's parameters to the name of the strategy_registry row
that declares it.

THE PROBLEM THIS REPLACES
-------------------------
Momentum had no named strategies, so the engine generated an identity string
from whichever parameters seemed to matter -- `top10_6m`, later widened to carry
the filters, grace_cycles and the exit variant. ML41 then declared the real grid
as 1,680 rows named

    {category}_b{band}_{rank_start}-{rank_end}_lb{N}mo_{rebalance}_top{N}

and the two never met: measured 2026-08-14, 10 of 93 ledger strategy_keys did not
resolve against the registry, and all 10 were momentum. The report's Definition
card showed nothing for them, they could not be deployed through A91, and their
signals sat under a key pointing at no row.

WHAT COUNTS AS IDENTITY (decided 2026-08-15)
--------------------------------------------
The declared strategy is the CATEGORY (which filter preset), the rank BAND, the
LOOKBACK, the REBALANCE cadence and TOP_N. Those decide which stocks are
selected, and each is a column of ML41's grid.

`grace_cycles` and `exit_variant` are deliberately NOT part of it. They are run
parameters. This is safe because strategy_signals' primary key is
(strategy_key, strategy_version, signal_date, ticker, source, run_id) -- run_id
is IN the key, so two runs of one strategy under different exit policies never
collide; they are told apart by run_id, joined to backtest_runs.

That is a narrower claim than the commit which first widened the key
(`e6c97160`). What that commit actually fixed was ATTRIBUTION READABILITY -- you
could not tell from the key alone which variant emitted a buy. The fix is still
needed for the FILTERS, and it survives here: filters map to the category, which
IS in the name. Only grace/exit move out, and those are recoverable from the run
row.

NO SILENT GUESSING
------------------
A parameter set that matches no declared row returns None rather than inventing
a name. The caller then falls back to the legacy descriptor, and the run is
visibly un-declared instead of being given a plausible-looking key that resolves
to nothing -- which was the original defect.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def _rebalance_label(cadence_days: Optional[int]) -> Optional[str]:
    """Map a rebalance cadence in trading days to ML41's label.

    Read from run_momentum_dynamic_report.REBALANCE_PERIODS rather than
    hardcoded, so adding a cadence there cannot silently stop resolving here.
    """
    if cadence_days is None:
        return None
    from scripts.run_momentum_dynamic_report import REBALANCE_PERIODS

    for label, days in REBALANCE_PERIODS.items():
        if days == cadence_days:
            return label
    return None


def _category_for_filters(
    *,
    min_adtv_cr: Optional[float],
    circuit_band_pct: Optional[float],
    quality_gate: Optional[Dict[str, Any]],
    disable_buys_in_regime: Optional[Any],
    orthogonalize_vs_size_beta: bool = False,
) -> Optional[str]:
    """Which of ML41's four cumulative presets a run's filter flags correspond to.

    The presets layer: all_risk (none) -> balanced (+liquidity floor, ADTV-capped
    sizing, circuit-lock proxy, quality gate) -> risk_managed (+regime gating) ->
    max_defensive (+size/beta orthogonalization).

    Matched from the OUTSIDE IN so the most specific preset wins; a run carrying
    orthogonalization is max_defensive regardless of what else it also sets.

    Returns None for a combination that is not one of the four -- an ad-hoc
    filter mix is a real thing to run, and it is not a declared strategy. Saying
    so is the point; guessing the nearest preset would attach a run's signals to
    a definition it did not use.
    """
    has_balanced = (
        min_adtv_cr is not None
        and circuit_band_pct is not None
        and bool(quality_gate)
    )
    has_regime = bool(disable_buys_in_regime)

    if orthogonalize_vs_size_beta:
        return "max_defensive" if (has_balanced and has_regime) else None
    if has_regime:
        return "risk_managed" if has_balanced else None
    if has_balanced:
        return "balanced"
    # Nothing set at all is the unfiltered baseline. A PARTIAL filter set is
    # not: it is neither all_risk nor balanced, and must not be rounded to one.
    if min_adtv_cr is None and circuit_band_pct is None and not quality_gate:
        return "all_risk"
    return None


def registry_name(
    *,
    rank_band_id: Optional[int],
    lookback_months: Optional[int],
    rebalance_cadence_days: Optional[int],
    top_n: Optional[int],
    min_adtv_cr: Optional[float] = None,
    circuit_band_pct: Optional[float] = None,
    quality_gate: Optional[Dict[str, Any]] = None,
    disable_buys_in_regime: Optional[Any] = None,
    orthogonalize_vs_size_beta: bool = False,
) -> Optional[str]:
    """The declared registry name for this run, or None if it declares none.

    None is returned rather than raised: an undeclared parameter combination is
    a legitimate thing to sweep, and the caller decides whether that is fatal.
    """
    if rank_band_id is None or lookback_months is None or top_n is None:
        return None

    category = _category_for_filters(
        min_adtv_cr=min_adtv_cr,
        circuit_band_pct=circuit_band_pct,
        quality_gate=quality_gate,
        disable_buys_in_regime=disable_buys_in_regime,
        orthogonalize_vs_size_beta=orthogonalize_vs_size_beta,
    )
    if category is None:
        return None

    rebalance = _rebalance_label(rebalance_cadence_days)
    if rebalance is None:
        return None

    from features.momentum_universe import RANK_BANDS

    band = next((b for b in RANK_BANDS if b[0] == rank_band_id), None)
    if band is None:
        return None
    _, rank_start, rank_end = band

    from strategies.migrations.momentum import variant_name

    # Built by ML41's OWN function rather than a second f-string here. Two
    # copies of a name format is how the generated key and the registry rows
    # came to disagree in the first place.
    return variant_name(
        category, rank_band_id, rank_start, rank_end, lookback_months, rebalance, top_n
    )
