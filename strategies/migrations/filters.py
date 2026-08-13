"""
strategies/migrations/filters.py

Owner: Platform / Architecture (A93)
Run: PYTHONPATH=$PWD .venv/bin/python -m strategies.migrations.filters [--dry-run]

Seeds filter_registry with the filter concepts that currently exist as three
independent declarations:

  * scripts/run_momentum_filter_overlays.py  -- MomentumBacktester kwargs
  * scripts/run_technical_filter_overlays.py -- FILTERS dict of CLI/job fields
  * backtest/adapters/fundamental_adapter.py -- _PRESETS_NEEDING_LIQUIDITY_FLOOR

Collapsing them exposed three places where the same NAME means different
RULES, which is the substantive finding of this migration and the reason the
registry is worth having. These are recorded as `divergence` notes on the
rows rather than silently resolved -- picking a winner is a strategy decision,
not a refactor:

1. circuit_lock_proxy: momentum uses circuit_band_pct=0.20, technical 0.19.
   The same proxy calibrated two ways, so a "circuit filter on" comparison
   across channels is not comparing the same filter.

2. regime_conditional: momentum disables buys in HIGH_VOL ("high_vol"),
   technical disables them in "bear". These are different regimes, not
   different spellings -- a strategy switched between channels silently
   changes which market states it sits out.

3. quality_gated: momentum gates on {min_f_score: 4, max_m_score: -1.78},
   technical on min_f_score only. The Beneish M-score half is simply absent
   from the technical path.

Defaults recorded here are the momentum values where the two disagree, on the
grounds that momentum's are the more conservative in each case (wider circuit
band, additional M-score gate) -- but the divergence note keeps the technical
value visible so the choice can be revisited rather than forgotten.

Each row names ONE implementation (invariant 2). Where an implementation does
not exist as a single shared function yet, implementation_ref points at the
function that should become it, and the row's description says so.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from strategies.registry import get_filter, register_filter

logger = logging.getLogger(__name__)

SOURCE_REF = (
    "scripts/run_momentum_filter_overlays.py + "
    "scripts/run_technical_filter_overlays.py + "
    "backtest/adapters/fundamental_adapter.py"
)

ALL_CHANNELS = ["momentum", "technical", "fundamental", "ml"]


def build_filters() -> List[Dict[str, Any]]:
    """The filter definitions to seed. Pure, so --dry-run can show them."""
    from config.settings import MAX_ORDER_VS_ADTV, MIN_ADTV_CR
    from systems.fundamental_analysis.quality.net_net import (
        LIQUIDITY_FLOOR_MARKET_CAP_CR,
    )

    return [
        {
            # A DIFFERENT CONCEPT from adtv_floor, despite both being called
            # "the liquidity floor" in conversation. ADTV is how much of a
            # stock trades; market cap is how big the company is. A large
            # company can be thinly traded and a small one heavily traded, so
            # collapsing the two would silently change which stocks three
            # fundamental presets are allowed to hold.
            "filter_id": "market_cap_floor",
            "name": "Size floor (market cap)",
            "description": (
                "Exclude tickers at or below a market-capitalisation floor. "
                "Applied by the fundamental adapter to small_cap_compounders, "
                "smile and under_followed -- the presets that hunt small "
                "companies and therefore need a floor under how small. "
                "A market cap of 0 or a missing lookup means 'not yet sourced', "
                "never 'genuinely tiny', and must not exclude."
            ),
            "filter_type": "universe",
            "params_schema": {
                "min_market_cap_cr": {
                    "type": "float",
                    "default": float(LIQUIDITY_FLOOR_MARKET_CAP_CR),
                    "min": 0.0,
                    "required": True,
                    "unit": "INR crore",
                }
            },
            "default_params": {
                "min_market_cap_cr": float(LIQUIDITY_FLOOR_MARKET_CAP_CR)
            },
            "applies_to_channels": ["fundamental"],
            "implementation_ref": (
                "backtest.adapters.fundamental_adapter.FundamentalAdapter"
            ),
            "status": "active",
        },
        {
            "filter_id": "adtv_floor",
            "name": "Liquidity floor (ADTV)",
            "description": (
                "Exclude tickers whose 20-day trailing average daily traded value "
                "is below the floor. Declared twice today: momentum passes "
                "min_adtv_cr with a volume_panel, technical passes --min-adtv-cr. "
                "NOT what fundamental applies -- see market_cap_floor, which is a "
                "size gate, not a liquidity one."
            ),
            "filter_type": "universe",
            "params_schema": {
                "min_adtv_cr": {
                    "type": "float",
                    "default": float(MIN_ADTV_CR),
                    "min": 0.0,
                    "required": True,
                    "unit": "INR crore",
                }
            },
            "default_params": {"min_adtv_cr": float(MIN_ADTV_CR)},
            "applies_to_channels": ALL_CHANNELS,
            "implementation_ref": "backtest.core.adtv.adtv_cr_for_ticker",
        },
        {
            "filter_id": "adtv_capped_sizing",
            "name": "Position size capped by ADTV",
            "description": (
                "Cap an order at a fraction of the ticker's ADTV so the backtest "
                "cannot fill a position the real market could not absorb. "
                "Momentum-only today (max_pct_of_adtv); the other channels size "
                "without any liquidity ceiling at all."
            ),
            "filter_type": "sizing",
            "params_schema": {
                "max_pct_of_adtv": {
                    "type": "float",
                    "default": float(MAX_ORDER_VS_ADTV),
                    "min": 0.0,
                    "max": 1.0,
                    "required": True,
                }
            },
            "default_params": {"max_pct_of_adtv": float(MAX_ORDER_VS_ADTV)},
            "applies_to_channels": ALL_CHANNELS,
            "implementation_ref": "backtest.momentum_backtest.MomentumBacktester",
            "divergence": (
                "Momentum-only. Not a naming difference -- Technical, Fundamental "
                "and ML apply no ADTV ceiling to position size, so their fills "
                "can exceed what the market would absorb."
            ),
        },
        {
            "filter_id": "circuit_lock",
            "name": "Circuit-locked bar: refuse the fill",
            "description": (
                "A bar whose high == low on a session that traded is a real "
                "price-band lock: the order could not have been filled. Distinct "
                "from the close-to-close circuit_band_pct proxy, which infers a "
                "lock from a large move."
            ),
            "filter_type": "entry",
            "params_schema": {
                "block_circuit_fills": {
                    "type": "bool",
                    "default": True,
                    "required": True,
                }
            },
            # A85 wants this on by default. The registry default says so even
            # though run_orchestrator_backtest.py still defaults it False --
            # that gap is the open half of A85.
            "default_params": {"block_circuit_fills": True},
            "applies_to_channels": ALL_CHANNELS,
            "implementation_ref": "backtest.trade_filters.is_circuit_locked",
            "divergence": (
                "Registry default is True (A85's target). "
                "run_orchestrator_backtest.py still defaults block_circuit_fills "
                "to False, and circuit_band_pct was None in all 195 unconstrained "
                "Technical runs -- so no run to date has excluded a locked fill."
            ),
        },
        {
            "filter_id": "circuit_lock_proxy",
            "name": "Circuit-lock proxy (close-to-close)",
            "description": (
                "Infer a lock from a close-to-close move at or beyond the band. "
                "Weaker than circuit_lock (a +-20% move is ambiguous; high == low "
                "on a traded session is not), retained because it is what the "
                "existing overlay sweeps measured."
            ),
            "filter_type": "entry",
            "params_schema": {
                "circuit_band_pct": {
                    "type": "float",
                    "default": 0.20,
                    "min": 0.0,
                    "max": 1.0,
                    "required": True,
                }
            },
            "default_params": {"circuit_band_pct": 0.20},
            "applies_to_channels": ALL_CHANNELS,
            "implementation_ref": "features.momentum_strategy.is_circuit_locked",
            "divergence": (
                "SAME NAME, DIFFERENT CALIBRATION: momentum uses 0.20, technical "
                "0.19. A cross-channel 'circuit filter on' comparison is "
                "therefore not comparing the same filter. 0.20 recorded as the "
                "default; the technical value needs an explicit decision."
            ),
        },
        {
            "filter_id": "chronic_circuit_locker",
            "name": "Chronic circuit locker: withhold the ticker",
            "description": (
                "Withhold a ticker whose locked-bar rate over a sufficient "
                "history is above the threshold -- 'no price from this security "
                "is trustworthy', a judgement about the ticker rather than a "
                "fact about a date. Deliberately NOT 'exclude anything that ever "
                "locked': 2,134 of 3,159 tickers with real history have locked at "
                "least once, so that rule discards 68% of the universe and "
                "preferentially the high-momentum names the screens exist to find."
            ),
            "filter_type": "universe",
            "params_schema": {
                "threshold_pct": {"type": "float", "default": None, "min": 0.0, "max": 100.0},
                "min_bars": {"type": "int", "default": None, "min": 1},
            },
            "default_params": {},
            "applies_to_channels": ALL_CHANNELS,
            "implementation_ref": "backtest.trade_filters.chronic_circuit_tickers",
        },
        {
            "filter_id": "downtrend_filter",
            "name": "Downtrend filter",
            "description": (
                "Skip buys when the index is a given percentage below its "
                "trailing high over the lookback window. The one filter concept "
                "whose parameters already agree across momentum and technical."
            ),
            "filter_type": "entry",
            "params_schema": {
                "downtrend_filter_pct": {
                    "type": "float",
                    "default": 0.05,
                    "min": 0.0,
                    "max": 1.0,
                    "required": True,
                },
                "downtrend_lookback_days": {
                    "type": "int",
                    "default": 20,
                    "min": 1,
                    "required": True,
                },
            },
            "default_params": {
                "downtrend_filter_pct": 0.05,
                "downtrend_lookback_days": 20,
            },
            "applies_to_channels": ALL_CHANNELS,
            "implementation_ref": "features.momentum_strategy.is_regime_disabled",
        },
        {
            "filter_id": "hmm_regime",
            "name": "Regime-conditional buys",
            "description": (
                "Disable new buys while the market is in one of the named "
                "regimes. Regimes come from features/regime_signal.py."
            ),
            "filter_type": "entry",
            "params_schema": {
                "disable_in_regimes": {
                    "type": "list",
                    "default": ["high_vol"],
                    "required": True,
                }
            },
            "default_params": {"disable_in_regimes": ["high_vol"]},
            "applies_to_channels": ALL_CHANNELS,
            "implementation_ref": "features.regime_signal.regime_series",
            "divergence": (
                "SAME NAME, DIFFERENT RULE: momentum disables buys in HIGH_VOL "
                "('high_vol'), technical in 'bear'. These are different market "
                "states, not different spellings -- moving a strategy between "
                "channels silently changes which conditions it sits out. Needs a "
                "decision, not a merge."
            ),
        },
        {
            "filter_id": "quality_gate",
            "name": "Fundamental quality gate",
            "description": (
                "Require a minimum Piotroski F-score and (momentum only) a "
                "Beneish M-score below the manipulation threshold."
            ),
            "filter_type": "entry",
            "params_schema": {
                "min_f_score": {"type": "int", "default": 4, "min": 0, "max": 9},
                "max_m_score": {"type": "float", "default": -1.78},
            },
            "default_params": {"min_f_score": 4, "max_m_score": -1.78},
            "applies_to_channels": ALL_CHANNELS,
            "implementation_ref": "features.momentum_strategy.passes_quality_gate",
            "divergence": (
                "SAME NAME, DIFFERENT STRICTNESS: momentum gates on both "
                "{min_f_score: 4, max_m_score: -1.78}; technical passes only "
                "quality_gate_min_f_score, so the Beneish M-score half is absent "
                "from the technical path entirely."
            ),
        },
        {
            "filter_id": "size_beta_orthogonalized",
            "name": "Size/beta orthogonalization",
            "description": (
                "Neutralise the momentum score against market cap and beta so "
                "the strategy is not just buying small, high-beta names."
            ),
            "filter_type": "entry",
            "params_schema": {
                "orthogonalize_vs_size_beta": {
                    "type": "bool",
                    "default": True,
                    "required": True,
                }
            },
            "default_params": {"orthogonalize_vs_size_beta": True},
            "applies_to_channels": ["momentum"],
            "implementation_ref": "features.momentum_strategy.build_category_presets",
            "divergence": (
                "Genuinely momentum-only -- it operates on a momentum score, so "
                "there is nothing for it to neutralise in the other channels. "
                "Recorded to distinguish 'only exists here for a reason' from "
                "'missing elsewhere by accident'."
            ),
        },
        {
            "filter_id": "data_blackout",
            "name": "Data blackout: force-close the position",
            "description": (
                "Force-close a position held through a run of sessions with no "
                "price data, rather than marking it to a stale price."
            ),
            "filter_type": "exit",
            "params_schema": {
                "max_blackout_sessions": {"type": "int", "default": None, "min": 1}
            },
            "default_params": {},
            "applies_to_channels": ALL_CHANNELS,
            "implementation_ref": "backtest.trade_filters.has_blackout",
        },
    ]


def migrate(
    *,
    db_path: Optional[Path] = None,
    dry_run: bool = False,
    created_by: str = "A93",
) -> Dict[str, int]:
    """Register any filter not already present. Idempotent.

    Existing rows are left alone rather than revised: a filter's parameters
    are a live calibration decision, and silently rewriting them from this
    file would undo a deliberate change made through the registry.
    """
    stats = {"registered": 0, "existing": 0}
    for spec in build_filters():
        spec = dict(spec)
        divergence = spec.pop("divergence", None)
        if divergence:
            spec["description"] = f"{spec['description']}\n\nDIVERGENCE: {divergence}"

        if get_filter(spec["filter_id"], db_path=db_path) is not None:
            stats["existing"] += 1
            continue
        if not dry_run:
            register_filter(
                db_path=db_path, created_by=created_by, source_ref=SOURCE_REF, **spec
            )
        stats["registered"] += 1
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--db-path", type=Path, default=None)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    stats = migrate(db_path=args.db_path, dry_run=args.dry_run)
    logger.info(
        "%sregistered=%d existing=%d",
        "[dry-run] " if args.dry_run else "",
        stats["registered"],
        stats["existing"],
    )
    for spec in build_filters():
        if spec.get("divergence"):
            logger.warning("%s: %s", spec["filter_id"], spec["divergence"])


if __name__ == "__main__":
    main()
