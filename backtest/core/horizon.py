"""
backtest/core/horizon.py

Phase: Unified Backtest & Paper Trading Umbrella, Phase 1
Owner: Platform / Backtest
Consumers: backtest/core/portfolio.py (position sizing), every channel
adapter (backtest/adapters/*.py, once built), backtest/core/engine.py

HorizonBucket: the strategy-classification taxonomy the user asked to
standardize across Technical, Fundamental, ML, and Momentum — "5 days,
21 Days, 63 days, 1 Year, MultiBagger etc." Every strategy a channel runs
must declare exactly one bucket; the bucket (not the channel) drives
default position sizing and rebalance cadence, per the user's explicit
"standard way of doing... Position tracking" requirement
(BacktestUmbrellaPlan.md "Standardized position sizing").

Percentages below are the plan's documented starter defaults
(BacktestUmbrellaPlan.md, "Standardized position sizing (horizon.py)")
pending final user sign-off in Phase 0 — they are read from this module
as overridable config, never hardcoded downstream, so a later sign-off
only requires editing HORIZON_SIZING here.
"""

from dataclasses import dataclass
from enum import Enum


class HorizonBucket(str, Enum):
    D5 = "5_day"
    D21 = "21_day"
    D63 = "63_day"
    Y1 = "1_year"
    MULTIBAGGER = "multibagger"
    CUSTOM = "custom"


@dataclass(frozen=True)
class HorizonSizingPolicy:
    horizon_bucket: HorizonBucket
    max_position_pct: float  # fraction of strategy capital in a single stock pick
    max_sector_pct: float  # fraction of strategy capital in a single sector
    default_rebalance_cadence_days: int  # trading days between rebalances
    min_holding_days: int  # floor on holding period before a non-exit-signal sell is allowed


# Defaults per BacktestUmbrellaPlan.md's "Standardized position sizing" table.
# NOT final — flagged in the plan as needing explicit user starter-config sign-off (Phase 0).
HORIZON_SIZING = {
    HorizonBucket.D5: HorizonSizingPolicy(
        horizon_bucket=HorizonBucket.D5, max_position_pct=0.02, max_sector_pct=0.15,
        default_rebalance_cadence_days=5, min_holding_days=1,
    ),
    HorizonBucket.D21: HorizonSizingPolicy(
        horizon_bucket=HorizonBucket.D21, max_position_pct=0.03, max_sector_pct=0.20,
        default_rebalance_cadence_days=21, min_holding_days=5,
    ),
    HorizonBucket.D63: HorizonSizingPolicy(
        horizon_bucket=HorizonBucket.D63, max_position_pct=0.04, max_sector_pct=0.20,
        default_rebalance_cadence_days=63, min_holding_days=21,
    ),
    HorizonBucket.Y1: HorizonSizingPolicy(
        horizon_bucket=HorizonBucket.Y1, max_position_pct=0.05, max_sector_pct=0.25,
        default_rebalance_cadence_days=252, min_holding_days=63,
    ),
    HorizonBucket.MULTIBAGGER: HorizonSizingPolicy(
        horizon_bucket=HorizonBucket.MULTIBAGGER, max_position_pct=0.05, max_sector_pct=0.25,
        default_rebalance_cadence_days=63, min_holding_days=252,
    ),
}


def sizing_for(horizon_bucket: HorizonBucket, overrides: "dict | None" = None) -> HorizonSizingPolicy:
    """
    Look up the sizing policy for a bucket, with optional per-run overrides
    (e.g. a strategy operator tightening max_position_pct for a specific
    backtest run). CUSTOM has no default — overrides is required for it.
    """
    if horizon_bucket == HorizonBucket.CUSTOM:
        if not overrides:
            raise ValueError("HorizonBucket.CUSTOM requires explicit sizing overrides")
        return HorizonSizingPolicy(horizon_bucket=HorizonBucket.CUSTOM, **overrides)
    base = HORIZON_SIZING[horizon_bucket]
    if not overrides:
        return base
    return HorizonSizingPolicy(**{**base.__dict__, **overrides})
