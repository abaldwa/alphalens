"""
backtest/strategy_id.py

Owner: Platform / Backtest
Consumers: backtest/run_orchestrator_backtest.py, backtest/
run_strategy_queue.py, datastore/api/routers/backtest_runs.py, the
Backtest page's trigger panels

Codifies `strategy_id` into one standard, parseable format instead of an
ad-hoc free-text label an operator has to invent every time — user
request: "codify the Strategy Id in some standard format for us to get
the date [back out]."

Format
------
    {channel}_{descriptor}_{horizon_code}_{YYYYMMDD}

    channel     : "ta" (technical) | "fund" (fundamental) | "mom" (momentum)
    descriptor  : channel-specific, always lowercase, no underscores of
                  its own collapsed oddly — a screener template name
                  ("e2"), a screener preset name ("garp"), or
                  "top{N}_{lookback}m" for momentum ("top10_6m")
    horizon_code: short form of HorizonBucket — see HORIZON_CODES
    YYYYMMDD    : the date this run was DEFINED/TRIGGERED (not the
                  backtest's start_date/end_date, which are separate
                  BacktestRun fields already) — this is what
                  parse_strategy_id() hands back as `run_date`, letting
                  a later query recover "when was this strategy
                  (re)triggered" straight from the id string alone,
                  without a DB round-trip.

Example: "ta_e2_21d_20260722", "fund_garp_63d_20260722",
"mom_top10_6m_21d_20260722".

This is a NAMING convention, not a uniqueness constraint: BacktestRun's
own `run_id` (a uuid) remains the actual primary key
(datastore/schema/create_backtest.py) — two runs of the identical
strategy on the identical day legitimately produce the same
strategy_id, same as re-running an unchanged cron job twice; that's
correct and expected, not a collision.
"""

import re
from dataclasses import dataclass
from datetime import date as date_type
from typing import Optional

from backtest.core.horizon import HorizonBucket

# Short codes for every HorizonBucket value — used in strategy_id and
# accepted as a --horizon-bucket shorthand (see run_orchestrator_backtest.py).
HORIZON_CODES = {
    HorizonBucket.D5: "5d",
    HorizonBucket.D21: "21d",
    HorizonBucket.D63: "63d",
    HorizonBucket.Y1: "1y",
    HorizonBucket.MULTIBAGGER: "mb",
    HorizonBucket.CUSTOM: "cust",
}
CODE_TO_HORIZON = {v: k for k, v in HORIZON_CODES.items()}

CHANNEL_PREFIX = {"technical": "ta", "fundamental": "fund", "momentum": "mom"}
PREFIX_TO_CHANNEL = {v: k for k, v in CHANNEL_PREFIX.items()}

_STRATEGY_ID_RE = re.compile(
    r"^(?P<channel>[a-z]+)_(?P<descriptor>[a-z0-9_]+)_(?P<horizon>5d|21d|63d|1y|mb|cust)_(?P<date>\d{8})$"
)

# ---------------------------------------------------------------------------
# Default horizon_bucket per strategy — user request #2: "default the
# number of days appropriately as what we have published in the Explainer
# document" (the Backtest Module Reference artifact's per-style table).
# ---------------------------------------------------------------------------

# From the Explainer's "Technical strategy reference" table, exactly as
# published: Mean Reversion -> 5-day, Momentum / Volatility -> 21-day,
# Trend Following -> 63-day.
STYLE_DEFAULT_HORIZON = {
    "Mean Reversion": HorizonBucket.D5,
    "Momentum": HorizonBucket.D21,
    "Volatility": HorizonBucket.D21,
    "Trend Following": HorizonBucket.D63,
}

# The Explainer doesn't cover Fundamental/Momentum (Technical-only), so
# these two are a reasoned EXTENSION of the same idea, not a republished
# figure — documented here rather than silently invented at call time.
# Fundamental screens move on quarterly filings, not daily price action:
# quality_compounder is a genuine buy-and-hold thesis (1 year), garp/
# turnaround need real time to play out but aren't multi-year holds (63d).
FUNDAMENTAL_PRESET_DEFAULT_HORIZON = {
    "quality_compounder": HorizonBucket.Y1,
    "garp": HorizonBucket.D63,
    "turnaround": HorizonBucket.D63,
}


def default_horizon_for_technical(template_style: str) -> HorizonBucket:
    """template_style: a systems.technical_analysis.screener.templates.TEMPLATE_STYLE value."""
    if template_style not in STYLE_DEFAULT_HORIZON:
        raise ValueError(f"unknown template style {template_style!r}; must be one of {list(STYLE_DEFAULT_HORIZON)}")
    return STYLE_DEFAULT_HORIZON[template_style]


def default_horizon_for_fundamental(preset: str) -> HorizonBucket:
    if preset not in FUNDAMENTAL_PRESET_DEFAULT_HORIZON:
        raise ValueError(f"unknown preset {preset!r}; must be one of {list(FUNDAMENTAL_PRESET_DEFAULT_HORIZON)}")
    return FUNDAMENTAL_PRESET_DEFAULT_HORIZON[preset]


def default_horizon_for_momentum(lookback_months: int) -> HorizonBucket:
    """A momentum rotation's natural holding period scales with its own
    lookback — a 1-3 month trailing-return signal decays over weeks
    (21-day), a 4-9 month signal over a quarter-ish (63-day), anything
    longer is a genuine annual-rebalance thesis (1-year). Our own
    reasoned default (the Explainer doesn't cover Momentum), not a
    republished figure."""
    if lookback_months <= 3:
        return HorizonBucket.D21
    if lookback_months <= 9:
        return HorizonBucket.D63
    return HorizonBucket.Y1


def _horizon_code(horizon_bucket: HorizonBucket) -> str:
    return HORIZON_CODES[horizon_bucket]


def build_strategy_id(
    channel: str, descriptor: str, horizon_bucket: HorizonBucket, as_of: Optional[date_type] = None,
) -> str:
    """
    Builds the canonical `{channel}_{descriptor}_{horizon_code}_{YYYYMMDD}`
    strategy_id (see module docstring).

    Parameters
    ----------
    channel : "technical" | "fundamental" | "momentum"
    descriptor : template name / preset name / momentum descriptor,
        lowercased and with any spaces replaced by underscores here (the
        caller does not need to pre-sanitize it).
    horizon_bucket : HorizonBucket
    as_of : defaults to today (UTC date) — the date embedded in the id.

    Raises
    ------
    ValueError
        If channel is not one of the three recognized channels.
    """
    if channel not in CHANNEL_PREFIX:
        raise ValueError(f"unknown channel {channel!r}; must be one of {list(CHANNEL_PREFIX)}")
    as_of = as_of or date_type.today()
    clean_descriptor = re.sub(r"[^a-z0-9_]+", "_", descriptor.lower()).strip("_")
    return f"{CHANNEL_PREFIX[channel]}_{clean_descriptor}_{_horizon_code(horizon_bucket)}_{as_of.strftime('%Y%m%d')}"


@dataclass(frozen=True)
class ParsedStrategyId:
    channel: str
    descriptor: str
    horizon_bucket: HorizonBucket
    run_date: date_type


def parse_strategy_id(strategy_id: str) -> ParsedStrategyId:
    """
    Recovers channel/descriptor/horizon_bucket/run_date from a
    canonical strategy_id built by build_strategy_id().

    Raises
    ------
    ValueError
        If `strategy_id` doesn't match the canonical format — e.g. an
        older free-text strategy_id predating this convention. Callers
        that need to tolerate both should catch ValueError, not assume
        every strategy_id in backtest_runs is canonical (pre-existing
        rows from before this convention are real data, not a bug).
    """
    m = _STRATEGY_ID_RE.match(strategy_id)
    if not m:
        raise ValueError(
            f"{strategy_id!r} is not a canonical strategy_id "
            f"({{channel}}_{{descriptor}}_{{horizon_code}}_{{YYYYMMDD}}) — cannot recover its date"
        )
    channel_prefix = m.group("channel")
    if channel_prefix not in PREFIX_TO_CHANNEL:
        raise ValueError(f"{strategy_id!r}: unrecognized channel prefix {channel_prefix!r}")
    return ParsedStrategyId(
        channel=PREFIX_TO_CHANNEL[channel_prefix],
        descriptor=m.group("descriptor"),
        horizon_bucket=CODE_TO_HORIZON[m.group("horizon")],
        run_date=date_type(int(m.group("date")[:4]), int(m.group("date")[4:6]), int(m.group("date")[6:8])),
    )
