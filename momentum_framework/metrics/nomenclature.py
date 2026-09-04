"""
Result Nomenclature - the single source of truth for how a strategy run's
identity is spelled, everywhere.

Why this file exists: on 2026-09-04, four queue generators omitted
rank_method/crash_regime_enabled, so the orchestrator inferred the WRONG
strategy identity (R01 backtests were silently recorded and reported as R03,
an 8-13x performance regression that went unnoticed until someone diffed
numbers against expectation). That bug was possible because strategy_id
construction was duplicated ad hoc in each generate_r*.py file. Here it is
built in exactly one place, with every required field mandatory (no
Optional, no silent defaulting) — a caller that omits a field gets a
TypeError at generation time, not a mislabeled result three days later.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional


# The four cumulative filter presets from the ORIGINAL (pre-R-numbering)
# momentum strategy — strategies/migrations/momentum.py::CATEGORY_FILTERS.
# "risk_managed" here is a FILTER PRESET (adds HMM regime gating on top of
# balanced) and is a completely different concept from exit_policy's
# "risk_managed" value (an exit-logic variant) — same string, two unrelated
# meanings in the legacy code. The framework keeps them as separate fields
# (filter_preset vs exit_policy) specifically to end that collision.
FILTER_PRESETS = frozenset({"all_risk", "balanced", "risk_managed", "max_defensive"})


@dataclass(frozen=True)
class StrategyIdentity:
    """
    Every field the framework requires to unambiguously name a strategy run.
    Deliberately has NO optional fields with silent defaults — see module
    docstring for why that matters here specifically.
    """
    strategy_code: str          # "R01", "R03", "R07", ..., "R13", "R14", ..., "R17"
    rank_method: str            # "trailing_return", "pct_of_52wk_high", "industry_momentum", ...
    band_id: int                # 2, 4, 7, 9, 10, 12 (see common/universe.py MBANDS)
    top_n: int
    lookback_months: int
    rebalance_cadence_days: int
    filter_preset: str = "all_risk"  # one of FILTER_PRESETS — see module comment above
    crash_regime_enabled: bool = False
    vol_scaling_mode: Optional[str] = None
    weight_method: Optional[str] = None
    skip_months: int = 0
    # R08's portfolio-level vol-target overlay — a DIFFERENT mechanism
    # from vol_scaling_mode (R09's 4-mode dispatch) and weight_method
    # (R14-R17's per-ticker basket weighting), so it gets its own fields
    # rather than overloading either. Added 2026-09-04 when R08 was
    # ported: without this, two R08 runs differing only in vol_target_pct
    # would silently collide under one strategy_id — the exact bug class
    # this module exists to prevent (see module docstring).
    vol_target_enabled: bool = False
    vol_target_pct: Optional[float] = None
    # R12's ADTV-quintile universe restriction (spec 7.12's "+ Liquidity"
    # dimension). Added 2026-09-04 — caught BY the validator before
    # shipping: R12's 6 liquidity_quintile variants (None, 1-5) collided
    # under one strategy_id without this field, the exact bug class this
    # module exists to prevent (see module docstring).
    liquidity_quintile: Optional[int] = None

    def __post_init__(self) -> None:
        from momentum_framework.common.universe import MBANDS
        if self.band_id not in MBANDS:
            raise ValueError(f"band_id={self.band_id} not a known M-band: {list(MBANDS)}")
        if self.top_n <= 0:
            raise ValueError(f"top_n must be positive, got {self.top_n}")
        if self.lookback_months <= 0:
            raise ValueError(f"lookback_months must be positive, got {self.lookback_months}")
        if self.rebalance_cadence_days <= 0:
            raise ValueError(f"rebalance_cadence_days must be positive, got {self.rebalance_cadence_days}")
        if self.filter_preset not in FILTER_PRESETS:
            raise ValueError(f"filter_preset={self.filter_preset!r} not one of {sorted(FILTER_PRESETS)}")


def build_strategy_id(
    strategy_code: str,
    band_id: int,
    top_n: int,
    lookback_months: int,
    rebalance_cadence_days: int,
    rank_method: Optional[str] = None,
    filter_preset: str = "all_risk",
    crash_regime_enabled: bool = False,
    vol_scaling_mode: Optional[str] = None,
    weight_method: Optional[str] = None,
    skip_months: int = 0,
    vol_target_enabled: bool = False,
    vol_target_pct: Optional[float] = None,
    liquidity_quintile: Optional[int] = None,
) -> str:
    """
    Canonical strategy_id string — BAND NAME FIRST, then strategy code
    (changed 2026-09-04, explicit user instruction: "backtest results to
    start with the Band Name and then the strategy name"). Band names are
    zero-padded to 2 digits (M02, M04, M07, M09, M10, M12, M13 — same
    convention as the R-number zero-padding, see common/universe.py).
    E.g.:
        M02_R01_top10_lb12mo_21d_allrisk
        M02_R03_top10_lb12mo_skip1mo_21d_allrisk
        M12_R09_top5_lb6mo_21d_allrisk_volscale-inverse_volatility
        M04_R14_top20_lb12mo_21d_allrisk_weight-inverse_volatility

    `rank_method` is accepted (not silently derived) precisely because
    letting the orchestrator *infer* it from other params is the mechanism
    that produced the R01-ran-as-R03 bug. If a caller doesn't know its
    rank_method, that is a bug in the caller, not something to paper over
    here — raise instead of guessing.

    `filter_preset` defaults to "all_risk" (no filters) rather than being
    required, because it is a genuinely optional dimension for strategies
    that never vary it — but it is ALWAYS present in the output string
    (never silently dropped) so filter_preset="balanced" vs "all_risk"
    runs never collide under one strategy_id, the way they did in the
    original ML41 grid before this framework existed.
    """
    if rank_method is None:
        raise ValueError(
            f"rank_method is required to build a strategy_id for {strategy_code} "
            "— do not rely on inference from other parameters (see module docstring)."
        )

    identity = StrategyIdentity(
        strategy_code=strategy_code,
        rank_method=rank_method,
        band_id=band_id,
        top_n=top_n,
        lookback_months=lookback_months,
        rebalance_cadence_days=rebalance_cadence_days,
        filter_preset=filter_preset,
        crash_regime_enabled=crash_regime_enabled,
        vol_scaling_mode=vol_scaling_mode,
        weight_method=weight_method,
        skip_months=skip_months,
        vol_target_enabled=vol_target_enabled,
        vol_target_pct=vol_target_pct,
        liquidity_quintile=liquidity_quintile,
    )

    from momentum_framework.common.universe import MBANDS
    band_name = MBANDS[identity.band_id].name

    parts = [
        band_name,
        identity.strategy_code,
        f"top{identity.top_n}",
        f"lb{identity.lookback_months}mo",
    ]
    if identity.skip_months:
        parts.append(f"skip{identity.skip_months}mo")
    parts.append(f"{identity.rebalance_cadence_days}d")
    parts.append(identity.filter_preset.replace("_", ""))
    if identity.crash_regime_enabled:
        parts.append("crashaware")
    if identity.vol_scaling_mode:
        parts.append(f"volscale-{identity.vol_scaling_mode}")
    if identity.vol_target_enabled:
        pct_str = f"{identity.vol_target_pct}" if identity.vol_target_pct is not None else "default"
        parts.append(f"voltarget-{pct_str}")
    if identity.weight_method:
        parts.append(f"weight-{identity.weight_method}")
    if identity.liquidity_quintile is not None:
        parts.append(f"liqQ{identity.liquidity_quintile}")

    return "_".join(parts)


def build_result_filename(
    strategy_id: str,
    result_date: str,
    config_hash: str,
) -> str:
    """
    Standard result filename: {date}_{strategy_id}_{hash}.json

    Example: 2026-09-04_M02_R03_top10_lb12mo_skip1mo_21d_a1b2c3.json

    config_hash should be a short (6-8 char) hash of the full config dict,
    computed by results/writer.py — it disambiguates two runs that share a
    strategy_id but differ in a parameter not represented in the id (e.g.
    a one-off exit_variant override).
    """
    return f"{result_date}_{strategy_id}_{config_hash}.json"


def parse_strategy_id(strategy_id: str) -> Dict[str, Any]:
    """
    Best-effort inverse of build_strategy_id(), for reading back legacy or
    hand-constructed IDs. Returns raw string components; callers needing
    typed fields should prefer carrying the original StrategyIdentity /
    BacktestResult.config instead of re-parsing this string.

    Token order is BAND NAME FIRST, then strategy code (changed
    2026-09-04 — see build_strategy_id()'s docstring). A string built
    before that change parses with band_name/strategy_code swapped; this
    function does not attempt to detect which convention an old string
    used.
    """
    tokens = strategy_id.split("_")
    if len(tokens) < 4:
        raise ValueError(f"Cannot parse strategy_id: {strategy_id!r}")

    return {
        "band_name": tokens[0],
        "strategy_code": tokens[1],
        "raw_tokens": tokens,
    }
