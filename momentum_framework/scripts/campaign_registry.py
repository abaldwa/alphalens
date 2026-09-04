"""
Campaign Strategy Registry — FULL GRID per strategy (rewritten 2026-09-04,
explicit user instruction after reviewing the Pass 1 legacy-vs-native
comparison: "I am looking at full run" + position sizing as a dimension
for every strategy, not just R14-R17).

Grid, per BASE strategy (R01, R03, R07, R08, R09, R10, R11, R12, R13):
  band_id            x  that strategy's own QueueGenerator.BANDS
  lookback_months    x  that strategy's own QueueGenerator.LOOKBACK_MONTHS
                        (fixed strategies — R11's 52wk window, R12's 1mo
                        reversal, R13's Bollinger window — sweep nothing
                        here; there is exactly one lookback, baked in)
  rebalance_cadence  x  REBALANCE_CADENCES = [5, 10, 21]
  position_sizing    x  ["equal", "inverse_volatility"]
  top_n              =  the smallest value in that band's TOP_N_BY_BAND
                         entry (unchanged scoping choice from the original
                         registry — top_n and filter_preset were NOT part
                         of the user's "full grid" request, only lookback/
                         cadence/sizing were)

position_sizing="inverse_volatility" is applied GENERICALLY by
StrategyBase.size_signals() (see strategies/base.py), called from
BacktestOrchestrator.run_native() right after rebalance() returns — it
does not require each strategy's own ranking logic to know about
weighting at all.

R14-R17 (StrategyBase.has_own_weighting=True) are NOT re-swept with a
position_sizing dimension here — they already ARE weighted variants (of
R01's trailing-return signal, each with a DIFFERENT scheme: inverse-
volatility/inverse-variance/target-volatility/downside-volatility) and
size themselves inline; layering the generic pass on top would silently
double-weight them. They keep their own existing band/lookback/cadence
grid, unchanged from before this rewrite.
"""

from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Tuple

from momentum_framework.backtesting.adapter import StrategyAdapter
from momentum_framework.common.universe import TOP_N_BY_BAND
from momentum_framework.strategies.r01_trailing_momentum import R01TrailingMomentum
from momentum_framework.strategies.r03_jt_skipmonth import R03JTSkipMonth
from momentum_framework.strategies.r07_crash_aware import R07CrashAware
from momentum_framework.strategies.r08_bsc_volscale import R08BSCVolScale
from momentum_framework.strategies.r09_mm_volscale import R09MMVolScale
from momentum_framework.strategies.r10_sector_momentum import R10SectorMomentum
from momentum_framework.strategies.r11_52wk_reversal import R11FiftyTwoWeekReversal
from momentum_framework.strategies.r12_reversal_1mo import R12Reversal1Mo
from momentum_framework.strategies.r13_bollinger_reversal import R13BollingerReversal
from momentum_framework.strategies.r14_inverse_volatility import R14InverseVolatility
from momentum_framework.strategies.r15_inverse_variance import R15InverseVariance
from momentum_framework.strategies.r16_target_volatility import R16TargetVolatility
from momentum_framework.strategies.r17_downside_volatility import R17DownsideVolatility

REBALANCE_CADENCES = [5, 10, 21]
POSITION_SIZINGS = ["equal", "inverse_volatility"]


@dataclass(frozen=True)
class BaseStrategySpec:
    strategy_code: str
    bands: List[int]
    lookback_months_grid: List[Optional[int]]  # None entries mean "fixed internally, don't pass"
    accepts_lookback_months: bool
    build: Callable[..., StrategyAdapter]  # (band_id, top_n, cadence, position_sizing, lookback_months|None) -> instance


def _top_n_for(band_id: int) -> int:
    return TOP_N_BY_BAND[band_id][0]


BASE_STRATEGIES: List[BaseStrategySpec] = [
    BaseStrategySpec(
        "R01", [2, 4, 7, 9, 10, 12, 13], [3, 6, 9, 12], True,
        lambda band_id, top_n, cadence, sizing, lb: R01TrailingMomentum(
            band_id=band_id, top_n=top_n, lookback_months=lb,
            rebalance_cadence_days=cadence, position_sizing=sizing)),
    BaseStrategySpec(
        "R03", [2, 4, 7, 9, 10, 12, 13], [3, 6, 9, 12], True,
        lambda band_id, top_n, cadence, sizing, lb: R03JTSkipMonth(
            band_id=band_id, top_n=top_n, lookback_months=lb,
            rebalance_cadence_days=cadence, position_sizing=sizing)),
    BaseStrategySpec(
        "R07", [2, 4, 9, 10, 12, 13], [3, 6, 9, 12], True,
        lambda band_id, top_n, cadence, sizing, lb: R07CrashAware(
            band_id=band_id, top_n=top_n, lookback_months=lb,
            rebalance_cadence_days=cadence, position_sizing=sizing)),
    BaseStrategySpec(
        "R08", [2, 4, 7, 9, 10, 12, 13], [3, 6, 9, 12], True,
        lambda band_id, top_n, cadence, sizing, lb: R08BSCVolScale(
            band_id=band_id, top_n=top_n, lookback_months=lb,
            rebalance_cadence_days=cadence, position_sizing=sizing)),
    BaseStrategySpec(
        "R09", [2, 4, 9, 10, 12, 13], [3, 6, 9, 12], True,
        lambda band_id, top_n, cadence, sizing, lb: R09MMVolScale(
            band_id=band_id, top_n=top_n, lookback_months=lb,
            rebalance_cadence_days=cadence, position_sizing=sizing)),
    BaseStrategySpec(
        "R10", [2, 4, 7, 9, 10, 12, 13], [3, 6, 9, 12], True,
        lambda band_id, top_n, cadence, sizing, lb: R10SectorMomentum(
            band_id=band_id, top_n=top_n, lookback_months=lb,
            rebalance_cadence_days=cadence, position_sizing=sizing)),
    BaseStrategySpec(
        "R11", [2, 4, 7, 9, 10, 12, 13], [None], False,
        lambda band_id, top_n, cadence, sizing, lb: R11FiftyTwoWeekReversal(
            band_id=band_id, top_n=top_n, lookback_months=12,  # fixed — the 52wk window itself
            rebalance_cadence_days=cadence, position_sizing=sizing)),
    BaseStrategySpec(
        "R12", [2, 4, 7, 9, 10, 12, 13], [None], False,
        lambda band_id, top_n, cadence, sizing, lb: R12Reversal1Mo(
            band_id=band_id, top_n=top_n, rebalance_cadence_days=cadence, position_sizing=sizing)),
    BaseStrategySpec(
        "R13", [2, 4, 7, 9, 10, 12, 13], [None], False,
        lambda band_id, top_n, cadence, sizing, lb: R13BollingerReversal(
            band_id=band_id, top_n=top_n, rebalance_cadence_days=cadence, position_sizing=sizing)),
]

# R14-R17: unchanged from the original registry — own weighting scheme
# baked in, own (band, lookback, cadence) grid, NO position_sizing sweep
# (has_own_weighting=True short-circuits StrategyBase.size_signals()).
DEFAULT_LOOKBACK_MONTHS = 6
_WEIGHTED_REBALANCE_CADENCE = 21

WEIGHTED_STRATEGIES: List[Tuple[str, List[int], Callable[[int, int], StrategyAdapter]]] = [
    ("R14", [2, 4, 7, 9, 10, 12, 13], lambda band_id, top_n: R14InverseVolatility(
        band_id=band_id, top_n=top_n, lookback_months=DEFAULT_LOOKBACK_MONTHS,
        rebalance_cadence_days=_WEIGHTED_REBALANCE_CADENCE)),
    ("R15", [2, 4, 7, 9, 10, 12, 13], lambda band_id, top_n: R15InverseVariance(
        band_id=band_id, top_n=top_n, lookback_months=DEFAULT_LOOKBACK_MONTHS,
        rebalance_cadence_days=_WEIGHTED_REBALANCE_CADENCE)),
    ("R16", [2, 4, 7, 9, 10, 12, 13], lambda band_id, top_n: R16TargetVolatility(
        band_id=band_id, top_n=top_n, lookback_months=DEFAULT_LOOKBACK_MONTHS,
        rebalance_cadence_days=_WEIGHTED_REBALANCE_CADENCE)),
    ("R17", [2, 4, 7, 9, 10, 12, 13], lambda band_id, top_n: R17DownsideVolatility(
        band_id=band_id, top_n=top_n, lookback_months=DEFAULT_LOOKBACK_MONTHS,
        rebalance_cadence_days=_WEIGHTED_REBALANCE_CADENCE)),
]


def all_configs() -> List[Any]:
    """Flattened (strategy_code, band_id, top_n, factory) tuples — one per
    campaign job. `factory` is always a zero-arg callable (band_id/top_n/
    etc. already bound via default-arg closure) — never reuse a factory's
    OWN produced instance across two runs (see project_windowed_backtest_
    analysis memory)."""
    out: List[Any] = []

    for spec in BASE_STRATEGIES:
        for band_id in spec.bands:
            top_n = _top_n_for(band_id)
            for lookback_months in spec.lookback_months_grid:
                for cadence in REBALANCE_CADENCES:
                    for sizing in POSITION_SIZINGS:
                        out.append((
                            spec.strategy_code, band_id, top_n,
                            (lambda f=spec.build, b=band_id, t=top_n, c=cadence, s=sizing, lb=lookback_months:
                             f(b, t, c, s, lb)),
                        ))

    for code, bands, build in WEIGHTED_STRATEGIES:
        for band_id in bands:
            top_n = _top_n_for(band_id)
            out.append((code, band_id, top_n, (lambda f=build, b=band_id, t=top_n: f(b, t))))

    return out
