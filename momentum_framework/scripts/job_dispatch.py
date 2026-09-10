"""
Job dict -> StrategyAdapter instance dispatch, for executing a queue file
(e.g. results/queues/campaign_2026_09_06_pass1.json) directly, as opposed
to campaign_registry.py::all_configs()'s own factory closures (which
build the SAME strategy classes but from Python code, not a serialized
job dict). Needed because run_campaign.py/run_full_campaign.py only know
how to run campaign_registry.py's configs, not an arbitrary queue JSON
file's job list.

Every strategy's __init__ ends in **kwargs, forwarded up to
StrategyAdapter.__init__ and stored in self.extra_params (see
backtesting/adapter.py) — so passing every non-metadata key from a job
dict as a constructor kwarg is safe generically, with no per-strategy
special-casing required. Only the ROUTING/METADATA keys (which
identify/describe the job, not configure the strategy) need stripping —
see simple_momentum_grid()'s job-dict shape in queues/generator.py.
"""

import inspect
from typing import Any, Dict, Type

from momentum_framework.backtesting.adapter import StrategyAdapter
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
from momentum_framework.strategies.r17_downside_volatility import R17DownsideVolatility

# R16 deliberately absent — retired 2026-09-06, see
# queues/active_generators.py's module docstring. A job with
# strategy_family=="R16" (should never exist in an active queue) raises
# KeyError below rather than silently running the retired strategy.
STRATEGY_CLASS_BY_CODE: Dict[str, Type[StrategyAdapter]] = {
    "R01": R01TrailingMomentum,
    "R03": R03JTSkipMonth,
    "R07": R07CrashAware,
    "R08": R08BSCVolScale,
    "R09": R09MMVolScale,
    "R10": R10SectorMomentum,
    "R11": R11FiftyTwoWeekReversal,
    "R12": R12Reversal1Mo,
    "R13": R13BollingerReversal,
    "R14": R14InverseVolatility,
    "R15": R15InverseVariance,
    "R17": R17DownsideVolatility,
}

# Keys in a simple_momentum_grid()-shaped job dict that describe/route the
# job rather than configure the strategy instance — never forwarded as a
# constructor kwarg. band_id/top_n/lookback_months/rebalance_cadence_days
# ARE constructor args but are passed explicitly by name below (renamed
# from rank_band_id), not via the generic kwargs pass-through.
_METADATA_KEYS = frozenset({
    "kind", "channel", "start_date", "end_date", "rank_band_id", "top_n",
    "lookback_months", "rebalance_cadence_days", "rank_method",
    "strategy_family", "capital_mode", "initial_capital", "max_tickers",
    "min_history_days", "exit_variant",
})

# Generic, strategy-agnostic kwargs (2026-09-06's E4 sensitivity toggle)
# that flow through **kwargs to EVERY strategy via StrategyAdapter.
# __init__ -> extra_params WITHOUT any strategy declaring them as a named
# parameter — unlike skip_months/crash_regime_enabled/select_lowest/
# weight_method/lookback_months (the collision-risk fields the ctor_params
# signature check below exists to catch), no strategy hardcodes or
# re-declares these, so there is no collision risk in forwarding them
# unconditionally. Found 2026-09-07: the strict "only forward keys named
# in ctor_params" rule below was ALSO silently dropping these two
# legitimate passthroughs (since **kwargs-only params are invisible to
# inspect.signature) — caught by dispatching a real exclusion-test job
# and checking extra_params before launching a real campaign with it.
_ALWAYS_FORWARD_KEYS = frozenset({
    "exclude_extraordinary_returns", "extraordinary_returns_top_n",
})


def strategy_from_job(job: Dict[str, Any]) -> StrategyAdapter:
    """
    Instantiate the right StrategyAdapter subclass for one queue job dict.

    A job-dict key is forwarded as a constructor kwarg ONLY if the target
    class's OWN __init__ actually names it as a parameter — several
    strategies hardcode an identity-defining field internally and pass it
    to their super().__init__() call themselves (R01/R03: skip_months;
    R07: crash_regime_enabled always True; R11/R08's non-toggle case
    aside: select_lowest for R11; R12/R13: lookback_months; R14/R15/R17:
    weight_method) while the job dict ALSO carries that same field, purely
    for strategy_id uniformity across strategies (see build_strategy_id's
    module docstring). Forwarding it anyway collides with the class's own
    hardcoded value at its inner super().__init__() call ("got multiple
    values for keyword argument ..."). Checked via inspect.signature
    rather than a hand-maintained per-class exception list, so a NEW
    strategy file added later is handled correctly with zero changes here
    — caught by directly instantiating one job per active strategy from a
    REAL queue file before this script was ever run for real, not
    discovered mid-campaign.
    """
    code = job["strategy_family"]
    if code not in STRATEGY_CLASS_BY_CODE:
        raise KeyError(
            f"strategy_family={code!r} has no dispatch entry in STRATEGY_CLASS_BY_CODE "
            "(R16 is retired and deliberately excluded; anything else is a real bug)"
        )
    cls = STRATEGY_CLASS_BY_CODE[code]
    ctor_params = inspect.signature(cls.__init__).parameters
    candidate_kwargs = {k: v for k, v in job.items() if k not in _METADATA_KEYS}
    filtered_kwargs = {
        k: v for k, v in candidate_kwargs.items()
        if k in ctor_params or k in _ALWAYS_FORWARD_KEYS
    }

    call_kwargs: Dict[str, Any] = {
        "band_id": job["rank_band_id"],
        "top_n": job["top_n"],
        "rebalance_cadence_days": job["rebalance_cadence_days"],
    }
    if "lookback_months" in ctor_params:
        call_kwargs["lookback_months"] = job["lookback_months"]
    call_kwargs.update(filtered_kwargs)
    return cls(**call_kwargs)


def strategy_id_for_job(job: Dict[str, Any]) -> str:
    """A job's strategy_id, computed without running the backtest — just
    instantiates the strategy and reads its describe() output. Lets a
    caller (e.g. run_pass_queue.py's resume/dedup check) know a job's
    identity cheaply, before deciding whether it's worth actually
    executing."""
    from momentum_framework.metrics.nomenclature import strategy_id_from_params
    return strategy_id_from_params(strategy_from_job(job).describe())
