"""
Queue generator smoke tests — every strategy's QueueGenerator must
produce a valid, non-colliding job list on its own, AND the full set of
13 together must not collide with each other. QueueGenerator.generate()
already validates + duplicate-checks internally (queues/validator.py) —
these tests exercise that machinery across the whole family at once,
the way an isolated per-strategy call cannot.
"""

import pytest

from momentum_framework.metrics.nomenclature import build_strategy_id
from momentum_framework.strategies.r01_trailing_momentum import R01QueueGenerator
from momentum_framework.strategies.r03_jt_skipmonth import R03QueueGenerator
from momentum_framework.strategies.r07_crash_aware import R07QueueGenerator
from momentum_framework.strategies.r08_bsc_volscale import R08QueueGenerator
from momentum_framework.strategies.r09_mm_volscale import R09QueueGenerator
from momentum_framework.strategies.r10_sector_momentum import R10QueueGenerator
from momentum_framework.strategies.r11_52wk_reversal import R11QueueGenerator
from momentum_framework.strategies.r12_reversal_1mo import R12QueueGenerator
from momentum_framework.strategies.r13_bollinger_reversal import R13QueueGenerator
from momentum_framework.strategies.r14_inverse_volatility import R14QueueGenerator
from momentum_framework.strategies.r15_inverse_variance import R15QueueGenerator
from momentum_framework.strategies.r16_target_volatility import R16QueueGenerator
from momentum_framework.strategies.r17_downside_volatility import R17QueueGenerator

ALL_GENERATORS = [
    R01QueueGenerator, R03QueueGenerator, R07QueueGenerator, R08QueueGenerator,
    R09QueueGenerator, R10QueueGenerator, R11QueueGenerator, R12QueueGenerator,
    R13QueueGenerator, R14QueueGenerator, R15QueueGenerator, R16QueueGenerator,
    R17QueueGenerator,
]

# Minimum expected job counts — a floor, not an exact match, so the test
# doesn't need updating every time a generator's grid is deliberately
# widened, but WILL catch a generator silently returning far fewer jobs
# than expected (e.g. a band accidentally dropped).
MIN_JOB_COUNT = {
    "R01": 264, "R03": 264, "R07": 200, "R08": 264, "R09": 900,
    "R10": 264, "R11": 60, "R12": 350, "R13": 60,
    "R14": 264, "R15": 264, "R16": 264, "R17": 264,
}


@pytest.mark.parametrize("generator_cls", ALL_GENERATORS)
def test_generator_produces_valid_queue(generator_cls):
    jobs = generator_cls().generate()  # raises internally on any validation/duplicate failure
    assert len(jobs) > 0

    strategy_code = jobs[0]["strategy_family"]
    floor = MIN_JOB_COUNT.get(strategy_code)
    if floor is not None:
        assert len(jobs) >= floor, f"{strategy_code} produced {len(jobs)} jobs, expected >= {floor}"

    for job in jobs:
        assert job["rank_method"], f"{strategy_code} job missing rank_method"
        assert isinstance(job["crash_regime_enabled"], bool)
        assert job["filter_preset"]


def test_all_13_strategies_combined_zero_collisions():
    """The definitive cross-strategy check: every job from every
    generator, combined, must produce a unique strategy_id."""
    all_ids = set()
    total_jobs = 0

    for generator_cls in ALL_GENERATORS:
        jobs = generator_cls().generate()
        total_jobs += len(jobs)
        for job in jobs:
            sid = build_strategy_id(
                strategy_code=job["strategy_family"],
                band_id=job["rank_band_id"],
                top_n=job["top_n"],
                lookback_months=job["lookback_months"],
                rebalance_cadence_days=job["rebalance_cadence_days"],
                rank_method=job["rank_method"],
                filter_preset=job["filter_preset"],
                crash_regime_enabled=job["crash_regime_enabled"],
                vol_scaling_mode=job.get("vol_scaling_mode"),
                weight_method=job.get("weight_method"),
                skip_months=job.get("skip_months", 0),
                vol_target_enabled=job.get("vol_target_enabled", False),
                vol_target_pct=job.get("vol_target_pct"),
                liquidity_quintile=job.get("liquidity_quintile"),
            )
            all_ids.add(sid)

    assert total_jobs >= 3000, f"Expected >= 3000 total jobs across 13 strategies, got {total_jobs}"
    assert len(all_ids) == total_jobs, (
        f"{total_jobs - len(all_ids)} strategy_id collision(s) across the combined 13-strategy set"
    )


def test_m13_band_present_with_wider_top_n():
    """M13 (band_id=13, full ADTV universe) must use top_n in {10,20,30,40},
    never the partitioned bands' {5,10,15} — see project_m13_band_added memory."""
    jobs = R01QueueGenerator().generate()
    m13_top_ns = {j["top_n"] for j in jobs if j["rank_band_id"] == 13}
    other_top_ns = {j["top_n"] for j in jobs if j["rank_band_id"] != 13}
    assert m13_top_ns == {10, 20, 30, 40}
    assert other_top_ns == {5, 10, 15}


def test_r05_never_generated():
    """R05 is a permanent exclusion (rejected at the Phase 3 gate) — no
    generator should ever produce an R05 job."""
    for generator_cls in ALL_GENERATORS:
        jobs = generator_cls().generate()
        for job in jobs:
            assert job["strategy_family"] != "R05", "R05 must never be generated — see docs/CODE_TRACEABILITY.md"


def test_no_generator_uses_retired_baseline_exit_variant():
    """Regression test: "baseline" is a RETIRED legacy exit policy
    (deprecated 2026-08-13, silently falls back to "risk_managed"'s
    aggressive daily exit barriers — 207x turnover bug, see commit
    3f441bb3 on fix/mypy-type-errors-api-routers). Found and fixed across
    the framework 2026-09-04; this test prevents it from creeping back in
    via a new strategy file that doesn't use simple_momentum_grid()."""
    for generator_cls in ALL_GENERATORS:
        jobs = generator_cls().generate()
        for job in jobs:
            assert job.get("exit_variant") != "baseline", (
                f"{job['strategy_family']} job uses the retired 'baseline' exit_variant — use 'unconstrained'"
            )
