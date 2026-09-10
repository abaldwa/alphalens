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
from momentum_framework.queues.active_generators import ACTIVE_GENERATORS
from momentum_framework.strategies.r01_trailing_momentum import R01QueueGenerator

# ALL_GENERATORS kept as a name here (re-exported from the single source
# of truth in queues/active_generators.py, added 2026-09-06 so the real
# campaign-queue assembly script and this test never drift apart — see
# that module's docstring). R16QueueGenerator is NOT in this list —
# retired 2026-09-06 (Category B2, mathematically redundant with R14);
# see test_r16_never_generated below and
# strategies/r16_target_volatility.py's module docstring.
ALL_GENERATORS = ACTIVE_GENERATORS

# Minimum expected job counts — a floor, not an exact match, so the test
# doesn't need updating every time a generator's grid is deliberately
# widened, but WILL catch a generator silently returning far fewer jobs
# than expected (e.g. a band accidentally dropped).

# Updated 2026-09-06: user decision shrank TOP_N_BY_BAND (partitioned
# bands [5,10,15]->[5,10]; M13 [10,20,30,40]->[10,15,20]) — floors below
# reflect the new, smaller, intentional grid sizes actually produced.
MIN_JOB_COUNT = {
    "R01": 170, "R03": 170, "R07": 150, "R08": 170, "R09": 600,
    "R10": 170, "R11": 40, "R12": 260, "R13": 40,
    "R14": 170, "R15": 170, "R17": 170,
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

    # Updated 2026-09-06: user decision shrank TOP_N_BY_BAND (was >= 3000
    # before that change) AND R16 was retired, dropping ALL_GENERATORS
    # from 13 to 12 strategies (see MIN_JOB_COUNT's comment above and
    # test_r16_never_generated).
    assert total_jobs >= 2300, f"Expected >= 2300 total jobs across 12 strategies, got {total_jobs}"
    assert len(all_ids) == total_jobs, (
        f"{total_jobs - len(all_ids)} strategy_id collision(s) across the combined 13-strategy set"
    )


def test_m13_band_present_with_wider_top_n():
    """M13 (band_id=13, full ADTV universe) must use top_n in {10,15,20},
    never the partitioned bands' {5,10} — see project_m13_band_added
    memory. Sets updated 2026-09-06 (user decision): M13 was
    {10,20,30,40}, partitioned bands were {5,10,15}."""
    jobs = R01QueueGenerator().generate()
    m13_top_ns = {j["top_n"] for j in jobs if j["rank_band_id"] == 13}
    other_top_ns = {j["top_n"] for j in jobs if j["rank_band_id"] != 13}
    assert m13_top_ns == {10, 15, 20}
    assert other_top_ns == {5, 10}


def test_r05_never_generated():
    """R05 is a permanent exclusion (rejected at the Phase 3 gate) — no
    generator should ever produce an R05 job."""
    for generator_cls in ALL_GENERATORS:
        jobs = generator_cls().generate()
        for job in jobs:
            assert job["strategy_family"] != "R05", "R05 must never be generated — see docs/CODE_TRACEABILITY.md"


def test_r16_never_generated():
    """R16 retired 2026-09-06 (Category B2: mathematically redundant with
    R14 — see strategies/r16_target_volatility.py's module docstring) —
    no generator in ALL_GENERATORS (R16QueueGenerator deliberately
    excluded from it) should ever produce an R16 job."""
    for generator_cls in ALL_GENERATORS:
        jobs = generator_cls().generate()
        for job in jobs:
            assert job["strategy_family"] != "R16", (
                "R16 is retired — see strategies/r16_target_volatility.py's module docstring"
            )


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
