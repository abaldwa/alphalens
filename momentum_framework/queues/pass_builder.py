"""
Pass-structured campaign queue builder (2026-09-06 remediation session).

WHY PASSES, NOT ONE MONOLITHIC QUEUE: every prior framework result is
stale against at least one of B1 (portfolio weighting), B3/B6 (crash
config), E1/E2 (top_n grid), or E5 (costs+tax) — see
docs/MASTER_ISSUES_CATALOG_2026_09_06.md. The user wants to re-run the
full campaign but review results incrementally rather than committing to
one multi-day run before seeing anything: PASS 1 (smallest top_n per
band, across every active strategy x band) runs first and gets reviewed,
then PASS 2 (each band's second top_n), then PASS 3 (M13's third top_n
only -- partitioned bands don't have one, since E2 dropped their top_n=15
option).

WHAT THIS DOES NOT DO: it does not change what a "job" looks like. Every
job in every pass comes verbatim from that strategy's own
QueueGenerator.build_jobs() (via .generate(), so validation/dedup still
runs) -- see queues/active_generators.py::ACTIVE_GENERATORS for which 12
strategies. This module only REGROUPS that combined output by top_n rank
into passes; band/lookback/cadence/rank_method/crash-params/etc are
untouched, exactly as directed ("only the top_n dimension is being
restructured into passes, nothing else about each strategy's grid should
change").

Pass assignment rule: a job's pass number is the 1-based INDEX of its
top_n within ITS OWN BAND's common/universe.py::TOP_N_BY_BAND entry, not
an absolute top_n value (5 and 10 mean different things for a partitioned
band [5, 10] vs. M13 [10, 15, 20] -- rank, not value, is what makes "pass
1" comparable across bands of different sizes):
    pass 1 = index 0  (partitioned bands: top_n=5;  M13: top_n=10)
    pass 2 = index 1  (partitioned bands: top_n=10; M13: top_n=15)
    pass 3 = index 2  (M13 only: top_n=20 -- partitioned bands have no
                        3rd top_n since E2 dropped 15)
"""

from typing import Any, Dict, List

from momentum_framework.common.universe import TOP_N_BY_BAND
from momentum_framework.queues.active_generators import ACTIVE_GENERATORS


def all_active_jobs() -> List[Dict[str, Any]]:
    """Every job from every ACTIVE_GENERATORS strategy, combined, each
    individually validated (QueueGenerator.generate() raises on any
    validation/duplicate failure within its own strategy first)."""
    jobs: List[Dict[str, Any]] = []
    for generator_cls in ACTIVE_GENERATORS:
        jobs.extend(generator_cls().generate())
    return jobs


def split_into_passes(jobs: List[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
    """Bucket `jobs` into {1: [...], 2: [...], 3: [...]} by each job's
    top_n RANK within its own band's TOP_N_BY_BAND entry. Raises
    ValueError on any job whose (band_id, top_n) pair isn't one of that
    band's declared values -- fail loud rather than silently drop a job
    into the wrong pass or no pass at all."""
    passes: Dict[int, List[Dict[str, Any]]] = {1: [], 2: [], 3: []}
    for job in jobs:
        band_id = job["rank_band_id"]
        top_n = job["top_n"]
        band_top_ns = TOP_N_BY_BAND[band_id]
        try:
            idx = band_top_ns.index(top_n)
        except ValueError:
            raise ValueError(
                f"Job top_n={top_n} for band_id={band_id} is not one of that "
                f"band's declared TOP_N_BY_BAND values {band_top_ns} "
                f"(strategy_family={job.get('strategy_family')}) -- "
                "refusing to guess which pass it belongs to."
            )
        pass_number = idx + 1
        passes[pass_number].append(job)
    return passes


def build_passes() -> Dict[int, List[Dict[str, Any]]]:
    """Convenience: all_active_jobs() + split_into_passes() in one call."""
    return split_into_passes(all_active_jobs())
