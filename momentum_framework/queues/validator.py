"""
QueueValidator - catches the exact bug class that hit production on
2026-09-04 (queue generators silently omitting rank_method /
crash_regime_enabled) BEFORE a queue file is written or executed.
"""

from typing import Any, Dict, FrozenSet, List

REQUIRED_JOB_FIELDS = frozenset({
    "rank_band_id",
    "top_n",
    "lookback_months",
    "rebalance_cadence_days",
    "rank_method",
    "crash_regime_enabled",
    "strategy_family",
    "start_date",
    "end_date",
    # filter_preset is the 4th mandatory field, added after discovering the
    # original ("M-family") strategy's 4 cumulative filter presets
    # (all_risk/balanced/risk_managed/max_defensive) were never part of any
    # strategy_id and could silently collide. Required, not defaulted, for
    # the same reason rank_method is: a caller that doesn't know its
    # filter_preset has a real bug, not something to paper over with a
    # default of "all_risk" here.
    "filter_preset",
})

FILTER_PRESETS = frozenset({"all_risk", "balanced", "risk_managed", "max_defensive"})


class QueueValidationError(ValueError):
    """A job spec is missing a required field or has an invalid value."""


class QueueValidator:
    """Validates every job in a generated queue before it is persisted."""

    def __init__(self, required_fields: FrozenSet[str] = REQUIRED_JOB_FIELDS):
        self.required_fields = required_fields

    def validate_job(self, job: Dict[str, Any], job_index: int) -> None:
        missing = self.required_fields - job.keys()
        if missing:
            raise QueueValidationError(
                f"job[{job_index}] missing required fields: {sorted(missing)} "
                f"— job={job}"
            )

        from momentum_framework.common.universe import MBANDS
        if job["rank_band_id"] not in MBANDS:
            raise QueueValidationError(
                f"job[{job_index}] rank_band_id={job['rank_band_id']} is not "
                f"a known M-band: {sorted(MBANDS)}"
            )

        if job["top_n"] <= 0:
            raise QueueValidationError(f"job[{job_index}] top_n must be positive")
        if job["lookback_months"] <= 0:
            raise QueueValidationError(f"job[{job_index}] lookback_months must be positive")
        if job["rebalance_cadence_days"] <= 0:
            raise QueueValidationError(f"job[{job_index}] rebalance_cadence_days must be positive")
        if not isinstance(job["crash_regime_enabled"], bool):
            raise QueueValidationError(
                f"job[{job_index}] crash_regime_enabled must be bool, "
                f"got {type(job['crash_regime_enabled'])}"
            )
        if job["start_date"] >= job["end_date"]:
            raise QueueValidationError(
                f"job[{job_index}] start_date must be before end_date"
            )
        if job["filter_preset"] not in FILTER_PRESETS:
            raise QueueValidationError(
                f"job[{job_index}] filter_preset={job['filter_preset']!r} not one of "
                f"{sorted(FILTER_PRESETS)}"
            )

    def validate_queue(self, jobs: List[Dict[str, Any]]) -> None:
        """Validate every job; raises on the first failure with full context."""
        if not jobs:
            raise QueueValidationError("Queue has zero jobs")
        for i, job in enumerate(jobs):
            self.validate_job(job, i)

    def check_duplicate_strategy_ids(self, jobs: List[Dict[str, Any]]) -> List[str]:
        """
        Returns a list of duplicate strategy_id strings, if any. Two jobs
        with the same identity in one queue is almost always a grid-building
        bug (e.g. a parameter that should vary was hardcoded).
        """
        from momentum_framework.metrics.nomenclature import build_strategy_id

        seen: Dict[str, int] = {}
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
            seen[sid] = seen.get(sid, 0) + 1

        return [sid for sid, count in seen.items() if count > 1]
