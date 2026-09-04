"""
QueueGenerator - common base class for building backtest job queues.

A concrete strategy's generator (e.g. strategies/r01_trailing_momentum.py's
R01QueueGenerator) subclasses this and implements build_jobs(); this base
class handles validation, duplicate detection, and JSON serialization
identically for every strategy so no generator can silently skip them
the way the pre-framework generate_r*.py scripts did.
"""

import json
from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from momentum_framework.queues.validator import QueueValidator, QueueValidationError

QUEUE_OUTPUT_DIR = Path(__file__).resolve().parent.parent / "results" / "queues"


class QueueGenerator(ABC):
    """Base class every strategy's queue generator must subclass."""

    #: Set by subclasses, e.g. "R01", "R09"
    strategy_family: str = "UNSET"

    def __init__(self, validator: Optional[QueueValidator] = None):
        self.validator = validator or QueueValidator()

    @abstractmethod
    def build_jobs(self) -> List[Dict[str, Any]]:
        """
        Return the full list of job dicts for this strategy's parameter
        grid. Every job MUST include all fields in
        queues.validator.REQUIRED_JOB_FIELDS explicitly — no relying on
        the orchestrator to infer rank_method or crash_regime_enabled.
        """
        raise NotImplementedError

    @staticmethod
    def band_top_n_pairs(bands: List[int]) -> List[Tuple[int, int]]:
        """
        (band_id, top_n) pairs for `bands`, using each band's own top_n
        set from common/universe.py::TOP_N_BY_BAND — NOT a single top_n
        list applied uniformly.

        This matters specifically for M13 (band_id=13, the full 800-stock
        ADTV universe): it is tested with wider baskets (top 10/20/30/40)
        than the partitioned bands M2/M4/M7/M9/M10/M12 (top 5/10/15),
        because M13 has 800 names to choose from vs. 75-550 for a
        partitioned band. Every strategy's generator should build its grid
        from this helper rather than hardcoding one TOP_NS list across all
        bands, so adding a band with a different top_n policy (as M13 was)
        never requires editing each strategy file's grid loop by hand.
        """
        from momentum_framework.common.universe import TOP_N_BY_BAND

        pairs = []
        for band_id in bands:
            if band_id not in TOP_N_BY_BAND:
                raise ValueError(
                    f"band_id={band_id} has no TOP_N_BY_BAND entry — "
                    "add it to common/universe.py before using it in a queue generator"
                )
            for top_n in TOP_N_BY_BAND[band_id]:
                pairs.append((band_id, top_n))
        return pairs

    def generate(self, skip_validation: bool = False) -> List[Dict[str, Any]]:
        """Build, validate, and return the job list (does not write to disk)."""
        jobs = self.build_jobs()

        if not skip_validation:
            self.validator.validate_queue(jobs)
            duplicates = self.validator.check_duplicate_strategy_ids(jobs)
            if duplicates:
                raise QueueValidationError(
                    f"{self.strategy_family} queue has duplicate strategy_ids "
                    f"(likely a grid-building bug): {duplicates}"
                )

        return jobs

    def save_queue(self, filename: str, description: str = "") -> Path:
        """Generate, validate, and write the queue to results/queues/<filename>."""
        jobs = self.generate()

        payload = {
            "_description": description or f"{self.strategy_family} backtest queue",
            "_metadata": {
                "strategy_family": self.strategy_family,
                "generated_at": date.today().isoformat(),
                "job_count": len(jobs),
                "framework_version": "1.0.0",
            },
            "jobs": jobs,
        }

        QUEUE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        out_path = QUEUE_OUTPUT_DIR / filename
        out_path.write_text(json.dumps(payload, indent=2))
        return out_path

    def simple_momentum_grid(
        self,
        strategy_code: str,
        rank_method: str,
        bands: List[int],
        lookback_months: List[int],
        rebalance_cadences: List[int],
        start_date: str,
        end_date: str,
        filter_presets: Sequence[str] = ("all_risk",),
        weight_method: Optional[str] = None,
        skip_months: int = 0,
        crash_regime_enabled: bool = False,
        vol_scaling_mode: Optional[str] = None,
        vol_target_enabled: bool = False,
        extra_fields: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Shared grid-builder for strategies whose only variation is the
        standard (band × lookback × cadence × top_n × filter_preset) sweep
        plus one constant per-strategy setting (weight_method for
        R14-R17, skip_months for R01/R03, crash_regime_enabled for R07,
        vol_scaling_mode for R09, vol_target_enabled for R08, ...).

        Reused instead of copy-pasting the same nested-loop job-dict
        construction into every strategy file's build_jobs() — see
        strategies/r14_inverse_volatility.py for a generator that is
        nothing but a call to this plus its own constants.

        `extra_fields` merges additional constant key/value pairs into
        every job (e.g. R08/R09's vol_target_pct, vol_scaling_lookback_days)
        — for fields with no dedicated parameter here, rather than each
        caller post-processing the returned list by hand.
        """
        jobs = []
        for band_id, top_n in self.band_top_n_pairs(list(bands)):
            for lookback in lookback_months:
                for cadence in rebalance_cadences:
                    for filter_preset in filter_presets:
                        job = {
                            "kind": "orchestrator",
                            "channel": "momentum",
                            "start_date": start_date,
                            "end_date": end_date,
                            "rank_band_id": band_id,
                            "top_n": top_n,
                            "lookback_months": lookback,
                            "rebalance_cadence_days": cadence,
                            "rank_method": rank_method,
                            "crash_regime_enabled": crash_regime_enabled,
                            "skip_months": skip_months,
                            "filter_preset": filter_preset,
                            "strategy_family": strategy_code,
                            "capital_mode": "lump",
                            "initial_capital": 1_000_000,
                            "max_tickers": 800,
                            "min_history_days": 60,
                            "exit_variant": "unconstrained",
                        }
                        if weight_method is not None:
                            job["weight_method"] = weight_method
                        if vol_scaling_mode is not None:
                            job["vol_scaling_mode"] = vol_scaling_mode
                        if vol_target_enabled:
                            job["vol_target_enabled"] = True
                        if extra_fields:
                            job.update(extra_fields)
                        jobs.append(job)
        return jobs
