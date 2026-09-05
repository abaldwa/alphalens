"""
datastore/integrity/runner.py

Phase: A20 (Data Integrity Checker)
Specs: FeatureBacklog.md A20
Owner: Data Layer / Ops / Scheduler
Consumers: ingestion/scheduler/daily_pipeline.py::step_data_integrity_check

Orchestrates the checks in datastore/integrity/checks.py (_CHECKS below),
inserts every returned Finding via datastore/integrity/findings.py, and
returns a summary the caller uses to decide whether the pipeline step
itself should fail (any 'critical' finding does).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date as date_type
from typing import Any, Callable, Dict, List

from datastore.integrity import checks as check_fns
from datastore.integrity.findings import Finding, insert_finding

# Each check_fns.check_* function takes optional keyword-only tuning params
# beyond (conn, as_of_date) (lookback_days, fyers_client, sample_size, ...),
# so their signatures don't unify into one Callable type on their own --
# annotated here since every call site (below) only ever calls them
# positionally with exactly these two args, using each function's own
# default for the rest.
_CHECKS: Dict[str, Callable[[Any, date_type], List[Finding]]] = {
    "corporate_actions": check_fns.check_corporate_actions,
    "null_sweep": check_fns.check_null_sweep,
    "holiday_leakage": check_fns.check_holiday_leakage,
    "spot_check": check_fns.check_spot_check,
    # 2026-07-30: A20 follow-up — daily completeness sweep for tickers
    # with zero corporate_actions rows despite substantial trading
    # history (see check_corporate_actions_coverage's docstring).
    "corporate_actions_coverage": check_fns.check_corporate_actions_coverage,
    # 2026-09-05: A20 follow-up — check_corporate_actions only covers
    # SPLIT/BONUS and depends on FYERS already being correctly adjusted;
    # this one is self-referential (no FYERS call) and covers every
    # action_type, closing the RIGHTS/DIVIDEND/OTHER blind spot that let
    # 95 tickers' worth of unadjusted price discontinuities go undetected
    # (see check_corporate_action_continuity's docstring).
    "corporate_action_continuity": check_fns.check_corporate_action_continuity,
}


@dataclass
class IntegrityCheckResult:
    as_of_date: date_type
    findings_by_check: Dict[str, int] = field(default_factory=dict)
    critical_count: int = 0
    finding_ids: List[int] = field(default_factory=list)

    @property
    def total_findings(self) -> int:
        return sum(self.findings_by_check.values())


def run_integrity_checks(conn: Any, as_of_date: date_type) -> IntegrityCheckResult:
    """
    Run every A20 check in _CHECKS against `conn` for `as_of_date`, inserting
    every finding as status='pending'. Never raises on a per-check
    failure — a single check's own exception (e.g. a Fyers outage) is
    logged and treated as zero findings for that check, so one flaky
    upstream source doesn't take down the whole integrity-check step;
    the caller decides whether the resulting critical_count should fail
    the pipeline step.
    """
    import logging

    logger = logging.getLogger(__name__)

    result = IntegrityCheckResult(as_of_date=as_of_date)
    for check_name, check_fn in _CHECKS.items():
        try:
            findings: List[Finding] = check_fn(conn, as_of_date)
        except Exception as exc:  # noqa: BLE001
            logger.error("run_integrity_checks: check %s raised: %s", check_name, exc)
            findings = []
        result.findings_by_check[check_name] = len(findings)
        for finding in findings:
            finding_id = insert_finding(conn, finding)
            result.finding_ids.append(finding_id)
            if finding.severity == "critical":
                result.critical_count += 1

    return result
