"""
datastore/integrity/runner.py

Phase: A20 (Data Integrity Checker)
Specs: FeatureBacklog.md A20
Owner: Data Layer / Ops / Scheduler
Consumers: ingestion/scheduler/daily_pipeline.py::step_data_integrity_check

Orchestrates the four checks in datastore/integrity/checks.py, inserts
every returned Finding via datastore/integrity/findings.py, and returns a
summary the caller uses to decide whether the pipeline step itself should
fail (any 'critical' finding does).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date as date_type
from typing import Dict, List

from datastore.integrity import checks as check_fns
from datastore.integrity.findings import Finding, insert_finding

_CHECKS = {
    "corporate_actions": check_fns.check_corporate_actions,
    "null_sweep": check_fns.check_null_sweep,
    "holiday_leakage": check_fns.check_holiday_leakage,
    "spot_check": check_fns.check_spot_check,
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


def run_integrity_checks(conn, as_of_date: date_type) -> IntegrityCheckResult:
    """
    Run all four A20 checks against `conn` for `as_of_date`, inserting
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
