"""
datastore/health/runner.py

Phase: A21 (Pipeline Health Checker)
Specs: FeatureBacklog.md A21
Owner: Ops / Scheduler
Consumers: ingestion/scheduler/pipeline_scheduler.py::_execute_job_health_check_job

Orchestrates datastore/health/checks.py's job-completeness check,
inserting every returned Finding via datastore/health/findings.py.
Mirrors datastore/integrity/runner.py's shape exactly.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date as date_type
from typing import Dict, List

from datastore.health.checks import check_job_completeness
from datastore.health.findings import Finding, insert_finding

logger = logging.getLogger(__name__)

_CHECKS = {
    "job_completeness": check_job_completeness,
}


@dataclass
class JobHealthCheckResult:
    as_of_date: date_type
    findings_by_check: Dict[str, int] = field(default_factory=dict)
    critical_count: int = 0
    finding_ids: List[int] = field(default_factory=list)

    @property
    def total_findings(self) -> int:
        return sum(self.findings_by_check.values())


def run_job_health_check(conn, as_of_date: date_type) -> JobHealthCheckResult:
    """
    Run all registered health checks against `conn` for `as_of_date`,
    inserting every finding as status='pending'. Never raises on a
    per-check failure — isolates one check's own exception so it doesn't
    take down the others; the caller decides whether the resulting
    critical_count should fail the job.
    """
    result = JobHealthCheckResult(as_of_date=as_of_date)
    for check_name, check_fn in _CHECKS.items():
        try:
            findings: List[Finding] = check_fn(conn, as_of_date)
        except Exception as exc:  # noqa: BLE001
            logger.error("run_job_health_check: check %s raised: %s", check_name, exc)
            findings = []
        result.findings_by_check[check_name] = len(findings)
        for finding in findings:
            finding_id = insert_finding(conn, finding)
            result.finding_ids.append(finding_id)
            if finding.severity == "critical":
                result.critical_count += 1

    return result
