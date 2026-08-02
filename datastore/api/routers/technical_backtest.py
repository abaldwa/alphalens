"""
datastore/api/routers/technical_backtest.py

Phase: Technical Analysis Momentum-parity backtest reporting (2026-08-01)
Owner: Platform / Backtest
Consumers: frontend/src/pages/technical/experimentation.tsx,
    recommended-strategies.tsx

New router (kept separate from datastore/api/routers/technical.py, which
is the existing live screener/alerts/analytics surface at /api/v1/ta —
this one is purely the new sweep-report reporting layer, same relationship
datastore/api/routers/momentum.py's /experimentation endpoints have to the
rest of momentum.py). Exact trigger/status/read pattern as momentum.py's
_launch_trigger/_trigger_status (same detached-background-subprocess
design, same polling contract) — reused here rather than reimplemented, so
the existing frontend SweepTriggerButton component works against these
endpoints unmodified.

Read endpoints return the report JSON file's raw dict rather than a
strict per-field Pydantic model (unlike momentum.py's ExperimentationReport)
— the three Technical sweep reports (experimentation/filter_overlays/
recommended_strategies) have meaningfully different variant shapes
(recommended_strategies variants additionally nest a signal_failures dict,
filter_overlays variants carry a "filter" key, etc.), and a shared strict
schema would either force lossy field omission or three near-duplicate
schemas for no real benefit over the frontend just consuming the JSON
directly (same shape scripts/run_technical_*.py already write it in).
"""

import json
import logging
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/technical_backtest", tags=["Technical Backtest"])

_REPORTS_DIR = Path(__file__).resolve().parents[3] / "backtest" / "reports" / "technical"
_TRIGGER_LOGS_DIR = _REPORTS_DIR / "trigger_logs"


class TriggerResponse(BaseModel):
    job_id: str
    status: str = "started"


class TriggerStatusResponse(BaseModel):
    job_id: str
    status: str  # "running" | "completed" | "failed" | "unknown"
    log_tail: Optional[str] = None
    report_file: Optional[str] = None


def _launch_trigger(module: str, job_prefix: str, extra_args: Optional[list] = None) -> TriggerResponse:
    job_id = f"{job_prefix}_{uuid.uuid4().hex[:10]}"
    _TRIGGER_LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = _TRIGGER_LOGS_DIR / f"{job_id}.log"
    cmd = [sys.executable, "-m", module, *(extra_args or [])]
    logger.info(f"technical_backtest._launch_trigger: job_id={job_id} cmd={' '.join(cmd)}")
    with open(log_path, "w") as log_fh:
        subprocess.Popen(cmd, stdout=log_fh, stderr=subprocess.STDOUT, start_new_session=True)
    return TriggerResponse(job_id=job_id)


def _trigger_status(job_id: str, report_glob: str) -> TriggerStatusResponse:
    log_path = _TRIGGER_LOGS_DIR / f"{job_id}.log"
    if not log_path.exists():
        return TriggerStatusResponse(job_id=job_id, status="unknown")

    log_tail = "".join(log_path.read_text(errors="replace").splitlines(keepends=True)[-40:])
    launched_at = log_path.stat().st_mtime
    newer_reports = sorted(
        (p for p in _REPORTS_DIR.glob(report_glob) if p.stat().st_mtime >= launched_at - 5),
        key=lambda p: p.stat().st_mtime,
    )
    if newer_reports:
        return TriggerStatusResponse(
            job_id=job_id, status="completed", log_tail=log_tail, report_file=newer_reports[-1].name,
        )
    if "Traceback (most recent call last)" in log_tail:
        return TriggerStatusResponse(job_id=job_id, status="failed", log_tail=log_tail)
    return TriggerStatusResponse(job_id=job_id, status="running", log_tail=log_tail)


def _read_latest_report(glob_pattern: str, not_found_detail: str) -> Dict[str, Any]:
    files = sorted(_REPORTS_DIR.glob(glob_pattern))
    if not files:
        raise HTTPException(status_code=404, detail=not_found_detail)
    latest = files[-1]
    data = json.loads(latest.read_text())
    data["report_file"] = latest.name
    return data


# --------------------------------------------------------------- experimentation

@router.get("/experimentation")
async def get_experimentation() -> Dict[str, Any]:
    """scripts/run_technical_experimentation.py's baseline sweep — every
    (template, exit_variant, max_hold_days, top_n) variant. 404 until that
    script has been run at least once."""
    return _read_latest_report(
        "technical_experimentation_*.json", "No technical experimentation report found yet",
    )


@router.post("/experimentation/trigger", response_model=TriggerResponse)
async def trigger_experimentation(quick: bool = False) -> TriggerResponse:
    """Launches scripts/run_technical_experimentation.py as a detached
    subprocess; poll /experimentation/trigger/status/{job_id}. quick=true
    runs the reduced grid (5 templates, 1 max_hold_days, 1 top_n) for
    faster iteration instead of the full 3,528-config sweep."""
    return _launch_trigger(
        "scripts.run_technical_experimentation", "technical_experimentation",
        extra_args=["--quick"] if quick else None,
    )


@router.get("/experimentation/trigger/status/{job_id}", response_model=TriggerStatusResponse)
async def get_experimentation_trigger_status(job_id: str) -> TriggerStatusResponse:
    return _trigger_status(job_id, "technical_experimentation_*.json")


# --------------------------------------------------------------- filter overlays

@router.get("/filter_overlays")
async def get_filter_overlays() -> Dict[str, Any]:
    """scripts/run_technical_filter_overlays.py's 5-entry-filter robustness
    sweep (liquidity_floor/quality_gated/downtrend_filter/circuit_lock_proxy/
    regime_conditional), each run individually against the same
    template x top_n grid."""
    return _read_latest_report(
        "technical_filter_overlays_*.json", "No technical filter-overlays report found yet",
    )


@router.post("/filter_overlays/trigger", response_model=TriggerResponse)
async def trigger_filter_overlays(quick: bool = False) -> TriggerResponse:
    return _launch_trigger(
        "scripts.run_technical_filter_overlays", "technical_filter_overlays",
        extra_args=["--quick"] if quick else None,
    )


@router.get("/filter_overlays/trigger/status/{job_id}", response_model=TriggerStatusResponse)
async def get_filter_overlays_trigger_status(job_id: str) -> TriggerStatusResponse:
    return _trigger_status(job_id, "technical_filter_overlays_*.json")


# --------------------------------------------------------- recommended strategies

@router.get("/recommended_strategies")
async def get_recommended_strategies() -> Dict[str, Any]:
    """scripts/run_technical_recommended_strategies.py's composite filter
    strategies (Balanced/Risk-Managed/Max-Defensive) across every template,
    plus curated cross-style combo strategies (TechnicalComboAdapter) and a
    per-variant signal-failure breakdown (losing trades with their entry
    signal snapshot)."""
    return _read_latest_report(
        "technical_recommended_strategies_*.json", "No technical recommended-strategies report found yet",
    )


@router.post("/recommended_strategies/trigger", response_model=TriggerResponse)
async def trigger_recommended_strategies(quick: bool = False) -> TriggerResponse:
    return _launch_trigger(
        "scripts.run_technical_recommended_strategies", "technical_recommended_strategies",
        extra_args=["--quick"] if quick else None,
    )


@router.get("/recommended_strategies/trigger/status/{job_id}", response_model=TriggerStatusResponse)
async def get_recommended_strategies_trigger_status(job_id: str) -> TriggerStatusResponse:
    return _trigger_status(job_id, "technical_recommended_strategies_*.json")
