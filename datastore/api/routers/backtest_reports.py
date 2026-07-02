"""
datastore/api/routers/backtest_reports.py

Phase: 3.x (Web UI — SPEC-UI-005 Backtest Results screen)
Specs: SPEC-UI-005
Owner: Platform / DataStore
Consumers: dashboard/static (Backtest Results screen)

Read-only passthrough for backtest/reports/*.json, written by
run_phase{1,2,3}_backtest.py. No DB table — these are static report
artifacts; this router just lists and serves them, path-sanitized to
backtest/reports/ only (no path traversal outside that directory).
"""

import json
import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/backtest", tags=["Backtest"])

REPORTS_DIR = Path("backtest/reports")


@router.get("/reports")
async def list_backtest_reports() -> dict:
    """List available backtest report filenames (without .json extension)."""
    if not REPORTS_DIR.exists():
        return {"reports": []}
    names = sorted(p.stem for p in REPORTS_DIR.glob("*.json"))
    return {"reports": names}


@router.get("/reports/{name}")
async def get_backtest_report(name: str) -> dict:
    """Serve one backtest report JSON as-is, by filename stem (e.g. 'phase2_20260624')."""
    # Path-sanitize: reject any name that isn't a bare filename stem (no
    # slashes, no '..') before joining onto REPORTS_DIR, to prevent
    # escaping the reports directory.
    if "/" in name or "\\" in name or name in ("..", "."):
        raise HTTPException(status_code=400, detail="Invalid report name")

    report_path = (REPORTS_DIR / f"{name}.json").resolve()
    if REPORTS_DIR.resolve() not in report_path.parents or not report_path.exists():
        raise HTTPException(status_code=404, detail=f"Report '{name}' not found")

    return json.loads(report_path.read_text())
