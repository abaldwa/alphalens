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
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/backtest", tags=["Backtest"])

REPORTS_DIR = Path("backtest/reports")


@router.get("/reports")
async def list_backtest_reports() -> dict[str, Any]:
    """List available backtest report filenames (without .json extension)."""
    if not REPORTS_DIR.exists():
        return {"reports": []}
    names = sorted(p.stem for p in REPORTS_DIR.glob("*.json"))
    return {"reports": names}


@router.get("/reports/{name}")
async def get_backtest_report(name: str) -> dict[str, Any]:
    """Serve one backtest report JSON as-is, by filename stem (e.g. 'phase2_20260624')."""
    # Path-sanitize: reject any name that isn't a bare filename stem (no
    # slashes, no '..') before joining onto REPORTS_DIR, to prevent
    # escaping the reports directory.
    if "/" in name or "\\" in name or name in ("..", "."):
        raise HTTPException(status_code=400, detail="Invalid report name")

    report_path = (REPORTS_DIR / f"{name}.json").resolve()
    if REPORTS_DIR.resolve() not in report_path.parents or not report_path.exists():
        raise HTTPException(status_code=404, detail=f"Report '{name}' not found")

    return json.loads(report_path.read_text())  # type: ignore


# --------------------------------------------------------------------------
# TA strategy comparison reports (backtest/ta_comparison_report.py)
#
# /reports above lists EVERY *.json in backtest/reports — that directory holds
# 11,000+ per-job artifacts, so it is unusable as a UI index. These two
# endpoints expose just the collated comparison reports, which is what the
# Backtest screen actually renders.
#
# Written by ta_comparison_report.write_reports() as
# ta_comparison_{suffix}_{tax_regime}.{json,csv,html}. Reports are served
# from the file artifact rather than recollated per request: a comparison
# spans up to 65 strategies x ~19 years of trades and is far too expensive
# to rebuild on every page load.
# --------------------------------------------------------------------------

_COMPARISON_PREFIX = "ta_comparison_"


@router.get("/ta-comparisons")
async def list_ta_comparisons() -> dict[str, Any]:
    """List collated TA comparison reports, newest first."""
    if not REPORTS_DIR.exists():
        return {"comparisons": []}

    out = []
    for path in REPORTS_DIR.glob(f"{_COMPARISON_PREFIX}*.json"):
        try:
            payload = json.loads(path.read_text())
        except (json.JSONDecodeError, OSError) as exc:
            # A half-written report (the autopilot collates while the queue
            # is still running) must not break the whole listing.
            logger.warning("ta-comparisons: skipping unreadable %s — %s", path.name, exc)
            continue
        out.append(
            {
                "name": path.stem,
                "queue_suffix": payload.get("queue_suffix"),
                "tax_regime": payload.get("tax_regime"),
                "generated_at": payload.get("generated_at"),
                "n_strategies": payload.get("n_strategies"),
                "n_failed": len(payload.get("failed_reports") or []),
            }
        )
    out.sort(key=lambda r: r.get("generated_at") or "", reverse=True)
    return {"comparisons": out}


@router.get("/ta-comparisons/{name}")
async def get_ta_comparison(name: str) -> dict[str, Any]:
    """Serve one collated TA comparison report by filename stem."""
    if "/" in name or "\\" in name or name in ("..", "."):
        raise HTTPException(status_code=400, detail="Invalid report name")
    if not name.startswith(_COMPARISON_PREFIX):
        raise HTTPException(
            status_code=400,
            detail=f"Not a TA comparison report (expected '{_COMPARISON_PREFIX}*')",
        )

    report_path = (REPORTS_DIR / f"{name}.json").resolve()
    if REPORTS_DIR.resolve() not in report_path.parents or not report_path.exists():
        raise HTTPException(status_code=404, detail=f"Comparison '{name}' not found")

    return json.loads(report_path.read_text())  # type: ignore


# --------------------------------------------------------------------------
# HTML Report Serving (Strategy Analysis Reports)
#
# /html-reports lists HTML analysis reports (strategy comparisons, band analysis, etc.)
# /html-reports/{name} serves the HTML file directly with proper content-type
# --------------------------------------------------------------------------


@router.get("/html-reports")
async def list_html_reports() -> dict[str, Any]:
    """List available HTML report filenames (strategy analysis reports)."""
    if not REPORTS_DIR.exists():
        return {"reports": []}
    names = sorted(p.stem for p in REPORTS_DIR.glob("*.html"))
    return {"reports": names}


@router.get("/html-reports/{name}")
async def get_html_report(name: str) -> FileResponse:
    """Serve HTML analysis report by filename stem (e.g. 'r0_band_analysis_detailed')."""

    # Path-sanitize: reject any name that isn't a bare filename stem
    if "/" in name or "\\" in name or name in ("..", "."):
        raise HTTPException(status_code=400, detail="Invalid report name")

    report_path = (REPORTS_DIR / f"{name}.html").resolve()
    if REPORTS_DIR.resolve() not in report_path.parents or not report_path.exists():
        raise HTTPException(status_code=404, detail=f"HTML report '{name}' not found")

    return FileResponse(path=report_path, media_type="text/html")
