"""
backtest/report_utils.py

ML17(b) (2026-07-13) — "one backtest per horizon model, its own fold/
report" restructuring, reusing ML17(a)'s existing real-Nifty-benchmark-
curve logic (backtest/engine.py's `benchmark_cagr`/`benchmark_sharpe`/
`excess_return`, already threaded through every `FoldResult`/
`BacktestResults.to_dict()`).

Previously `run_phase2_backtest.py`/`run_phase3_backtest.py` only wrote
one combined JSON report per script (e.g. `reports/phase3_20260713.json`)
bundling every horizon variant's results together under one
`gate_passed`/`comparison` umbrella — a caller wanting just the 63d
variant's own fold-by-fold record (including its own benchmark curve
comparison) had to reach into the combined file and know which key held
it. `write_per_horizon_reports()` is a pure, injectable function (no
network/DB access of its own — it only serialises `results.to_dict()`
dicts BacktestEngine already produced) that additionally writes one
standalone JSON report per horizon variant, so each horizon's own
fold-level results + its own real-benchmark comparison stand on their
own, independent of whichever other variant(s) that script happened to
run alongside it that day.

Does not change `BacktestEngine`/`compute_fold_metrics` at all — this is
purely a reporting-layer restructuring on top of already-computed
per-variant `results.to_dict()` output, so it carries none of the
re-training/re-running risk a deeper engine change would.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict

logger = logging.getLogger(__name__)


def write_per_horizon_reports(
    variants: Dict[str, Dict[str, Any]],
    reports_dir: Path,
    run_date_str: str,
    report_prefix: str,
) -> Dict[str, Path]:
    """
    Write one standalone JSON report file per horizon variant.

    Parameters
    ----------
    variants : dict
        {variant_name: results_dict}, e.g. {"signal_5d": phase1.to_dict(),
        "signal_63d_watchlist": phase2.to_dict()} — each `results_dict` is
        exactly what `BacktestResults.to_dict()` returns (fold-level
        cagr/sharpe/benchmark_cagr/benchmark_sharpe/excess_return plus the
        aggregate dict), never re-derived or guessed here.
    reports_dir : Path
        Directory the combined report already writes to
        (`backtest/reports/`, per each script's existing `REPORTS_DIR`).
    run_date_str : str
        Same `run_date.strftime("%Y%m%d")` string the combined report file
        uses, so per-horizon and combined reports for the same run share a
        date stamp.
    report_prefix : str
        e.g. "phase2" or "phase3" — distinguishes which script produced
        these per-horizon files alongside its own combined report.

    Returns
    -------
    dict
        {variant_name: written file Path} — for logging/tests.
    """
    reports_dir.mkdir(parents=True, exist_ok=True)
    written: Dict[str, Path] = {}
    for variant_name, results_dict in variants.items():
        path = reports_dir / f"{report_prefix}_{variant_name}_{run_date_str}.json"
        with open(path, "w") as fh:
            json.dump(results_dict, fh, indent=2, default=str)
        written[variant_name] = path
        logger.info(f"write_per_horizon_reports: wrote {variant_name}'s own report to {path}")
    return written
