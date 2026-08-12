"""
backtest/ta_comparison_report.py

Aggregates a Technical-channel backtest sweep into the single comparison
dataset the user specified: for every (template, exit variant) pair, the three
performance measures side by side.

  Measure 1  CAGR / Sharpe / Sortino / Calmar / max drawdown  (capital_mode="lump")
  Measure 2  Year-on-year returns by Indian FY, plus rolling 2/3/4/5-year returns
  Measure 3  Annual reset: start each FY on the base capital, withdraw booked
             profit after tax, top up after a losing year (capital_mode="annual_reset"),
             reported per LTCG regime with BOTH pre-tax and post-tax withdrawals

WHY THIS READS REPORT FILES AND NOT THE DATABASE
------------------------------------------------
backtest_runs/backtest_trades hold the trades, but not the FY ledger and not the
equity curve, and the DB is single-writer — building a report against it blocks
(and is blocked by) any running sweep. The per-run JSON reports are complete,
immutable once written, and free to read concurrently.

KEYING: strategy_id, NOT template_name
--------------------------------------
run.config_json.template_name is null on these runs; the template survives only
inside strategy_id ("ta_c6_63d_20260812" -> C6). Anything keying on
template_name silently groups everything under None.

SUPERSEDED RUNS
---------------
Reports are matched by (template, variant, capital_mode, regime). If a sweep is
partially re-run after a fix, the re-run reports live under a different
--report-suffix and the stale ones must be moved out of the reports directory
(see backtest/reports/superseded_by_taxfix_20260812/), or both will be found and
the later-created one silently wins. `build()` raises on a duplicate key rather
than picking one, because "silently wins" is how a corrected sweep gets mixed
back together with the numbers it was meant to replace.
"""

from __future__ import annotations

import glob
import json
import logging
import os
from collections import defaultdict
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

ROLLING_WINDOWS_YEARS = (2, 3, 4, 5)

# [2026-08-12] Measure 3 is computed and shipped in this dataset, but must NOT
# be presented as a final result yet. backtest/core/portfolio.py's
# apply_due_annual_reset computes the FY tax and uses it to cap the withdrawal,
# but never debits it from the portfolio — there is no `self.cash -= tax` in
# that path. The portfolio therefore compounds tax-free while the ledger reports
# tax as paid. F4/unconstrained reports Rs 1.23 crore of tax across 17 FYs that
# never left the portfolio.
#
# The correct behaviour already exists in the momentum channel
# (momentum_backtest.py withhold_fy_tax: sell to raise cash, then
# cash -= actual_tax_paid). Applying it here changes tax -> cash -> capital ->
# which trades execute, so it requires re-running all 260 annual-reset jobs
# (~4h). The user has deliberately deferred that re-run, so the numbers stay in
# the dataset (they are the best available and the ledger structure is sound)
# but every consumer is told they are provisional.
#
# The lump measures are unaffected: they never modelled tax as a cash outflow,
# they report pre-tax growth with tax applied post-hoc for reporting.
MEASURE_3_STATUS = {
    "status": "provisional",
    "reason": (
        "FY tax is computed and reported but not debited from the portfolio, so "
        "equity compounds tax-free and 'tax paid' overstates cash actually "
        "leaving. Withdrawal figures are directionally right but the compounding "
        "base is too high. Pending a re-run of the annual-reset sweep."
    ),
    "affects": "annual_reset (measure 3) only — lump CAGR/Sharpe/YoY/rolling are unaffected",
}


def _template_of(strategy_id: str) -> str:
    """'ta_c6_63d_20260812' -> 'C6'. See the module docstring on why this exists."""
    parts = (strategy_id or "").split("_")
    return parts[1].upper() if len(parts) > 1 else (strategy_id or "?")


def _fy_end_of(d: date) -> date:
    return date(d.year + 1 if d.month >= 4 else d.year, 3, 31)


def fy_returns(equity_curve: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Year-on-year return per Indian FY from the equity curve.

    Each FY's return is measured from the equity at its opening point to the
    equity at its close, so the series compounds back to the full-period return.
    The first FY is partial whenever the run does not start on 1 April; it is
    flagged rather than dropped or annualised, because annualising a stub year
    inflates it and dropping it hides capital the strategy actually deployed.
    """
    if not equity_curve:
        return []

    by_fy: Dict[date, List[Tuple[date, float]]] = defaultdict(list)
    for point in equity_curve:
        d = date.fromisoformat(point["date"][:10])
        by_fy[_fy_end_of(d)].append((d, float(point["equity"])))

    out: List[Dict[str, Any]] = []
    prev_close: Optional[float] = None
    first_fy = min(by_fy)
    for fy_end in sorted(by_fy):
        points = sorted(by_fy[fy_end])
        open_equity = prev_close if prev_close is not None else points[0][1]
        close_equity = points[-1][1]
        out.append({
            "fy_end": fy_end.isoformat(),
            "fy_label": f"FY{fy_end.year - 1}-{str(fy_end.year)[2:]}",
            "opening_equity": open_equity,
            "closing_equity": close_equity,
            "return_pct": ((close_equity / open_equity) - 1.0) * 100.0 if open_equity else None,
            "partial": fy_end == first_fy and points[0][0] != date(fy_end.year - 1, 4, 1),
        })
        prev_close = close_equity
    return out


def rolling_returns(equity_curve: List[Dict[str, Any]],
                    windows_years=ROLLING_WINDOWS_YEARS) -> Dict[str, Any]:
    """Annualised return over every N-year window, stepped by FY.

    Reported as the distribution (best/median/worst/n) rather than a single
    number: the point of a rolling measure is to show how much the answer
    depends on when you happened to start, which one figure cannot say.
    """
    curve = [(date.fromisoformat(p["date"][:10]), float(p["equity"])) for p in equity_curve]
    if not curve:
        return {}
    curve.sort()

    def equity_on_or_before(target: date) -> Optional[float]:
        best = None
        for d, e in curve:
            if d <= target:
                best = e
            else:
                break
        return best

    start_d, end_d = curve[0][0], curve[-1][0]
    out: Dict[str, Any] = {}
    for years in windows_years:
        rets: List[float] = []
        anchor_year = start_d.year
        while True:
            w_start = date(anchor_year, 4, 1)
            w_end = date(anchor_year + years, 3, 31)
            if w_start < start_d:
                anchor_year += 1
                continue
            if w_end > end_d:
                break
            e0, e1 = equity_on_or_before(w_start), equity_on_or_before(w_end)
            if e0 and e1 and e0 > 0:
                rets.append(((e1 / e0) ** (1.0 / years) - 1.0) * 100.0)
            anchor_year += 1
        if rets:
            rets_sorted = sorted(rets)
            out[f"{years}y"] = {
                "n_windows": len(rets),
                "best_pct": rets_sorted[-1],
                "median_pct": rets_sorted[len(rets_sorted) // 2],
                "worst_pct": rets_sorted[0],
                "positive_windows": sum(1 for r in rets if r > 0),
            }
    return out


def _ledger_summary(fy_ledger: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Measure 3 rolled up. Keeps pre-tax AND post-tax withdrawals separate —
    the gap between them is the tax drag on an income strategy, which is the
    number this measure exists to expose."""
    if not fy_ledger:
        return {}
    withdrawn = sum(r["withdrawn"] for r in fy_ledger)
    pretax = sum(r["withdrawn_pretax"] for r in fy_ledger)
    topped = sum(r["topped_up"] for r in fy_ledger)
    return {
        "n_financial_years": len(fy_ledger),
        "withdrawn_pretax_total": pretax,
        "withdrawn_post_tax_total": withdrawn,
        "tax_paid_total": sum(r["tax"] for r in fy_ledger),
        "topped_up_total": topped,
        "net_extracted": withdrawn - topped,
        "losing_years": sum(1 for r in fy_ledger if r["topped_up"] > 0),
        "base_capital": fy_ledger[0].get("base_capital"),
        "final_opening_capital": fy_ledger[-1].get("opening_capital_next"),
        "ledger": fy_ledger,
    }


def build(report_globs: List[str]) -> Dict[str, Any]:
    """Aggregate every matching run report into one comparison dataset."""
    paths: List[str] = []
    for pattern in report_globs:
        paths.extend(glob.glob(pattern))
    if not paths:
        raise SystemExit(f"no reports matched {report_globs}")

    strategies: Dict[Tuple[str, str], Dict[str, Any]] = {}
    seen: Dict[Tuple[str, str, str, Optional[str]], str] = {}

    for path in sorted(paths):
        with open(path) as fh:
            report = json.load(fh)
        run = report.get("run") or {}
        if not run.get("strategy_id"):
            continue
        template = _template_of(run["strategy_id"])
        variant = report.get("exit_policy_variant") or "baseline"
        mode = run.get("capital_mode") or "lump"
        regime = run.get("annual_reset_regime_label")

        dup_key = (template, variant, mode, regime)
        if dup_key in seen:
            raise ValueError(
                f"duplicate run for {dup_key}:\n  {seen[dup_key]}\n  {path}\n"
                "Move superseded reports out of the reports directory — see this "
                "module's docstring."
            )
        seen[dup_key] = path

        entry = strategies.setdefault((template, variant), {
            "template": template, "exit_variant": variant,
            "lump": None, "annual_reset": {},
        })

        if mode == "annual_reset":
            entry["annual_reset"][regime or "unspecified"] = {
                "run_id": run.get("run_id"),
                "ltcg_rate": run.get("annual_reset_ltcg_rate"),
                "ltcg_exemption": run.get("annual_reset_ltcg_exemption"),
                **_ledger_summary(report.get("fy_ledger") or []),
                # See MEASURE_3_STATUS. Carried on every row so a consumer that
                # reaches past the top-level flag still cannot present these
                # numbers as final by accident.
                "unverified": True,
                "unverified_reason": MEASURE_3_STATUS["reason"],
            }
        else:
            metrics = report.get("metrics") or {}
            curve = report.get("equity_curve") or []
            entry["lump"] = {
                "run_id": run.get("run_id"),
                "start_date": run.get("start_date"), "end_date": run.get("end_date"),
                "cagr_pct": (metrics.get("cagr") or 0.0) * 100.0,
                "benchmark_cagr_pct": (metrics.get("benchmark_cagr") or 0.0) * 100.0,
                "sharpe": metrics.get("sharpe"), "sortino": metrics.get("sortino"),
                "calmar": metrics.get("calmar"),
                "max_drawdown_pct": (metrics.get("max_drawdown") or 0.0) * 100.0,
                "total_trades": metrics.get("total_trades"),
                "win_rate_pct": (metrics.get("win_rate") or 0.0) * 100.0,
                "profit_factor": metrics.get("profit_factor"),
                "final_capital": metrics.get("final_capital"),
                "avg_days_held": metrics.get("avg_days_held"),
                "fy_returns": fy_returns(curve),
                "rolling_returns": rolling_returns(curve),
            }

    rows = sorted(strategies.values(), key=lambda r: (r["template"], r["exit_variant"]))
    incomplete = [f"{r['template']}/{r['exit_variant']}" for r in rows if r["lump"] is None]
    if incomplete:
        logger.warning("%d strategy/variant pairs have no lump run: %s",
                       len(incomplete), ", ".join(incomplete[:10]))

    return {
        "generated_from": sorted(os.path.basename(p) for p in paths),
        "n_runs": len(paths),
        "n_strategies": len(rows),
        "rolling_windows_years": list(ROLLING_WINDOWS_YEARS),
        "measure_3_status": MEASURE_3_STATUS,
        "strategies": rows,
    }
