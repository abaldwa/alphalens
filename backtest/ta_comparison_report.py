"""
backtest/ta_comparison_report.py

Owner: Platform / Backtest
Consumers: scripts/run_ta_5year_backtest.py, operator CLI

Collates one queue's worth of TA strategy backtests (one report JSON per
template — backtest/reports/orchestrator_{suffix}_job{N}.json) into a
single comparison: a JSON payload, a flat CSV leaderboard, and a
self-contained HTML page written into backtest/reports/.

HTML is written to disk here, NOT published as a Claude Artifact — per the
project's standing rule that every HTML report ships inside the app (see
backtest/export_trade_book.py, which does the same for per-run trade
books).
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import logging
import os
from collections import defaultdict
from datetime import date, datetime
from html import escape
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from backtest.ta_comprehensive_metrics import compute_comprehensive_metrics

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPORTS_DIR = Path(__file__).resolve().parent / "reports"

# Leaderboard columns, in display order: (json path, header, formatter key)
_CSV_COLUMNS = [
    ("template_name", "template"),
    ("style", "style"),
    ("closed_trades", "closed_trades"),
    ("engine_metrics.cagr", "cagr"),
    ("engine_metrics.sharpe", "sharpe"),
    ("engine_metrics.sortino", "sortino"),
    ("engine_metrics.calmar", "calmar"),
    ("engine_metrics.max_drawdown", "max_drawdown"),
    ("engine_metrics.win_rate", "win_rate"),
    ("engine_metrics.profit_factor", "profit_factor"),
    ("engine_metrics.benchmark_cagr", "benchmark_cagr"),
    ("engine_metrics.excess_return", "excess_return"),
    ("avg_holding_days", "avg_holding_days"),
    ("realized_pnl_inr", "pre_tax_pnl_inr"),
    ("post_tax_pnl_inr", "post_tax_pnl_inr"),
    ("post_tax_return_on_capital", "post_tax_return"),
    ("holdings.avg_concurrent_positions_calendar", "avg_stocks_held"),
    ("entries.avg_entries_per_month", "entries_per_month"),
    # Rolling N-year windows (mark-to-market, not realized — see
    # ta_comprehensive_metrics.rolling_returns). Median total return per
    # window plus the share of windows that were positive; the annualized
    # median is in the JSON for anyone comparing 2y against 5y directly.
    ("rolling.2y.median", "roll2y_median"),
    ("rolling.2y.positive_share", "roll2y_pos_share"),
    ("rolling.3y.median", "roll3y_median"),
    ("rolling.3y.positive_share", "roll3y_pos_share"),
    ("rolling.4y.median", "roll4y_median"),
    ("rolling.4y.positive_share", "roll4y_pos_share"),
    ("rolling.5y.median", "roll5y_median"),
    ("rolling.5y.positive_share", "roll5y_pos_share"),
]


def _dig(payload: Dict[str, Any], dotted: str) -> Any:
    node: Any = payload
    for part in dotted.split("."):
        if not isinstance(node, dict):
            return None
        node = node.get(part)
    return node


def collect_run_reports(suffix: str, reports_dir: Path = REPORTS_DIR) -> List[Path]:
    """Every orchestrator report belonging to one queue run, in job order.
    A missing job number is simply absent (a failed job writes no report) —
    the caller reports the gap rather than this silently renumbering."""
    paths = sorted(
        reports_dir.glob(f"orchestrator_{suffix}_job*.json"),
        key=lambda p: int(p.stem.rsplit("job", 1)[1]),
    )
    return paths


def benchmark_cagr(start_date: str, end_date: str, index_name: str = "Nifty 500") -> Optional[float]:
    """Real Nifty 500 CAGR over the run window, read straight from
    index_ohlcv.

    Why this is computed here rather than taken from the run's own
    metrics: backtest/core/metrics.py::benchmark_metrics() hardcodes
    `index_ohlcv_min_date=date(2023, 7, 3)` and returns
    benchmark_status="insufficient_benchmark_history" for any run starting
    earlier — so every job in a 2021-start sweep reports benchmark_cagr and
    excess_return as null. That cutoff is stale: index_ohlcv now holds
    Nifty 500 continuously from 2012-03-13 (verified 2026-08-08). Fixing
    the engine default is the real fix and belongs in its own change with
    its own review — this recomputes the same quantity from the same table
    so the comparison isn't left without a benchmark in the meantime.
    Returns None if the index genuinely lacks data for the window.
    """
    from config.settings import DUCKDB_PATH
    from datastore.api.db import get_duckdb_connection
    from backtest.core.metrics import calendar_cagr

    try:
        with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
            rows = conn.execute(
                "SELECT close FROM index_ohlcv WHERE index_name = ? AND date BETWEEN ? AND ? ORDER BY date",
                [index_name, start_date, end_date],
            ).fetchall()
    except Exception:  # noqa: BLE001 — a locked/unavailable DB must not sink the report
        logger.warning("ta_comparison_report: benchmark lookup failed — leaving benchmark unset", exc_info=True)
        return None
    if len(rows) < 2:
        return None
    return calendar_cagr(rows[0][0], rows[-1][0], start_date, end_date)


def build_comparison(
    suffix: str, reports_dir: Path = REPORTS_DIR, tax_regime: str = "ltcg_12_5pct_1_25L",
) -> Dict[str, Any]:
    # [A95-R1, 2026-08-14] Style comes from the strategy_registry row, not from
    # an imported TEMPLATE_STYLE dict, so the report labels a run with the same
    # declared style the backtest resolved its horizon from.
    #
    # Unlike the orchestrator's lookup this one TOLERATES a missing row and
    # falls back to "Unknown" — deliberately, and it is not the silent fallback
    # strategies/definitions.py forbids. This is a collation over historical
    # report JSONs, some naming templates that no longer exist; the surrounding
    # loop already treats one unreadable run as skippable rather than fatal, and
    # failing the whole comparison because a retired template has no current row
    # would lose every other strategy's numbers. Nothing is decided from this
    # value; it is a display label.
    from strategies.definitions import DefinitionNotFound, technical_template_style

    def _style(template_name: Optional[str]) -> str:
        if not template_name:
            return "Unknown"
        try:
            # Annotated rather than returned directly: the import is untyped,
            # so returning it straight is an implicit Any -> str.
            style: str = technical_template_style(template_name)
            return style
        except DefinitionNotFound:
            return "Unknown"

    bench_cache: Dict[Tuple[str, str], Optional[float]] = {}

    strategies: List[Dict[str, Any]] = []
    failures: List[Dict[str, str]] = []
    for path in collect_run_reports(suffix, reports_dir):
        try:
            report = json.loads(path.read_text())
            row = compute_comprehensive_metrics(report)
        except Exception as exc:  # noqa: BLE001 — one bad job must not sink the collation
            failures.append({"report": path.name, "error": f"{type(exc).__name__}: {exc}"})
            logger.warning(f"ta_comparison_report: skipping {path.name} — {exc}")
            continue
        row["style"] = _style(row.get("template_name"))
        tax = row["taxes"][tax_regime]
        row["post_tax_pnl_inr"] = tax["post_tax_pnl_inr"]
        row["total_tax_inr"] = tax["total_tax_inr"]
        capital = row.get("initial_capital") or 0.0
        row["post_tax_return_on_capital"] = (tax["post_tax_pnl_inr"] / capital) if capital else None

        # Benchmark recomputed here — see benchmark_cagr()'s docstring for why
        # the engine's own value is null on a 2021-start run.
        window = (str(row.get("start_date"))[:10], str(row.get("end_date"))[:10])
        if window not in bench_cache:
            bench_cache[window] = benchmark_cagr(*window)
        bench = bench_cache[window]
        row["engine_metrics"]["benchmark_cagr"] = bench
        strategy_cagr = row["engine_metrics"].get("cagr")
        row["engine_metrics"]["excess_return"] = (
            strategy_cagr - bench if bench is not None and strategy_cagr is not None else None
        )
        strategies.append(row)

    ranked = sorted(
        strategies,
        key=lambda r: (_dig(r, "engine_metrics.sharpe") if _dig(r, "engine_metrics.sharpe") is not None else -9e9),
        reverse=True,
    )
    return {
        "generated_at": datetime.now().isoformat(),
        "queue_suffix": suffix,
        "tax_regime": tax_regime,
        "basis": "realized",
        "n_strategies": len(ranked),
        "failed_reports": failures,
        "strategies": ranked,
    }


def write_csv(comparison: Dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow([header for _, header in _CSV_COLUMNS])
        for row in comparison["strategies"]:
            writer.writerow([_dig(row, dotted) for dotted, _ in _CSV_COLUMNS])


def _fmt(value: Any, kind: str = "num") -> str:
    if value is None:
        return "—"
    if kind == "pct":
        return f"{value * 100:.2f}%"
    if kind == "inr":
        return f"₹{value:,.0f}"
    if isinstance(value, float):
        return f"{value:.3f}"
    return escape(str(value))


def _yearly_table(comparison: Dict[str, Any]) -> str:
    years = sorted({b["trading_year"] for r in comparison["strategies"] for b in r["yearly"]})
    head = "".join(f"<th>{escape(y)}</th>" for y in years)
    rows = []
    for r in comparison["strategies"]:
        by_year = {b["trading_year"]: b["return_pct"] for b in r["yearly"]}
        cells = "".join(
            f'<td class="{"pos" if (by_year.get(y) or 0) > 0 else "neg" if by_year.get(y) is not None else ""}">'
            f"{_fmt(by_year.get(y), 'pct')}</td>"
            for y in years
        )
        rows.append(f"<tr><td class='k'>{escape(str(r['template_name']))}</td>{cells}</tr>")
    return (
        f"<table><thead><tr><th>Template</th>{head}</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
    )


def render_html(comparison: Dict[str, Any]) -> str:
    header_cells = "".join(f"<th>{escape(h)}</th>" for _, h in _CSV_COLUMNS)
    body_rows = []
    for r in comparison["strategies"]:
        cells = []
        for dotted, header in _CSV_COLUMNS:
            value = _dig(r, dotted)
            kind = (
                "pct" if header in {"cagr", "max_drawdown", "win_rate", "benchmark_cagr", "excess_return", "post_tax_return"}
                or header.startswith("roll")
                else "inr" if header.endswith("_inr")
                else "num"
            )
            cells.append(f"<td>{_fmt(value, kind)}</td>")
        body_rows.append(f"<tr>{''.join(cells)}</tr>")

    failed = comparison["failed_reports"]
    failed_html = (
        "<p class='warn'>"
        + escape(f"{len(failed)} report(s) could not be collated: ")
        + escape(", ".join(f["report"] for f in failed))
        + "</p>"
        if failed
        else ""
    )
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>TA Strategy Comparison — {escape(comparison['queue_suffix'])}</title>
<style>
  :root {{ color-scheme: light dark; --bd:#d0d7de; --mut:#57606a; }}
  body {{ font: 14px/1.5 system-ui, sans-serif; margin: 24px; }}
  h1 {{ font-size: 20px; margin-bottom: 4px; }}
  .meta {{ color: var(--mut); margin-bottom: 20px; }}
  .warn {{ color: #b35900; }}
  .scroll {{ overflow-x: auto; }}
  table {{ border-collapse: collapse; width: 100%; font-variant-numeric: tabular-nums; }}
  th, td {{ border: 1px solid var(--bd); padding: 5px 8px; text-align: right; white-space: nowrap; }}
  th {{ background: rgba(127,127,127,.12); position: sticky; top: 0; }}
  td:first-child, th:first-child, td.k {{ text-align: left; font-weight: 600; }}
  td.pos {{ color: #1a7f37; }} td.neg {{ color: #cf222e; }}
  h2 {{ font-size: 16px; margin-top: 32px; }}
</style></head><body>
<h1>Technical Strategy Comparison — {escape(comparison['queue_suffix'])}</h1>
<p class="meta">
  {comparison['n_strategies']} strategies · generated {escape(comparison['generated_at'])} ·
  tax regime <code>{escape(comparison['tax_regime'])}</code> ·
  all P&amp;L figures are <strong>realized</strong> (closed trades only; open positions excluded).
</p>
{failed_html}
<h2>Leaderboard (ranked by Sharpe)</h2>
<div class="scroll"><table><thead><tr>{header_cells}</tr></thead><tbody>{''.join(body_rows)}</tbody></table></div>
<h2>Year-on-year realized return (trading year, 1 Apr – 31 Mar)</h2>
<div class="scroll">{_yearly_table(comparison)}</div>
</body></html>"""


def write_reports(
    suffix: str, reports_dir: Path = REPORTS_DIR, tax_regime: str = "ltcg_12_5pct_1_25L",
) -> Dict[str, Path]:
    comparison = build_comparison(suffix, reports_dir, tax_regime)
    # [2026-08-11] The tax regime is part of the FILENAME. It used to be absent,
    # so generating both regimes for one queue wrote the same three files twice
    # and the second silently overwrote the first — only ever one regime
    # survived on disk, and the UI's report picker (which labels entries
    # "<suffix> · <tax_regime>") could only ever offer one of them.
    #
    # Every report already carries BOTH regimes under strategies[].taxes; what
    # the regime selects is the HEADLINE post_tax_pnl_inr /
    # post_tax_return_on_capital, which is exactly what the comparison table
    # sorts and ranks on. So the two files are genuinely different documents,
    # not cosmetic variants.
    base = reports_dir / f"ta_comparison_{suffix}__{tax_regime}"
    json_path = base.with_suffix(".json")
    csv_path = base.with_suffix(".csv")
    html_path = base.with_suffix(".html")
    json_path.write_text(json.dumps(comparison, indent=2, default=str))
    write_csv(comparison, csv_path)
    html_path.write_text(render_html(comparison))
    logger.info(f"ta_comparison_report: wrote {json_path.name}, {csv_path.name}, {html_path.name}")
    return {"json": json_path, "csv": csv_path, "html": html_path}


def main() -> None:
    parser = argparse.ArgumentParser(description="Collate one TA backtest queue into a comparison report")
    parser.add_argument("--suffix", required=True, help="Queue report suffix (orchestrator_{suffix}_job*.json)")
    parser.add_argument("--reports-dir", default=str(REPORTS_DIR))
    parser.add_argument("--tax-regime", default="ltcg_12_5pct_1_25L", choices=["ltcg_12_5pct_1_25L", "ltcg_10pct_1L"])
    args = parser.parse_args()

    paths = write_reports(args.suffix, Path(args.reports_dir), args.tax_regime)
    for kind, path in paths.items():
        print(f"{kind}: {path}")


if __name__ == "__main__":
    main()


# ===========================================================================
# 2009-2026 long-history comparison dataset (added 2026-08-12)
#
# Feeds /api/v1/technical_backtest/comparison and the /technical-comparison
# dashboard. Deliberately additive: the CSV/HTML report writers above
# (write_reports, render_html, build_comparison) remain the producers for
# scripts/run_ta_5year_backtest.py and backtest_reports.py, and are untouched.
#
# [INCIDENT 2026-08-12] This module was briefly overwritten wholesale while
# adding the block below, destroying those writers and breaking
# run_ta_5year_backtest.py's `from backtest.ta_comparison_report import
# write_reports`. Restored from git and merged. If you are adding to this
# file, append -- do not rewrite it.
# ===========================================================================

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
            "fy_label": f"FY{fy_end.year}",
            "opening_equity": open_equity,
            "closing_equity": close_equity,
            "return_pct": ((close_equity / open_equity) - 1.0) * 100.0 if open_equity else None,
            "partial": fy_end == first_fy and points[0][0] != date(fy_end.year - 1, 4, 1),
        })
        prev_close = close_equity
    return out


def _basis(metrics: Dict[str, Any], want: str) -> Optional[float]:
    """The run's CAGR on the requested tax basis, or None if unavailable.

    `cagr` is on metrics["tax_basis"]; `cagr_other_basis` is the reconstructed
    opposite (A86) and is None on runs where it could not be reconstructed.
    """
    stated = metrics.get("tax_basis") or "pre_tax"
    return metrics.get("cagr") if stated == want else metrics.get("cagr_other_basis")


def _as_pct(value: Optional[float]) -> Optional[float]:
    """Fraction -> percent, preserving None rather than collapsing it to 0.0."""
    return None if value is None else value * 100.0


def rolling_returns(equity_curve: List[Dict[str, Any]],
                    windows_years: Sequence[int] = ROLLING_WINDOWS_YEARS) -> Dict[str, Any]:
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


def _monthly_equity(equity_curve: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Last observation of each calendar month, normalised to 100 at the start.

    Normalised so strategies are comparable on one axis regardless of how much
    capital each ended with — the comparison question is shape and relative
    growth, not absolute rupees.
    """
    if not equity_curve:
        return []
    by_month: Dict[str, Any] = {}
    for point in equity_curve:
        by_month[point["date"][:7]] = point  # later dates overwrite earlier
    ordered = [by_month[m] for m in sorted(by_month)]
    base = float(ordered[0]["equity"]) or 1.0
    return [
        {"date": p["date"], "index": round(float(p["equity"]) / base * 100.0, 2)}
        for p in ordered
    ]


def trade_stats(trade_log_path: Optional[str]) -> Dict[str, Any]:
    """Win/loss shape of a run, read from its own trade log.

    UNITS: trade_log's `pnl_pct` is a FRACTION, not a percentage — a -5% trade
    is stored as -0.05. Getting this wrong silently rescales every figure by
    100x, which is exactly what happened once during the exit-threshold
    investigation (a bucketing pass reported "zero stop-losses" because the
    thresholds were in percent). Everything here is converted to percent ONCE,
    at the boundary, and named `_pct` only after conversion.

    Averages are reported separately for winners and losers rather than as one
    mean, because the mean of a strategy that wins small and loses big looks
    identical to one that wins big and loses small. The payoff ratio
    (avg win / avg loss) is what actually distinguishes them, and combined with
    win rate it says whether a strategy is viable at all.
    """
    import csv
    import os
    from datetime import date as _date

    empty = {
        "n_closed": 0, "n_wins": 0, "n_losses": 0, "win_rate_pct": None,
        "avg_win_pct": None, "avg_loss_pct": None, "payoff_ratio": None,
        "avg_hold_days": None, "avg_win_hold_days": None, "avg_loss_hold_days": None,
        "best_trade_pct": None, "worst_trade_pct": None, "expectancy_pct": None,
    }
    if not trade_log_path or not os.path.exists(trade_log_path):
        return empty

    wins: List[float] = []
    losses: List[float] = []
    win_hold: List[int] = []
    loss_hold: List[int] = []
    with open(trade_log_path) as fh:
        for row in csv.DictReader(fh):
            try:
                pnl = float(row["pnl_pct"]) * 100.0  # fraction -> percent, once
                held = (_date.fromisoformat(row["sale_date"][:10])
                        - _date.fromisoformat(row["buy_date"][:10])).days
            except (KeyError, ValueError, TypeError):
                continue
            if pnl >= 0:
                wins.append(pnl)
                win_hold.append(held)
            else:
                losses.append(pnl)
                loss_hold.append(held)

    n = len(wins) + len(losses)
    if n == 0:
        return empty

    avg_win = sum(wins) / len(wins) if wins else None
    avg_loss = sum(losses) / len(losses) if losses else None
    all_hold = win_hold + loss_hold
    win_rate = 100.0 * len(wins) / n
    return {
        "n_closed": n,
        "n_wins": len(wins),
        "n_losses": len(losses),
        "win_rate_pct": win_rate,
        "avg_win_pct": avg_win,
        "avg_loss_pct": avg_loss,
        # abs() so the ratio reads naturally: >1 means winners outsize losers.
        "payoff_ratio": (avg_win / abs(avg_loss)) if (avg_win and avg_loss) else None,
        "avg_hold_days": sum(all_hold) / len(all_hold) if all_hold else None,
        "avg_win_hold_days": sum(win_hold) / len(win_hold) if win_hold else None,
        "avg_loss_hold_days": sum(loss_hold) / len(loss_hold) if loss_hold else None,
        "best_trade_pct": max(wins) if wins else None,
        "worst_trade_pct": min(losses) if losses else None,
        # Per-trade expected return: what one trade is worth on average.
        "expectancy_pct": (
            (win_rate / 100.0) * (avg_win or 0.0) + (1 - win_rate / 100.0) * (avg_loss or 0.0)
        ),
    }


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
                # Straight pass-through of metrics core/metrics.py already
                # computes. None stays None: a blank cell and a zero are
                # different facts, so these deliberately skip the `or 0.0`
                # coercion the older fields above use.
                "xirr_pct": _as_pct(metrics.get("xirr")),
                "volatility_pct": _as_pct(metrics.get("volatility")),
                "excess_return_pct": _as_pct(metrics.get("excess_return")),
                "churn_per_year": metrics.get("churn_per_year"),
                "turnover_ratio": metrics.get("turnover_ratio"),
                "n_distinct_tickers_traded": metrics.get("n_distinct_tickers_traded"),
                "benchmark_index_name": metrics.get("benchmark_index_name"),
                # A86: `cagr` is stated on ONE basis, named by tax_basis, and
                # cagr_other_basis is the reconstructed opposite. Resolve both
                # here so a consumer never has to know which way round it was
                # -- reading them the wrong way silently swaps a pre-tax figure
                # into a post-tax column.
                "tax_basis": metrics.get("tax_basis"),
                "cagr_pre_tax_pct": _as_pct(_basis(metrics, "pre_tax")),
                "cagr_post_tax_pct": _as_pct(_basis(metrics, "post_tax")),
                "total_tax_paid": metrics.get("total_tax_paid"),
                "trade_log_path": report.get("trade_log_path"),
                "trade_stats": trade_stats(report.get("trade_log_path")),
                "fy_returns": fy_returns(curve),
                "rolling_returns": rolling_returns(curve),
                # Sampled for the comparison chart. The full curve is ~4,300
                # points per run; 130 runs of that is a 2.6 MB payload the
                # browser cannot chart usefully anyway. Month-end sampling keeps
                # shape and drawdowns visible at ~5% of the size.
                "equity_monthly": _monthly_equity(curve),
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
