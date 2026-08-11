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
import json
import logging
from datetime import datetime
from html import escape
from pathlib import Path
from typing import Any, Dict, List, Optional

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
    from systems.technical_analysis.screener.templates import TEMPLATE_STYLE

    bench_cache: Dict[tuple, Optional[float]] = {}

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
        row["style"] = TEMPLATE_STYLE.get(row.get("template_name"), "Unknown")
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
