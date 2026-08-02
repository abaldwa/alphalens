"""
scripts/precompute_technical_screener_matches.py

Phase: Technical Analysis sweep performance (2026-08-02)
Owner: Platform / Backtest
Consumers: operator CLI (`python3 -m scripts.precompute_technical_screener_matches`),
    backtest/adapters/technical_adapter.py's precomputed_matches_dir

Entry-signal generation (ScreenerEngine.screen() — "which tickers does
template X match on date Y") depends ONLY on (template, date), never on
top_n/exit_variant/max_hold_days. The full Technical sweep grid is 42
templates x 7 exit variants x 4 hold-days x 3 top_n = 2,772 jobs — every
one of the ~66 variant jobs per template was independently re-screening
the exact same (template, date) pairs from scratch (confirmed via
profiling, see FeatureBacklog.md 2026-08-02 entry).

backtest/core/screener_cache.py already solves this exact problem for
live/sequential runs via a DuckDB table + a live BACKTEST_DUCKDB_PATH
connection — but that connection is unsafe to hold open during the
multi-worker parallel compute phase (defer_db_writes=True): DuckDB does
not allow a read-only connection to open on a file while another process
holds a read-write connection to it, and every parallel job's short final
save tail does briefly hold one. Reusing screener_cache.py as-is there
would reintroduce the exact cross-job DB contention defer_db_writes was
built to eliminate.

This script instead does a ONE-TIME precompute pass per template, writing
plain per-template Parquet files — the same "many processes read one
Parquet file concurrently" pattern already safely used for
FEATURES_DAILY_DIR (no DuckDB connection, no locking question at all).
Every later sweep job reads its template's small file once (via
TechnicalAdapter's precomputed_matches_dir) instead of ever calling
screen() live for entry signals.

Run once before a sweep:
    python3 -m scripts.precompute_technical_screener_matches \\
        --start-date 2016-08-04 --end-date 2026-08-02

Safe to re-run: a template is skipped if its output already covers the
requested date range (idempotent, not a partial-date patcher).
"""

import argparse
import json
import logging
import time
from datetime import date as date_type
from pathlib import Path
from typing import List, Optional

import pandas as pd

from backtest.core.screener_cache import _CACHE_POPULATE_LIMIT
from backtest.run_orchestrator_backtest import _fetch_real_ohlcv
from systems.technical_analysis.screener.engine import ScreenerEngine
from systems.technical_analysis.screener.templates import TEMPLATES

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent.parent / "backtest" / "reports" / "technical" / "screener_cache"

_PARQUET_COLUMNS = ["date", "ticker", "matched_conditions", "total_conditions", "score", "key_values_json"]


def _trading_days(start_date: date_type, end_date: date_type) -> List[str]:
    """Same real trading-day calendar every real orchestrator job derives
    (backtest/run_orchestrator_backtest.py::_build_config —
    pd.DatetimeIndex(sorted(ohlcv["date"].unique()))) — no second
    calendar implementation. min_history_days=1/max_tickers=None so
    virtually every real trading day in the window is captured regardless
    of any one ticker's own listing history."""
    ohlcv = _fetch_real_ohlcv(max_tickers=None, min_history_days=1, start_date=start_date, end_date=end_date)
    days = sorted({d.date().isoformat() for d in pd.DatetimeIndex(ohlcv["date"].unique())})
    return days


def _already_covers(output_dir: Path, template_name: str, start_date: date_type, end_date: date_type) -> bool:
    parquet_path = output_dir / f"{template_name}.parquet"
    manifest_path = output_dir / f"{template_name}.manifest.json"
    if not parquet_path.exists() or not manifest_path.exists():
        return False
    try:
        manifest = json.loads(manifest_path.read_text())
    except (json.JSONDecodeError, OSError):
        return False
    return manifest.get("start_date") is not None and manifest.get("start_date") <= start_date.isoformat() \
        and manifest.get("end_date") >= end_date.isoformat()


def precompute_template(
    template_name: str, trading_days: List[str], output_dir: Path,
    start_date: date_type, end_date: date_type, engine: Optional[ScreenerEngine] = None,
) -> int:
    """Walks `trading_days` once for `template_name`, writing
    {template_name}.parquet + .manifest.json to output_dir. Returns the
    number of (date, ticker) match rows written."""
    engine = engine or ScreenerEngine()
    rows = []
    for date_str in trading_days:
        results = engine.screen(template_name, date=date_str, limit=_CACHE_POPULATE_LIMIT)
        for r in results:
            rows.append((
                date_str, r.ticker, r.matched_conditions, r.total_conditions, r.score,
                json.dumps(r.key_values, default=str),
            ))

    output_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows, columns=_PARQUET_COLUMNS)
    # date stored as plain string (object dtype) — deliberately NOT cast
    # to a pyarrow date/timestamp type, so a reader gets back exactly the
    # same string it wrote without a round-trip-type surprise (see
    # TechnicalAdapter._load_precomputed_matches's defensive str() note).
    df["date"] = df["date"].astype(str)
    df.to_parquet(output_dir / f"{template_name}.parquet", index=False)

    manifest = {
        "template_name": template_name,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "trading_days": trading_days,
    }
    (output_dir / f"{template_name}.manifest.json").write_text(json.dumps(manifest))
    return len(rows)


def run_precompute(
    start_date: date_type, end_date: date_type, templates: Optional[List[str]] = None,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> None:
    template_names = templates or [t.name for t in TEMPLATES]
    trading_days = _trading_days(start_date, end_date)
    logger.info(
        f"precompute_technical_screener_matches: {len(template_names)} templates x "
        f"{len(trading_days)} trading days [{start_date} .. {end_date}]"
    )

    for i, template_name in enumerate(template_names, start=1):
        if _already_covers(output_dir, template_name, start_date, end_date):
            logger.info(f"[{i}/{len(template_names)}] {template_name}: already covers this range, skipping")
            continue
        t0 = time.monotonic()
        n_rows = precompute_template(template_name, trading_days, output_dir, start_date, end_date)
        elapsed = time.monotonic() - t0
        logger.info(
            f"[{i}/{len(template_names)}] {template_name}: {len(trading_days)} trading days, "
            f"{n_rows} matches, {elapsed:.1f}s"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Precompute Technical screener matches once per template for sweep reuse")
    parser.add_argument("--start-date", type=date_type.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date_type.fromisoformat, required=True)
    parser.add_argument("--templates", default=None, help="Comma-separated template names (default: all 42)")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    templates = args.templates.split(",") if args.templates else None
    run_precompute(args.start_date, args.end_date, templates=templates, output_dir=args.output_dir)


if __name__ == "__main__":
    main()
