"""
scripts/build_momentum_benchmark_db.py

Phase: FeatureBacklog.md ML38 — momentum strategy YoY report
Owner: Platform / Backtest
Consumers: scripts/build_momentum_yoy_report.py

2026-07-18 user request: Nifty Midcap 150 / Nifty Smallcap 250 returns
alongside the momentum strategy's own year-on-year returns. These indices
were never in the tracked-index allowlist (ingestion/scrapers/nse_indices.
TRACKED_INDICES), so they're not in the production index_ohlcv table —
but the raw daily NSE indices-close CSVs already on disk under
datastore/raw/nse_indices/ (2023-07-03 onward, ~753 files) DO contain
them (NSE publishes ~80 indices per file, only a subset is loaded).

NSE's archive host is currently blocking fetches (403/503) from this
environment, so a live backfill further back than 2023-07 isn't possible
this session (user 2026-07-18 decision: proceed now with what's on disk,
revisit backfilling older years later).

Writes a standalone local DuckDB (NOT the production DB — this is
scratch/analysis data per the user's "keep in temp tables" request) with
one table: benchmark_index(date, index_name, close).
"""

import logging
from pathlib import Path

import duckdb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

RAW_DIR = Path(__file__).resolve().parent.parent / "datastore" / "raw" / "nse_indices"
OUT_DB = Path(__file__).resolve().parent.parent / "backtest" / "reports" / "momentum" / "momentum_yoy.duckdb"

TRACKED = {
    "Nifty Midcap 150": "nifty_midcap_150",
    "Nifty Smallcap 250": "nifty_smallcap_250",
    "Nifty 500": "nifty_500",
    "Nifty 50": "nifty_50",
}


def build() -> pd.DataFrame:
    rows = []
    files = sorted(RAW_DIR.glob("*.csv"))
    logger.info("Scanning %d raw NSE index files under %s", len(files), RAW_DIR)
    for path in files:
        try:
            df = pd.read_csv(path)
        except Exception as exc:
            logger.warning("Skipping unreadable %s: %s", path, exc)
            continue
        df.columns = [c.strip() for c in df.columns]
        if "Index Name" not in df.columns or "Closing Index Value" not in df.columns:
            continue
        df["Index Name"] = df["Index Name"].str.strip()
        hit = df[df["Index Name"].isin(TRACKED.keys())]
        for _, r in hit.iterrows():
            try:
                close = float(r["Closing Index Value"])
            except (ValueError, TypeError):
                continue
            rows.append({
                "date": pd.to_datetime(r["Index Date"], format="%d-%m-%Y").date().isoformat(),
                "index_name": TRACKED[r["Index Name"]],
                "close": close,
            })
    out = pd.DataFrame(rows).drop_duplicates(subset=["date", "index_name"]).sort_values(["index_name", "date"])
    logger.info("Parsed %d (date, index) rows across %d indices", len(out), out["index_name"].nunique())
    return out


def main():
    df = build()
    OUT_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(OUT_DB))
    conn.execute("DROP TABLE IF EXISTS benchmark_index")
    conn.execute("CREATE TABLE benchmark_index (date DATE, index_name VARCHAR, close DOUBLE)")
    conn.register("df_view", df)
    conn.execute("INSERT INTO benchmark_index SELECT date::DATE, index_name, close FROM df_view")
    counts = conn.execute("SELECT index_name, min(date), max(date), count(*) FROM benchmark_index GROUP BY 1 ORDER BY 1").fetchall()
    for row in counts:
        logger.info("  %s: %s..%s (%d rows)", *row)
    conn.close()
    logger.info("Wrote %s", OUT_DB)


if __name__ == "__main__":
    main()
