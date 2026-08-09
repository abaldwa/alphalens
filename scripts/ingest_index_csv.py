#!/usr/bin/env python3
"""
scripts/ingest_index_csv.py

Phase: Technical feature backfill (2007-2026)
Owner: Platform / Ingestion
Consumers: index_ohlcv -> features/matrix_builder.py (Category 7 relative
           strength: rs_vs_nifty50_21d / rs_vs_nifty100_21d /
           rs_vs_nifty500_21d, beta_63d, alpha_21d)

Loads NSE's own "Historical PR" index CSV exports into index_ohlcv. These
cover history the daily ind_close_all scraper cannot reach:

  * Nifty 100 had NO rows at all — it was missing from
    ingestion/scrapers/nse_indices.py::TRACKED_INDICES, so the daily filter
    dropped it every day (fixed 2026-08-09, which fixes FUTURE dates only).
  * Nifty 50's real series in index_ohlcv starts 2012-03-01, leaving
    1,468 trading days of 2006-2012 uncovered.

Without these, Category 7 falls back to ETF proxies (NIF100BEES lists 2015,
MONIFTY500 lists 2023) and is simply empty for the earlier years.

Input format (as exported by NSE, one file per financial year):
    "Index Name","Date","Open","High","Low","Close"
    "NIFTY 100","30 Mar 2007","3683.25","3711.00","3675.70","3701.55"

There is no volume column — an index has no traded volume of its own, so
`volume` is left NULL rather than filled with a fabricated 0 (a 0 would be
indistinguishable from a real no-trade day to any downstream consumer).

Idempotent: rows are UPSERTed on (date, index_name), so re-running is safe
and overlapping files (e.g. a duplicated download) cannot create dupes.
Existing rows are only overwritten when --overwrite is passed; by default
an already-present (date, index_name) is left untouched, so this can never
silently clobber scraped data with CSV data.

Usage
-----
    # Ingest every NIFTY *_Historical_PR_*.csv in the project root
    PYTHONPATH=$PWD .venv/bin/python scripts/ingest_index_csv.py --glob 'NIFTY*_Historical_PR_*.csv'

    # Dry run (parse + report, write nothing)
    PYTHONPATH=$PWD .venv/bin/python scripts/ingest_index_csv.py --glob '...' --dry-run
"""

import argparse
import glob as globmod
import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# NSE's export writes the index name in upper case ("NIFTY 100"); index_ohlcv
# and every consumer use the canonical mixed-case form ("Nifty 100"). Mapped
# explicitly rather than title-cased, so an unexpected name fails loudly
# instead of being silently written under a name nothing queries.
_INDEX_NAME_MAP = {
    "NIFTY 50": "Nifty 50",
    "NIFTY 100": "Nifty 100",
    "NIFTY 500": "Nifty 500",
}


def parse_csv(path: Path) -> pd.DataFrame:
    """One NSE historical-PR CSV -> (date, index_name, open, high, low, close)."""
    df = pd.read_csv(path)
    df.columns = [c.strip().strip('"').lower().replace(" ", "_") for c in df.columns]
    required = {"index_name", "date", "open", "high", "low", "close"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{path.name}: missing column(s) {sorted(missing)} — got {list(df.columns)}")

    raw_names = set(df["index_name"].str.strip().unique())
    unknown = raw_names - set(_INDEX_NAME_MAP)
    if unknown:
        raise ValueError(f"{path.name}: unmapped index name(s) {sorted(unknown)} — add to _INDEX_NAME_MAP")

    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df["date"].str.strip(), format="%d %b %Y", errors="coerce").dt.date
    out["index_name"] = df["index_name"].str.strip().map(_INDEX_NAME_MAP)
    for col in ("open", "high", "low", "close"):
        # NSE thousands separators ("1,234.50") make these read as strings.
        out[col] = pd.to_numeric(
            df[col].astype(str).str.replace(",", "", regex=False).str.strip(), errors="coerce"
        )

    bad_dates = int(out["date"].isna().sum())
    if bad_dates:
        logger.warning(f"{path.name}: {bad_dates} row(s) had an unparseable date — dropped")
        out = out[out["date"].notna()]
    bad_close = int(out["close"].isna().sum())
    if bad_close:
        logger.warning(f"{path.name}: {bad_close} row(s) had no usable close — dropped")
        out = out[out["close"].notna()]
    return out.reset_index(drop=True)


def ingest(paths, dry_run: bool = False, overwrite: bool = False) -> None:
    frames = []
    for p in sorted(paths):
        df = parse_csv(Path(p))
        if df.empty:
            logger.warning(f"{Path(p).name}: no usable rows")
            continue
        frames.append(df)
        logger.info(f"{Path(p).name}: {len(df)} rows, {df['date'].min()} .. {df['date'].max()}")

    if not frames:
        raise SystemExit("no usable rows parsed from any input file")

    combined = pd.concat(frames, ignore_index=True)
    before = len(combined)
    # Overlapping/duplicated downloads (e.g. "... (1).csv") are expected —
    # keep one row per (date, index_name).
    combined = combined.drop_duplicates(subset=["date", "index_name"], keep="last")
    if before != len(combined):
        logger.info(f"dropped {before - len(combined)} duplicate (date, index_name) row(s) across files")

    for name, grp in combined.groupby("index_name"):
        logger.info(f"TOTAL {name}: {len(grp)} rows, {grp['date'].min()} .. {grp['date'].max()}")

    if dry_run:
        logger.info("--dry-run: nothing written")
        return

    conflict = (
        "DO UPDATE SET open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close"
        if overwrite
        else "DO NOTHING"
    )
    with get_duckdb_connection(DUCKDB_PATH, read_only=False, persist=False) as conn:
        pre = {
            n: c for n, c in conn.execute(
                "SELECT index_name, COUNT(*) FROM index_ohlcv GROUP BY 1"
            ).fetchall()
        }
        conn.register("_idx_csv", combined)
        conn.execute(
            f"""
            INSERT INTO index_ohlcv (date, index_name, open, high, low, close, volume)
            SELECT date, index_name, open, high, low, close, NULL FROM _idx_csv
            ON CONFLICT (date, index_name) {conflict}
            """
        )
        conn.unregister("_idx_csv")
        conn.commit()
        post = {
            n: c for n, c in conn.execute(
                "SELECT index_name, COUNT(*) FROM index_ohlcv GROUP BY 1"
            ).fetchall()
        }
    for name in sorted(set(pre) | set(post)):
        delta = post.get(name, 0) - pre.get(name, 0)
        if delta:
            logger.info(f"{name}: {pre.get(name, 0)} -> {post.get(name, 0)} rows (+{delta})")


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest NSE historical index PR CSVs into index_ohlcv")
    parser.add_argument("--glob", required=True, help="Glob for input CSVs, e.g. 'NIFTY*_Historical_PR_*.csv'")
    parser.add_argument("--dry-run", action="store_true", help="Parse and report, write nothing")
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Overwrite an existing (date, index_name) row. Default leaves scraped rows untouched.",
    )
    args = parser.parse_args()

    paths = globmod.glob(args.glob)
    if not paths:
        raise SystemExit(f"no files matched {args.glob!r}")
    logger.info(f"matched {len(paths)} file(s)")
    ingest(paths, dry_run=args.dry_run, overwrite=args.overwrite)


if __name__ == "__main__":
    main()
