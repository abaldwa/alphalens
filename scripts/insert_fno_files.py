"""
scripts/insert_fno_files.py

Phase: 3 (F&O Historical Data — bulk insert)
Specs: SPEC-PIPE-001
Owner: Platform / Ingestion

Reads all pre-downloaded CSVs from datastore/raw/fno/ and bulk-inserts
them into fno_data using a SINGLE DuckDB connection.

Handles both NSE bhavcopy formats automatically (detected by column names):

  New UDiFF (2024+)  — TckrSymb, FinInstrmTp, XpryDt, StrkPric, OptnTp,
                        OpnIntrst, ChngInOpnIntrst, TtlTradgVol,
                        SttlmPric, ClsPric, UndrlygPric

  Old archive (pre-2024) — SYMBOL, INSTRUMENT, EXPIRY_DT, STRIKE_PR,
                            OPTION_TYP, OPEN_INT, CHG_IN_OI, CONTRACTS,
                            SETTLE_PR, CLOSE  (no underlying price)

Why this is fast
-----------------
  - One DuckDB connection for the entire run
  - Commits in batches of --batch-size dates
  - Skips dates already in fno_data (safe to resume)

Prerequisites
-------------
  - Run download_fno_files.py first
  - No API server needed (writes directly to DuckDB)
  - Stop uvicorn and any other DuckDB writer before running

Usage
-----
    .venv/bin/python3 scripts/insert_fno_files.py

    # Dry-run: parse + count, no DB writes
    .venv/bin/python3 scripts/insert_fno_files.py --dry-run

    # Test with 10 most-recent dates
    .venv/bin/python3 scripts/insert_fno_files.py --limit 10

    # Background
    nohup .venv/bin/python3 scripts/insert_fno_files.py \\
        > logs/fno_insert.log 2>&1 &

Timing
------
  ~0.3-0.5 s/date → ~15 min for 2,800 dates.
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)

# Old-format instrument codes → UDiFF codes
_INSTR_MAP = {
    "FUTIDX": "IDF",
    "FUTSTK": "STF",
    "OPTIDX": "IDO",
    "OPTSTK": "STO",
}

BATCH_SIZE_DEFAULT = 50


def _parse_new_format(raw: pd.DataFrame, trade_date: str, universe: set) -> pd.DataFrame:
    """Parse UDiFF format (2024+). Returns a DataFrame ready for DuckDB insert."""
    for col in ("TckrSymb", "FinInstrmTp", "OptnTp"):
        if col in raw.columns and raw[col].dtype == object:
            raw[col] = raw[col].str.strip()

    df = pd.DataFrame({
        "trade_date":       trade_date,
        "ticker":           raw["TckrSymb"],
        "instrument":       raw["FinInstrmTp"],
        "expiry":           pd.to_datetime(raw["XpryDt"], errors="coerce").dt.strftime("%Y-%m-%d"),
        "strike":           pd.to_numeric(raw["StrkPric"], errors="coerce"),
        "option_type":      raw["OptnTp"].where(raw["OptnTp"].notna(), None),
        "oi":               pd.to_numeric(raw["OpnIntrst"], errors="coerce"),
        "oi_change":        pd.to_numeric(raw["ChngInOpnIntrst"], errors="coerce"),
        "volume":           pd.to_numeric(raw["TtlTradgVol"], errors="coerce"),
        "settle_price":     pd.to_numeric(raw["SttlmPric"], errors="coerce"),
        "close_price":      pd.to_numeric(raw["ClsPric"], errors="coerce"),
        "underlying_price": pd.to_numeric(raw["UndrlygPric"], errors="coerce"),
    })

    if universe:
        df = df[df["ticker"].isin(universe)]
    return df


def _parse_old_format(raw: pd.DataFrame, trade_date: str, universe: set) -> pd.DataFrame:
    """Parse old NSE archive format (pre-2024). Returns a DataFrame ready for DuckDB insert."""
    for col in ("INSTRUMENT", "SYMBOL", "OPTION_TYP"):
        if col in raw.columns and raw[col].dtype == object:
            raw[col] = raw[col].str.strip()

    expiry = pd.to_datetime(raw["EXPIRY_DT"], format="%d-%b-%Y", errors="coerce").dt.strftime("%Y-%m-%d")
    instrument = raw["INSTRUMENT"].map(lambda x: _INSTR_MAP.get(x, x))
    opt_type = raw["OPTION_TYP"].map(lambda x: None if str(x).strip() in ("XX", "-", "") else str(x).strip())
    strike = pd.to_numeric(raw["STRIKE_PR"], errors="coerce").where(lambda s: s > 0)

    df = pd.DataFrame({
        "trade_date":       trade_date,
        "ticker":           raw["SYMBOL"],
        "instrument":       instrument,
        "expiry":           expiry,
        "strike":           strike,
        "option_type":      opt_type,
        "oi":               pd.to_numeric(raw["OPEN_INT"], errors="coerce"),
        "oi_change":        pd.to_numeric(raw["CHG_IN_OI"], errors="coerce"),
        "volume":           pd.to_numeric(raw["CONTRACTS"], errors="coerce"),
        "settle_price":     pd.to_numeric(raw["SETTLE_PR"], errors="coerce"),
        "close_price":      pd.to_numeric(raw["CLOSE"], errors="coerce"),
        "underlying_price": pd.Series([None] * len(raw), dtype="float64"),
    })

    if universe:
        df = df[df["ticker"].isin(universe)]
    return df


def _parse_csv(csv_path: Path, trade_date: str, universe: set) -> tuple[pd.DataFrame, str]:
    """Auto-detect format, return (DataFrame, format_label)."""
    try:
        raw = pd.read_csv(csv_path, low_memory=False)
    except Exception as exc:
        raise ValueError(f"Cannot read {csv_path.name}: {exc}") from exc

    if "TckrSymb" in raw.columns:
        return _parse_new_format(raw, trade_date, universe), "new"
    elif "SYMBOL" in raw.columns:
        return _parse_old_format(raw, trade_date, universe), "old"
    else:
        raise ValueError(f"Unrecognised CSV format in {csv_path.name}; "
                         f"columns: {list(raw.columns)[:8]}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bulk-insert pre-downloaded F&O CSVs into fno_data (single DuckDB connection)"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse + count but do not write to DB")
    parser.add_argument("--limit", type=int, default=None,
                        help="Process at most N files, newest-first (for testing)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE_DEFAULT,
                        help=f"Commit every N dates (default: {BATCH_SIZE_DEFAULT})")
    parser.add_argument("--universe-only", action="store_true",
                        help="Filter to Nifty 500 universe only (default: all tickers)")
    parser.add_argument("--publish-mode", choices=["direct", "staged"], default="direct",
                        help="'direct' (default): per-date DELETE+INSERT, unchanged legacy path. "
                             "'staged' (A25): stage the whole batch through datastore/staging/gate.py "
                             "and publish atomically once at the end via datastore/staging/publish.py — "
                             "see FeatureBacklog.md A25.")
    args = parser.parse_args()

    from config.settings import DUCKDB_PATH, RAW_DIR
    raw_dir = Path(RAW_DIR) / "fno"

    csv_files = sorted(raw_dir.glob("*.csv"), reverse=True)   # newest-first
    if not csv_files:
        logger.error("No CSVs found in %s — run download_fno_files.py first", raw_dir)
        sys.exit(1)
    logger.info("Found %d CSVs in %s", len(csv_files), raw_dir)

    universe: set = set()
    if args.universe_only:
        from config.universe import get_tickers
        universe = set(get_tickers())
        logger.info("Universe filter: %d tickers", len(universe))

    from datastore.api.db import _attach_fno_db

    conn = duckdb.connect(str(DUCKDB_PATH))
    _attach_fno_db(conn, read_only=False)
    try:
        existing_dates = {
            r[0] for r in conn.execute(
                "SELECT DISTINCT CAST(trade_date AS VARCHAR) FROM fno_data"
            ).fetchall()
        }
        logger.info("fno_data already has %d dates", len(existing_dates))

        pending = [f for f in csv_files if f.stem not in existing_dates]
        if args.limit:
            pending = pending[:args.limit]
        logger.info("%d dates to insert (dry_run=%s)", len(pending), args.dry_run)

        if not pending:
            logger.info("Nothing to do.")
            return

        ok = skipped = err = 0
        fmt_counts = {"new": 0, "old": 0}
        total_rows = 0
        t_start = time.monotonic()
        staged_batches = []  # A25 staged mode: accumulated instead of per-date DELETE+INSERT

        for i, csv_path in enumerate(pending, start=1):
            trade_date = csv_path.stem
            try:
                df, fmt = _parse_csv(csv_path, trade_date, universe)
                if df.empty:
                    skipped += 1
                    continue

                if not args.dry_run:
                    if args.publish_mode == "staged":
                        staged_batches.append(df)
                    else:
                        conn.execute("DELETE FROM fno_data WHERE trade_date = ?", [trade_date])
                        # Register DataFrame as a virtual table and INSERT in one SQL call —
                        # orders of magnitude faster than executemany for 40-50k rows.
                        conn.register("_fno_batch", df)
                        conn.execute("""
                            INSERT INTO fno_data
                                (trade_date, ticker, instrument, expiry, strike, option_type,
                                 oi, oi_change, volume, settle_price, close_price, underlying_price)
                            SELECT trade_date, ticker, instrument, expiry, strike, option_type,
                                   oi, oi_change, volume, settle_price, close_price, underlying_price
                            FROM _fno_batch
                        """)
                        conn.unregister("_fno_batch")
                        if i % args.batch_size == 0:
                            conn.commit()

                ok += 1
                fmt_counts[fmt] += 1
                total_rows += len(df)

                if i <= 3 or i % 100 == 0 or i == len(pending):
                    elapsed = time.monotonic() - t_start
                    rate = i / elapsed
                    eta_min = (len(pending) - i) / rate / 60 if rate > 0 else 0
                    logger.info(
                        "[%d/%d] %s [%s]: %d rows | total=%d | %.1f d/s ETA~%.0f min",
                        i, len(pending), trade_date, fmt, len(df), total_rows, rate, eta_min,
                    )

            except Exception as exc:
                logger.warning("[%d/%d] %s FAILED: %s", i, len(pending), trade_date, exc)
                err += 1

        if not args.dry_run and args.publish_mode == "staged" and staged_batches:
            from datastore.staging.gate import null_check_validator, stage_via_sql
            from datastore.staging.publish import publish_fno_data, publish_run_lock

            # fno_data is 100M+ rows — merge entirely inside DuckDB
            # (stage_via_sql) rather than round-tripping the whole
            # production table through pandas (confirmed live: doing that
            # here pushed the process to 8GB+ RSS and into swap; see
            # datastore/staging/gate.py::stage_via_sql's docstring).
            new_df = pd.concat(staged_batches, ignore_index=True)
            new_dates = list(new_df["trade_date"].unique())
            placeholders = ", ".join("?" * len(new_dates))
            merge_sql = (
                f"SELECT * FROM fno_data WHERE trade_date NOT IN ({placeholders}) "
                "UNION ALL SELECT * FROM _stage_new_batch"
            )

            with publish_run_lock() as acquired:
                if not acquired:
                    logger.error("Another publish is in progress — aborting staged publish.")
                    sys.exit(1)
                result = stage_via_sql(
                    conn, "fno_data", new_df, merge_sql, new_dates,
                    validators=[null_check_validator(["trade_date", "ticker"])],
                )
                if not result.ok:
                    logger.error("Staging gate rejected the entire new batch — nothing published.")
                    sys.exit(1)
                published_rows = publish_fno_data(conn)
                logger.info(
                    "Staged publish: %d new rows staged, %d rejected, %d now in fno_data",
                    result.staged_rows, result.rejected_rows, published_rows,
                )

        if not args.dry_run:
            conn.commit()

        elapsed_min = (time.monotonic() - t_start) / 60
        logger.info("─" * 60)
        logger.info("Insert complete in %.1f min", elapsed_min)
        logger.info("  Dates inserted : %d (new-fmt=%d old-fmt=%d)", ok, fmt_counts["new"], fmt_counts["old"])
        logger.info("  Dates skipped  : %d (empty after filtering)", skipped)
        logger.info("  Errors         : %d", err)
        logger.info("  Total rows     : %d", total_rows)

        if not args.dry_run:
            n_dates, n_rows = conn.execute(
                "SELECT COUNT(DISTINCT trade_date), COUNT(*) FROM fno_data"
            ).fetchone()
            logger.info("  fno_data now   : %d rows across %d dates", n_rows, n_dates)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
