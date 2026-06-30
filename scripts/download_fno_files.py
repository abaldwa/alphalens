"""
scripts/download_fno_files.py

Phase: 3 (F&O Historical Data — download-only)
Specs: SPEC-PIPE-001
Owner: Platform / Ingestion

Downloads NSE F&O bhavcopy ZIPs newest-first and saves the extracted CSV
to datastore/raw/fno/{date}.csv.  No DuckDB writes — pure HTTP.

URL strategy (two formats, auto-detected per date)
---------------------------------------------------
  2024-01-01 → today : UDiFF unified bhavcopy
      https://nsearchives.nseindia.com/content/fo/
          BhavCopy_NSE_FO_0_0_0_{YYYYMMDD}_F_0000.csv.zip

  Pre-2024 (fallback) : old NSE archives bhavcopy
      https://archives.nseindia.com/content/historical/DERIVATIVES/
          {YYYY}/{MMM}/fo{DD}{MMM}{YYYY}bhav.csv.zip

Both are saved as raw CSVs.  insert_fno_files.py auto-detects the format
by column names (TckrSymb = new, SYMBOL = old).

After download, run insert_fno_files.py to bulk-insert into DuckDB.

Usage
-----
    # Default: today → 2015-01-01 (newest-first)
    .venv/bin/python3 scripts/download_fno_files.py

    # Custom range
    .venv/bin/python3 scripts/download_fno_files.py --from-date 2010-01-01

    # Background run
    nohup .venv/bin/python3 scripts/download_fno_files.py \\
        > logs/fno_download.log 2>&1 &
    tail -f logs/fno_download.log

Timing
------
  ~0.5 s/file with persistent session.
  2015→2026 ≈ 2,800 files → ~25 min.
"""

import argparse
import io
import logging
import sys
import time
import zipfile
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)

# New UDiFF format (2024+)
UDIFF_URL = (
    "https://nsearchives.nseindia.com/content/fo/"
    "BhavCopy_NSE_FO_0_0_0_{yyyymmdd}_F_0000.csv.zip"
)

# Old archive format (pre-2024)
OLD_URL = (
    "https://archives.nseindia.com/content/historical/DERIVATIVES/"
    "{yyyy}/{mmm}/fo{dd}{mmm}{yyyy}bhav.csv.zip"
)

NSE_HOME = "https://www.nseindia.com"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
SESSION_REFRESH_EVERY = 200


def _new_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "*/*", "Referer": NSE_HOME})
    try:
        s.get(NSE_HOME, timeout=15)
    except requests.RequestException as exc:
        logger.warning("NSE homepage prefetch failed: %s — continuing", exc)
    return s


def _extract_csv(content: bytes) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        csv_name = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
        with zf.open(csv_name) as fh:
            return pd.read_csv(fh)


def _download_one(session: requests.Session, d: date) -> pd.DataFrame | None:
    """
    Try UDiFF URL first; fall back to old archive URL on 404.
    Returns None if both return 404 (holiday / non-trading day).
    """
    # --- Try new UDiFF format ---
    url_new = UDIFF_URL.format(yyyymmdd=d.strftime("%Y%m%d"))
    r = session.get(url_new, timeout=25)
    if r.status_code == 200:
        return _extract_csv(r.content)
    if r.status_code != 404:
        r.raise_for_status()

    # --- Fall back to old archive format ---
    mmm = d.strftime("%b").upper()   # JAN, FEB, …
    dd  = d.strftime("%d")           # 02, 15, …
    yyyy = d.strftime("%Y")
    url_old = OLD_URL.format(yyyy=yyyy, mmm=mmm, dd=dd)
    r2 = session.get(url_old, timeout=25)
    if r2.status_code == 200:
        return _extract_csv(r2.content)
    if r2.status_code == 404:
        return None          # Holiday / weekend missed by weekday filter
    r2.raise_for_status()
    return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download NSE F&O bhavcopy files (no DB writes). "
                    "Tries UDiFF URL first, falls back to old archive format."
    )
    parser.add_argument("--from-date", default="2015-01-01", metavar="YYYY-MM-DD",
                        help="Earliest date to attempt (default: 2015-01-01)")
    parser.add_argument("--sleep", type=float, default=0.4,
                        help="Seconds between requests (default: 0.4)")
    args = parser.parse_args()

    from config.settings import RAW_DIR
    out_dir = Path(RAW_DIR) / "fno"
    out_dir.mkdir(parents=True, exist_ok=True)

    from_dt = date.fromisoformat(args.from_date)
    today = date.today()

    # Build weekday list newest-first
    all_weekdays: list[date] = []
    d = today
    while d >= from_dt:
        if d.weekday() < 5:
            all_weekdays.append(d)
        d -= timedelta(days=1)

    # Skip dates whose CSV is already saved
    pending = [d for d in all_weekdays if not (out_dir / f"{d.isoformat()}.csv").exists()]
    already_done = len(all_weekdays) - len(pending)

    logger.info(
        "F&O download: %s → %s | weekdays=%d already_saved=%d to_fetch=%d",
        from_dt, today, len(all_weekdays), already_done, len(pending),
    )
    if not pending:
        logger.info("Nothing to do — all files already saved.")
        return

    session = _new_session()
    session_req_count = 0
    ok_new = ok_old = skipped = err = 0
    t_start = time.monotonic()

    for i, d in enumerate(pending, start=1):
        if session_req_count > 0 and session_req_count % SESSION_REFRESH_EVERY == 0:
            logger.info("Refreshing NSE session (after %d requests)…", session_req_count)
            session = _new_session()

        try:
            df = _download_one(session, d)
            session_req_count += 1

            if df is None:
                skipped += 1
                time.sleep(args.sleep * 0.3)
                continue

            out_path = out_dir / f"{d.isoformat()}.csv"
            df.to_csv(out_path, index=False)

            # Track which format was used (by column presence)
            if "TckrSymb" in df.columns:
                ok_new += 1
            else:
                ok_old += 1

            if i <= 5 or i % 100 == 0 or i == len(pending):
                elapsed = time.monotonic() - t_start
                rate = i / elapsed
                eta_min = (len(pending) - i) / rate / 60 if rate > 0 else 0
                logger.info(
                    "[%d/%d] saved %s (%d rows) | new=%d old=%d skip=%d err=%d | %.1f f/s ETA~%.0f min",
                    i, len(pending), d, len(df), ok_new, ok_old, skipped, err, rate, eta_min,
                )

        except Exception as exc:
            err += 1
            logger.warning("[%d/%d] %s FAILED: %s", i, len(pending), d, exc)
            try:
                session = _new_session()
                session_req_count = 0
            except Exception:
                pass

        time.sleep(args.sleep)

    elapsed_min = (time.monotonic() - t_start) / 60
    total_saved = len(list(out_dir.glob("*.csv")))
    logger.info("─" * 60)
    logger.info("Download complete in %.1f min", elapsed_min)
    logger.info("  UDiFF (2024+) : %d", ok_new)
    logger.info("  Old archive   : %d", ok_old)
    logger.info("  Skipped/404   : %d (holidays + true non-trading days)", skipped)
    logger.info("  Errors        : %d", err)
    logger.info("  Total CSVs in %s: %d", out_dir, total_saved)


if __name__ == "__main__":
    main()
