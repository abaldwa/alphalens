"""
scripts/probe_nse_ca_symbol_endpoint.py

Phase: A20 follow-up (2026-07-30), throwaway investigation script

Tests a hypothesis for why some actively-traded tickers (GUJGASLTD,
AKZOINDIA, HIL, SWANENERGY — confirmed not ticker-rename cases, see
scripts/detect_ticker_renames.py) ended up with zero corporate_actions
rows despite scripts/backfill_corporate_actions.py's full 2006-today
quarterly backfill: does NSE's corporates-corporateActions endpoint
return more when queried with a symbol= param for one ticker at a time,
versus the bulk index=equities&from_date=...&to_date=... query
ingestion/scrapers/corporate_actions.py and scripts/backfill_corporate_actions.py
use exclusively today?

Not a pipeline change — prints a comparison and exits. If this confirms
the bulk endpoint silently drops some symbols, the real fix (a per-symbol
retry whenever datastore/integrity/checks.py's
check_corporate_actions_coverage flags a ticker as fully empty) is future
work, not built here.

Usage
-----
    .venv/bin/python3 scripts/probe_nse_ca_symbol_endpoint.py
    .venv/bin/python3 scripts/probe_nse_ca_symbol_endpoint.py --tickers GUJGASLTD,AKZOINDIA
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests

NSE_CA_URL = "https://www.nseindia.com/api/corporates-corporateActions"
NSE_HOMEPAGE_URL = "https://www.nseindia.com"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)
DEFAULT_TICKERS = ["GUJGASLTD", "AKZOINDIA", "HIL", "SWANENERGY"]


def _nse_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "*/*"})
    s.get(NSE_HOMEPAGE_URL, timeout=15)
    return s


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe NSE's corporate-actions API with symbol= vs bulk queries")
    parser.add_argument("--tickers", default=",".join(DEFAULT_TICKERS))
    args = parser.parse_args()
    tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]

    session = _nse_session()

    for ticker in tickers:
        print(f"\n=== {ticker} ===")
        try:
            r = session.get(NSE_CA_URL, params={"index": "equities", "symbol": ticker}, timeout=20)
            r.raise_for_status()
            payload = r.json()
            records = payload if isinstance(payload, list) else payload.get("data", [])
            print(f"symbol= query: {len(records)} record(s)")
            for rec in records[:5]:
                print(f"  {rec.get('exDate')}  {rec.get('series')}  {rec.get('purpose') or rec.get('subject')}")
            if len(records) > 5:
                print(f"  ... and {len(records) - 5} more")
        except Exception as exc:  # noqa: BLE001
            print(f"symbol= query FAILED: {exc}")


if __name__ == "__main__":
    main()
