"""
scripts/apply_company_metadata_enrichment.py

Phase: Backlog #31 follow-up (2026-07-04); BSE source added 2026-09-05

Merges config/company_metadata_enrichment_progress.csv (screener.in) and
config/company_metadata_enrichment_bse_progress.csv (BSE fallback, added
2026-09-05 — see ingestion/scrapers/screener_sector_lookup.py) into
config/nifty500_universe.csv's company_name/sector columns, then
regenerates config/tickers_missing_company_name.csv so it only lists
tickers still genuinely unresolved by either source.

Screener.in takes priority where both resolved the same ticker (it's the
longer-verified source); BSE only fills a ticker screener.in didn't
resolve at all — this only affects the ~9% of the backlog where screener
had no exact match but BSE did, not a systematic override.

Kept as a separate apply step (not done automatically by either
enrichment script) so the resolved rows can be spot-checked before
touching the real universe file.
"""

import csv
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
UNIVERSE_CSV = PROJECT_ROOT / "config" / "nifty500_universe.csv"
PROGRESS_CSV = PROJECT_ROOT / "config" / "company_metadata_enrichment_progress.csv"
BSE_PROGRESS_CSV = PROJECT_ROOT / "config" / "company_metadata_enrichment_bse_progress.csv"
MISSING_CSV = PROJECT_ROOT / "config" / "tickers_missing_company_name.csv"


def main() -> None:
    enrichment = {}
    if BSE_PROGRESS_CSV.exists():
        with open(BSE_PROGRESS_CSV, newline="") as f:
            enrichment.update({row["ticker"]: row for row in csv.DictReader(f)})
    with open(PROGRESS_CSV, newline="") as f:
        enrichment.update({row["ticker"]: row for row in csv.DictReader(f)})

    with open(UNIVERSE_CSV, newline="") as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames is not None, f"{UNIVERSE_CSV} has no header row"
        fieldnames = list(reader.fieldnames)
        rows = list(reader)

    updated = 0
    for row in rows:
        enr = enrichment.get(row["ticker"])
        if enr is None:
            continue
        if not row.get("company_name", "").strip():
            row["company_name"] = enr["company_name"]
            updated += 1
        if not row.get("sector", "").strip() and enr.get("sector"):
            row["sector"] = enr["sector"]

    with open(UNIVERSE_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    still_blank = [r for r in rows if not r.get("company_name", "").strip()]
    still_blank.sort(key=lambda r: (r.get("is_nifty500") != "True", r.get("is_fno_eligible") != "True", -_safe_float(r.get("market_cap_cr"))))
    cols = ["ticker", "sector", "tier", "market_cap_cr", "adtv_cr", "is_nifty500", "is_fno_eligible", "isin"]
    with open(MISSING_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        for r in still_blank:
            writer.writerow({c: r.get(c, "") for c in cols})

    print(f"Updated {updated} rows in {UNIVERSE_CSV}")
    print(f"{len(still_blank)} tickers still blank, written to {MISSING_CSV}")


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


if __name__ == "__main__":
    main()
