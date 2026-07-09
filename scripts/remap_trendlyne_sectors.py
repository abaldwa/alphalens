"""
scripts/remap_trendlyne_sectors.py

Phase: Backlog #31 follow-up (2026-07-05)

Re-derives the `sector` column in
config/company_metadata_enrichment_trendlyne_progress.csv from its
already-captured `trendlyne_sector_raw` column, using the CURRENT
_SECTOR_MAP in scripts/enrich_missing_company_metadata_trendlyne.py.

No network calls — lets _SECTOR_MAP grow over the course of a long
enrichment run (new raw sector labels appear as more tickers are
processed) without re-fetching rows resolved before a mapping existed.
"""

import csv
from pathlib import Path

from scripts.enrich_missing_company_metadata_trendlyne import PROGRESS_FIELDS, _SECTOR_MAP

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROGRESS_CSV = PROJECT_ROOT / "config" / "company_metadata_enrichment_trendlyne_progress.csv"


def main() -> None:
    with open(PROGRESS_CSV, newline="") as f:
        rows = list(csv.DictReader(f))

    updated = 0
    for row in rows:
        raw = (row.get("trendlyne_sector_raw") or "").strip()
        mapped = _SECTOR_MAP.get(raw.lower(), "")
        if mapped and row.get("sector") != mapped:
            row["sector"] = mapped
            updated += 1

    with open(PROGRESS_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=PROGRESS_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    still_unmapped = sorted({row["trendlyne_sector_raw"] for row in rows if row.get("trendlyne_sector_raw") and not row.get("sector")})
    print(f"Re-mapped {updated} rows.")
    print(f"{len(still_unmapped)} distinct raw sector labels still unmapped: {still_unmapped}")


if __name__ == "__main__":
    main()
