"""
config/sector_index_map.py

Phase: FutureDevelopment #25 (ML12 steps 4-6 — daily sector rotation report)
Owner: Platform / Features
Consumers: features/sector_rotation.py, datastore/api/routers/sector_rotation.py

Maps this project's `sector` taxonomy (config/nifty500_universe.csv's
`sector` column, ~21 distinct raw text values including a couple of
punctuation-only duplicates from NSE's own source data — e.g. "Oil Gas &
Consumable Fuels" vs "Oil, Gas & Consumable Fuels") to the tracked NSE
sector indices ingestion/scrapers/nse_indices.py already downloads daily
into index_ohlcv (TRACKED_INDICES there).

Only sectors with a genuinely matching NSE sector index are mapped. NSE
does not publish an index for every GICS-like sector bucket this
project's taxonomy uses (e.g. no "Nifty Capital Goods", no "Nifty
Construction", no "Nifty Textiles") — those sectors are deliberately left
OUT of SECTOR_INDEX_MAP rather than pointed at a loosely-related
substitute index (CLAUDE.md Absolute Rule 6 in spirit: no fabricated
stand-ins). features/sector_rotation.py excludes any sector not present
in this map from the ranking entirely, rather than guessing.

Two taxonomy values are explicitly considered but NOT mapped even though
a same-named-ish index exists:
  - "Power" — NSE's closest published index, "Nifty Energy", is a mixed
    oil-and-gas + power-utility basket (e.g. includes Reliance, ONGC, IOC
    alongside NTPC/Power Grid), not a pure power-sector index. Mapping
    "Power" to it would misrepresent the sector's real relative strength.
  - "Telecommunication" — no NSE sector index for telecom is in
    TRACKED_INDICES (or published by NSE's indices-close archive at all
    as a narrow telecom-only index); left unmapped.
"""

from typing import Dict, List

# sector (config/nifty500_universe.csv's raw `sector` value) -> tracked
# NSE index name (must match ingestion/scrapers/nse_indices.py's
# TRACKED_INDICES exactly).
SECTOR_INDEX_MAP: Dict[str, str] = {
    "Financial Services": "Nifty Financial Services",
    "Information Technology": "Nifty IT",
    "Fast Moving Consumer Goods": "Nifty FMCG",
    "Healthcare": "Nifty Healthcare Index",
    "Automobile and Auto Components": "Nifty Auto",
    "Metals & Mining": "Nifty Metal",
    "Realty": "Nifty Realty",
    # NSE's own source data has both spellings of this sector across
    # different universe-CSV vintages; both map to the same real index.
    "Oil Gas & Consumable Fuels": "Nifty Oil & Gas",
    "Oil, Gas & Consumable Fuels": "Nifty Oil & Gas",
    # Same punctuation-variant situation as Oil & Gas above.
    "Media, Entertainment & Publication": "Nifty Media",
    "Media Entertainment & Publication": "Nifty Media",
}

# Every other raw sector value seen in config/nifty500_universe.csv as of
# 2026-07-11, kept here so a test can assert every taxonomy value is
# accounted for (either mapped above or explicitly excluded here) rather
# than silently falling through. Not exhaustive of all possible future
# values — config/build_universe.py sources this text straight from NSE's
# Nifty 500 constituent list and it can add new sector strings over time.
EXPLICITLY_EXCLUDED_SECTORS: List[str] = [
    "Capital Goods",
    "Chemicals",
    "Services",
    "Consumer Services",
    "Consumer Durables",
    "Construction",
    "Textiles",
    "Construction Materials",
    "Telecommunication",
    "Utilities",
    "Forest Materials",
    "Diversified",
    "Power",
]


def get_index_for_sector(sector: str) -> str:
    """
    Real tracked NSE index name for a sector, or None if this sector has
    no matching index (see EXPLICITLY_EXCLUDED_SECTORS / module docstring).
    """
    return SECTOR_INDEX_MAP.get(sector)


def sectors_for_index(index_name: str) -> List[str]:
    """All raw sector taxonomy values that map to a given tracked index name."""
    return [sector for sector, idx in SECTOR_INDEX_MAP.items() if idx == index_name]
