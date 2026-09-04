"""
M-Band Universe Definitions

Maps market-cap rank bands to Nifty benchmarks for systematic momentum analysis.
Source: features/momentum_universe.py (adapted)
"""

from dataclasses import dataclass
from typing import Dict, List


@dataclass
class MBand:
    """Represents a market-cap rank band (M-band)."""
    band_id: int
    name: str
    rank_start: int
    rank_end: int
    nifty_benchmark: str
    description: str

    @property
    def count(self) -> int:
        """Number of stocks in this band."""
        return self.rank_end - self.rank_start + 1


# M-Band Definitions (Nifty-benchmark mapping)
MBANDS: Dict[int, MBand] = {
    2: MBand(
        band_id=2,
        name="M02",
        rank_start=1,
        rank_end=75,
        nifty_benchmark="Nifty 50",
        description="Large-cap flagship stocks"
    ),
    4: MBand(
        band_id=4,
        name="M04",
        rank_start=76,
        rank_end=160,
        nifty_benchmark="Nifty Midcap 150",
        description="Established mid-cap companies"
    ),
    7: MBand(
        band_id=7,
        name="M07",
        rank_start=161,
        rank_end=275,
        nifty_benchmark="Nifty Midcap 250",
        description="Broader mid-cap segment"
    ),
    9: MBand(
        band_id=9,
        name="M09",
        rank_start=276,
        rank_end=550,
        nifty_benchmark="Nifty Smallcap 250",
        description="Smaller companies with growth potential"
    ),
    10: MBand(
        band_id=10,
        name="M10",
        rank_start=301,
        rank_end=500,
        nifty_benchmark="Nifty Smallcap 250 (subset)",
        description="Narrower smallcap segment (historical R09 leverage runs)"
    ),
    12: MBand(
        band_id=12,
        name="M12",
        rank_start=551,
        rank_end=800,
        nifty_benchmark="Nifty Microcap",
        description="Smallest liquid companies"
    ),
    13: MBand(
        band_id=13,
        name="M13",
        rank_start=1,
        rank_end=800,
        nifty_benchmark="Nifty 800 (full ADTV universe)",
        description=(
            "Top 800 ADTV stocks — the FULL universe, not a partition. "
            "Deliberately overlaps every other band (see "
            "TOP_N_BY_BAND: M13 tests wider, deeper baskets "
            "(top 10/20/30/40) than the partitioned bands "
            "(top 5/10/15) since it draws from the whole universe."
        ),
    ),
}

# Per-band top_n test set. M13 (the full 800-stock universe) is tested with
# wider baskets than the partitioned bands (M2/M4/M7/M9/M10/M12), which each
# only have 75-550 stocks to choose from — a top_n=40 cut on a 75-stock band
# (M2) would be half the band, not a concentrated selection.
TOP_N_BY_BAND: Dict[int, List[int]] = {
    2: [5, 10, 15],
    4: [5, 10, 15],
    7: [5, 10, 15],
    9: [5, 10, 15],
    10: [5, 10, 15],
    12: [5, 10, 15],
    13: [10, 20, 30, 40],
}


class UniverseDefinition:
    """Manages M-band universe definitions and lookups."""

    def __init__(self) -> None:
        self.bands = MBANDS

    def get_band(self, band_id: int) -> MBand:
        """Get band definition by ID."""
        if band_id not in self.bands:
            raise ValueError(f"Unknown band_id: {band_id}. Valid: {list(self.bands.keys())}")
        return self.bands[band_id]

    def get_bands_by_rank(self, rank: int) -> List[MBand]:
        """
        All bands containing this market-cap rank. Bands OVERLAP by design
        (M10 sits inside M9; M13 spans all of them) — never assume a rank
        maps to exactly one band. See agent doc "Band overlap
        misunderstanding" known-risk entry.
        """
        matches = [b for b in self.bands.values() if b.rank_start <= rank <= b.rank_end]
        if not matches:
            raise ValueError(f"Rank {rank} not in any band (max: 800)")
        return matches

    def get_band_by_rank(self, rank: int) -> MBand:
        """
        Convenience wrapper returning the NARROWEST band containing this
        rank (smallest stock count) — a reasonable single-answer default
        for a rank that belongs to more than one overlapping band. Prefer
        get_bands_by_rank() when you need every matching band, not just one.
        """
        return min(self.get_bands_by_rank(rank), key=lambda b: b.count)

    def list_all_bands(self) -> List[MBand]:
        """Get all band definitions sorted by rank_start."""
        return sorted(self.bands.values(), key=lambda b: b.rank_start)

    def describe(self) -> str:
        """Pretty-print all band definitions."""
        lines = ["M-Band Universe Definitions:", ""]
        for band in self.list_all_bands():
            lines.append(
                f"  {band.name:4s} (band_id={band.band_id:2d}): "
                f"Rank {band.rank_start:3d}–{band.rank_end:3d} "
                f"({band.count:3d} stocks) ~ {band.nifty_benchmark}"
            )
        return "\n".join(lines)


# Singleton instance
_universe = UniverseDefinition()

def get_universe() -> UniverseDefinition:
    """Get the global universe definition instance."""
    return _universe
