"""Minimal source-fusion helpers for expanding fundamental-data coverage."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


class CsvFundamentalSourceAdapter:
    """Read a simple CSV export as a fundamental-data source."""

    def __init__(self, csv_path: Path | str, preferred_source_order: Optional[List[str]] = None) -> None:
        self.csv_path = Path(csv_path)
        self.preferred_source_order = preferred_source_order or ["official", "alternate"]

    def fetch_ticker_history(self, ticker: str, as_of: Optional[str] = None, lookback_years: int = 1) -> List[Dict[str, Any]]:
        if not self.csv_path.exists():
            return []

        rows: List[Dict[str, Any]] = []
        with self.csv_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                if row.get("ticker") != ticker:
                    continue
                if as_of and row.get("as_of_date") and row["as_of_date"] > as_of:
                    continue
                rows.append(
                    {
                        "ticker": row.get("ticker"),
                        "metric": row.get("metric"),
                        "as_of_date": row.get("as_of_date"),
                        "value": float(row["value"]),
                        "source": row.get("source", "unknown"),
                        "confidence": float(row.get("confidence", 0.0)),
                    }
                )
        return rows


def merge_fundamental_rows(
    rows: Iterable[Dict[str, Any]],
    preferred_source_order: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Keep one row per metric, preferring higher-authority sources."""
    preferred_source_order = preferred_source_order or ["official", "alternate"]
    preferred_rank = {source: i for i, source in enumerate(preferred_source_order)}

    grouped: Dict[tuple[Any, Any, Any], Dict[str, Any]] = {}
    for row in rows:
        key = (row.get("ticker"), row.get("metric"), row.get("as_of_date"))
        existing = grouped.get(key)
        if existing is None:
            grouped[key] = dict(row)
            continue

        existing_rank = preferred_rank.get(existing.get("source", ""), len(preferred_source_order))
        new_rank = preferred_rank.get(row.get("source", ""), len(preferred_source_order))
        if new_rank < existing_rank:
            grouped[key] = dict(row)
            continue
        if new_rank == existing_rank and row.get("confidence", 0.0) > existing.get("confidence", 0.0):
            grouped[key] = dict(row)

    return list(grouped.values())
