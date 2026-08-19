"""
strategies/migrations/retire_legacy_band_ids.py

Owner: Platform / Architecture
Run: PYTHONPATH=$PWD .venv/bin/python -m strategies.migrations.retire_legacy_band_ids [--dry-run]

One-shot companion to the 2026-08-19 RANK_BANDS renumbering
(features/momentum_universe.py): the band ids became contiguous 1-7, where
they previously ran 1,2,3,4,6,7,8 around the hole left by the retired
100-200 band.

`strategies.migrations.momentum` is additive and idempotent -- re-running it
after the renumber REGISTERS the new `b5_201-300` / `b6_301-500` /
`b7_501-800` keys but does not touch the old `b6_201-300` / `b7_301-500` /
`b8_501-800` rows, which stay `active`. That leaves two active registry keys
describing the SAME rank range under two different ids, and any consumer
resolving "the active strategies" would double-count all three bands.

This script closes that: it retires exactly the old-id rows whose
(band_id, rank_start, rank_end) is one of the three superseded triples. It
does not delete anything -- retire_strategy() marks the current version
retired and history is preserved, so a historical run that recorded the old
key still resolves.

Deliberately NOT retired: band_id 5 (ranks 100-200). That one was retired
long ago by a different change and is already status='retired'; it has no
current equivalent, since it overlapped bands 3 and 4 rather than
partitioning with them.
"""

from __future__ import annotations

import argparse
import json
import logging
from typing import List, Set, Tuple

import duckdb

from config.settings import BACKTEST_DUCKDB_PATH
from strategies.registry import retire_strategy

logger = logging.getLogger(__name__)

#: (old band_id, rank_start, rank_end) triples superseded by the renumber.
#: The RANGES are unchanged -- only the id moved -- so each of these is the
#: same universe as the new id in LEGACY_BAND_ID_TO_CURRENT.
SUPERSEDED_BANDS: Set[Tuple[int, int, int]] = {
    (6, 201, 300),
    (7, 301, 500),
    (8, 501, 800),
}


def find_superseded_keys() -> List[str]:
    """Active momentum registry keys still filed under a superseded band id."""
    conn = duckdb.connect(str(BACKTEST_DUCKDB_PATH), read_only=True)
    try:
        rows = conn.execute(
            "SELECT strategy_key, definition_json FROM strategy_registry "
            "WHERE channel = 'momentum' AND status = 'active'"
        ).fetchall()
    finally:
        conn.close()

    keys = set()
    for key, definition_json in rows:
        definition = json.loads(definition_json)
        triple = (
            definition.get("band_id"),
            definition.get("rank_start"),
            definition.get("rank_end"),
        )
        if triple in SUPERSEDED_BANDS:
            keys.add(key)
    return sorted(keys)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    keys = find_superseded_keys()
    logger.info("found %d active rows under a superseded band id", len(keys))
    if args.dry_run:
        for key in keys[:5]:
            logger.info("[dry-run] would retire %s", key)
        logger.info("[dry-run] retired=%d", len(keys))
        return

    for key in keys:
        retire_strategy(key)
    logger.info("retired=%d", len(keys))


if __name__ == "__main__":
    main()
