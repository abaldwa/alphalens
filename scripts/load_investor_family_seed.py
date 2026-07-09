"""
scripts/load_investor_family_seed.py

Phase B (Big Investor Activity — plan: gentle-wobbling-swing.md)

Loads datastore/seed/investor_family_seed.yaml into the investor_family
DuckDB table. The seed file is hand-curated and reviewed with the user
before being loaded — this script is never invoked automatically.

Usage:
    python -m scripts.load_investor_family_seed --dry-run
    python -m scripts.load_investor_family_seed --apply
"""

import argparse
import logging
from datetime import date

import yaml

from config.settings import BIG_INVESTOR_FAMILY_SEED_PATH, DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from ingestion.scrapers.bulk_deal_attribution import normalize_client_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def load_seed_entities(seed_path: str) -> list[dict]:
    """Parse the seed YAML into a flat list of (entity_name, family_id, family_display_name, notes)."""
    with open(seed_path) as f:
        doc = yaml.safe_load(f)

    entities = []
    for fam in doc.get("families", []):
        for raw_name in fam["entities"]:
            entities.append(
                {
                    "entity_name": normalize_client_name(raw_name),
                    "family_id": fam["family_id"],
                    "family_display_name": fam["family_display_name"],
                    "notes": fam.get("notes"),
                }
            )
    return entities


def diff_against_db(conn, entities: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    """Return (to_add, to_update, unchanged) vs the current investor_family table."""
    existing = {
        row[0]: {"family_id": row[1], "family_display_name": row[2]}
        for row in conn.execute("SELECT entity_name, family_id, family_display_name FROM investor_family").fetchall()
    }
    to_add, to_update, unchanged = [], [], []
    for e in entities:
        current = existing.get(e["entity_name"])
        if current is None:
            to_add.append(e)
        elif current["family_id"] != e["family_id"] or current["family_display_name"] != e["family_display_name"]:
            to_update.append(e)
        else:
            unchanged.append(e)
    return to_add, to_update, unchanged


def apply_seed(conn, entities: list[dict]) -> None:
    today = date.today().isoformat()
    for e in entities:
        conn.execute(
            """
            INSERT INTO investor_family (entity_name, family_id, family_display_name, match_type, source, confidence, added_date, notes)
            VALUES (?, ?, ?, 'exact', 'seed_yaml', 1.0, ?, ?)
            ON CONFLICT (entity_name) DO UPDATE SET
                family_id = excluded.family_id,
                family_display_name = excluded.family_display_name,
                notes = excluded.notes
            """,
            [e["entity_name"], e["family_id"], e["family_display_name"], today, e["notes"]],
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true", help="Print add/update/unchanged summary, no DB writes")
    group.add_argument("--apply", action="store_true", help="Load the seed into investor_family")
    args = parser.parse_args()

    entities = load_seed_entities(BIG_INVESTOR_FAMILY_SEED_PATH)
    logger.info(f"Parsed {len(entities)} entities from {BIG_INVESTOR_FAMILY_SEED_PATH}")

    with get_duckdb_connection(DUCKDB_PATH, read_only=args.dry_run, persist=False) as conn:
        to_add, to_update, unchanged = diff_against_db(conn, entities)
        logger.info(f"Add: {len(to_add)}  Update: {len(to_update)}  Unchanged: {len(unchanged)}")
        for e in to_add:
            logger.info(f"  + {e['entity_name']} -> {e['family_id']} ({e['family_display_name']})")
        for e in to_update:
            logger.info(f"  ~ {e['entity_name']} -> {e['family_id']} ({e['family_display_name']})")

        if args.apply:
            apply_seed(conn, entities)
            logger.info(f"Applied {len(to_add) + len(to_update)} rows to investor_family")


if __name__ == "__main__":
    main()
