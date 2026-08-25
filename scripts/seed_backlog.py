#!/usr/bin/env python3
"""
Seed backlog with initial items from backlog/items.json

Usage:
    python3 scripts/seed_backlog.py
"""

import json
import sys
from pathlib import Path

import duckdb

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import BACKTEST_DUCKDB_PATH


def seed_backlog():
    """Load and seed backlog items from JSON."""
    backlog_file = project_root / "backlog" / "items.json"

    if not backlog_file.exists():
        print(f"✗ Backlog file not found: {backlog_file}")
        sys.exit(1)

    try:
        with open(backlog_file, "r") as f:
            items = json.load(f)
    except json.JSONDecodeError as e:
        print(f"✗ Invalid JSON: {e}")
        sys.exit(1)

    db = duckdb.connect(str(BACKTEST_DUCKDB_PATH))

    # Clear existing items (optional)
    # db.execute("DELETE FROM backlog_items")
    # db.execute("DELETE FROM backlog_dependencies")

    added_count = 0
    skipped_count = 0
    error_count = 0

    # Insert items
    for item in items:
        try:
            # Check if item already exists
            existing = db.execute(
                "SELECT item_id FROM backlog_items WHERE item_id = ?",
                [item["item_id"]],
            ).fetchall()

            if existing:
                print(f"⊙ {item['item_id']}: already exists")
                skipped_count += 1
                continue

            db.execute(
                """
                INSERT INTO backlog_items
                (item_id, title, description, category, status, priority,
                 criticality, reason_critical, assigned_to, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp, current_timestamp)
                """,
                [
                    item["item_id"],
                    item["title"],
                    item.get("description"),
                    item.get("category", "feature"),
                    item.get("status", "pending"),
                    item.get("priority", 3),
                    item.get("criticality", "medium"),
                    item.get("reason_critical"),
                    item.get("assigned_to"),
                ],
            )
            print(f"✓ {item['item_id']}: {item['title'][:50]}")
            added_count += 1
        except Exception as e:
            print(f"✗ {item['item_id']}: {e}")
            error_count += 1

    # Insert dependencies
    deps_added = 0
    for item in items:
        if "blocks_on" in item:
            for blocks_on_id in item["blocks_on"]:
                try:
                    # Check if dependency already exists
                    existing = db.execute(
                        "SELECT 1 FROM backlog_dependencies WHERE dependent_id = ? AND blocks_on_id = ?",
                        [item["item_id"], blocks_on_id],
                    ).fetchall()

                    if existing:
                        continue

                    db.execute(
                        """
                        INSERT INTO backlog_dependencies
                        (dependent_id, blocks_on_id, created_at)
                        VALUES (?, ?, current_timestamp)
                        """,
                        [item["item_id"], blocks_on_id],
                    )
                    deps_added += 1
                except Exception as e:
                    print(f"  ⚠ Dependency {item['item_id']} → {blocks_on_id}: {e}")

    print()
    print("Summary:")
    print(f"  Added: {added_count}")
    print(f"  Skipped: {skipped_count}")
    print(f"  Dependencies: {deps_added}")
    print(f"  Errors: {error_count}")

    if error_count == 0:
        print("\n✓ Backlog seeding complete!")
    else:
        print(f"\n✗ {error_count} errors occurred")
        sys.exit(1)


if __name__ == "__main__":
    seed_backlog()
