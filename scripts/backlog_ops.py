#!/usr/bin/env python3
"""
Backlog Operations Helper

Manage backlog items directly:
- Add new items
- Update status
- Mark as in-progress or resolved
- Add dependencies
- Bulk import from markdown files

Usage:
    python3 scripts/backlog_ops.py add --id ITEM-001 --title "Fix X" --category defect --priority 1 --criticality critical --reason "Risk description"
    python3 scripts/backlog_ops.py mark-status ITEM-001 pending
    python3 scripts/backlog_ops.py mark-in-progress ITEM-001
    python3 scripts/backlog_ops.py mark-resolved ITEM-001
    python3 scripts/backlog_ops.py add-dependency ITEM-001 ITEM-002 "ITEM-001 is blocked by ITEM-002"
    python3 scripts/backlog_ops.py bulk-from-docs --scan          # Report findings
    python3 scripts/backlog_ops.py bulk-from-docs --create        # Create items
    python3 scripts/backlog_ops.py bulk-from-docs --dry-run       # Preview
"""

import argparse
import sys
from pathlib import Path

import duckdb

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import BACKTEST_DUCKDB_PATH


def get_db():
    """Get DuckDB connection."""
    return duckdb.connect(str(BACKTEST_DUCKDB_PATH))


def add_item(
    item_id: str,
    title: str,
    category: str,
    priority: int = 3,
    criticality: str = "medium",
    reason_critical: str = None,
    description: str = None,
):
    """Add a new backlog item."""
    db = get_db()
    try:
        db.execute(
            """
            INSERT INTO backlog_items
            (item_id, title, description, category, status, priority,
             criticality, reason_critical, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, current_timestamp, current_timestamp)
            """,
            [item_id, title, description, category, priority, criticality, reason_critical],
        )
        print(f"✓ {item_id}: {title}")
    except Exception as e:
        print(f"✗ Error adding item: {e}")
        sys.exit(1)


def mark_status(item_id: str, status: str):
    """Update item status."""
    valid_statuses = ["blocked", "pending", "in-progress", "resolved"]
    if status not in valid_statuses:
        print(f"✗ Invalid status: {status}. Must be one of: {', '.join(valid_statuses)}")
        sys.exit(1)

    db = get_db()
    try:
        result = db.execute(
            "UPDATE backlog_items SET status = ?, updated_at = current_timestamp WHERE item_id = ?",
            [status, item_id],
        )
        if result.rows_affected == 0:
            print(f"✗ Item not found: {item_id}")
            sys.exit(1)
        print(f"✓ {item_id} → {status.upper()}")
    except Exception as e:
        print(f"✗ Error updating status: {e}")
        sys.exit(1)


def mark_in_progress(item_id: str):
    """Mark item as in-progress."""
    db = get_db()
    try:
        db.execute(
            "UPDATE backlog_items SET status = 'in-progress', assigned_to = 'scrum-master', updated_at = current_timestamp WHERE item_id = ?",
            [item_id],
        )
        print(f"✓ {item_id} → IN-PROGRESS (assigned to scrum-master)")
    except Exception as e:
        print(f"✗ Error: {e}")
        sys.exit(1)


def mark_resolved(item_id: str, notes: str = ""):
    """Mark item as resolved."""
    db = get_db()
    try:
        result = db.execute(
            """
            UPDATE backlog_items
            SET status = 'resolved',
                assigned_to = NULL,
                updated_at = current_timestamp
            WHERE item_id = ?
            """,
            [item_id],
        )
        if result.rows_affected == 0:
            print(f"✗ Item not found: {item_id}")
            sys.exit(1)
        print(f"✓ {item_id} → RESOLVED")
        if notes:
            print(f"  Notes: {notes}")
    except Exception as e:
        print(f"✗ Error: {e}")
        sys.exit(1)


def add_dependency(dependent_id: str, blocks_on_id: str, reason: str = None):
    """Add a dependency relationship."""
    db = get_db()
    try:
        db.execute(
            """
            INSERT INTO backlog_dependencies
            (dependent_id, blocks_on_id, reason, created_at)
            VALUES (?, ?, ?, current_timestamp)
            """,
            [dependent_id, blocks_on_id, reason],
        )
        print(f"✓ {dependent_id} is blocked by {blocks_on_id}")
        if reason:
            print(f"  Reason: {reason}")
    except Exception as e:
        print(f"✗ Error adding dependency: {e}")
        sys.exit(1)


def list_items(status: str = None, criticality: str = None):
    """List backlog items."""
    db = get_db()
    query = "SELECT item_id, title, status, priority, criticality, assigned_to FROM backlog_items WHERE 1=1"
    params = []

    if status:
        query += " AND status = ?"
        params.append(status)

    if criticality:
        query += " AND criticality = ?"
        params.append(criticality)

    query += " ORDER BY priority ASC, criticality DESC"

    try:
        results = db.execute(query, params).fetchall()
        if not results:
            print("No items found")
            return

        print(f"\n{'ID':<15} {'Title':<40} {'Status':<15} {'Pri':<3} {'Criticality':<10} {'Assigned To':<15}")
        print("-" * 98)
        for row in results:
            item_id, title, status_val, priority, crit, assigned = row
            print(
                f"{item_id:<15} {title[:39]:<40} {status_val:<15} {priority:<3} {crit:<10} {assigned or '—':<15}"
            )
    except Exception as e:
        print(f"✗ Error listing items: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Backlog Operations")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Add command
    add_parser = subparsers.add_parser("add", help="Add a new backlog item")
    add_parser.add_argument("--id", required=True, help="Item ID (e.g., ARCH-001)")
    add_parser.add_argument("--title", required=True, help="Item title")
    add_parser.add_argument("--category", required=True, help="Category (blocker, defect, dependency, feature, research)")
    add_parser.add_argument("--priority", type=int, default=3, help="Priority (1-5, default 3)")
    add_parser.add_argument("--criticality", default="medium", help="Criticality (critical, high, medium, low)")
    add_parser.add_argument("--reason", help="Reason criticality (if critical or high)")
    add_parser.add_argument("--description", help="Description")

    # Status commands
    mark_parser = subparsers.add_parser("mark-status", help="Update item status")
    mark_parser.add_argument("item_id", help="Item ID")
    mark_parser.add_argument("status", help="Status (blocked, pending, in-progress, resolved)")

    in_prog_parser = subparsers.add_parser("mark-in-progress", help="Mark item as in-progress")
    in_prog_parser.add_argument("item_id", help="Item ID")

    resolved_parser = subparsers.add_parser("mark-resolved", help="Mark item as resolved")
    resolved_parser.add_argument("item_id", help="Item ID")
    resolved_parser.add_argument("--notes", help="Resolution notes")

    # Dependency command
    dep_parser = subparsers.add_parser("add-dependency", help="Add dependency between items")
    dep_parser.add_argument("dependent_id", help="Item that is blocked")
    dep_parser.add_argument("blocks_on_id", help="Item that blocks")
    dep_parser.add_argument("--reason", help="Reason for dependency")

    # List command
    list_parser = subparsers.add_parser("list", help="List backlog items")
    list_parser.add_argument("--status", help="Filter by status")
    list_parser.add_argument("--criticality", help="Filter by criticality")

    # Bulk import from docs command
    bulk_parser = subparsers.add_parser("bulk-from-docs", help="Import items from markdown files")
    bulk_parser.add_argument("--scan", action="store_true", help="Scan and report findings")
    bulk_parser.add_argument("--create", action="store_true", help="Create backlog items")
    bulk_parser.add_argument("--dry-run", action="store_true", help="Preview without creating")

    args = parser.parse_args()

    if args.command == "add":
        add_item(
            item_id=args.id,
            title=args.title,
            category=args.category,
            priority=args.priority,
            criticality=args.criticality,
            reason_critical=args.reason,
            description=args.description,
        )
    elif args.command == "mark-status":
        mark_status(args.item_id, args.status)
    elif args.command == "mark-in-progress":
        mark_in_progress(args.item_id)
    elif args.command == "mark-resolved":
        mark_resolved(args.item_id, args.notes or "")
    elif args.command == "add-dependency":
        add_dependency(args.dependent_id, args.blocks_on_id, args.reason)
    elif args.command == "list":
        list_items(args.status, args.criticality)
    elif args.command == "bulk-from-docs":
        # Import the backlog_from_docs module dynamically
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "backlog_from_docs",
            project_root / "scripts" / "backlog_from_docs.py"
        )
        backlog_from_docs = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(backlog_from_docs)

        findings = backlog_from_docs.scan_directory(
            project_root,
            [
                "FeatureBacklog*.md",
                "docs/**/*.md",
                "specs/**/*.md",
                ".claude/**/*.md",
            ],
        )

        if args.scan or not args.create:
            backlog_from_docs.report_findings(findings)

        if args.create:
            backlog_from_docs.create_backlog_items(findings, dry_run=False)

        if args.dry_run:
            print("🔮 DRY-RUN: Would create the following items:\n")
            backlog_from_docs.create_backlog_items(findings, dry_run=True)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
