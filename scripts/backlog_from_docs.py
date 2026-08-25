#!/usr/bin/env python3
"""
Extract pending items from markdown files across the project.

Scans all .md files in docs/, specs/, and .claude/ for:
- Status markers (⏳ PENDING, 🔴 BLOCKED, 🔧 IN-PROGRESS, ✅ RESOLVED)
- TODO markers ([TODO(...)])
- NEEDS CLARIFICATION markers

Creates backlog items from findings.

Usage:
    python3 scripts/backlog_from_docs.py --scan          # Report findings
    python3 scripts/backlog_from_docs.py --create        # Create backlog items
    python3 scripts/backlog_from_docs.py --dry-run       # Preview without creating
"""

import argparse
import re
import sys
from pathlib import Path

import duckdb

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import BACKTEST_DUCKDB_PATH


# Status marker mapping
STATUS_MARKERS = {
    "⏳": "pending",
    "🔴": "blocked",
    "🔧": "in-progress",
    "✅": "resolved",
    "PENDING": "pending",
    "BLOCKED": "blocked",
    "IN-PROGRESS": "in-progress",
    "RESOLVED": "resolved",
}

# Criticality inference
CRITICALITY_KEYWORDS = {
    "critical": "critical",
    "urgent": "critical",
    "blocking": "critical",
    "blocker": "critical",
    "important": "high",
    "must": "high",
    "should": "high",
    "optional": "medium",
    "nice-to-have": "low",
    "enhancement": "low",
}


def extract_status(line: str) -> tuple[str, str]:
    """Extract status from line. Returns (status, cleaned_text)."""
    cleaned = line.strip()

    # Check emoji markers
    for marker, status in STATUS_MARKERS.items():
        if marker in cleaned:
            cleaned = cleaned.replace(marker, "").strip()
            return status, cleaned

    # Check word markers
    for keyword, status in STATUS_MARKERS.items():
        if keyword in cleaned.upper():
            pattern = rf"\b{keyword}\b"
            cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE).strip()
            return status, cleaned

    return "pending", cleaned


def extract_todo_items(text: str, filename: str) -> list[dict]:
    """Extract [TODO(...)] markers from text."""
    items = []
    # Match [TODO(...)] with possibly nested parens
    pattern = r"\[TODO\(([^)]*(?:\([^)]*\)[^)]*)*)\)\]"
    matches = re.finditer(pattern, text)

    for match in matches:
        todo_text = match.group(1).strip()
        items.append({
            "type": "todo",
            "text": todo_text,
            "source": filename,
        })

    return items


def extract_clarification_items(text: str, filename: str) -> list[dict]:
    """Extract [NEEDS CLARIFICATION: ...] markers from text."""
    items = []
    pattern = r"\[NEEDS CLARIFICATION:\s*([^\]]+)\]"
    matches = re.finditer(pattern, text)

    for match in matches:
        clarif_text = match.group(1).strip()
        items.append({
            "type": "clarification",
            "text": clarif_text,
            "source": filename,
        })

    return items


def extract_status_items(text: str, filename: str) -> list[dict]:
    """Extract lines with status markers."""
    items = []

    for line in text.split("\n"):
        # Skip headers and code blocks
        if line.strip().startswith("#") or line.strip().startswith("```"):
            continue

        # Check for status markers
        has_status = any(marker in line for marker in STATUS_MARKERS.keys())
        if not has_status:
            continue

        status, cleaned_text = extract_status(line)

        # Skip if too short
        if len(cleaned_text) < 10:
            continue

        items.append({
            "type": "status",
            "status": status,
            "text": cleaned_text[:200],
            "source": filename,
        })

    return items


def infer_criticality(text: str) -> str:
    """Infer criticality from text keywords."""
    text_lower = text.lower()

    for keyword, crit in CRITICALITY_KEYWORDS.items():
        if keyword in text_lower:
            return crit

    return "medium"


def scan_directory(root_dir: Path, patterns: list[str]) -> dict[str, list[dict]]:
    """Scan directory for .md files and extract items."""
    findings = {}

    for pattern in patterns:
        for md_file in root_dir.glob(pattern):
            if not md_file.is_file():
                continue

            try:
                content = md_file.read_text(encoding="utf-8")
            except Exception as e:
                print(f"⚠ Skipping {md_file.name}: {e}")
                continue

            items = []
            items.extend(extract_todo_items(content, md_file.name))
            items.extend(extract_clarification_items(content, md_file.name))
            items.extend(extract_status_items(content, md_file.name))

            if items:
                findings[str(md_file.relative_to(root_dir))] = items

    return findings


def report_findings(findings: dict):
    """Print scan report."""
    total_items = sum(len(items) for items in findings.values())

    print(f"\n📋 Scan Results: {total_items} items found across {len(findings)} files\n")

    for filepath, items in sorted(findings.items()):
        print(f"📄 {filepath} ({len(items)} items)")

        for item in items:
            if item["type"] == "status":
                status_emoji = {
                    "pending": "⏳",
                    "blocked": "🔴",
                    "in-progress": "🔧",
                    "resolved": "✅",
                }.get(item["status"], "❓")
                print(f"  {status_emoji} {item['status']}: {item['text'][:80]}")
            elif item["type"] == "todo":
                print(f"  📌 TODO: {item['text'][:80]}")
            elif item["type"] == "clarification":
                print(f"  ❓ CLARIFY: {item['text'][:80]}")

        print()


def create_backlog_items(findings: dict, dry_run: bool = False):
    """Create backlog items from findings."""
    if not findings:
        print("No items to create.")
        return

    db = duckdb.connect(str(BACKTEST_DUCKDB_PATH))
    created_count = 0
    skipped_count = 0

    for filepath, items in findings.items():
        for idx, item in enumerate(items, 1):
            # Generate item ID
            file_prefix = Path(filepath).stem[:4].upper()
            item_id = f"SCAN-{file_prefix}-{idx:03d}"

            # Build item details
            title = item["text"][:100]
            category = {
                "todo": "task",
                "status": "tracking",
                "clarification": "research",
            }.get(item["type"], "feature")

            status = item.get("status", "pending")
            criticality = infer_criticality(item["text"])
            reason_critical = f"Found in {filepath}" if criticality != "low" else None

            if dry_run:
                print(f"Would create: {item_id}")
                print(f"  Title: {title}")
                print(f"  Status: {status} | Criticality: {criticality}")
                print(f"  Category: {category} | Source: {filepath}\n")
                continue

            # Check if already exists
            try:
                existing = db.execute(
                    "SELECT item_id FROM backlog_items WHERE item_id = ?",
                    [item_id],
                ).fetchall()

                if existing:
                    print(f"⊙ {item_id}: already exists")
                    skipped_count += 1
                    continue

                db.execute(
                    """
                    INSERT INTO backlog_items
                    (item_id, title, description, category, status, priority,
                     criticality, reason_critical, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp, current_timestamp)
                    """,
                    [
                        item_id,
                        title,
                        f"Extracted from: {filepath}\n\n{item['text']}",
                        category,
                        status,
                        3,  # default priority
                        criticality,
                        reason_critical,
                    ],
                )
                print(f"✓ {item_id}: {title[:60]}")
                created_count += 1
            except Exception as e:
                print(f"✗ {item_id}: {e}")

    print()
    print("Summary:")
    print(f"  Created: {created_count}")
    print(f"  Skipped: {skipped_count}")

    if created_count > 0:
        print(f"\n✓ Created {created_count} new backlog items from docs")


def main():
    parser = argparse.ArgumentParser(
        description="Extract pending items from markdown files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 scripts/backlog_from_docs.py --scan           # Report all findings
  python3 scripts/backlog_from_docs.py --create         # Create backlog items
  python3 scripts/backlog_from_docs.py --dry-run        # Preview without creating
        """,
    )

    parser.add_argument(
        "--scan",
        action="store_true",
        help="Scan and report findings (default if no action specified)",
    )
    parser.add_argument(
        "--create",
        action="store_true",
        help="Create backlog items from findings",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview items without creating them",
    )

    args = parser.parse_args()

    # Default to scan if no action specified
    if not any([args.scan, args.create, args.dry_run]):
        args.scan = True

    # Scan patterns
    patterns = [
        "FeatureBacklog*.md",
        "docs/**/*.md",
        "specs/**/*.md",
        ".claude/**/*.md",
    ]

    print("🔍 Scanning for pending items...\n")
    findings = scan_directory(project_root, patterns)

    if args.scan:
        report_findings(findings)

    if args.create:
        create_backlog_items(findings, dry_run=False)

    if args.dry_run:
        print("🔮 DRY-RUN: Would create the following items:\n")
        create_backlog_items(findings, dry_run=True)


if __name__ == "__main__":
    main()
