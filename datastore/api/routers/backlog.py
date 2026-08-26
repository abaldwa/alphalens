"""Backlog management API routes."""

from typing import Any, Optional
from pathlib import Path
from datetime import datetime
import yaml

from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api/v1/backlog", tags=["backlog"])

BACKLOG_FILE = Path(__file__).parent.parent.parent.parent / "backlog_items.yaml"


def load_backlog() -> dict[str, Any]:
    """Load backlog from YAML file (no DuckDB lock contention)."""
    try:
        with open(BACKLOG_FILE) as f:
            docs = list(yaml.safe_load_all(f))
        # Merge all documents into one
        merged = {}
        for doc in docs:
            if isinstance(doc, dict):
                merged.update(doc)
        return merged or {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load backlog: {str(e)}")


def save_backlog(data: dict[str, Any]) -> None:
    """Save backlog to YAML file."""
    try:
        with open(BACKLOG_FILE, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save backlog: {str(e)}")


def transform_backlog_item(item_id: str, item: dict[str, Any]) -> dict[str, Any]:
    """Transform YAML item to API format."""
    deps_data = load_backlog().get("backlog_dependencies", {})
    blocks_on_count = 0
    blocks_count = 0
    if item_id in deps_data:
        dep = deps_data[item_id]
        blocks_on_count = len(dep.get("blocked_by", []))
        blocks_count = len(dep.get("blocks", []))

    return {
        "item_id": item_id,
        "title": item.get("title", ""),
        "description": item.get("description", ""),
        "category": item.get("category", "feature"),
        "status": item.get("status", "pending"),
        "priority": item.get("priority", 3),
        "criticality": item.get("criticality", "medium"),
        "reason_critical": item.get("reason_critical"),
        "document_reference": item.get("document_reference"),
        "assigned_to": item.get("assigned_to"),
        "created_at": item.get("created_at", ""),
        "updated_at": item.get("updated_at", ""),
        "blocks_on_count": blocks_on_count,
        "blocks_count": blocks_count,
    }


@router.get("")
def list_backlog(status: Optional[str] = None, criticality: Optional[str] = None) -> list[dict[str, Any]]:
    """List all backlog items with optional filtering."""
    backlog_data = load_backlog()
    items = backlog_data.get("backlog_items", {})

    result = []
    for item_id, item in items.items():
        if status and item.get("status") != status:
            continue
        if criticality and item.get("criticality") != criticality:
            continue

        result.append(transform_backlog_item(item_id, item))

    result.sort(key=lambda x: (x["priority"], -{"critical": 4, "high": 3, "medium": 2, "low": 1}.get(x["criticality"], 0)))
    return result


@router.get("/{item_id}")
def get_backlog_item(item_id: str) -> dict[str, Any]:
    """Get a single backlog item with its dependencies."""
    backlog_data = load_backlog()
    items = backlog_data.get("backlog_items", {})
    deps = backlog_data.get("backlog_dependencies", {})

    if item_id not in items:
        raise HTTPException(status_code=404, detail="Item not found")

    item = transform_backlog_item(item_id, items[item_id])

    # Add resolved dependency titles
    if item_id in deps:
        dep_entry = deps[item_id]
        blocked_by_ids = dep_entry.get("blocked_by", [])
        item["blocks_on"] = [
            {"item_id": bid, "title": items.get(bid, {}).get("title", "Unknown")}
            for bid in blocked_by_ids
            if bid in items
        ]
        blocks_ids = dep_entry.get("blocks", [])
        item["blocks"] = [
            {"item_id": bid, "title": items.get(bid, {}).get("title", "Unknown")}
            for bid in blocks_ids
            if bid in items
        ]

    return item


@router.post("")
def create_backlog_item(
    item_id: str,
    title: str,
    category: str = "feature",
    priority: int = 3,
    criticality: str = "medium",
    reason_critical: Optional[str] = None,
    description: Optional[str] = None,
    document_reference: Optional[str] = None,
    assigned_to: Optional[str] = None,
) -> dict[str, Any]:
    """Create a new backlog item."""
    backlog_data = load_backlog()
    items = backlog_data.setdefault("backlog_items", {})

    if item_id in items:
        raise HTTPException(status_code=400, detail=f"Item {item_id} already exists")

    now = datetime.utcnow().isoformat() + "Z"
    items[item_id] = {
        "title": title,
        "description": description or "",
        "category": category,
        "status": "pending",
        "priority": priority,
        "criticality": criticality,
        "reason_critical": reason_critical,
        "document_reference": document_reference,
        "assigned_to": assigned_to,
        "created_at": now,
        "updated_at": now,
    }

    save_backlog(backlog_data)
    return {"item_id": item_id, "status": "created", "created_at": now}


@router.patch("/{item_id}")
def update_backlog_item(
    item_id: str,
    title: Optional[str] = None,
    description: Optional[str] = None,
    status: Optional[str] = None,
    priority: Optional[int] = None,
    criticality: Optional[str] = None,
    assigned_to: Optional[str] = None,
    document_reference: Optional[str] = None,
) -> dict[str, Any]:
    """Update a backlog item."""
    valid_statuses = ["blocked", "pending", "in-progress", "resolved"]
    if status and status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"Invalid status: {status}")

    backlog_data = load_backlog()
    items = backlog_data.get("backlog_items", {})

    if item_id not in items:
        raise HTTPException(status_code=404, detail="Item not found")

    item = items[item_id]
    if title is not None:
        item["title"] = title
    if description is not None:
        item["description"] = description
    if status is not None:
        item["status"] = status
    if priority is not None:
        item["priority"] = priority
    if criticality is not None:
        item["criticality"] = criticality
    if assigned_to is not None:
        item["assigned_to"] = assigned_to
    if document_reference is not None:
        item["document_reference"] = document_reference

    item["updated_at"] = datetime.utcnow().isoformat() + "Z"
    save_backlog(backlog_data)
    return {"item_id": item_id, "status": "updated"}


@router.delete("/{item_id}")
def delete_backlog_item(item_id: str) -> dict[str, Any]:
    """Delete a backlog item."""
    backlog_data = load_backlog()
    items = backlog_data.get("backlog_items", {})
    deps = backlog_data.get("backlog_dependencies", {})

    if item_id not in items:
        raise HTTPException(status_code=404, detail="Item not found")

    del items[item_id]
    if item_id in deps:
        del deps[item_id]

    # Remove item from other dependencies
    for dep_item in deps.values():
        if "blocked_by" in dep_item:
            dep_item["blocked_by"] = [bid for bid in dep_item["blocked_by"] if bid != item_id]
        if "blocks" in dep_item:
            dep_item["blocks"] = [bid for bid in dep_item["blocks"] if bid != item_id]

    save_backlog(backlog_data)
    return {"item_id": item_id, "status": "deleted"}


@router.post("/{item_id}/block-on/{blocks_on_id}")
def add_dependency(item_id: str, blocks_on_id: str) -> dict[str, Any]:
    """Add a dependency: item_id is blocked by blocks_on_id."""
    backlog_data = load_backlog()
    items = backlog_data.get("backlog_items", {})
    deps = backlog_data.setdefault("backlog_dependencies", {})

    if item_id not in items or blocks_on_id not in items:
        raise HTTPException(status_code=404, detail="One or both items not found")

    if item_id not in deps:
        deps[item_id] = {"blocked_by": [], "blocks": []}
    if blocks_on_id not in deps:
        deps[blocks_on_id] = {"blocked_by": [], "blocks": []}

    if blocks_on_id not in deps[item_id]["blocked_by"]:
        deps[item_id]["blocked_by"].append(blocks_on_id)
    if item_id not in deps[blocks_on_id]["blocks"]:
        deps[blocks_on_id]["blocks"].append(item_id)

    save_backlog(backlog_data)
    return {"item_id": item_id, "blocked_by": blocks_on_id, "status": "added"}


@router.delete("/{item_id}/block-on/{blocks_on_id}")
def remove_dependency(item_id: str, blocks_on_id: str) -> dict[str, Any]:
    """Remove a dependency."""
    backlog_data = load_backlog()
    deps = backlog_data.get("backlog_dependencies", {})

    if item_id not in deps or blocks_on_id not in deps.get(item_id, {}).get("blocked_by", []):
        raise HTTPException(status_code=404, detail="Dependency not found")

    deps[item_id]["blocked_by"].remove(blocks_on_id)
    if item_id in deps[blocks_on_id]["blocks"]:
        deps[blocks_on_id]["blocks"].remove(item_id)

    save_backlog(backlog_data)
    return {"item_id": item_id, "blocked_by": blocks_on_id, "status": "removed"}


@router.get("/stats/summary")
def get_backlog_stats() -> dict[str, Any]:
    """Get backlog summary statistics."""
    backlog_data = load_backlog()
    items = backlog_data.get("backlog_items", {})

    total = len(items)
    stats = {
        "total_items": total,
        "blocked_count": sum(1 for item in items.values() if item.get("status") == "blocked"),
        "pending_count": sum(1 for item in items.values() if item.get("status") == "pending"),
        "in_progress_count": sum(1 for item in items.values() if item.get("status") == "in-progress"),
        "resolved_count": sum(1 for item in items.values() if item.get("status") == "resolved"),
        "critical_count": sum(1 for item in items.values() if item.get("criticality") == "critical"),
        "high_count": sum(1 for item in items.values() if item.get("criticality") == "high"),
    }
    return stats


@router.post("/bulk-import")
def bulk_import_items(items: list[dict[str, Any]]) -> dict[str, Any]:
    """Bulk import backlog items from list of dicts."""
    backlog_data = load_backlog()
    backlog_items = backlog_data.setdefault("backlog_items", {})
    deps = backlog_data.setdefault("backlog_dependencies", {})

    now = datetime.utcnow().isoformat() + "Z"
    for item in items:
        item_id = item.get("item_id")
        if not item_id:
            continue

        backlog_items[item_id] = {
            "title": item.get("title", ""),
            "description": item.get("description", ""),
            "category": item.get("category", "feature"),
            "status": item.get("status", "pending"),
            "priority": item.get("priority", 3),
            "criticality": item.get("criticality", "medium"),
            "reason_critical": item.get("reason_critical"),
            "document_reference": item.get("document_reference"),
            "assigned_to": item.get("assigned_to"),
            "created_at": item.get("created_at", now),
            "updated_at": item.get("updated_at", now),
        }

        # Import dependencies if provided
        if "blocks_on" in item:
            if item_id not in deps:
                deps[item_id] = {"blocked_by": [], "blocks": []}
            for blocks_on_id in item["blocks_on"]:
                if blocks_on_id not in deps[item_id]["blocked_by"]:
                    deps[item_id]["blocked_by"].append(blocks_on_id)
                if blocks_on_id not in deps:
                    deps[blocks_on_id] = {"blocked_by": [], "blocks": []}
                if item_id not in deps[blocks_on_id]["blocks"]:
                    deps[blocks_on_id]["blocks"].append(item_id)

    save_backlog(backlog_data)
    return {"status": "imported", "count": len(items)}
