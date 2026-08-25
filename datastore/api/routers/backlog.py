"""Backlog management API routes."""

from typing import Optional

from fastapi import APIRouter, HTTPException

from datastore.api.db import get_duckdb_connection

router = APIRouter(prefix="/api/v1/backlog", tags=["backlog"])


@router.get("")
def list_backlog(status: Optional[str] = None, criticality: Optional[str] = None):
    """List all backlog items with optional filtering."""
    db = get_duckdb_connection()

    query = """
        SELECT
            b.item_id,
            b.title,
            b.description,
            b.category,
            b.status,
            b.priority,
            b.criticality,
            b.reason_critical,
            b.assigned_to,
            b.created_at,
            b.updated_at,
            COUNT(DISTINCT bd.blocks_on_id) as blocks_on_count,
            COUNT(DISTINCT bd2.dependent_id) as blocks_count
        FROM backlog_items b
        LEFT JOIN backlog_dependencies bd ON b.item_id = bd.dependent_id
        LEFT JOIN backlog_dependencies bd2 ON b.item_id = bd2.blocks_on_id
        WHERE 1=1
    """
    params = []

    if status:
        query += " AND b.status = ?"
        params.append(status)

    if criticality:
        query += " AND b.criticality = ?"
        params.append(criticality)

    query += """
        GROUP BY b.item_id, b.title, b.description, b.category, b.status,
                 b.priority, b.criticality, b.reason_critical, b.assigned_to,
                 b.created_at, b.updated_at
        ORDER BY b.priority ASC, b.criticality DESC, b.created_at DESC
    """

    try:
        results = db.execute(query, params).fetchall()
        columns = [desc[0] for desc in db.description]
        return [dict(zip(columns, row)) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{item_id}")
def get_backlog_item(item_id: str):
    """Get a single backlog item with its dependencies."""
    db = get_duckdb_connection()

    # Get the item
    item_result = db.execute(
        """
        SELECT
            item_id, title, description, category, status, priority,
            criticality, reason_critical, assigned_to, created_at, updated_at
        FROM backlog_items
        WHERE item_id = ?
        """,
        [item_id],
    ).fetchall()

    if not item_result:
        raise HTTPException(status_code=404, detail="Item not found")

    item = dict(zip([desc[0] for desc in db.description], item_result[0]))

    # Get dependencies (items this one blocks on)
    blocks_on = db.execute(
        """
        SELECT bd.blocks_on_id, bi.title, bd.reason
        FROM backlog_dependencies bd
        JOIN backlog_items bi ON bd.blocks_on_id = bi.item_id
        WHERE bd.dependent_id = ?
        """,
        [item_id],
    ).fetchall()
    item["blocks_on"] = [
        {"item_id": row[0], "title": row[1], "reason": row[2]} for row in blocks_on
    ]

    # Get dependents (items that block on this one)
    blocks = db.execute(
        """
        SELECT bd.dependent_id, bi.title, bd.reason
        FROM backlog_dependencies bd
        JOIN backlog_items bi ON bd.dependent_id = bi.item_id
        WHERE bd.blocks_on_id = ?
        """,
        [item_id],
    ).fetchall()
    item["blocks"] = [{"item_id": row[0], "title": row[1], "reason": row[2]} for row in blocks]

    return item


@router.post("")
def create_backlog_item(
    item_id: str,
    title: str,
    category: str,
    priority: int = 3,
    criticality: str = "medium",
    reason_critical: Optional[str] = None,
    description: Optional[str] = None,
):
    """Create a new backlog item."""
    db = get_duckdb_connection()

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
        return {"item_id": item_id, "status": "created"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.patch("/{item_id}/status")
def update_status(item_id: str, status: str, assigned_to: Optional[str] = None):
    """Update item status and optionally assign to someone."""
    db = get_duckdb_connection()

    valid_statuses = ["blocked", "pending", "in-progress", "resolved"]
    if status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"Invalid status: {status}")

    try:
        query = """
            UPDATE backlog_items
            SET status = ?, updated_at = current_timestamp
        """
        params = [status]

        if assigned_to:
            query += ", assigned_to = ?"
            params.append(assigned_to)

        query += " WHERE item_id = ?"
        params.append(item_id)

        result = db.execute(query, params)
        if result.rows_affected == 0:
            raise HTTPException(status_code=404, detail="Item not found")

        return {"item_id": item_id, "status": status, "assigned_to": assigned_to}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{item_id}/block-on/{blocks_on_id}")
def add_dependency(item_id: str, blocks_on_id: str, reason: Optional[str] = None):
    """Add a dependency: item_id is blocked by blocks_on_id."""
    db = get_duckdb_connection()

    try:
        db.execute(
            """
            INSERT INTO backlog_dependencies
            (dependent_id, blocks_on_id, reason, created_at)
            VALUES (?, ?, ?, current_timestamp)
            """,
            [item_id, blocks_on_id, reason],
        )
        return {"dependent_id": item_id, "blocks_on_id": blocks_on_id, "status": "added"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/stats/summary")
def get_backlog_stats():
    """Get backlog summary statistics."""
    db = get_duckdb_connection()

    try:
        stats = db.execute(
            """
            SELECT
                COUNT(*) as total_items,
                COUNTIF(status = 'blocked') as blocked_count,
                COUNTIF(status = 'pending') as pending_count,
                COUNTIF(status = 'in-progress') as in_progress_count,
                COUNTIF(status = 'resolved') as resolved_count,
                COUNTIF(criticality = 'critical') as critical_count,
                COUNTIF(criticality = 'high') as high_count
            FROM backlog_items
            """
        ).fetchall()

        if stats:
            cols = [desc[0] for desc in db.description]
            return dict(zip(cols, stats[0]))
        return {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/bulk-import")
def bulk_import_items(items: list[dict]):
    """Bulk import backlog items from list of dicts."""
    db = get_duckdb_connection()

    try:
        for item in items:
            db.execute(
                """
                INSERT INTO backlog_items
                (item_id, title, description, category, status, priority,
                 criticality, reason_critical, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp, current_timestamp)
                """,
                [
                    item.get("item_id"),
                    item.get("title"),
                    item.get("description"),
                    item.get("category", "feature"),
                    item.get("status", "pending"),
                    item.get("priority", 3),
                    item.get("criticality", "medium"),
                    item.get("reason_critical"),
                ],
            )

        # Import dependencies if provided
        for item in items:
            if "blocks_on" in item:
                for blocks_on_id in item["blocks_on"]:
                    try:
                        db.execute(
                            """
                            INSERT INTO backlog_dependencies
                            (dependent_id, blocks_on_id, created_at)
                            VALUES (?, ?, current_timestamp)
                            """,
                            [item.get("item_id"), blocks_on_id],
                        )
                    except Exception:
                        pass  # Dependency may already exist

        return {
            "status": "imported",
            "count": len(items),
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
