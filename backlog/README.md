# AlphaLens Backlog System

Centralized tracking for project work items, blockers, technical debt, and defects.

## Quick Start

### 1. Initialize the Backlog

The backlog tables are created automatically when the API starts (`datastore/schema/create_normalised.py`). To seed with initial items:

```bash
python3 scripts/seed_backlog.py
```

### 2. View the Backlog

Open the frontend at http://localhost:3000/backlog (after `npm run dev` in the frontend directory).

### 3. Manage Items via CLI

Add a new item:
```bash
python3 scripts/backlog_ops.py add \
  --id "FEAT-001" \
  --title "Add user authentication" \
  --category "feature" \
  --priority 1 \
  --criticality "high" \
  --reason "Blocks deployment"
```

Update status:
```bash
python3 scripts/backlog_ops.py mark-status FEAT-001 in-progress
python3 scripts/backlog_ops.py mark-resolved FEAT-001
```

Add a dependency:
```bash
python3 scripts/backlog_ops.py add-dependency FEAT-001 FEAT-002 "FEAT-001 must be done before FEAT-002"
```

List items:
```bash
python3 scripts/backlog_ops.py list
python3 scripts/backlog_ops.py list --status blocked
python3 scripts/backlog_ops.py list --criticality critical
```

## Backlog Item Structure

| Field | Description | Example |
|-------|-------------|---------|
| `item_id` | Unique identifier | `ARCH-001`, `SPEC-002`, `IMPL-005` |
| `title` | Short summary | "Fix DuckDB lock contention" |
| `description` | Detailed explanation | (optional) |
| `category` | Type of work | `blocker`, `defect`, `dependency`, `feature`, `research` |
| `status` | Current state | `blocked`, `pending`, `in-progress`, `resolved` |
| `priority` | P1-P5 (1=highest) | `1`, `2`, `3` |
| `criticality` | Impact level | `critical`, `high`, `medium`, `low` |
| `reason_critical` | Why it's critical | "Blocks all implementation if not fixed" |
| `assigned_to` | Who's working on it | `"scrum-master"`, `"claude"`, or username |
| `blocks_on` | Dependencies (other item IDs) | `["ARCH-001", "SPEC-001"]` |

## Criticality Levels

- **🔴 CRITICAL**: Prevents implementation; wrong decision → 100% rework risk
- **🟠 HIGH**: Gates a major phase; missing → cascading failures
- **🟡 MEDIUM**: Improves workflow but not blocking; low risk if delayed
- **⚪ LOW**: Optimization; can be done anytime

## Status Workflow

```
blocked → pending → in-progress → resolved
   ↑                                  ↓
   ←───────────────────────────────────
```

- **blocked**: Cannot start due to dependencies; waiting for other items
- **pending**: Ready to start; no dependencies blocking
- **in-progress**: Currently being worked on (assigned to someone)
- **resolved**: Completed and verified

## API Endpoints

### Read Operations
- `GET /api/v1/backlog` — List all items (optional: `?status=pending&criticality=critical`)
- `GET /api/v1/backlog/{item_id}` — Get item detail + dependencies
- `GET /api/v1/backlog/stats/summary` — Get summary stats (counts by status/criticality)

### Write Operations
- `POST /api/v1/backlog` — Create a new item
- `PATCH /api/v1/backlog/{item_id}/status` — Update status
- `POST /api/v1/backlog/{item_id}/block-on/{blocks_on_id}` — Add dependency
- `POST /api/v1/backlog/bulk-import` — Seed items from JSON

## Scrum-Master Agent Integration

When requesting work on a backlog item:

```
User: "Scrum-Master, work on item SPEC-001"
```

The Scrum-Master agent should:

1. **Mark in-progress**:
   ```python
   python3 scripts/backlog_ops.py mark-in-progress SPEC-001
   ```

2. **Execute the work** (run /speckit-clarify, fix tests, etc.)

3. **Mark resolved**:
   ```python
   python3 scripts/backlog_ops.py mark-resolved SPEC-001 --notes "Spec updated with correct interface"
   ```

4. **Automatically unblock** any items that were waiting on SPEC-001 (they'll transition from `blocked` to `pending`)

## Data Model

### backlog_items table
```sql
CREATE TABLE backlog_items (
  item_id VARCHAR PRIMARY KEY,
  title VARCHAR NOT NULL,
  description TEXT,
  category VARCHAR,
  status VARCHAR DEFAULT 'pending',
  priority INTEGER DEFAULT 3,
  criticality VARCHAR DEFAULT 'medium',
  reason_critical TEXT,
  assigned_to VARCHAR,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);
```

### backlog_dependencies table
```sql
CREATE TABLE backlog_dependencies (
  dependent_id VARCHAR,
  blocks_on_id VARCHAR,
  reason TEXT,
  created_at TIMESTAMP,
  PRIMARY KEY (dependent_id, blocks_on_id)
);
```

## Examples

### Adding a Defect
```bash
python3 scripts/backlog_ops.py add \
  --id "DEFECT-042" \
  --title "Fundamentals PIT bug: announcement_date wrong type" \
  --category "defect" \
  --priority 1 \
  --criticality "high" \
  --reason "Wrong type causes PIT validation failures"
```

### Adding Technical Debt
```bash
python3 scripts/backlog_ops.py add \
  --id "DEBT-015" \
  --title "Refactor DuckDB locking strategy for concurrent pipelines" \
  --category "technical-debt" \
  --priority 3 \
  --criticality "medium" \
  --reason "Current lock contention blocks parallel backfills"
```

### Adding a Feature
```bash
python3 scripts/backlog_ops.py add \
  --id "FEAT-001" \
  --title "Live portfolio performance dashboard" \
  --category "feature" \
  --priority 2 \
  --criticality "high" \
  --reason "Needed for paper trading launch"
```

### Adding a Dependency
```bash
python3 scripts/backlog_ops.py add-dependency FEAT-001 FEAT-002 \
  --reason "Need authentication (FEAT-002) before performance dashboard"
```

## Frontend Workflow

1. **View backlog** at `/backlog`
2. **Toggle views**: Table ↔ Kanban
3. **Filter by**:
   - Status: blocked, pending, in-progress, resolved
   - Criticality: critical, high, medium, low
4. **Click items** to see dependencies and details
5. **Statuses update automatically** when Scrum-Master marks items resolved

## Reporting

Use the backlog stats endpoint for dashboards:
```bash
curl http://localhost:8123/api/v1/backlog/stats/summary
```

Response:
```json
{
  "total_items": 18,
  "blocked_count": 3,
  "pending_count": 8,
  "in_progress_count": 2,
  "resolved_count": 5,
  "critical_count": 3,
  "high_count": 4
}
```

## Release Gates

| Gate | Status | Unblocks | When |
|------|--------|----------|------|
| A: Clarification | 🔴 BLOCKED | Implementation begins | Resolve ARCH-001, ARCH-002, ARCH-003 |
| B: Spec Accuracy | 🔴 BLOCKED | /speckit-tasks | Complete SPEC-001, SPEC-002, SPEC-003, SPEC-004, SPEC-005 |
| C: Implementation Ready | ⏳ PENDING | Backtest parity | Complete IMPL-001, IMPL-002, IMPL-003, IMPL-004 |
| D: Production Ready | ⏳ PENDING | Paper trading | Complete IMPL-005, validation tests |

## Next Steps

1. ✅ Database schema created
2. ✅ API endpoints deployed
3. ✅ React UI built (table + kanban views)
4. ✅ CLI helper script available
5. ⏳ Initial items seeded (run `python3 scripts/seed_backlog.py`)
6. ⏳ Scrum-Master integration (run `/speckit-clarify` on architectural questions)
7. ⏳ First item resolved (when ARCH-001 is complete, ARCH-002 and ARCH-003 transition to `pending`)
