# Backlog System - Setup & Launch

## ✅ Completed Components

### 1. **Database Schema** ✓
- `backlog_items` table (item_id, title, category, status, priority, criticality, reason_critical, assigned_to)
- `backlog_dependencies` table (dependent_id, blocks_on_id, reason)
- Added to `datastore/schema/create_normalised.py`

### 2. **Backend API** ✓
- New router: `datastore/api/routers/backlog.py`
- Endpoints:
  - `GET /api/v1/backlog` — List items with filtering
  - `GET /api/v1/backlog/{item_id}` — Get item detail
  - `POST /api/v1/backlog` — Create item
  - `PATCH /api/v1/backlog/{item_id}/status` — Update status
  - `POST /api/v1/backlog/{item_id}/block-on/{blocks_on_id}` — Add dependency
  - `GET /api/v1/backlog/stats/summary` — Get stats
  - `POST /api/v1/backlog/bulk-import` — Bulk import
- Registered in `datastore/api/main.py`

### 3. **Frontend UI** ✓
- `frontend/src/pages/backlog/BacklogPage.tsx` — Main page with table/kanban toggle
- `frontend/src/pages/backlog/components/BacklogTable.tsx` — ag-grid table view
- `frontend/src/pages/backlog/components/BacklogKanban.tsx` — Kanban board view
- `frontend/src/pages/backlog/hooks/useBacklog.ts` — TanStack Query hooks
- Navigation added to `frontend/src/lib/ui/nav.ts` (top-level menu)
- Route added to `frontend/src/app/router.tsx`

### 4. **CLI Tools** ✓
- `scripts/backlog_ops.py` — Command-line backlog manager
  - `add` — Add new items
  - `mark-status` — Update status
  - `mark-in-progress` — Mark as in-progress
  - `mark-resolved` — Mark as resolved
  - `add-dependency` — Add dependencies
  - `list` — List items
- `scripts/seed_backlog.py` — Import items from JSON

### 5. **Initial Backlog** ✓
- `backlog/items.json` — 16 initial items with full criticality & dependencies
- `backlog/README.md` — Complete documentation

---

## 🚀 Launch Instructions

### Step 1: Start the API Server
This will create the backlog database tables:

```bash
cd /home/amit/projects/AlphaLens
python3 -m uvicorn datastore.api.main:app --host 127.0.0.1 --port 8123 --reload
```

**Wait for**: `Application startup complete`

### Step 2: Seed the Backlog (in a new terminal)
```bash
cd /home/amit/projects/AlphaLens
python3 scripts/seed_backlog.py
```

**Expected output**:
```
✓ ARCH-001: Clarify StrategyAdapter protocol: actual vs. spec mismatch
✓ ARCH-002: Reconcile existing ml_adapter.py: is it a StrategyAdapter or result translator?
... (16 items total)
✓ Backlog seeding complete!
```

### Step 3: Start the Frontend
In another terminal:

```bash
cd /home/amit/projects/AlphaLens/frontend
npm run dev
```

### Step 4: Open the Backlog
Navigate to: **http://localhost:5173/backlog**

You should see:
- **Stats bar** with counts (blocked, pending, in-progress, resolved, critical, high)
- **Table view** showing all 16 backlog items (default)
- **Toggle buttons** to switch to Kanban view
- **Filter dropdowns** for status and criticality
- **Color-coded** priority and criticality badges

---

## 📋 Initial Backlog Items

### Blockers (Must resolve before implementation)
| ID | Title | Status | Criticality |
|---|---|---|---|
| ARCH-001 | Clarify StrategyAdapter protocol mismatch | 🔴 Blocked | 🔴 CRITICAL |
| ARCH-002 | Reconcile ml_adapter.py role | 🔴 Blocked | 🔴 CRITICAL |
| ARCH-003 | Verify BacktestEngine architecture decision | 🔴 Blocked | 🔴 CRITICAL |

### Research & Spec Fixes (High priority)
| ID | Title | Status | Criticality |
|---|---|---|---|
| SPEC-001 | Run /speckit-clarify (3 questions) | ⏳ Pending | 🟠 HIGH |
| SPEC-002 | Update spec.md (2 methods, not 3) | ⏳ Pending | 🟠 HIGH |
| SPEC-003 | Update data-model.md | ⏳ Pending | 🟠 HIGH |
| SPEC-004 | Update quickstart.md | ⏳ Pending | 🟠 HIGH |
| SPEC-005 | Update research.md | ⏳ Pending | 🟡 MEDIUM |

### Implementation Tasks (High priority, blocked until specs fixed)
| ID | Title | Status | Criticality |
|---|---|---|---|
| IMPL-001 | Generate /speckit-tasks | ⏳ Pending | 🟠 HIGH |
| IMPL-002 | Implement generate_signals() | ⏳ Pending | 🟠 HIGH |
| IMPL-003 | Implement feature_vector() | ⏳ Pending | 🟠 HIGH |
| IMPL-004 | Add ML adapter tests | ⏳ Pending | 🟠 HIGH |
| IMPL-005 | Register ML strategies | ⏳ Pending | 🟡 MEDIUM |

### Backlog Management (Infrastructure)
| ID | Title | Status | Criticality |
|---|---|---|---|
| BACKLOG-001 | Create backlog UI | 🔵 In Progress | 🟡 MEDIUM |
| BACKLOG-002 | Add menu item "Backlog" | 🔵 In Progress | 🟡 MEDIUM |
| BACKLOG-003 | Document backlog workflows | ⏳ Pending | 🟡 MEDIUM |

---

## 💡 Usage Examples

### View backlog in table format
Default view at http://localhost:5173/backlog

### Switch to Kanban view
Click the "🔲" icon in the control bar

### Filter by status
Select from dropdown: "All Statuses", "Blocked", "Pending", "In Progress", "Resolved"

### Filter by criticality
Select from dropdown: "All Levels", "🔴 Critical", "🟠 High", "🟡 Medium", "⚪ Low"

### Add a new backlog item via CLI
```bash
python3 scripts/backlog_ops.py add \
  --id "TECH-001" \
  --title "Refactor session management" \
  --category "technical-debt" \
  --priority 3 \
  --criticality "high" \
  --reason "Current implementation has concurrency bugs"
```

### Mark an item as in-progress (Scrum-Master)
```bash
python3 scripts/backlog_ops.py mark-in-progress SPEC-001
```

### Mark an item as resolved
```bash
python3 scripts/backlog_ops.py mark-resolved SPEC-001 --notes "All 3 questions answered"
```

### Add a dependency
```bash
python3 scripts/backlog_ops.py add-dependency SPEC-002 SPEC-001 \
  --reason "Depends on clarification questions being answered"
```

### List only critical items
```bash
python3 scripts/backlog_ops.py list --criticality critical
```

### List only blocked items
```bash
python3 scripts/backlog_ops.py list --status blocked
```

---

## 📊 Dashboard Stats

The backlog provides real-time stats:
```json
{
  "total_items": 16,
  "blocked_count": 3,
  "pending_count": 8,
  "in_progress_count": 2,
  "resolved_count": 0,
  "critical_count": 3,
  "high_count": 4
}
```

Fetch via API:
```bash
curl http://localhost:8123/api/v1/backlog/stats/summary
```

---

## 🔄 Workflow: From Backlog to Done

1. **User identifies work** → Add to backlog with criticality level
2. **Scrum-Master picks up item** → Request with `/speckit-clarify` or similar task
3. **Scrum-Master marks in-progress** → `mark-in-progress ITEM-ID`
4. **Work completed** → Scrum-Master updates dependent items if any
5. **Mark resolved** → `mark-resolved ITEM-ID`
6. **Auto-unblock** → Items blocked on ITEM-ID transition to pending

---

## 📝 Next Steps After Launch

1. ✅ Verify backlog loads at http://localhost:5173/backlog
2. ✅ Run `/speckit-clarify` to answer the 3 architectural questions (SPEC-001)
3. ✅ Mark SPEC-001 resolved (will auto-unblock SPEC-002, SPEC-003, etc.)
4. ✅ Update specs based on clarification answers
5. ✅ Mark SPEC-002, SPEC-003, SPEC-004, SPEC-005 as resolved
6. ✅ Run `/speckit-tasks` to generate implementation task list (IMPL-001)
7. ✅ Begin implementation tasks (IMPL-002, IMPL-003, etc.)

---

## 🐛 Troubleshooting

**Backlog page shows "Loading backlog items..."**
→ Check API is running at http://localhost:8123/api/v1/backlog (should return JSON list)

**Can't find "Backlog" in menu**
→ Restart frontend dev server (`npm run dev` in frontend directory)

**Error: "backlog_items table does not exist"**
→ Restart API server to trigger schema creation

**Items not appearing after seed**
→ Run `python3 scripts/seed_backlog.py` and check for errors
→ Verify API server is running

**Dependencies not showing in UI**
→ Run `/speckit-clarify` on ARCH-001 first to see how dependencies work

---

## 📚 Documentation

- **[backlog/README.md](backlog/README.md)** — Detailed backlog documentation
- **[scripts/backlog_ops.py](scripts/backlog_ops.py)** — CLI tool documentation
- **Database schema** — in `datastore/schema/create_normalised.py`
- **API endpoints** — in `datastore/api/routers/backlog.py`
- **Frontend** — in `frontend/src/pages/backlog/`

---

## ✨ Features

✅ **Read-only dashboard** with table & kanban views  
✅ **CLI management** for adding/updating/resolving items  
✅ **Dependency tracking** to show what blocks what  
✅ **Real-time stats** for project overview  
✅ **Bulk import** from JSON configuration  
✅ **Status filtering** (blocked, pending, in-progress, resolved)  
✅ **Criticality levels** (critical, high, medium, low)  
✅ **Color-coded** badges and indicators  
✅ **Agent integration** ready for Scrum-Master automation  

---

Ready to launch! Start with the three commands above and you'll have a fully functional backlog system.
