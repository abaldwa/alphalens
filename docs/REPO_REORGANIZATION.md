# Repository Reorganization: Centralized Backlog & Plans

This document describes the three-part system for managing work items and plans across AlphaLens.

## System Overview

### Part 1: Centralized Backlog (`backlog_items` table)

**Purpose**: Single source of truth for all work — discoveries from docs automatically become backlog items.

**What goes here**:
- Features (new functionality)
- Defects (bugs to fix)
- Technical debt (refactoring)
- Blockers (architectural issues)
- Research (investigations)
- Anything with a status (pending/in-progress/resolved)

**Mechanism**:
1. Scan script finds status markers in `.md` files: `⏳ PENDING`, `🔴 BLOCKED`, `🔧 IN-PROGRESS`, `✅ RESOLVED`
2. Scan script also finds `[TODO(...)]` and `[NEEDS CLARIFICATION: ...]` markers
3. Automatically creates backlog items + infers criticality from keyword matching
4. User reviews + updates priority/criticality as needed
5. Scrum-Master marks items in-progress/resolved via CLI

---

### Part 2: Plans Directory (`docs/plans/`)

**Purpose**: Strategic documents for work that needs coordination across phases/subsystems.

**What goes here**:
- Multi-phase implementation strategies (not yet in specs/)
- Roadmaps (quarterly priorities)
- Pre-spec brainstorming (exploration documents)
- Architectural proposals
- Dependency graphs for large initiatives

**When to use**:
- Complex work that spans multiple specs
- Decisions that affect multiple domains
- Anything that needs timeline/phase breakdown

**Distinction from specs/**:
```
Plan:  "How should we approach the ML adapter integration?"
Spec:  "What does the ML adapter need to do?"

Plan:  "Phase 1: Clarify architecture → Phase 2: Spec → Phase 3: Implement"
Spec:  "StrategyAdapter protocol: 2 methods, feature vectors, signal generation"
```

---

### Part 3: Archive Directory (`docs/archive/`)

**Purpose**: Preserve historical decisions + rationale without cluttering active project.

**What goes here**:
- Completed phase files (PHASE_*.md)
- Superseded plans (no longer active)
- Old session logs (BuildLog-*.md)
- Deprecated guides

**When to archive**:
- Plan has been executed and moved to specs/
- Phase has completed (PHASE_5_IMPLEMENTATION.md → archive)
- Session is older than 6 months

**Key rule**: Archive files are **read-only**. If you need to update something, it should be:
- In an active spec under `specs/`
- In an active plan under `docs/plans/`
- In the backlog table (for tracking work)

---

## Workflow

### Scenario 1: Discover a TODO while working

```markdown
# Some feature doc
There's a TODO that needs fixing:
[TODO(Fix the XYZ algorithm - it's computing wrong results)]
```

**Workflow**:
1. Run: `python3 scripts/backlog_ops.py bulk-from-docs --scan`
2. Scan finds the TODO → suggests: `SCAN-FEATURE-001: Fix the XYZ algorithm`
3. Approve: `python3 scripts/backlog_ops.py bulk-from-docs --create`
4. Backlog now tracks it; scrum-master can pick it up

---

### Scenario 2: Need multi-phase coordination

You're designing "ML Adapter integration". It spans:
- StrategyAdapter protocol decisions
- Feature vector generation
- Testing strategy
- Registry updates

**Workflow**:
1. Create `docs/plans/ml-adapter-integration/plan.md` with phases
2. Outline dependencies, timeline, risk areas
3. Once finalized, run `/speckit-specify` to create formal spec under `specs/ml/`
4. Archive plan (or link to it from spec for context)

---

### Scenario 3: Completed phase needs archival

You've finished PHASE_5_IMPLEMENTATION.md. Its decisions are now in specs/.

**Workflow**:
1. Rename: `PHASE_5_IMPLEMENTATION.md` → `docs/archive/PHASE_5_IMPLEMENTATION_20260825.md`
2. Add to `docs/archive/README.md` contents table
3. Delete original at repo root (it's now in archive)
4. Update any links pointing to the old location → point to archive instead

---

## CLI Commands

### Scan for items in markdown files

```bash
# See what the scanner would find
python3 scripts/backlog_ops.py bulk-from-docs --scan

# Output:
# 📋 Scan Results: 47 items found across 12 files
#
# 📄 FeatureBacklog.md (23 items)
#   ⏳ pending: Implement ML adapter feature_vector() method
#   🔴 blocked: Clarify StrategyAdapter protocol (blocked by ARCH-001)
#   ...
```

### Preview what would be created (dry-run)

```bash
python3 scripts/backlog_ops.py bulk-from-docs --dry-run

# Output:
# 🔮 DRY-RUN: Would create the following items:
# Would create: SCAN-FEATB-001
#   Title: Implement ML adapter feature_vector() method
#   Status: pending | Criticality: high
#   Category: feature | Source: FeatureBacklog.md
# ...
```

### Actually create backlog items

```bash
python3 scripts/backlog_ops.py bulk-from-docs --create

# Output:
# ✓ SCAN-FEATB-001: Implement ML adapter feature_vector() method
# ✓ SCAN-FEATB-002: Fix XYZ algorithm in Feature computation
# ⊙ SCAN-FEATB-003: already exists
# ...
# 
# Summary:
#   Created: 45
#   Skipped: 2
```

---

## Scanning Behavior

The scanning script looks for three patterns:

### 1. Status Markers

```markdown
⏳ PENDING: Implement feature X
🔴 BLOCKED: Need clarification on Y
🔧 IN-PROGRESS: Working on Z
✅ RESOLVED: Completed W
```

→ Creates item with that status

### 2. TODO Markers

```markdown
[TODO(Fix the performance bug in query layer)]
[TODO(Add validation for user input)]
```

→ Creates item with `category: task`, `status: pending`

### 3. Clarification Markers

```markdown
[NEEDS CLARIFICATION: Should we use async or sync API for signals?]
```

→ Creates item with `category: research`, `status: pending`, `criticality: high`

---

## Best Practices

### Do

✅ Run `bulk-from-docs --scan` weekly to discover emerging work  
✅ Create a plan for anything that needs >3 backlog items  
✅ Archive old files immediately after moving to spec/backlog  
✅ Link to archive files when citing past decisions  
✅ Update backlog items' criticality based on impact assessment  

### Don't

❌ Leave status markers in docs without running the scanner  
❌ Keep completed phases at repo root (archive them)  
❌ Edit archived files (copy to active location if you need to change something)  
❌ Create plans for work that fits in a single spec  
❌ Forget to add items to archive README when moving files  

---

## Integration with Scrum-Master

The `scrum-master` agent automatically:

1. **Pre-run**: Runs `bulk-from-docs --scan` to surface new discoveries
2. **During run**: Burns one clear backlog item
3. **Post-run**: Checks if burned item left behind any new `.md` files → archives them

This ensures no work gets lost between sessions.

---

## Maintenance Schedule

| Task | Frequency | Owner |
|------|-----------|-------|
| Scan for new items | Weekly (before scrum-master run) | scrum-master agent |
| Review + triage scanned items | Weekly | user |
| Archive old sessions | Monthly (1st of month) | scrum-master agent |
| Quarterly cleanup (archive >90 days) | Quarterly | user |

---

## Directory Structure

```
AlphaLens/
├── docs/
│   ├── archive/              ← Completed phases, old sessions
│   │   ├── README.md         ← Index + metadata
│   │   ├── PHASE_5_*_20260825.md
│   │   └── BuildLog_*_20260719.md
│   ├── plans/                ← Active multi-phase strategies
│   │   ├── README.md
│   │   ├── ml-adapter-integration/
│   │   │   ├── plan.md
│   │   │   ├── research.md
│   │   │   └── decisions.md
│   │   └── q3-roadmap.md
│   └── workspaces/           ← Session logs + discoveries
│       ├── README.md
│       ├── BuildLog-20260825.md
│       └── scan_results_20260825.json
├── specs/                    ← Formal specifications (Spec-Kit)
│   ├── momentum/
│   ├── ml/
│   └── technical/
├── backlog/
│   ├── items.json            ← Initial seed items
│   └── README.md
└── scripts/
    ├── backlog_ops.py        ← CLI tool (with bulk-from-docs command)
    └── backlog_from_docs.py  ← Scanning engine

Database:
├── backtest.duckdb
│   ├── backlog_items table   ← All work tracked here
│   └── backlog_dependencies  ← Dependency graph
```

---

## Examples

### Example 1: Weekly scan + triage

```bash
# Monday morning: scan for new discoveries
python3 scripts/backlog_ops.py bulk-from-docs --scan

# Review output, identify critical items
# Then approve creation:
python3 scripts/backlog_ops.py bulk-from-docs --create

# Check updated backlog via UI
# http://localhost:5173/backlog
```

### Example 2: Plan before implementing large feature

```bash
# Create a plan document
cat > docs/plans/valuation-screener-integration/plan.md << 'EOF'
# Valuation Screener Integration

## Phases
1. **Phase 1**: Extract valuation data from Damodaran
2. **Phase 2**: Build data pipeline into feature store
3. **Phase 3**: Create screener UI
4. **Phase 4**: Backtest with valuation signals

## Dependencies
- Phase 2 blocks Phase 3
- Phase 3 requires Phase 1 + data pipeline completion
