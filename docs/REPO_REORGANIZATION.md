# Repository Reorganization: Centralized Backlog & Plans

This document describes the three-part system for managing work items and plans across AlphaLens.

## System Overview

### Part 1: Centralized Backlog

**Purpose**: Single source of truth for all work — discoveries from docs become backlog items.

All work tracked in `backlog_items` database table.

### Part 2: Plans Directory (`docs/plans/`)

**Purpose**: Strategic documents for work needing coordination.

- Multi-phase strategies
- Roadmaps
- Architectural proposals
- Pre-spec brainstorming

### Part 3: Archive Directory (`docs/archive/`)

**Purpose**: Preserve historical decisions (read-only).

- Completed phase files
- Superseded plans
- Old session logs
- Deprecated guides

## Quick Start

```bash
# Scan markdown files for pending items
python3 scripts/backlog_ops.py bulk-from-docs --scan

# Preview items to create
python3 scripts/backlog_ops.py bulk-from-docs --dry-run

# Create backlog items from findings
python3 scripts/backlog_ops.py bulk-from-docs --create
```

## What Gets Scanned

1. **Status markers**: ⏳ PENDING, 🔴 BLOCKED, 🔧 IN-PROGRESS, ✅ RESOLVED
2. **TODO markers**: [TODO(description)]
3. **Clarification markers**: [NEEDS CLARIFICATION: question]

## Next Steps

1. Run initial scan: `python3 scripts/backlog_ops.py bulk-from-docs --scan`
2. Create backlog items: `python3 scripts/backlog_ops.py bulk-from-docs --create`
3. Archive legacy files: Move completed work to `docs/archive/`
4. Use scrum-master to burn items

See full guide in this file for details.
