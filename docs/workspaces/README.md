# Session Workspaces

Build logs, decision records, and session outputs from Claude Code sessions.

## Purpose

This directory captures:
- **Build logs** — Timestamped summaries of what was built each session
- **Decision records** — Architectural decisions + rationale (ADR format)
- **Session outputs** — Research findings, audit reports, scan results
- **Experiment logs** — One-off exploration results (not part of main backlog)

## Contents

| Session | Date | Summary |
|---------|------|---------|
| | | |

## Usage

1. **Create a workspace** per major session:
   - `BuildLog-20260825.md` — what was accomplished
   - `decisions/ADR-001-*.md` — architectural choices made
   - `scan_results_20260825.json` — structured findings

2. **Link to backlog** — if session discovers work, create backlog item + link to discovery document

3. **Archive** — after 6 months, move to `docs/archive/` (compress if large)

## Conventions

- Use ISO date format (YYYY-MM-DD) in filenames
- Keep session docs brief (decision body, not narrative)
- Link to code locations using repo-relative paths
- When multiple sessions build on same work, use consistent prefix (e.g., `ml-adapter-*`)

---

**Note**: This is where discovery happens. Move findings to `FeatureBacklog.md`, `backlog_items` table, or `specs/` to persist them.
