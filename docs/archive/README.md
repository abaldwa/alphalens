# Archive: Historical Plans & Documentation

This folder contains historical plan documents, completed implementation notes, and reference material from previous phases of AlphaLens development. These files are preserved for historical context but are no longer the source of truth for ongoing work.

## Contents

| File | Date | Purpose | Status |
|------|------|---------|--------|
| PHASE1_REGIME_COMPLETION.md | 2026-08-23 | Regime detector implementation completion | ✅ Complete, 26 tests passing |
| PHASE2_REGIME_INTEGRATION.md | 2026-08-23 | Engine integration of regime features | ✅ Complete, 42 tests passing |
| PHASE5_IMPLEMENTATION_SUMMARY.md | 2026-08-23 | Risk-adjusted composite momentum implementation | ✅ Complete, ready for validation |
| PHASE5_VALIDATION_RESULTS.md | 2026-08-23 | Validation results for Phase 5 work | ✅ Complete, gate decision PASS |
| PHASE6_R6_IC_ANALYSIS_RESULTS.md | 2026-08-23 | IC analysis for momentum variants | ✅ Complete analysis |
| BacktestPlan_StreamlinedM1toM13.md | 2026-08-23 | Specific backtest execution plan for M1-M13 | ✅ Superseded by ongoing queue management |
| EMA_RSI_REGIME_PLAN.md | 2026-08-22 | Position-sizing regime overlay plan | ✅ Implemented (commit 253afab9) |
| BacktestUmbrellaPlan.md | N/A | Unified backtest architecture specification | 📋 Reference: foundational architecture decisions |
| UnifiedGeneratorRefactorPlan.md | N/A | Live trading path unification proposal | 📋 Reference: architectural gap documentation |
| EXECUTION_FRAMEWORK_QUICKSTART.md | 2025-06-19 | Framework v2.0 setup guide | ⚠️ Outdated (2025) |
| FRAMEWORK_SETUP_COMPLETE.md | 2025-06-19 | Framework v2.0 setup completion notes | ⚠️ Outdated (2025) |
| HANDOFF_technical_backtest_20260819.md | 2026-08-19 | Technical backtest job queue handoff | ✅ Completed, job-specific |
| UNIFIED_BACKTEST_QUICK_START.md | 2026-08-19 | Job queue launch operational guide | 📋 Reference: operational procedures |
| CLEANUP_GUIDE.md | 2026-08-23 | Directory reorganization documentation | 📋 Reference: directory structure |

## How to Use This Archive

- **Completed work (✅):** Historical reference only. Actual implementation is verified by passing tests and commit history. See `git log` for exact completion dates and commit messages.
- **Reference documents (📋):** Contain architectural reasoning, decisions, and design rationale worth understanding. If you're working on related systems, read these for context before implementing changes.
- **Outdated docs (⚠️):** Created >6 months ago and may reference superseded tooling/versions. Check `CLAUDE.md` for current guidance.

## Active Work

For **forward-looking specifications**, see the top-level `specs/` directory, organized by domain and pipeline stage. Each spec includes:
- `spec_id` (unique identifier like SPEC-BT-001)
- `stages` (which pipeline stages it applies to)
- `status` (draft, active, completed)
- Clear acceptance criteria and open questions

For **prioritized work**, see `FeatureBacklog.md` with its `Spec` column linking to relevant specs.

---

**Last updated:** 2026-08-25
