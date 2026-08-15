# BuildLog Updated

This document is a detailed, LLM-oriented rewrite of the implementation history. It preserves the substantive field-level, table-level, database-level, web-source-level, and design-level details from the original build log while reorganizing them by capability area so they are easier to scan and reason over.

## 1. Framework

### What was implemented
- Created the full AlphaLens project skeleton aligned to the architecture and specification documents.
- Established the package structure for ingestion, datastore, features, systems, backtest, dashboard, contracts, tests, and configuration.
- Added the base documentation and setup guidance for environment creation, dependency installation, and local execution.
- Created a project-local Python 3.11 environment using uv, avoiding dependency conflicts with the host OS Python.

### Environment and dependency decisions
- The project is intended to run in a dedicated virtual environment rather than the system Python.
- Requirements were pinned and grouped by phase so the environment can be recreated deterministically.
- The original mlfinlab dependency was replaced with a native Python/NumPy implementation for triple-barrier labeling, avoiding a dependency that was not necessary for the core functional requirement.

### Design decisions
- Favor a portable, dependency-light implementation over vendor-specific packages when the core behavior can be reproduced natively.
- Keep runtime setup explicit and documented; avoid implicit assumptions about the operating system or global Python installation.

---

## 2. Architecture

### System layering
- Ingestion layer: acquires raw market and fundamental data from external sources.
- Datastore layer: persists data in storage formats appropriate for transactional logging versus analytical querying.
- Feature layer: transforms raw data into feature matrices for ML and strategy logic.
- Systems layer: houses signal engines, forecasting logic, scoring logic, and model execution.
- Backtest and dashboard layers: consume persisted output and expose results to operators or analysts.

### Storage architecture
- SQLite is used for transactional pipeline and run logging.
- DuckDB is used for analytical tables such as OHLCV, fundamentals, corporate actions, signals, and related intermediate structures.
- This split reflects the different access patterns of operational logging versus large analytical data work.

### Datastore design
- A normalised store was created for canonical market and reference data.
- A signals store was created for pipeline state, model outputs, and signal tables.
- The API shell exposes read-only access to consumers and keeps storage internals out of downstream systems.

### Table-level design
- Store 2 (Normalised):
  - ohlcv_adjusted
  - corporate_actions
  - fundamentals
  - shareholding
  - macro_indicators
  - stock_master
- Store 4 (Signals):
  - pipeline_runs
  - pipeline_checkpoints
  - ml_signals
  - ml_multibagger
  - ml_forensic

### Database-level decisions
- Pipeline run state is stored in SQLite because it is operational and transactional.
- Analytical tables live in DuckDB because they are frequently queried, joined, and updated in bulk.
- The scheduler and ingestion layers use explicit in-memory test modes so tests can run without creating permanent files.

### API and service design
- A datastore client was implemented for consumer-side reads.
- The health endpoint reports last pipeline run state and degrades gracefully if the pipeline log tables do not exist yet.
- The service is intentionally read-oriented for consumers; ingestion writes directly to the underlying datastore rather than using the consumer API as a write path.

---

## 3. Pipeline

### Pipeline scope
The pipeline was built to support:
- market data ingestion,
- corporate-action-adjusted pricing,
- fundamentals and ownership data,
- macro data refreshes,
- signal generation,
- historical backfills,
- daily runs with resume support.

### Scheduler design
- Added a scheduler capable of daily execution and backfill execution.
- Implemented gap detection between the last successful pipeline date and the current date.
- Built checkpointing so a partially completed run can resume from the correct step rather than restarting from scratch.
- Backfill behavior was designed to process older dates first and to continue even if one date fails.

### Checkpointing details
- Checkpoints are stored in the pipeline logging database.
- Each checkpoint records the date, step name, step index, status, timestamps, error detail, and retry count.
- Resume logic identifies the next unfinished step rather than blindly replaying everything.
- The design explicitly avoids re-running steps that already completed successfully.

### Gap detection details
- Trading days are determined from weekday status plus NSE holiday exclusion.
- Weekend dates are excluded.
- The system detects gaps between the last successful run date and today.
- The logic is designed to support both backfill and ordinary daily execution.

### Operational design decisions
- The pipeline is resilient to partial failures.
- Long-running backfills are explicitly resumable.
- External network failures are treated as recoverable issues rather than fatal pipeline problems when the architecture allows it.

---

## 4. Ingestion Sources and Web Sources

### NSE bhavcopy
- The system ingests NSE equity bhavcopy data from the current combined archive format, using the newer combined report rather than older split-file conventions.
- The ingestion layer filters to equity series only and drops other series categories.
- Validation rules were added to ensure:
  - no duplicate tickers,
  - positive price values,
  - delivery percentage values stay within a valid range,
  - the source dataset meets a minimum completeness threshold.
- Raw files are retained in the raw-data storage folder for traceability.

### F&O bhavcopy
- A separate ingestion path was added for F&O bhavcopy data.
- The parser extracts fields such as instrument, ticker, expiry, strike, option type, OI, volume, and settlement price.
- This path is designed for derivatives-specific analysis and is kept separate from the equity pipeline.

### Macro sources
- The pipeline supports VIX, FII/DII, and FX ingestion.
- When a live fetch fails, the system falls back to the most recent prior stored value rather than failing the whole pipeline.
- FII/DII-specific fallback logic marks the data as stale when the live fetch is unavailable.

### FYERS historical data
- FYERS history is used for historical OHLCV backfills.
- The ingestion flow resolves an access token from several possible sources:
  1. an explicit token passed into the backfill object,
  2. a cached token on disk for the same day,
  3. an environment variable,
  4. an interactive or non-interactive OAuth-based login flow.
- History requests are chunked into manageable windows to respect API limits.
- The backfill loop uses rate limiting and daily call budgets.
- The system stores raw history artifacts per ticker and date range for reproducibility.

### NSE delivery history
- The delivery loader replays NSE archive data over the relevant backfill range.
- It updates delivery fields in rows that already exist in the OHLCV-adjusted table rather than creating a new data model.
- The loader is throttled to avoid overloading NSE archive endpoints.
- It is designed to skip dates that are unavailable or malformed rather than aborting the whole job.

### Universe and metadata sources
- The universe builder pulls official Nifty 500 constituent lists from NSE sources.
- The system assigns a tier based on the index sub-group the ticker belongs to.
- The universe file stores reference fields such as ticker, company name, sector, tier, and whether the company belongs to the Nifty 500 universe.
- Market capitalization and ADTV values are initially stored as placeholder or not-yet-sourced values where the data is unavailable.

---

## 5. ML and Signal Generation

### Triple-barrier labeling
- Implemented a native triple-barrier labeling function rather than relying on mlfinlab.
- The labeling logic accepts close prices, ATR, horizon, profit multiplier, stop multiplier, vertical barrier, and optional P&D blocking.
- It produces labels in the set {-1, 0, 1} and uses NaN for rows with insufficient future history.
- The implementation is vectorized and avoids Python-level loops over rows.

### ML target design
- The design was kept aligned with the documented model contract.
- P&D blocking can downgrade some positive labels to neutral labels when the date is blocked by a corporate or event-based restriction.

### Decision rationale
- The core requirement is the target-generation logic, not the presence of a specific third-party library.
- Re-implementing the logic natively keeps the project more maintainable and easier to audit.

---

## 6. Technical Features

### Technical infrastructure
- The scheduler and data pipeline support repeated execution over dates without manual intervention.
- The feature pipeline is designed to consume the canonical datastore rather than ad hoc scratch files.
- The implementation is set up so technical features can later be computed consistently over a broad universe.

### Technical design choices
- The system avoids hardcoding source-specific thresholds and paths across the codebase.
- Where a threshold or path is externally meaningful, it is routed through central settings rather than duplicated locally.
- The pipeline is robust to missing or delayed data by using fallback logic and non-fatal handling where warranted.

---

## 7. Fundamental Data

### Fundamental data model
- The datastore schema includes a fundamentals table for company financial statements and related metrics.
- The design is intended to support point-in-time queries, where the same metric can be interpreted differently depending on the as-of date.

### PIT design
- Point-in-time correctness is enforced through the query layer and the metadata semantics rather than by attempting to encode all historical circumstances in a static schema constraint.
- Announcement dates and filing dates are treated as important preconditions for correct point-in-time interpretation.

### Data handling rules
- The pipeline distinguishes between raw fundamentals, refreshed fundamentals, and the latest available values.
- The architecture ensures that consumers can request both current and historical values without depending on a single static snapshot.

### Decision rationale
- Fundamentals are not merely “latest values”; they must be interpretable historically.
- The system therefore uses explicit temporal semantics for these records rather than assuming all rows are equally valid for every date.

---

## 8. Forensic Accounting

### Implemented scope
- The repository includes forensic feature and model scaffolding intended for use in an accounting-risk or fraud-risk layer.
- There is explicit support for classical forensic scoring and ML-based composite scoring.
- Regression tests were created to ensure known fraud and known clean companies receive materially different scores.

### Design approach
- The forensic layer combines deterministic rules with ML-based enrichment.
- The scoring logic is designed to be interpretable, auditable, and testable against historical case studies.

### Practical implication
- Forensic logic is treated as a distinct model family rather than an add-on to a generic technical strategy.
- It can be used as a parallel signal stream in the broader ML and ranking pipeline.

---

## 9. Damodaran-style Valuation

### Current status
- No full Damodaran-style valuation workflow was completed in this build pass.
- The schema and data architecture are ready for the future addition of valuation-oriented features, but the actual modelling work is not yet implemented.

### Design decision
- Valuation work was intentionally deferred until the core price, fundamental, and signal infrastructure matured.
- The project currently prioritizes data ingestion, reliability, and signal generation over full valuation modelling.

---

## 10. Bulk Deals

### Current status
- No dedicated bulk-deal ingestion module was implemented in this pass.
- This was left as a future extension rather than a premature build target.

### Design decision
- The project first focused on core market data, corporate actions, fundamentals, and core signal infrastructure.
- Bulk-deal data can be attached later as an additional signal source without changing the higher-level architecture.

---

## 11. Start Investors

### Current status
- No dedicated start-investor dataset or ingestion flow was implemented in this pass.

### Design decision
- The system architecture can support additional alternative datasets later, but the first build cycle focused on the core data backbone and execution reliability.

---

## 12. Corporate Actions

### What was implemented
- Added corporate-action adjustment logic for splits, bonus issues, and related changes that alter price continuity.
- Implemented idempotent adjustment logic so the same adjustment is not applied repeatedly on reruns.
- Added continuity checks around ex-dates to detect abnormal price discontinuities.

### Field-level behavior
- The adjustment logic recomputes cumulative adjustment factors from the complete corporate-action history for a ticker.
- It compares the recomputed target factor against the stored factor and only changes rows when necessary.
- This avoids the common bug where the same corporate-action adjustment is applied multiple times.

### Database-level behavior
- Corporate-action data is stored in the normalised store.
- Price adjustment updates are applied in bulk rather than through slow per-row loops.
- The process is designed to be deterministic and repeatable.

### Design decision
- Corporate actions are handled as a post-processing layer over OHLCV rather than being embedded directly into every ingestion write path.
- This keeps the market-data ingestion path simple while allowing the adjustment layer to enforce consistency later.

---

## 13. Data Model Details by Table

### ohlcv_adjusted
- Purpose: canonical adjusted OHLCV data used for downstream analysis.
- Key fields include date, ticker, open, high, low, close, volume, and adjustment-related fields.
- Used as the primary source for price- and volume-based features.
- Stores both raw and adjusted pricing context so downstream systems can reason about splits, bonuses, and similar events.

### corporate_actions
- Purpose: stores corporate-action events and their effect on price scaling.
- Key fields include the event date, ticker, action type, ratio, and related announcement or filing dates.
- This table is the source of truth for price-adjustment logic.

### fundamentals
- Purpose: stores company financial and accounting metrics linked to a ticker and time period.
- Includes enough structure for point-in-time queries and historical inspection.
- Supports later feature generation for quality, valuation, quality, and growth-related signals.

### shareholding
- Purpose: stores ownership data such as promoter holding and other investor-level position data.
- Used for governance and ownership-related analysis.

### macro_indicators
- Purpose: stores macro and market environment data such as VIX, FII/DII, and FX.
- Designed to be merged into the broader feature set without coupling those features to a single source implementation.

### stock_master
- Purpose: stores reference metadata such as ticker, company name, sector, universe membership, and tier.
- Helps keep the universe consistent across ingestion, signals, and backtesting.

### pipeline_runs
- Purpose: tracks high-level pipeline execution runs.
- Fields include run identifier, execution date, start/end timestamps, status, stocks processed, and error detail.

### pipeline_checkpoints
- Purpose: tracks per-step progress within a run.
- Fields include the checkpoint date, step name, step index, execution state, retry count, timestamps, and error detail.

### ml_signals / ml_multibagger / ml_forensic
- Purpose: stores intermediate and final signal outputs for different model families.
- These tables are separated to make it easier to reason about model-specific outputs and avoid mixing signal types in a single structure.

---

## 14. Key Decisions that Shaped the Build

1. Use a layered architecture rather than embedding all logic into one script.
2. Split transactional logging and analytical data into different databases.
3. Keep ingestion writes direct to the datastore while consumer systems read through API/client abstractions.
4. Make the pipeline resumable rather than brittle to temporary outages.
5. Use a native Python implementation for triple-barrier labeling instead of a third-party dependency that was not essential.
6. Treat corporate actions as a first-class adjustment layer rather than a one-off patch.
7. Treat missing or partially sourced metadata as “not yet sourced” rather than silently excluding it from the universe.
8. Use sequential DuckDB writes to avoid lock conflicts when multiple jobs try to write the same file.
9. Keep raw-source artifacts for traceability and debugging.
10. Favor explicit configuration and settings-driven thresholds over hardcoded values.

---

## 15. Verification and Operational Notes

### What was verified
- The core environment and dependency install path were validated.
- The scheduler and checkpoint logic were tested through unit and integration tests.
- The schema and store creation paths were verified with dedicated tests.
- The price-adjustment logic was verified to be idempotent.
- The FYERS backfill flow was tested for resume behavior and token fallback behavior.
- The delivery-loader logic was tested for correct row updates and no cross-date contamination.

### Notable operational insights
- Network and API failures are common in live scraping and must be handled as recoverable conditions.
- Daily call budgets and token expiry are important constraints for live backfills.
- Historical backfills are large, long-running jobs and should be treated as operator-driven tasks rather than one-shot local commands.
- The pipeline must be run carefully to avoid database lock conflicts and partial-state issues.

---

## 16. Current Status Snapshot

- The framework, datastore, scheduler, ingestion, and adjustment layers are now present and connected.
- Historical backfills and daily pipeline execution patterns are supported.
- Corporate-action adjustment and delivery-data replay are implemented.
- The universe has been expanded from a placeholder sample toward a broader Nifty-based universe.
- The project is now moving from scaffold-building into operational refinement, coverage expansion, and data-quality hardening.
