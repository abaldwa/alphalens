"""
ingestion/scheduler/exception_catalog.py

Phase: Pipeline & Monitoring Remediation, Phase 0.3
Owner: Platform / Scheduler
Consumers: datastore/api/routers/ops.py (Ops dashboard "Exceptions" panel)

Catalog of every intentionally-swallowed exception path in the daily
pipeline (SPEC-PIPE-006's "mark unavailable, non-critical" philosophy).
Each of `daily_pipeline.py`'s `except Exception: ...log-and-continue`
blocks is deliberate, but until now there was no single place that told
an operator, for a given step, what breaks downstream if it actually
fires and what to do about it. This module is that place.

This catalog is a static registry, not a log parser: it documents the
*design* of each catch site (impact + remediation), keyed by
`module:line` so an entry is tied to the exact code it describes and
goes stale (rather than silently drifting) if that line moves without
the catalog being updated — `test_exception_catalog.py` asserts every
`file:line` still points at an `except` statement.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class ExceptionCatalogEntry:
    step_name: str
    location: str  # "module.py:line"
    caught: str  # what kind of failure this except block catches
    impact: str  # what breaks downstream if this fires
    remediation: str  # concrete operator action
    severity: str  # "info" | "warning" | "critical"


CATALOG: list[ExceptionCatalogEntry] = [
    # 2026-07-29: download_fno was promoted to critical (its scrape/write
    # failure now propagates instead of being caught here) — removed from
    # this catalog since it's no longer a swallowed exception. See
    # step_download_fno's docstring in daily_pipeline.py.
    ExceptionCatalogEntry(
        step_name="download_index_ohlcv",
        location="ingestion/scheduler/daily_pipeline.py:726",
        caught="NSE indices-close archive fetch or index_ohlcv write "
        "failure (A31).",
        impact="index_ohlcv missing for the date — sector-rotation report "
        "and backtest benchmark curve for this date are stale/incomplete. "
        "Not on the critical path for signal generation.",
        remediation="Non-critical — rerun via "
        "POST /api/v1/ops/steps/download_index_ohlcv/force if needed for "
        "a specific historical date.",
        severity="info",
    ),
    # 2026-07-30: download_corporate_actions was promoted to critical (its
    # scrape/write failure now propagates instead of being caught here,
    # mirroring download_fno's 2026-07-29 fix) — removed from this catalog
    # since it's no longer a swallowed exception. See
    # step_download_corporate_actions's docstring in daily_pipeline.py.
    ExceptionCatalogEntry(
        step_name="download_large_deals",
        location="ingestion/scheduler/daily_pipeline.py:1074",
        caught="Combined NSE+BSE bulk/block deal fetch or persist "
        "failure (each of the 4 underlying sources is independently "
        "caught inside download_large_deals() itself; this is the "
        "outer wrap + DB write).",
        impact="large_deals has no new rows for the date; "
        "step_attribute_bulk_deals (hard-depends on this step) is "
        "skipped for the date via checkpoint.py's dependency logic — "
        "bulk_deal_positions attribution silently has a gap for that day.",
        remediation="Non-critical for same-day signal generation. Rerun "
        "via POST /api/v1/ops/steps/download_large_deals/force, then "
        "force-run attribute_bulk_deals for the same date once large_deals "
        "is backfilled.",
        severity="warning",
    ),
    ExceptionCatalogEntry(
        step_name="attribute_bulk_deals",
        location="ingestion/scheduler/daily_pipeline.py:1114",
        caught="Wash-trade netting / investor_family attribution failure "
        "over that date's own large_deals rows.",
        impact="bulk_deal_positions missing for the date. Purely a "
        "downstream analytics table — no other STEP depends on it.",
        remediation="Non-critical — rerun via "
        "POST /api/v1/ops/steps/attribute_bulk_deals/force once the "
        "underlying cause (usually a large_deals data-shape issue) is "
        "understood.",
        severity="info",
    ),
    ExceptionCatalogEntry(
        step_name="compute_momentum",
        location="ingestion/scheduler/daily_pipeline.py:2247",
        caught="strategy_signals dual-write failure (B1) for one momentum "
        "strategy. momentum_rankings for the same date is already committed "
        "at this point.",
        impact="The dashboard and every momentum read path are UNAFFECTED — "
        "they serve momentum_rankings. What is lost is the audit trail: this "
        "date has no strategy_signals row naming the registry revision that "
        "produced the pick, so a live trade from this date cannot be traced "
        "back to its declaration.",
        remediation="Usually a DuckDB write-lock contention on "
        "BACKTEST_DUCKDB_PATH — check whether a backtest queue was running. "
        "Re-running the step for the date is safe and idempotent "
        "(write_signals replaces rows on the same key).",
        severity="warning",
    ),
    ExceptionCatalogEntry(
        step_name="propose_paper_trades",
        location="ingestion/scheduler/daily_pipeline.py:2131",
        caught="One deployed strategy declares a filter the live path has no "
        "data source for (quality scores, HMM regime labels, market-cap/beta "
        "panels are backtest-time inputs). A deliberate refusal, not a fault.",
        impact="That ONE strategy gets no proposal today; every other active "
        "deployment is unaffected. Nothing wrong is proposed — the refusal "
        "exists because running a strategy without its declared filters is "
        "running a different strategy from the one that was backtested.",
        remediation="Either give the live path a real source for the declared "
        "filter, or stop deploying a strategy that depends on it. Never "
        "'fix' this by dropping the filter from the registry row.",
        severity="warning",
    ),
    ExceptionCatalogEntry(
        step_name="propose_paper_trades",
        location="ingestion/scheduler/daily_pipeline.py:2134",
        caught="Any other per-deployment failure while building the adapter "
        "or generating today's proposals (missing registry row, missing "
        "feature snapshot, a channel with no declared holdings count).",
        impact="That ONE deployment has no proposal queued for the date; the "
        "loop continues to the next. Nothing executes either way — every "
        "proposal requires a human accept() — so the cost is a missed day of "
        "review for that strategy, not a wrong trade.",
        remediation="Read the logged traceback: a missing registry row means "
        "the deployment references a strategy that was never declared; a "
        "missing feature snapshot means compute_features did not finish for "
        "the date. Re-run the step after fixing the cause.",
        severity="warning",
    ),
    ExceptionCatalogEntry(
        step_name="publish_and_snapshot",
        location="ingestion/scheduler/daily_pipeline.py:2387",
        caught="N=7 rollback snapshot (fno_data, ohlcv_adjusted) write or "
        "prune failure. Runs last, after every other writer for the date.",
        impact="No new rollback snapshot exists for this date — "
        "scripts/restore_snapshot.py cannot roll back to this date's "
        "state if a later bug corrupts fno_data/ohlcv_adjusted. Does NOT "
        "affect the correctness of today's data, only the safety net for "
        "future days.",
        remediation="If this fires for more than 1-2 consecutive days, "
        "treat as critical — the rollback safety net is degrading. Check "
        "disk space under config.settings.SNAPSHOT_DIR and rerun via "
        "POST /api/v1/ops/steps/publish_and_snapshot/force.",
        severity="warning",
    ),
    ExceptionCatalogEntry(
        step_name="main (scheduler startup)",
        location="ingestion/scheduler/daily_pipeline.py:2895",
        caught="scheduler.remove_job('backfill_catchup') raising "
        "JobLookupError when the stale persisted job doesn't exist (the "
        "common case after the first cleanup run).",
        impact="None if the job doesn't exist (expected steady state). "
        "If remove_job fails for a different reason (job store "
        "corruption), the stale backfill_catchup job keeps firing "
        "silently forever, since this is a bare except with no logging "
        "of the actual exception.",
        remediation="Not currently actionable — the exception's own "
        "message is discarded. See CATALOG's `known_gap` note below: this "
        "one should log the exception type/message even when it's "
        "expected-and-ignored, so a persistent job-store issue isn't "
        "invisible.",
        severity="info",
    ),
]

# Known gap surfaced while building this catalog (2026-07-10 remediation
# session): the `main (scheduler startup)` entry above catches too
# broadly (bare `except Exception: pass`) to distinguish "job didn't
# exist" (expected) from "job store is broken" (should alert). Logged as
# a FeatureBacklog.md follow-up rather than silently fixed here, since
# narrowing this catch requires confirming APScheduler's exact
# JobLookupError import path is stable across the pinned version.


def entries_for_step(step_name: str) -> list[ExceptionCatalogEntry]:
    """Return all catalog entries for a given pipeline step name."""
    return [entry for entry in CATALOG if entry.step_name == step_name]


def all_entries() -> list[ExceptionCatalogEntry]:
    """Return the full exception catalog."""
    return list(CATALOG)
