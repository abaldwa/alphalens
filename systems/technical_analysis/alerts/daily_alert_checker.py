"""
systems/technical_analysis/alerts/daily_alert_checker.py

Phase: 3.x (Technical Analysis Alerts)
Specs: SPEC-TA-006
Owner: Technical Analysis / Alerts
Consumers: ingestion/scheduler/daily_pipeline.py (called after compute_features
           completes), datastore/api/routers/technical.py (alerts/* endpoints)

Evaluates all 42 screener templates against the current universe daily and
persists matches to the `ta_signals` table in the signals DuckDB
(config.settings.SIGNALS_DUCKDB_PATH).

Called by the daily pipeline after features are written to the Parquet store.
Writes only full matches (score = 1.0) to the signals table — partial matches
are available via the screener API but are NOT stored as long-lived alerts.

SPEC-SCHED-013: uses `persist=False` with `get_duckdb_connection` so the
signals DuckDB file lock is released immediately after each write batch,
allowing the scheduler's subsequent steps to open the file without conflict.
"""

import json
import logging
from typing import Dict, List, Optional

import pandas as pd

from config.settings import SIGNALS_DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from datastore.api.utils.feature_store import resolve_date
from systems.technical_analysis.screener.engine import ScreenerEngine, ScreenerResult
from systems.technical_analysis.screener.registry_templates import list_templates

logger = logging.getLogger(__name__)

# DDL for the ta_signals table (SPEC-TA-006 / SPEC-TA-008)
_CREATE_TA_SIGNALS_SQL = """
CREATE TABLE IF NOT EXISTS ta_signals (
    date DATE NOT NULL,
    ticker VARCHAR NOT NULL,
    template_name VARCHAR NOT NULL,
    category VARCHAR NOT NULL,
    score FLOAT NOT NULL,
    matched_conditions INTEGER NOT NULL,
    total_conditions INTEGER NOT NULL,
    key_values JSON,
    PRIMARY KEY (date, ticker, template_name)
)
"""

# Bulk upsert via a registered DataFrame + INSERT...SELECT...ON CONFLICT,
# NOT conn.executemany() with one parameterized statement per row — measured
# ~7s for ~5,000 rows via executemany against a table this size (row-by-row
# prepared-statement overhead) vs ~0.03s for the equivalent single bulk
# statement below (a ~250x difference), found while building
# scripts/backfill_ta_signals.py's historical replay. Same upsert semantics,
# just executed as one vectorized statement instead of N individual ones.
_BULK_UPSERT_SQL = """
INSERT INTO ta_signals (date, ticker, template_name, category, score,
    matched_conditions, total_conditions, key_values)
SELECT date, ticker, template_name, category, score,
    matched_conditions, total_conditions, key_values FROM _ta_signals_upsert_batch
ON CONFLICT (date, ticker, template_name) DO UPDATE SET
    category = EXCLUDED.category,
    score = EXCLUDED.score,
    matched_conditions = EXCLUDED.matched_conditions,
    total_conditions = EXCLUDED.total_conditions,
    key_values = EXCLUDED.key_values
"""

# Template name → category map is built at runtime from the registry to avoid
# importing DB-backed rows at module import time (long-lived process safety).



class DailyAlertChecker:
    """Runs all 42 templates daily and writes full matches to ta_signals.

    The checker is intentionally stateless — it computes everything from
    the date's feature Parquet and always writes/replaces, so re-running
    on the same date is idempotent.

    Parameters
    ----------
    None — paths are read from config.settings (SPEC-QUALITY-003).

    Spec References
    ---------------
    SPEC-TA-006: Technical Alerts — evaluated daily after pipeline completes
    SPEC-TA-008: ta_signals table schema
    SPEC-SCHED-013: release DuckDB lock after write (persist=False)
    """

    def __init__(self) -> None:
        self._engine = ScreenerEngine()

    def _ensure_db_and_table(self, conn) -> None:
        """Create the ta_signals table if it does not already exist.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            Open DuckDB connection to SIGNALS_DUCKDB_PATH.

        Spec References
        ---------------
        SPEC-TA-008: ta_signals table DDL
        """
        conn.execute(_CREATE_TA_SIGNALS_SQL)

    def _write_all_results(
        self,
        conn,
        date_str: str,
        template_results: Dict[str, List[ScreenerResult]],
        template_category: Dict[str, str],
    ) -> int:
        """Upsert every template's full-match rows for one date in a
        single bulk statement (see _BULK_UPSERT_SQL's docstring note for
        why this replaced a per-row executemany loop). Returns the number
        of rows written.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            Open DuckDB connection.
        date_str : str
            The feature date (YYYY-MM-DD) for all rows in this batch.
        template_results : dict
            {template_name: [ScreenerResult, ...]} across all 42 templates.

        Spec References
        ---------------
        SPEC-TA-008: ta_signals upsert (ON CONFLICT DO UPDATE)
        SPEC-DS-004: "same date+ticker+system replaces, never duplicates"
        """
        rows = [
            (
                date_str,
                r.ticker,
                r.template_name,
                template_category.get(r.template_name, "custom"),
                r.score,
                r.matched_conditions,
                r.total_conditions,
                json.dumps(r.key_values) if r.key_values else None,
            )
            for results in template_results.values()
            for r in results
            # Only persist full matches as alerts (score == 1.0)
            if r.score >= 1.0 - 1e-9
        ]
        if not rows:
            return 0

        batch_df = pd.DataFrame(rows, columns=[
            "date", "ticker", "template_name", "category", "score",
            "matched_conditions", "total_conditions", "key_values",
        ])
        conn.register("_ta_signals_upsert_batch", batch_df)
        try:
            conn.execute(_BULK_UPSERT_SQL)
        finally:
            conn.unregister("_ta_signals_upsert_batch")
        return len(rows)

    def evaluate(self, run_date: Optional[str] = None) -> "tuple[Optional[str], Dict[str, List[ScreenerResult]]]":
        """Run all 42 templates against a date's feature Parquet — compute only, no DB write.

        Split out from run() (SPEC-SCHED-013 follow-up, 2026-07-02) so
        callers running in a different process than the DataStore API
        (i.e. the scheduler) can POST the results through the API's
        /api/v1/ta/signals/write endpoint instead of opening a direct
        DuckDB connection to SIGNALS_DUCKDB_PATH — that file already has
        a long-lived cached connection held by the API process for the
        life of its run, so a second, independent process (the scheduler)
        opening its own connection loses the race for DuckDB's single-
        writer-per-file lock (observed via a live "check_ta_alerts" Ops
        Monitor failure — see BuildLog.md). Routing writes through the
        API instead means only the API process ever opens the file
        directly, matching SPEC-DS-002.

        Parameters
        ----------
        run_date : str, optional
            YYYY-MM-DD. Defaults to the latest available feature Parquet date.

        Returns
        -------
        tuple of (str or None, dict)
            (resolved_date, {template_name: [ScreenerResult, ...]}) — only
            full matches (score == 1.0) are included per template.
            resolved_date is None (and the dict empty) if no feature
            Parquet is available for run_date.

        Spec References
        ----------------
        SPEC-TA-006: daily alert evaluation
        """
        resolved = resolve_date(run_date)
        if resolved is None:
            logger.warning(
                "DailyAlertChecker.evaluate: no feature Parquet available for date '%s'",
                run_date,
            )
            return None, {}

        logger.info("DailyAlertChecker: evaluating templates for date %s", resolved)

        # Load the date's feature Parquet ONCE and reuse it across all
        # templates, rather than calling ScreenerEngine.screen() per
        # template (each of which re-reads the same file from disk).
        df = self._engine._load_df(resolved)  # noqa: SLF001 — same module family, no public API needed for this

        # Fetch the declared templates from the registry at call time so a
        # long-lived process (API/scheduler) picks up edits without restart.
        template_list = list_templates()
        # expose for callers that need the same list (run() uses this)
        self._last_template_list = template_list

        template_results: Dict[str, List[ScreenerResult]] = {}
        total_matches = 0

        for template in template_list:
            try:
                if df is None:
                    full_matches = []
                else:
                    # screen() already limits to 10k results; for alert storage we
                    # want ALL full matches — use a very large limit (universe ≤ 5 000)
                    matches = self._engine._screen_df(df, template, resolved, limit=10_000)  # noqa: SLF001
                    full_matches = [r for r in matches if r.score >= 1.0 - 1e-9]
                template_results[template.name] = full_matches
                total_matches += len(full_matches)
            except Exception as exc:
                logger.error(
                    "DailyAlertChecker: template %s failed: %s", template.name, exc
                )
                template_results[template.name] = []

        logger.info(
            "DailyAlertChecker: %d total full matches across templates for %s",
            total_matches,
            resolved,
        )
        return resolved, template_results

    def run(self, run_date: Optional[str] = None) -> Dict[str, int]:
        """Run all 42 templates against today's feature Parquet and write results.

        In-process convenience wrapper around evaluate() + a direct DuckDB
        write — safe to call from the same process as the DataStore API
        (e.g. tests, one-off scripts), but NOT from the scheduler process;
        see evaluate()'s docstring for why. ingestion/scheduler/
        daily_pipeline.py's step_check_ta_alerts uses evaluate() + the
        /api/v1/ta/signals/write HTTP endpoint instead.

        Parameters
        ----------
        run_date : str, optional
            YYYY-MM-DD. Defaults to the latest available feature Parquet date.
            Useful for backfilling or re-running a specific date.

        Returns
        -------
        dict
            {template_name: match_count} for each of the 42 templates.
            Templates with zero full matches appear with value 0.
            Returns {} if no feature Parquet is available for the date.

        Spec References
        ---------------
        SPEC-TA-006: daily alert evaluation
        SPEC-SCHED-013: persist=False to release the DuckDB file lock quickly
        """
        resolved, template_results = self.evaluate(run_date)
        if resolved is None:
            return {}

        # Ensure the signals directory exists before opening DuckDB
        SIGNALS_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)

        # Write all results to the signals DuckDB in a single connection
        # SPEC-SCHED-013: persist=False releases the lock immediately on exit
        # Build the template category map from the same runtime list so the
        # writer and the evaluator agree on categories without a module-level
        # import-time DB dependency.
        template_category = {t.name: t.category for t in getattr(self, "_last_template_list", list_templates())}

        with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False) as conn:
            self._ensure_db_and_table(conn)
            self._write_all_results(conn, resolved, template_results, template_category)

        return {name: len(res) for name, res in template_results.items()}
