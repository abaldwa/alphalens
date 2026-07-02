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

from config.settings import SIGNALS_DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from datastore.api.utils.feature_store import resolve_date
from systems.technical_analysis.screener.engine import ScreenerEngine, ScreenerResult
from systems.technical_analysis.screener.templates import TEMPLATES

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

_INSERT_SQL = """
INSERT INTO ta_signals (
    date, ticker, template_name, category, score,
    matched_conditions, total_conditions, key_values
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (date, ticker, template_name) DO UPDATE SET
    category = EXCLUDED.category,
    score = EXCLUDED.score,
    matched_conditions = EXCLUDED.matched_conditions,
    total_conditions = EXCLUDED.total_conditions,
    key_values = EXCLUDED.key_values
"""

# Template name → category map (built once at import time for fast lookup)
_TEMPLATE_CATEGORY: Dict[str, str] = {t.name: t.category for t in TEMPLATES}


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

    def _write_results_batch(
        self,
        conn,
        date_str: str,
        results: List[ScreenerResult],
    ) -> None:
        """Upsert a batch of ScreenerResult rows into ta_signals.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            Open DuckDB connection.
        date_str : str
            The feature date (YYYY-MM-DD) for all rows in this batch.
        results : list of ScreenerResult
            Full-match results from one template run.

        Spec References
        ---------------
        SPEC-TA-008: ta_signals upsert (ON CONFLICT DO UPDATE)
        SPEC-DS-004: "same date+ticker+system replaces, never duplicates"
        """
        if not results:
            return

        rows = [
            (
                date_str,
                r.ticker,
                r.template_name,
                _TEMPLATE_CATEGORY.get(r.template_name, "custom"),
                r.score,
                r.matched_conditions,
                r.total_conditions,
                json.dumps(r.key_values) if r.key_values else None,
            )
            for r in results
            # Only persist full matches as alerts (score == 1.0)
            if r.score >= 1.0 - 1e-9
        ]

        if rows:
            conn.executemany(_INSERT_SQL, rows)

    def run(self, run_date: Optional[str] = None) -> Dict[str, int]:
        """Run all 42 templates against today's feature Parquet and write results.

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
        resolved = resolve_date(run_date)
        if resolved is None:
            logger.warning(
                "DailyAlertChecker.run: no feature Parquet available for date '%s'",
                run_date,
            )
            return {}

        logger.info("DailyAlertChecker: evaluating 42 templates for date %s", resolved)

        # Ensure the signals directory exists before opening DuckDB
        SIGNALS_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)

        template_results: Dict[str, List[ScreenerResult]] = {}
        total_matches = 0

        for template in TEMPLATES:
            try:
                # screen() already limits to 10k results; for alert storage we
                # want ALL full matches — use a very large limit (universe ≤ 5 000)
                matches = self._engine.screen(
                    template.name, date=resolved, limit=10_000
                )
                # Keep only full matches for the alerts table
                full_matches = [r for r in matches if r.score >= 1.0 - 1e-9]
                template_results[template.name] = full_matches
                total_matches += len(full_matches)
            except Exception as exc:
                logger.error(
                    "DailyAlertChecker: template %s failed: %s", template.name, exc
                )
                template_results[template.name] = []

        logger.info(
            "DailyAlertChecker: %d total full matches across 42 templates for %s",
            total_matches,
            resolved,
        )

        # Write all results to the signals DuckDB in a single connection
        # SPEC-SCHED-013: persist=False releases the lock immediately on exit
        with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False) as conn:
            self._ensure_db_and_table(conn)
            for template_name, results in template_results.items():
                self._write_results_batch(conn, resolved, results)

        return {name: len(res) for name, res in template_results.items()}
