"""
backtest/backfill_dsr.py

2026-07-26 (REV6 wiring, one-time use): computes deflated Sharpe for
backtest_runs rows that completed BEFORE the live, event-driven DSR
wiring (backtest/run_strategy_queue.py::_compute_and_write_dsr) existed —
specifically the 49 technical-tower runs from queue_1755a11a0802 that
completed earlier today, 2026-07-26.

Per the user's explicit retain-not-discard decision: these runs were
produced by real algorithm code against real data — nothing wrong with
the DATA, only the statistical-validation layer was missing at the time.
Rather than re-running them, this reconciles them against the fix
retroactively, with dsr_computed_post_hoc=True so no reader can mistake
this for the live-gated DSR every run going forward gets.

n_trials basis: the FINAL count of jobs completed in this queue at
backfill time — not an attempt to reconstruct each run's own sequential
position (that ordering wasn't tracked when these ran, and reconstructing
it now would itself be a form of after-the-fact rationalization). This
is the standard post-hoc DSR usage in the literature (Bailey & Lopez de
Prado): "how surprising is this Sharpe given everything that was
eventually tried," evaluated with the full, now-known trial set — a
DIFFERENT and more conservative question than the live wiring's
per-completion n_trials_so_far, which is why the two get different
dsr_computed_post_hoc labels rather than being merged into one number.

Usage: python -m backtest.backfill_dsr --queue-id queue_1755a11a0802
"""

import argparse
import json
import logging

from config.settings import (
    BACKTEST_DUCKDB_PATH,
    DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS,
    DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S,
    DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S,
)
from datastore.api.db import get_duckdb_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def backfill_dsr_for_queue(queue_id: str) -> dict:
    from backtest.core.run_store import update_dsr
    from backtest.core.metrics import TRADING_DAYS_PER_YEAR
    from backtest.overfit_checks import deflated_sharpe_ratio

    with get_duckdb_connection(
        BACKTEST_DUCKDB_PATH, read_only=False, persist=False,
        retry_attempts=DUCKDB_WRITE_LOCK_RETRY_ATTEMPTS,
        retry_base_delay_s=DUCKDB_WRITE_LOCK_RETRY_BASE_DELAY_S,
        retry_max_delay_s=DUCKDB_WRITE_LOCK_RETRY_MAX_DELAY_S,
    ) as conn:
        rows = conn.execute(
            "SELECT run_id, metrics_json, start_date, end_date FROM backtest_runs "
            "WHERE queue_id = ? AND dsr IS NULL",
            [queue_id],
        ).fetchall()
        n_trials = conn.execute(
            "SELECT COUNT(*) FROM backtest_runs WHERE queue_id = ?", [queue_id],
        ).fetchone()[0]

        updated, skipped = 0, 0
        for run_id, metrics_json, start_date, end_date in rows:
            metrics = json.loads(metrics_json) if metrics_json else {}
            sharpe = metrics.get("sharpe")
            if sharpe is None:
                skipped += 1
                continue
            n_obs = max(int((end_date - start_date).days * (252 / 365.25)), 1)
            # [BUG FIX, 4th fundamental-strategies review] `sharpe` here is the
            # ANNUALIZED Sharpe from backtest/core/metrics.py::sharpe_ratio() —
            # deflated_sharpe_ratio expects a per-period (daily) Sharpe. See
            # run_strategy_queue.py::_compute_and_write_dsr for the full
            # rationale; de-annualizing by dividing out sqrt(TRADING_DAYS_
            # PER_YEAR) is exact, not an approximation.
            raw_sharpe = sharpe / (TRADING_DAYS_PER_YEAR ** 0.5)
            dsr = deflated_sharpe_ratio(sharpe=raw_sharpe, n_trials=n_trials, n_obs=n_obs)
            update_dsr(conn, run_id, dsr, n_trials, post_hoc=True)
            updated += 1

    logger.info(f"backfill_dsr: queue_id={queue_id} n_trials={n_trials} updated={updated} skipped_no_sharpe={skipped}")
    return {"queue_id": queue_id, "n_trials": n_trials, "updated": updated, "skipped_no_sharpe": skipped}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue-id", required=True)
    args = parser.parse_args()
    result = backfill_dsr_for_queue(args.queue_id)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
