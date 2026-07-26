"""
backtest/core/run_store.py

Phase: Unified Backtest & Paper Trading Umbrella, Phase 3
Owner: Platform / Backtest
Consumers: datastore/api/routers/backtest_runs.py

Persistence layer for BacktestRunResult -> the backtest_runs DuckDB table
(datastore/schema/create_backtest.py, Store 6). Every BacktestOrchestrator/
WalkForwardRunner run (backtest, walk_forward, or eventually paper mode)
is saved here so Phase 3's API/UI can list and compare runs across all
four channels in one place — this is the "unified run records" the
umbrella plan's whole premise depends on.

Per the Phase 6 hard boundary (BacktestUmbrellaPlan.md): live_eligible is
never set by save_run_result() or any function in this module — it's
DEFAULT FALSE at the schema level and only a separate, explicitly
human-invoked function (not built yet — Phase 5/6) may ever flip it.
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from backtest.core.run_context import BacktestRunResult

logger = logging.getLogger(__name__)


def save_run_result(conn, result: BacktestRunResult, queue_id: Optional[str] = None) -> None:
    """Upsert one BacktestRunResult into backtest_runs. Idempotent on
    run_id — a rerun of the same run_id (e.g. a resumed background job)
    overwrites its own prior row rather than erroring or duplicating.

    queue_id: 2026-07-26 (REV6 wiring) — which backtest/run_strategy_queue.py
    sweep (--report-suffix) this run belongs to, if any. None for a
    standalone (non-queue) run. dsr/dsr_n_trials are NOT set here — they're
    written separately, event-driven, by run_strategy_queue.py via
    update_dsr() once this run's own job completes and the queue's
    running trial count for this point is known (see that function).
    """
    run = result.run
    conn.execute(
        """
        INSERT INTO backtest_runs
            (run_id, parent_run_id, channel, strategy_id, horizon_bucket, mode, universe_spec,
             start_date, end_date, capital_mode, initial_capital, sip_amount, sip_cadence_days,
             random_seed, config_hash, config_json, created_at,
             metrics_json, data_gaps_json, integrity_passed, integrity_detail_json, regime_breakdown_json,
             exit_policy_variant, regime_label, trade_log_path, queue_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (run_id) DO UPDATE SET
            metrics_json = excluded.metrics_json,
            data_gaps_json = excluded.data_gaps_json,
            integrity_passed = excluded.integrity_passed,
            integrity_detail_json = excluded.integrity_detail_json,
            regime_breakdown_json = excluded.regime_breakdown_json,
            exit_policy_variant = excluded.exit_policy_variant,
            regime_label = excluded.regime_label,
            trade_log_path = excluded.trade_log_path,
            queue_id = excluded.queue_id
        """,
        [
            run.run_id, run.parent_run_id, run.channel, run.strategy_id, run.horizon_bucket.value,
            run.mode, run.universe_spec, run.start_date, run.end_date, run.capital_mode,
            run.initial_capital, run.sip_amount, run.sip_cadence_days, run.random_seed,
            run.config_hash, json.dumps(run.config, default=str), run.created_at,
            json.dumps(result.metrics, default=str), json.dumps(result.data_gaps, default=str),
            result.integrity_passed, json.dumps(result.integrity_detail, default=str),
            json.dumps(result.regime_breakdown, default=str),
            result.exit_policy_variant, result.regime_label, result.trade_log_path, queue_id,
        ],
    )
    logger.info(f"Saved backtest run {run.run_id} ({run.channel}/{run.strategy_id}/{run.horizon_bucket.value})")


def update_dsr(conn, run_id: str, dsr: Optional[float], n_trials: int, post_hoc: bool = False) -> None:
    """Write a run's deflated Sharpe ratio back after computing it against
    the trial count known at write time (see save_run_result's queue_id
    docstring). post_hoc=True only for the one-off backfill of runs that
    completed before this wiring existed — see backtest/backfill_dsr.py.
    No-op (logs a warning) if run_id doesn't exist rather than raising —
    this is always called after the row itself was already written."""
    exists = conn.execute("SELECT 1 FROM backtest_runs WHERE run_id = ?", [run_id]).fetchone()
    if exists is None:
        logger.warning(f"update_dsr: run_id {run_id!r} not found in backtest_runs, nothing updated")
        return
    conn.execute(
        "UPDATE backtest_runs SET dsr = ?, dsr_n_trials = ?, dsr_computed_post_hoc = ? WHERE run_id = ?",
        [dsr, n_trials, post_hoc, run_id],
    )


_COLUMNS = (
    "run_id", "parent_run_id", "channel", "strategy_id", "horizon_bucket", "mode", "universe_spec",
    "start_date", "end_date", "capital_mode", "initial_capital", "sip_amount", "sip_cadence_days",
    "random_seed", "config_hash", "config_json", "created_at", "metrics_json", "data_gaps_json",
    "integrity_passed", "integrity_detail_json", "live_eligible", "regime_breakdown_json",
    "exit_policy_variant", "regime_label", "trade_log_path",
)


def _row_to_dict(row: tuple) -> Dict[str, Any]:
    d = dict(zip(_COLUMNS, row))
    d["config"] = json.loads(d.pop("config_json"))
    d["metrics"] = json.loads(d.pop("metrics_json")) if d["metrics_json"] else None
    d["data_gaps"] = json.loads(d.pop("data_gaps_json")) if d["data_gaps_json"] else []
    d["integrity_detail"] = json.loads(d.pop("integrity_detail_json")) if d["integrity_detail_json"] else {}
    d["regime_breakdown"] = json.loads(d.pop("regime_breakdown_json")) if d["regime_breakdown_json"] else []
    for date_field in ("start_date", "end_date", "created_at"):
        if isinstance(d[date_field], (datetime,)):
            d[date_field] = d[date_field].isoformat()
        elif d[date_field] is not None:
            d[date_field] = str(d[date_field])
    return d


def get_run(conn, run_id: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(f"SELECT {', '.join(_COLUMNS)} FROM backtest_runs WHERE run_id = ?", [run_id]).fetchone()
    return _row_to_dict(row) if row else None


_SORT_COLUMNS = {
    "created_at": "created_at",
    # cagr lives inside metrics_json (a JSON string column, not a native
    # field) — TRY_CAST + json_extract_string so a run with no metrics yet
    # (metrics_json NULL) or a non-numeric/missing cagr sorts as NULL
    # (NULLS LAST) rather than erroring or floating to the top.
    "cagr": "TRY_CAST(json_extract_string(metrics_json, '$.cagr') AS DOUBLE)",
}


def list_runs(
    conn, channel: Optional[str] = None, mode: Optional[str] = None,
    strategy_id: Optional[str] = None, limit: int = 100, sort_by: str = "created_at",
) -> List[Dict[str, Any]]:
    """List runs, optionally filtered by channel/mode/strategy_id.

    sort_by: "created_at" (default, most recent first) or "cagr" (highest
    CAGR first, NULLS LAST — the Backtest page's "Top N by CAGR" view).
    """
    if sort_by not in _SORT_COLUMNS:
        raise ValueError(f"sort_by must be one of {sorted(_SORT_COLUMNS)}, got {sort_by!r}")
    where = []
    params: List[Any] = []
    if channel is not None:
        where.append("channel = ?")
        params.append(channel)
    if mode is not None:
        where.append("mode = ?")
        params.append(mode)
    if strategy_id is not None:
        where.append("strategy_id = ?")
        params.append(strategy_id)
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""
    order_col = _SORT_COLUMNS[sort_by]
    params.append(limit)
    rows = conn.execute(
        f"SELECT {', '.join(_COLUMNS)} FROM backtest_runs {where_clause} "
        f"ORDER BY {order_col} DESC NULLS LAST LIMIT ?",
        params,
    ).fetchall()
    return [_row_to_dict(r) for r in rows]


def count_runs(
    conn, channel: Optional[str] = None, mode: Optional[str] = None, strategy_id: Optional[str] = None,
) -> int:
    """Total matching row count, ignoring any page/limit — lets a caller
    show "N runs total" even when list_runs()'s own `limit` truncates the
    rows actually returned (e.g. the Backtest page's run counter, which
    otherwise freezes at the default limit=100 once the real count passes
    it)."""
    where = []
    params: List[Any] = []
    if channel is not None:
        where.append("channel = ?")
        params.append(channel)
    if mode is not None:
        where.append("mode = ?")
        params.append(mode)
    if strategy_id is not None:
        where.append("strategy_id = ?")
        params.append(strategy_id)
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""
    return conn.execute(f"SELECT COUNT(*) FROM backtest_runs {where_clause}", params).fetchone()[0]


def list_experiments(
    conn,
    strategy_id: Optional[str] = None,
    channel: Optional[str] = None,
    exit_policy_variant: Optional[str] = None,
    regime_label: Optional[str] = None,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    """List runs for the Experiments comparison page (270-job exit-variant
    x template/preset matrix, experiment_matrix_45x6.json) — most recent
    first, optionally filtered by strategy_id/channel/exit_policy_variant/
    regime_label. Unlike list_runs(), this doesn't filter on `mode` (every
    exit-variant matrix job is a plain 'backtest' mode run today, but this
    view shouldn't silently hide walk_forward/paper rows should the matrix
    ever grow to include them)."""
    where = []
    params: List[Any] = []
    if strategy_id is not None:
        where.append("strategy_id = ?")
        params.append(strategy_id)
    if channel is not None:
        where.append("channel = ?")
        params.append(channel)
    if exit_policy_variant is not None:
        where.append("exit_policy_variant = ?")
        params.append(exit_policy_variant)
    if regime_label is not None:
        where.append("regime_label = ?")
        params.append(regime_label)
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""
    params.append(limit)
    rows = conn.execute(
        f"SELECT {', '.join(_COLUMNS)} FROM backtest_runs {where_clause} ORDER BY created_at DESC LIMIT ?",
        params,
    ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_signal_counts(conn, run_ids: List[str]) -> Dict[str, Dict[str, int]]:
    """Buy/sell signal counts per run_id, from backtest_feature_log's
    decision_taken column ('buy'/'sell' — see core/engine.py's _log_feature
    call sites). One batched query for a page of runs rather than N+1, and
    only counts the two decision kinds the Runs table cares about — every
    other decision_taken value (held/skipped_*) is left out of this
    aggregate on purpose, it's not a full decision breakdown.

    Returns {run_id: {"buy": n, "sell": n}}; a run with no feature_log rows
    (an older run predating feature logging, or a channel/mode that never
    wired a feature_log_writer) is simply absent from the result — callers
    should default missing entries to 0/0, not treat absence as an error.
    """
    if not run_ids:
        return {}
    placeholders = ", ".join("?" * len(run_ids))
    rows = conn.execute(
        f"""
        SELECT run_id, decision_taken, COUNT(*) FROM backtest_feature_log
        WHERE run_id IN ({placeholders}) AND decision_taken IN ('buy', 'sell')
        GROUP BY run_id, decision_taken
        """,
        run_ids,
    ).fetchall()
    counts: Dict[str, Dict[str, int]] = {}
    for run_id, decision, n in rows:
        counts.setdefault(run_id, {"buy": 0, "sell": 0})[decision] = n
    return counts


def get_run_lineage(conn, run_id: str) -> List[Dict[str, Any]]:
    """Walk parent_run_id back to the root, returning [root, ..., run_id] —
    the feedback-loop "compare to parent run" chain (BacktestUmbrellaPlan.md
    Feature-Vector Logging & Feedback Loop section)."""
    chain: List[Dict[str, Any]] = []
    current = get_run(conn, run_id)
    seen = set()
    while current is not None and current["run_id"] not in seen:
        chain.append(current)
        seen.add(current["run_id"])
        parent_id = current.get("parent_run_id")
        current = get_run(conn, parent_id) if parent_id else None
    return list(reversed(chain))
