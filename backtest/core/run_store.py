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


def save_run_result(conn, result: BacktestRunResult) -> None:
    """Upsert one BacktestRunResult into backtest_runs. Idempotent on
    run_id — a rerun of the same run_id (e.g. a resumed background job)
    overwrites its own prior row rather than erroring or duplicating."""
    run = result.run
    conn.execute(
        """
        INSERT INTO backtest_runs
            (run_id, parent_run_id, channel, strategy_id, horizon_bucket, mode, universe_spec,
             start_date, end_date, capital_mode, initial_capital, sip_amount, sip_cadence_days,
             random_seed, config_hash, config_json, created_at,
             metrics_json, data_gaps_json, integrity_passed, integrity_detail_json, regime_breakdown_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (run_id) DO UPDATE SET
            metrics_json = excluded.metrics_json,
            data_gaps_json = excluded.data_gaps_json,
            integrity_passed = excluded.integrity_passed,
            integrity_detail_json = excluded.integrity_detail_json,
            regime_breakdown_json = excluded.regime_breakdown_json
        """,
        [
            run.run_id, run.parent_run_id, run.channel, run.strategy_id, run.horizon_bucket.value,
            run.mode, run.universe_spec, run.start_date, run.end_date, run.capital_mode,
            run.initial_capital, run.sip_amount, run.sip_cadence_days, run.random_seed,
            run.config_hash, json.dumps(run.config, default=str), run.created_at,
            json.dumps(result.metrics, default=str), json.dumps(result.data_gaps, default=str),
            result.integrity_passed, json.dumps(result.integrity_detail, default=str),
            json.dumps(result.regime_breakdown, default=str),
        ],
    )
    logger.info(f"Saved backtest run {run.run_id} ({run.channel}/{run.strategy_id}/{run.horizon_bucket.value})")


_COLUMNS = (
    "run_id", "parent_run_id", "channel", "strategy_id", "horizon_bucket", "mode", "universe_spec",
    "start_date", "end_date", "capital_mode", "initial_capital", "sip_amount", "sip_cadence_days",
    "random_seed", "config_hash", "config_json", "created_at", "metrics_json", "data_gaps_json",
    "integrity_passed", "integrity_detail_json", "live_eligible", "regime_breakdown_json",
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


def list_runs(
    conn, channel: Optional[str] = None, mode: Optional[str] = None,
    strategy_id: Optional[str] = None, limit: int = 100,
) -> List[Dict[str, Any]]:
    """List runs, most recent first, optionally filtered by channel/mode/strategy_id."""
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
