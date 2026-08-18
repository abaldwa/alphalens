"""
strategies/signals.py

Owner: Platform / Architecture (A94)
Consumers: backtest adapters and BacktestOrchestrator (source="backtest"),
the technical alert checker and daily inference (source="live"), paper
trading (source="paper"), the report API.

Write/read API for strategy_signals -- the ledger that records what every
strategy said to do, on what date, for which ticker.

Why this exists: today a live paper trade cannot be traced back to the signal
that produced it. ml_signals is ML-only, ta_signals is alerts-only, Momentum
and Fundamental persist nothing at all, and technical_screener_cache is a
compute cache rather than a record of decisions. The ledger closes that audit
gap and is the substrate A87 needs to generate signals once and simulate many
variants against them.

Two constraints from the schema module, enforced here rather than documented
and hoped for:

* EMITTED signals only. 63 templates x ~800 tickers x ~4,300 sessions is
  order 1e8 rows if every evaluated ticker is written. write_signals() rejects
  a batch containing action="hold" unless explicitly opted in, because "hold"
  for the whole universe is precisely the shape that blows the table up.

* One writer. Everything goes through get_duckdb_connection (which carries
  the project's write-lock retry) or through a caller-supplied connection
  inside an existing transaction. Nothing here opens a concurrent writer.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

from strategies.db import open_connection

logger = logging.getLogger(__name__)

ACTIONS = frozenset({"buy", "sell", "hold", "forced_close"})
SOURCES = frozenset({"backtest", "paper", "live"})

# run_id is part of the primary key, and DuckDB PK columns cannot be NULL.
# Live and paper signals therefore use this sentinel. Storing NULL instead
# would drop them out of the key and let duplicates through unnoticed.
NO_RUN = ""

# Same reasoning as NO_RUN, for strategy_version: the column is NOT NULL and
# part of the primary key, so a run that cannot resolve its registry version
# (pre-registry, or a strategy not registered) needs a value. 0 is used
# because registry versions are append-only from 1, so it can never collide
# with a real one -- and unlike defaulting to 1, it does not claim the run
# executed a definition it may never have seen.
UNVERSIONED = 0


class SignalError(ValueError):
    """A signal batch was rejected."""


def write_signals(
    signals: Sequence[Dict[str, Any]],
    *,
    strategy_key: str,
    strategy_version: Optional[int],
    source: str,
    run_id: str = NO_RUN,
    allow_hold: bool = False,
    db_path: Optional[Path] = None,
    conn: Any = None,
) -> int:
    """Persist a batch of emitted signals. Returns the number of rows written.

    Each signal dict needs at minimum {"signal_date", "ticker", "action"}, and
    may carry conviction, rank, size_multiplier and context (a dict).

    Re-writing the same (strategy, version, date, ticker, source, run) replaces
    the previous row rather than erroring: a resumed backtest job re-emits the
    signals for the day it was interrupted on, and that must be idempotent or
    every resume would fail on a primary-key collision.

    Args:
        allow_hold: permit action="hold" rows. Off by default because holds
            for the full universe are what turn this table from millions of
            rows into hundreds of millions. Pass it only when the holds
            themselves are the thing being recorded.
    """
    if source not in SOURCES:
        raise SignalError(f"unknown source {source!r}; valid: {sorted(SOURCES)}")
    if strategy_version is None:
        strategy_version = UNVERSIONED
    if source == "backtest" and not run_id:
        raise SignalError("backtest signals must carry a run_id")
    if not signals:
        return 0

    rows = []
    now = datetime.now()
    for i, sig in enumerate(signals):
        action = sig.get("action")
        if action not in ACTIONS:
            raise SignalError(
                f"signals[{i}]: unknown action {action!r}; valid: {sorted(ACTIONS)}"
            )
        if action == "hold" and not allow_hold:
            raise SignalError(
                f"signals[{i}]: action='hold' rejected. Persist emitted signals "
                "only -- universe-wide holds are what make this table "
                "unmanageable. Pass allow_hold=True if the holds are the point."
            )
        signal_date = sig.get("signal_date")
        if signal_date is None:
            raise SignalError(f"signals[{i}]: missing signal_date")
        ticker = sig.get("ticker")
        if not ticker:
            raise SignalError(f"signals[{i}]: missing ticker")

        context = sig.get("context")
        rows.append(
            [
                strategy_key,
                strategy_version,
                _as_date(signal_date),
                ticker,
                action,
                sig.get("conviction"),
                sig.get("rank"),
                sig.get("size_multiplier"),
                json.dumps(context) if context is not None else None,
                source,
                run_id,
                now,
            ]
        )

    with _conn(db_path, conn) as c:
        # DuckDB has no ON CONFLICT for this shape across all versions in use,
        # and a resumed job legitimately re-emits a day's signals, so delete
        # the affected (date, ticker) keys first within the same transaction.
        c.executemany(
            "DELETE FROM strategy_signals WHERE strategy_key = ? AND "
            "strategy_version = ? AND signal_date = ? AND ticker = ? AND "
            "source = ? AND run_id = ?",
            [[r[0], r[1], r[2], r[3], r[9], r[10]] for r in rows],
        )
        c.executemany(
            "INSERT INTO strategy_signals (strategy_key, strategy_version, "
            "signal_date, ticker, action, conviction, rank, size_multiplier, "
            "context_json, source, run_id, generated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
    logger.debug(
        "Wrote %d signals for %s v%d (%s)", len(rows), strategy_key, strategy_version, source
    )
    return len(rows)


def read_signals(
    *,
    strategy_key: str,
    strategy_version: Optional[int] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    source: Optional[str] = None,
    run_id: Optional[str] = None,
    actions: Optional[Iterable[str]] = None,
    db_path: Optional[Path] = None,
    conn: Any = None,
) -> List[Dict[str, Any]]:
    """Read signals back, ordered by date then ticker.

    This is the read path A87 uses to simulate many variants against one
    generated signal set instead of regenerating signals per variant.
    """
    sql = ["SELECT * FROM strategy_signals WHERE strategy_key = ?"]
    params: List[Any] = [strategy_key]
    if strategy_version is not None:
        sql.append("AND strategy_version = ?")
        params.append(strategy_version)
    if start_date is not None:
        sql.append("AND signal_date >= ?")
        params.append(start_date)
    if end_date is not None:
        sql.append("AND signal_date <= ?")
        params.append(end_date)
    if source is not None:
        sql.append("AND source = ?")
        params.append(source)
    if run_id is not None:
        sql.append("AND run_id = ?")
        params.append(run_id)
    action_list = list(actions) if actions else []
    if action_list:
        sql.append(f"AND action IN ({','.join('?' * len(action_list))})")
        params.extend(action_list)
    sql.append("ORDER BY signal_date, ticker")

    with _conn(db_path, conn) as c:
        df = c.execute(" ".join(sql), params).fetchdf()

    out = []
    for row in df.to_dict("records"):
        row["context"] = (
            json.loads(row.pop("context_json"))
            if row.get("context_json") not in (None, "")
            else None
        )
        # fetchdf hands back a pandas Timestamp for a DATE column. Callers
        # compare these against datetime.date (a backtest's current session,
        # a report's window bounds), and Timestamp != date, so normalise here
        # rather than leaving every caller to remember.
        row["signal_date"] = _as_date(row["signal_date"])
        out.append(row)
    return out


def delete_run_signals(
    run_id: str, *, db_path: Optional[Path] = None, conn: Any = None
) -> int:
    """Remove every signal written by one backtest run. Used when a run is
    discarded, so the ledger does not accumulate rows attributed to results
    that no longer exist. Live and paper signals are never deleted -- they are
    the audit record.
    """
    if not run_id or run_id == NO_RUN:
        raise SignalError("delete_run_signals requires a real run_id")
    with _conn(db_path, conn) as c:
        before = c.execute(
            "SELECT count(*) FROM strategy_signals WHERE run_id = ?", [run_id]
        ).fetchone()[0]
        c.execute("DELETE FROM strategy_signals WHERE run_id = ?", [run_id])
    return int(before)


@dataclass
class SupersedeReport:
    """What a supersede pass found and did.

    `identical` is the answer to the question the contract exists to ask: did
    regenerating this strategy over this window reproduce the same decisions?
    None means there was no prior set to compare against.
    """

    prior_runs: List[str]
    deleted_rows: int
    kept_rows: int
    identical: Optional[bool]
    added: int = 0
    removed: int = 0
    detail_changed: int = 0
    examples: List[str] = field(default_factory=list)

    def summary(self) -> str:
        if not self.prior_runs:
            return f"no prior backtest signals in window; kept {self.kept_rows} rows"
        if self.identical:
            return (
                f"regenerated identically to {len(self.prior_runs)} prior run(s); "
                f"superseded {self.deleted_rows} duplicate rows, kept {self.kept_rows}"
            )
        return (
            f"DIFFERS from prior run(s) {self.prior_runs}: +{self.added} new, "
            f"-{self.removed} gone, {self.detail_changed} changed in rank/size/conviction; "
            f"superseded {self.deleted_rows} rows, kept {self.kept_rows}"
        )


def supersede_backtest_signals(
    *,
    strategy_key: str,
    run_id: str,
    start_date: Any,
    end_date: Any,
    db_path: Optional[Path] = None,
    conn: Any = None,
) -> SupersedeReport:
    """Keep exactly ONE set of backtest signals per strategy per date window.

    Regenerating a backtest over a period it has already covered previously
    left both sets in the table: run_id is part of the primary key, so a
    second run of the same strategy over the same dates collided with
    nothing and simply doubled the rows. Every consumer that reads the
    ledger by (strategy, date) then had to pick a run, and "the signals for
    this strategy on this date" stopped having one answer.

    This makes the newest run authoritative for the window it covered: prior
    runs' rows in [start_date, end_date] are compared against the new set,
    reported on, and then deleted.

    SCOPE, DELIBERATELY NARROW -- backtest source only.
    -------------------------------------------------
    Live and paper rows are never touched. They record what was actually
    acted on, on a day that has already happened; superseding them would be
    rewriting history rather than deduplicating a recomputation. That
    asymmetry is the whole reason this is not simply a narrower primary key.

    ACROSS VERSIONS ON PURPOSE
    --------------------------
    The window is keyed on strategy_key, not (strategy_key, version). If the
    definition was revised between runs, the older version's backtest rows
    for these dates are stale output of a definition that is no longer in
    force -- keeping them is the duplication this closes. The comparison
    still reports what changed, so a version bump that silently altered the
    selection is visible rather than absorbed.
    """
    if not run_id or run_id == NO_RUN:
        raise SignalError("supersede_backtest_signals requires a real run_id")
    start, end = _as_date(start_date), _as_date(end_date)
    if start > end:
        raise SignalError(f"start_date {start} is after end_date {end}")

    with _conn(db_path, conn) as c:
        window = (
            "FROM strategy_signals WHERE strategy_key = ? AND source = 'backtest' "
            "AND signal_date >= ? AND signal_date <= ?"
        )
        base = [strategy_key, start, end]

        prior_runs = [
            r[0]
            for r in c.execute(
                f"SELECT DISTINCT run_id {window} AND run_id <> ? ORDER BY 1", base + [run_id]
            ).fetchall()
        ]
        current = c.execute(
            f"SELECT signal_date, ticker, action, rank, size_multiplier, conviction "
            f"{window} AND run_id = ?",
            base + [run_id],
        ).fetchall()
        kept = len(current)

        if not prior_runs:
            return SupersedeReport(
                prior_runs=[], deleted_rows=0, kept_rows=kept, identical=None
            )

        prior = c.execute(
            f"SELECT signal_date, ticker, action, rank, size_multiplier, conviction "
            f"{window} AND run_id <> ?",
            base + [run_id],
        ).fetchall()

        report = _compare_signal_sets(prior, current)
        report.prior_runs = prior_runs
        report.kept_rows = kept

        c.execute(f"DELETE {window} AND run_id <> ?", base + [run_id])
        report.deleted_rows = len(prior)

    if report.identical:
        logger.info("supersede %s: %s", strategy_key, report.summary())
    else:
        # Not a warning about a failure -- a regeneration that changes the
        # decisions is legitimate (revised definition, corrected data). It is
        # logged loudly because it is the one outcome nobody should discover
        # later from a changed chart.
        logger.warning("supersede %s: %s", strategy_key, report.summary())
    return report


def _compare_signal_sets(prior: Sequence[Any], current: Sequence[Any]) -> SupersedeReport:
    """Decision-level diff of two signal sets.

    Identity is (date, ticker, action) -- what the strategy decided. rank,
    size_multiplier and conviction are compared separately as `detail_changed`
    rather than folded into identity, because a run that picks the same
    stocks on the same days but sizes them differently is a different
    finding from one that picks different stocks, and collapsing the two
    would report both as "changed" with no way to tell which happened.
    """

    def decisions(rows: Sequence[Any]) -> Dict[Any, Any]:
        return {(r[0], r[1], r[2]): (r[3], r[4], r[5]) for r in rows}

    old, new = decisions(prior), decisions(current)
    added = sorted(set(new) - set(old))
    removed = sorted(set(old) - set(new))
    changed = [k for k in set(old) & set(new) if old[k] != new[k]]

    examples: List[str] = []
    for label, keys in (("+", added), ("-", removed), ("~", changed)):
        for k in keys[:3]:
            examples.append(f"{label}{k[0]} {k[1]} {k[2]}")

    return SupersedeReport(
        prior_runs=[],
        deleted_rows=0,
        kept_rows=len(current),
        identical=not (added or removed or changed),
        added=len(added),
        removed=len(removed),
        detail_changed=len(changed),
        examples=examples[:9],
    )


def signal_counts(
    *, strategy_key: Optional[str] = None, db_path: Optional[Path] = None, conn: Any = None
) -> List[Dict[str, Any]]:
    """Row counts grouped by strategy/source/action. The cheap way to notice
    the table growing the way A94 warns it can."""
    sql = (
        "SELECT strategy_key, source, action, count(*) AS n, "
        "min(signal_date) AS first_date, max(signal_date) AS last_date "
        "FROM strategy_signals"
    )
    params: List[Any] = []
    if strategy_key:
        sql += " WHERE strategy_key = ?"
        params.append(strategy_key)
    sql += " GROUP BY strategy_key, source, action ORDER BY n DESC"
    with _conn(db_path, conn) as c:
        return c.execute(sql, params).fetchdf().to_dict("records")


def _as_date(v: Any) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    if isinstance(v, str):
        return date.fromisoformat(v[:10])
    raise SignalError(f"cannot interpret {v!r} as a date")


class _ConnCtx:
    """Same borrow-or-open pattern as strategies/registry.py."""

    def __init__(self, db_path: Optional[Path], conn: Any):
        self._conn = conn
        self._owned = conn is None
        self._db_path = db_path

    def __enter__(self):
        if self._conn is not None:
            return self._conn
        # A105: strategies/db.py, not a bare get_duckdb_connection — the
        # ledger write runs inside a backtest job's deferred tail and needs
        # that path's write-lock retry budget, not the API's default.
        self._ctx = open_connection(self._db_path)
        self._conn = self._ctx.__enter__()
        return self._conn

    def __exit__(self, *exc):
        if self._owned:
            return self._ctx.__exit__(*exc)
        return False


def _conn(db_path: Optional[Path], conn: Any) -> _ConnCtx:
    return _ConnCtx(db_path, conn)
