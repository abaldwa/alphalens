"""
strategies/registry.py

Owner: Platform / Architecture (A92, A93)
Consumers: the four channel migrations (T15, ML41, ML42, F7), backtest
adapters, datastore/api/routers/strategies.py, the technical screener and
alert checker.

Read/write API over strategy_registry and filter_registry.

The whole point of this module is that definitions are APPEND-ONLY and
point-in-time versioned. There is deliberately no update() and no delete():

    register()   writes version 1 of a new strategy
    revise()     writes version N+1 and closes version N's valid_to
    retire()     marks the current version retired without deleting history
    get()        reads the version in force -- optionally AS OF a past date

`get(..., as_of=...)` is what makes a historical backtest reproducible: a run
records the version it executed, and re-reading that version returns the
definition as it stood, not as it stands now. Without it, editing a strategy
silently invalidates every result that used the old rules while leaving the
run record looking untouched -- the same class of defect as the present-day
ADTV snapshot applied across history (A84).

Writes go through get_duckdb_connection, which carries the project's
single-writer lock retry. Nothing here opens a second writer.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from strategies.db import open_connection
from strategies.predicates import PredicateError, validate_predicates

logger = logging.getLogger(__name__)

CHANNELS = frozenset({"momentum", "technical", "fundamental", "ml"})
STRATEGY_STATUSES = frozenset({"draft", "active", "retired"})
FILTER_TYPES = frozenset({"universe", "entry", "exit", "sizing"})


class RegistryError(ValueError):
    """A registry write was rejected. Never swallowed -- a bad definition
    stored is worse than a write that fails, because it backtests as a
    plausible-looking flat result."""


def strategy_key(channel: str, name: str) -> str:
    """The canonical cross-application identity (A89).

    Readable rather than hashed on purpose: this string is a URL parameter in
    the frontend and appears in report JSON, so an operator debugging a report
    must be able to read it. strategy_catalog's sha1 key cannot serve that.
    """
    if channel not in CHANNELS:
        raise RegistryError(f"unknown channel {channel!r}; valid: {sorted(CHANNELS)}")
    if not name or ":" in name:
        raise RegistryError(f"invalid strategy name {name!r} (must be non-empty, no ':')")
    return f"{channel}:{name}"


def parse_strategy_key(key: str) -> tuple[str, str]:
    """Inverse of strategy_key(). Raises on anything that is not one."""
    channel, _, name = key.partition(":")
    if not name or channel not in CHANNELS:
        raise RegistryError(f"malformed strategy_key {key!r}")
    return channel, name


# ---------------------------------------------------------------------------
# strategy_registry
# ---------------------------------------------------------------------------


def register_strategy(
    *,
    channel: str,
    name: str,
    display_label: str,
    definition: Dict[str, Any],
    entry_criterion: Sequence[Dict[str, Any]],
    exit_criterion: Dict[str, Any],
    filter_ids: Sequence[str] = (),
    description: Optional[str] = None,
    category: Optional[str] = None,
    universe_spec: Optional[str] = None,
    benchmark_index_name: Optional[str] = None,
    regime_index_name: Optional[str] = None,
    status: str = "draft",
    valid_from: Optional[date] = None,
    source_ref: Optional[str] = None,
    created_by: Optional[str] = None,
    db_path: Optional[Path] = None,
    conn: Any = None,
) -> str:
    """Write version 1 of a new strategy. Returns its strategy_key.

    Raises RegistryError if the key already exists -- use revise() for that.
    """
    key = strategy_key(channel, name)
    _validate_strategy_fields(
        display_label=display_label,
        entry_criterion=entry_criterion,
        exit_criterion=exit_criterion,
        status=status,
    )

    with _conn(db_path, conn) as c:
        existing = c.execute(
            "SELECT max(version) FROM strategy_registry WHERE strategy_key = ?", [key]
        ).fetchone()
        if existing and existing[0] is not None:
            raise RegistryError(
                f"{key} already exists at version {existing[0]}; use revise()"
            )
        _insert_strategy_version(
            c,
            key=key,
            version=1,
            channel=channel,
            name=name,
            display_label=display_label,
            description=description,
            category=category,
            definition=definition,
            entry_criterion=entry_criterion,
            exit_criterion=exit_criterion,
            filter_ids=filter_ids,
            universe_spec=universe_spec,
            benchmark_index_name=benchmark_index_name,
            regime_index_name=regime_index_name,
            status=status,
            valid_from=valid_from or date.today(),
            source_ref=source_ref,
            created_by=created_by,
        )
    logger.info("Registered %s v1 (%s)", key, status)
    return key


def revise_strategy(
    key: str,
    *,
    valid_from: Optional[date] = None,
    source_ref: Optional[str] = None,
    created_by: Optional[str] = None,
    db_path: Optional[Path] = None,
    conn: Any = None,
    **changes: Any,
) -> int:
    """Write the next version of an existing strategy, carrying forward every
    field not named in `changes`. Closes the previous version's valid_to.

    Returns the new version number. The previous version's row is left
    otherwise untouched, so any run that recorded it still resolves.
    """
    with _conn(db_path, conn) as c:
        current = _fetch_current(c, key)
        if current is None:
            raise RegistryError(f"{key} is not registered; use register_strategy()")

        new_version = int(current["version"]) + 1
        effective = valid_from or date.today()

        merged = {
            "display_label": current["display_label"],
            "description": current["description"],
            "category": current["category"],
            "definition": json.loads(current["definition_json"]),
            "entry_criterion": json.loads(current["entry_criterion_json"]),
            "exit_criterion": json.loads(current["exit_criterion_json"]),
            "filter_ids": _as_list(current["filter_ids"]),
            "universe_spec": current["universe_spec"],
            "benchmark_index_name": current["benchmark_index_name"],
            "regime_index_name": current["regime_index_name"],
            "status": current["status"],
        }
        unknown = set(changes) - set(merged)
        if unknown:
            raise RegistryError(f"unknown field(s) for revise: {sorted(unknown)}")
        merged.update(changes)

        _validate_strategy_fields(
            display_label=merged["display_label"],
            entry_criterion=merged["entry_criterion"],
            exit_criterion=merged["exit_criterion"],
            status=merged["status"],
        )

        # Close the outgoing version first. valid_to is the last date it was
        # in force, so a run dated before `effective` still resolves to it.
        c.execute(
            "UPDATE strategy_registry SET valid_to = ? "
            "WHERE strategy_key = ? AND version = ?",
            [effective, key, current["version"]],
        )
        _insert_strategy_version(
            c,
            key=key,
            version=new_version,
            channel=current["channel"],
            name=current["name"],
            valid_from=effective,
            source_ref=source_ref,
            created_by=created_by,
            **merged,
        )
    logger.info("Revised %s -> v%d", key, new_version)
    return new_version


def retire_strategy(
    key: str, *, db_path: Optional[Path] = None, conn: Any = None
) -> int:
    """Mark the current version retired. History is preserved."""
    return revise_strategy(key, status="retired", db_path=db_path, conn=conn)


def get_strategy(
    key: str,
    *,
    version: Optional[int] = None,
    as_of: Optional[date] = None,
    db_path: Optional[Path] = None,
    conn: Any = None,
) -> Optional[Dict[str, Any]]:
    """Read one strategy.

    version=N   that exact version (what a run recorded)
    as_of=D     the version in force on date D
    neither     the current version

    Returns a dict with definition/entry/exit already JSON-decoded, or None.
    """
    with _conn(db_path, conn) as c:
        if version is not None:
            row = c.execute(
                "SELECT * FROM strategy_registry WHERE strategy_key = ? AND version = ?",
                [key, version],
            ).fetchdf()
        elif as_of is not None:
            row = c.execute(
                "SELECT * FROM strategy_registry WHERE strategy_key = ? "
                "AND valid_from <= ? AND (valid_to IS NULL OR valid_to > ?) "
                "ORDER BY version DESC LIMIT 1",
                [key, as_of, as_of],
            ).fetchdf()
        else:
            row = c.execute(
                "SELECT * FROM strategy_registry WHERE strategy_key = ? "
                "AND valid_to IS NULL ORDER BY version DESC LIMIT 1",
                [key],
            ).fetchdf()
    if row.empty:
        return None
    return _decode_strategy(row.iloc[0].to_dict())


def list_strategies(
    *,
    channel: Optional[str] = None,
    status: Optional[str] = "active",
    db_path: Optional[Path] = None,
    conn: Any = None,
) -> List[Dict[str, Any]]:
    """Current versions only, optionally filtered by channel and status.

    status=None returns every current version regardless of status.
    """
    sql = "SELECT * FROM strategy_registry WHERE valid_to IS NULL"
    params: List[Any] = []
    if channel:
        sql += " AND channel = ?"
        params.append(channel)
    if status:
        sql += " AND status = ?"
        params.append(status)
    sql += " ORDER BY channel, name"

    with _conn(db_path, conn) as c:
        df = c.execute(sql, params).fetchdf()
    return [_decode_strategy(r) for r in df.to_dict("records")]


# ---------------------------------------------------------------------------
# filter_registry
# ---------------------------------------------------------------------------


def register_filter(
    *,
    filter_id: str,
    name: str,
    filter_type: str,
    params_schema: Dict[str, Any],
    default_params: Dict[str, Any],
    applies_to_channels: Sequence[str],
    implementation_ref: str,
    description: Optional[str] = None,
    status: str = "active",
    valid_from: Optional[date] = None,
    source_ref: Optional[str] = None,
    created_by: Optional[str] = None,
    db_path: Optional[Path] = None,
    conn: Any = None,
) -> str:
    """Write version 1 of a filter. Returns filter_id."""
    if filter_type not in FILTER_TYPES:
        raise RegistryError(
            f"unknown filter_type {filter_type!r}; valid: {sorted(FILTER_TYPES)}"
        )
    bad_channels = set(applies_to_channels) - CHANNELS
    if bad_channels:
        raise RegistryError(f"unknown channel(s): {sorted(bad_channels)}")
    if not implementation_ref or "." not in implementation_ref:
        raise RegistryError(
            f"implementation_ref must be a dotted path, got {implementation_ref!r}"
        )
    # Invariant 2: one implementation per filter. Defaults that name a
    # parameter the schema does not declare mean the two have drifted apart,
    # which is how three copies of an ADTV floor happened in the first place.
    undeclared = set(default_params) - set(params_schema)
    if undeclared:
        raise RegistryError(
            f"default_params names undeclared param(s): {sorted(undeclared)}"
        )

    with _conn(db_path, conn) as c:
        existing = c.execute(
            "SELECT max(version) FROM filter_registry WHERE filter_id = ?", [filter_id]
        ).fetchone()
        if existing and existing[0] is not None:
            raise RegistryError(f"filter {filter_id} already exists")
        c.execute(
            "INSERT INTO filter_registry (filter_id, version, name, description, "
            "filter_type, params_schema_json, default_params_json, "
            "applies_to_channels, implementation_ref, status, valid_from, valid_to, "
            "source_ref, created_at, created_by) "
            "VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)",
            [
                filter_id,
                name,
                description,
                filter_type,
                json.dumps(params_schema),
                json.dumps(default_params),
                list(applies_to_channels),
                implementation_ref,
                status,
                valid_from or date.today(),
                source_ref,
                datetime.now(),
                created_by,
            ],
        )
    logger.info("Registered filter %s v1", filter_id)
    return filter_id


def get_filter(
    filter_id: str, *, db_path: Optional[Path] = None, conn: Any = None
) -> Optional[Dict[str, Any]]:
    """Read the current version of one filter."""
    with _conn(db_path, conn) as c:
        df = c.execute(
            "SELECT * FROM filter_registry WHERE filter_id = ? AND valid_to IS NULL "
            "ORDER BY version DESC LIMIT 1",
            [filter_id],
        ).fetchdf()
    if df.empty:
        return None
    return _decode_filter(df.iloc[0].to_dict())


def list_filters(
    *,
    channel: Optional[str] = None,
    db_path: Optional[Path] = None,
    conn: Any = None,
) -> List[Dict[str, Any]]:
    """Current versions of every filter, optionally those applying to one
    channel."""
    with _conn(db_path, conn) as c:
        df = c.execute(
            "SELECT * FROM filter_registry WHERE valid_to IS NULL ORDER BY filter_id"
        ).fetchdf()
    rows = [_decode_filter(r) for r in df.to_dict("records")]
    if channel:
        rows = [r for r in rows if channel in r["applies_to_channels"]]
    return rows


def resolve_filters(
    filter_ids: Sequence[str],
    overrides: Optional[Dict[str, Dict[str, Any]]] = None,
    *,
    db_path: Optional[Path] = None,
    conn: Any = None,
) -> List[Dict[str, Any]]:
    """Resolve a strategy's filter_ids into concrete {filter_id, params,
    implementation_ref} entries, applying per-strategy overrides on top of
    each filter's declared defaults.

    Raises if a filter is unknown or an override names an undeclared param --
    a typo'd filter param that silently does nothing is precisely how
    circuit_band_pct ended up None in all 195 unconstrained Technical runs.
    """
    overrides = overrides or {}
    unknown_overrides = set(overrides) - set(filter_ids)
    if unknown_overrides:
        raise RegistryError(
            f"override(s) for filter(s) not used by this strategy: "
            f"{sorted(unknown_overrides)}"
        )

    resolved: List[Dict[str, Any]] = []
    for fid in filter_ids:
        f = get_filter(fid, db_path=db_path, conn=conn)
        if f is None:
            raise RegistryError(f"unknown filter_id {fid!r}")
        params = dict(f["default_params"])
        for pname, pvalue in (overrides.get(fid) or {}).items():
            if pname not in f["params_schema"]:
                raise RegistryError(
                    f"filter {fid!r} has no parameter {pname!r}; "
                    f"declared: {sorted(f['params_schema'])}"
                )
            params[pname] = pvalue
        resolved.append(
            {
                "filter_id": fid,
                "filter_type": f["filter_type"],
                "params": params,
                "implementation_ref": f["implementation_ref"],
            }
        )
    return resolved


# ---------------------------------------------------------------------------
# internals
# ---------------------------------------------------------------------------


class _ConnCtx:
    """Use a caller-supplied connection if given, else open one. Only the
    connection we opened gets closed -- a caller batching many registrations
    inside one transaction keeps control of its own."""

    def __init__(self, db_path: Optional[Path], conn: Any):
        self._conn = conn
        self._owned = conn is None
        self._db_path = db_path

    def __enter__(self):
        if self._conn is not None:
            return self._conn
        # A105: strategies/db.py, not a bare get_duckdb_connection. This is
        # read by resolve_strategy_version at the START of every backtest
        # job; on the API's short default budget it loses the race against
        # other workers' write tails and the run silently records
        # strategy_version=UNVERSIONED instead of the version it executed.
        self._ctx = open_connection(self._db_path)
        self._conn = self._ctx.__enter__()
        return self._conn

    def __exit__(self, *exc):
        if self._owned:
            return self._ctx.__exit__(*exc)
        return False


def _conn(db_path: Optional[Path], conn: Any) -> _ConnCtx:
    return _ConnCtx(db_path, conn)


def _validate_strategy_fields(
    *,
    display_label: str,
    entry_criterion: Sequence[Dict[str, Any]],
    exit_criterion: Dict[str, Any],
    status: str,
) -> None:
    if not display_label:
        raise RegistryError("display_label is required (it is the ONE label every screen shows)")
    if status not in STRATEGY_STATUSES:
        raise RegistryError(
            f"unknown status {status!r}; valid: {sorted(STRATEGY_STATUSES)}"
        )
    try:
        validate_predicates(list(entry_criterion), where="entry_criterion")
    except PredicateError as exc:
        raise RegistryError(str(exc)) from exc
    if not isinstance(exit_criterion, dict):
        raise RegistryError("exit_criterion must be a dict")
    if not exit_criterion.get("variant"):
        raise RegistryError("exit_criterion needs a 'variant'")
    try:
        validate_predicates(
            list(exit_criterion.get("conditions") or []), where="exit_criterion.conditions"
        )
    except PredicateError as exc:
        raise RegistryError(str(exc)) from exc


def _insert_strategy_version(
    c: Any,
    *,
    key: str,
    version: int,
    channel: str,
    name: str,
    display_label: str,
    description: Optional[str],
    category: Optional[str],
    definition: Dict[str, Any],
    entry_criterion: Sequence[Dict[str, Any]],
    exit_criterion: Dict[str, Any],
    filter_ids: Sequence[str],
    universe_spec: Optional[str],
    benchmark_index_name: Optional[str],
    regime_index_name: Optional[str],
    status: str,
    valid_from: date,
    source_ref: Optional[str],
    created_by: Optional[str],
) -> None:
    c.execute(
        "INSERT INTO strategy_registry (strategy_key, version, channel, name, "
        "display_label, description, category, definition_json, "
        "entry_criterion_json, exit_criterion_json, filter_ids, universe_spec, "
        "benchmark_index_name, regime_index_name, status, valid_from, valid_to, "
        "source_ref, created_at, created_by) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)",
        [
            key,
            version,
            channel,
            name,
            display_label,
            description,
            category,
            json.dumps(definition),
            json.dumps(list(entry_criterion)),
            json.dumps(exit_criterion),
            list(filter_ids),
            universe_spec,
            benchmark_index_name,
            regime_index_name,
            status,
            valid_from,
            source_ref,
            datetime.now(),
            created_by,
        ],
    )


def _fetch_current(c: Any, key: str) -> Optional[Dict[str, Any]]:
    df = c.execute(
        "SELECT * FROM strategy_registry WHERE strategy_key = ? AND valid_to IS NULL "
        "ORDER BY version DESC LIMIT 1",
        [key],
    ).fetchdf()
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def _nulls(row: Dict[str, Any]) -> Dict[str, Any]:
    """fetchdf() renders SQL NULL as pandas NaN/NaT, which is not None and
    which every downstream consumer then has to defend against -- a NaN in an
    Optional[date] field reaches Pydantic as a float and raises. Normalising
    here means callers see None for "no value", which is what the schema says
    they get.

    valid_to is the field that matters: NULL there means "this is the current
    version", so a NaN leaking through turns the live definition into an
    unparseable row."""
    out = dict(row)
    for k, v in out.items():
        if v is None:
            continue
        # NaT and NaN are both != themselves; anything else is a real value.
        if isinstance(v, float) and v != v:
            out[k] = None
        elif type(v).__name__ == "NaTType":
            out[k] = None
    return out


def _decode_strategy(row: Dict[str, Any]) -> Dict[str, Any]:
    out = _nulls(row)
    out["definition"] = json.loads(out.pop("definition_json"))
    out["entry_criterion"] = json.loads(out.pop("entry_criterion_json"))
    out["exit_criterion"] = json.loads(out.pop("exit_criterion_json"))
    out["filter_ids"] = _as_list(out.get("filter_ids"))
    return out


def _decode_filter(row: Dict[str, Any]) -> Dict[str, Any]:
    out = _nulls(row)
    out["params_schema"] = json.loads(out.pop("params_schema_json"))
    out["default_params"] = json.loads(out.pop("default_params_json"))
    out["applies_to_channels"] = _as_list(out.get("applies_to_channels"))
    return out


def _as_list(v: Any) -> List[Any]:
    """DuckDB returns LIST columns as numpy arrays, whose truthiness raises
    rather than being falsy when empty -- so `v or []` is not safe here."""
    if v is None:
        return []
    return list(v)
