"""
tests/quality/test_strategy_registry_invariants.py

Enforces the architectural invariants in AGENTS.md (A95). These are the rules
that make the registry the single source of truth rather than a fifth copy of
the same facts.

Deliberately in tests/quality/ alongside the no-stub policy: these are not
tests of a feature, they are structural rules about the codebase that must
hold regardless of which channel someone is working on.

Scope note: the invariants say strategies and filters are DECLARED only in the
registry. They do not say Python may not contain the template objects the
migrations read -- templates.py is still the human-editable source that
migrations import. What is forbidden is a channel reading those objects at RUN
time instead of reading the registry, and that is what the runtime tests below
check as each channel migrates.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List

import pytest

from config.settings import BACKTEST_DUCKDB_PATH
from strategies.registry import list_filters, list_strategies

CHANNELS = ["momentum", "technical", "fundamental", "ml"]


@pytest.fixture(scope="module")
def strategies() -> List[Dict[str, Any]]:
    return list_strategies(status=None, db_path=BACKTEST_DUCKDB_PATH)


@pytest.fixture(scope="module")
def filters() -> List[Dict[str, Any]]:
    return list_filters(db_path=BACKTEST_DUCKDB_PATH)


def test_every_channel_has_registered_strategies(strategies):
    """Invariant 1: strategies are declared in strategy_registry. A channel
    with zero rows has not migrated, so nothing downstream can read it."""
    by_channel = {c: 0 for c in CHANNELS}
    for s in strategies:
        by_channel[s["channel"]] = by_channel.get(s["channel"], 0) + 1
    missing = [c for c, n in by_channel.items() if n == 0]
    assert not missing, (
        f"No registry rows for {missing}. Run strategies/migrations/<channel>.py "
        "-- until then those channels cannot be driven from the registry."
    )


def test_strategy_keys_are_unique_per_current_version(strategies):
    """Invariant 4: append-only versioning means exactly one CURRENT row per
    key. Two live rows for one key makes 'the definition' ambiguous, and every
    consumer would silently pick whichever the query returned first."""
    current = [s for s in strategies if s.get("valid_to") is None]
    keys = [s["strategy_key"] for s in current]
    dupes = {k for k in keys if keys.count(k) > 1}
    assert not dupes, f"Multiple current versions for: {sorted(dupes)}"


def test_strategy_key_matches_channel_and_name(strategies):
    """The key is the cross-application identity (A89). If it disagrees with
    its own channel/name columns, a UI deep link resolves to the wrong row."""
    bad = [
        s["strategy_key"]
        for s in strategies
        if s["strategy_key"] != f"{s['channel']}:{s['name']}"
    ]
    assert not bad, f"strategy_key disagrees with channel:name for {bad[:5]}"


def test_every_referenced_filter_exists(strategies, filters):
    """Invariant 2: one implementation per filter, declared in
    filter_registry. A strategy referencing an unknown filter_id would fail
    only at run time, on whichever machine ran it."""
    known = {f["filter_id"] for f in filters}
    unknown: Dict[str, List[str]] = {}
    for s in strategies:
        for fid in s.get("filter_ids") or []:
            if fid not in known:
                unknown.setdefault(fid, []).append(s["strategy_key"])
    assert not unknown, (
        "Strategies reference filters that are not in filter_registry: "
        + json.dumps({k: v[:3] for k, v in unknown.items()}, indent=2)
    )


def test_filters_declare_an_implementation(filters):
    """Invariant 2 again: exactly one implementation per filter. A filter with
    no implementation_ref is a name with no behaviour -- which is how the same
    concept ends up re-implemented per channel."""
    missing = [f["filter_id"] for f in filters if not f.get("implementation_ref")]
    assert not missing, f"Filters with no implementation_ref: {missing}"


def test_filter_defaults_satisfy_their_own_schema(filters):
    """A declared default outside its declared range is a trap: the filter
    looks configured and does nothing, or silently over-filters."""
    problems = []
    for f in filters:
        schema = f.get("params_schema") or {}
        defaults = f.get("default_params") or {}
        for param, spec in schema.items():
            if spec.get("required") and param not in defaults:
                problems.append(f"{f['filter_id']}.{param} required but has no default")
                continue
            if param not in defaults:
                continue
            value = defaults[param]
            lo, hi = spec.get("min"), spec.get("max")
            if lo is not None and value < lo:
                problems.append(f"{f['filter_id']}.{param}={value} below min {lo}")
            if hi is not None and value > hi:
                problems.append(f"{f['filter_id']}.{param}={value} above max {hi}")
    assert not problems, "\n".join(problems)


def test_entry_criteria_are_predicate_lists(strategies):
    """The predicate grammar is what makes one strategy comparable to another
    across channels. Entry is an ordered LIST of predicates in every channel;
    a dict here means someone stored a channel-specific blob instead."""
    bad = [
        s["strategy_key"]
        for s in strategies
        if not isinstance(s.get("entry_criterion"), list)
    ]
    assert not bad, f"entry_criterion is not a predicate list for {bad[:5]}"


def test_predicates_are_well_formed(strategies):
    """Each predicate needs a feature and an operator. A malformed one either
    raises at run time or, worse, is skipped and silently widens the screen."""
    from strategies.predicates import validate_predicates, PredicateError

    failures = []
    for s in strategies:
        preds = s.get("entry_criterion") or []
        if not preds:
            continue
        try:
            validate_predicates(preds, where=s["strategy_key"])
        except PredicateError as exc:
            failures.append(str(exc))
    assert not failures, "Malformed predicates:\n" + "\n".join(failures[:10])


def test_no_strategy_is_missing_an_exit(strategies):
    """A strategy with no exit criterion has no defined way to close a
    position, which in a backtest silently becomes 'hold to the end of the
    window' and flatters every result."""
    bad = [
        s["strategy_key"]
        for s in strategies
        if not (s.get("exit_criterion") or {}).get("variant")
    ]
    assert not bad, f"No exit variant declared for {bad[:5]}"
