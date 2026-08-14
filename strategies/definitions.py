"""
strategies/definitions.py

Phase: A95 (make the registries load-bearing)
Owner: Platform / Architecture
Consumers: backtest/run_orchestrator_backtest.py; screener / alert checker /
daily inference as A95-R2 lands.

The READ side of strategy_registry, for code that needs to know what a strategy
IS. Everything here answers a question that was previously answered by importing
the definition module directly -- TEMPLATE_STYLE, TEMPLATES, STRATEGY_CATALOG.

WHY NOT JUST IMPORT THE DICT
----------------------------
Because then there are two copies. strategies/migrations/*.py wrote every
template, preset and model into the registry (T15, ML41, F7, ML42) and the API
serves those rows to the frontend, but the backtest kept reading the Python
dict. So a definition could be revised in one place and not the other, and the
guarantee A95 exists to provide -- that a deployed strategy's definition and its
backtested definition are the same row -- did not hold. The dicts remain as the
migration's SOURCE; they stop being a second runtime authority.

NO SILENT FALLBACK
------------------
A missing row raises. The tempting alternative -- fall back to the imported dict
when the registry has no answer -- would make the registry optional, and an
optional source of truth is the drift this module exists to end: the fallback
path would be exercised silently, forever, and nobody would learn the migration
had not been run. The error names the migration to run instead.

CACHING
-------
Lookups are memoised per process. That is deliberate rather than merely an
optimisation: a single backtest must resolve a strategy against ONE definition
for the whole run, so a revision applied mid-run must not be picked up halfway
through. A long-lived process (the API) should not use this module for rows it
expects to change under it -- it has the router, which reads per request.
"""

from functools import lru_cache
from typing import Any, Dict, Optional

from strategies.registry import RegistryError, get_strategy, strategy_key


class DefinitionNotFound(RegistryError):
    """No registry row for the requested strategy.

    Distinct from a malformed key so callers can tell "you asked for something
    that does not exist" from "the migration has not been run", which have
    different fixes.
    """


@lru_cache(maxsize=None)
def _cached_strategy(key: str, version: Optional[int]) -> Dict[str, Any]:
    try:
        row = get_strategy(key, version=version) if version is not None else get_strategy(key)
    except Exception as exc:  # registry raises its own types; re-wrap with the fix
        raise DefinitionNotFound(
            f"no strategy_registry row for {key!r}"
            + (f" at version {version}" if version is not None else "")
            + ". Run the channel's migration (strategies/migrations/) -- the backtest reads "
            "definitions from the registry (A95) and does not fall back to the Python dicts."
        ) from exc
    if not row:
        raise DefinitionNotFound(
            f"no strategy_registry row for {key!r}. Run the channel's migration "
            "(strategies/migrations/)."
        )
    return row


def get_definition(channel: str, name: str, version: Optional[int] = None) -> Dict[str, Any]:
    """One strategy's full registry row.

    `version` pins a historical revision; omit it for the current one. Pass the
    version a RUN recorded when explaining that run, for the same reason the
    frontend does -- a version 3 run explained with version 5's rules is wrong
    in a way nothing else will catch.
    """
    return _cached_strategy(strategy_key(channel, name), version)


def technical_template_style(template_name: str) -> str:
    """A technical template's style ("Trend Following", "Mean Reversion", ...).

    Replaces `TEMPLATE_STYLE[template_name]`. The style drives the run's default
    horizon bucket, so reading it from the row rather than the dict is what
    makes the horizon a property of the declared strategy instead of a second
    fact maintained beside it.
    """
    row = get_definition("technical", template_name)
    style = (row.get("definition") or {}).get("style")
    if not style:
        raise DefinitionNotFound(
            f"strategy_registry row technical:{template_name} carries no definition.style. "
            "The row exists but is incomplete -- re-run strategies/migrations/technical.py."
        )
    return str(style)


def assert_declared(channel: str, name: str) -> Dict[str, Any]:
    """Raise unless `name` is a declared strategy on `channel`; return its row.

    The validation an adapter runs on its own strategy argument. Previously
    each adapter checked membership of whichever Python dicts it happened to
    import, which meant a name could be runnable without being declared — see
    the four fundamental presets registered on 2026-08-15.

    Returns the row so a caller that needs the definition does not pay a second
    lookup; the memoisation above makes that free anyway.
    """
    return get_definition(channel, name)


def clear_cache() -> None:
    """Drop the memoised rows.

    For tests that register a strategy and then read it back in one process, and
    for anything that applies a migration and must observe the result without
    restarting. Production run paths should not need it -- see CACHING above.
    """
    _cached_strategy.cache_clear()
