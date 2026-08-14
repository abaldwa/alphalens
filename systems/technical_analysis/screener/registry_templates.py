"""
systems/technical_analysis/screener/registry_templates.py

Phase: A95-R2 (screener reads the registry)
Owner: Platform / Architecture
Consumers: systems/technical_analysis/screener/engine.py (once switched over),
tests/unit/test_registry_templates.py

Builds a ScreenerTemplate from its strategy_registry row instead of from the
TEMPLATES list in templates.py.

WHY THIS IS A SAFE SWAP AND NOT A REWRITE
-----------------------------------------
T15 stored each template's `conditions` VERBATIM -- build_rows() does
`[dict(c) for c in t.conditions]` after validating them against
strategies/predicates.py, and refuses to migrate anything the grammar cannot
express. Verified 2026-08-15 across all 63 templates: the stored
`entry_criterion` is byte-identical to the Python `conditions`, using 7 ops that
all already exist in the grammar. There are no non-expressible technical
templates.

So this module does not reinterpret anything. It reads back the same dicts the
screener already evaluates, and the parity test asserts that field by field
rather than trusting the claim.

WHY IT MATTERS ANYWAY
---------------------
The one-generator rule says backtest, paper trading and live must share ONE
implementation of "what does this strategy select". Keeping the screener on the
Python list while the report, the deploy page and the API explain strategies
from registry rows leaves TWO declarations of the same entry criteria, free to
drift. Nothing would crash; the screener would simply select on one definition
while every surface described another. That is the same "the backtest said X but
we own Y" failure the quality gate is named for, displaced from code into data.

CACHING: DELIBERATELY NOT MEMOISED HERE
---------------------------------------
strategies/definitions.py memoises per process, which is right for a backtest
(one run must resolve one definition throughout) and wrong here. The screener
runs inside the long-lived API and the daily scheduler; a template revised in
the registry must take effect without a restart, and a memoised row would serve
the old definition indefinitely. This reads through on every call. That is one
small indexed lookup against a table of 63 rows, next to a Parquet load that
dominates it -- see load_template's note on the cost.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from strategies.definitions import get_definition
from strategies.registry import get_strategy, strategy_key
from systems.technical_analysis.screener.templates import ScreenerTemplate

logger = logging.getLogger(__name__)


def template_from_row(row: Dict[str, Any]) -> ScreenerTemplate:
    """Reconstruct a ScreenerTemplate from a strategy_registry row.

    Field mapping, all of it fixed by how strategies/migrations/technical.py
    wrote the row:

        name                 <- name
        category             <- category
        description          <- description
        conditions           <- entry_criterion   (stored verbatim by T15)
        key_display_features <- definition.key_display_features
        exit_*               <- exit_criterion.{stop_pct,target_pct,max_hold_days}

    `description` falls back to display_label and then to the name: the
    migration wrote t.description into BOTH fields, so they agree today, but a
    later revision could set only one and a template with no description at all
    would otherwise render as None in the UI.
    """
    definition = row.get("definition") or {}
    exit_criterion = row.get("exit_criterion") or {}

    return ScreenerTemplate(
        name=row["name"],
        category=row.get("category") or "",
        description=row.get("description") or row.get("display_label") or row["name"],
        # list(...) rather than the row's own list object: the caller must not
        # be able to mutate a cached registry payload through the template it
        # was handed. The dicts inside are shallow-copied for the same reason.
        conditions=[dict(c) for c in (row.get("entry_criterion") or [])],
        key_display_features=list(definition.get("key_display_features") or []),
        exit_stop_pct=exit_criterion.get("stop_pct"),
        exit_target_pct=exit_criterion.get("target_pct"),
        exit_max_hold_days=exit_criterion.get("max_hold_days"),
    )


def load_template(template_name: str) -> ScreenerTemplate:
    """One template, from the registry. Raises DefinitionNotFound if absent.

    Not memoised -- see the module docstring. The cost is one indexed lookup on
    a 63-row table; the Parquet load in the same screen() call reads the whole
    day's feature panel and dominates it by orders of magnitude.
    """
    return template_from_row(get_definition("technical", template_name))


def list_templates(status: Optional[str] = "active") -> List[ScreenerTemplate]:
    """Every declared technical template, ordered by name.

    Replaces iteration over the TEMPLATES list. Ordered explicitly because
    callers render this into a picker and an unordered listing would reshuffle
    the UI between calls for no reason.
    """
    from strategies.registry import list_strategies

    rows = list_strategies(channel="technical", status=status)
    return [template_from_row(r) for r in sorted(rows, key=lambda r: r["name"])]


def template_exists(template_name: str) -> bool:
    """Whether `template_name` is a declared technical strategy.

    Replaces `template_name in TEMPLATE_MAP`. Kept separate from load_template
    so the caller's error message stays the screener's own rather than the
    registry's -- screen() raises KeyError with the available names, which is a
    published contract (SPEC-TA-005).
    """
    # get_strategy returns None for an absent key rather than raising, so the
    # result must be TESTED, not merely awaited. An earlier version of this
    # wrapped the call in try/except and returned True on no exception, which
    # reported every unknown template as declared -- and would have let
    # screen() proceed to load a template that does not exist.
    try:
        return get_strategy(strategy_key("technical", template_name)) is not None
    except Exception:
        # No database, or a malformed key. Unknown, not declared.
        return False
