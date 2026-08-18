"""
tests/quality/test_registry_is_load_bearing.py

A95-R3 — switch the quality gate on.

`test_strategy_registry_invariants.py` checks that the rows in the registry are
well formed. That is necessary and not sufficient: a perfectly consistent
registry that nothing reads at run time is documentation, not a source of
truth. This file enforces the other half — that the registry is LOAD BEARING.

WHAT IS ASSERTED, AND WHY IT IS NOT "NO IMPORTS"
-----------------------------------------------
Per this repo's A95-R1 finding (FeatureBacklog.md, "Observations from the
A95-R1 conversion"): the registry can never hold executable logic —
`features/fundamental_composites.SCORE_FUNCTIONS` maps names to Python
functions and a row cannot carry a callable. So a literal "no consumer may
import templates.py / fundamental_composites.py" rule is unsatisfiable, and a
test asserting it would be gamed (a local import, an indirection module) rather
than met.

The rule that IS meaningful is the declaration/implementation split:

  * DECLARATIONS — which strategies exist, their entry/exit criteria, their
    parameters, their version — are read from the registry at run time.
  * IMPLEMENTATIONS — the scoring functions, the ScreenerTemplate dataclass,
    the evaluators — stay in Python and are looked up BY NAME.

So the import checks below target the definition CONTAINERS (`TEMPLATES`,
`TEMPLATE_MAP`, `STRATEGY_CATALOG`, `SCREENER_PRESETS`, ...) — the module-level
objects that constitute a second declaration — and deliberately permit type and
callable imports from the same modules.

THE EXEMPTION LIST
------------------
`_UNCONVERTED` names every run-time site that still reads a definition
container, each with the reason it has not converted. It is an allowlist that
may only SHRINK: a new site reading a container fails this test, and removing
the last entry removes the exemption machinery. Converting a site means
deleting its line here, which is the point — the list is the remaining work,
visible, rather than a silence.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any, Dict, List, Set

import pytest

from config.settings import BACKTEST_DUCKDB_PATH
from strategies.registry import list_strategies

REPO_ROOT = Path(__file__).resolve().parents[2]

# Modules that DECLARE strategies in Python, and the module-level containers in
# each that constitute a declaration. Names not listed here (ScreenerTemplate,
# SCORE_FUNCTIONS, individual scoring callables) are implementations and are
# explicitly allowed — see the module docstring.
_DEFINITION_CONTAINERS: Dict[str, Set[str]] = {
    "systems.technical_analysis.screener.templates": {
        "TEMPLATES",
        "TEMPLATE_MAP",
        "TEMPLATE_STYLE",
        "STRATEGY_STYLES",
    },
    "features.fundamental_composites": {
        "STRATEGY_CATALOG",
        "SCREENER_PRESETS",
    },
}

# Run-time trees. Scripts, migrations and tests are excluded on purpose:
# strategies/migrations/ is what WRITES the registry from the Python source, so
# it must import it, and scripts/ are operator tools, not the live paths the
# one-generator rule is about.
_RUNTIME_DIRS = [
    "systems",
    "datastore",
    "backtest",
    "features",
    "ingestion",
    "strategies",
]
_EXCLUDED_PARTS = {"migrations", "tests", "__pycache__"}

# Run-time sites that still read a definition container. MAY ONLY SHRINK.
_UNCONVERTED: Dict[str, str] = {
    "datastore/api/routers/technical.py": (
        "Serves template styles/metadata from TEMPLATE_STYLE + STRATEGY_STYLES. "
        "A95-R2 converted the screening path; the metadata endpoints still read "
        "the dicts."
    ),
    "datastore/api/routers/fundamentals.py": (
        "Reads STRATEGY_CATALOG/SCREENER_PRESETS. Blocked on A106 — the six "
        "derived fundamental columns must exist before the fundamental half of "
        "A95-R2 can convert."
    ),
    "backtest/adapters/fundamental_adapter.py": (
        "Same A106 blocker: preset VALIDATION already reads the registry "
        "(A95-R1), preset EXECUTION still reads the Python presets."
    ),
    "systems/technical_analysis/alerts/alert_store.py": "Categorises stored alerts via TEMPLATE_MAP.",
    "systems/copilot/dedup.py": "Dedup keys off TEMPLATE_MAP categories.",
    "systems/technical_analysis/screener/outcomes.py": "Backfill/analysis helper over TEMPLATES.",
    "systems/ml_signal_engine/models/exit/per_template_exit_policy.py": "Exit policy keyed per template.",
    "systems/ml_signal_engine/models/exit/condition_based_exit_policy.py": "Exit policy reads template conditions.",
    "backtest/diagnose_ta_signal_quality.py": "Diagnostic over TEMPLATES.",
}


def _python_files() -> List[Path]:
    out: List[Path] = []
    for d in _RUNTIME_DIRS:
        for p in (REPO_ROOT / d).rglob("*.py"):
            if _EXCLUDED_PARTS & set(p.parts):
                continue
            out.append(p)
    return out


def _container_reads(path: Path) -> Set[str]:
    """Definition-container names imported by `path`, at any scope.

    Walks the AST rather than grepping so a name inside a docstring or comment
    -- of which this repo has many, since the conversions are documented in
    prose next to the code -- is not mistaken for a read.
    """
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError:  # pragma: no cover - a broken file is a different test's problem
        return set()

    found: Set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module in _DEFINITION_CONTAINERS:
            banned = _DEFINITION_CONTAINERS[node.module]
            found |= {a.name for a in node.names if a.name in banned}
    return found


def test_no_runtime_site_reads_a_definition_container_unlisted():
    """The declaration/implementation split, enforced.

    A run-time module may import the ScreenerTemplate type or a scoring
    callable; it may not import the dicts/lists that declare WHICH strategies
    exist and what they select. Every site that still does is in _UNCONVERTED
    with its reason.
    """
    offenders: Dict[str, Set[str]] = {}
    for path in _python_files():
        names = _container_reads(path)
        if not names:
            continue
        rel = path.relative_to(REPO_ROOT).as_posix()
        if rel in _UNCONVERTED:
            continue
        offenders[rel] = names

    assert not offenders, (
        "These run-time modules read a strategy DEFINITION container instead of "
        "the registry:\n"
        + "\n".join(f"  {k}: {sorted(v)}" for k, v in sorted(offenders.items()))
        + "\n\nRead the declaration from strategies/registry (technical: "
        "systems/technical_analysis/screener/registry_templates.py), or add the "
        "file to _UNCONVERTED with the reason it cannot convert yet."
    )


def test_the_screener_itself_is_fully_converted():
    """The screener is the one consumer A95-R2 actually cut over, so it is
    asserted directly and not merely covered by the sweep above -- a
    regression there would silently restore the second declaration the cutover
    removed."""
    for module in [
        "systems/technical_analysis/screener/engine.py",
        "systems/technical_analysis/alerts/daily_alert_checker.py",
    ]:
        names = _container_reads(REPO_ROOT / module)
        assert not names, f"{module} reads definition containers {sorted(names)}"


def test_exemption_list_has_no_stale_entries():
    """An entry that no longer reads a container means the site converted and
    the line was left behind. Stale exemptions are how an allowlist stops
    shrinking."""
    stale = [
        rel
        for rel in _UNCONVERTED
        if not (REPO_ROOT / rel).exists() or not _container_reads(REPO_ROOT / rel)
    ]
    assert not stale, f"_UNCONVERTED entries that no longer apply -- delete them: {stale}"


# ---------------------------------------------------------------------------
# The other half of load-bearing: what the registry declares must be runnable,
# and what is runnable must be declared.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def active() -> List[Dict[str, Any]]:
    return list_strategies(status="active", db_path=BACKTEST_DUCKDB_PATH)


def test_every_python_template_is_declared(active):
    """templates.py is still the human-editable source the migration reads, so
    a template added there and not migrated is a strategy the screener can no
    longer run -- screen() resolves from the registry now, so an unmigrated
    template raises rather than silently using the Python object."""
    from systems.technical_analysis.screener.templates import TEMPLATE_MAP

    declared = {r["name"] for r in active if r["channel"] == "technical"}
    missing = sorted(set(TEMPLATE_MAP) - declared)
    assert not missing, (
        f"Templates in templates.py with no registry row: {missing}. "
        "Run strategies/migrations/technical.py -- screen() cannot run them."
    )


def test_declared_technical_templates_load_and_match_python(active):
    """Declaration and implementation must agree where both exist: the row the
    screener now evaluates must reconstruct to the same conditions as the
    Python template it replaced. This is the parity proven once at cutover,
    asserted continuously so a registry edit that drifts from templates.py is
    caught here rather than in a screen result nobody re-checks."""
    from systems.technical_analysis.screener.registry_templates import load_template
    from systems.technical_analysis.screener.templates import TEMPLATE_MAP

    mismatches = []
    for row in [r for r in active if r["channel"] == "technical"]:
        name = row["name"]
        tpl = load_template(name)
        py = TEMPLATE_MAP.get(name)
        if py is None:
            continue
        if [dict(c) for c in tpl.conditions] != [dict(c) for c in py.conditions]:
            mismatches.append(name)
    assert not mismatches, (
        "Registry row and templates.py declare different conditions for: "
        f"{mismatches}. One of the two has drifted; the registry is what runs."
    )


def test_every_fundamental_declaration_names_a_real_implementation(active):
    """A composite_score row whose score_function does not exist, or a preset
    row with no preset behind it, is a strategy that is declared, selectable in
    the UI, and unrunnable. Looking implementations up BY NAME is the whole
    contract of the split, so the names must resolve."""
    from features.fundamental_composites import SCORE_FUNCTIONS, SCREENER_PRESETS

    unresolved = []
    for row in [r for r in active if r["channel"] == "fundamental"]:
        defn = row.get("definition") or {}
        kind = defn.get("kind")
        if kind == "composite_score":
            fn = defn.get("score_function") or row["name"]
            if fn not in SCORE_FUNCTIONS:
                unresolved.append(f"{row['strategy_key']} -> SCORE_FUNCTIONS[{fn}]")
        elif kind == "preset":
            if row["name"] not in SCREENER_PRESETS:
                unresolved.append(f"{row['strategy_key']} -> SCREENER_PRESETS[{row['name']}]")
    assert not unresolved, "Declared with no implementation to look up:\n" + "\n".join(unresolved)


def test_every_runnable_fundamental_preset_is_declared(active):
    """The A95-R1 governance gap, held shut: the backtest must not accept a
    --preset that has no registry row (no declared definition, no version, and
    signals landing under a key that resolves to nothing)."""
    from features.fundamental_composites import SCORE_FUNCTIONS, SCREENER_PRESETS

    declared = {r["name"] for r in active if r["channel"] == "fundamental"}
    runnable = set(SCORE_FUNCTIONS) | set(SCREENER_PRESETS)
    undeclared = sorted(runnable - declared)
    assert not undeclared, (
        f"Runnable but undeclared fundamental strategies: {undeclared}. "
        "Register them (strategies/migrations/fundamental.py) or stop accepting them."
    )
