"""
tests/unit/test_registry_templates.py

A95-R2. Proves that reading a screener template from strategy_registry yields
the SAME template the screener builds from templates.py today.

This is the evidence that switching the screener's source is a data-source swap
and not a behaviour change. Without it the switch would rest on the claim that
T15 stored conditions verbatim -- which is true, and which is exactly the kind
of claim that stops being true the first time someone edits one side.

Requires the registry DB, so every test here skips (never fails) without one:
the screener's own unit tests must stay runnable in CI with no database, and a
parity test that cannot see both sides has nothing to say.

PIT Assumptions
---------------
None -- compares two in-memory declarations of the same templates.
"""

from __future__ import annotations

import pytest

from systems.technical_analysis.screener.templates import TEMPLATE_MAP, TEMPLATES


def _registry_available() -> bool:
    try:
        from strategies.registry import list_strategies

        return bool(list_strategies(channel="technical"))
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _registry_available(),
    reason="strategy_registry not populated; the screener's other tests must "
           "keep running without a database",
)


class TestParityWithTemplatesPy:
    def test_every_template_has_a_row(self):
        """A template the screener can run but the registry does not declare is
        the technical-channel version of the gap that had four fundamental
        presets runnable-but-undeclared until 2026-08-15."""
        from systems.technical_analysis.screener.registry_templates import template_exists

        undeclared = [t.name for t in TEMPLATES if not template_exists(t.name)]
        assert not undeclared, (
            f"runnable but undeclared: {undeclared}. Re-run "
            "strategies/migrations/technical.py."
        )

    def test_conditions_are_byte_identical(self):
        """The load-bearing assertion.

        The screener EXECUTES these dicts. If the stored copy differed by so
        much as an op or a threshold, the registry-fed screener would select a
        different set of stocks from the templates.py-fed one -- silently, since
        both produce a perfectly valid result."""
        from systems.technical_analysis.screener.registry_templates import load_template

        drift = []
        for t in TEMPLATES:
            got = load_template(t.name).conditions
            if got != [dict(c) for c in t.conditions]:
                drift.append((t.name, t.conditions, got))
        assert not drift, "conditions differ between templates.py and the registry:\n" + "\n".join(
            f"  {n}\n    templates.py: {a}\n    registry    : {b}" for n, a, b in drift
        )

    def test_display_and_exit_fields_match(self):
        """Everything else the screener and the exit policy read off a template.

        exit_* matter beyond display: PerTemplateExitPolicy takes its stop,
        target and max-hold from these, so a mismatch would change when
        positions close, not just what a page renders."""
        from systems.technical_analysis.screener.registry_templates import load_template

        mismatches = []
        for t in TEMPLATES:
            got = load_template(t.name)
            for field in (
                "name", "category", "description", "key_display_features",
                "exit_stop_pct", "exit_target_pct", "exit_max_hold_days",
            ):
                a, b = getattr(t, field), getattr(got, field)
                if a != b:
                    mismatches.append(f"{t.name}.{field}: templates.py={a!r} registry={b!r}")
        assert not mismatches, "\n".join(mismatches)

    def test_listing_covers_the_same_set(self):
        from systems.technical_analysis.screener.registry_templates import list_templates

        assert {t.name for t in list_templates()} == set(TEMPLATE_MAP)

    def test_listing_is_ordered(self):
        """Callers render this into a picker; an unordered listing would
        reshuffle the UI between calls."""
        from systems.technical_analysis.screener.registry_templates import list_templates

        names = [t.name for t in list_templates()]
        assert names == sorted(names)


class TestIsolation:
    def test_mutating_a_returned_template_cannot_corrupt_the_next_read(self):
        """The registry layer may cache rows. If a template handed to a caller
        shared those dicts, one caller appending a condition would change what
        every later screen() run selects."""
        from systems.technical_analysis.screener.registry_templates import load_template

        name = TEMPLATES[0].name
        first = load_template(name)
        original = len(first.conditions)
        first.conditions.append({"feature": "junk", "op": "gt", "value": 1})
        if first.conditions:
            first.conditions[0]["feature"] = "MUTATED"

        second = load_template(name)
        assert len(second.conditions) == original
        assert second.conditions[0]["feature"] != "MUTATED"


class TestUnknownTemplate:
    def test_unknown_name_is_not_declared(self):
        from systems.technical_analysis.screener.registry_templates import template_exists

        assert template_exists("definitely_not_a_template") is False

    def test_loading_an_unknown_name_raises(self):
        from strategies.definitions import DefinitionNotFound
        from systems.technical_analysis.screener.registry_templates import load_template

        with pytest.raises(DefinitionNotFound):
            load_template("definitely_not_a_template")
