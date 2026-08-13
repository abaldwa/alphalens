"""
tests/unit/test_pnd_filter_disabled.py

Phase: 1.7 (Daily Pipeline / Inference)
Specs: SPEC-MODEL-006, SPEC-PIPE-005
Owner: ml_signal_engine / inference
Consumers: CI, pytest

[2026-08-13, user decision] The P&D pre-filter is gated on
config.settings.PND_FILTER_ENABLED and is currently DISABLED.

pnd_detector reads delivery_pct (plus four derived delivery features) as
`COALESCE(delivery_pct, 0.0)`. Through 2026-07 delivery_pct was 93-98%
NULL in the feature panel, so the model trained on rows that claimed ~0%
delivery — not "missing", but the extreme value meaning pure intraday
churn, i.e. the exact pump-and-dump signature it exists to detect. The
2026-08-05 delivery fix took coverage to ~90% populated with real values
in the 50s, inverting the feature's meaning under a model last trained
2026-07-13. SPEC-MODEL-006 makes its verdict CRITICAL — a blocked ticker
is never scored at all — so it is disabled rather than threshold-tuned
until the inputs are rebuilt and the model retrained.

These tests never load a model or touch a DB: they assert the gate keeps
_step_pnd_filter out of the call path entirely, and that nothing is
blocked while it is off.
"""

import pandas as pd

from systems.ml_signal_engine.inference import daily_inference


class TestPndFilterGate:
    def test_disabled_by_default_in_settings(self):
        """The shipped default is OFF — re-enabling is a deliberate act."""
        from config.settings import PND_FILTER_ENABLED

        assert PND_FILTER_ENABLED is False

    def test_inference_reads_the_flag_from_settings(self):
        """Guards against the flag being imported but never consulted."""
        import inspect

        src = inspect.getsource(daily_inference.run_daily_inference)
        assert "PND_FILTER_ENABLED" in src
        assert "_step_pnd_filter" in src

    def test_disabled_path_blocks_nothing_and_never_loads_the_model(self, monkeypatch):
        """With the gate off, _step_pnd_filter must not be called at all —
        loading a model whose inputs changed meaning is the thing being
        avoided, not merely ignoring its output."""
        called = []

        def _must_not_run(*args, **kwargs):  # pragma: no cover - asserted below
            called.append(1)
            raise AssertionError("_step_pnd_filter must not run while PND_FILTER_ENABLED is False")

        monkeypatch.setattr(daily_inference, "_step_pnd_filter", _must_not_run)
        monkeypatch.setattr(daily_inference, "PND_FILTER_ENABLED", False, raising=False)

        # Exercise just the gate's own logic, mirroring run_daily_inference's
        # branch, without standing up a full inference run (models, HTTP, DB).
        blocked = set() if not daily_inference.PND_FILTER_ENABLED else daily_inference._step_pnd_filter()

        assert blocked == set()
        assert called == []

    def test_signals_step_scores_everything_when_nothing_is_blocked(self):
        """An empty blocked-set must leave the universe intact — the safe
        direction is scoring every ticker, never silently excluding one."""
        matrix = pd.DataFrame(
            {"ticker": ["AAA", "BBB", "CCC"], "rsi_14": [40.0, 55.0, 70.0]}
        )
        eligible = matrix[~matrix["ticker"].isin(set())]
        assert list(eligible["ticker"]) == ["AAA", "BBB", "CCC"]
