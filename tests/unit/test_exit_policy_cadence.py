"""tests/unit/test_exit_policy_cadence.py — OrchestratorConfig.exit_policy_cadence.

Momentum is a periodic strategy: it ranks the universe every N trading days
and holds its selection until the next ranking, so entries exist ONLY on
rebalance dates. The orchestrator nonetheless ran its exit policy every
trading day for every channel, which meant a momentum position stopped out on
day 3 of a 63-day cadence left its slot empty for the remaining 60 days.

Measured on 2026-08-14 at top_n=10: momentum lb6 held a MEDIAN of 1 position
(mean 1.55), lb3 a mean of 3.86. The reported CAGRs described a book that was
15-40% deployed with the balance idle in cash.

These tests pin the cadence itself, and the per-channel default -- the failure
mode is silent (a run completes and reports plausible numbers), so nothing
else would catch a regression.
"""

from __future__ import annotations

import inspect

from backtest.core.engine import BacktestOrchestrator, OrchestratorConfig
from backtest.run_orchestrator_backtest import _exit_policy_cadence_for


class TestPerChannelDefault:
    def test_momentum_is_rebalance_only(self):
        assert _exit_policy_cadence_for("momentum") == "rebalance"

    def test_daily_signal_channels_keep_daily_exits(self):
        """A technical stop is part of the strategy, not an artifact of
        cadence -- these must not be swept up in the momentum fix."""
        assert _exit_policy_cadence_for("technical") == "daily"
        assert _exit_policy_cadence_for("ml") == "daily"

    def test_default_is_daily_so_existing_callers_are_unchanged(self):
        assert OrchestratorConfig.__dataclass_fields__["exit_policy_cadence"].default == "daily"


class TestRunLoopHonoursIt:
    def test_non_rebalance_days_skip_the_exit_pass_when_rebalance_only(self):
        """The gate must sit on the non-rebalance branch. Gating the
        rebalance-day call instead would disable exits altogether."""
        src = inspect.getsource(BacktestOrchestrator.run)
        before, _, after = src.partition("if not is_rebalance_date:")
        assert after, "the non-rebalance branch moved; this test needs updating"
        gate = 'config.exit_policy_cadence == "daily"'
        # Guarded inside the non-rebalance branch...
        non_rebalance = after[: after.index("            # Walk-Forward")]
        assert gate in non_rebalance
        assert non_rebalance.index(gate) < non_rebalance.index("_apply_exit_policy")

    def test_the_rebalance_day_exit_pass_is_unconditional(self):
        """Momentum still exits -- periodically, alongside rotation. If this
        call were gated too, positions would never be stopped out at all."""
        src = inspect.getsource(BacktestOrchestrator.run)
        after_branch = src[src.index("# Walk-Forward"):]
        assert "_apply_exit_policy" in after_branch
        assert "exit_policy_cadence" not in after_branch


def test_both_orchestrator_construction_paths_set_the_cadence():
    """The deferred path is the one the parallel queue runs; the immediate
    path is the one interactive runs use. Both were missed independently on
    the last filter-wiring fix."""
    from backtest import run_orchestrator_backtest as ro

    src = inspect.getsource(ro)
    assert src.count("config.exit_policy_cadence = _exit_policy_cadence_for(channel)") == 2
