"""tests/unit/test_grace_cycles_wiring.py — momentum's --grace-cycles.

grace_cycles decides how many rebalances a holding is retained after it drops
out of the top_n. It is the only lever that directly controls how long a
winner is kept, and it was hardcoded to the MomentumAdapter default of 2 with
no CLI flag and no queue field, so no run before 2026-08-14 ever swept it.

With the exit policy set to "unconstrained" (no engine-imposed barrier), the
rank-drop rule IS the entire exit, and grace_cycles=0 makes it exact: a name
sells the rebalance it leaves the top_n. These tests pin that path, both
construction sites, and the recording of the value on the run.
"""

from __future__ import annotations

import inspect

import pandas as pd

from backtest.adapters.momentum_adapter import MomentumAdapter
from features.momentum_strategy import decide_grace_transitions


def _panel():
    idx = pd.bdate_range("2024-01-01", periods=30)
    return pd.DataFrame({"A": [100.0] * 30, "B": [50.0] * 30}, index=idx)


class TestGraceZeroSellsImmediately:
    def test_zero_grace_marks_a_dropped_holding_for_immediate_sale(self):
        """grace_remaining <= 0 is what generate_signals turns into a sell."""
        updated = decide_grace_transitions({"HELD": None}, target_set=set(), grace_cycles=0)
        assert updated["HELD"] == 0

    def test_a_holding_still_in_target_is_never_sold(self):
        updated = decide_grace_transitions({"HELD": None}, target_set={"HELD"}, grace_cycles=0)
        assert updated["HELD"] is None

    def test_default_of_two_still_defers_the_sale(self):
        """The historical behaviour must be unchanged for callers that do not
        pass the new flag -- every run before 2026-08-14 used this path."""
        updated = decide_grace_transitions({"HELD": None}, target_set=set(), grace_cycles=2)
        assert updated["HELD"] == 2


class TestAdapterAcceptsIt:
    def test_constructor_takes_zero_and_keeps_it(self):
        """0 is falsy -- an `if grace_cycles:` guard anywhere in the chain
        would silently restore the default of 2."""
        adapter = MomentumAdapter(price_panel=_panel(), top_n=10, lookback_months=12, grace_cycles=0)
        assert adapter.grace_cycles == 0

    def test_default_is_unchanged(self):
        assert MomentumAdapter(price_panel=_panel(), top_n=10, lookback_months=12).grace_cycles == 2


class TestOrchestratorWiring:
    def test_both_momentum_construction_paths_pass_grace_cycles(self):
        """The deferred path is what the queue runs; the immediate path is
        what an interactive run uses. Both have been missed independently
        before, on the panel-filter wiring."""
        from backtest import run_orchestrator_backtest as ro

        src = inspect.getsource(ro)
        sites = src.count("adapter = MomentumAdapter(")
        assert sites == 2, f"expected 2 construction sites, found {sites}"
        for site in src.split("adapter = MomentumAdapter(")[1:]:
            block = site[: site.index("\n        )") if "\n        )" in site else len(site)]
            assert "grace_cycles=grace_cycles" in block

    def test_the_value_is_recorded_on_the_run(self):
        """A run whose config does not state its grace_cycles cannot be
        reproduced, and two sweeps of different values would be
        indistinguishable in the report (AGENTS.md invariant 4)."""
        from backtest import run_orchestrator_backtest as ro

        src = inspect.getsource(ro)
        assert src.count('"grace_cycles": grace_cycles,') == 2

    def test_cli_exposes_it(self):
        from backtest.run_orchestrator_backtest import build_arg_parser

        args = build_arg_parser().parse_args(["--channel", "momentum", "--grace-cycles", "0"])
        assert args.grace_cycles == 0
        assert build_arg_parser().parse_args(["--channel", "momentum"]).grace_cycles == 2

    def test_queue_accepts_it_as_a_job_field(self):
        from backtest.run_strategy_queue import _job_to_cmd

        cmd = _job_to_cmd(
            {"kind": "orchestrator", "channel": "momentum", "grace_cycles": 0}, 0, "t",
        )
        assert "--grace-cycles" in cmd and cmd[cmd.index("--grace-cycles") + 1] == "0"


def test_unconstrained_variant_imposes_no_barrier():
    """With the exit policy off, the rank-drop rule is the whole exit. If
    "unconstrained" ever grew a real barrier, the momentum runs built on it
    would silently stop testing what they claim to."""
    from backtest.core.engine import (
        _NO_MAX_HOLD_DAYS_SENTINEL,
        _UNCONSTRAINED_STOP_PCT,
        _UNCONSTRAINED_TARGET_PCT,
        build_exit_model_for_variant,
    )

    assert _UNCONSTRAINED_TARGET_PCT >= 10.0  # +1000%
    assert _UNCONSTRAINED_STOP_PCT <= -0.99
    assert _NO_MAX_HOLD_DAYS_SENTINEL >= 10**9
    build_exit_model_for_variant("unconstrained")  # constructs without a max_hold_days
