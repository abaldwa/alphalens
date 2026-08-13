"""
tests/unit/test_instrumentation.py

Instrumentation's cardinal rule is that it must not change what it measures.
These tests pin that (results identical with timing on and off), pin the
accounting (no phase silently unattributed, no typo'd phase name creating a
second bucket), and pin the naming trap that made this work necessary at all:
`execution_timing` is a FILL POLICY, not a timing, and the two must never be
conflated again.
"""

from datetime import date

import pandas as pd
import pytest

from backtest.instrumentation import (
    PHASES,
    NullTimer,
    PhaseTimings,
    RunTimer,
    format_timings,
)


def test_timer_accumulates_seconds_and_calls_per_phase():
    timer = RunTimer()
    for _ in range(3):
        with timer.phase("signals"):
            pass
    with timer.phase("prices"):
        pass
    t = timer.finish()
    assert t.calls["signals"] == 3
    assert t.calls["prices"] == 1
    assert t.calls["equity"] == 0
    assert t.seconds["signals"] >= 0.0


def test_a_failing_phase_is_still_timed_and_still_raises():
    """The duration of a phase that blew up is often the most informative
    number in a run — a hang and a fast crash look identical without it. The
    exception must propagate unchanged."""
    timer = RunTimer()
    with pytest.raises(ValueError, match="boom"):
        with timer.phase("signals"):
            raise ValueError("boom")
    assert timer.timings.calls["signals"] == 1


def test_unknown_phase_name_raises_rather_than_creating_a_bucket():
    """A typo'd phase would otherwise split one cost across two names and make
    the percentages quietly wrong."""
    timer = RunTimer()
    with pytest.raises(KeyError, match="unknown phase"):
        with timer.phase("signalz"):
            pass


def test_unattributed_time_is_reported_not_hidden():
    """Shares are computed against MEASURED time, and the gap between measured
    and wall clock is surfaced. Normalising the phases to sum to 100% would
    conceal a missing phase, which is the failure this whole step exists to
    correct."""
    t = PhaseTimings()
    t.record("signals", 1.0)
    t.total_seconds = 4.0
    d = t.as_dict()
    assert d["measured_seconds"] == 1.0
    assert d["unattributed_seconds"] == 3.0
    assert d["phases"]["signals"]["pct_of_measured"] == 100.0


def test_unattributed_never_goes_negative():
    """total_seconds is measured around the outside, but nested or overlapping
    phases could sum past it; a negative residual would read as nonsense in a
    report."""
    t = PhaseTimings()
    t.record("signals", 5.0)
    t.total_seconds = 1.0
    assert t.as_dict()["unattributed_seconds"] == 0.0


def test_null_timer_matches_the_interface_and_records_nothing():
    timer = NullTimer()
    with timer.phase("signals"):
        pass
    assert timer.finish().calls["signals"] == 0


def test_slowest_phase_identifies_the_optimisation_target():
    t = PhaseTimings()
    t.record("signals", 10.0)
    t.record("prices", 2.0)
    assert t.slowest() == "signals"


def test_slowest_is_none_when_nothing_ran():
    assert PhaseTimings().slowest() is None


def test_every_declared_phase_appears_in_the_output():
    """A phase with zero calls must still be present, so a reader can tell
    'this phase cost nothing' from 'this phase was never instrumented'."""
    d = PhaseTimings().as_dict()
    assert set(d["phases"]) == set(PHASES)


def test_format_timings_ranks_by_cost_and_omits_unused_phases():
    t = PhaseTimings()
    t.record("prices", 1.0)
    t.record("signals", 9.0)
    t.total_seconds = 10.0
    out = format_timings(t)
    assert out.index("signals") < out.index("prices")
    assert "equity" not in out


# ---------------------------------------------------------------------------
# The parity guarantee, exercised through the real orchestrator
# ---------------------------------------------------------------------------

def _minimal_run():
    from backtest.core.engine import BacktestOrchestrator, OrchestratorConfig, Signal
    from backtest.core.horizon import HorizonBucket
    from backtest.core.run_context import BacktestRun

    days = pd.bdate_range("2024-01-01", "2024-02-15")

    class _Adapter:
        channel = "technical"

        def generate_signals(self, universe, as_of_date, horizon_bucket):
            # Emitted every day; the engine itself only acts on rebalance
            # dates. Gating here instead would risk never landing on one and
            # leaving the parity assertion below comparing two empty runs.
            return [Signal(ticker="AAA", action="buy", sector="IT", conviction=1.0, adtv_cr=50.0)]

        def feature_vector(self, ticker, as_of_date):
            return {}

    run = BacktestRun(
        run_id="timing_parity", channel="technical", strategy_id="s",
        horizon_bucket=HorizonBucket.D21, mode="backtest",
        start_date=date(2024, 1, 1), end_date=date(2024, 2, 15),
        initial_capital=1_000_000.0, universe_spec="test", capital_mode="lump",
    )
    config = OrchestratorConfig(
        trading_days=days,
        universe_provider=lambda d: ["AAA"],
        price_lookup=lambda t, d: 100.0,
        sector_lookup=lambda t: "IT",
    )
    return BacktestOrchestrator(), run, _Adapter(), config


def test_timing_does_not_change_results():
    """The one guarantee that matters. If instrumentation can move a number,
    every measured run is suspect and the measurement is worse than useless."""
    orch, run, adapter, config = _minimal_run()
    config.collect_timings = True
    timed = orch.run(run, adapter, config)

    orch2, run2, adapter2, config2 = _minimal_run()
    config2.collect_timings = False
    untimed = orch2.run(run2, adapter2, config2)

    assert timed.metrics == untimed.metrics
    # The equity curve is the strictest available comparison: every trade,
    # fill price and cash movement shows up in it, so an identical curve means
    # instrumentation moved nothing.
    assert list(timed.equity_curve) == list(untimed.equity_curve)
    assert timed.equity_curve, "parity asserted over an empty run proves nothing"
    assert timed.phase_timings != {}
    assert untimed.phase_timings == {}


def test_run_records_real_durations_for_the_phases_that_ran():
    orch, run, adapter, config = _minimal_run()
    result = orch.run(run, adapter, config)
    phases = result.phase_timings["phases"]
    assert phases["signals"]["calls"] > 0
    assert phases["equity"]["calls"] > 0
    assert result.phase_timings["total_seconds"] > 0


def test_execution_timing_is_a_policy_and_phase_timings_is_the_measurement():
    """Guards the exact confusion that left 500 runs with no timing data:
    execution_timing looks like instrumentation, is recorded on every run, and
    is actually a fill convention. They must stay separate fields with
    unrelated types."""
    orch, run, adapter, config = _minimal_run()
    result = orch.run(run, adapter, config)
    assert result.execution_timing in ("same_day_close", "next_day_open")
    assert isinstance(result.phase_timings, dict)
    assert "phases" in result.phase_timings
