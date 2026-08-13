"""
tests/unit/test_module_contract_defaults.py

A84/A85: the backtest module's contract says the default tradeable universe is
the top 800 by POINT-IN-TIME ADTV, and that fills on circuit-locked bars are
refused.

Both capabilities already existed and both defaulted to OFF, which is the
worst of both worlds: the code looked as if the protection were in place while
every run that did not explicitly opt in was still exposed. These tests pin
the defaults so a future refactor cannot quietly return to the permissive
setting.

Why each matters:

- Static ADTV ranking is LOOKAHEAD. Ranking on a present-day snapshot and
  applying it across history admits a stock into the universe *because of* the
  rally the backtest then claims to capture. INDOTECH ranked 671 statically
  but 1,554 on its actual entry date, and produced the single largest trade in
  the run history.
- A circuit-locked bar has no opposing side, so a fill at that price is money
  the simulation grants itself. It was off for all 195 unconstrained Technical
  runs, which therefore contain fills that could not have happened.
"""

from __future__ import annotations

import argparse
import datetime as dt
import inspect

import pytest

from backtest.run_orchestrator_backtest import (
    DEFAULT_PIT_ADTV_TOP_N,
    EARLIEST_RELIABLE_START,
    build_arg_parser,
    resolve_window,
    run_orchestrator_backtest,
)


def _defaults() -> argparse.Namespace:
    return build_arg_parser().parse_args(
        [
            "--channel", "technical",
            "--start-date", "2020-01-01",
            "--end-date", "2020-12-31",
        ]
    )


def test_pit_universe_defaults_to_top_800():
    assert DEFAULT_PIT_ADTV_TOP_N == 800
    assert _defaults().pit_adtv_top_n == 800


def test_circuit_fills_are_blocked_by_default():
    assert _defaults().block_circuit_fills is True


def test_circuit_blocking_can_be_turned_off_explicitly():
    """Reproducing a historical run must stay possible -- the old behaviour is
    still reachable, it just is not what you get by accident."""
    args = build_arg_parser().parse_args(
        [
            "--channel", "technical",
            "--start-date", "2020-01-01",
            "--end-date", "2020-12-31",
            "--no-block-circuit-fills",
        ]
    )
    assert args.block_circuit_fills is False


def test_pit_ranking_can_be_disabled_with_zero():
    args = build_arg_parser().parse_args(
        [
            "--channel", "technical",
            "--start-date", "2020-01-01",
            "--end-date", "2020-12-31",
            "--pit-adtv-top-n", "0",
        ]
    )
    assert args.pit_adtv_top_n == 0  # normalised to None at the call site


def test_programmatic_callers_get_the_same_contract():
    """A caller importing the function must not get quietly weaker defaults
    than a caller using the CLI -- that difference is invisible in the results
    and shows up only as an unexplained discrepancy months later."""
    sig = inspect.signature(run_orchestrator_backtest)
    assert sig.parameters["pit_adtv_top_n"].default == DEFAULT_PIT_ADTV_TOP_N
    assert sig.parameters["block_circuit_fills"].default is True


# ---------------------------------------------------------------------------
# A96 -- the backtest window as a first-class parameter
# ---------------------------------------------------------------------------


def test_window_in_years_resolves_backwards_from_the_end_date():
    start, end = resolve_window("10y", None, dt.date(2026, 3, 31))
    assert (start, end) == (dt.date(2016, 3, 31), dt.date(2026, 3, 31))


def test_max_window_starts_at_the_earliest_reliable_date():
    start, _ = resolve_window("max", None, dt.date(2026, 3, 31))
    assert start == EARLIEST_RELIABLE_START


def test_window_survives_a_leap_day_end_date():
    """29 Feb minus three years is not a real date. Silently raising here
    would fail a run for a reason that has nothing to do with the strategy."""
    start, _ = resolve_window("3y", None, dt.date(2024, 2, 29))
    assert start == dt.date(2021, 2, 28)


def test_pre_2009_start_is_clamped_not_honoured():
    """A window reaching past the legacy/Fyers seam does not produce a longer
    track record, it produces a fabricated one (A99-A102)."""
    start, _ = resolve_window(None, dt.date(2005, 1, 1), dt.date(2020, 1, 1))
    assert start == EARLIEST_RELIABLE_START


def test_window_and_start_date_together_are_rejected():
    """Honouring one and discarding the other would leave the caller believing
    they ran a window they did not run."""
    with pytest.raises(ValueError, match="not both"):
        resolve_window("10y", dt.date(2011, 1, 1), None)


def test_neither_window_nor_start_date_is_rejected():
    with pytest.raises(ValueError, match="required"):
        resolve_window(None, None, dt.date(2020, 1, 1))


def test_unknown_window_preset_is_rejected():
    with pytest.raises(ValueError, match="Unknown"):
        resolve_window("7y", None, dt.date(2020, 1, 1))


def test_empty_window_is_rejected():
    with pytest.raises(ValueError, match="Empty window"):
        resolve_window(None, dt.date(2021, 1, 1), dt.date(2020, 1, 1))
