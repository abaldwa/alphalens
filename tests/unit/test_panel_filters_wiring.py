"""tests/unit/test_panel_filters_wiring.py — backtest/adapters/panel_filters.py
and its wiring into FundamentalAdapter / MomentumAdapter / the orchestrator.

These exist because of a defect measured on 2026-08-14: across all 26
fundamental presets, a control run and a run passing --min-adtv-cr 1.0
--downtrend-filter-pct 0.15 --circuit-band-pct 0.05 produced 168 trades each,
with zero buys added or removed. FundamentalAdapter accepted none of those
parameters and the orchestrator passed none, so the flags were recorded in
config_json and rendered in the report while filtering nothing.

The tests that would have caught it are the wiring ones: that the adapter
takes the parameters, that they change the emitted signals, and that the
orchestrator hands them over on BOTH construction paths (the deferred branch
is the one the parallel queue runs, and it was missed independently).
"""

from __future__ import annotations

import inspect
from datetime import date

import pandas as pd
import pytest

from backtest.adapters.fundamental_adapter import FundamentalAdapter
from backtest.adapters.panel_filters import apply_entry_filters, is_circuit_locked


def _panels(tickers, n_days=40, price=100.0, volume=1_000_000.0):
    idx = pd.bdate_range("2024-01-01", periods=n_days)
    prices = pd.DataFrame({t: [price] * n_days for t in tickers}, index=idx)
    volumes = pd.DataFrame({t: [volume] * n_days for t in tickers}, index=idx)
    return prices, volumes


class TestApplyEntryFilters:
    def test_no_filters_configured_is_identity_and_preserves_order(self):
        """Callers rank either side of this; a silent reshuffle would change
        which names survive a later top_n cut."""
        assert apply_entry_filters(["C", "A", "B"], date(2024, 2, 1)) == ["C", "A", "B"]

    def test_liquidity_floor_drops_only_names_below_it(self):
        prices, volumes = _panels(["RICH", "POOR"])
        volumes["POOR"] = 1.0  # ~0 crore of turnover
        kept = apply_entry_filters(
            ["RICH", "POOR"], date(2024, 2, 1),
            price_panel=prices, volume_panel=volumes, min_adtv_cr=1.0,
        )
        assert kept == ["RICH"]

    def test_unmeasurable_liquidity_is_dropped_not_kept(self):
        """"We could not measure this name" is not evidence it clears the
        floor. Keeping it is how an illiquid name reaches a book that
        reports itself as liquidity-filtered."""
        prices, volumes = _panels(["KNOWN"])
        kept = apply_entry_filters(
            ["KNOWN", "NO_PANEL_DATA"], date(2024, 2, 1),
            price_panel=prices, volume_panel=volumes, min_adtv_cr=1.0,
        )
        assert kept == ["KNOWN"]

    def test_missing_panels_entirely_drop_everything_rather_than_pass_everything(self):
        assert apply_entry_filters(["A", "B"], date(2024, 2, 1), min_adtv_cr=1.0) == []

    def test_circuit_locked_bar_is_rejected(self):
        prices, volumes = _panels(["JUMPY", "CALM"])
        prices.iloc[-1, prices.columns.get_loc("JUMPY")] = 100.0 * 1.20  # +20% day
        kept = apply_entry_filters(
            ["JUMPY", "CALM"], prices.index[-1].date(),
            price_panel=prices, volume_panel=volumes, circuit_band_pct=0.05,
        )
        assert kept == ["CALM"]

    def test_insufficient_history_never_locks_a_ticker(self):
        """A data gap must not become a trading decision."""
        prices, _ = _panels(["A"])
        assert is_circuit_locked(prices, "A", prices.index[0].date(), 0.05) is False


class TestFundamentalAdapterAcceptsFilters:
    def test_constructor_takes_the_three_panel_filters(self):
        adapter = FundamentalAdapter(
            preset="quality_compounder", min_adtv_cr=1.0,
            circuit_band_pct=0.05, downtrend_filter_pct=0.15,
        )
        assert adapter.min_adtv_cr == 1.0
        assert adapter.circuit_band_pct == 0.05
        assert adapter.downtrend_filter_pct == 0.15

    def test_filters_default_to_off_so_existing_callers_are_unchanged(self):
        adapter = FundamentalAdapter(preset="quality_compounder")
        assert adapter.min_adtv_cr is None
        assert adapter.circuit_band_pct is None
        assert adapter.downtrend_filter_pct is None

    def test_filters_are_applied_before_the_top_n_cut(self):
        """The ordering that matters: filtering AFTER selection would leave
        the rejected names' slots empty, so a preset told to hold N would
        hold fewer while reporting itself fully deployed."""
        src = inspect.getsource(FundamentalAdapter.generate_signals)
        assert "apply_entry_filters(" in src
        assert src.index("apply_entry_filters(") < src.index("target = set(matched[")


class TestOrchestratorWiring:
    """Both construction sites, because they were wrong independently — the
    deferred branch is the one the parallel queue actually runs."""

    def test_both_fundamental_construction_paths_pass_the_filters(self):
        from backtest import run_orchestrator_backtest as ro

        src = inspect.getsource(ro)
        sites = src.count("adapter = FundamentalAdapter(")
        assert sites == 2, f"expected 2 construction sites, found {sites}"
        # Every site must hand over all three.
        for site in src.split("adapter = FundamentalAdapter(")[1:]:
            # To the end of the call, not the first ")" — the argument list
            # contains _real_market_cap_map(), whose paren closes first.
            block = site[: site.index("\n        )") if "\n        )" in site else len(site)]
            for param in ("min_adtv_cr", "circuit_band_pct", "downtrend_filter_pct"):
                assert param in block, f"{param} missing from a FundamentalAdapter call"

    def test_regime_gate_is_refused_for_fundamental_rather_than_ignored(self):
        """Accept-and-ignore is the defect this file exists for. The adapter
        has no regime connection, so the only honest options are wire it or
        refuse it."""
        from backtest import run_orchestrator_backtest as ro

        src = inspect.getsource(ro)
        assert src.count("not supported for channel=fundamental") == 2


class TestSingleImplementation:
    """AGENTS.md invariant 2: one implementation per filter."""

    def test_momentum_adapter_delegates_rather_than_reimplementing(self):
        from backtest.adapters import momentum_adapter as ma

        src = inspect.getsource(ma)
        assert "from backtest.adapters.panel_filters import" in src
        # The circuit-lock arithmetic must live in exactly one place.
        assert "abs((cur_price - prev_price) / prev_price)" not in src


@pytest.mark.parametrize("bad", [0.0, -1.0])
def test_zero_or_negative_floor_is_still_a_real_floor(bad):
    """0.0 is falsy — a `if min_adtv_cr:` guard anywhere in the chain would
    turn an explicit floor of zero into no filter at all."""
    prices, volumes = _panels(["A"])
    volumes["A"] = 0.0
    kept = apply_entry_filters(
        ["A"], date(2024, 2, 1), price_panel=prices, volume_panel=volumes, min_adtv_cr=bad,
    )
    assert kept == ["A"]
