"""
tests/unit/test_adtv.py

Regression test for the 5th fundamental-strategies model review, item 3:
adtv_cr_for_ticker's `.loc[:ts].tail(n)` silently produces the WRONG
(understated) ADTV when price_panel's date index is not sorted ascending —
it does not raise, it just slices the wrong rows. fundamental_adapter.py/
technical_adapter.py/momentum_adapter.py all sort volume_panel in
__init__ but previously left price_panel as whatever row order the caller
passed in. This test builds a deliberately UNSORTED price_panel (matching
the shape a real caller could hand in) and confirms the adapters now
produce the correct, sort-order-independent ADTV.
"""

import pandas as pd

from backtest.adapters.fundamental_adapter import FundamentalAdapter
from backtest.adapters.momentum_adapter import MomentumAdapter
from backtest.adapters.technical_adapter import TechnicalAdapter
from backtest.core.adtv import adtv_cr_for_ticker


def _panels():
    dates = pd.bdate_range("2023-01-01", periods=30)
    prices = pd.DataFrame({"TICK": range(100, 130)}, index=dates).astype(float)
    volumes = pd.DataFrame({"TICK": [10_000.0] * 30}, index=dates)
    return prices, volumes


class TestAdtvCrForTickerSortOrderIndependence:
    def test_unsorted_price_panel_gives_wrong_answer_without_presorting(self):
        # adtv_cr_for_ticker itself trusts its inputs are already sorted
        # (`.loc[:ts].tail(n)` — this is what silently produces the wrong
        # window on unsorted input); it does not defensively sort. This is
        # exactly why the fix belongs in the adapters' __init__ (see
        # TestAdaptersSortPricePanelLikeVolumePanel below), not here.
        prices, volumes = _panels()
        as_of = prices.index[-1].date()

        expected = adtv_cr_for_ticker("TICK", as_of, prices, volumes, adtv_lookback_days=20)

        shuffled_prices = prices.sample(frac=1.0, random_state=42)
        wrong = adtv_cr_for_ticker("TICK", as_of, shuffled_prices, volumes, adtv_lookback_days=20)
        assert wrong != expected

        # Pre-sorting (what the adapters now do in __init__) recovers the
        # correct answer regardless of input row order.
        recovered = adtv_cr_for_ticker(
            "TICK", as_of, shuffled_prices.sort_index(), volumes, adtv_lookback_days=20,
        )
        assert recovered == expected


class TestAdaptersSortPricePanelLikeVolumePanel:
    def test_fundamental_adapter_sorts_price_panel(self):
        prices, volumes = _panels()
        shuffled_prices = prices.sample(frac=1.0, random_state=7)
        adapter = FundamentalAdapter(
            preset="quality_compounder", price_panel=shuffled_prices, volume_panel=volumes,
        )
        assert adapter.price_panel.index.is_monotonic_increasing

    def test_technical_adapter_sorts_price_panel(self):
        prices, volumes = _panels()
        shuffled_prices = prices.sample(frac=1.0, random_state=7)
        adapter = TechnicalAdapter(
            template_name="momentum_breakout", price_panel=shuffled_prices, volume_panel=volumes,
        )
        assert adapter.price_panel.index.is_monotonic_increasing

    def test_momentum_adapter_sorts_price_panel(self):
        prices, volumes = _panels()
        shuffled_prices = prices.sample(frac=1.0, random_state=7)
        adapter = MomentumAdapter(price_panel=shuffled_prices, volume_panel=volumes)
        assert adapter.price_panel.index.is_monotonic_increasing
