"""
tests/unit/test_pit_adtv_universe.py

The backtest was ALREADY truncating its universe to "top 800 by ADTV", which
is why nobody looked closer for months. But it ranked on
config/universe.py::get_top_adtv_tickers, whose adtv_cr column is a single
snapshot of TODAY'S liquidity, applied uniformly across 2009-2026. A stock
that became liquid because of a rally was therefore admitted to the universe
for all the years before that rally — exactly when it was untradeable.

INDOTECH is the worked example and the reason these tests exist: static CSV
rank 671 (inside the top 800), actual trailing-21-session rank 1,554 on its
2023-04-25 entry date, and the source of the single largest trade in the whole
run history (+1,493.95%, replicated across six templates).

These tests pin the point-in-time replacement, and specifically pin that it
is strictly backward-looking — a ranking that peeks at the current bar
reintroduces the same class of bias in a subtler form.
"""

import numpy as np
import pandas as pd
import pytest

from backtest.run_orchestrator_backtest import _build_config, _build_pit_adtv_panel


def _ohlcv(spec: dict, dates: pd.DatetimeIndex) -> pd.DataFrame:
    """spec: {ticker: (close, volume_series_or_scalar)}."""
    frames = []
    for ticker, (close, volume) in spec.items():
        vol = np.full(len(dates), volume) if np.isscalar(volume) else np.asarray(volume)
        frames.append(pd.DataFrame({
            "ticker": ticker, "date": dates, "close": close,
            "open": close, "high": close, "low": close, "volume": vol,
        }))
    return pd.concat(frames, ignore_index=True)


@pytest.fixture
def dates():
    return pd.bdate_range("2024-01-01", periods=60)


def test_panel_is_shifted_so_a_date_never_sees_its_own_bar(dates):
    """The lookahead guard. On the day a name spikes on news, including that
    day's own volume is what promotes the stock you could not have bought."""
    vol = np.ones(len(dates)) * 1_000
    vol[-1] = 10_000_000  # a single enormous day at the end
    panel = _build_pit_adtv_panel(_ohlcv({"AAA": (100.0, vol)}, dates), lookback=21)

    spike_day = dates[-1]
    prior_day = dates[-2]
    # The spike must not affect its own date's ranking value at all.
    assert panel.loc[spike_day, "AAA"] == pytest.approx(panel.loc[prior_day, "AAA"], rel=1e-9)


def test_universe_is_restricted_to_the_top_n_by_pit_adtv(dates):
    liquid = {f"LIQ{i}": (100.0, 100_000) for i in range(3)}
    thin = {f"THIN{i}": (100.0, 10) for i in range(5)}
    ohlcv = _ohlcv({**liquid, **thin}, dates)

    config = _build_config(ohlcv, sector_map={}, top_n_by_adtv=3)
    universe = config.universe_provider(dates[-1].date())

    assert sorted(universe) == sorted(liquid)
    assert not any(t.startswith("THIN") for t in universe)


def test_a_name_that_becomes_liquid_late_is_excluded_early(dates):
    """The INDOTECH shape, reduced: a stock that is thin for most of the
    window and enormous at the end. A static present-day ranking admits it
    throughout; a point-in-time ranking admits it only once it is actually
    liquid."""
    late = np.concatenate([np.full(40, 5), np.full(len(dates) - 40, 5_000_000)])
    ohlcv = _ohlcv({
        "LATE": (100.0, late),
        "STEADY1": (100.0, 10_000),
        "STEADY2": (100.0, 9_000),
    }, dates)
    config = _build_config(ohlcv, sector_map={}, top_n_by_adtv=2)

    early = config.universe_provider(dates[30].date())
    assert "LATE" not in early, "an illiquid name must not be tradeable before it is liquid"

    late_day = config.universe_provider(dates[-1].date())
    assert "LATE" in late_day, "once genuinely liquid it must become tradeable"


def test_omitting_top_n_preserves_the_previous_behaviour_exactly(dates):
    """The change must be inert until a caller opts in — every historical
    entry point keeps working unchanged."""
    ohlcv = _ohlcv({"AAA": (100.0, 10), "BBB": (100.0, 10_000_000)}, dates)
    config = _build_config(ohlcv, sector_map={}, top_n_by_adtv=None)
    assert sorted(config.universe_provider(dates[-1].date())) == ["AAA", "BBB"]


def test_dates_before_any_adtv_history_return_nothing_rather_than_everything(dates):
    """At the very start of the window there is no basis to rank on. Falling
    back to the unranked candidate list would silently disable the filter for
    the opening stretch of every run — the same 'filter appears to be applied
    but is not' failure this whole change exists to correct."""
    ohlcv = _ohlcv({"AAA": (100.0, 10_000), "BBB": (100.0, 20_000)}, dates)
    config = _build_config(ohlcv, sector_map={}, top_n_by_adtv=1)
    assert config.universe_provider(dates[0].date()) == []


def test_ranking_tracks_turnover_not_volume(dates):
    """ADTV is traded VALUE. A penny stock trading vast share counts is not
    more liquid than a high-priced stock trading fewer — ranking on raw volume
    would systematically favour the former."""
    ohlcv = _ohlcv({
        "PENNY": (1.0, 1_000_000),      # turnover 1e6
        "PRICEY": (5_000.0, 1_000),     # turnover 5e6
    }, dates)
    config = _build_config(ohlcv, sector_map={}, top_n_by_adtv=1)
    assert config.universe_provider(dates[-1].date()) == ["PRICEY"]


def test_delisted_and_unlisted_names_are_still_excluded(dates):
    """The ADTV filter composes with the existing listing/staleness check
    rather than replacing it."""
    ohlcv = _ohlcv({"AAA": (100.0, 10_000), "BBB": (100.0, 20_000)}, dates)
    ohlcv = ohlcv[~((ohlcv.ticker == "BBB") & (ohlcv.date > dates[10]))]
    config = _build_config(ohlcv, sector_map={}, top_n_by_adtv=5)
    assert "BBB" not in config.universe_provider(dates[-1].date())
