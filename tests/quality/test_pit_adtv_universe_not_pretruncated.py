"""
tests/quality/test_pit_adtv_universe_not_pretruncated.py

Phase: Backtest universe correctness (2026-08-20)
Owner: Platform / Backtest
Consumers: CI

Guards the fix for the defect found on 2026-08-20: the backtest candidate
pool was truncated to `get_top_adtv_tickers(max_tickers)` -- a rank on the
adtv_cr column of TODAY'S universe CSV -- BEFORE the point-in-time ADTV
stage ever saw the data. Both filters keep 800 names, so running the static
one first made the PIT one a no-op: ranking 800 and taking the top 800
returns all 800, and every historical date screened a pool selected by 2026
liquidity.

WHY A TEST AND NOT A COMMENT
----------------------------
A84 was recorded as DONE in FeatureBacklog.md, and `pit_adtv_top_n` really
did default to 800 -- the PIT ranker was wired, enabled, and running. It was
simply second in line. Nothing failed, no run errored, and the 19-Aug
momentum grid (1,260 runs) shipped to the dashboard with band 501-800
retaining 16.7% of its names in 2009 rising to 51.7% in 2026, the shape of
survivorship bias. A correctness test on any single run cannot see this;
only the ORDER of the two filters is checkable, so that is what this file
checks.

WHAT THIS DOES NOT PROVE
------------------------
It does not prove the PIT ranking is itself correct (that is
_build_pit_adtv_panel's shift-by-one, covered elsewhere), and it does not
prove any particular run traded a sensible universe. It proves only that
asking for PIT ranking is no longer silently defeated upstream.
"""

from __future__ import annotations

from datetime import date

import pytest

from backtest import shared_panels
import backtest.run_orchestrator_backtest as orch


def test_ohlcv_cache_key_separates_pit_from_static():
    """Two runs differing only in pit_adtv_top_n get different row sets, so
    they must not share one cached frame. Without this the second run in a
    sweep silently inherits the first one's universe."""
    static = shared_panels.ohlcv_key(800, 60, date(2009, 4, 1), date(2026, 6, 30), None, None)
    pit = shared_panels.ohlcv_key(800, 60, date(2009, 4, 1), date(2026, 6, 30), None, 800)
    assert static != pit, (
        "ohlcv_key ignores pit_adtv_top_n — a PIT run and a static run would "
        "share one cached OHLCV frame and one of them would screen the wrong universe"
    )


def test_pit_run_loads_full_universe_not_static_top_n(monkeypatch):
    """The candidate pool for a PIT run is the full curated universe.

    Asserted by capturing which universe helper is consulted rather than by
    fetching real OHLCV: the point is which list the pool is DRAWN from, and
    a bulk fetch of 2,300 tickers does not belong in CI.
    """
    calls = []

    def fake_get_tickers():
        calls.append("full")
        return [f"T{i}" for i in range(2317)]

    def fake_get_top_adtv_tickers(n):
        calls.append("static")
        return [f"T{i}" for i in range(n)]

    monkeypatch.setattr(orch, "get_tickers", fake_get_tickers)
    monkeypatch.setattr(orch, "get_top_adtv_tickers", fake_get_top_adtv_tickers)
    monkeypatch.setattr(orch, "DataStoreClient", lambda: object())

    # Stop at the first step AFTER the pool is chosen: DataStoreClient() is
    # constructed before the selection, so raising there would prove nothing.
    class _Stop(Exception):
        pass

    def stop_here(tickers, context=None):
        raise _Stop()

    monkeypatch.setattr(orch, "apply_exclusions", stop_here)

    with pytest.raises(_Stop):
        orch._fetch_real_ohlcv_uncached(
            max_tickers=800, min_history_days=60,
            start_date=date(2009, 4, 1), end_date=date(2026, 6, 30),
            ohlcv_snapshot_dir=None, pit_adtv_top_n=800,
        )

    assert calls == ["full"], (
        f"a PIT run consulted {calls!r}; it must draw from the full curated universe so "
        "universe_provider's per-date ADTV rank has something to rank. Truncating by "
        "static ADTV first makes the PIT stage a no-op (2026-08-20 defect)"
    )


def test_non_pit_run_still_truncates_statically(monkeypatch):
    """Callers that do NOT ask for PIT ranking keep their previous behaviour
    exactly — the fix must not change what a static run screens."""
    calls = []
    monkeypatch.setattr(orch, "get_tickers", lambda: (calls.append("full"), ["A"])[1])
    monkeypatch.setattr(
        orch, "get_top_adtv_tickers", lambda n: (calls.append("static"), ["A"])[1]
    )
    monkeypatch.setattr(orch, "DataStoreClient", lambda: object())

    class _Stop(Exception):
        pass

    def stop_here(tickers, context=None):
        raise _Stop()

    monkeypatch.setattr(orch, "apply_exclusions", stop_here)

    with pytest.raises(_Stop):
        orch._fetch_real_ohlcv_uncached(
            max_tickers=800, min_history_days=60,
            start_date=date(2009, 4, 1), end_date=date(2026, 6, 30),
            ohlcv_snapshot_dir=None, pit_adtv_top_n=None,
        )

    assert calls == ["static"]
