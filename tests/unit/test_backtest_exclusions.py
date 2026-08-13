"""
tests/unit/test_backtest_exclusions.py

The committed backtest exclusion list (config/backtest_excluded_tickers.json).

Excluding tickers is not free: dropping names whose numbers look implausible
is precisely how a backtest is made to flatter itself, and it reintroduces the
survivorship bias the point-in-time universe exists to avoid. So these tests
are less about the loader working -- that is trivial -- than about the list
staying honest:

* every entry carries a concrete, measured reason, not "(no reason recorded)";
* the list stays small enough to be a caveat rather than a filter;
* the exclusion is actually reachable from the backtest, since a list nothing
  reads is worse than no list at all (it looks like protection).
"""

from __future__ import annotations

import json

from config.backtest_exclusions import (
    EXCLUSIONS_PATH,
    apply_exclusions,
    excluded_tickers,
    load_exclusions,
)

# The 19 mixed-source tickers measured on 2026-08-13: mostly source='fyers'
# (pre-adjusted, adj_factor=1.0) with a few legacy source=NULL rows filling
# dates Fyers lacked. Those island rows sit on a different adjustment basis,
# so the price dives and recovers within days.
EXPECTED = frozenset({
    "COALINDIA", "FILATEX", "IBULLSLTD", "IDEA", "IMFA", "INDOTHAI", "MARINE",
    "MINDACORP", "NHPC", "NTPC", "PGIL", "POWERMECH", "PTC", "SHAKTIPUMP",
    "SJVN", "SSWL", "STAR", "SURYAROSNI", "TRIVENI",
})


def test_the_committed_list_is_exactly_the_reviewed_set():
    """Asserted as equality, not containment. A ticker appearing here without
    a review is the failure mode that turns a documented caveat into a quiet
    filter on the universe."""
    assert excluded_tickers() == set(EXPECTED)


def test_every_exclusion_states_a_measured_reason():
    """'this ticker's PRICE HISTORY is unverifiable' is a valid reason;
    'this ticker's RETURNS are inconvenient' is not, and an entry with no
    reason cannot be told apart from the second kind."""
    for ticker, reason in load_exclusions().items():
        assert "no reason recorded" not in reason, ticker
        assert "adjustment-basis break" in reason, ticker
        # The measurement date is what makes the claim checkable later.
        assert "measured 2026-08-13" in reason, ticker


def test_exclusions_stay_a_caveat_not_a_filter():
    """19 of a top-800 universe is ~2%. If this ever approaches a material
    fraction, the right response is fixing the data, not growing the list."""
    assert len(excluded_tickers()) <= 40


def test_apply_exclusions_drops_only_listed_tickers_and_keeps_order():
    """Callers rely on ADTV-descending order surviving the filter."""
    kept = apply_exclusions(["RELIANCE", "COALINDIA", "TCS", "SURYAROSNI", "INFY"])
    assert kept == ["RELIANCE", "TCS", "INFY"]


def test_the_file_records_why_pre_2009_breaks_are_not_listed():
    """51 further breaks sit before 2009-04-01 and are deliberately absent
    because backtests start there. Without that note, a future reader would
    reasonably conclude the measurement had missed them."""
    raw = json.loads(EXCLUSIONS_PATH.read_text())
    assert "2009-04-01" in raw["_comment"]


def test_the_backtest_actually_applies_the_list():
    """A list nothing reads is worse than no list: it looks like protection.
    Pinned to the call site in the orchestrator's OHLCV load, which is where
    the universe is built -- filtering later would let excluded tickers reach
    the PIT ADTV ranking first."""
    import inspect

    from backtest import run_orchestrator_backtest as ro

    src = inspect.getsource(ro)
    assert "apply_exclusions(" in src
