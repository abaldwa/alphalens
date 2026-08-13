"""
tests/unit/test_circuit_and_chronic_filters.py

Two distinct circuit rules, tested separately because they answer different
questions:

  per-bar   "this PRICE was not fillable"    — a fact about a date
  chronic   "no price from this SECURITY is trustworthy" — a judgement about
            the ticker

Collapsing them would either let a chronic name trade freely on its unlocked
days, or discard a sound large cap for one locked afternoon after a results
announcement.

Context for the thresholds: a circuit filter already existed
(circuit_band_pct) but is a close-to-close PROXY and was None in all 195
unconstrained Technical runs, so no run has ever excluded a locked fill.
"""

from datetime import date

import pandas as pd
import pytest

from backtest.trade_filters import (
    CHRONIC_CIRCUIT_LOCK_PCT,
    MIN_BARS_FOR_CHRONIC_JUDGEMENT,
    chronic_circuit_tickers,
    is_circuit_locked,
)


# ---------------------------------------------------------------------------
# Per-bar detection
# ---------------------------------------------------------------------------

def test_locked_bar_is_a_traded_day_with_no_intraday_range():
    bars = pd.DataFrame({
        "high": [100.0, 100.0, 105.0],
        "low": [100.0, 100.0, 95.0],
        "volume": [5_000, 0, 5_000],
    })
    assert list(is_circuit_locked(bars)) == [True, False, False]


def test_a_flat_bar_with_no_volume_is_dormancy_not_a_lock():
    """A carried-forward price on a non-trading day looks identical to a lock
    on high/low alone. Counting it would withhold dormant small caps for a
    reason that has nothing to do with circuits."""
    bars = pd.DataFrame({"high": [42.0], "low": [42.0], "volume": [0]})
    assert not is_circuit_locked(bars).iloc[0]


# ---------------------------------------------------------------------------
# Chronic-ticker rule
# ---------------------------------------------------------------------------

def _stats(rows):
    return pd.DataFrame(rows, columns=["ticker", "n_bars", "n_locked"])


def test_chronic_lockers_are_withheld_and_occasional_ones_are_not():
    stats = _stats([
        ("CHRONIC", 500, 250),   # 50% — the INTEGRA/NUCENT population
        ("OCCASIONAL", 500, 5),  # 1% — a large cap that hit a band a few times
        ("CLEAN", 500, 0),
    ])
    assert chronic_circuit_tickers(stats) == {"CHRONIC"}


def test_a_thin_sample_is_not_judged_chronic():
    """A newly listed name with a handful of bars and one lock has a 100% lock
    RATE and nothing to conclude from it."""
    stats = _stats([("NEWLY_LISTED", MIN_BARS_FOR_CHRONIC_JUDGEMENT - 1, 50)])
    assert chronic_circuit_tickers(stats) == set()


def test_excluding_every_ticker_that_ever_locked_is_rejected_by_construction():
    """The rule this design deliberately does NOT implement. On real data,
    'exclude any share that ever hits circuit' discards 2,134 of 3,159 tickers
    (68% of the universe) — and preferentially the high-momentum names the
    Technical screens exist to find, which is a bias rather than a filter. A
    ticker locking once must survive."""
    stats = _stats([("ONE_LOCK", 1_000, 1)])
    assert chronic_circuit_tickers(stats) == set()


def test_threshold_is_a_parameter_not_a_hardcoded_constant():
    stats = _stats([("BORDERLINE", 1_000, 15)])  # 1.5%
    assert chronic_circuit_tickers(stats, threshold_pct=1.0) == {"BORDERLINE"}
    assert chronic_circuit_tickers(stats, threshold_pct=CHRONIC_CIRCUIT_LOCK_PCT) == set()


# ---------------------------------------------------------------------------
# The engine actually refuses the fill
# ---------------------------------------------------------------------------

def _run_with_circuit(locked_dates):
    from backtest.core.engine import BacktestOrchestrator, OrchestratorConfig, Signal
    from backtest.core.horizon import HorizonBucket
    from backtest.core.run_context import BacktestRun

    days = pd.bdate_range("2024-01-01", periods=60)

    class _Adapter:
        channel = "technical"

        def generate_signals(self, universe, as_of_date, horizon_bucket):
            return [Signal(ticker="AAA", action="buy", sector="IT", conviction=1.0, adtv_cr=50.0)]

        def feature_vector(self, ticker, as_of_date):
            return {}

    run = BacktestRun(
        run_id="circuit_test", channel="technical", strategy_id="s",
        horizon_bucket=HorizonBucket.D21, mode="backtest",
        start_date=date(2024, 1, 1), end_date=days[-1].date(),
        initial_capital=1_000_000.0, universe_spec="test", capital_mode="lump",
    )
    config = OrchestratorConfig(
        trading_days=days,
        universe_provider=lambda d: ["AAA"],
        price_lookup=lambda t, d: 100.0,
        sector_lookup=lambda t: "IT",
        circuit_locked_lookup=(lambda t, d: d in locked_dates) if locked_dates is not None else None,
    )
    return BacktestOrchestrator().run(run, _Adapter(), config)


def test_a_locked_bar_blocks_the_fill_and_is_recorded_as_a_gap():
    """Blocked fills must be visible. A position we intended to open or exit
    and could not is a real risk the run has to show, not simply an absent
    trade — silently skipping would make the run look like the signal never
    fired."""
    all_dates = {d.date() for d in pd.bdate_range("2024-01-01", periods=60)}
    result = _run_with_circuit(all_dates)
    reasons = {g["reason"] for g in result.data_gaps}
    assert "circuit_locked_buy_not_fillable" in reasons


def test_no_lookup_preserves_prior_behaviour():
    unblocked = _run_with_circuit(None)
    reasons = {g["reason"] for g in unblocked.data_gaps}
    assert not any("circuit_locked" in r for r in reasons)


# ---------------------------------------------------------------------------
# Data-blackout force close
# ---------------------------------------------------------------------------
# The third INDOTECH defect, and the one with no fill-time analogue: a
# blackout is only visible across a HOLDING WINDOW, not at a single fill. The
# engine previously carried a position at its last known price for as long as
# the data was missing — 209 calendar days in INDOTECH's case — and no stop,
# target or max-hold can fire on a day with no bar.

def _run_with_blackout(missing_after_index, max_blackout_sessions):
    """AAA prices normally, then stops having bars entirely."""
    from backtest.core.engine import BacktestOrchestrator, OrchestratorConfig, Signal
    from backtest.core.horizon import HorizonBucket
    from backtest.core.run_context import BacktestRun

    days = pd.bdate_range("2024-01-01", periods=80)
    cutoff = days[missing_after_index]

    class _Adapter:
        channel = "technical"

        def generate_signals(self, universe, as_of_date, horizon_bucket):
            if as_of_date <= cutoff.date():
                return [Signal(ticker="AAA", action="buy", sector="IT", conviction=1.0, adtv_cr=50.0)]
            return []

        def feature_vector(self, ticker, as_of_date):
            return {}

    def price_lookup(ticker, as_of):
        return None if as_of > cutoff.date() else 100.0

    run = BacktestRun(
        run_id="blackout_test", channel="technical", strategy_id="s",
        horizon_bucket=HorizonBucket.D21, mode="backtest",
        start_date=days[0].date(), end_date=days[-1].date(),
        initial_capital=1_000_000.0, universe_spec="test", capital_mode="lump",
    )
    config = OrchestratorConfig(
        trading_days=days,
        universe_provider=lambda d: ["AAA"],
        price_lookup=price_lookup,
        sector_lookup=lambda t: "IT",
        max_blackout_sessions=max_blackout_sessions,
    )
    return BacktestOrchestrator().run(run, _Adapter(), config)


def test_a_position_is_force_closed_after_a_blackout():
    result = _run_with_blackout(missing_after_index=20, max_blackout_sessions=5)
    reasons = [g["reason"] for g in result.data_gaps]
    assert any(r.startswith("data_blackout_forced_close_after_") for r in reasons)


def test_the_close_happens_only_after_the_threshold_is_exceeded():
    """A single missing day is a suspension or a holiday quirk, not a
    blackout. Closing on the first absent bar would churn positions out of the
    book for ordinary data noise."""
    result = _run_with_blackout(missing_after_index=20, max_blackout_sessions=5)
    closes = [g for g in result.data_gaps if g["reason"].startswith("data_blackout_forced_close_after_")]
    assert closes, "expected exactly one forced close"
    sessions = int(closes[0]["reason"].split("_after_")[1].split("_")[0])
    assert sessions == 6, f"should close on the 6th consecutive missing session, got {sessions}"


def test_without_the_setting_the_position_is_carried_indefinitely():
    """Prior behaviour must be preserved for callers that do not opt in."""
    result = _run_with_blackout(missing_after_index=20, max_blackout_sessions=None)
    reasons = [g["reason"] for g in result.data_gaps]
    assert not any("blackout" in r for r in reasons)
    assert any(r == "no_price_marking_open_position_at_last_known_price" for r in reasons)


def test_blackout_is_counted_in_trading_days_not_rebalance_dates():
    """Regression guard for a real bug in the first implementation.

    The check originally lived inline in the daily loop's REBALANCE branch,
    which returns early for non-rebalance days. The streak therefore advanced
    once per rebalance date rather than once per trading day, so on a 21-day
    cadence a 5-session threshold silently became a 105-session one — the
    position stayed in the book through months of missing data, which is
    exactly the behaviour the check exists to remove. It only surfaced because
    the test below asserts the EXACT session count rather than merely that a
    close eventually happened.

    With 80 business days, a cadence of 21, and data stopping at index 20, a
    rebalance-counted streak could never reach 6 within the window at all.
    """
    result = _run_with_blackout(missing_after_index=20, max_blackout_sessions=5)
    closes = [
        g for g in result.data_gaps
        if g["reason"].startswith("data_blackout_forced_close_after_")
    ]
    assert len(closes) == 1, f"expected exactly one forced close, got {len(closes)}"

    # The close must land 6 TRADING days after the data stops, and the
    # position must be gone afterwards — no further mark-to-market gaps for it.
    close_date = closes[0]["as_of_date"]
    later_marks = [
        g for g in result.data_gaps
        if g["reason"] == "no_price_marking_open_position_at_last_known_price"
        and g["as_of_date"] > close_date
    ]
    assert not later_marks, (
        "position still being marked after its blackout close — the force-close "
        "did not actually remove it from the book"
    )


@pytest.fixture(autouse=True)
def _a94_ledger_never_touches_the_real_db(tmp_path, monkeypatch):
    """A94: OrchestratorConfig.persist_signals defaults True, so any run in
    this module now writes to strategy_signals. Project policy forbids a
    test writing to the real DuckDB even transiently — redirect the default
    path instead of relying on each test to opt out."""
    import config.settings as settings

    monkeypatch.setattr(settings, "BACKTEST_DUCKDB_PATH", tmp_path / "a94_ledger.duckdb")
