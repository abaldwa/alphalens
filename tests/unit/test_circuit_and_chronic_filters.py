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
