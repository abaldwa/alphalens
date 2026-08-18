"""
Regression test for the annual-reset price-map bug (2026-08-18).

`StrategyPortfolio.apply_due_annual_reset` decides the FY-boundary withdrawal
against MARK-TO-MARKET equity -- its own docstring says so, and it is the
reason that call takes a `prices` argument at all when the SIP equivalent
beside it does not.

In `BacktestOrchestrator.run()` it was being called from the rebalance branch
BEFORE that branch populated `prices`. Only the non-rebalance branch filled the
map, and that branch ends in `continue`, so on every rebalance date the reset
received the empty dict initialised at the top of the iteration.

Nothing crashed, because `total_equity()` falls back to `pos.entry_price` for
any ticker missing from the map. That is precisely what made it dangerous: the
reset silently decided on COST BASIS instead of market value, so a year of
gains withdrew too little and a losing year topped up too little, with no error
anywhere.

The test spies on the call rather than asserting a final number, because the
final number is the thing that was wrong -- pinning it would have pinned the
bug. What must hold is that the reset SEES today's marked prices.
"""

from datetime import date

import pandas as pd
import pytest

from backtest.core.engine import BacktestOrchestrator, OrchestratorConfig, Signal
from backtest.core.horizon import HorizonBucket
from backtest.core.portfolio import StrategyPortfolio
from backtest.core.run_context import BacktestRun

TICKER = "RELIANCE"
ENTRY_PRICE = 100.0
MARKED_PRICE = 250.0  # deliberately far from entry so the two bases cannot be confused


@pytest.fixture(autouse=True)
def _ledger_writes_go_to_a_throwaway_db(tmp_path, monkeypatch):
    """Project policy: no test ever writes to the real DuckDB."""
    import backtest.core.signal_ledger as ledger_mod

    monkeypatch.setattr(
        ledger_mod.SignalLedgerRecorder, "db_path", tmp_path / "ledger.duckdb", raising=False
    )
    monkeypatch.setattr(ledger_mod, "write_signals", lambda *a, **k: 0)
    monkeypatch.setattr(ledger_mod, "supersede_backtest_signals", lambda *a, **k: None)


class _BuyOnceAdapter:
    """Buys on the first rebalance date, then emits nothing, so the position is
    still open and unrealised when the FY boundary arrives."""

    channel = "technical"

    def __init__(self):
        self._bought = False

    def generate_signals(self, universe, as_of_date, horizon_bucket):
        if self._bought:
            return []
        self._bought = True
        return [Signal(ticker=TICKER, action="buy", conviction=1.0)]

    def feature_vector(self, ticker, as_of_date):
        return {"dummy_feature": 1.0}


def _step_price(as_of_date):
    """Flat at entry through FY2020-21, then a step up. The step is before the
    1 Apr 2021 reset, so a correctly-priced reset sees MARKED_PRICE."""
    return ENTRY_PRICE if as_of_date < date(2021, 3, 1) else MARKED_PRICE


def test_annual_reset_receives_marked_prices_not_an_empty_map():
    trading_days = pd.bdate_range("2020-06-01", "2021-06-30")

    seen = []
    original = StrategyPortfolio.apply_due_annual_reset

    def _spy(self, as_of_date, prices):
        # Record only the boundary calls that actually have a position to mark;
        # every other trading day legitimately passes through with nothing due.
        if self.positions:
            seen.append((as_of_date, dict(prices)))
        return original(self, as_of_date, prices)

    StrategyPortfolio.apply_due_annual_reset = _spy
    try:
        run = BacktestRun(
            channel="technical", strategy_id="annual_reset_price_regression",
            horizon_bucket=HorizonBucket.D5, mode="backtest", universe_spec="test_universe",
            start_date=date(2020, 6, 1), end_date=date(2021, 6, 30),
            capital_mode="annual_reset", initial_capital=1_000_000.0,
            annual_reset_ltcg_rate=0.125, annual_reset_regime_label="post_2024",
        )
        config = OrchestratorConfig(
            trading_days=trading_days,
            universe_provider=lambda d: [TICKER],
            price_lookup=lambda t, d: _step_price(d),
            persist_signals=False,
        )
        BacktestOrchestrator().run(run, _BuyOnceAdapter(), config)
    finally:
        StrategyPortfolio.apply_due_annual_reset = original

    assert seen, "apply_due_annual_reset was never called while holding a position"

    empty_calls = [d for d, prices in seen if not prices]
    assert not empty_calls, (
        "apply_due_annual_reset was handed an EMPTY price map on "
        f"{empty_calls[:3]} — it would silently value open positions at cost "
        "basis instead of mark-to-market, which is the bug this guards."
    )

    after_step = [(d, p) for d, p in seen if d >= date(2021, 3, 1)]
    assert after_step, "no reset call observed after the price step"
    for as_of_date, prices in after_step:
        assert prices.get(TICKER) == MARKED_PRICE, (
            f"on {as_of_date} the reset saw {prices.get(TICKER)} for {TICKER}, "
            f"expected the marked price {MARKED_PRICE}. Seeing {ENTRY_PRICE} means "
            "it is valuing the position at cost again."
        )
