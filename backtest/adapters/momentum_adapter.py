"""
backtest/adapters/momentum_adapter.py

Phase: Unified Backtest & Paper Trading Umbrella, Phase 2
Owner: Platform / Backtest
Consumers: backtest/core/engine.py::BacktestOrchestrator

A genuine backtest.core.engine.StrategyAdapter — unlike ml_adapter.py,
Momentum's signal ("rank the universe by trailing return, hold the top
N") is a stateless-per-date computation that maps naturally onto
BacktestOrchestrator's per-rebalance-date generate_signals() loop, so
this adapter drives the shared orchestrator directly rather than
translating a separate self-contained backtest's output.

Consistent with the confirmed "wrap, don't refactor" principle applied
to backtest/engine.py: backtest/momentum_backtest.py (the existing
standalone MomentumBacktester, which backs currently-published external
results) is NOT modified. This adapter reuses only its underlying PURE
functions — features/momentum_signal.py's trailing_momentum_from_panel
(stateless, already in-memory, no DB access during a run) — rather than
touching MomentumBacktester's class itself. The two engines coexist:
MomentumBacktester continues to serve existing callers
(scripts/run_momentum_experimentation.py, the external published
artifacts) unchanged; this adapter is the path new unified-Backtest-menu
runs use going forward, per BacktestUmbrellaPlan.md Phase 2's migration
guidance ("verified by diffing new-adapter output against the
pre-migration momentum_backtest.py standalone results on the same real
window — must match within a documented tolerance").

State note: BacktestOrchestrator's StrategyAdapter protocol has no
portfolio-state parameter — generate_signals() only receives the
universe/date/horizon_bucket. A rank-rotation strategy needs to know
what it currently holds to decide sells (fell out of the top N) vs buys
(entered the top N), so this adapter tracks its own _currently_held set,
updated at the end of each generate_signals() call to the NEW target
set — valid because BacktestOrchestrator always calls generate_signals()
exactly once per rebalance date, in date order, and executes the
returned signals before the next call (verified in
backtest/core/engine.py's run() loop).
"""

import logging
from datetime import date as date_type
from typing import Any, Dict, List, Optional

import pandas as pd

from backtest.core.engine import Signal
from backtest.core.horizon import HorizonBucket
from features.momentum_signal import lookback_trading_days, trailing_momentum_from_panel

logger = logging.getLogger(__name__)


class MomentumAdapter:
    channel = "momentum"

    def __init__(
        self, price_panel: pd.DataFrame, top_n: int = 10, lookback_months: int = 6,
        sector_lookup: Optional[Dict[str, str]] = None, volume_panel: Optional[pd.DataFrame] = None,
        adtv_lookback_days: int = 20,
    ) -> None:
        """
        price_panel : wide DataFrame (date index, ticker columns, close
            prices) — see features/momentum_signal.py::load_price_panel.
            Supplied by the caller; this adapter never queries the DB
            itself (same in-memory design as MomentumBacktester).
        top_n : how many top-ranked tickers to hold at any time.
        lookback_months : trailing-return lookback window.
        sector_lookup : optional ticker -> sector map, used only to
            populate Signal.sector for core/portfolio.py's sector-cap
            check; defaults every ticker to "Unknown" (no sector cap
            bites) if omitted.
        volume_panel : optional wide adjusted-volume DataFrame, same shape
            as price_panel (2026-07-20, Truthful Review Gap #6 fix). When
            supplied, populates Signal.adtv_cr with the real trailing
            adtv_lookback_days-day average daily traded value (INR crore)
            per ticker — the same real formula MomentumBacktester's
            `_adtv_cr()` already uses — so core/portfolio.py's existing
            ADTV hard cap (DEFAULT_ADTV_CAP_FRACTION) actually engages
            instead of silently being bypassed (adtv_cr defaults to None
            when omitted, matching prior behavior exactly — no adapter
            populated it before this fix).
        """
        if top_n <= 0:
            raise ValueError("top_n must be positive")
        # [BUG FIX, 5th fundamental-strategies review, item 3] same
        # unsorted-price_panel bug as fundamental_adapter.py/
        # technical_adapter.py (adtv.py's adtv_cr_for_ticker's
        # `.loc[:ts].tail(n)` silently returns a wrong window on an
        # unsorted index) — not yet triggered here in practice (this
        # adapter's own momentum-ranking code already required a sorted
        # price_panel elsewhere), but fixed for consistency since it's the
        # same underlying bug.
        self.price_panel = price_panel.sort_index() if price_panel is not None else None
        self.top_n = top_n
        self.lookback_days = lookback_trading_days(lookback_months)
        self._sector_lookup = sector_lookup or {}
        self.volume_panel = volume_panel.sort_index() if volume_panel is not None else None
        self.adtv_lookback_days = adtv_lookback_days
        self._currently_held: set = set()
        self._last_momentum: pd.Series = pd.Series(dtype=float)

    def _adtv_cr(self, ticker: str, as_of_date: date_type) -> Optional[float]:
        """Real trailing-window average daily traded value (INR crore) for
        one ticker, or None if volume_panel wasn't supplied or the ticker/
        date has no real data — never fabricated, matches
        MomentumBacktester._adtv_cr's own real-gap NaN handling."""
        if self.volume_panel is None:
            return None
        if ticker not in self.volume_panel.columns or ticker not in self.price_panel.columns:
            return None
        ts = pd.Timestamp(as_of_date)
        window_prices = self.price_panel[ticker].loc[:ts].tail(self.adtv_lookback_days)
        window_volume = self.volume_panel[ticker].loc[:ts].tail(self.adtv_lookback_days)
        traded_value_cr = (window_prices * window_volume) / 1e7
        value = traded_value_cr.mean(skipna=True)
        return float(value) if pd.notna(value) else None

    def generate_signals(self, universe: List[str], as_of_date: date_type, horizon_bucket: HorizonBucket) -> List[Signal]:
        momentum = trailing_momentum_from_panel(
            self.price_panel, universe, str(as_of_date), self.lookback_days
        )
        self._last_momentum = momentum
        if momentum.empty:
            # No fabricated ranking when there isn't enough real history yet
            # (No-Mock-Data Policy) — just hold whatever's already held.
            return []

        target = set(momentum.sort_values(ascending=False).head(self.top_n).index.tolist())
        signals: List[Signal] = []

        for ticker in sorted(self._currently_held - target):
            signals.append(Signal(
                ticker=ticker, action="sell", sector=self._sector_lookup.get(ticker, "Unknown"),
                conviction=0.0, adtv_cr=self._adtv_cr(ticker, as_of_date),
            ))
        for ticker in sorted(target - self._currently_held):
            conviction = float(momentum.get(ticker, 0.0))
            signals.append(Signal(
                ticker=ticker, action="buy", sector=self._sector_lookup.get(ticker, "Unknown"),
                conviction=conviction, adtv_cr=self._adtv_cr(ticker, as_of_date),
            ))

        self._currently_held = target
        return signals

    def feature_vector(self, ticker: str, as_of_date: date_type) -> Dict[str, Any]:
        return {
            "trailing_momentum": float(self._last_momentum.get(ticker)) if ticker in self._last_momentum.index else None,
            "lookback_days": self.lookback_days,
            "in_top_n": ticker in self._currently_held,
        }
