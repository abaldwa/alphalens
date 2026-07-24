"""
backtest/adapters/technical_adapter.py

Phase: Unified Backtest & Paper Trading Umbrella, Phase 2
Owner: Platform / Backtest
Consumers: backtest/core/engine.py::BacktestOrchestrator

The first real backtest capability for the Technical channel — previously
only backtest/strategy_confidence.py's historical win-rate LOOKUP existed
(not a fold-based backtest with an equity curve). Per
BacktestUmbrellaPlan.md Phase 2: strategy_confidence.py stays in place as
a separate validation cross-check, not replaced.

Reuses the EXISTING, already-materialized daily feature Parquet store
(config.settings.FEATURES_DAILY_DIR, real coverage 2007-01-03 -> today,
4,837 files verified 2026-07-20) via
systems.technical_analysis.screener.engine.ScreenerEngine — the same
engine that backs the live /screener/run/{template_name} endpoint. This
adapter does NOT recompute technical indicators from raw OHLCV itself; it
reads the same daily snapshots the production screener reads, so a
backtested signal is guaranteed to match what the live system would have
shown a user on that date (no drift between backtest and live logic).

Signal semantics mirror momentum_adapter.py's rotation pattern (buy when
a ticker enters the template's top-N matches, sell when it drops out) —
appropriate for screener templates, which re-evaluate every rebalance
date rather than firing a one-shot entry signal. ScreenerEngine itself is
NOT modified (consistent with the "wrap, don't refactor" principle
applied elsewhere in this initiative).
"""

import logging
from datetime import date as date_type
from typing import Any, Dict, List, Optional

from backtest.core.engine import Signal
from backtest.core.horizon import HorizonBucket
from systems.technical_analysis.screener.engine import ScreenerEngine

logger = logging.getLogger(__name__)


class TechnicalAdapter:
    channel = "technical"

    def __init__(
        self, template_name: str, screener_engine: Optional[ScreenerEngine] = None,
        top_n: int = 10, sector_lookup: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        template_name : one of the 42 pre-built screener templates
            (systems/technical_analysis/screener/templates.py TEMPLATE_MAP),
            e.g. "A1", "E2", "S004".
        screener_engine : injected for testability; defaults to a fresh
            ScreenerEngine() reading the real daily feature Parquet store.
        top_n : how many top-scored matches to hold at any time.
        """
        if top_n <= 0:
            raise ValueError("top_n must be positive")
        self.template_name = template_name
        self.top_n = top_n
        self._engine = screener_engine or ScreenerEngine()
        self._sector_lookup = sector_lookup or {}
        self._currently_held: set = set()
        self._last_results: Dict[str, Any] = {}  # ticker -> ScreenerResult, from the most recent generate_signals() call

    def generate_signals(self, universe: List[str], as_of_date: date_type, horizon_bucket: HorizonBucket) -> List[Signal]:
        universe_set = set(universe)
        # Over-fetch (limit=top_n * 5) since results aren't pre-filtered to `universe`
        # (ScreenerEngine screens its whole feature snapshot) — trimmed below.
        results = self._engine.screen(self.template_name, date=str(as_of_date), limit=self.top_n * 5)
        in_universe = [r for r in results if r.ticker in universe_set]
        self._last_results = {r.ticker: r for r in in_universe}

        if not in_universe:
            # No real screener match this date (No-Mock-Data Policy: never
            # fabricate a signal) — nothing new, but existing holdings that
            # dropped out of even the over-fetched result set still get sold.
            target: set = set()
        else:
            ranked = sorted(in_universe, key=lambda r: -r.score)[: self.top_n]
            target = {r.ticker for r in ranked}

        signals: List[Signal] = []
        for ticker in sorted(self._currently_held - target):
            signals.append(Signal(
                ticker=ticker, action="sell", sector=self._sector_lookup.get(ticker, "Unknown"), conviction=0.0,
            ))
        for ticker in sorted(target - self._currently_held):
            result = self._last_results[ticker]
            signals.append(Signal(
                ticker=ticker, action="buy", sector=self._sector_lookup.get(ticker, "Unknown"),
                conviction=result.score, template=self.template_name,
            ))

        self._currently_held = target
        return signals

    def feature_vector(self, ticker: str, as_of_date: date_type) -> Dict[str, Any]:
        result = self._last_results.get(ticker)
        if result is None:
            return {"template_name": self.template_name, "matched": False}
        return {
            "template_name": self.template_name,
            "matched": True,
            "score": result.score,
            "matched_conditions": result.matched_conditions,
            "total_conditions": result.total_conditions,
            **{f"feature__{k}": v for k, v in result.key_values.items()},
        }
