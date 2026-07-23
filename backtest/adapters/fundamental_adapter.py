"""
backtest/adapters/fundamental_adapter.py

Phase: Unified Backtest & Paper Trading Umbrella, Phase 2
Owner: Platform / Backtest
Consumers: backtest/core/engine.py::BacktestOrchestrator

The first backtest capability the Fundamental channel has ever had — no
prior module (backtest.py-adjacent or otherwise) existed for it.

Reuses the same daily feature Parquet store technical_adapter.py reads
(config.settings.FEATURES_DAILY_DIR, via datastore/api/utils/feature_store
.read_feature_day) and the real screener-preset logic already backing the
live GET /api/v1/fundamental/screener endpoint
(features/fundamental_composites.py::matches_screener_preset,
SCREENER_PRESETS — quality_compounder / garp / turnaround). Neither is
modified.

Real-data caveat this adapter does NOT need to re-litigate: raw
fundamentals coverage is near-empty before 2020 (BacktestUmbrellaPlan.md
Known Data Gaps #1), so ratio z-score columns in the feature Parquet will
mostly be NaN before then — matches_screener_preset already treats a
missing input as "conservatively fails the screen," and
backtest.core.run_context.BacktestRun independently hard-blocks any
channel="fundamental" run starting before 2020-01-01. This adapter relies
on both of those rather than adding its own date-gating logic.

Signal semantics mirror technical_adapter.py/momentum_adapter.py's
rotation pattern: hold while a ticker keeps matching the preset, sell
when it stops. Fundamental screens are naturally lower-turnover (ratios
move slowly, one filing at a time) so this will churn far less than
Technical/Momentum in practice — a property of the data, not something
this adapter special-cases.
"""

import logging
from datetime import date as date_type
from typing import Any, Dict, List, Optional

import pandas as pd

from backtest.core.engine import Signal
from backtest.core.horizon import HorizonBucket
from datastore.api.utils.feature_store import read_feature_day
from features.fundamental_composites import SCREENER_PRESETS, matches_screener_preset

logger = logging.getLogger(__name__)

RATIO_FEATURES = ("roe", "roce", "debt_to_equity", "revenue_growth_yoy", "pe_ratio", "eps_growth_yoy")


class FundamentalAdapter:
    channel = "fundamental"

    def __init__(
        self, preset: str, top_n: int = 10, sector_lookup: Optional[Dict[str, str]] = None,
    ) -> None:
        if preset not in SCREENER_PRESETS:
            raise ValueError(f"Unknown screener preset {preset!r}. Valid: {list(SCREENER_PRESETS.keys())}")
        if top_n <= 0:
            raise ValueError("top_n must be positive")
        self.preset = preset
        self.top_n = top_n
        self._sector_lookup = sector_lookup or {}
        self._currently_held: set = set()
        self._last_ratios: Dict[str, Dict[str, float]] = {}  # ticker -> ratio dict, from the most recent call

    def generate_signals(self, universe: List[str], as_of_date: date_type, horizon_bucket: HorizonBucket) -> List[Signal]:
        universe_set = set(universe)
        panel = read_feature_day(str(as_of_date))
        self._last_ratios = {}

        if panel is None:
            # No materialized feature snapshot for this date (No-Mock-Data
            # Policy: never fabricate one) — nothing new matches, but
            # existing holdings are still re-evaluated against an empty
            # match set below, so they get sold rather than held forever
            # on stale information.
            matched: List[str] = []
        else:
            in_universe = panel[panel["ticker"].isin(universe_set)]
            matched = []
            for _, row in in_universe.iterrows():
                ratios = {c: row.get(c) for c in RATIO_FEATURES if c in in_universe.columns}
                if matches_screener_preset(ratios, self.preset):
                    matched.append(row["ticker"])
                    self._last_ratios[row["ticker"]] = ratios

        target = set(matched[: self.top_n]) if len(matched) <= self.top_n else set(
            sorted(matched, key=lambda t: -_composite_strength(self._last_ratios[t]))[: self.top_n]
        )

        signals: List[Signal] = []
        for ticker in sorted(self._currently_held - target):
            signals.append(Signal(
                ticker=ticker, action="sell", sector=self._sector_lookup.get(ticker, "Unknown"), conviction=0.0,
            ))
        for ticker in sorted(target - self._currently_held):
            signals.append(Signal(
                ticker=ticker, action="buy", sector=self._sector_lookup.get(ticker, "Unknown"),
                conviction=_composite_strength(self._last_ratios[ticker]),
            ))

        self._currently_held = target
        return signals

    def feature_vector(self, ticker: str, as_of_date: date_type) -> Dict[str, Any]:
        ratios = self._last_ratios.get(ticker)
        if ratios is None:
            return {"preset": self.preset, "matched": False}
        return {"preset": self.preset, "matched": True, **{f"ratio__{k}": v for k, v in ratios.items()}}


def _composite_strength(ratios: Dict[str, float]) -> float:
    """Sum of sign-adjusted z-scores actually used by the preset, for
    ranking matched candidates when more than top_n qualify — matched
    tickers are all real screener passes, this only orders among them."""
    values = [v for v in ratios.values() if v is not None and not pd.isna(v)]
    return sum(values) if values else 0.0
