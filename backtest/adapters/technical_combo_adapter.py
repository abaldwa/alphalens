"""
backtest/adapters/technical_combo_adapter.py

Phase: Technical Analysis Momentum-parity backtest reporting (2026-08-01)
Owner: Platform / Backtest
Consumers: backtest/core/engine.py::BacktestOrchestrator,
    scripts/run_technical_recommended_strategies.py

2026-08-01 user request: "Combination of strategies to be tested" — a
strategy that pools candidate matches from 2+ screener templates into one
ranked selection, instead of testing each template in isolation. Wraps N
underlying TechnicalAdapter instances (reusing each one's own
_filtered_candidates() — entry-side filters and all — rather than
duplicating that fetch/filter logic) and pools their per-date candidate
pools before applying ONE combined top_n ranking.

Same StrategyAdapter protocol as TechnicalAdapter (channel,
generate_signals, feature_vector) — plugs into BacktestOrchestrator
unmodified, no engine changes needed.

Ranking across templates with different score scales: each sub-adapter's
ScreenerResult.score is normalized to a 0-1 percentile WITHIN that
template's own candidate pool for this date, before combining — otherwise
a template whose score scale happens to run higher would systematically
dominate the combined ranking regardless of genuine match quality. A
ticker matched by more than one template keeps its single best
normalized score (not summed) — being flagged by two templates makes a
ticker a candidate at all, it doesn't entitle it to double weight.
"""

from datetime import date as date_type
from typing import Any, Dict, List

from backtest.adapters.technical_adapter import TechnicalAdapter
from backtest.core.engine import Signal
from backtest.core.horizon import HorizonBucket


class TechnicalComboAdapter:
    channel = "technical"

    def __init__(self, adapters: List[TechnicalAdapter], top_n: int = 10) -> None:
        """
        adapters : 2+ already-constructed TechnicalAdapter instances (each
            with its own template_name, entry filters, price/volume panels,
            screener_cache_conn, etc. already wired) — this class only
            pools their candidate output, it never constructs or configures
            a sub-adapter itself.
        top_n : how many top-scored (combined-ranked) matches to hold.
        """
        if len(adapters) < 2:
            raise ValueError("TechnicalComboAdapter needs at least 2 underlying adapters")
        if top_n <= 0:
            raise ValueError("top_n must be positive")
        self.adapters = adapters
        self.top_n = top_n
        self.combo_name = "+".join(a.template_name for a in adapters)
        self._currently_held: set = set()
        self._last_results: Dict[str, Any] = {}  # ticker -> (source_template, ScreenerResult)

    def generate_signals(self, universe: List[str], as_of_date: date_type, horizon_bucket: HorizonBucket) -> List[Signal]:
        universe_set = set(universe)
        pooled: Dict[str, Any] = {}  # ticker -> best (normalized_score, template_name, result)
        for adapter in self.adapters:
            candidates = adapter._filtered_candidates(universe_set, as_of_date)
            if not candidates:
                continue
            scores = [c.score for c in candidates]
            lo, hi = min(scores), max(scores)
            spread = hi - lo
            for c in candidates:
                norm = (c.score - lo) / spread if spread > 0 else 1.0
                existing = pooled.get(c.ticker)
                if existing is None or norm > existing[0]:
                    pooled[c.ticker] = (norm, adapter.template_name, c)

        self._last_results = {t: (tmpl, r) for t, (norm, tmpl, r) in pooled.items()}

        if not pooled:
            target: set = set()
        else:
            ranked = sorted(pooled.items(), key=lambda kv: -kv[1][0])[: self.top_n]
            target = {ticker for ticker, _ in ranked}

        signals: List[Signal] = []
        for ticker in sorted(self._currently_held - target):
            signals.append(Signal(ticker=ticker, action="sell", conviction=0.0))
        for ticker in sorted(target - self._currently_held):
            norm, tmpl, _result = pooled[ticker]
            signals.append(Signal(
                ticker=ticker, action="buy", conviction=norm * 100, template=self.combo_name,
            ))

        self._currently_held = target
        return signals

    def feature_vector(self, ticker: str, as_of_date: date_type) -> Dict[str, Any]:
        entry = self._last_results.get(ticker)
        if entry is None:
            return {"combo_name": self.combo_name, "matched": False}
        source_template, result = entry
        return {
            "combo_name": self.combo_name,
            "source_template": source_template,
            "matched": True,
            "score": result.score,
            "matched_conditions": result.matched_conditions,
            "total_conditions": result.total_conditions,
            **{f"feature__{k}": v for k, v in result.key_values.items()},
        }
