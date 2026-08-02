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

import json
import logging
from datetime import date as date_type
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union

import pandas as pd

from backtest.core.adtv import adtv_cr_for_ticker
from backtest.core.engine import Signal
from backtest.core.horizon import HorizonBucket
from features.momentum_signal import trailing_momentum_from_panel
from systems.technical_analysis.screener.engine import ScreenerEngine, ScreenerResult

logger = logging.getLogger(__name__)


class TechnicalAdapter:
    channel = "technical"

    def __init__(
        self, template_name: str, screener_engine: Optional[ScreenerEngine] = None,
        top_n: int = 10, sector_lookup: Optional[Dict[str, str]] = None,
        screener_cache_conn=None,
        price_panel: Optional[pd.DataFrame] = None, volume_panel: Optional[pd.DataFrame] = None,
        adtv_lookback_days: int = 20,
        min_adtv_cr: Optional[float] = None,
        quality_scores: Optional[Dict[str, Dict[str, float]]] = None,
        quality_gate: Optional[Dict[str, float]] = None,
        downtrend_filter_pct: Optional[float] = None,
        downtrend_lookback_days: int = 20,
        circuit_band_pct: Optional[float] = None,
        regime_conn=None,
        regime_index_name: str = "Nifty 500",
        regime_method: Optional[str] = None,
        disable_buys_in_regime: Optional[Set[str]] = None,
        precomputed_matches_dir: Optional[Union[str, Path]] = None,
    ) -> None:
        """
        template_name : one of the 42 pre-built screener templates
            (systems/technical_analysis/screener/templates.py TEMPLATE_MAP),
            e.g. "A1", "E2", "S004".
        screener_engine : injected for testability; defaults to a fresh
            ScreenerEngine() reading the real daily feature Parquet store.
        top_n : how many top-scored matches to hold at any time.
        screener_cache_conn : optional open DuckDB connection to
            BACKTEST_DUCKDB_PATH (backtest/run_orchestrator_backtest.py
            wires this in for real technical-channel runs). When given,
            entry-signal candidates are read from/written to the
            technical_screener_cache table (backtest/core/screener_cache.py)
            instead of always calling screener_engine.screen() live —
            since screen()'s output for a given (template, date) is exit-
            policy-agnostic, this lets every exit-variant job for the same
            template reuse one computation (2026-07-25 fix, reviewed by
            ml-rigor-reviewer + backtest-reviewer — see FeatureBacklog.md).
            None (the default) preserves the original always-live behavior
            exactly — every existing caller (tests, any adapter constructed
            without this param) is unaffected.

        2026-08-01 (Momentum-parity entry filters, all None/off by default —
        every existing caller is unaffected): the five kwargs below mirror
        backtest/momentum_backtest.py::MomentumBacktester's optional filter
        set (liquidity_floor, quality_gated, circuit_lock_proxy,
        downtrend_filter, regime_conditional) applied here to `in_universe`
        before ranking, instead of to MomentumBacktester's selection_pool —
        same semantics, same "never exclude on missing data" convention.

        min_adtv_cr : optional liquidity floor (crores) — a candidate below
            this trailing average daily traded value (or with no volume
            data) is dropped before ranking.
        quality_scores / quality_gate : same shape as MomentumBacktester's
            (ticker -> {"f_score":..., "m_score":...}, and
            {"min_f_score":..., "max_m_score":...}) — a candidate present in
            quality_scores that fails an explicit threshold is dropped;
            missing-from-quality_scores or no quality_gate set never excludes.
        downtrend_filter_pct / downtrend_lookback_days : a candidate whose
            trailing price return over downtrend_lookback_days is <=
            -downtrend_filter_pct is dropped (reuses
            features.momentum_signal.trailing_momentum_from_panel against
            `price_panel`, same helper MomentumBacktester itself uses).
        circuit_band_pct : a candidate whose latest 1-day return (from
            price_panel) meets/exceeds this magnitude is dropped as a proxy
            for "likely circuit-locked, don't trust this close as fillable".
        regime_conn / regime_index_name / regime_method / disable_buys_in_regime :
            when a date's confirmed regime (systems.regime.regime_store,
            same source backtest/core/engine.py's own
            BacktestOrchestrator._regime_for_date reads — this adapter
            self-fetches rather than requiring a StrategyAdapter protocol
            change to thread regime through generate_signals) is in
            disable_buys_in_regime, no NEW buys are generated this date
            (existing holdings that drop out of the target set still sell
            normally) — mirrors MomentumBacktester's disable_in_regimes.

        precomputed_matches_dir : (2026-08-02, sweep-scale entry-signal
            reuse) directory containing this template's
            scripts/precompute_technical_screener_matches.py output —
            {template_name}.parquet + {template_name}.manifest.json.
            Entry-signal generation for a (template, date) is exit-policy-
            agnostic (same fact screener_cache_conn already exploits
            within one process/DB), so every one of a sweep's ~66 variant
            jobs per template can share ONE precomputed pass instead of
            each independently calling screener_engine.screen() for the
            same dates. Unlike screener_cache_conn (a live DuckDB
            connection — unsafe to hold open during the multi-worker
            parallel compute phase, see run_orchestrator_backtest.py's
            defer_db_writes docstring), this is a plain per-template
            Parquet file — safe for many concurrent job processes to read,
            same pattern already used for FEATURES_DAILY_DIR. Loaded ONCE
            at construction into an in-memory per-date dict. A date inside
            the manifest's covered range is served from this cache
            (including a genuine zero-match day); a date OUTSIDE it falls
            back to live screener_engine.screen()/screener_cache_conn
            exactly as before. None (default) preserves today's
            always-live behavior exactly — every existing caller is
            unaffected.
        """
        if top_n <= 0:
            raise ValueError("top_n must be positive")
        self.template_name = template_name
        self.top_n = top_n
        self._engine = screener_engine or ScreenerEngine()
        self._sector_lookup = sector_lookup or {}
        self._screener_cache_conn = screener_cache_conn
        # [BUG FIX, 4th fundamental-strategies review, item 2] see
        # fundamental_adapter.py's matching comment — same optional
        # price_panel/volume_panel wiring so Signal.adtv_cr is real, not
        # always None, for this channel too.
        # [BUG FIX, 5th fundamental-strategies review, item 3] see the
        # matching note in fundamental_adapter.py — adtv.py's
        # adtv_cr_for_ticker's `.loc[:ts].tail(n)` silently produces the
        # wrong ADTV on an unsorted price_panel; only volume_panel was
        # being sorted here.
        self.price_panel = price_panel.sort_index() if price_panel is not None else None
        self.volume_panel = volume_panel.sort_index() if volume_panel is not None else None
        self.adtv_lookback_days = adtv_lookback_days
        self.min_adtv_cr = min_adtv_cr
        self.quality_scores = quality_scores or {}
        self.quality_gate = quality_gate or {}
        self.downtrend_filter_pct = downtrend_filter_pct
        self.downtrend_lookback_days = downtrend_lookback_days
        self.circuit_band_pct = circuit_band_pct
        self._regime_conn = regime_conn
        self._regime_index_name = regime_index_name
        self._regime_method = regime_method
        self.disable_buys_in_regime = disable_buys_in_regime or set()
        self._regime_segments_cache: Optional[List[dict]] = None
        self._currently_held: set = set()
        self._last_results: Dict[str, Any] = {}  # ticker -> ScreenerResult, from the most recent generate_signals() call
        self._precomputed_by_date, self._precomputed_dates = self._load_precomputed_matches(precomputed_matches_dir)

    def _load_precomputed_matches(
        self, precomputed_matches_dir: Optional[Union[str, Path]],
    ) -> tuple:
        """Loads {template_name}.parquet + .manifest.json once (a few MB
        for one template across 10 years, not the full universe panel) —
        see the precomputed_matches_dir __init__ docstring for the design.
        Missing files (template not yet precomputed) degrade to "nothing
        cached, always fall back to live" rather than raising — a job
        whose template wasn't included in a partial precompute run must
        still work correctly, just without the speedup."""
        if precomputed_matches_dir is None:
            return {}, frozenset()
        base = Path(precomputed_matches_dir)
        parquet_path = base / f"{self.template_name}.parquet"
        manifest_path = base / f"{self.template_name}.manifest.json"
        if not parquet_path.exists() or not manifest_path.exists():
            logger.warning(
                "TechnicalAdapter: no precomputed matches for template %s at %s; "
                "falling back to live screening for every date", self.template_name, base,
            )
            return {}, frozenset()
        manifest = json.loads(manifest_path.read_text())
        dates = frozenset(manifest["trading_days"])
        df = pd.read_parquet(parquet_path)
        by_date: Dict[str, List[Any]] = {d: [] for d in dates}
        for row in df.itertuples(index=False):
            row_date = str(row.date)  # defensive: pyarrow may round-trip a date-like column as Timestamp, not str
            by_date.setdefault(row_date, []).append(
                ScreenerResult(
                    ticker=row.ticker, date=row_date, template_name=self.template_name,
                    matched_conditions=row.matched_conditions, total_conditions=row.total_conditions,
                    score=row.score, key_values=json.loads(row.key_values_json),
                )
            )
        return by_date, dates

    def _adtv_cr(self, ticker: str, as_of_date: date_type) -> Optional[float]:
        return adtv_cr_for_ticker(
            ticker, as_of_date, self.price_panel, self.volume_panel, self.adtv_lookback_days,
        )

    def _passes_quality_gate(self, ticker: str) -> bool:
        """Same logic/convention as MomentumBacktester._passes_quality_gate —
        False only if `ticker` has quality_scores AND fails an explicitly-set
        threshold; never excludes on missing data or an empty quality_gate."""
        if not self.quality_gate:
            return True
        scores = self.quality_scores.get(ticker)
        if scores is None:
            return True
        min_f = self.quality_gate.get("min_f_score")
        if min_f is not None:
            f_score = scores.get("f_score")
            if f_score is not None and f_score < min_f:
                return False
        max_m = self.quality_gate.get("max_m_score")
        if max_m is not None:
            m_score = scores.get("m_score")
            if m_score is not None and m_score > max_m:
                return False
        return True

    def _is_circuit_locked(self, as_of_date: date_type, ticker: str) -> bool:
        """Same proxy as MomentumBacktester._is_circuit_locked, against
        this adapter's own price_panel (not forward-filled here — a missing
        price on either side just means "can't tell", never locked)."""
        if self.circuit_band_pct is None or self.price_panel is None or ticker not in self.price_panel.columns:
            return False
        idx = self.price_panel.index
        ts = pd.Timestamp(as_of_date)
        pos = idx.searchsorted(ts)
        if pos <= 0 or pos >= len(idx) or idx[pos] != ts:
            return False
        prev_price = self.price_panel[ticker].iloc[pos - 1]
        cur_price = self.price_panel[ticker].iloc[pos]
        if pd.isna(prev_price) or pd.isna(cur_price) or prev_price <= 0:
            return False
        ret = (cur_price - prev_price) / prev_price
        return abs(ret) >= self.circuit_band_pct

    def _regime_for_date(self, as_of: date_type) -> Optional[str]:
        """Self-fetched regime lookup, same source/segments shape as
        BacktestOrchestrator._regime_for_date (backtest/core/engine.py) —
        duplicated rather than shared because StrategyAdapter has no
        reference back to the orchestrator instance; both read the same
        systems.regime.regime_store segments so they can never disagree."""
        if self._regime_conn is None:
            return None
        if self._regime_segments_cache is None:
            from systems.regime.market_regime import METHOD_NAME
            from systems.regime.regime_store import list_regime_segments

            try:
                self._regime_segments_cache = list_regime_segments(
                    self._regime_conn, self._regime_index_name,
                    method=self._regime_method or METHOD_NAME,
                )
            except Exception:
                logger.warning("TechnicalAdapter: regime segments unavailable; disable_buys_in_regime inert", exc_info=True)
                self._regime_segments_cache = []
        for seg in self._regime_segments_cache:
            if seg["start_date"] <= as_of <= seg["end_date"]:
                return seg["regime"]
        return None

    def _is_buys_disabled(self, as_of_date: date_type) -> bool:
        if not self.disable_buys_in_regime:
            return False
        regime = self._regime_for_date(as_of_date)
        return regime is not None and regime in self.disable_buys_in_regime

    def _filtered_candidates(self, universe_set: set, as_of_date: date_type) -> List[Any]:
        """Every real screener match for this template/date, restricted to
        `universe_set` and passing this adapter's own entry-side filters —
        i.e. everything generate_signals does UP TO (not including) top_n
        ranking. Extracted (2026-08-01) so TechnicalComboAdapter can pool
        several templates' candidate pools before a single combined ranking,
        without duplicating the fetch/filter logic."""
        date_str = str(as_of_date)
        if date_str in self._precomputed_dates:
            # Precomputed cache (2026-08-02) always wins over
            # screener_cache_conn when both are present — it's the
            # cheaper, already-in-memory path, and covers exactly the same
            # (template, date) key space. A date outside the manifest's
            # range falls through to the branches below unchanged.
            results = self._precomputed_by_date.get(date_str, [])
        elif self._screener_cache_conn is not None:
            from backtest.core.screener_cache import get_or_compute

            # get_or_compute always returns every real full match (never
            # limit-truncated to this adapter's own top_n) — see
            # screener_cache.py's module docstring for why a shared cache
            # must not be scoped to any one job's top_n.
            results = get_or_compute(self._screener_cache_conn, self._engine, self.template_name, date_str)
        else:
            # Over-fetch (limit=top_n * 5) since results aren't pre-filtered to `universe`
            # (ScreenerEngine screens its whole feature snapshot) — trimmed below.
            results = self._engine.screen(self.template_name, date=date_str, limit=self.top_n * 5)
        in_universe = [r for r in results if r.ticker in universe_set]

        # Entry-side filters (2026-08-01, Momentum-parity), applied in the
        # same order MomentumBacktester applies its analogues: liquidity ->
        # circuit-lock proxy -> downtrend -> quality gate -> regime gate.
        if self.min_adtv_cr is not None and in_universe:
            in_universe = [
                r for r in in_universe
                if (adtv := self._adtv_cr(r.ticker, as_of_date)) is not None and adtv >= self.min_adtv_cr
            ]
        if self.circuit_band_pct is not None and in_universe:
            in_universe = [r for r in in_universe if not self._is_circuit_locked(as_of_date, r.ticker)]
        if self.downtrend_filter_pct is not None and in_universe and self.price_panel is not None:
            short_term = trailing_momentum_from_panel(
                self.price_panel, [r.ticker for r in in_universe], date_str, self.downtrend_lookback_days,
            )
            in_universe = [
                r for r in in_universe
                if r.ticker not in short_term.index or short_term[r.ticker] > -self.downtrend_filter_pct
            ]
        if self.quality_gate and in_universe:
            in_universe = [r for r in in_universe if self._passes_quality_gate(r.ticker)]
        return in_universe

    def generate_signals(self, universe: List[str], as_of_date: date_type, horizon_bucket: HorizonBucket) -> List[Signal]:
        universe_set = set(universe)
        in_universe = self._filtered_candidates(universe_set, as_of_date)
        self._last_results = {r.ticker: r for r in in_universe}

        if not in_universe:
            # No real screener match this date (No-Mock-Data Policy: never
            # fabricate a signal) — nothing new, but existing holdings that
            # dropped out of even the over-fetched result set still get sold.
            target: set = set()
        else:
            ranked = sorted(in_universe, key=lambda r: -r.score)[: self.top_n]
            target = {r.ticker for r in ranked}
            if self._is_buys_disabled(as_of_date):
                # Regime-disabled: strip any ticker that would be a NEW buy
                # (not already held) out of target — a currently-held name
                # that fell out of the ranked top_n is still dropped from
                # target (and so still sold normally) exactly as it would
                # be with buys enabled; only new entries are blocked. Same
                # convention as MomentumBacktester's _is_regime_disabled
                # (filters new_entrants, doesn't touch target_set itself).
                target &= self._currently_held

        signals: List[Signal] = []
        for ticker in sorted(self._currently_held - target):
            signals.append(Signal(
                ticker=ticker, action="sell", sector=self._sector_lookup.get(ticker, "Unknown"), conviction=0.0,
                adtv_cr=self._adtv_cr(ticker, as_of_date),
            ))
        for ticker in sorted(target - self._currently_held):
            result = self._last_results[ticker]
            signals.append(Signal(
                ticker=ticker, action="buy", sector=self._sector_lookup.get(ticker, "Unknown"),
                conviction=result.score, template=self.template_name,
                adtv_cr=self._adtv_cr(ticker, as_of_date),
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
