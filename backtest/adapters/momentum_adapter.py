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
results) is NOT modified. This adapter reuses its underlying PURE
functions — features/momentum_signal.py's trailing_momentum_from_panel /
orthogonalize_momentum_vs_factors and momentum_backtest.py's own
decide_grace_transitions (imported, never re-implemented) — rather than
touching MomentumBacktester's class itself.

2026-08-05 (Momentum engine consolidation, Phase 1): every remaining
real MomentumBacktester feature is ported here so the standalone engine
can eventually be retired — grace-period churn reduction, ADTV liquidity
floor, circuit-lock proxy, downtrend filter, quality gate,
regime-conditioning, size/beta orthogonalization,
exclude_approximated_mcap, the min_momentum floor and volume-weighted
position sizing. Every one of them defaults to off/None, so the queue
jobs that already ran through this adapter behave EXACTLY as before
unless a caller opts in. `rebalance_offset_days` is deliberately NOT
ported (a retired overfitting-robustness research knob, not a strategy
feature). SIP cash injection and FY-netted tax are handled generically
by StrategyPortfolio/core/tax.py for every channel and need no
adapter-level logic here.

State note: BacktestOrchestrator's StrategyAdapter protocol has no
portfolio-state parameter — generate_signals() only receives the
universe/date/horizon_bucket. A rank-rotation strategy needs to know
what it currently holds to decide sells (fell out of the top N) vs buys
(entered the top N), so this adapter tracks its own holdings state
(_held_grace: {ticker: grace_remaining}), updated at the end of each
generate_signals() call — valid because BacktestOrchestrator always
calls generate_signals() exactly once per rebalance date, in date order,
and executes the returned signals before the next call (verified in
backtest/core/engine.py's run() loop).
"""

import logging
from datetime import date as date_type
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from backtest.core.engine import Signal
from backtest.core.horizon import HorizonBucket
from backtest.momentum_backtest import decide_grace_transitions
from features.momentum_signal import (
    lookback_trading_days,
    orthogonalize_momentum_vs_factors,
    trailing_momentum_from_panel,
)

logger = logging.getLogger(__name__)


class MomentumAdapter:
    channel = "momentum"

    def __init__(
        self, price_panel: pd.DataFrame, top_n: int = 10, lookback_months: int = 6,
        sector_lookup: Optional[Dict[str, str]] = None, volume_panel: Optional[pd.DataFrame] = None,
        adtv_lookback_days: int = 20,
        grace_cycles: int = 2,
        min_adtv_cr: Optional[float] = None,
        circuit_band_pct: Optional[float] = None,
        downtrend_filter_pct: Optional[float] = None,
        downtrend_lookback_days: int = 20,
        quality_scores: Optional[Dict[str, Dict[str, float]]] = None,
        quality_gate: Optional[Dict[str, float]] = None,
        regime_conn=None,
        regime_index_name: str = "Nifty 500",
        regime_method: Optional[str] = None,
        disable_buys_in_regime: Optional[Set[str]] = None,
        orthogonalize_vs_size_beta: bool = False,
        market_cap_panel: Optional[pd.DataFrame] = None,
        beta_map: Optional[Dict[str, float]] = None,
        exclude_approximated_mcap: bool = False,
        approximation_flags: Optional[Dict[str, Dict[str, bool]]] = None,
        min_momentum: Optional[float] = None,
        volume_weighted: bool = False,
        rank_start: Optional[int] = None,
        yearly_rank_lookup: Optional[Dict[str, Dict[str, int]]] = None,
    ) -> None:
        """
        price_panel : wide DataFrame (date index, ticker columns, close
            prices) — see features/momentum_signal.py::load_price_panel.
            Supplied by the caller; this adapter never queries the DB
            itself for prices (same in-memory design as MomentumBacktester).
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
            instead of silently being bypassed. Also required for
            min_adtv_cr / volume_weighted to do anything.

        Ported from MomentumBacktester (2026-08-05, Phase 1) — all default
        to off, preserving this adapter's prior behavior exactly:

        grace_cycles : churn reduction. A held ticker that drops out of
            the top_n is kept for this many more rebalances before a sell
            Signal is emitted, unless it re-enters first (grace resets, no
            sell/rebuy round trip). Decided by the SHARED pure function
            momentum_backtest.decide_grace_transitions, so this adapter,
            the standalone engine and features/momentum_live.py can never
            drift apart.
        min_adtv_cr / adtv_lookback_days : liquidity FLOOR (distinct from
            core/portfolio.py's ADTV position-SIZE cap, which is generic
            and already applies once Signal.adtv_cr is populated) — a
            ticker whose trailing ADTV is below the floor, or unknown
            (NaN/no volume data), is dropped from the selection pool.
            Never assumed liquid on missing data; never applied to an
            already-held ticker's grace/sell evaluation.
        circuit_band_pct : circuit-lock proxy — a ticker whose realized
            1-day return into this date is >= this magnitude is treated as
            "close probably not fillable" and skipped for BOTH new buys
            and force-sells this rebalance (re-evaluated next call).
        downtrend_filter_pct / downtrend_lookback_days : a ticker whose
            short-window trailing return is <= -downtrend_filter_pct is
            dropped from selection. Insufficient history leaves a ticker
            eligible (never excluded on missing data).
        quality_scores / quality_gate : ticker -> {"f_score","m_score"}
            real forensic scores, screened against {"min_f_score",
            "max_m_score"} thresholds. A ticker missing from
            quality_scores always passes.
        regime_conn / regime_index_name / regime_method /
            disable_buys_in_regime : when the date's confirmed regime
            (systems.regime.regime_store, the same source
            BacktestOrchestrator._regime_for_date reads) is in
            disable_buys_in_regime, NEW buys are suppressed this date;
            existing holdings still run grace/sell normally
            (skip-don't-force-liquidate, same as circuit-lock). Uses the
            self-fetched regime_conn pattern TechnicalAdapter already
            established rather than an injected regime Series, so both
            channels condition on one identical regime source.
        orthogonalize_vs_size_beta / market_cap_panel / beta_map : rank on
            the cross-sectional residual of momentum after regressing out
            log(market_cap) and a beta proxy, instead of raw momentum
            (features.momentum_signal.orthogonalize_momentum_vs_factors,
            unchanged). Silently no-ops without market_cap_panel.
        exclude_approximated_mcap / approximation_flags : drop tickers
            whose rank-band membership on the active year_start came from
            an approximated shares-outstanding fallback.
            approximation_flags is {first_trading_day_of_year: {ticker:
            bool}}, the shape
            features.momentum_universe.yearly_band_approximation_flags_from_rankings
            already produces.
        min_momentum : only tickers scoring STRICTLY above this floor are
            eligible for the target set — a rebalance may end up holding
            fewer than top_n names rather than padding with a
            below-floor pick.
        volume_weighted : scale each new buy's position size by that
            ticker's trailing ADTV relative to the target set's mean ADTV
            (mirrors MomentumBacktester._volume_weights), carried to the
            orchestrator on Signal.size_multiplier. Falls back to equal
            weight with a one-time warning when volume_panel is missing.

        rank_start / yearly_rank_lookup : (2026-08-05, Momentum engine
            consolidation Phase 3) the "sticky-promotion" rule. With a
            rank-band universe (features.momentum_universe), each year's
            band membership is fixed on the first trading day of the year,
            so a holding that GREW out of its band — promoted to a
            smaller-numbered, higher-market-cap band — silently vanishes
            from `universe` at the next year boundary and gets force-sold
            by grace expiry, purely for having done well. That's an
            artifact of how the universe is sliced, not a strategy
            decision. When both are supplied, a currently-held (or
            in-grace) ticker whose rank on the active year_start is
            STRICTLY better (smaller) than this adapter's own rank_start
            is re-added to the ranking pool, so it competes on real
            momentum and exits only through the normal Exit Criteria
            (falls out of top_n, then exhausts grace).

            Deliberately asymmetric: a DEMOTED holding (worse-or-equal
            rank) and one with no rank at all (dropped out of the tracked
            universe / delisted) get NO special treatment — they fall
            through to the unchanged grace-then-sell path. And because
            only tickers already in _held_grace are ever added, a promoted
            ticker that is not held (including one that already fully
            exited) can never be bought back in through this path.

            rank_start is this adapter instance's band start (e.g. 51 for
            RANK_BANDS band 2). yearly_rank_lookup is
            {first_trading_day_of_year: {ticker: rank}}, the shape
            features.momentum_universe.yearly_rank_lookup_from_rankings
            produces — the FULL ranking, not the band slice, since the
            ranks that matter here are the ones above the band.
            Both None (the default) is today's unchanged behavior.
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
        self.grace_cycles = grace_cycles
        self.min_adtv_cr = min_adtv_cr
        self.circuit_band_pct = circuit_band_pct
        self.downtrend_filter_pct = downtrend_filter_pct
        self.downtrend_lookback_days = downtrend_lookback_days
        self.quality_scores = quality_scores or {}
        self.quality_gate = quality_gate or {}
        self._regime_conn = regime_conn
        self._regime_index_name = regime_index_name
        self._regime_method = regime_method
        self.disable_buys_in_regime = disable_buys_in_regime or set()
        self._regime_segments_cache: Optional[List[dict]] = None
        self.orthogonalize_vs_size_beta = orthogonalize_vs_size_beta
        self.market_cap_panel = market_cap_panel.sort_index() if market_cap_panel is not None else None
        self.beta_map = beta_map or {}
        self.exclude_approximated_mcap = exclude_approximated_mcap
        self.approximation_flags = {
            pd.Timestamp(k): v for k, v in (approximation_flags or {}).items()
        }
        self.min_momentum = min_momentum
        self.volume_weighted = volume_weighted
        self.rank_start = rank_start
        self.yearly_rank_lookup = {
            pd.Timestamp(k): v for k, v in (yearly_rank_lookup or {}).items()
        }
        self._volume_weighted_fallback_warned = False
        # {ticker: grace_remaining} — None = "core" holding (in the latest
        # target set), an int = rebalances left before a forced sell.
        self._held_grace: Dict[str, Optional[int]] = {}
        self._last_momentum: pd.Series = pd.Series(dtype=float)

    @property
    def _currently_held(self) -> set:
        """Every ticker this adapter believes it holds, core or in-grace.
        Kept as a read-only view over _held_grace so callers/tests that
        only care about membership (and feature_vector below) don't have
        to know about grace bookkeeping."""
        return set(self._held_grace)

    # ===== liquidity =====
    def _adtv_series(self, tickers: List[str], as_of_date: date_type) -> pd.Series:
        """Trailing adtv_lookback_days-day average daily traded value (INR
        crore) per ticker — price(t) * volume(t), no forward-fill, NaN days
        simply don't contribute to the mean. Same formula (and same
        never-assume-liquid-on-missing-data handling) as
        MomentumBacktester._adtv_cr, vectorized across the pool."""
        if self.volume_panel is None or self.price_panel is None or not tickers:
            return pd.Series(dtype=float)
        cols = [t for t in tickers if t in self.volume_panel.columns and t in self.price_panel.columns]
        if not cols:
            return pd.Series(dtype=float)
        ts = pd.Timestamp(as_of_date)
        window_prices = self.price_panel[cols].loc[:ts].tail(self.adtv_lookback_days)
        window_volume = self.volume_panel[cols].loc[:ts].tail(self.adtv_lookback_days)
        return ((window_prices * window_volume) / 1e7).mean(skipna=True)

    def _adtv_cr(self, ticker: str, as_of_date: date_type) -> Optional[float]:
        """Single-ticker ADTV, or None if volume_panel wasn't supplied or
        the ticker/date has no real data — never fabricated."""
        series = self._adtv_series([ticker], as_of_date)
        if ticker not in series.index:
            return None
        value = series[ticker]
        return float(value) if pd.notna(value) else None

    def _volume_weights(self, target: Set[str], as_of_date: date_type) -> Optional[Dict[str, float]]:
        """Per-ticker size multiplier for volume_weighted=True:
        adtv_ticker / mean(adtv over target), so an average-liquidity name
        gets 1.0 (identical to equal weighting). None (equal weight
        everywhere) when there's no usable volume data — mirrors
        MomentumBacktester._volume_weights, including its one-time warning."""
        if self.volume_panel is None:
            if not self._volume_weighted_fallback_warned:
                logger.warning(
                    "MomentumAdapter: volume_weighted=True but no volume_panel was supplied — "
                    "falling back to equal-weighted position sizing."
                )
                self._volume_weighted_fallback_warned = True
            return None
        adtv = self._adtv_series(sorted(target), as_of_date)
        adtv = adtv[adtv.notna() & (adtv > 0)]
        if adtv.empty:
            return None
        return (adtv / adtv.mean()).to_dict()

    # ===== entry filters, all ported verbatim from MomentumBacktester =====
    def _passes_quality_gate(self, ticker: str) -> bool:
        """False only if `ticker` has quality_scores AND fails an
        explicitly-set threshold. True for a ticker missing from
        quality_scores entirely, or when quality_gate is empty — never
        excludes on missing data."""
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
        """True if `ticker`'s realized 1-day return into `as_of_date` meets
        or exceeds circuit_band_pct in either direction — a coarse proxy
        for "likely circuit-locked, don't trust this close as a fillable
        price" (real bands vary 5/10/20% by tier). False on insufficient
        history: missing data never locks a ticker."""
        if self.circuit_band_pct is None or self.price_panel is None or ticker not in self.price_panel.columns:
            return False
        idx = self.price_panel.index
        ts = pd.Timestamp(as_of_date)
        pos = idx.searchsorted(ts)
        if pos <= 0 or pos >= len(idx):
            return False
        prev_price = self.price_panel[ticker].iloc[pos - 1]
        cur_price = self.price_panel[ticker].iloc[pos]
        if pd.isna(prev_price) or pd.isna(cur_price) or prev_price <= 0:
            return False
        return abs((cur_price - prev_price) / prev_price) >= self.circuit_band_pct

    def _regime_for_date(self, as_of: date_type) -> Optional[str]:
        """Self-fetched regime lookup, same source/segments shape as
        BacktestOrchestrator._regime_for_date and TechnicalAdapter's own —
        duplicated rather than shared because StrategyAdapter has no
        reference back to the orchestrator instance; all three read the
        same systems.regime.regime_store segments so they can never
        disagree."""
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
                logger.warning("MomentumAdapter: regime segments unavailable; disable_buys_in_regime inert", exc_info=True)
                self._regime_segments_cache = []
        from systems.regime.regime_store import regime_known_as_of

        return regime_known_as_of(self._regime_segments_cache, as_of)

    def _is_buys_disabled(self, as_of_date: date_type) -> bool:
        if not self.disable_buys_in_regime:
            return False
        regime = self._regime_for_date(as_of_date)
        return regime is not None and regime in self.disable_buys_in_regime

    def _selection_pool(self, momentum: pd.Series, as_of_date: date_type) -> pd.Series:
        """Momentum scores restricted to tickers eligible to be SELECTED
        into the target set this rebalance — filters applied in exactly the
        order MomentumBacktester.run() applies them (quality -> approximated
        mcap -> orthogonalization -> liquidity -> circuit-lock -> downtrend
        -> min_momentum). Deliberately never consulted when deciding an
        already-held ticker's grace/sell status: a filter must not force a
        liquidation, only block a new entry."""
        pool = momentum

        if self.quality_gate and not pool.empty:
            failing = [t for t in pool.index if not self._passes_quality_gate(t)]
            pool = pool.drop(index=failing)

        if self.exclude_approximated_mcap and self.approximation_flags and not pool.empty:
            applicable_starts = [d for d in self.approximation_flags if d <= pd.Timestamp(as_of_date)]
            if applicable_starts:
                flags = self.approximation_flags[max(applicable_starts)]
                pool = pool.drop(index=[t for t in pool.index if flags.get(t)])

        # Rank/select on the residual after regressing out size/beta, not
        # raw momentum. orthogonalize_momentum_vs_factors itself no-ops
        # (returns fewer/no rows) when too few tickers have both values;
        # those keep their raw score rather than being silently excluded.
        if self.orthogonalize_vs_size_beta and self.market_cap_panel is not None and not pool.empty:
            mcap_history = self.market_cap_panel.loc[:pd.Timestamp(as_of_date)]
            mcap_row = mcap_history.iloc[-1] if not mcap_history.empty else pd.Series(dtype=float)
            mcap_for_pool = mcap_row.reindex(pool.index)
            beta_for_pool = pd.Series(
                {t: self.beta_map[t] for t in pool.index if t in self.beta_map}, dtype=float
            ).reindex(pool.index)
            residual = orthogonalize_momentum_vs_factors(pool, mcap_for_pool, beta_for_pool)
            pool = residual.combine_first(pool)

        if self.min_adtv_cr is not None and not pool.empty:
            adtv = self._adtv_series(list(pool.index), as_of_date).reindex(pool.index)
            illiquid = adtv[adtv.isna() | (adtv < self.min_adtv_cr)].index
            pool = pool.drop(index=[t for t in illiquid if t in pool.index])

        if self.circuit_band_pct is not None and not pool.empty:
            pool = pool.drop(index=[t for t in pool.index if self._is_circuit_locked(as_of_date, t)])

        if self.downtrend_filter_pct is not None and not pool.empty:
            short_term = trailing_momentum_from_panel(
                self.price_panel, list(pool.index), str(as_of_date), self.downtrend_lookback_days,
            )
            sharply_down = short_term[short_term <= -self.downtrend_filter_pct].index
            pool = pool.drop(index=[t for t in sharply_down if t in pool.index])

        if self.min_momentum is not None and not pool.empty:
            pool = pool[pool > self.min_momentum]

        return pool

    def _sticky_promoted_holdings(self, universe: List[str], as_of_date: date_type) -> List[str]:
        """Currently-held (or in-grace) tickers that have been PROMOTED out
        of this adapter's band and so are missing from `universe`, but
        should still be ranked — see the rank_start/yearly_rank_lookup
        docstring. Empty (this rule fully off) unless both were supplied.

        Only reads self._held_grace, never `target` or the price panel, so
        by construction this can never introduce a ticker that isn't
        already held — a promoted name that was never bought, or that has
        already fully exited, is not in _held_grace and therefore not
        returned here."""
        if self.rank_start is None or not self.yearly_rank_lookup or not self._held_grace:
            return []
        applicable_starts = [d for d in self.yearly_rank_lookup if d <= pd.Timestamp(as_of_date)]
        if not applicable_starts:
            return []
        ranks = self.yearly_rank_lookup[max(applicable_starts)]
        in_universe = set(universe)
        promoted = []
        for ticker in self._held_grace:
            if ticker in in_universe:
                continue
            rank = ranks.get(ticker)
            # No rank at all (left the tracked universe/delisted) or a
            # worse-or-equal rank (demoted) => no special treatment.
            if rank is not None and rank < self.rank_start:
                promoted.append(ticker)
        return promoted

    def generate_signals(self, universe: List[str], as_of_date: date_type, horizon_bucket: HorizonBucket) -> List[Signal]:
        # Sticky-promotion (Phase 3): extend the ranking pool BEFORE
        # momentum is computed, so a promoted holding's score is real and
        # it genuinely competes for a top_n slot rather than being scored
        # after the cut. No-op unless rank_start + yearly_rank_lookup were
        # both supplied.
        sticky = self._sticky_promoted_holdings(universe, as_of_date)
        if sticky:
            universe = list(universe) + sorted(sticky)
        momentum = trailing_momentum_from_panel(
            self.price_panel, universe, str(as_of_date), self.lookback_days
        )
        self._last_momentum = momentum
        if momentum.empty:
            # No fabricated ranking when there isn't enough real history yet
            # (No-Mock-Data Policy) — just hold whatever's already held.
            return []

        pool = self._selection_pool(momentum, as_of_date)
        target = (
            set(pool.sort_values(ascending=False).head(self.top_n).index)
            if not pool.empty else set()
        )

        # Grace bookkeeping BEFORE emitting anything: a held ticker outside
        # `target` only becomes a sell once its grace is exhausted.
        updated_grace = decide_grace_transitions(self._held_grace, target, self.grace_cycles)
        buys_disabled = self._is_buys_disabled(as_of_date)

        signals: List[Signal] = []
        new_held: Dict[str, Optional[int]] = {}

        for ticker in sorted(updated_grace):
            grace_remaining = updated_grace[ticker]
            if grace_remaining is not None and grace_remaining <= 0:
                if self._is_circuit_locked(as_of_date, ticker):
                    # Left open at an unfillable close and re-evaluated next
                    # rebalance (grace stays exhausted, so it sells then).
                    new_held[ticker] = grace_remaining
                    continue
                signals.append(Signal(
                    ticker=ticker, action="sell", sector=self._sector_lookup.get(ticker, "Unknown"),
                    conviction=0.0, adtv_cr=self._adtv_cr(ticker, as_of_date),
                ))
                continue
            new_held[ticker] = grace_remaining

        new_entrants = [] if buys_disabled else sorted(target - set(self._held_grace))
        volume_weights = self._volume_weights(target, as_of_date) if self.volume_weighted else None
        for ticker in new_entrants:
            if self._is_circuit_locked(as_of_date, ticker):
                # Skipped this rebalance only; target is recomputed fresh
                # each call, so it's naturally reconsidered next time.
                continue
            signals.append(Signal(
                ticker=ticker, action="buy", sector=self._sector_lookup.get(ticker, "Unknown"),
                conviction=float(momentum.get(ticker, 0.0)), adtv_cr=self._adtv_cr(ticker, as_of_date),
                size_multiplier=(volume_weights or {}).get(ticker),
            ))
            new_held[ticker] = None

        self._held_grace = new_held
        return signals

    def feature_vector(self, ticker: str, as_of_date: date_type) -> Dict[str, Any]:
        return {
            "trailing_momentum": float(self._last_momentum.get(ticker)) if ticker in self._last_momentum.index else None,
            "lookback_days": self.lookback_days,
            "in_top_n": ticker in self._held_grace and self._held_grace[ticker] is None,
            "grace_remaining": self._held_grace.get(ticker),
        }
