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
orthogonalize_momentum_vs_factors — rather than touching
MomentumBacktester's class itself.

[2026-08-18, user decision] Pure-play momentum is a plain rank rotation:
rank the band, hold the top_n, and on each rebalance sell what left the list
and buy what entered it. Seven knobs were deprecated with that decision and
are gone from this adapter — grace cycles, the asymmetric exit band
(exit_rank), ADTV-capped sizing, trailing stops, per-ticker HMM regime, the
min_momentum floor and volume-weighted sizing. What remains is the buy-side
filter chain (liquidity floor, circuit-lock proxy, downtrend, quality gate,
regime-conditioning, size/beta orthogonalization, exclude_approximated_mcap),
all still default-off, plus sticky promotion.

Momentum strategies under the TECHNICAL umbrella are unaffected: they are
Technical strategies, they use technical indicators, and they run through
TechnicalAdapter. This class is only for the pure-play band strategies.

SIP cash injection and FY-netted tax are handled generically by
StrategyPortfolio/core/tax.py for every channel and need no adapter-level
logic here.

State note: BacktestOrchestrator's StrategyAdapter protocol has no
portfolio-state parameter — generate_signals() only receives the
universe/date/horizon_bucket. A rank-rotation strategy needs to know
what it currently holds to decide sells (fell out of the top N) vs buys
(entered the top N), so this adapter tracks its own holdings state
(_held: the ticker set), updated at the end of each
generate_signals() call — valid because BacktestOrchestrator always
calls generate_signals() exactly once per rebalance date, in date order,
and executes the returned signals before the next call (verified in
backtest/core/engine.py's run() loop).
"""

import logging
from datetime import date as date_type
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from backtest.adapters.panel_filters import adtv_series, is_circuit_locked
from backtest.core.engine import Signal
from backtest.core.horizon import HorizonBucket
from features.momentum_signal import (
    lookback_trading_days, pct_of_52wk_high, crash_regime_detector,
)
from features.momentum_strategy import (
    rank_universe,
    select_buy_pool,
    sticky_promoted_holdings,
    rank_fn_for_skip_months,
    rank_sectors,
    rank_constituents_within_sectors,
)

logger = logging.getLogger(__name__)


class MomentumAdapter:
    channel = "momentum"

    def __init__(
        self, price_panel: pd.DataFrame, top_n: int = 10, lookback_months: int = 6,
        sector_lookup: Optional[Dict[str, str]] = None, volume_panel: Optional[pd.DataFrame] = None,
        adtv_lookback_days: int = 20,
        min_adtv_cr: Optional[float] = None,
        circuit_band_pct: Optional[float] = None,
        downtrend_filter_pct: Optional[float] = None,
        downtrend_lookback_days: int = 20,
        quality_scores: Optional[Dict[str, Dict[str, float]]] = None,
        quality_gate: Optional[Dict[str, float]] = None,
        regime_conn: Optional[Any] = None,
        regime_index_name: str = "Nifty 500",
        regime_method: Optional[str] = None,
        disable_buys_in_regime: Optional[Set[str]] = None,
        orthogonalize_vs_size_beta: bool = False,
        market_cap_panel: Optional[pd.DataFrame] = None,
        beta_map: Optional[Dict[str, float]] = None,
        exclude_approximated_mcap: bool = False,
        approximation_flags: Optional[Dict[str, Dict[str, bool]]] = None,
        rank_start: Optional[int] = None,
        yearly_rank_lookup: Optional[Dict[str, Dict[str, int]]] = None,
        # R-family strategy dispatch params (all default-off for backward compatibility)
        rank_method: str = "trailing_return",
        skip_months: int = 0,
        rank_fn: Optional[Any] = None,
        top_sectors: int = 5,
        volatility_measure: str = "daily_return_stddev",
        # Phase 7: crash-aware momentum overlay (all default-off)
        crash_regime_enabled: bool = False,
        drawdown_threshold: float = -0.15,
        vol_percentile_threshold: float = 0.75,
        vol_lookback_days: int = 20,
        crash_disable_buys: bool = True,
        crash_reduce_sizing: Optional[float] = None,
        # Phase 8: volatility-managed momentum overlay (all default-off)
        vol_target_enabled: bool = False,
        vol_target_pct: float = 0.15,
        vol_target_lookback_days: int = 126,
        vol_target_leverage_cap: float = 1.0,
        # Phase 9: factor volatility scaling (R9, all default-off)
        vol_scaling_mode: Optional[str] = None,
        vol_scaling_lookback_days: int = 126,
        vol_scaling_leverage_cap: Optional[float] = None,
        # Phase R0: per-ticker volatility weighting (all default-off; replaces
        # equal-weight sizing with a within-basket re-weighting — orthogonal
        # to vol_scaling_mode above, which scales the WHOLE portfolio).
        weight_method: Optional[str] = None,
        weight_lookback_days: int = 126,
        # B-027: Regime-switching for vol-scaling strategies
        regime_switching_enabled: bool = False,
        rank_band_id: Optional[int] = None,
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
            min_adtv_cr to do anything.

        The buy-side filter chain, all default-off (2026-08-18: retained;
        the seven knobs listed in the module docstring were deprecated):

        min_adtv_cr / adtv_lookback_days : liquidity FLOOR (distinct from
            core/portfolio.py's ADTV position-SIZE cap, which is generic
            and already applies once Signal.adtv_cr is populated) — a
            ticker whose trailing ADTV is below the floor, or unknown
            (NaN/no volume data), is dropped from the selection pool.
            Never assumed liquid on missing data; never applied to an
            already-held ticker's sell evaluation.
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
            existing holdings still sell normally
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
        rank_start / yearly_rank_lookup : (2026-08-05, Momentum engine
            consolidation Phase 3) the "sticky-promotion" rule. With a
            rank-band universe (features.momentum_universe), each year's
            band membership is fixed on the first trading day of the year,
            so a holding that GREW out of its band — promoted to a
            smaller-numbered, higher-market-cap band — silently vanishes
            from `universe` at the next year boundary and gets force-sold
            purely for having done well. That's an artifact of how the
            universe is sliced, not a strategy decision. When both are
            supplied, a currently-held ticker whose rank on the active
            year_start is STRICTLY better (smaller) than this adapter's own
            rank_start is re-added to the ranking pool, so it competes on
            real momentum and exits only by falling out of the top_n.

            [2026-08-18] Promotion is judged on MARKET-CAP rank only, never
            on ADTV rank: a name that falls out of the liquid universe is
            sold, because liquidity is a tradability constraint rather than
            a ranking artifact.

            Deliberately asymmetric: a DEMOTED holding (worse-or-equal
            rank) and one with no rank at all (dropped out of the tracked
            universe / delisted) get NO special treatment — they fall
            through to the unchanged sell path. And because
            only tickers already held are ever added, a promoted
            ticker that is not held (including one that already fully
            exited) can never be bought back in through this path.

            rank_start is this adapter instance's band start (e.g. 51 for
            RANK_BANDS band 2). yearly_rank_lookup is
            {first_trading_day_of_year: {ticker: rank}}, the shape
            features.momentum_universe.yearly_rank_lookup_from_rankings
            produces — the FULL ranking, not the band slice, since the
            ranks that matter here are the ones above the band.
            Both None (the default) is today's unchanged behavior.

        Phase 9 (R9): factor volatility-managed overlays (Moreira-Muir framework):
        vol_scaling_mode : Optional[str]
            When set to one of ("inverse_volatility", "inverse_variance",
            "target_volatility", "downside_volatility"), applies a scaling
            multiplier to position sizing based on realized portfolio volatility.
            None (default) = no scaling. vol_target_enabled takes precedence if
            both vol_scaling_mode and vol_target_enabled are set (backward-compat).
        vol_scaling_lookback_days : int
            Rolling window for realized vol computation (e.g., 126 = ~6 months).
            Ignored if vol_scaling_mode is None.
        vol_scaling_leverage_cap : Optional[float]
            Maximum exposure multiplier. When None, mode-dependent defaults apply:
            - "inverse_volatility" / "inverse_variance" / "downside_volatility" → 2.0
            - "target_volatility" → 1.0

        Phase R0: per-ticker volatility weighting (replaces equal-weight
        sizing within the basket; orthogonal to vol_scaling_mode above,
        which scales total portfolio exposure rather than re-weighting
        within it):
        weight_method : Optional[str]
            One of ("baseline", "inverse_volatility", "inverse_variance",
            "target_volatility", "downside_volatility") — see
            features/volatility_scaling.py's WEIGHT_DISPATCH. None
            (default) = plain equal-weight (same as "baseline"). Computed
            fresh at each rebalance date from the basket's own price
            history — never precomputed for the whole backtest range.
        weight_lookback_days : int
            Rolling window for the per-ticker volatility computation
            (default 126 = ~6 months). Ignored if weight_method is None.

        B-027: Regime-switching for vol-scaling strategies (Moreira-Muir adaptive):
        regime_switching_enabled : bool
            When True, select vol_scaling_mode dynamically based on market regime.
            Bull → inverse_volatility (aggressive), Bear → downside_volatility
            (conservative), Choppy → target_volatility (balanced).
            Only active if vol_scaling_mode is set (default False).
        rank_band_id : Optional[int]
            Market-cap band identifier (1-5 corresponding to Nifty 50, Next 50,
            Nifty 150, Smallcap 250, Microcap) used to select market-cap-specific
            regime detector. Required if regime_switching_enabled=True.
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
        self._regime_segments_cache: Optional[List[Dict[str, Any]]] = None
        self.orthogonalize_vs_size_beta = orthogonalize_vs_size_beta
        self.market_cap_panel = market_cap_panel.sort_index() if market_cap_panel is not None else None
        self.beta_map = beta_map or {}
        self.exclude_approximated_mcap = exclude_approximated_mcap
        self.approximation_flags = {
            pd.Timestamp(k): v for k, v in (approximation_flags or {}).items()
        }
        self.rank_start = rank_start
        self.yearly_rank_lookup = {
            pd.Timestamp(k): v for k, v in (yearly_rank_lookup or {}).items()
        }
        # R-family strategy dispatch params (Phase 0).
        self.rank_method = rank_method
        self.skip_months = skip_months
        self.top_sectors = top_sectors
        self.volatility_measure = volatility_measure
        # Phase 7: crash-aware momentum overlay (all default-off).
        self.crash_regime_enabled = crash_regime_enabled
        self.drawdown_threshold = drawdown_threshold
        self.vol_percentile_threshold = vol_percentile_threshold
        self.vol_lookback_days = vol_lookback_days
        self.crash_disable_buys = crash_disable_buys
        self.crash_reduce_sizing = crash_reduce_sizing
        # Phase 8: volatility-managed momentum overlay (all default-off).
        self.vol_target_enabled = vol_target_enabled
        self.vol_target_pct = vol_target_pct
        self.vol_target_lookback_days = vol_target_lookback_days
        self.vol_target_leverage_cap = vol_target_leverage_cap
        # Phase 9: factor volatility scaling (R9, all default-off).
        self.vol_scaling_mode = vol_scaling_mode
        self.vol_scaling_lookback_days = vol_scaling_lookback_days
        self.vol_scaling_leverage_cap = vol_scaling_leverage_cap
        # Phase R0: per-ticker volatility weighting (all default-off).
        self.weight_method = weight_method
        self.weight_lookback_days = weight_lookback_days
        # B-027: Regime-switching for vol-scaling strategies (all default-off).
        self.regime_switching_enabled = regime_switching_enabled
        self.rank_band_id = rank_band_id
        self._regime_detector: Optional[EnsembleRegimeDetector] = None
        self._regime_cache: Dict[date_type, str] = {}  # Cache detected regimes
        if regime_switching_enabled and rank_band_id is not None:
            self._init_regime_detector()
        # Shared equity history cache used by crash-aware, vol-target, and vol-scaling overlays
        self._equity_history: Optional[pd.Series] = None
        # Build rank_fn from rank_method if not explicitly provided
        if rank_fn is None and rank_method == "pct_of_52wk_high":
            def _rank_fn_pct_52wk(price_panel: pd.DataFrame, universe: List[str], date: date_type, lookback_days: int) -> pd.Series:
                date_str = date if isinstance(date, str) else date.isoformat()
                return pct_of_52wk_high(price_panel, universe, date_str, lookback_days)
            rank_fn = _rank_fn_pct_52wk
        elif rank_fn is None and rank_method == "trailing_reversal_1mo":
            from features.momentum_signal import trailing_reversal_1mo

            def _rank_fn_reversal(price_panel: pd.DataFrame, universe: List[str], date: date_type, lookback_days: int) -> pd.Series:
                date_str = date if isinstance(date, str) else date.isoformat()
                # Reversal uses fixed 21-day lookback (1 month), ignoring the passed lookback_days
                return trailing_reversal_1mo(price_panel, universe, date_str, lookback_days=21)
            rank_fn = _rank_fn_reversal
        elif rank_fn is None and rank_method == "bollinger_mean_reversion":
            from features.momentum_signal import bollinger_mean_reversion

            def _rank_fn_bollinger(price_panel: pd.DataFrame, universe: List[str], date: date_type, lookback_days: int) -> pd.Series:
                date_str = date if isinstance(date, str) else date.isoformat()
                # Bollinger Band uses 20-day default lookback, ignoring passed lookback_days for BB window
                return bollinger_mean_reversion(price_panel, universe, date_str, lookback_days=20)
            rank_fn = _rank_fn_bollinger
        elif rank_fn is None and rank_method == "risk_adjusted_composite":
            from features.momentum_signal import risk_adjusted_momentum_score

            def _rank_fn_risk_adj(price_panel: pd.DataFrame, universe: List[str], date: date_type, lookback_days: int) -> pd.Series:
                date_str = date if isinstance(date, str) else date.isoformat()
                return risk_adjusted_momentum_score(
                    price_panel, universe, date_str,
                    volatility_measure=self.volatility_measure,
                    use_skip_month=(skip_months > 0),
                )
            rank_fn = _rank_fn_risk_adj
        elif rank_fn is None and skip_months > 0:
            rank_fn = rank_fn_for_skip_months(skip_months)
        self.rank_fn = rank_fn
        # [2026-08-18] What this adapter believes it holds. A plain set:
        # grace cycles are gone, so a name is held or it is not.
        self._held: Set[str] = set()
        self._last_momentum: pd.Series = pd.Series(dtype=float)

    @property
    def _currently_held(self) -> Set[str]:
        """Every ticker this adapter believes it holds."""
        return set(self._held)

    # ===== liquidity =====
    def _adtv_series(self, tickers: List[str], as_of_date: date_type) -> pd.Series:
        """Trailing adtv_lookback_days-day average daily traded value (INR
        crore) per ticker. Delegates to panel_filters.adtv_series, which is
        the single implementation shared with FundamentalAdapter (A93)."""
        return adtv_series(
            self.price_panel, self.volume_panel, tickers, as_of_date, self.adtv_lookback_days,
        )

    def _adtv_cr(self, ticker: str, as_of_date: date_type) -> Optional[float]:
        """Single-ticker ADTV, or None if volume_panel wasn't supplied or
        the ticker/date has no real data — never fabricated."""
        series = self._adtv_series([ticker], as_of_date)
        if ticker not in series.index:
            return None
        value = series[ticker]
        return float(value) if pd.notna(value) else None

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
        or exceeds circuit_band_pct in either direction. Delegates to
        panel_filters.is_circuit_locked (single implementation, A93)."""
        return bool(is_circuit_locked(self.price_panel, ticker, as_of_date, self.circuit_band_pct))

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

        label: Optional[str] = regime_known_as_of(self._regime_segments_cache, as_of)
        return label

    def _is_buys_disabled(self, as_of_date: date_type) -> bool:
        if not self.disable_buys_in_regime:
            return False
        regime = self._regime_for_date(as_of_date)
        return regime is not None and regime in self.disable_buys_in_regime

    def _selection_pool(self, momentum: pd.Series, as_of_date: date_type) -> pd.Series:
        """Momentum scores restricted to tickers eligible to be SELECTED into
        the target set this rebalance. Deliberately never consulted when
        deciding an already-held ticker's sell status: a filter must not
        force a liquidation, only block a new entry.

        [ML40, 2026-08-14] The filter chain itself is no longer implemented
        here. It is features.momentum_strategy.select_buy_pool — the same call
        MomentumBacktester.run() now makes — so the ordering (quality ->
        approximated mcap -> orthogonalization -> liquidity -> circuit-lock ->
        downtrend -> HMM regime) and every filter's never-exclude-on-missing-
        data convention exist once rather than in two engines that were being
        kept in step by hand.

        min_momentum stays here rather than moving into select_buy_pool: it is
        a floor on the SCORE, applied after every filter has run, and
        MomentumBacktester applies it at the same point (its `eligible` line).
        """
        pool = select_buy_pool(
            momentum,
            pd.Timestamp(as_of_date),
            price_panel=self.price_panel,
            # This adapter has no separate forward-filled panel; its
            # circuit-lock check has always read the raw panel, and
            # panel_filters.is_circuit_locked (used by _is_circuit_locked
            # below) does the same. Passing the raw panel keeps that
            # behaviour byte-identical rather than silently introducing
            # forward-filled prices into this adapter's decisions.
            price_panel_ffilled=self.price_panel,
            volume_panel=self.volume_panel,
            quality_scores=self.quality_scores,
            quality_gate=self.quality_gate,
            approximation_flags=self.approximation_flags,
            exclude_approximated_mcap=self.exclude_approximated_mcap,
            orthogonalize_vs_size_beta=self.orthogonalize_vs_size_beta,
            market_cap_panel=self.market_cap_panel,
            beta_map=self.beta_map,
            min_adtv_cr=self.min_adtv_cr,
            adtv_lookback_days=self.adtv_lookback_days,
            circuit_band_pct=self.circuit_band_pct,
            downtrend_filter_pct=self.downtrend_filter_pct,
            downtrend_lookback_days=self.downtrend_lookback_days,
        )


        return pool

    def _sticky_promoted_holdings(self, universe: List[str], as_of_date: date_type) -> List[str]:
        """Currently-held tickers that have been PROMOTED out
        of this adapter's band and so are missing from `universe`, but
        should still be ranked — see the rank_start/yearly_rank_lookup
        docstring. Empty (this rule fully off) unless both were supplied.

        Only reads self._held, never `target` or the price panel, so
        by construction this can never introduce a ticker that isn't
        already held — a promoted name that was never bought, or that has
        already fully exited, is not in _held and therefore not
        returned here.

        [ML40, 2026-08-14] The rule itself is
        features.momentum_strategy.sticky_promoted_holdings — this method is
        now the binding of this adapter's state to that shared function. The
        logic was copied into momentum_strategy.py on 2026-08-09 and left
        duplicated here; that second copy is gone."""
        return sticky_promoted_holdings(
            self._held, universe, pd.Timestamp(as_of_date),
            self.rank_start, self.yearly_rank_lookup,
        )

    def generate_signals(self, universe: List[str], as_of_date: date_type, horizon_bucket: HorizonBucket) -> List[Signal]:
        # Sticky-promotion (Phase 3): extend the ranking pool BEFORE
        # momentum is computed, so a promoted holding's score is real and
        # it genuinely competes for a top_n slot rather than being scored
        # after the cut. No-op unless rank_start + yearly_rank_lookup were
        # both supplied.
        sticky = self._sticky_promoted_holdings(universe, as_of_date)
        if sticky:
            universe = list(universe) + sorted(sticky)
        # [ML40] One ranking implementation, shared with MomentumBacktester.
        # [Phase 0] Support custom rank functions (rank_fn) for R-family strategies.
        # [Phase 2] Use skip_months to wire Jegadeesh-Titman variants (e.g., 12-7, 6-2).
        rank_fn = self.rank_fn or rank_fn_for_skip_months(self.skip_months)
        momentum = rank_universe(self.price_panel, universe, as_of_date, self.lookback_days, rank_fn=rank_fn)
        self._last_momentum = momentum
        if momentum.empty:
            # No fabricated ranking when there isn't enough real history yet
            # (No-Mock-Data Policy) — just hold whatever's already held.
            return []

        # [Phase 4] Sector momentum (industry_momentum): two-stage ranking
        if self.rank_method == "industry_momentum" and self._sector_lookup:
            sector_scores = rank_sectors(momentum, self._sector_lookup, self.top_sectors)
            if not sector_scores.empty:
                top_sectors_list = sector_scores.head(self.top_sectors).index.tolist()
                momentum = rank_constituents_within_sectors(momentum, self._sector_lookup, top_sectors_list)
            if momentum.empty:
                return []

        # BUY side: the filtered pool. Every entry filter is buy-side only.
        pool = self._selection_pool(momentum, as_of_date)
        target = (
            set(pool.sort_values(ascending=False).head(self.top_n).index)
            if not pool.empty else set()
        )

        # HOLD side: the same cut on RAW momentum, before any filter. A
        # buy-side filter must never force a sell — a held name that goes
        # briefly illiquid, gaps down, or fails the quality gate is not
        # thereby a sell decision, it is merely not a fresh buy. Deciding
        # sells off the filtered pool would turn every entry filter into an
        # exit rule nobody asked for.
        keep = (
            set(momentum.sort_values(ascending=False).head(self.top_n).index)
            if not momentum.empty else set()
        )

        # [2026-08-18, user decision] The rotation is a plain list swap: this
        # period's top_n is List 2, what we hold is List 1. Anything held and
        # no longer in List 2 is sold; anything in List 2 and not held is
        # bought. Grace cycles and the asymmetric exit band are gone — a name
        # that leaves the top_n leaves the book.
        #
        # Sticky promotion survives, and is the one exception: a held name
        # missing from `universe` because it grew out of this band (by MARKET
        # CAP, never ADTV) was already added to the ranking pool above, so if
        # it still earns a top_n slot it stays. If it does not, it sells here
        # like anything else.
        #
        # [Phase 7] Crash-aware overlay: disable buys or reduce sizing during
        # crash regimes (drawdown + elevated volatility).
        buys_disabled = self._is_buys_disabled(as_of_date)
        in_crash_regime = self._is_crash_regime_today(as_of_date)
        if in_crash_regime and self.crash_disable_buys:
            buys_disabled = True
        signals: List[Signal] = []
        new_held: Set[str] = set()

        for ticker in sorted(self._held):
            if ticker in keep:
                new_held.add(ticker)
                continue
            if self._is_circuit_locked(as_of_date, ticker):
                # Unfillable at this close: left open and re-evaluated next
                # rebalance rather than booked at a price nobody could get.
                new_held.add(ticker)
                continue
            signals.append(Signal(
                ticker=ticker, action="sell", sector=self._sector_lookup.get(ticker, "Unknown"),
                conviction=0.0, adtv_cr=self._adtv_cr(ticker, as_of_date),
            ))

        new_entrants = [] if buys_disabled else sorted(target - self._held)
        # [Phase R0] Per-ticker volatility weighting: computed once per
        # rebalance over the FULL basket (target, not just new_entrants), so
        # a new entrant's weight reflects its volatility relative to the
        # whole basket being held this period — not just the other new
        # buys. Already-held survivors keep whatever size they entered at
        # (this adapter, like the R8/R9 overlays above, only sizes fresh
        # buy signals; it never re-weights a position it isn't re-issuing a
        # signal for).
        weight_mults: Optional[pd.Series] = None
        if new_entrants and self.weight_method is not None:
            from features.volatility_scaling import WEIGHT_DISPATCH
            weight_fn = WEIGHT_DISPATCH[self.weight_method]
            kwargs: Dict[str, Any] = {"lookback_days": self.weight_lookback_days}
            if self.weight_method == "target_volatility":
                kwargs["target_vol"] = self.vol_target_pct
            weight_mults = weight_fn(self.price_panel, sorted(target), as_of_date, **kwargs)
        for ticker in new_entrants:
            if self._is_circuit_locked(as_of_date, ticker):
                # Skipped this rebalance only; target is recomputed fresh each
                # call, so it is naturally reconsidered next time.
                continue
            conviction = float(momentum.get(ticker, 0.0))
            # [Phase 7] Reduce conviction during crash regime if configured
            if in_crash_regime and self.crash_reduce_sizing is not None:
                conviction *= self.crash_reduce_sizing
            # [Phase 8/9] Compute exposure multiplier (R8 vol-target or R9 generic vol scaling)
            size_mult = self._compute_exposure_multiplier_today(as_of_date)
            # [Phase R0] Fold in the per-ticker basket re-weighting (orthogonal
            # to the portfolio-level R8/R9 multiplier above — this one
            # redistributes the same capital budget across the basket rather
            # than scaling the whole book up or down).
            if weight_mults is not None:
                size_mult *= float(weight_mults.get(ticker, 1.0))
            size_mult_for_signal = size_mult if size_mult != 1.0 else None
            if size_mult != 1.0 and len(signals) == 0:
                # Log first few tickers with non-trivial multiplier on each rebalance
                logger.info(f"R0/R9 {self.weight_method}/{self.vol_scaling_mode}: {ticker} on {as_of_date} size_mult={size_mult:.6f} → signal.size_multiplier={size_mult_for_signal}")
            signals.append(Signal(
                ticker=ticker, action="buy", sector=self._sector_lookup.get(ticker, "Unknown"),
                conviction=conviction, adtv_cr=self._adtv_cr(ticker, as_of_date),
                size_multiplier=size_mult_for_signal,
            ))
            new_held.add(ticker)

        self._held = new_held
        return signals

    def feature_vector(self, ticker: str, as_of_date: date_type) -> Dict[str, Any]:
        return {
            "trailing_momentum": float(self._last_momentum.get(ticker)) if ticker in self._last_momentum.index else None,
            "lookback_days": self.lookback_days,
            "in_top_n": ticker in self._held,
        }

    def update_portfolio_equity(self, date: date_type, equity: float) -> None:
        """
        Track portfolio equity value for crash-regime detection (Phase 7),
        volatility-target exposure scaling (Phase 8), and volatility scaling
        (Phase 9, R9). Called by BacktestOrchestrator (engine.py) after each
        day's rebalance/update. Builds time series: {date: equity_value}.
        """
        if not (self.crash_regime_enabled or self.vol_target_enabled or self.vol_scaling_mode):
            logger.debug(f"R9 {self.vol_scaling_mode}: update_portfolio_equity disabled (crash={self.crash_regime_enabled}, vol_target={self.vol_target_enabled}, vol_scaling={bool(self.vol_scaling_mode)})")
            return
        if self._equity_history is None:
            self._equity_history = pd.Series(dtype=float)
            if self.vol_scaling_mode:
                logger.info(f"R9 {self.vol_scaling_mode}: initializing equity history")
        date_ts = pd.Timestamp(date) if not isinstance(date, pd.Timestamp) else date
        self._equity_history = pd.concat([
            self._equity_history,
            pd.Series([equity], index=[date_ts])
        ])
        if self.vol_scaling_mode:
            if len(self._equity_history) % 50 == 0:
                logger.info(f"R9 {self.vol_scaling_mode}: equity history accumulated {len(self._equity_history)} days")
            elif len(self._equity_history) <= 5:
                logger.info(f"R9 {self.vol_scaling_mode}: equity history accumulated {len(self._equity_history)} days (first few)")

    def _is_crash_regime_today(self, as_of_date: date_type) -> bool:
        """
        Check if today is a crash regime day (drawdown + elevated vol).
        Returns False if crash detection is disabled or insufficient data.
        """
        if not self.crash_regime_enabled or self._equity_history is None or self._equity_history.empty:
            return False
        try:
            crash_series = crash_regime_detector(
                self._equity_history,
                drawdown_threshold=self.drawdown_threshold,
                vol_percentile_threshold=self.vol_percentile_threshold,
                vol_lookback_days=self.vol_lookback_days,
            )
            date_ts = pd.Timestamp(as_of_date) if not isinstance(as_of_date, pd.Timestamp) else as_of_date
            return bool(crash_series.get(date_ts, False))
        except (ValueError, KeyError):
            # Insufficient data for detector, treat as not in crash regime
            return False

    def _vol_target_multiplier_today(self, as_of_date: date_type) -> float:
        """
        Compute volatility-target exposure multiplier for today (Phase 8).
        Returns 1.0 if vol targeting is disabled or insufficient data.
        Kept for backward compatibility; _compute_exposure_multiplier_today dispatches.
        """
        if not self.vol_target_enabled or self._equity_history is None or self._equity_history.empty:
            return 1.0
        try:
            from features.momentum_signal import realized_vol_target_multiplier
            mult_series = realized_vol_target_multiplier(
                self._equity_history,
                target_vol=self.vol_target_pct,
                lookback_days=self.vol_target_lookback_days,
                leverage_cap=self.vol_target_leverage_cap,
            )
            date_ts = pd.Timestamp(as_of_date) if not isinstance(as_of_date, pd.Timestamp) else as_of_date
            return float(mult_series.get(date_ts, 1.0))
        except (ValueError, KeyError, Exception):
            # Insufficient data or computation error, default to no scaling
            return 1.0

    def _init_regime_detector(self) -> None:
        """Initialize ensemble regime detector for this market cap band."""
        if not self.regime_switching_enabled or self.rank_band_id is None:
            return
        try:
            market_cap_band = self._get_market_cap_band_name()
            if market_cap_band is None:
                logger.warning(f"B-027: Unknown rank_band_id {self.rank_band_id}, regime switching disabled")
                self.regime_switching_enabled = False
                return
            self._regime_detector = EnsembleRegimeDetector(market_cap_band)
            logger.info(f"B-027: Initialized ensemble regime detector for {market_cap_band} (band {self.rank_band_id})")
        except Exception as e:
            logger.error(f"B-027: Failed to initialize regime detector: {e}")
            self.regime_switching_enabled = False

    def _get_market_cap_band_name(self) -> Optional[str]:
        """Map rank_band_id to market cap band name."""
        band_mapping = {
            1: "nifty_50",
            2: "nifty_next_50",
            3: "nifty_150",
            4: "nifty_smallcap_250",
            5: "nifty_microcap",
        }
        return band_mapping.get(self.rank_band_id)

    def _detect_regime(self, as_of_date: date_type) -> str:
        """Detect market regime for given date (cached)."""
        if not self.regime_switching_enabled or self._regime_detector is None:
            return "Choppy"  # Default to neutral regime if disabled
        if as_of_date in self._regime_cache:
            return self._regime_cache[as_of_date]
        try:
            ohlcv_df = self._regime_detector.load_index_ohlcv(as_of_date - pd.Timedelta(days=500), as_of_date)
            if ohlcv_df.empty:
                regime = "Choppy"
            else:
                regime_df = self._regime_detector.detect(ohlcv_df)
                regime = regime_df.loc[as_of_date, 'regime'] if as_of_date in regime_df.index else "Choppy"
            self._regime_cache[as_of_date] = regime
            logger.debug(f"B-027: Detected regime on {as_of_date}: {regime}")
            return regime
        except Exception as e:
            logger.warning(f"B-027: Regime detection failed on {as_of_date}: {e}")
            return "Choppy"

    def _select_vol_scaling_mode(self, regime: str) -> Optional[str]:
        """Select vol-scaling mode based on market regime."""
        if not self.regime_switching_enabled or regime == "Choppy":
            return self.vol_scaling_mode  # Use configured mode, or None
        if regime == "Bull":
            return "inverse_volatility"  # Aggressive
        elif regime == "Bear":
            return "downside_volatility"  # Conservative
        else:
            return self.vol_scaling_mode  # Fallback

    def _compute_exposure_multiplier_today(self, as_of_date: date_type) -> float:
        """
        Compute exposure multiplier for today (Phase 8/9/B-027).
        Dispatches between:
        - R8 (vol_target_enabled): Barroso-Santa-Clara vol-target scaling
        - R9 (vol_scaling_mode): Moreira-Muir generalized volatility scaling
        - B-027 (regime_switching_enabled): Regime-aware mode selection for R9
        Returns 1.0 if disabled or insufficient data.
        Note: R8 takes precedence if both are set (backward-compat).
        """
        if self._equity_history is None or self._equity_history.empty:
            if self.vol_scaling_mode is not None:
                logger.warning(f"R9 {self.vol_scaling_mode}: no equity history yet (as_of={as_of_date}), returning 1.0")
            return 1.0

        # R8 takes precedence over R9 if both are set (backward-compat)
        if self.vol_target_enabled:
            return self._vol_target_multiplier_today(as_of_date)

        if self.vol_scaling_mode is None:
            return 1.0

        # B-027: Determine vol_scaling_mode based on regime if enabled
        active_mode = self.vol_scaling_mode
        regime = "Choppy"
        if self.regime_switching_enabled:
            regime = self._detect_regime(as_of_date)
            active_mode = self._select_vol_scaling_mode(regime)
            if active_mode is None:
                return 1.0
            logger.info(f"B-027: {as_of_date} regime={regime} → mode={active_mode}")

        try:
            from features.volatility_scaling import VOL_SCALING_DISPATCH
            scaling_fn = VOL_SCALING_DISPATCH[active_mode]
            kwargs = {"lookback_days": self.vol_scaling_lookback_days, "leverage_cap": self.vol_scaling_leverage_cap}
            if active_mode == "target_volatility":
                kwargs["target_vol"] = self.vol_target_pct
            mult_series = scaling_fn(self._equity_history, **kwargs)
            date_ts = pd.Timestamp(as_of_date) if not isinstance(as_of_date, pd.Timestamp) else as_of_date

            # Note: generate_signals() is called BEFORE update_portfolio_equity(),
            # so equity_history doesn't include today's equity yet. Use the most
            # recent available multiplier (typically yesterday's).
            if date_ts in mult_series.index:
                mult = float(mult_series.loc[date_ts])
                logger.info(f"R9 {active_mode}: {as_of_date} found in mult_series, using={mult:.6f}")
            elif len(mult_series) > 0:
                # Use last available multiplier (off-by-one: yesterday's data)
                mult = float(mult_series.iloc[-1])
                logger.info(f"R9 {active_mode}: {as_of_date} NOT in mult_series (only has {len(mult_series)} days), using iloc[-1]={mult:.6f}")
            else:
                mult = 1.0
                logger.info(f"R9 {active_mode}: {as_of_date} equity_hist_len={len(self._equity_history)} empty mult_series, using default={mult:.6f}")
            if len(self._equity_history) > 200 and len(self._equity_history) % 100 == 0:
                logger.info(f"R9 {active_mode}: {as_of_date} equity_hist_len={len(self._equity_history)} multiplier={mult:.6f} (mid-backtest check)")
            return mult
        except (ValueError, KeyError, Exception) as e:
            # Insufficient data or computation error, default to no scaling
            logger.warning(f"R9 {active_mode}: computation error on {as_of_date}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return 1.0
