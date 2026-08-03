"""
backtest/momentum_backtest.py

Phase: FeatureBacklog.md ML38 — momentum strategy implementation
Owner: Platform / Backtest
Consumers: scripts/run_momentum_experimentation.py

MomentumBacktester: a standalone, in-memory (no DB access during run())
backtest engine for ML38's momentum strategy. Distinct from
backtest/engine.py's BacktestEngine (which drives the ML signal-model
stack — P&D filter -> signal model -> MetaLabeler -> exit model); this is
a classical long-only rank/momentum factor strategy with none of that
model machinery, so it does not reuse BacktestEngine. It does reuse
backtest/costs.py's IndianTransactionCosts for realistic net returns.

Strategy, per FeatureBacklog.md ML38's confirmed scope (2026-07-13/14):
  - Universe: one of 4 market-cap-rank bands (features/momentum_universe.py),
    fixed on each year's first real trading day.
  - Signal: trailing N-month price momentum (features/momentum_signal.py),
    one lookback window per backtest run — never blended across windows.
  - Portfolio: top-20 (`top_n`) by momentum, equal-weighted.
  - Churn reduction: a stock dropping out of the top-20 is held for
    `grace_cycles` (2) more rebalances before being force-sold, unless it
    re-enters the top-20 first (grace resets, no sell/rebuy).
  - Capital: starting_capital split investable_pct (0.8) / buffer_pct
    (0.2). New top-20 entrants are funded first from cash; if cash can't
    cover a new buy (buffer exhausted), the longest-in-grace held
    position(s) are force-sold to raise cash (2026-07-14 user decision).
  - No trimming/rebalancing of existing core (non-grace) holdings back to
    equal weight each period — a position is sized once, on entry, and
    left alone until it's sold. This is a deliberate design choice to
    minimize churn (the whole point of the grace-period rule); it means
    winners are allowed to become overweight over time, which is a known,
    accepted trade-off of this design, not an oversight.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd

from backtest.costs import IndianTransactionCosts
from features.momentum_signal import orthogonalize_momentum_vs_factors, trailing_momentum_from_panel

logger = logging.getLogger(__name__)

# [DATA QUALITY, 2026-08-02] Dedicated log for trade_cagr's OverflowError guard
# (see trade_cagr's docstring) — these trades' extreme price ratios are a
# real-data-corruption signal (see
# backtest/reports/technical/screener_cache/../ANMOL root-cause investigation:
# an un-smoothed adj_factor discontinuity at a corporate action), not just a
# numeric edge case, so every occurrence is recorded here for later data-quality
# triage rather than silently dropped alongside the None return value.
DATA_QUALITY_ANOMALY_LOG_PATH = Path(__file__).resolve().parent.parent / "logs" / "data_quality_anomalies.log"
_anomaly_logger = logging.getLogger("backtest.data_quality_anomalies")
if not _anomaly_logger.handlers:
    DATA_QUALITY_ANOMALY_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    _handler = logging.FileHandler(DATA_QUALITY_ANOMALY_LOG_PATH, mode="a")
    _handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    _anomaly_logger.addHandler(_handler)
    _anomaly_logger.setLevel(logging.WARNING)
    _anomaly_logger.propagate = False


def decide_grace_transitions(
    held_tickers: Dict[str, Optional[int]],
    target_set: Set[str],
    grace_cycles: int,
) -> Dict[str, Optional[int]]:
    """Pure grace-period decision rule, extracted from
    MomentumBacktester.run() (2026-07-14, FeatureBacklog.md ML38) so the
    live rebalance-suggestion engine (features/momentum_live.py) can reuse
    the exact same rule instead of a second hand-written copy that could
    silently drift from the validated backtest's behavior.

    held_tickers : {ticker: current grace_remaining}, None = core (was in
        the prior period's top_n, never entered grace).
    target_set : this period's top_n ticker set.
    grace_cycles : how many more periods a dropped-out ticker is held
        before being force-sold.

    Returns {ticker: new grace_remaining} for every key in held_tickers —
    None = back in target_set (core again, grace reset), an int = periods
    left (<=0 means the caller should force-sell this ticker now).
    """
    updated: Dict[str, Optional[int]] = {}
    for ticker, grace_remaining in held_tickers.items():
        if ticker in target_set:
            updated[ticker] = None
        elif grace_remaining is None:
            updated[ticker] = grace_cycles
        else:
            updated[ticker] = grace_remaining - 1
    return updated


def trade_cagr(
    buy_price: float, sell_price: Optional[float], holding_days: Optional[int],
    *, ticker: Optional[str] = None, buy_date: Optional[str] = None,
    sell_date: Optional[str] = None, run_id: Optional[str] = None,
) -> Optional[float]:
    """Per-trade annualized price gain: (sell/buy)^(365.25/holding_days) - 1.

    None for a still-open position (sell_price/holding_days not yet known)
    or a same-day round-trip (holding_days <= 0 makes annualizing
    meaningless — division by a near-zero exponent denominator blows up).

    ticker/buy_date/sell_date/run_id are optional, log-only context (not
    used in the calculation) so an OverflowError anomaly can be traced back
    to the actual trade without changing this function's return contract
    for existing positional callers.
    """
    if sell_price is None or holding_days is None or holding_days <= 0 or buy_price <= 0:
        return None
    try:
        return (sell_price / buy_price) ** (365.25 / holding_days) - 1
    except OverflowError:
        # A short holding period compounded to an annualized rate can exceed
        # float range (e.g. a 1-day trade with an extreme price ratio from a
        # circuit-limit move or corrupted OHLCV bar — see
        # project_ohlcv_adjfactor_discontinuities_20260802 memory: 960 such
        # discontinuities found across ohlcv_adjusted) — not a meaningful
        # annualized figure either way, so treat it the same as "unknown",
        # but record it for data-quality triage rather than dropping it.
        _anomaly_logger.warning(
            "trade_cagr_overflow ticker=%s run_id=%s buy_date=%s sell_date=%s "
            "buy_price=%s sell_price=%s ratio=%s holding_days=%s",
            ticker, run_id, buy_date, sell_date, buy_price, sell_price,
            (sell_price / buy_price) if buy_price else None, holding_days,
        )
        return None


@dataclass
class Position:
    qty: int
    entry_price: float
    entry_date: str
    entry_rank: Optional[int]
    grace_remaining: Optional[int] = None  # None = core (in current top-N) holding


@dataclass
class MomentumBacktestResult:
    equity_curve: List[Dict] = field(default_factory=list)  # [{"date", "total_value"}]
    rebalance_events: List[Dict] = field(default_factory=list)  # [{"date", "n_bought", "n_sold", ...}]
    transactions: List[Dict] = field(default_factory=list)  # per-ticker buy/sell ledger, see run()'s docstring
    starting_capital: float = 0.0
    ending_value: float = 0.0
    start_date: str = ""
    end_date: str = ""
    cash_flows: List[Dict] = field(default_factory=list)  # [{"date","amount"}], SIP contributions if sip_amount set
    total_contributed: float = 0.0  # starting_capital + every SIP contribution (== starting_capital if no SIP)
    total_signals: int = 0  # sum of |target_set| across every rebalance — post-filter buy signals generated


class MomentumBacktester:
    # 2026-08-01 root-cause fix: an OHLCV ingestion outage from 2024-07-08 to
    # 2024-07-31 (17 trading days, coverage collapsed from ~1,750 tickers/day
    # to ~90) was being silently papered over by an unbounded price_panel.ffill()
    # below — every trade whose nominal exit date fell inside the gap got
    # priced at the frozen pre-gap close, fabricating returns up to +418% for
    # names like COCHINSHIP/IRCON/POWERINDIA that happened to be near a local
    # high right before the gap. A short gap (a holiday, a single-day halt)
    # should still be bridged, but a multi-week outage must surface as a
    # missing price (NaN) — every call site already skips/defers on NaN
    # (buys at line ~659, mark-to-market at ~472, grace force-sells at ~634)
    # rather than crashing, so capping the fill is safe.
    MAX_FORWARD_FILL_TRADING_DAYS = 5

    def __init__(
        self,
        price_panel: pd.DataFrame,
        yearly_universes: Dict[str, List[str]],
        lookback_days: int,
        rebalance_every_n_trading_days: int,
        starting_capital: float = 1_000_000.0,
        investable_pct: float = 0.8,
        top_n: int = 20,
        grace_cycles: int = 2,
        costs: Optional[IndianTransactionCosts] = None,
        min_momentum: Optional[float] = None,
        sip_amount: Optional[float] = None,
        rebalance_offset_days: int = 0,
        downtrend_filter_pct: Optional[float] = None,
        downtrend_lookback_days: int = 20,
        volume_panel: Optional[pd.DataFrame] = None,
        min_adtv_cr: Optional[float] = None,
        adtv_lookback_days: int = 20,
        max_pct_of_adtv: Optional[float] = None,
        circuit_band_pct: Optional[float] = None,
        approximation_flags: Optional[Dict[str, Dict[str, bool]]] = None,
        exclude_approximated_mcap: bool = False,
        volume_weighted: bool = False,
        regime_series: Optional[pd.Series] = None,
        disable_in_regimes: Optional[Set[str]] = None,
        orthogonalize_vs_size_beta: bool = False,
        market_cap_panel: Optional[pd.DataFrame] = None,
        beta_map: Optional[Dict[str, float]] = None,
        quality_scores: Optional[Dict[str, Dict[str, float]]] = None,
        quality_gate: Optional[Dict[str, float]] = None,
    ):
        """
        price_panel : wide close-price DataFrame (index=date, columns=ticker),
            from features.momentum_signal.load_price_panel — real prices
            only, never guessed/interpolated beyond forward-fill applied
            here purely for mark-to-market valuation (never for the
            momentum-return calc itself, which uses raw closes).
        yearly_universes : {first_trading_day_of_year_iso: [tickers]} for
            one rank band, from features.momentum_universe.
        lookback_days : trailing momentum window in trading days (e.g. 63
            for 3 months).
        rebalance_every_n_trading_days : e.g. 5=weekly, 10=biweekly,
            21=monthly, 63=quarterly (matches the codebase's existing
            21-trading-day-per-month convention).
        min_momentum : optional win-rate-improvement experiment
            (2026-07-14, FeatureBacklog.md ML38) — if set, a ticker only
            enters target_set when its trailing momentum exceeds this
            floor (e.g. 0.0 = only ever buy names with genuinely positive
            trailing momentum), even if it would otherwise rank in the
            top_n. This can leave a rebalance with fewer than top_n picks
            (never padded with a negative-momentum "least bad" name to
            fill the slot) — the hypothesis being that buying a top-ranked
            but still-falling name inside a weak band/period is what hurts
            win rate. None (default) preserves the original behavior:
            always fill top_n regardless of sign.
        sip_amount : optional SIP comparison (2026-07-14, FeatureBacklog.md
            ML38) — if set, this amount is injected into cash on the
            first real trading day of every calendar month after the
            first (the first month's contribution is starting_capital
            itself, not doubled). Since a SIP's cash arrives on many
            different dates, its correct return measure is XIRR (see
            backtest/momentum_metrics.py::xirr), not plain CAGR — plain
            CAGR assumes one lump sum in and one lump sum out.
        rebalance_offset_days : trading-day index into price_panel.index
            the rebalance schedule starts counting from (2026-07-14
            overfitting-robustness check, FeatureBacklog.md ML38) — e.g.
            offset=3 with rebalance_every_n_trading_days=32 rebalances on
            trading days 3, 35, 67, ... instead of 0, 32, 64, .... A
            genuine edge shouldn't be sensitive to which exact calendar
            dates the schedule happens to land on; shifting the offset
            while holding every other parameter fixed isolates that from
            a real, date-independent momentum effect.
        downtrend_filter_pct : optional recent-reversal filter (2026-07-15,
            FeatureBacklog.md ML38 comparison request) — if set (e.g. 0.05),
            a ticker is excluded from target_set selection this rebalance
            if its trailing `downtrend_lookback_days`-day return is <=
            -downtrend_filter_pct (a >=5% drop in the last 20 trading days,
            with 0.05 as the example threshold) — the idea being to skip
            names whose long-lookback momentum score still looks good but
            that have already started reversing hard in the short term.
            A ticker with a milder recent dip (downtrend < the threshold,
            including any positive short-term return) stays eligible.
            None (default) applies no such filter, matching prior
            behavior. A ticker without enough history for the short-term
            window is left eligible (no filter applied), never excluded
            on missing data.
        downtrend_lookback_days : trading-day window for the above filter
            (default 20, i.e. one trading month).
        volume_panel : optional wide adjusted-volume DataFrame (same shape
            as price_panel), from features.momentum_signal.load_volume_panel
            (2026-07-19 full-codebase-review Fix 1). Required for
            min_adtv_cr/max_pct_of_adtv/circuit_band_pct to have any
            effect — without it those filters are silently no-ops, since
            there's no volume data to filter on.
        min_adtv_cr : optional liquidity floor (2026-07-19 Fix 1) — a
            ticker whose trailing `adtv_lookback_days`-day average daily
            traded value (INR crore, price * volume) is below this
            threshold is excluded from target_set selection this
            rebalance, the same way downtrend_filter_pct excludes names
            from selection_pool. None (default) applies no liquidity
            filter, matching prior behavior. Reuses the same INR-crore
            convention as config.settings.MIN_ADTV_CR.
        adtv_lookback_days : trading-day window for the ADTV calculation
            above (default 20).
        max_pct_of_adtv : optional position-sizing cap (2026-07-19 Fix 1)
            — when set (e.g. 0.05, matching config.settings.
            MAX_ORDER_VS_ADTV), a new buy's qty is capped so that
            qty * price never exceeds max_pct_of_adtv * that ticker's
            trailing ADTV, in addition to the existing investable_per_slot
            cap — whichever is smaller wins. None (default) applies no cap.
        circuit_band_pct : optional circuit-lock proxy (2026-07-19 Fix 1)
            — since no bid/ask/circuit data exists in this DB, a day where
            a ticker's realized return (using the same forward-filled
            close series everything else in this engine uses) has
            abs(return) >= circuit_band_pct is treated as "likely
            circuit-locked, unrealistic to fill at this close": that
            ticker is skipped for BOTH new buys and force-sells this
            rebalance (an existing position is simply left open and
            re-evaluated at the next rebalance rather than force-sold at
            a stale/frozen close; a skipped buy is naturally reconsidered
            next rebalance since target_set is recomputed fresh each
            time). None (default) applies no such filter, matching prior
            behavior —
            this is a coarse proxy (real circuit bands vary 5/10/20% by
            stock tier and surveillance stage), not a precise circuit
            detector.
        approximation_flags : optional {first_trading_day_of_year_iso:
            {ticker: shares_outstanding_is_approximated}} from
            features.momentum_universe.yearly_band_approximation_flags_from_rankings
            (2026-07-19 Fix 6) — lets exclude_approximated_mcap below
            actually do something. None (default) means no flags are
            known, so exclude_approximated_mcap has no effect even if set.
        exclude_approximated_mcap : if True, a ticker whose rank-band
            membership on the active year_start came from the pre-2024
            earliest-known-shares-outstanding fallback (rather than a real
            PIT-eligible shares_outstanding row) is excluded from
            target_set selection this rebalance (2026-07-19 Fix 6).
            Defaults to False so existing backtest results remain exactly
            reproducible unless explicitly opted in.
        volume_weighted : if True, a new buy's capital allocation is
            scaled by that ticker's trailing adtv_lookback_days-day dollar
            volume relative to the rest of this rebalance's target_set,
            instead of the default equal investable_per_slot split
            (2026-07-19 full-codebase-review Fix B1 — "volume-weighted
            momentum": higher-liquidity names in the top-N get a larger
            allocation). Requires volume_panel; silently falls back to
            equal-weighting (with a one-time warning) if volume_panel is
            None, since there's no volume data to weight by. Defaults to
            False so existing backtest results remain exactly reproducible
            unless explicitly opted in.
        regime_series : pd.Series, optional
            Date-indexed regime labels (e.g. from
            features.regime_signal.compute_realized_vol_regime) — real
            data, computed from an actual benchmark series (2026-07-19
            full-codebase-review Fix B2: "regime-conditioning on
            momentum" — momentum strategies are well-documented to crash
            in regime transitions/high-volatility periods). None
            (default) disables regime filtering entirely.
        disable_in_regimes : set of str, optional
            Regime labels (matched against regime_series) for which new
            buys are skipped this rebalance — existing positions are left
            open and re-evaluated at the next rebalance, same
            skip-don't-force-liquidate pattern as circuit_band_pct.
            Ignored unless regime_series is also set. A rebalance date
            with no regime_series entry (NaN/missing — insufficient
            trailing history) is never treated as disabled.
        orthogonalize_vs_size_beta : if True, momentum scores used for
            ranking/selection are cross-sectionally residualized against
            log(market_cap) and a sector-beta proxy each rebalance
            (features.momentum_signal.orthogonalize_momentum_vs_factors)
            before picking the top_n — standard factor-neutralization so
            the strategy isn't a disguised small-cap-beta bet (2026-07-19
            full-codebase-review Fix B3). Requires market_cap_panel and
            beta_map; silently falls back to raw momentum ranking if
            either is missing, or if fewer than 10 tickers have both
            values on a given rebalance date. Defaults to False so
            existing backtest results remain exactly reproducible unless
            explicitly opted in.
        market_cap_panel : pd.DataFrame, optional
            Wide market-cap DataFrame (index=date, columns=ticker), same
            shape as price_panel — e.g. from
            features.momentum_universe.market_cap_snapshot reshaped, or
            any other real market-cap source. Only used when
            orthogonalize_vs_size_beta=True.
        beta_map : dict, optional
            ticker -> beta proxy (e.g. a SECTOR_UNLEVERED_BETAS lookup
            per ticker's sector). Only used when
            orthogonalize_vs_size_beta=True.
        quality_scores : dict, optional
            ticker -> {"f_score": float, "m_score": float, ...} — real
            forensic scores sourced from the already-correct, production-
            wired systems.ml_signal_engine.models.forensic.classical_scores
            (piotroski_f_score/beneish_m_score), e.g. read from the
            ml_forensic DuckDB table via datastore/api/routers/forensic.py
            (2026-07-19 full-codebase-review Fix B5 — "quality-gated
            momentum": use the already-correct-and-wired forensic scores
            as a momentum-candidate screen, no new scoring logic here). A
            ticker missing from this dict is never excluded (never
            excluded on missing data, same convention as every other
            optional filter in this engine).
        quality_gate : dict, optional
            Threshold(s) applied against quality_scores, e.g.
            {"min_f_score": 4} excludes any ticker whose f_score is below
            4, {"max_m_score": -1.78} excludes any ticker whose m_score
            exceeds the Beneish manipulator threshold. Any subset of
            {"min_f_score", "max_m_score"} may be supplied. None (default,
            or an empty dict) applies no quality filter, matching prior
            behavior.
        """
        if price_panel.empty:
            raise ValueError("price_panel must not be empty")
        self.price_panel = price_panel.sort_index()
        self.price_panel_ffilled = self.price_panel.ffill(limit=self.MAX_FORWARD_FILL_TRADING_DAYS)
        self.yearly_universes = {pd.Timestamp(k): v for k, v in yearly_universes.items()}
        self.lookback_days = lookback_days
        self.rebalance_every_n_trading_days = rebalance_every_n_trading_days
        self.starting_capital = starting_capital
        self.investable_pct = investable_pct
        self.top_n = top_n
        self.grace_cycles = grace_cycles
        self.costs = costs or IndianTransactionCosts()
        self.min_momentum = min_momentum
        self.sip_amount = sip_amount
        self.rebalance_offset_days = rebalance_offset_days
        self.downtrend_filter_pct = downtrend_filter_pct
        self.downtrend_lookback_days = downtrend_lookback_days
        self.volume_panel = volume_panel.sort_index() if volume_panel is not None else None
        self.min_adtv_cr = min_adtv_cr
        self.adtv_lookback_days = adtv_lookback_days
        self.max_pct_of_adtv = max_pct_of_adtv
        self.circuit_band_pct = circuit_band_pct
        self.approximation_flags = {
            pd.Timestamp(k): v for k, v in (approximation_flags or {}).items()
        }
        self.exclude_approximated_mcap = exclude_approximated_mcap
        self.volume_weighted = volume_weighted
        self._volume_weighted_fallback_warned = False
        self.regime_series = regime_series.sort_index() if regime_series is not None else None
        self.disable_in_regimes = disable_in_regimes or set()
        self.orthogonalize_vs_size_beta = orthogonalize_vs_size_beta
        self.market_cap_panel = market_cap_panel.sort_index() if market_cap_panel is not None else None
        self.beta_map = beta_map or {}
        self.quality_scores = quality_scores or {}
        self.quality_gate = quality_gate or {}

        self.cash = starting_capital
        self.positions: Dict[str, Position] = {}

    def _active_universe(self, date: pd.Timestamp) -> List[str]:
        applicable_starts = [d for d in self.yearly_universes if d <= date]
        if not applicable_starts:
            return []
        year_start = max(applicable_starts)
        return self.yearly_universes[year_start]

    def _adtv_cr(self, date: pd.Timestamp, tickers: List[str]) -> pd.Series:
        """Trailing adtv_lookback_days-day average daily traded value (INR
        crore) per ticker, ending on `date` — price(t) * volume(t), same
        real-gap NaN handling as the rest of this engine (no forward-fill
        of volume itself; NaN days simply don't contribute to the mean).
        Empty/no-data tickers get NaN, which min_adtv_cr filtering below
        treats as "excluded" (never assumed liquid on missing data)."""
        if self.volume_panel is None or not tickers:
            return pd.Series(dtype=float)
        cols = [t for t in tickers if t in self.volume_panel.columns and t in self.price_panel.columns]
        if not cols:
            return pd.Series(dtype=float)
        window_prices = self.price_panel[cols].loc[:date].tail(self.adtv_lookback_days)
        window_volume = self.volume_panel[cols].loc[:date].tail(self.adtv_lookback_days)
        traded_value_cr = (window_prices * window_volume) / 1e7
        return traded_value_cr.mean(skipna=True)

    def _volume_weights(self, date: pd.Timestamp, target_set: Set[str]) -> Optional[Dict[str, float]]:
        """Per-ticker capital-allocation multiplier for volume_weighted=True
        (Fix B1): weight_ticker = adtv_ticker / mean(adtv over target_set),
        so a ticker with average liquidity gets multiplier 1.0 (same as
        equal-weighting) and a more/less liquid ticker gets proportionally
        more/less capital. Returns None (falls back to equal-split at the
        call site) if volume_panel is missing or no ticker in target_set
        has usable ADTV data."""
        if self.volume_panel is None:
            if not self._volume_weighted_fallback_warned:
                logger.warning(
                    "volume_weighted=True but no volume_panel was supplied — "
                    "falling back to equal-weighted position sizing."
                )
                self._volume_weighted_fallback_warned = True
            return None
        adtv = self._adtv_cr(date, list(target_set))
        adtv = adtv[adtv.notna() & (adtv > 0)]
        if adtv.empty:
            return None
        mean_adtv = adtv.mean()
        return (adtv / mean_adtv).to_dict()

    def _passes_quality_gate(self, ticker: str) -> bool:
        """False only if `ticker` has quality_scores AND fails an
        explicitly-set threshold (Fix B5). True (passes) for a ticker
        missing from quality_scores entirely, or when quality_gate is
        empty — never excludes on missing data."""
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

    def _is_regime_disabled(self, date: pd.Timestamp) -> bool:
        """True if `date`'s active regime (most recent regime_series entry
        on or before `date`) is in disable_in_regimes. False (never
        disabled) when regime_series is None or has no entry on/before
        `date` (unknown regime, same never-exclude-on-missing-data
        convention as this engine's other optional filters)."""
        if self.regime_series is None or not self.disable_in_regimes:
            return False
        eligible = self.regime_series.loc[:date].dropna()
        if eligible.empty:
            return False
        return eligible.iloc[-1] in self.disable_in_regimes

    def _is_circuit_locked(self, date: pd.Timestamp, ticker: str) -> bool:
        """True if `ticker`'s realized 1-day return into `date` (via the
        same forward-filled close series everything else in this engine
        uses) meets/exceeds circuit_band_pct in either direction — a
        coarse proxy for "likely circuit-locked, don't trust this close
        as a fillable price" (Fix 1). Returns False (never locked) when
        circuit_band_pct is None, or on insufficient history."""
        if self.circuit_band_pct is None or ticker not in self.price_panel_ffilled.columns:
            return False
        idx = self.price_panel_ffilled.index
        pos = idx.searchsorted(date)
        if pos <= 0:
            return False
        prev_price = self.price_panel_ffilled[ticker].iloc[pos - 1]
        cur_price = self.price_panel_ffilled[ticker].iloc[pos]
        if pd.isna(prev_price) or pd.isna(cur_price) or prev_price <= 0:
            return False
        ret = (cur_price - prev_price) / prev_price
        return abs(ret) >= self.circuit_band_pct

    def _price_row(self, date: pd.Timestamp) -> pd.Series:
        """Forward-filled close price per ticker as of `date`. Uses .loc
        directly (date is always a real row of price_panel — every caller
        draws rebalance dates from price_panel.index itself) rather than
        DataFrame.asof(), which is NOT column-independent: by default it
        requires an entire row to be simultaneously non-null across every
        column, and with 90+ tickers of staggered listing/delisting
        history that's almost never true — asof() silently returned an
        all-NaN row for nearly every date in this backtest's first real
        run (caught by the flat equity curve it produced end to end)."""
        return self.price_panel_ffilled.loc[date]

    def _mark_to_market(self, date: pd.Timestamp, prices: pd.Series) -> float:
        holdings_value = 0.0
        for ticker, pos in self.positions.items():
            price = prices.get(ticker)
            if price is not None and pd.notna(price):
                holdings_value += pos.qty * price
        return self.cash + holdings_value

    def _one_leg_cost(self, price: float, qty: int) -> float:
        """Approximate one-side (buy or sell) transaction cost as half the
        round-trip rate — costs.py only exposes a round-trip helper since
        its existing callers always cost a full open+close position; here
        buys and sells happen independently across different rebalances,
        so each leg is costed at roundtrip_pct/2, an approximation stated
        explicitly rather than silently assuming full round-trip cost
        applies to a single leg."""
        if qty <= 0 or price <= 0:
            return 0.0
        turnover = price * qty
        return turnover * self.costs.compute_roundtrip_cost_pct(price, qty) / 2.0

    def _record_sell(self, result: MomentumBacktestResult, ticker: str, sell_date: str, sell_price: float, sell_rank: Optional[int]) -> None:
        pos = self.positions[ticker]
        holding_days = (pd.Timestamp(sell_date) - pd.Timestamp(pos.entry_date)).days
        result.transactions.append({
            "ticker": ticker,
            "buy_date": pos.entry_date,
            "sell_date": sell_date,
            "buy_price": pos.entry_price,
            "sell_price": sell_price,
            "qty": pos.qty,
            "holding_days": holding_days,
            "trade_cagr": trade_cagr(
                pos.entry_price, sell_price, holding_days,
                ticker=ticker, buy_date=pos.entry_date, sell_date=sell_date,
            ),
            "buy_momentum_rank": pos.entry_rank,
            "sell_momentum_rank": sell_rank,
            "status": "closed",
        })

    def _monthly_injection_dates(self, trading_days: pd.DatetimeIndex) -> List[pd.Timestamp]:
        """First real trading day of every calendar month in the panel,
        EXCLUDING the very first month (that month's contribution is
        starting_capital itself, applied once at the top of run() —
        injecting a SIP amount there too would double-count month 1)."""
        seen_months = set()
        dates = []
        for d in trading_days:
            key = (d.year, d.month)
            if key not in seen_months:
                seen_months.add(key)
                dates.append(d)
        return dates[1:]

    def run(self) -> MomentumBacktestResult:
        trading_days = self.price_panel.index
        rebalance_dates = trading_days[self.rebalance_offset_days :: self.rebalance_every_n_trading_days]

        result = MomentumBacktestResult(
            starting_capital=self.starting_capital,
            start_date=str(trading_days[0].date()),
            end_date=str(trading_days[-1].date()),
        )
        cash_flows = [{"date": str(trading_days[0].date()), "amount": -self.starting_capital}]
        total_contributed = self.starting_capital
        injection_dates = self._monthly_injection_dates(trading_days) if self.sip_amount else []
        injection_idx = 0

        for date in rebalance_dates:
            n_bought = 0
            n_sold = 0

            # SIP: apply every monthly contribution due on or before this
            # rebalance date (contributions between rebalances just sit in
            # cash, unused, until the next rebalance actually deploys
            # them — same cash-drag treatment as the buffer already gets).
            while injection_idx < len(injection_dates) and injection_dates[injection_idx] <= date:
                self.cash += self.sip_amount
                total_contributed += self.sip_amount
                cash_flows.append({"date": str(injection_dates[injection_idx].date()), "amount": -self.sip_amount})
                injection_idx += 1

            universe = self._active_universe(date)
            momentum = trailing_momentum_from_panel(self.price_panel, universe, str(date.date()), self.lookback_days)

            selection_pool = momentum

            if self.quality_gate and not selection_pool.empty:
                failing = [t for t in selection_pool.index if not self._passes_quality_gate(t)]
                selection_pool = selection_pool.drop(index=failing)

            if self.exclude_approximated_mcap and self.approximation_flags and not selection_pool.empty:
                applicable_starts = [d for d in self.approximation_flags if d <= date]
                if applicable_starts:
                    flags = self.approximation_flags[max(applicable_starts)]
                    approximated = [t for t in selection_pool.index if flags.get(t)]
                    selection_pool = selection_pool.drop(index=approximated)

            # orthogonalize_vs_size_beta (Fix B3): rank/select on the
            # residual after regressing out size/beta, not raw momentum.
            # Silently no-ops (returns selection_pool unchanged) if
            # market_cap_panel/beta_map are missing or too few tickers
            # have both values this rebalance — see
            # orthogonalize_momentum_vs_factors' own docstring.
            if self.orthogonalize_vs_size_beta and self.market_cap_panel is not None and not selection_pool.empty:
                mcap_history = self.market_cap_panel.loc[:date]
                mcap_row = mcap_history.iloc[-1] if not mcap_history.empty else pd.Series(dtype=float)
                mcap_for_pool = mcap_row.reindex(selection_pool.index)
                beta_for_pool = pd.Series(
                    {t: self.beta_map[t] for t in selection_pool.index if t in self.beta_map}
                ).reindex(selection_pool.index)
                residual = orthogonalize_momentum_vs_factors(selection_pool, mcap_for_pool, beta_for_pool)
                # Tickers dropped by the regression (missing mcap/beta)
                # keep their original raw momentum score rather than
                # being silently excluded from selection.
                selection_pool = residual.combine_first(selection_pool)

            adtv = pd.Series(dtype=float)
            if self.min_adtv_cr is not None and not selection_pool.empty:
                adtv = self._adtv_cr(date, list(selection_pool.index))
                # NaN (no volume data) is excluded, same as below-threshold
                # — never assumed liquid on missing data.
                illiquid = adtv[adtv.isna() | (adtv < self.min_adtv_cr)].index
                selection_pool = selection_pool.drop(index=[t for t in illiquid if t in selection_pool.index])

            if self.circuit_band_pct is not None and not selection_pool.empty:
                locked = [t for t in selection_pool.index if self._is_circuit_locked(date, t)]
                selection_pool = selection_pool.drop(index=locked)

            if self.downtrend_filter_pct is not None and not selection_pool.empty:
                short_term = trailing_momentum_from_panel(
                    self.price_panel, list(selection_pool.index), str(date.date()), self.downtrend_lookback_days
                )
                # Tickers with no short-term-window history stay eligible
                # (never excluded on missing data) — only a confirmed
                # >=threshold drop over the window excludes a ticker.
                sharply_down = short_term[short_term <= -self.downtrend_filter_pct].index
                selection_pool = selection_pool.drop(index=[t for t in sharply_down if t in selection_pool.index])

            eligible = selection_pool[selection_pool > self.min_momentum] if self.min_momentum is not None else selection_pool
            target_set = set(eligible.sort_values(ascending=False).head(self.top_n).index) if not eligible.empty else set()
            # 1 = highest momentum, within the active universe band this
            # rebalance — the "Momentum Rank" surfaced per transaction
            # (FeatureBacklog.md ML38, 2026-07-14 drill-down request).
            rank_series = momentum.rank(ascending=False, method="min").astype(int) if not momentum.empty else pd.Series(dtype=int)
            result.total_signals += len(target_set)

            prices = self._price_row(date)
            total_value = self._mark_to_market(date, prices)
            date_str = str(date.date())

            # 1. Update grace status for currently-held names (see
            #    decide_grace_transitions' docstring for why this is a
            #    shared pure function rather than inlined here).
            held_grace = {t: p.grace_remaining for t, p in self.positions.items()}
            updated_grace = decide_grace_transitions(held_grace, target_set, self.grace_cycles)
            for ticker, grace_remaining in updated_grace.items():
                self.positions[ticker].grace_remaining = grace_remaining

            # 2. Force-sell names whose grace period has fully elapsed.
            #    A circuit-locked name is left open this rebalance (its
            #    close is treated as unrealistic to fill at) and
            #    re-evaluated next rebalance instead.
            for ticker in list(self.positions.keys()):
                pos = self.positions[ticker]
                if pos.grace_remaining is not None and pos.grace_remaining <= 0:
                    if self._is_circuit_locked(date, ticker):
                        continue
                    price = prices.get(ticker)
                    if price is None or pd.isna(price):
                        # No real (or fill-bridged) price available — e.g. a
                        # multi-week OHLCV gap past MAX_FORWARD_FILL_TRADING_DAYS.
                        # Previously this fell through to del/n_sold below
                        # anyway, silently dropping the position with no sell
                        # record at all (neither open nor closed) — a trade
                        # just vanished. Leave it open and re-evaluate next
                        # rebalance instead, same as the circuit-lock branch.
                        continue
                    proceeds = pos.qty * price
                    self.cash += proceeds - self._one_leg_cost(price, pos.qty)
                    sell_rank = int(rank_series[ticker]) if ticker in rank_series.index else None
                    self._record_sell(result, ticker, date_str, price, sell_rank)
                    del self.positions[ticker]
                    n_sold += 1

            # 3. Buy new entrants (tickers in target_set not currently held),
            #    highest-momentum-first; force-sell the longest-in-grace
            #    holding to raise cash if the buffer runs out mid-way.
            #    Regime-disabled rebalances (Fix B2) skip new buys entirely
            #    — existing positions stay open, re-evaluated next rebalance.
            new_entrants = (
                []
                if self._is_regime_disabled(date)
                else [t for t in momentum.sort_values(ascending=False).index if t in target_set and t not in self.positions]
            )
            investable_per_slot = self.investable_pct * total_value / self.top_n
            volume_weights = self._volume_weights(date, target_set) if self.volume_weighted else None

            for ticker in new_entrants:
                if self._is_circuit_locked(date, ticker):
                    continue
                price = prices.get(ticker)
                if price is None or pd.isna(price) or price <= 0:
                    continue
                # volume_weighted (Fix B1): scale this slot's allocation by
                # the ticker's trailing dollar-volume relative to the mean
                # across target_set, instead of a flat equal split. A
                # ticker missing from volume_weights (no volume data)
                # falls back to the equal-split investable_per_slot.
                slot_budget = (
                    investable_per_slot * volume_weights[ticker]
                    if volume_weights is not None and ticker in volume_weights
                    else investable_per_slot
                )
                qty = int(slot_budget // price)
                if self.max_pct_of_adtv is not None:
                    ticker_adtv = self._adtv_cr(date, [ticker])
                    adtv_cr = ticker_adtv.get(ticker) if not ticker_adtv.empty else None
                    if adtv_cr is not None and pd.notna(adtv_cr) and adtv_cr > 0:
                        max_qty_by_adtv = int((self.max_pct_of_adtv * adtv_cr * 1e7) // price)
                        qty = min(qty, max_qty_by_adtv)
                if qty <= 0:
                    continue
                cost = self._one_leg_cost(price, qty)
                required_cash = qty * price + cost

                while required_cash > self.cash:
                    grace_holdings = [
                        (t, p) for t, p in self.positions.items() if p.grace_remaining is not None
                    ]
                    if not grace_holdings:
                        break  # no buffer left to draw on; skip/size down this buy
                    oldest_ticker, oldest_pos = min(grace_holdings, key=lambda tp: tp[1].grace_remaining)
                    oldest_price = prices.get(oldest_ticker)
                    if oldest_price is None or pd.isna(oldest_price):
                        break
                    proceeds = oldest_pos.qty * oldest_price
                    self.cash += proceeds - self._one_leg_cost(oldest_price, oldest_pos.qty)
                    oldest_rank = int(rank_series[oldest_ticker]) if oldest_ticker in rank_series.index else None
                    self._record_sell(result, oldest_ticker, date_str, oldest_price, oldest_rank)
                    del self.positions[oldest_ticker]
                    n_sold += 1

                if required_cash > self.cash:
                    qty = int((self.cash / (1 + self.costs.compute_roundtrip_cost_pct(price, 1) / 2.0)) // price)
                    if qty <= 0:
                        continue
                    cost = self._one_leg_cost(price, qty)
                    required_cash = qty * price + cost

                self.cash -= required_cash
                buy_rank = int(rank_series[ticker]) if ticker in rank_series.index else None
                self.positions[ticker] = Position(
                    qty=qty, entry_price=price, entry_date=date_str, entry_rank=buy_rank, grace_remaining=None
                )
                n_bought += 1

            result.rebalance_events.append({"date": date_str, "n_bought": n_bought, "n_sold": n_sold})
            result.equity_curve.append({"date": date_str, "total_value": self._mark_to_market(date, prices)})

        # Flush any SIP contributions due after the last rebalance but
        # before the final date — they still count as real capital
        # committed (and real idle cash sitting in the account, added
        # straight to ending_value below), even though there's no further
        # rebalance left to deploy them.
        trailing_sip_cash = 0.0
        while injection_idx < len(injection_dates):
            self.cash += self.sip_amount
            total_contributed += self.sip_amount
            trailing_sip_cash += self.sip_amount
            cash_flows.append({"date": str(injection_dates[injection_idx].date()), "amount": -self.sip_amount})
            injection_idx += 1

        # Positions still open at the end of the run: surface them in the
        # ledger too (sell fields left None), rather than silently dropping
        # whatever the backtest was still holding on the final date.
        final_date = str(trading_days[-1].date())
        final_prices = self._price_row(trading_days[-1])
        for ticker, pos in self.positions.items():
            price = final_prices.get(ticker)
            mark_price = float(price) if price is not None and pd.notna(price) else None
            holding_days = (pd.Timestamp(final_date) - pd.Timestamp(pos.entry_date)).days
            result.transactions.append({
                "ticker": ticker,
                "buy_date": pos.entry_date,
                "sell_date": None,
                "buy_price": pos.entry_price,
                "sell_price": mark_price,
                "qty": pos.qty,
                "holding_days": holding_days,
                # Unrealized annualized gain to date (mark-to-market, not a
                # realized exit) — same formula as a closed trade's trade_cagr.
                "trade_cagr": trade_cagr(
                    pos.entry_price, mark_price, holding_days,
                    ticker=ticker, buy_date=pos.entry_date, sell_date=final_date,
                ),
                "buy_momentum_rank": pos.entry_rank,
                "sell_momentum_rank": None,
                "status": "open",
            })

        result.ending_value = (result.equity_curve[-1]["total_value"] if result.equity_curve else self.starting_capital) + trailing_sip_cash
        result.cash_flows = cash_flows
        result.total_contributed = total_contributed
        return result
