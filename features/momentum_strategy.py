"""
features/momentum_strategy.py

Phase: FeatureBacklog.md ML38 — momentum strategy consolidation (2026-08-09)
Owner: Platform / Backtest
Consumers: backtest/momentum_backtest.py (MomentumBacktester), features/momentum_live.py

The single canonical home for every momentum STRATEGY decision (universe
selection filters, sell/exit rules, category filter presets). Extracted
from backtest/momentum_backtest.py's MomentumBacktester, which used to
implement all of this inline, standalone — meaning the live daily
signal path (features/momentum_live.py) had no access to it and only
ever did plain top-N ranking + grace-period hold/exit, silently missing
every risk filter ("Balanced"/"Risk-Managed"/"Max-Defensive") that the
backtest was actually validated with.

This is a MECHANICAL extraction, not a rewrite: every function here is
moved out of MomentumBacktester with its exact prior logic preserved
(verified via a before/after parity diff — see tests/unit/test_momentum_backtest.py
and tests/unit/test_momentum_strategy.py), so both MomentumBacktester and
features/momentum_live.py can call the exact same code instead of two
independently-maintained copies. Functions here are pure (take panels/
config, return decisions) and are engine-state-free by design — they
never touch cash, positions, or transaction ledgers, which stay the
backtest engine's job.

decide_grace_transitions was already extracted this way on 2026-07-14
(previously living in backtest/momentum_backtest.py); it moves here too
so there is exactly one "strategy decisions" module instead of a mix of
"some in features/, some in backtest/".

sticky_promoted_holdings is ported from backtest/adapters/momentum_adapter.py
(2026-08-05), the one genuinely new decision rule found there that
MomentumBacktester didn't have — kept opt-in/default-off, same convention
as every other optional filter in this module.
"""

from typing import Dict, List, Optional, Set

import pandas as pd

from backtest.momentum_tax import LTCG_HOLDING_DAYS, LTCG_RATE, STCG_RATE
from features.momentum_signal import orthogonalize_momentum_vs_factors, trailing_momentum_from_panel


# ---------------------------------------------------------------------------
# Category filter presets
# ---------------------------------------------------------------------------
# Relocated from scripts/run_momentum_dynamic_report.py::_build_strategies()
# so the sweep script and the live signal path share one definition instead
# of the sweep script owning a local, only-it-knows-about copy. Each preset
# is a dict of MomentumBacktester/momentum_strategy kwargs; volume_panel/
# market_cap_panel/beta_map/quality_scores/regime_series are real per-run
# data objects threaded in by the caller (build_category_presets), not
# static config, so they can't be module-level constants.
def build_category_presets(
    volume_panel=None,
    market_cap_panel=None,
    beta_map=None,
    regime_series=None,
    quality_scores=None,
    *,
    min_adtv_cr: float,
    max_pct_of_adtv: float,
    circuit_band_pct: float,
    quality_gate: Dict,
    disable_in_high_vol_regime: str,
) -> Dict[str, Dict]:
    """All Risk / Balanced / Risk-Managed / Max-Defensive filter-preset
    dicts, each layering more filters on top of the last:
      - all_risk: unfiltered baseline (zero kwargs).
      - balanced: + ADTV liquidity floor, order-size-vs-ADTV cap,
        circuit-lock proxy, quality gate.
      - risk_managed: + regime-conditional buy-disabling in high-vol
        periods.
      - max_defensive: + size/beta orthogonalization.
    """
    all_risk: Dict = {}
    balanced = {
        "volume_panel": volume_panel,
        "min_adtv_cr": min_adtv_cr,
        "max_pct_of_adtv": max_pct_of_adtv,
        "circuit_band_pct": circuit_band_pct,
        "quality_scores": quality_scores,
        "quality_gate": quality_gate,
    }
    risk_managed = dict(balanced, regime_series=regime_series, disable_in_regimes={disable_in_high_vol_regime})
    max_defensive = dict(
        risk_managed, orthogonalize_vs_size_beta=True, market_cap_panel=market_cap_panel, beta_map=beta_map,
    )
    return {
        "all_risk": all_risk,
        "balanced": balanced,
        "risk_managed": risk_managed,
        "max_defensive": max_defensive,
    }


# ---------------------------------------------------------------------------
# Grace-period hold/exit rule
# ---------------------------------------------------------------------------
def decide_grace_transitions(
    held_tickers: Dict[str, Optional[int]],
    target_set: Set[str],
    grace_cycles: int,
) -> Dict[str, Optional[int]]:
    """Pure grace-period decision rule (originally extracted 2026-07-14 into
    backtest/momentum_backtest.py; relocated here 2026-08-09 so it lives on
    the strategy side, not the backtest side).

    held_tickers : {ticker: current grace_remaining}, None = core (was in
        the prior period's top_n, never entered grace).
    target_set : this period's top_n ticker set (or exit_rank-widened
        keep_set — see MomentumBacktester.run()'s asymmetric-exit-band
        handling).
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


# ---------------------------------------------------------------------------
# Liquidity / circuit-lock helpers (shared by filtering AND cost sizing)
# ---------------------------------------------------------------------------
def adtv_cr(
    price_panel: pd.DataFrame,
    volume_panel: Optional[pd.DataFrame],
    date: pd.Timestamp,
    tickers: List[str],
    lookback_days: int,
) -> pd.Series:
    """Trailing lookback_days-day average daily traded value (INR crore)
    per ticker, ending on `date` — price(t) * volume(t). NaN days simply
    don't contribute to the mean (no forward-fill of volume). Empty/
    no-data tickers get NaN, which callers treat as "excluded" (never
    assumed liquid on missing data)."""
    if volume_panel is None or not tickers:
        return pd.Series(dtype=float)
    cols = [t for t in tickers if t in volume_panel.columns and t in price_panel.columns]
    if not cols:
        return pd.Series(dtype=float)
    window_prices = price_panel[cols].loc[:date].tail(lookback_days)
    window_volume = volume_panel[cols].loc[:date].tail(lookback_days)
    traded_value_cr = (window_prices * window_volume) / 1e7
    return traded_value_cr.mean(skipna=True)


def is_circuit_locked(
    price_panel_ffilled: pd.DataFrame,
    date: pd.Timestamp,
    ticker: str,
    circuit_band_pct: Optional[float],
) -> bool:
    """True if `ticker`'s realized 1-day return into `date` (via the
    forward-filled close series) meets/exceeds circuit_band_pct in either
    direction — a coarse proxy for "likely circuit-locked, don't trust
    this close as a fillable price". False (never locked) when
    circuit_band_pct is None, or on insufficient history."""
    if circuit_band_pct is None or ticker not in price_panel_ffilled.columns:
        return False
    idx = price_panel_ffilled.index
    pos = idx.searchsorted(date)
    if pos <= 0:
        return False
    prev_price = price_panel_ffilled[ticker].iloc[pos - 1]
    cur_price = price_panel_ffilled[ticker].iloc[pos]
    if pd.isna(prev_price) or pd.isna(cur_price) or prev_price <= 0:
        return False
    ret = (cur_price - prev_price) / prev_price
    return abs(ret) >= circuit_band_pct


def passes_quality_gate(ticker: str, quality_scores: Dict, quality_gate: Dict) -> bool:
    """False only if `ticker` has quality_scores AND fails an
    explicitly-set threshold. True (passes) for a ticker missing from
    quality_scores entirely, or when quality_gate is empty — never
    excludes on missing data."""
    if not quality_gate:
        return True
    scores = quality_scores.get(ticker)
    if scores is None:
        return True
    min_f = quality_gate.get("min_f_score")
    if min_f is not None:
        f_score = scores.get("f_score")
        if f_score is not None and f_score < min_f:
            return False
    max_m = quality_gate.get("max_m_score")
    if max_m is not None:
        m_score = scores.get("m_score")
        if m_score is not None and m_score > max_m:
            return False
    return True


def is_regime_disabled(regime_series: Optional[pd.Series], disable_in_regimes: Set[str], date: pd.Timestamp) -> bool:
    """True if `date`'s active regime (most recent regime_series entry on
    or before `date`) is in disable_in_regimes. False (never disabled)
    when regime_series is None or has no entry on/before `date` (unknown
    regime, never-exclude-on-missing-data convention)."""
    if regime_series is None or not disable_in_regimes:
        return False
    eligible = regime_series.loc[:date].dropna()
    if eligible.empty:
        return False
    return eligible.iloc[-1] in disable_in_regimes


def trailing_stop_check(price: float, peak_price: Optional[float], trailing_stop_pct: float) -> "tuple[float, bool]":
    """Pure trailing-stop decision: given today's price and the position's
    running peak, returns (updated_peak, should_stop). Before a name has
    risen, peak == entry_price, so this also acts as a stop-loss that cuts
    fresh losers from day one. Caller (the backtest engine) owns the
    actual sell mechanics (cash movement, transaction recording)."""
    new_peak = price if peak_price is None or price > peak_price else peak_price
    floor_price = new_peak * (1 - trailing_stop_pct)
    return new_peak, price <= floor_price


# ---------------------------------------------------------------------------
# Buy-pool selection — the full filter chain, in the exact order
# MomentumBacktester.run() previously applied inline
# ---------------------------------------------------------------------------
def select_buy_pool(
    selection_pool: pd.Series,
    date: pd.Timestamp,
    *,
    price_panel: pd.DataFrame,
    price_panel_ffilled: pd.DataFrame,
    volume_panel: Optional[pd.DataFrame] = None,
    quality_scores: Optional[Dict] = None,
    quality_gate: Optional[Dict] = None,
    approximation_flags: Optional[Dict[pd.Timestamp, Dict[str, bool]]] = None,
    exclude_approximated_mcap: bool = False,
    orthogonalize_vs_size_beta: bool = False,
    market_cap_panel: Optional[pd.DataFrame] = None,
    beta_map: Optional[Dict[str, float]] = None,
    min_adtv_cr: Optional[float] = None,
    adtv_lookback_days: int = 20,
    circuit_band_pct: Optional[float] = None,
    downtrend_filter_pct: Optional[float] = None,
    downtrend_lookback_days: int = 20,
    per_ticker_hmm_regime: Optional[Dict[str, pd.DataFrame]] = None,
    disable_hmm_regimes: Optional[Set[float]] = None,
) -> pd.Series:
    """Applies every optional buy-side filter to `selection_pool` (a
    momentum-score Series indexed by ticker), in the same order
    MomentumBacktester.run() used inline: quality gate -> approximated-mcap
    exclusion -> size/beta orthogonalization -> ADTV liquidity floor ->
    circuit-lock -> downtrend filter -> HMM regime filter. Every filter is
    a no-op when its corresponding config is None/empty, matching each
    filter's individual documented default-off behavior. Returns the
    filtered Series (same values, fewer/reweighted rows)."""
    quality_scores = quality_scores or {}
    quality_gate = quality_gate or {}
    approximation_flags = approximation_flags or {}
    disable_hmm_regimes = disable_hmm_regimes if disable_hmm_regimes is not None else {0.0}
    per_ticker_hmm_regime = per_ticker_hmm_regime or {}

    pool = selection_pool

    if quality_gate and not pool.empty:
        failing = [t for t in pool.index if not passes_quality_gate(t, quality_scores, quality_gate)]
        pool = pool.drop(index=failing)

    if exclude_approximated_mcap and approximation_flags and not pool.empty:
        applicable_starts = [d for d in approximation_flags if d <= date]
        if applicable_starts:
            flags = approximation_flags[max(applicable_starts)]
            approximated = [t for t in pool.index if flags.get(t)]
            pool = pool.drop(index=approximated)

    if orthogonalize_vs_size_beta and market_cap_panel is not None and not pool.empty:
        mcap_history = market_cap_panel.loc[:date]
        mcap_row = mcap_history.iloc[-1] if not mcap_history.empty else pd.Series(dtype=float)
        mcap_for_pool = mcap_row.reindex(pool.index)
        beta_for_pool = pd.Series(
            {t: beta_map[t] for t in pool.index if beta_map and t in beta_map}
        ).reindex(pool.index)
        residual = orthogonalize_momentum_vs_factors(pool, mcap_for_pool, beta_for_pool)
        # Tickers dropped by the regression (missing mcap/beta) keep their
        # original raw momentum score rather than being silently excluded.
        pool = residual.combine_first(pool)

    if min_adtv_cr is not None and not pool.empty:
        adtv = adtv_cr(price_panel, volume_panel, date, list(pool.index), adtv_lookback_days)
        illiquid = adtv[adtv.isna() | (adtv < min_adtv_cr)].index
        pool = pool.drop(index=[t for t in illiquid if t in pool.index])

    if circuit_band_pct is not None and not pool.empty:
        locked = [t for t in pool.index if is_circuit_locked(price_panel_ffilled, date, t, circuit_band_pct)]
        pool = pool.drop(index=locked)

    if downtrend_filter_pct is not None and not pool.empty:
        short_term = trailing_momentum_from_panel(price_panel, list(pool.index), str(date.date()), downtrend_lookback_days)
        # Tickers with no short-term-window history stay eligible (never
        # excluded on missing data) — only a confirmed >=threshold drop
        # over the window excludes a ticker.
        sharply_down = short_term[short_term <= -downtrend_filter_pct].index
        pool = pool.drop(index=[t for t in sharply_down if t in pool.index])

    if per_ticker_hmm_regime and not pool.empty:
        bearish_tickers = set()
        for ticker in pool.index:
            if ticker in per_ticker_hmm_regime:
                regime_df = per_ticker_hmm_regime[ticker]
                eligible_regimes = regime_df.loc[:date]
                if not eligible_regimes.empty:
                    latest_regime = eligible_regimes["hmm_regime"].iloc[-1]
                    if pd.notna(latest_regime) and latest_regime in disable_hmm_regimes:
                        bearish_tickers.add(ticker)
        pool = pool.drop(index=[t for t in bearish_tickers if t in pool.index])

    return pool


# ---------------------------------------------------------------------------
# Sticky-promotion — ported from backtest/adapters/momentum_adapter.py
# ---------------------------------------------------------------------------
def sticky_promoted_holdings(
    held_grace: Dict[str, Optional[int]],
    universe: List[str],
    date: pd.Timestamp,
    rank_start: Optional[int],
    yearly_rank_lookup: Optional[Dict[pd.Timestamp, Dict[str, int]]],
) -> List[str]:
    """Currently-held (or in-grace) tickers that have been PROMOTED out of
    the active band (missing from `universe`) but should still be ranked
    and allowed to genuinely compete for a top_n slot on their real
    momentum score, instead of being force-exited purely because they
    changed bands. Empty (fully off) unless both rank_start and
    yearly_rank_lookup are supplied — opt-in, matching every other
    optional filter's default-off convention.

    Ported as-is from backtest/adapters/momentum_adapter.py's
    _sticky_promoted_holdings (2026-08-05) — the one behavior
    MomentumBacktester didn't have. Only reads held_grace, never `target`
    or the price panel, so by construction this can never introduce a
    ticker that isn't already held — a promoted name that was never
    bought, or has already fully exited, isn't in held_grace and is not
    returned.
    """
    if rank_start is None or not yearly_rank_lookup or not held_grace:
        return []
    applicable_starts = [d for d in yearly_rank_lookup if d <= date]
    if not applicable_starts:
        return []
    ranks = yearly_rank_lookup[max(applicable_starts)]
    in_universe = set(universe)
    promoted = []
    for ticker in held_grace:
        if ticker in in_universe:
            continue
        rank = ranks.get(ticker)
        # No rank at all (left the tracked universe/delisted) or a
        # worse-or-equal rank (demoted) => no special treatment.
        if rank is not None and rank < rank_start:
            promoted.append(ticker)
    return promoted


# ---------------------------------------------------------------------------
# Year-on-year capital-gains tax withholding (2026-08-09, user-requested
# correction — see backtest/momentum_tax.py's module docstring for the
# per-transaction rates this builds on)
# ---------------------------------------------------------------------------


def compute_fy_net_tax(closed_transactions_in_fy: List[Dict]) -> float:
    """Net capital-gains tax owed for ONE fiscal year: gains are netted within
    the STCG and LTCG buckets, the Indian inter-head set-off is applied, and
    each remaining positive bucket is taxed at its rate.

    [BUG FIX 2026-08-12] This used to floor each bucket at zero independently
    and documented that as a deliberate simplification. It is not a
    simplification — it is wrong. A net SHORT-TERM loss may be set off against
    long-term gains (Income-tax Act s.70/s.74); only a long-term loss is
    restricted to its own bucket. Flooring both independently overstates tax in
    any FY that pairs a short-term loss with a long-term gain. The same defect
    existed in backtest/core/tax.py and was measured there on the 2009-2026
    technical sweep: 69 of 390 runs affected, ~Rs 31.25 lakh overstated.

    The set-off now comes from backtest.core.tax.apply_stcg_loss_setoff rather
    than being reimplemented here, because two independent copies of this rule
    is exactly how the two channels came to disagree with the law in the same
    way but at different call sites.

    Loss carry-forward into a later FY remains unmodeled (real law allows 8
    years); that IS a genuine simplification, and a shared one — see the note
    in backtest/core/tax.py::net_buckets_after_setoff.

    `closed_transactions_in_fy` must already be filtered by the caller to
    trades whose sell_date falls within the target fiscal year — this
    function does no date filtering itself, only the STCG/LTCG netting.
    """
    from backtest.core.tax import apply_stcg_loss_setoff

    stcg_net = 0.0
    ltcg_net = 0.0
    for t in closed_transactions_in_fy:
        if t.get("sell_price") is None:
            continue
        gain = (t["sell_price"] - t["buy_price"]) * t["qty"]
        if t["holding_days"] >= LTCG_HOLDING_DAYS:
            ltcg_net += gain
        else:
            stcg_net += gain
    stcg_net, ltcg_net = apply_stcg_loss_setoff(stcg_net, ltcg_net)
    return max(stcg_net, 0.0) * STCG_RATE + max(ltcg_net, 0.0) * LTCG_RATE


def fy_end_dates_through(start_date: pd.Timestamp, end_date: pd.Timestamp) -> List[pd.Timestamp]:
    """Every Indian fiscal-year-end (March 31) on or before `end_date`,
    starting from the FY containing `start_date` — the withholding
    schedule a backtest walks: each date here is a point where the prior
    FY's realized gains become taxable and due. A partial FY still in
    progress at `end_date` has no boundary here (its tax isn't due yet)."""
    first_fy_end_year = start_date.year if start_date.month >= 4 else start_date.year - 1
    boundaries = []
    year = first_fy_end_year + 1
    while True:
        fy_end = pd.Timestamp(year=year, month=3, day=31)
        if fy_end > end_date:
            break
        boundaries.append(fy_end)
        year += 1
    return boundaries


def select_forced_sell_for_shortfall(
    positions: Dict[str, object],
    prices: pd.Series,
    exclude: Optional[Set[str]] = None,
) -> Optional[str]:
    """Pick the next held ticker to force-sell to raise cash (FY tax
    withholding, or a buy that outran the cash buffer) — grace-period
    holdings closest to expiry first (they're leaving soon regardless),
    then the weakest core holding by entry_rank (highest/worst rank
    number). Requires a real, non-NaN price to be sellable this date;
    `positions` values are duck-typed (need only `.grace_remaining` and
    `.entry_rank` attributes) so this works against MomentumBacktester's
    Position dataclass without importing it here."""
    exclude = exclude or set()
    candidates = {
        t: p for t, p in positions.items()
        if t not in exclude and pd.notna(prices.get(t))
    }
    if not candidates:
        return None
    grace_candidates = {t: p for t, p in candidates.items() if getattr(p, "grace_remaining", None) is not None}
    if grace_candidates:
        return min(grace_candidates.items(), key=lambda tp: tp[1].grace_remaining)[0]
    return max(candidates.items(), key=lambda tp: getattr(tp[1], "entry_rank", None) or 0)[0]
