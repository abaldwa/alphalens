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

from datetime import date as _date
from typing import Any, Dict, Iterable, List, Optional, Set, Union

import pandas as pd

# Tax regime from core/tax.py -- the one declaration every channel taxes
# through. This is a live path (features/momentum_live.py), so it must not
# read the rates from the module being retired with MomentumBacktester.
from backtest.core.tax import LTCG_HOLDING_DAYS, LTCG_RATE, STCG_RATE
from features.momentum_signal import (
    downtrend_tickers,
    orthogonalize_momentum_vs_factors,
    trailing_momentum_from_panel,
    trailing_momentum_skip_recent,
)


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
    volume_panel: Optional[pd.DataFrame] = None,
    market_cap_panel: Optional[pd.DataFrame] = None,
    beta_map: Optional[Dict[str, float]] = None,
    regime_series: Optional[pd.Series] = None,
    quality_scores: Optional[Dict[str, Dict[str, Any]]] = None,
    *,
    min_adtv_cr: float,
    max_pct_of_adtv: float,
    circuit_band_pct: float,
    quality_gate: Dict[str, Any],
    disable_in_high_vol_regime: str,
) -> Dict[str, Dict[str, Any]]:
    """All Risk / Balanced / Risk-Managed / Max-Defensive filter-preset
    dicts, each layering more filters on top of the last:
      - all_risk: unfiltered baseline (zero kwargs).
      - balanced: + ADTV liquidity floor, order-size-vs-ADTV cap,
        circuit-lock proxy, quality gate.
      - risk_managed: + regime-conditional buy-disabling in high-vol
        periods.
      - max_defensive: + size/beta orthogonalization.
    """
    all_risk: Dict[str, Any] = {}
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
# Rank-function builders (Phase 0: R-family strategy dispatch)
# ---------------------------------------------------------------------------
def rank_fn_for_skip_months(skip_months: int) -> Optional[Any]:
    """Returns a rank_fn closure for Jegadeesh-Titman style skip-month
    momentum, or None if skip_months == 0 (use default trailing momentum).

    The returned function has signature: (price_panel, universe, date, lookback_days) -> pd.Series
    """
    if skip_months <= 0:
        return None

    skip_days = skip_months * 21  # Convert months to trading days

    def rank_skip_month(price_panel: pd.DataFrame, universe: List[str], date: str, lookback_days: int) -> pd.Series:
        return trailing_momentum_skip_recent(price_panel, universe, date, lookback_days, skip_days)

    return rank_skip_month


# ---------------------------------------------------------------------------
# The momentum ranking itself — the ONE implementation
# ---------------------------------------------------------------------------
# [ML40, 2026-08-14] Both momentum engines used to rank the universe with
# their own inline call to trailing_momentum_from_panel: MomentumAdapter in
# generate_signals(), MomentumBacktester in run(). Two call sites means two
# places the ranking's date handling, its momentum_panel short-circuit and its
# missing-data convention can drift, which is exactly the backtest-vs-live
# divergence tests/quality/test_one_generator_per_channel.py exists to catch
# (its `duplicate_momentum_ranking` rule names momentum_backtest.py directly).
#
# This function is that one implementation. It lives here rather than in
# either engine because features/ is the shared primitive layer both sides
# legitimately compose — the same reason select_buy_pool and
# decide_grace_transitions already live here.
def rank_universe(
    price_panel: pd.DataFrame,
    universe: List[str],
    date: Union[str, _date, pd.Timestamp],
    lookback_days: int,
    momentum_panel: Optional[pd.DataFrame] = None,
    rank_fn: Optional[Any] = None,
) -> pd.Series:
    """Trailing-momentum score per ticker in `universe`, as of `date`.

    `momentum_panel` is an optional PRE-COMPUTED wide momentum panel (date
    index, ticker columns). When supplied AND it carries a row for this exact
    date, that row is used instead of recomputing from prices — the
    fast path MomentumBacktester already had. Tickers with no score on that
    row are dropped, never defaulted: a NaN means "no real momentum value for
    this ticker on this date", which is not the same as zero momentum.

    Falls back to computing from `price_panel` whenever no panel row is
    available, so the two paths agree on the same missing-data convention.

    `date` may be a datetime.date, a pd.Timestamp or an ISO string — the two
    callers historically passed different types (the adapter a date, the
    backtester a Timestamp), and normalising here is what lets them share
    one function instead of each formatting the date its own way.

    `rank_fn` is an optional callable that computes a custom ranking score
    (e.g., 52-week-high, risk-adjusted composite) instead of the default
    trailing-momentum ranking. When None, defaults to trailing-momentum.
    Signature: rank_fn(price_panel, universe, date, lookback_days) -> pd.Series.
    """
    ts = pd.Timestamp(date)
    if momentum_panel is not None and ts in momentum_panel.index:
        return momentum_panel.loc[ts].reindex(universe).dropna()
    if rank_fn is not None:
        return rank_fn(price_panel, universe, str(ts.date()), lookback_days)
    return trailing_momentum_from_panel(price_panel, universe, str(ts.date()), lookback_days)


def keep_set_for_exit(
    momentum: pd.Series,
    target_set: Set[str],
    exit_rank: Optional[int],
) -> Set[str]:
    """The set a held position must fall OUT of before its grace countdown
    starts — the asymmetric exit band (Tier 1, 2026-08-08).

    Entry stays the top_n (`target_set`). With `exit_rank` set, a held name is
    still considered "kept" while its raw momentum rank is <= exit_rank, so a
    winner that slips from rank 10 to rank 12 under top_n=10/exit_rank=15 is
    ridden rather than rotated out. Only a name beyond exit_rank begins grace.

    None (the default) returns `target_set` unchanged — the symmetric
    behaviour where leaving the top_n immediately starts the countdown.

    Extracted from MomentumBacktester.run()'s inline block (ML40, 2026-08-14)
    so MomentumAdapter can apply the identical rule; ranks are computed with
    method="min" so ties share the better rank, matching the original.
    """
    if exit_rank is None or momentum.empty:
        return target_set
    ranks = momentum.rank(ascending=False, method="min")
    return set(ranks[ranks <= exit_rank].index) | target_set


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
    circuit_band_pct is None, or on insufficient history.

    [ML40, 2026-08-14] This is now the single implementation:
    backtest/adapters/panel_filters.py::is_circuit_locked delegates here
    rather than keeping the second copy it had. The `pos >= len(idx)` bound
    below came from that copy and was MISSING here — searchsorted returns
    len(idx) for a date past the panel's end, so this version raised
    IndexError where the other returned False. Keeping the safer of the two
    behaviours is the point of having one.
    """
    if circuit_band_pct is None or price_panel_ffilled is None or ticker not in price_panel_ffilled.columns:
        return False
    idx = price_panel_ffilled.index
    pos = idx.searchsorted(pd.Timestamp(date))
    if pos <= 0 or pos >= len(idx):
        return False
    prev_price = price_panel_ffilled[ticker].iloc[pos - 1]
    cur_price = price_panel_ffilled[ticker].iloc[pos]
    if pd.isna(prev_price) or pd.isna(cur_price) or prev_price <= 0:
        return False
    ret = (cur_price - prev_price) / prev_price
    locked: bool = abs(ret) >= circuit_band_pct
    return locked


def passes_quality_gate(
    ticker: str, quality_scores: Dict[str, Dict[str, Any]], quality_gate: Dict[str, Any]
) -> bool:
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
    quality_scores: Optional[Dict[str, Dict[str, Any]]] = None,
    quality_gate: Optional[Dict[str, Any]] = None,
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

    if min_adtv_cr is not None and not pool.empty and volume_panel is not None:
        # [ML40, 2026-08-14] Two DIFFERENT missing-data cases, which the two
        # engines used to conflate in opposite directions:
        #
        #   (a) No volume panel at all -> the filter cannot be evaluated for
        #       anyone. Skipped entirely (the `volume_panel is not None` guard
        #       above). This is a run-configuration omission, not evidence
        #       about any ticker, and MomentumBacktester's docstring has always
        #       promised min_adtv_cr is a no-op here. Excluding the whole
        #       universe instead would turn a forgotten argument into an empty
        #       book that still reports itself as a liquidity-filtered run.
        #
        #   (b) Volume panel present, but THIS ticker has no data in it ->
        #       excluded. Here the absence really is about the ticker, and the
        #       standing rule applies: never assume liquid on missing data.
        #
        # `.reindex(pool.index)` is what implements (b) and is load-bearing.
        # adtv_cr returns a row only for tickers present in BOTH panels, so a
        # ticker missing from the volume panel was absent from this Series
        # rather than NaN in it — and therefore never matched the `isna()`
        # test, never got dropped, and was silently treated as LIQUID.
        #
        # MomentumAdapter had the reindex and was correct; this shared
        # function (and so MomentumBacktester, which inlined the same
        # unguarded form) did not. Consolidating the two exposed the
        # divergence — the reason ML40 exists. Taking the safer behaviour
        # means a MomentumBacktester run that supplies a volume panel can now
        # exclude thinly-covered names it previously bought: a fix, not a
        # regression, but it does change results for such runs.
        adtv = adtv_cr(price_panel, volume_panel, date, list(pool.index), adtv_lookback_days).reindex(pool.index)
        illiquid = adtv[adtv.isna() | (adtv < min_adtv_cr)].index
        pool = pool.drop(index=[t for t in illiquid if t in pool.index])

    if circuit_band_pct is not None and not pool.empty:
        locked = [t for t in pool.index if is_circuit_locked(price_panel_ffilled, date, t, circuit_band_pct)]
        pool = pool.drop(index=locked)

    if downtrend_filter_pct is not None and not pool.empty:
        # Tickers with no short-term-window history stay eligible (never
        # excluded on missing data) — only a confirmed >=threshold drop over
        # the window excludes a ticker. [ML40] Delegates to momentum_signal's
        # downtrend_tickers, the same helper panel_filters already calls,
        # instead of re-deriving the threshold test from the raw primitive.
        sharply_down = downtrend_tickers(
            price_panel, list(pool.index), str(date.date()), downtrend_filter_pct, downtrend_lookback_days,
        )
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
    held: Iterable[str],
    universe: List[str],
    date: pd.Timestamp,
    rank_start: Optional[int],
    yearly_rank_lookup: Optional[Dict[pd.Timestamp, Dict[str, int]]],
) -> List[str]:
    """Currently-held tickers that have been PROMOTED out of
    the active band (missing from `universe`) but should still be ranked
    and allowed to genuinely compete for a top_n slot on their real
    momentum score, instead of being force-exited purely because they
    changed bands. Empty (fully off) unless both rank_start and
    yearly_rank_lookup are supplied — opt-in, matching every other
    optional filter's default-off convention.

    Promotion is judged on MARKET-CAP rank only (2026-08-18 user decision) —
    never on ADTV rank. A name that falls out of the liquid universe is sold,
    because liquidity is a tradability constraint rather than a ranking
    artifact.

    Only reads `held`, never `target` or the price panel, so by construction
    this can never introduce a ticker that isn't already held — a promoted
    name that was never bought, or has already fully exited, isn't in `held`
    and is not returned.
    """
    held = list(held)
    if rank_start is None or not yearly_rank_lookup or not held:
        return []
    applicable_starts = [d for d in yearly_rank_lookup if d <= date]
    if not applicable_starts:
        return []
    ranks = yearly_rank_lookup[max(applicable_starts)]
    in_universe = set(universe)
    promoted = []
    for ticker in held:
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


def compute_fy_net_tax(closed_transactions_in_fy: List[Dict[str, Any]]) -> float:
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
        return min(grace_candidates.items(), key=lambda tp: getattr(tp[1], "grace_remaining"))[0]
    return max(candidates.items(), key=lambda tp: getattr(tp[1], "entry_rank", None) or 0)[0]


# ---------------------------------------------------------------------------
# Phase 4: Sector momentum — two-stage ranking
# ---------------------------------------------------------------------------
def rank_sectors(
    momentum: pd.Series,
    sector_lookup: Dict[str, str],
    top_sectors: int = 5,
) -> pd.Series:
    """Rank sectors by average momentum of their constituents, returning
    a sector-level momentum score for each sector in the input.

    momentum : ticker -> momentum_score Series
    sector_lookup : ticker -> sector mapping
    top_sectors : used by the caller to select which sectors are "top"

    Returns a Series indexed by sector name with average momentum score.
    Tickers with "Unknown" or missing sector are grouped together as
    "Unknown" (never excluded on missing data per the strategy convention)."""
    if momentum.empty or not sector_lookup:
        return pd.Series(dtype=float)

    sector_scores: dict[str, list[float]] = {}
    for ticker, score in momentum.items():
        sector = sector_lookup.get(ticker, "Unknown")
        if sector not in sector_scores:
            sector_scores[sector] = []
        sector_scores[sector].append(score)

    sector_avg: dict[str, float] = {}
    for sector, scores in sector_scores.items():
        if scores:
            sector_avg[sector] = pd.Series(scores).mean()

    return pd.Series(sector_avg).sort_values(ascending=False)


def rank_constituents_within_sectors(
    momentum: pd.Series,
    sector_lookup: Dict[str, str],
    top_sectors_list: List[str],
) -> pd.Series:
    """Filter momentum scores to only tickers within the specified top
    sectors. Used as the second stage of two-stage sector momentum ranking.

    momentum : ticker -> momentum_score Series
    sector_lookup : ticker -> sector mapping
    top_sectors_list : list of sector names that are "top" (already ranked)

    Returns filtered momentum Series with only constituents of top sectors,
    preserving original momentum scores (no re-weighting)."""
    if momentum.empty or not sector_lookup or not top_sectors_list:
        return momentum

    top_sectors_set = set(top_sectors_list)
    top_constituents = []
    for ticker, score in momentum.items():
        sector = sector_lookup.get(ticker, "Unknown")
        if sector in top_sectors_set:
            top_constituents.append((ticker, score))

    if not top_constituents:
        return pd.Series(dtype=float)

    result = pd.Series(dict(top_constituents))
    return result.sort_values(ascending=False)
