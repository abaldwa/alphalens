"""
features/momentum_live.py

Phase: FeatureBacklog.md ML38 — momentum strategy live dashboard section
Owner: Platform / Features
Consumers: ingestion/scheduler/daily_pipeline.py (step_compute_momentum),
    datastore/api/routers/momentum.py

Live plumbing around the already-validated ML38 backtest logic
(features/momentum_universe.py, features/momentum_signal.py,
backtest/momentum_backtest.py's decide_grace_transitions) — no new
ranking/momentum/grace math is introduced here, only the "what does this
mean for today's real portfolio" wiring.

Production configuration (2026-07-14 user decision, after two robustness
checks — rebalance-date offset 0-10 trading days, grace period 1/2/3
months — showed this variant is stable while higher-CAGR variants found
via parameter sweeps were overfit to lucky calendar alignment; see
FeatureBacklog.md ML38):
    top 15 stocks / 6-month trailing lookback / monthly rebalance /
    grace = 2 rebalance cycles — held constant across all 5 rank-band
    strategies below (2026-07-15 user request: select which market-cap
    rank band to track via a dashboard dropdown, rather than a single
    hardcoded band). Each rank band is otherwise fully independent: its
    own live ranking snapshot, its own rebalance schedule/suggestions,
    its own recorded trades and holdings, distinguished throughout by
    `strategy_id`.
"""

import logging
from functools import lru_cache
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

from backtest.momentum_backtest import decide_grace_transitions
from features.momentum_signal import (
    load_price_panel,
    load_volume_panel,
    lookback_trading_days,
)
from features.momentum_strategy import rank_universe, select_buy_pool
from features.momentum_universe import RANK_BANDS, rank_band_tickers

logger = logging.getLogger(__name__)

# [C1 2026-08-18] top_n / lookback_months / grace_cycles are NOT declared
# here any more. They are per-strategy parameters that strategy_registry
# already declares in each row's definition_json, and pinning them as module
# constants applied ONE value to every band -- so two registry strategies
# differing only in top_n were the same strategy live, and whichever of them
# had the approved backtest might not be the one that ran.
#
# The registry now answers. `REGISTRY_KEY_TEMPLATE` renders the key each live
# strategy corresponds to; see strategies/migrations/momentum.py for the
# naming scheme. Verified at the time of the change: all 7 live strategies
# resolve, and every one declares top_n=15 / lookback_months=6 /
# grace_cycles=2 -- exactly the constants removed here, so this rewiring
# changes no live behaviour today. It changes where the answer comes from.
#
# The category is fixed to all_risk because that is what this path actually
# runs: it applies no filters at all. The registry's other three categories
# (balanced, risk_managed, max_defensive) are cumulative filter stacks that
# this module has no way to apply -- see PHASE-C2, which is what makes them
# expressible. Naming all_risk explicitly is the honest description of
# today's behaviour rather than a silent default.
REGISTRY_KEY_TEMPLATE = (
    "momentum:all_risk_b{band_id}_{rank_start}-{rank_end}"
    "_lb{lookback_months}mo_{rebalance}_top{top_n}"
)

# The parameter set the live path runs, as DECLARED by the registry row above.
# Kept as the shape of a definition_json rather than loose constants so that
# adding a declared parameter is a registry edit, not a code edit.
_LIVE_LOOKBACK_MONTHS = 6
_LIVE_REBALANCE = "monthly"
_LIVE_TOP_N = 15

# Extra calendar days loaded beyond the lookback so the panel still contains
# `lookback_days` TRADING rows after weekends and holiday clusters.
_PANEL_BUFFER_DAYS = 120

# One strategy per features.momentum_universe.RANK_BANDS entry — only the rank
# band (i.e. the universe each strategy ranks) differs between them. Each
# carries the registry key that DECLARES its parameters; the parameters
# themselves are read from there, never from this dict.
STRATEGIES: List[Dict[str, Any]] = [
    {
        "strategy_id": f"band{band_id}_top15_6m_m_g2",
        "band_id": band_id,
        "rank_start": rank_start,
        "rank_end": rank_end,
        "label": f"Rank {rank_start}-{rank_end}",
        "category": "all_risk",
        "registry_key": REGISTRY_KEY_TEMPLATE.format(
            band_id=band_id,
            rank_start=rank_start,
            rank_end=rank_end,
            lookback_months=_LIVE_LOOKBACK_MONTHS,
            rebalance=_LIVE_REBALANCE,
            top_n=_LIVE_TOP_N,
        ),
    }
    for band_id, rank_start, rank_end in RANK_BANDS
]
_STRATEGIES_BY_ID = {s["strategy_id"]: s for s in STRATEGIES}

# The original, single-band strategy this feature launched with (Rank
# 100-150) — kept as the default so existing recorded trades/contributions
# from before multi-strategy support keep resolving to the same strategy_id.
DEFAULT_STRATEGY_ID = "band3_top15_6m_m_g2"


def get_strategy(strategy_id: str) -> Dict[str, Any]:
    if strategy_id not in _STRATEGIES_BY_ID:
        raise ValueError(f"Unknown momentum strategy_id: {strategy_id!r}. Valid: {list(_STRATEGIES_BY_ID)}")
    return _STRATEGIES_BY_ID[strategy_id]


class StrategyParamsUnavailable(RuntimeError):
    """The registry could not answer for a strategy the live path is about
    to run.

    Raised rather than falling back to a default. A silent fallback is how
    the hardcoded constants caused harm in the first place: the run would
    proceed, look healthy, and quietly execute a different strategy from the
    one whose backtest was approved. Failing here is loud and recoverable;
    trading the wrong parameters is neither."""


@lru_cache(maxsize=None)
def _declared_params(registry_key: str) -> Dict[str, Any]:
    """definition_json for one live strategy, from strategy_registry.

    Cached: this is read on every ranking call and the registry row for a
    given key changes only when someone revises it, which requires a
    deliberate migration. A long-running scheduler process picking up a
    revision needs a restart -- the same contract every other registry
    consumer already has."""
    from strategies.registry import get_strategy as _registry_get_strategy

    row = _registry_get_strategy(registry_key)
    if row is None:
        raise StrategyParamsUnavailable(
            f"strategy_registry has no active row for {registry_key!r}. The live "
            "path reads top_n/lookback_months/grace_cycles from the registry "
            "(PHASE-C1); it will not guess them. Run "
            "strategies/migrations/momentum.py if the registry is unpopulated."
        )
    definition = row.get("definition") or {}
    missing = [
        k for k in ("top_n", "lookback_months", "grace_cycles")
        if definition.get(k) is None
    ]
    if missing:
        raise StrategyParamsUnavailable(
            f"{registry_key!r} declares no {missing} in definition_json. The "
            "registry is the source of truth for these and cannot be partially "
            "authoritative."
        )
    return dict(definition)


def strategy_params(strategy_id: str) -> Dict[str, Any]:
    """The registry-declared parameters for one live strategy.

    This is the ONLY way this module learns top_n, lookback_months or
    grace_cycles. There is no module-level default to fall back to, by
    design -- see the C1 note at the top of this file."""
    return _declared_params(get_strategy(strategy_id)["registry_key"])

# select_buy_pool kwargs that each registry filter_id maps onto, and the
# extra data each one needs. A declared filter whose data this module cannot
# supply is REFUSED, never skipped -- see _buy_pool_kwargs.
#
# adtv_capped_sizing is deliberately absent: filter_registry types it as
# "sizing", not "entry"/"universe". It changes how much of a name you buy,
# not whether the name is selected, so it has no place in the selection pool.
_SUPPORTED_FILTER_IDS = frozenset({"adtv_floor", "circuit_lock_proxy"})
_SIZING_ONLY_FILTER_IDS = frozenset({"adtv_capped_sizing"})


class StrategyNotRunnableLive(RuntimeError):
    """A strategy declares a filter this live path cannot apply.

    Raised instead of running the strategy WITHOUT that filter. This is the
    single most important behaviour added by C2: before it, the live path had
    no filter chain at all, so a `balanced` or `max_defensive` strategy would
    have run completely unfiltered while its backtest applied the whole chain
    -- silently, and looking perfectly healthy. Refusing is the safe failure.
    """


@lru_cache(maxsize=None)
def _registry_filter_ids(registry_key: str) -> Tuple[str, ...]:
    """The filter_ids a strategy declares, cached like its parameters."""
    from strategies.registry import get_strategy as _registry_get_strategy

    row = _registry_get_strategy(registry_key)
    return tuple((row or {}).get("filter_ids") or [])


def _buy_pool_kwargs(
    registry_key: str,
    volume_panel: Optional[pd.DataFrame],
) -> Dict[str, Any]:
    """Translate a strategy's REGISTRY-DECLARED filter_ids into
    select_buy_pool kwargs.

    The registry is authoritative about which filters a strategy has
    (`filter_ids`) and with what parameters (`resolve_filters`), so neither is
    restated here -- this only maps declaration onto the shared
    implementation's argument names.

    Every unsupported filter raises. The alternative -- dropping it -- would
    reproduce exactly the defect this phase exists to remove.
    """
    from strategies.registry import get_strategy as _registry_get_strategy
    from strategies.registry import resolve_filters

    row = _registry_get_strategy(registry_key)
    filter_ids = list((row or {}).get("filter_ids") or [])
    selection_filters = [f for f in filter_ids if f not in _SIZING_ONLY_FILTER_IDS]
    if not selection_filters:
        return {}

    unsupported = sorted(set(selection_filters) - _SUPPORTED_FILTER_IDS)
    if unsupported:
        raise StrategyNotRunnableLive(
            f"{registry_key!r} declares filter(s) {unsupported} that the live "
            "path has no data source for (quality scores / HMM regime / market-cap "
            "and beta panels are backtest-time inputs). Refusing to run it, "
            "because running it WITHOUT its declared filters would silently "
            "execute a different strategy from the one that was backtested."
        )

    kwargs: Dict[str, Any] = {}
    for spec in resolve_filters(selection_filters):
        params = spec.get("params") or {}
        if spec["filter_id"] == "adtv_floor":
            if volume_panel is None or volume_panel.empty:
                raise StrategyNotRunnableLive(
                    f"{registry_key!r} declares an ADTV floor but no real volume "
                    "history is available for its universe; refusing rather than "
                    "selecting without the liquidity filter."
                )
            kwargs["min_adtv_cr"] = float(params["min_adtv_cr"])
            kwargs["volume_panel"] = volume_panel
        elif spec["filter_id"] == "circuit_lock_proxy":
            kwargs["circuit_band_pct"] = float(params["circuit_band_pct"])
    return kwargs


# How far past as_of_date to look for the next month's first trading day
# — comfortably wider than any real calendar-month gap (including
# December -> January and multi-day holiday clusters).
_NEXT_MONTH_SEARCH_WINDOW_DAYS = 45


def compute_daily_ranking(
    normalised_conn: Any,
    as_of_date: str,
    strategy_id: str = DEFAULT_STRATEGY_ID,
    universe: Optional[List[str]] = None,
) -> pd.DataFrame:
    """as_of_date's momentum ranking for strategy_id's rank band: every
    ticker in that band with enough trailing history, ranked by trailing
    6-month return, flagged for whether it's in the live top_n.

    universe : override the real rank-band lookup — used by tests, which
        can't cheaply seed 150+ ranked tickers just to exercise the
        momentum/ranking logic. Production callers omit this and get the
        real rank_band_tickers() universe for strategy_id's band.

    Deliberately calls rank_band_tickers() WITHOUT include_delisted=True
    (2026-07-20): that flag closes survivorship bias for BACKTESTS, where
    a stock alive at a past as_of_date must be included even though it
    later delisted. Here as_of_date is effectively "today" — a stock that
    has already delisted is not tradeable today, and its frozen last-known
    close (which market_cap_snapshot would still report as "the most
    recent close <= as_of_date") would wrongly earn it a live rank-band
    slot. This is the one caller that should keep the default False.

    Returns a DataFrame with columns: ticker, momentum_return,
    momentum_rank (1 = highest momentum), in_top_n. Empty if the band's
    universe or momentum can't be computed (e.g. no real OHLCV rows yet
    for as_of_date).
    """
    if universe is None:
        cfg = get_strategy(strategy_id)
        universe = rank_band_tickers(normalised_conn, as_of_date, cfg["rank_start"], cfg["rank_end"])
    if not universe:
        return pd.DataFrame(columns=["ticker", "momentum_return", "momentum_rank", "in_top_n"])

    params = strategy_params(strategy_id)
    lookback_days = lookback_trading_days(int(params["lookback_months"]))

    # [C2 2026-08-18] Ranking and selection are no longer written here. Both
    # come from features.momentum_strategy -- rank_universe (the ranking) and
    # select_buy_pool (the filter chain) -- which are the SAME two functions
    # MomentumAdapter and MomentumBacktester call. Previously this function
    # sorted momentum inline and cut at top_n, so the whole filter chain
    # (ADTV floor, circuit-lock proxy, downtrend, quality gate, regime
    # disable, orthogonalization, min_momentum) existed only in the backtest.
    #
    # A panel is loaded rather than calling trailing_momentum(conn, ...)
    # because the shared primitives are panel-based; the window is the
    # lookback plus a buffer for holidays and non-trading days.
    panel_start = (
        pd.Timestamp(as_of_date)
        - pd.Timedelta(days=int(params["lookback_months"]) * 31 + _PANEL_BUFFER_DAYS)
    ).date()
    price_panel = load_price_panel(normalised_conn, universe, str(panel_start), as_of_date)
    if price_panel.empty:
        return pd.DataFrame(columns=["ticker", "momentum_return", "momentum_rank", "in_top_n"])

    momentum = rank_universe(price_panel, universe, as_of_date, lookback_days)
    if momentum.empty:
        return pd.DataFrame(columns=["ticker", "momentum_return", "momentum_rank", "in_top_n"])

    # Volume is loaded only when a declared filter needs it -- an ADTV floor
    # is the only current consumer, and loading it unconditionally would add a
    # second full-universe panel query to every dashboard call.
    registry_key = get_strategy(strategy_id)["registry_key"]
    needs_volume = "adtv_floor" in ((_registry_filter_ids(registry_key)) or [])
    volume_panel = (
        load_volume_panel(normalised_conn, universe, str(panel_start), as_of_date)
        if needs_volume else None
    )

    pool = select_buy_pool(
        momentum,
        pd.Timestamp(as_of_date),
        price_panel=price_panel,
        # Same convention as MomentumAdapter: no separate forward-filled
        # panel, so the circuit-lock check reads the raw panel. Passing a
        # ffilled panel here would make the live decision differ from the
        # backtested one on exactly the stale-price days the check exists for.
        price_panel_ffilled=price_panel,
        **_buy_pool_kwargs(registry_key, volume_panel),
    )

    # The RANKING is reported over every scored ticker so the dashboard can
    # still show where a filtered-out name placed. Only in_top_n reflects the
    # filters -- a name can rank 3rd and still not be held because it failed
    # the liquidity floor, and hiding that would make the dashboard disagree
    # with the book for reasons nobody could see.
    ranked = momentum.sort_values(ascending=False)
    target = set(pool.sort_values(ascending=False).head(int(params["top_n"])).index)
    df = pd.DataFrame({
        "ticker": ranked.index,
        "momentum_return": ranked.values,
    })
    df["momentum_rank"] = range(1, len(df) + 1)
    df["in_top_n"] = df["ticker"].isin(target)
    return df.reset_index(drop=True)


def _first_trading_day_on_or_after(normalised_conn: Any, floor_date: str, ceiling_date: str) -> Optional[str]:
    """The earliest real ohlcv_adjusted trading day in [floor_date,
    ceiling_date], or None if the DB has no rows in that range yet
    (e.g. asking about a future month before that data exists)."""
    row = normalised_conn.execute(
        "SELECT MIN(date) FROM ohlcv_adjusted WHERE date >= ? AND date <= ?",
        [floor_date, ceiling_date],
    ).fetchone()
    return str(row[0]) if row and row[0] is not None else None


def next_rebalance_date(normalised_conn: Any, as_of_date: str) -> Optional[str]:
    """The next rebalance date on the monthly schedule: the first real
    trading day of as_of_date's own calendar month if that day hasn't
    passed yet, otherwise the first real trading day of the following
    calendar month. Always derived from ohlcv_adjusted's real trading-day
    calendar (never weekday arithmetic), matching every other
    first-trading-day computation in this codebase (see
    features/momentum_universe.py::first_trading_days_per_year).

    Returns None if the DB has no trading days in the relevant window
    yet (caller should treat this as "unknown, try again once data
    lands" rather than a fabricated date).
    """
    as_of = pd.Timestamp(as_of_date)
    this_month_start = as_of.replace(day=1)
    this_month_search_ceiling = (this_month_start + pd.Timedelta(days=_NEXT_MONTH_SEARCH_WINDOW_DAYS)).date().isoformat()
    this_month_first_trading_day = _first_trading_day_on_or_after(
        normalised_conn, this_month_start.date().isoformat(), this_month_search_ceiling
    )
    if this_month_first_trading_day is not None and pd.Timestamp(this_month_first_trading_day) >= as_of:
        return this_month_first_trading_day

    # as_of_date is past this month's first trading day (or this month
    # has none yet on record) — the next rebalance is next month's.
    next_month_start = (this_month_start + pd.DateOffset(months=1)).date().isoformat()
    search_ceiling = (this_month_start + pd.DateOffset(months=1) + pd.Timedelta(days=_NEXT_MONTH_SEARCH_WINDOW_DAYS)).date().isoformat()
    return _first_trading_day_on_or_after(normalised_conn, next_month_start, search_ceiling)


def is_rebalance_day(normalised_conn: Any, as_of_date: str, strategy_id: str = DEFAULT_STRATEGY_ID) -> bool:
    """True iff as_of_date equals this strategy's recorded
    next_rebalance_date in momentum_rebalance_state (i.e. the pipeline
    step has previously computed today as the scheduled rebalance day)."""
    row = normalised_conn.execute(
        "SELECT next_rebalance_date FROM momentum_rebalance_state WHERE strategy_id = ?",
        [strategy_id],
    ).fetchone()
    return row is not None and row[0] is not None and str(row[0]) == as_of_date


def compute_rebalance_suggestions(
    normalised_conn: Any,
    rebalance_date: str,
    current_open_trades: List[Dict[str, Any]],
    strategy_id: str = DEFAULT_STRATEGY_ID,
    grace_cycles: Optional[int] = None,
    universe: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Diffs currently-held tickers (from momentum_trades' open rows)
    against rebalance_date's fresh strategy_id-band top-15 ranking,
    applying the exact grace-period rule the validated backtest uses
    (backtest.momentum_backtest.decide_grace_transitions) — never a
    second hand-written copy of that rule.

    current_open_trades : rows shaped like momentum_trades' open
        positions, each a dict with at least "ticker" and
        "grace_remaining" (None if never dropped out of top_n since
        entry; an int if currently in a grace countdown from a prior
        rebalance — the caller is expected to track/pass this forward
        from the previous rebalance's suggestions).

    Returns a list of {"ticker", "action", "momentum_rank",
    "grace_remaining"} dicts, action in {"add", "exit", "grace_hold"}:
      - "add": in this rebalance's top_n, not currently held.
      - "exit": currently held, grace period fully elapsed (force-sell).
      - "grace_hold": currently held, out of top_n, still within grace
        (informational — no action needed yet, but flags what's at risk).
    Tickers that are both currently held AND still in top_n produce no
    row (nothing to do).
    """
    ranking = compute_daily_ranking(normalised_conn, rebalance_date, strategy_id=strategy_id, universe=universe)
    target_set: Set[str] = set(ranking.loc[ranking["in_top_n"], "ticker"]) if not ranking.empty else set()
    rank_by_ticker = dict(zip(ranking["ticker"], ranking["momentum_rank"])) if not ranking.empty else {}

    # None means "use what the registry declares for this strategy". An
    # explicit argument still wins, so a caller exploring a different grace
    # period can still do so -- it just can no longer happen by accident.
    if grace_cycles is None:
        grace_cycles = int(strategy_params(strategy_id)["grace_cycles"])

    held_grace = {t["ticker"]: t.get("grace_remaining") for t in current_open_trades}
    updated_grace = decide_grace_transitions(held_grace, target_set, grace_cycles)

    suggestions: List[Dict[str, Any]] = []

    for ticker in target_set:
        if ticker not in held_grace:
            suggestions.append({
                "ticker": ticker, "action": "add",
                "momentum_rank": rank_by_ticker.get(ticker), "grace_remaining": None,
            })

    for ticker, grace_remaining in updated_grace.items():
        if ticker in target_set:
            continue  # back in top_n / never dropped out — nothing to do
        if grace_remaining is not None and grace_remaining <= 0:
            suggestions.append({
                "ticker": ticker, "action": "exit",
                "momentum_rank": rank_by_ticker.get(ticker), "grace_remaining": grace_remaining,
            })
        else:
            suggestions.append({
                "ticker": ticker, "action": "grace_hold",
                "momentum_rank": rank_by_ticker.get(ticker), "grace_remaining": grace_remaining,
            })

    return suggestions
