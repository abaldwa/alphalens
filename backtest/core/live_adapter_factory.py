"""
backtest/core/live_adapter_factory.py

Phase: Unified Generator Refactor, Phase F (F1)
Owner: Platform / Backtest
Consumers: datastore/api/routers/paper_trading_unified.py (the propose
endpoint), and any caller that needs today's adapter for a strategy.

Builds the SAME adapter a backtest of `strategy_id` would build, configured
from that strategy's registry row, plus the universe it should see today.
LiveSignalRunner and PaperTradingRunner both take an adapter and a universe;
this is where those two arguments come from outside a backtest.

WHY A FACTORY RATHER THAN CONSTRUCTING IN THE ROUTER
---------------------------------------------------
An adapter's constructor kwargs ARE the strategy: top_n, the lookback, the
filter chain. Assembling them at each call site is how a live path comes to
run a strategy the backtest never measured — quietly, because a partly
configured adapter still returns plausible signals. One factory means one
answer to "what were the parameters".

WHAT IT REFUSES TO DO
---------------------
* An unsupported declared filter raises `StrategyNotRunnableLive` rather than
  being dropped. Running a strategy without its declared filters is running a
  different strategy from the one that was backtested.
* `top_n` is REQUIRED for technical and fundamental, because their registry
  rows genuinely do not declare a holdings count — momentum's do. Defaulting
  it here would invent a portfolio size and present it as the strategy's own.
"""

from __future__ import annotations

import logging
from datetime import date as date_type
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from backtest.core.engine import StrategyAdapter
from features.momentum_live import StrategyNotRunnableLive

logger = logging.getLogger(__name__)

# How much price/volume history the entry filters need behind `as_of_date`.
# Generous relative to the 20-day ADTV/downtrend windows so holidays and
# thin-trading names still have a full window; small enough that this stays
# one bounded query, not a full-history pull.
PANEL_LOOKBACK_DAYS = 400

# Filters this path can honour. Deliberately the same shape (and the same
# refusal) as features/momentum_live.py's _SUPPORTED_FILTER_IDS: quality
# scores, HMM regime labels and market-cap/beta panels are backtest-time
# inputs with no live equivalent.
SUPPORTED_FILTER_IDS = frozenset({"adtv_floor", "circuit_lock_proxy", "downtrend_filter"})
SIZING_ONLY_FILTER_IDS = frozenset({"adtv_capped_sizing"})

# Channels with a live adapter. ML is absent because ml_adapter has no
# generate_signals yet (PHASE-H5), and the check is up front so an ML request
# gets that answer rather than a registry lookup failure that reads like a
# missing migration.
LIVE_CHANNELS = frozenset({"technical", "fundamental", "momentum"})


def _filter_kwargs(strategy_key: str, filter_ids: List[str]) -> Dict[str, Any]:
    """Registry-declared filter_ids -> adapter kwargs.

    The registry owns WHICH filters a strategy has and with what parameters;
    this only maps that declaration onto the adapters' argument names.
    """
    from strategies.registry import resolve_filters

    selection_filters = [f for f in filter_ids if f not in SIZING_ONLY_FILTER_IDS]
    if not selection_filters:
        return {}

    unsupported = sorted(set(selection_filters) - SUPPORTED_FILTER_IDS)
    if unsupported:
        raise StrategyNotRunnableLive(
            f"{strategy_key!r} declares filter(s) {unsupported} that the live path has "
            "no data source for. Refusing to run it, because running it WITHOUT its "
            "declared filters would silently execute a different strategy from the "
            "one that was backtested."
        )

    kwargs: Dict[str, Any] = {}
    for spec in resolve_filters(selection_filters):
        params = spec.get("params") or {}
        if spec["filter_id"] == "adtv_floor":
            kwargs["min_adtv_cr"] = float(params["min_adtv_cr"])
        elif spec["filter_id"] == "circuit_lock_proxy":
            kwargs["circuit_band_pct"] = float(params["circuit_band_pct"])
        elif spec["filter_id"] == "downtrend_filter":
            kwargs["downtrend_filter_pct"] = float(params["downtrend_filter_pct"])
    return kwargs


def _sector_lookup() -> Dict[str, Optional[str]]:
    from config.universe import load_universe_raw

    raw = load_universe_raw()
    return dict(zip(raw["ticker"], raw["sector"]))


def _known_sectors(lookup: Dict[str, Optional[str]]) -> Dict[str, str]:
    """The subset with a real sector.

    TechnicalAdapter and MomentumAdapter type sector_lookup as str-valued and
    fall back to "Unknown" for an absent ticker, which is the same meaning a
    null sector carries — so dropping the nulls here is equivalent, and keeps
    "we do not know" from being spelled two ways inside one adapter.
    """
    return {k: v for k, v in lookup.items() if v is not None}


def _panels(conn: Any, universe: List[str], as_of_date: date_type) -> Tuple[pd.DataFrame, pd.DataFrame]:
    from features.momentum_signal import load_price_panel, load_volume_panel

    start = (pd.Timestamp(as_of_date) - pd.Timedelta(days=PANEL_LOOKBACK_DAYS)).date()
    return (
        load_price_panel(conn, universe, str(start), str(as_of_date)),
        load_volume_panel(conn, universe, str(start), str(as_of_date)),
    )


def _feature_day_universe(as_of_date: date_type) -> List[str]:
    """The tickers the day's real feature snapshot covers.

    No fabricated universe: if the snapshot for the date does not exist, the
    universe is empty and the caller generates nothing.
    """
    from datastore.api.utils.feature_store import read_feature_day, resolve_date

    resolved = resolve_date(str(as_of_date))
    if resolved is None:
        return []
    panel = read_feature_day(resolved)
    if panel is None:
        return []
    return [str(t) for t in panel["ticker"]]


def build_live_adapter(
    channel: str,
    strategy_id: str,
    as_of_date: date_type,
    *,
    conn: Any = None,
    top_n: Optional[int] = None,
) -> Tuple[StrategyAdapter, List[str]]:
    """(adapter, universe) for `strategy_id` as of `as_of_date`.

    conn : an open read connection to the normalised DuckDB, used for the
        price/volume panels the entry filters need. Optional only for
        strategies that declare no panel-dependent filter; passing it is
        always safe.
    """
    from strategies.definitions import get_definition

    if channel not in LIVE_CHANNELS:
        raise ValueError(
            f"unsupported channel {channel!r} — technical, fundamental and momentum have "
            "live adapters; ML does not yet (PHASE-H5)"
        )

    definition = get_definition(channel, strategy_id)
    declared = dict(definition.get("definition") or {})
    filter_ids = list(definition.get("filter_ids") or [])
    strategy_key = str(definition.get("strategy_key") or f"{channel}:{strategy_id}")
    kwargs = _filter_kwargs(strategy_key, filter_ids)
    sector_lookup = _sector_lookup()

    if channel == "momentum":
        from backtest.adapters.momentum_adapter import MomentumAdapter
        from features.momentum_universe import rank_band_tickers

        if conn is None:
            raise ValueError("channel='momentum' needs a normalised DuckDB connection for its rank band and panels")
        universe = rank_band_tickers(conn, str(as_of_date), int(declared["rank_start"]), int(declared["rank_end"]))
        price_panel, volume_panel = _panels(conn, universe, as_of_date)
        adapter: StrategyAdapter = MomentumAdapter(
            price_panel=price_panel, volume_panel=volume_panel,
            top_n=int(declared["top_n"]),
            lookback_months=int(declared["lookback_months"]),
            sector_lookup=_known_sectors(sector_lookup),
            grace_cycles=int(declared.get("grace_cycles") or 0),
            rank_start=declared.get("rank_start"),
            **kwargs,
        )
        return adapter, universe

    if top_n is None:
        raise ValueError(
            f"channel={channel!r} requires an explicit top_n: its registry row declares the "
            "strategy's entry rule but not how many names to hold, and inventing one here "
            "would present a made-up portfolio size as the strategy's own."
        )

    universe = _feature_day_universe(as_of_date)
    price_panel, volume_panel = (
        _panels(conn, universe, as_of_date) if (conn is not None and universe) else (None, None)
    )

    if channel == "technical":
        from backtest.adapters.technical_adapter import TechnicalAdapter

        return TechnicalAdapter(
            template_name=str(declared.get("template_name") or strategy_id),
            top_n=top_n, sector_lookup=_known_sectors(sector_lookup),
            price_panel=price_panel, volume_panel=volume_panel,
            **kwargs,
        ), universe

    if channel == "fundamental":
        from backtest.adapters.fundamental_adapter import BESPOKE_PRESETS, FundamentalAdapter

        preset = str(declared.get("preset") or strategy_id)
        fundamental = FundamentalAdapter(
            preset=preset,
            top_n=top_n, sector_lookup=sector_lookup,
            price_panel=price_panel, volume_panel=volume_panel,
            **kwargs,
        )
        if preset in BESPOKE_PRESETS:
            # The three raw-PIT presets read financial history during
            # generate_signals, via the same deferred-wiring convention
            # run_orchestrator_backtest.py uses. Refuse up front rather than
            # letting the adapter fail mid-generation with no connection.
            if conn is None:
                raise ValueError(
                    f"preset={preset!r} reads raw PIT financials and needs a DuckDB "
                    "connection; pass conn="
                )
            fundamental._db_conn = conn
        return fundamental, universe

    raise AssertionError(f"channel {channel!r} is in LIVE_CHANNELS but has no branch above")
