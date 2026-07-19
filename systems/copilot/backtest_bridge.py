"""
systems/copilot/backtest_bridge.py

Translates a StrategySpec into calls against the EXISTING, already-reviewed
backtest engines (backtest/momentum_backtest.py + features/momentum_universe.py
+ features/momentum_signal.py) — never a bespoke re-implementation, and
never anything but real DuckDB/Parquet data.

Known v1 limitation, surfaced honestly rather than silently ignored:
MomentumBacktester ranks a fixed universe by trailing momentum only; it has
no walk-forward mechanism for arbitrary technical/fundamental/valuation
conditions. So spec.technical/fundamental/valuation conditions are applied
as a ONE-TIME, latest-available-date filter (via the existing
ScreenerEngine.screen_custom for technical conditions) to narrow the
candidate universe, not re-evaluated at every historical rebalance. This
is disclosed in the returned `caveats` list, never hidden.
"""

import logging
from datetime import date as date_type
from typing import Any, Dict, List, Optional

from backtest.costs import IndianTransactionCosts
from backtest.momentum_backtest import MomentumBacktester
from backtest.momentum_metrics import cagr as compute_cagr
from backtest.momentum_metrics import churn_factor, total_return
from config.settings import COPILOT_BACKTEST_YEARS, DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from features.momentum_universe import yearly_band_universes
from features.momentum_signal import load_price_panel
from systems.copilot.strategy_spec import StrategySpec
from systems.technical_analysis.screener.engine import ScreenerEngine

logger = logging.getLogger(__name__)

DEFAULT_RANK_START = 1
DEFAULT_RANK_END = 200


def _apply_technical_prefilter(spec: StrategySpec, candidate_tickers: List[str]) -> tuple:
    """Point-in-time technical screen against the latest feature Parquet.
    Returns (filtered_tickers, caveat_or_None)."""
    if not spec.technical:
        return candidate_tickers, None

    engine = ScreenerEngine()
    results = engine.screen_custom(spec.technical, date=None, limit=len(candidate_tickers) or 10000)
    matched = {r.ticker for r in results if r.score == 1.0}
    if not matched:
        return [], "No tickers matched the technical conditions on the latest available date."

    filtered = [t for t in candidate_tickers if t in matched]
    caveat = (
        "Technical conditions were applied as a single latest-date screen "
        "to narrow the universe, not re-evaluated at every historical "
        "rebalance date."
    )
    return filtered, caveat


def run_backtest(spec: StrategySpec) -> Dict[str, Any]:
    """Run spec through MomentumBacktester. Returns a plain dict result.

    Any metric that cannot be computed from real data is set to None with
    a `reason`, never fabricated — matching backtest/engine.py's convention.
    """
    caveats: List[str] = []

    if spec.fundamental or spec.valuation:
        caveats.append(
            "Fundamental/valuation conditions in this spec are not yet "
            "applied by the backtest bridge (v1 scope: technical + momentum "
            "only) — they are recorded on the strategy but do not affect "
            "this backtest's results."
        )

    if not spec.rules.lookback_days or not spec.rules.rebalance_every_n_trading_days:
        return {
            "mode": "unsupported",
            "reason": (
                "Strategy spec has no lookback_days/rebalance_every_n_trading_days "
                "rules — Co-Pilot's v1 backtest bridge only supports momentum-style "
                "rank/rebalance strategies. Add rebalance rules to the spec, or use "
                "the Technical Screener page for a point-in-time-only screen."
            ),
            "caveats": caveats,
        }

    end_date = date_type.today()
    start_date = date_type(end_date.year - COPILOT_BACKTEST_YEARS, end_date.month, end_date.day)
    rank_start = spec.universe.rank_start or DEFAULT_RANK_START
    rank_end = spec.universe.rank_end or DEFAULT_RANK_END

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        yearly_universes = yearly_band_universes(
            conn, start_date.isoformat(), end_date.isoformat(), rank_start, rank_end
        )
        if not any(yearly_universes.values()):
            return {
                "mode": "backtest",
                "reason": "No real market-cap ranking data available for this universe/date range.",
                "caveats": caveats,
            }

        candidate_tickers = sorted({t for tickers in yearly_universes.values() for t in tickers})
        filtered_tickers, tech_caveat = _apply_technical_prefilter(spec, candidate_tickers)
        if tech_caveat:
            caveats.append(tech_caveat)
        if not filtered_tickers:
            return {
                "mode": "backtest",
                "reason": "No tickers remained after applying the strategy's technical conditions.",
                "caveats": caveats,
            }

        filtered_set = set(filtered_tickers)
        yearly_universes = {
            date_str: [t for t in tickers if t in filtered_set]
            for date_str, tickers in yearly_universes.items()
        }

        price_panel = load_price_panel(conn, filtered_tickers, start_date.isoformat(), end_date.isoformat())

    if price_panel.empty:
        return {
            "mode": "backtest",
            "reason": "No real price history available for the filtered ticker set.",
            "caveats": caveats,
        }

    engine = MomentumBacktester(
        price_panel=price_panel,
        yearly_universes=yearly_universes,
        lookback_days=spec.rules.lookback_days,
        rebalance_every_n_trading_days=spec.rules.rebalance_every_n_trading_days,
        top_n=spec.rules.top_n or 20,
        grace_cycles=spec.rules.grace_cycles,
        min_momentum=spec.rules.min_momentum,
        costs=IndianTransactionCosts(),
    )
    result = engine.run()

    ending_value = result.ending_value
    starting_capital = result.starting_capital
    cagr_value: Optional[float] = None
    total_return_value: Optional[float] = None
    if result.start_date and result.end_date and starting_capital:
        cagr_value = compute_cagr(starting_capital, ending_value, result.start_date, result.end_date)
        total_return_value = total_return(starting_capital, ending_value)

    return {
        "mode": "backtest",
        "start_date": result.start_date,
        "end_date": result.end_date,
        "starting_capital": starting_capital,
        "ending_value": ending_value,
        "cagr": cagr_value,
        "total_return": total_return_value,
        "churn_factor": churn_factor(result.rebalance_events) if result.rebalance_events else None,
        "n_rebalances": len(result.rebalance_events),
        "n_transactions": len(result.transactions),
        "universe_size": len(filtered_tickers),
        "caveats": caveats,
    }
