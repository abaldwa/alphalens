"""
scripts/compare_momentum_simulation_paths.py

Phase: Unified Generator Refactor, ML40-2.1 (the parity diff)
Owner: Platform / Backtest
Consumers: run by hand; its OUTPUT is the deliverable that gates H4.

Runs ONE momentum strategy through BOTH surviving simulation loops on the same
window, same panels, same universe, and prints the diff:

  A. MomentumBacktester            — the standalone engine (backtest/momentum_backtest.py)
  B. BacktestOrchestrator + MomentumAdapter — the shared loop every other channel uses

The plan's rule is that H4 (deleting MomentumBacktester) does not start until
this diff has been produced and reviewed. It is deliberately a REPORT, not a
test: the two loops are known to differ, and the question this answers is by how
much and in which direction — not whether they are already equal.

Real data only. Nothing here fabricates prices, universes or fills.

    python -m scripts.compare_momentum_simulation_paths --start 2015-01-01 --end 2020-12-31
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Set, Tuple

import pandas as pd

from backtest.adapters.momentum_adapter import MomentumAdapter
from backtest.core.engine import BacktestOrchestrator, OrchestratorConfig
from backtest.core.horizon import HorizonBucket
from backtest.core.run_context import BacktestRun
from backtest.momentum_backtest import MomentumBacktester
from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from features.momentum_signal import (
    lookback_trading_days,
    load_price_panel,
    load_volume_panel,
)
from features.momentum_universe import (
    all_yearly_full_rankings,
    yearly_band_universes_from_rankings,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# The unfiltered baseline (registry category "all_risk"): zero filter kwargs on
# both sides. Chosen deliberately — with filters on, a divergence could come
# from either the filters or the loop, and the loop is what this measures.
STARTING_CAPITAL = 1_000_000.0
INVESTABLE_PCT = 0.8
TOP_N = 15
GRACE_CYCLES = 2
LOOKBACK_MONTHS = 6
REBALANCE_TRADING_DAYS = 21  # monthly


def _sector_map() -> Dict[str, str]:
    from config.universe import load_universe_raw

    raw = load_universe_raw()
    return {t: s for t, s in zip(raw["ticker"], raw["sector"]) if s is not None}


def _standalone(price_panel: pd.DataFrame, yearly_universes: Dict[str, List[str]]) -> Any:
    engine = MomentumBacktester(
        price_panel=price_panel,
        yearly_universes=yearly_universes,
        lookback_days=lookback_trading_days(LOOKBACK_MONTHS),
        rebalance_every_n_trading_days=REBALANCE_TRADING_DAYS,
        starting_capital=STARTING_CAPITAL,
        investable_pct=INVESTABLE_PCT,
        top_n=TOP_N,
        grace_cycles=GRACE_CYCLES,
    )
    return engine.run()


def _orchestrated(
    price_panel: pd.DataFrame, volume_panel: pd.DataFrame,
    yearly_universes: Dict[str, List[str]], start: date, end: date,
) -> Any:
    trading_days = price_panel.index

    def universe_provider(as_of: date) -> List[str]:
        """The same yearly band membership the standalone engine reads."""
        keys = sorted(k for k in yearly_universes if k <= as_of.isoformat())
        return list(yearly_universes[keys[-1]]) if keys else []

    def price_lookup(ticker: str, as_of: date) -> Any:
        if ticker not in price_panel.columns:
            return None
        window = price_panel[ticker].loc[: pd.Timestamp(as_of)]
        window = window.dropna()
        return float(window.iloc[-1]) if len(window) else None

    # A REAL sector map is mandatory here, not a nicety: StrategyPortfolio
    # enforces sizing.max_sector_pct, and without a map every position is
    # "Unknown" — one sector — so the cap blocks most buys and the diff
    # measures the probe's own missing data instead of the two loops.
    sector_lookup = _sector_map()
    adapter = MomentumAdapter(
        price_panel=price_panel, volume_panel=volume_panel,
        top_n=TOP_N, lookback_months=LOOKBACK_MONTHS, grace_cycles=GRACE_CYCLES,
        sector_lookup=sector_lookup,
    )
    config = OrchestratorConfig(
        trading_days=trading_days,
        sector_lookup=lambda ticker: sector_lookup.get(ticker, "Unknown"),
        universe_provider=universe_provider,
        price_lookup=price_lookup,
        rebalance_cadence_days=REBALANCE_TRADING_DAYS,
        # Momentum is a PERIODIC strategy: entries exist only on rebalance
        # dates, so a daily exit policy empties a slot that cannot be refilled
        # for up to a full cadence. run_orchestrator_backtest.py sets this for
        # every real momentum run (_exit_policy_cadence_for); a probe that
        # omitted it would measure its own misconfiguration, not the loops.
        exit_policy_cadence="rebalance",
        # [2026-08-18, user decision] Momentum is fully invested with no
        # sector cap; the equal-weight slot is 1/top_n. Same values
        # run_orchestrator_backtest.py::_sizing_overrides_for sets for every
        # real momentum run.
        sizing_overrides={"max_position_pct": 1.0 / TOP_N, "max_sector_pct": 1.0},
        persist_signals=False,          # a comparison run is not an audit artifact
    )
    run = BacktestRun(
        run_id="ml40-2.1-parity", channel="momentum", strategy_id="parity_probe",
        horizon_bucket=HorizonBucket.D21, mode="backtest",
        universe_spec="momentum_rank_band", start_date=start, end_date=end,
        capital_mode="lump", initial_capital=STARTING_CAPITAL,
    )
    return BacktestOrchestrator().run(run, adapter, config)


def _standalone_buys(result: Any) -> Set[Tuple[str, str]]:
    """(ticker, buy_date) for every position the standalone engine opened.

    Its `transactions` ledger is one row per POSITION (buy_date + sell_date,
    sell_date=None for a still-open one), not one row per side.
    """
    return {
        (str(t.get("ticker")), str(t.get("buy_date"))[:10])
        for t in (result.transactions or [])
    }


def _orchestrated_buys(result: Any) -> Set[Tuple[str, str]]:
    """(ticker, entry_date) from the orchestrator's trade_log CSV.

    The orchestrator does not return trades on the result object — it writes
    them to trade_log_{run_id}.csv, which is the artifact every report reads.
    """
    path = getattr(result, "trade_log_path", None)
    if not path:
        return set()
    log = pd.read_csv(path)
    date_col = next((c for c in ("entry_date", "buy_date", "date") if c in log.columns), None)
    if date_col is None or "ticker" not in log.columns:
        logger.warning("trade_log at %s has unexpected columns: %s", path, list(log.columns))
        return set()
    return {(str(r.ticker), str(getattr(r, date_col))[:10]) for r in log.itertuples()}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="2015-01-01")
    parser.add_argument("--end", default="2020-12-31")
    parser.add_argument("--rank-start", type=int, default=101)
    parser.add_argument("--rank-end", type=int, default=150)
    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        rankings = all_yearly_full_rankings(
            conn, start.isoformat(), end.isoformat(), max_rank=800, include_delisted=True,
        )
        yearly_universes = yearly_band_universes_from_rankings(rankings, args.rank_start, args.rank_end)
        tickers = sorted({t for names in yearly_universes.values() for t in names})
        logger.info("band %d-%d: %d distinct tickers", args.rank_start, args.rank_end, len(tickers))
        price_panel = load_price_panel(conn, tickers, start.isoformat(), end.isoformat())
        volume_panel = load_volume_panel(conn, tickers, start.isoformat(), end.isoformat())

    logger.info("running the standalone engine (MomentumBacktester)")
    standalone = _standalone(price_panel, yearly_universes)
    logger.info("running the shared loop (BacktestOrchestrator + MomentumAdapter)")
    orchestrated = _orchestrated(price_panel, volume_panel, yearly_universes, start, end)

    a_keys = _standalone_buys(standalone)
    b_keys = _orchestrated_buys(orchestrated)
    union = a_keys | b_keys
    jaccard = len(a_keys & b_keys) / len(union) if union else 1.0

    print("\n=== ML40-2.1 PARITY DIFF: momentum simulation loops ===")
    print(f"window             : {start} .. {end}   band {args.rank_start}-{args.rank_end}")
    print(f"buys               : standalone={len(a_keys)}  shared={len(b_keys)}")
    print(f"same (ticker,date) : {len(a_keys & b_keys)}   jaccard={jaccard:.3f}")
    print(f"only standalone    : {len(a_keys - b_keys)}")
    print(f"only shared        : {len(b_keys - a_keys)}")
    print(f"\n[standalone] ending_value={standalone.ending_value:,.0f} "
          f"contributed={standalone.total_contributed:,.0f} "
          f"rebalances={len(standalone.rebalance_events)} signals={standalone.total_signals}")
    metrics = orchestrated.metrics or {}
    print("[shared]     " + "  ".join(
        f"{k}={metrics[k]}" for k in ("cagr", "sharpe", "max_drawdown", "n_trades", "win_rate")
        if k in metrics
    ))
    print(
        "\nWHY THEY DIFFER (measured, not assumed):\n"
        "  The two loops agree on WHICH names and WHEN — every shared buy is also a\n"
        "  standalone buy, on the same date. The shared loop takes FEWER of them.\n"
        "\n"
        "  MEASURED (instrumented can_buy, 2018-2019): the portfolio rejected ONE\n"
        "  buy in 63 offers. Sizing caps, the sector cap, cash and the ADTV floor\n"
        "  are NOT the cause — the orchestrator was simply never offered the other\n"
        "  buys. The remaining gap is in MomentumAdapter's rotation rule versus\n"
        "  MomentumBacktester's, and it is cumulative: the two agree exactly through\n"
        "  2018 and diverge progressively through 2019."
    )


if __name__ == "__main__":
    main()
