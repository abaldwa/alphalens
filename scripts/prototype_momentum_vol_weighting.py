"""
scripts/prototype_momentum_vol_weighting.py

RESEARCH PROTOTYPE ONLY -- not wired into any production path.

Context: 2026-08-18 the user decided pure-play momentum should be a plain
equal-weight rank rotation, and backtest/adapters/momentum_adapter.py had its
per-ticker weighting knobs (including the old `volume_weighted` sizing)
removed -- MomentumAdapter now raises TypeError if you pass one. This script
tests whether inverse-volatility position sizing is worth reopening that
decision for, WITHOUT touching MomentumAdapter or any production code: it
subclasses the adapter in-place and monkeypatches it into
momentum_orchestrator_runner only for the duration of this script's own
process, via `unittest.mock.patch`.

A/B's one of the strategies flagged as an actual deployment candidate
(M9_276_550_allrisk_lb9mo_bimonthly_top20_63d -- Sharpe 1.02, CAGR 16.6%,
max_drawdown -41.6% on the live 2026-08-21 sweep) against the same strategy
with inverse-volatility sizing on new buy signals. Equal-weight baseline vs
vol-weighted variant, same universe/lookback/cadence/top_n, so any difference
is attributable to sizing alone.

Run: PYTHONPATH=$PWD .venv/bin/python -m scripts.prototype_momentum_vol_weighting
"""

from __future__ import annotations

import dataclasses
import logging
from datetime import date
from typing import Any, List
from unittest.mock import patch

import numpy as np
import pandas as pd

from backtest.adapters.momentum_adapter import MomentumAdapter
from backtest.core.engine import Signal
from backtest.core.metrics import cagr, sharpe_ratio, max_drawdown
from backtest.momentum_orchestrator_runner import run_momentum_orchestrated
from config.settings import DUCKDB_PATH
from config.timezone import now_ist
from datastore.api.db import get_duckdb_connection
from features.momentum_signal import lookback_trading_days, load_price_panel
from features.momentum_universe import all_yearly_full_rankings, yearly_band_universes_from_rankings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# M9_276_550_allrisk_lb9mo_bimonthly_top20_63d
RANK_START, RANK_END = 276, 550
LOOKBACK_MONTHS = 9
REBALANCE_TRADING_DAYS = 42  # bimonthly, matches this band's convention elsewhere in the codebase
TOP_N = 20
STARTING_CAPITAL = 1_000_000.0
INVESTABLE_PCT = 0.8
VOL_LOOKBACK_DAYS = 63  # ~3 trading months, standard realized-vol window

#: Clamp so one illiquid/near-zero-vol name can't blow the slot size up --
#: a research knob, not a claim about what the "right" cap is.
MAX_SIZE_MULTIPLIER = 3.0
MIN_SIZE_MULTIPLIER = 0.33


class VolWeightedMomentumAdapter(MomentumAdapter):
    """MomentumAdapter, but NEW buy signals get a size_multiplier inversely
    proportional to trailing realized volatility, normalized so the average
    multiplier across today's buy signals is 1.0 -- i.e. the same total
    capital gets deployed this rebalance, just redistributed toward the
    calmer names in the target list.

    Held (non-buy) positions are untouched here -- BacktestOrchestrator only
    consults size_multiplier at buy time (engine.py:1343), so a name that
    stays held keeps whatever weight it was bought at. That is a real
    difference from a full periodic-rebalance-to-target-weights scheme; this
    prototype only tests whether the ENTRY sizing choice matters, deliberately
    the smallest possible change to isolate that one variable.
    """

    def generate_signals(self, universe: List[str], as_of_date: date, horizon_bucket: str) -> List[Signal]:
        signals = super().generate_signals(universe, as_of_date, horizon_bucket)
        buys = [s for s in signals if s.action == "buy"]
        if not buys:
            return signals

        if self.price_panel is None:
            return signals
        window = self.price_panel.loc[: pd.Timestamp(as_of_date)].tail(VOL_LOOKBACK_DAYS + 1)
        daily_returns = window.pct_change(fill_method=None).tail(VOL_LOOKBACK_DAYS)

        inv_vols = {}
        for s in buys:
            vol = daily_returns[s.ticker].std() if s.ticker in daily_returns.columns else float("nan")
            inv_vols[s.ticker] = 1.0 / vol if vol and vol > 0 else np.nan

        valid = {t: v for t, v in inv_vols.items() if not np.isnan(v)}
        if not valid:
            return signals  # no history for any of today's buys -- fall back to equal-weight untouched

        mean_inv_vol = sum(valid.values()) / len(valid)
        # Signal is a frozen dataclass (engine.py:319) -- replace rather than mutate.
        resized = {}
        for s in buys:
            iv = inv_vols.get(s.ticker)
            multiplier = (
                1.0 if iv is None or np.isnan(iv)  # no vol history -- don't guess, size normally
                else float(np.clip(iv / mean_inv_vol, MIN_SIZE_MULTIPLIER, MAX_SIZE_MULTIPLIER))
            )
            resized[s.ticker] = dataclasses.replace(s, size_multiplier=multiplier)
        return [resized.get(s.ticker, s) if s.action == "buy" else s for s in signals]


def _summary(result: Any, label: str) -> dict[str, object]:
    curve = pd.Series(
        {pd.Timestamp(pt["date"]): pt["total_value"] for pt in result.equity_curve}
    ).sort_index()
    returns = curve.pct_change().dropna()
    cagr_val = cagr(result.starting_capital, result.ending_value, result.start_date, result.end_date)
    sharpe_val = sharpe_ratio(returns) if len(returns) else None
    dd_val = max_drawdown(curve) if len(curve) else None
    return {
        "label": label,
        "cagr_pct": round(cagr_val * 100, 2) if cagr_val is not None else None,
        "sharpe": round(sharpe_val, 2) if sharpe_val is not None else None,
        "max_drawdown_pct": round(dd_val * 100, 2) if dd_val is not None else None,
        "n_transactions": len(result.transactions),
    }


def main() -> None:
    end_date = now_ist().date()
    start_date = date(end_date.year - 10, end_date.month, end_date.day)
    lookback_days = lookback_trading_days(LOOKBACK_MONTHS)

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        logger.info("Computing yearly rankings %s..%s for band %d-%d", start_date, end_date, RANK_START, RANK_END)
        yearly_rankings = all_yearly_full_rankings(
            conn, start_date.isoformat(), end_date.isoformat(),
            max_rank=RANK_END, include_delisted=True,
        )
        if not yearly_rankings:
            raise RuntimeError("No real ohlcv_adjusted rows found in the requested date range -- cannot run.")

        yearly_universes = yearly_band_universes_from_rankings(yearly_rankings, RANK_START, RANK_END)
        candidate_tickers = sorted({t for u in yearly_universes.values() for t in u})
        logger.info("Loading price panel for %d candidate tickers", len(candidate_tickers))
        price_panel = load_price_panel(conn, candidate_tickers, start_date.isoformat(), end_date.isoformat())
        if price_panel.empty:
            raise RuntimeError("Price panel came back empty -- cannot run.")

    common_kwargs = dict(
        price_panel=price_panel,
        yearly_universes=yearly_universes,
        lookback_days=lookback_days,
        rebalance_every_n_trading_days=REBALANCE_TRADING_DAYS,
        starting_capital=STARTING_CAPITAL,
        investable_pct=INVESTABLE_PCT,
        top_n=TOP_N,
    )

    logger.info("Running equal-weight baseline...")
    baseline = run_momentum_orchestrated(strategy_id="M9_baseline_equal_weight", **common_kwargs)

    logger.info("Running inverse-volatility variant...")
    with patch("backtest.momentum_orchestrator_runner.MomentumAdapter", VolWeightedMomentumAdapter):
        vol_weighted = run_momentum_orchestrated(strategy_id="M9_prototype_vol_weighted", **common_kwargs)

    rows = [_summary(baseline, "equal_weight (current production sizing)"),
            _summary(vol_weighted, "inverse_volatility (prototype)")]
    print()
    print(f"{'label':<40} {'cagr_pct':>10} {'sharpe':>8} {'max_drawdown_pct':>18} {'n_transactions':>15}")
    for r in rows:
        print(f"{r['label']:<40} {r['cagr_pct']:>10} {r['sharpe']:>8} {r['max_drawdown_pct']:>18} {r['n_transactions']:>15}")
    print()
    print("This is a research comparison only -- nothing here is written to backtest_runs "
          "or any production store, and MomentumAdapter itself is unmodified.")


if __name__ == "__main__":
    main()
