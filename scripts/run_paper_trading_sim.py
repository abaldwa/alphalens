#!/usr/bin/env python3
"""
scripts/run_paper_trading_sim.py

Phase: 3.x (Paper Trading Logic Fix — Exit Signal bootstrap)
Specs: SPEC-BT-002 (position sizing/costs), SPEC-MODEL-002 (exit contract)
Owner: ml_signal_engine / exit
Consumers: paper_trading/historical_sim/ (NOT paper_trading/executions/)

Historical paper trading simulation using pre-trained Signal5D + MetaLabeler
for entries and a pluggable exit policy (rule-based bootstrap, or a real
trained ExitSignalModel once one exists). Loads models once, then runs
day-by-day inference on existing feature parquets without any model
retraining. Reuses backtest/portfolio.py's PortfolioSimulator for real
position sizing, sector-exposure caps, and Indian transaction costs — same
exit-context construction pattern as backtest/engine.py's
BacktestEngine._apply_exits (days_held, unrealised_pnl_pct,
drawdown_from_peak, momentum_3m, pnd_score; days_to_next_earnings/
hmm_regime stay honestly NaN, per CLAUDE.md Rule 6, same as BacktestEngine).

Output goes to paper_trading/historical_sim/ by default via
PaperTradingTracker, writing one fully-closed row per trade (exit_date
populated). This is deliberately NOT paper_trading/executions/ — Phase 3
Gate 7 counts distinct dated CSVs in that exact directory as the measure of
genuine forward-time paper trading, and historical/bootstrap replay must
never silently satisfy that gate. _assert_not_executions_dir() makes this a
hard error, not just a comment.

Usage:
    python3 scripts/run_paper_trading_sim.py                          # rule-based, 90 days from 2007-01-03
    python3 scripts/run_paper_trading_sim.py --from-date 2023-01-01   # specific start
    python3 scripts/run_paper_trading_sim.py --days 180               # longer window
    python3 scripts/run_paper_trading_sim.py --exit-policy model      # use a real trained ExitSignalModel
"""

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path
from typing import Dict, List

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from backtest.engine import EXIT_CONTEXT_COLUMNS
from backtest.portfolio import PortfolioSimulator
from config.universe import load_universe_raw
from features.pnd_features import PND_FEATURES, compute_pnd_features
from scripts.paper_trading_tracker import PaperTradingTracker
from systems.ml_signal_engine.inference.paper_trading_step import apply_daily_entries, apply_daily_exits
from systems.ml_signal_engine.models.exit.exit_signal import ExitSignalModel
from systems.ml_signal_engine.models.exit.rule_based_exit_policy import RuleBasedExitPolicy
from systems.ml_signal_engine.models.pnd.pnd_detector import PnDDetector
from systems.ml_signal_engine.models.signal.meta_labeler import MetaLabeler
from systems.ml_signal_engine.models.signal.signal_5d import Signal5DModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

DUCKDB_PATH = Path("datastore/normalised/alphalens.duckdb")
FEATURES_DAILY_DIR = Path("datastore/features/daily")
MODELS_DIR = Path("datastore/models")

SIGNAL_MODEL_NAME = "signal_5d"
META_MODEL_NAME = "meta_labeler"
PND_MODEL_NAME = "pnd_detector"
EXIT_MODEL_NAME = "exit_signal"
N_POSITIONS = 10
INITIAL_CAPITAL = 10_000_000  # 1 crore INR
DEFAULT_OUTPUT_DIR = Path("paper_trading/historical_sim")
EXECUTIONS_DIR = Path("paper_trading/executions")


def _assert_not_executions_dir(output_dir: Path) -> None:
    """Refuse to ever write historical/bootstrap simulation output into the
    directory Phase 3 Gate 7 counts (distinct dated CSVs = genuine forward-
    time paper trading days). A historical replay landing there would
    silently and falsely satisfy that gate."""
    if output_dir.resolve() == EXECUTIONS_DIR.resolve():
        raise ValueError(
            f"Refusing to write historical simulation output to {EXECUTIONS_DIR} — "
            "that directory is what Phase 3 Gate 7 counts as genuine forward-time "
            "paper trading days. Use a different --output-dir (default: "
            f"{DEFAULT_OUTPUT_DIR})."
        )


def _get_trading_dates(from_date: date, n_days: int) -> List[date]:
    available = sorted(
        date.fromisoformat(p.stem) for p in FEATURES_DAILY_DIR.glob("*.parquet")
    )
    start_idx = next((i for i, d in enumerate(available) if d >= from_date), None)
    if start_idx is None:
        raise ValueError(f"No feature dates on or after {from_date}")
    chosen = available[start_idx : start_idx + n_days]
    log.info("Simulation window: %s → %s (%d trading days)", chosen[0], chosen[-1], len(chosen))
    return chosen


def _load_ohlcv_panel(dates: List[date]) -> pd.DataFrame:
    """Load full OHLCV (+delivery_pct) for all tickers across the simulation window — needed for both
    next-day entry pricing and compute_pnd_features (high/low/volume/delivery_pct)."""
    start, end = dates[0], dates[-1]
    log.info("Loading OHLCV panel from DuckDB: %s → %s …", start, end)
    with duckdb.connect(str(DUCKDB_PATH), read_only=True) as conn:
        df = conn.execute(
            """
            SELECT date, ticker, open, high, low, close, volume, delivery_pct
            FROM ohlcv_adjusted
            WHERE date >= ? AND date <= ?
            ORDER BY ticker, date
            """,
            [str(start), str(end)],
        ).df()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    log.info("OHLCV panel: %d rows, %d tickers", len(df), df["ticker"].nunique())
    return df


def _build_momentum(ohlcv: pd.DataFrame) -> pd.Series:
    """63-day close-to-close momentum per (date, ticker) — same exit-context proxy as
    backtest/engine.py's BacktestEngine._build_momentum."""
    df = ohlcv.sort_values(["ticker", "date"]).copy()
    df["momentum_3m"] = df.groupby("ticker", sort=False)["close"].transform(lambda s: s / s.shift(63) - 1)
    return df.set_index(["date", "ticker"])["momentum_3m"]


def _load_exit_policy(exit_policy: str):
    if exit_policy == "rule_based":
        log.info("Exit policy: RuleBasedExitPolicy (bootstrap pass)")
        return RuleBasedExitPolicy()
    if exit_policy == "model":
        model_path = MODELS_DIR / EXIT_MODEL_NAME / f"{EXIT_MODEL_NAME}_current.pkl"
        if not model_path.exists():
            raise FileNotFoundError(
                f"--exit-policy model requires a trained ExitSignalModel at {model_path} — "
                "none exists yet. Run --exit-policy rule_based first to bootstrap enough "
                "closed trades, then train ExitSignalModel."
            )
        log.info("Exit policy: trained ExitSignalModel (%s)", model_path)
        model = ExitSignalModel()
        model.load(str(model_path))
        return model
    raise ValueError(f"Unknown exit_policy: {exit_policy}")


def run_simulation(
    from_date: date,
    n_days: int,
    n_positions: int,
    output_dir: Path,
    exit_policy_name: str,
) -> Dict:
    _assert_not_executions_dir(output_dir)

    trading_dates = _get_trading_dates(from_date, n_days)
    ohlcv = _load_ohlcv_panel(trading_dates)
    momentum = _build_momentum(ohlcv)
    price_lookup = ohlcv.set_index(["date", "ticker"])["close"]
    pnd_feature_panel = compute_pnd_features(ohlcv).set_index(["date", "ticker"])

    universe = load_universe_raw()
    sector_map = dict(zip(universe["ticker"], universe["sector"]))

    log.info("Loading pre-trained models …")
    signal_model = Signal5DModel()
    signal_model.load(str(MODELS_DIR / SIGNAL_MODEL_NAME / f"{SIGNAL_MODEL_NAME}_current.pkl"))
    meta_model = MetaLabeler()
    meta_model.load(str(MODELS_DIR / META_MODEL_NAME / f"{META_MODEL_NAME}_current.pkl"))
    pnd_detector = PnDDetector()
    pnd_detector.load(str(MODELS_DIR / PND_MODEL_NAME / f"{PND_MODEL_NAME}_current.pkl"))
    exit_policy = _load_exit_policy(exit_policy_name)
    log.info("Models loaded: %d signal features, MetaLabeler threshold=%.3f",
             len(signal_model._feature_names), meta_model._threshold)

    output_dir.mkdir(parents=True, exist_ok=True)
    tracker = PaperTradingTracker(logs_dir=str(output_dir))
    portfolio = PortfolioSimulator(
        initial_capital=INITIAL_CAPITAL, sizing_mode="equal_weight", n_target_positions=n_positions,
    )

    # entry_date/buy_prob/meta_prob captured per ticker at buy time, looked up
    # again when the position closes so the trade log carries the same entry
    # context the existing portfolio.trades_df() already has on Trade objects.
    entry_context: Dict[str, Dict] = {}
    skipped_dates = 0

    def _pnd_scores(d: date, tickers: List[str]) -> pd.Series:
        keys = [(d, t) for t in tickers]
        present = [k for k in keys if k in pnd_feature_panel.index]
        if not present:
            return pd.Series(0.0, index=tickers)
        rows = pnd_feature_panel.loc[present, PND_FEATURES]
        scores = pnd_detector.predict_full(rows)["pnd_score"]
        scores.index = [k[1] for k in present]
        return scores.reindex(tickers).fillna(0.0)

    def _pnd_blocked(d: date, tickers: List[str]) -> pd.Series:
        keys = [(d, t) for t in tickers]
        present = [k for k in keys if k in pnd_feature_panel.index]
        if not present:
            return pd.Series(False, index=tickers)
        rows = pnd_feature_panel.loc[present, PND_FEATURES]
        blocked = pnd_detector.predict_full(rows)["pnd_block"]
        blocked.index = [k[1] for k in present]
        return blocked.reindex(tickers).fillna(False)

    def _apply_exits(d: date, prices_today: Dict[str, float]) -> None:
        held = [t for t in portfolio.positions if t in prices_today]
        if not held:
            return
        pnd_scores = _pnd_scores(d, held)
        rows = []
        for t in held:
            pos = portfolio.positions[t]
            price = prices_today[t]
            days_held = max((pd.Timestamp(d) - pd.Timestamp(pos.entry_date)).days, 0)
            mom = momentum.get((d, t), 0.0)
            rows.append({
                "ticker": t, "entry_price": pos.entry_price, "days_held": float(days_held),
                "unrealised_pnl_pct": (price - pos.entry_price) / pos.entry_price,
                "days_to_next_earnings": np.nan,
                "drawdown_from_peak": (price - pos.peak_price) / pos.peak_price if pos.peak_price else 0.0,
                "momentum_3m": 0.0 if pd.isna(mom) else mom,
                "pnd_score": pnd_scores.get(t, 0.0),
                "hmm_regime": np.nan,
                "atr_pct": pos.entry_atr_pct if pos.entry_atr_pct is not None else np.nan,
            })
        exit_ctx = pd.DataFrame(rows).set_index("ticker")[EXIT_CONTEXT_COLUMNS]
        apply_daily_exits(portfolio, exit_policy, exit_ctx, prices_today, d, tracker, entry_context)

    def _apply_entries(d: date, fm: pd.DataFrame, prices_today: Dict[str, float]) -> None:
        candidates = fm[~fm.index.isin(portfolio.positions.keys())]
        if candidates.empty:
            return
        feat_cols = [c for c in signal_model._feature_names if c in candidates.columns]
        if not feat_cols:
            return
        X = candidates[feat_cols].replace([np.inf, -np.inf], np.nan)

        blocked = _pnd_blocked(d, list(X.index))
        X = X.loc[~blocked.to_numpy()]
        if X.empty:
            return

        try:
            proba = signal_model.predict_signals(X)
            direction = signal_model.predict(X)
            meta_out = meta_model.predict_full(X)
        except Exception as e:
            log.warning("Inference failed for %s: %s", d, e)
            return

        scored = pd.DataFrame({
            "buy_prob": proba["signal_buy_prob"], "direction": direction,
            "meta_act": meta_out["meta_label_act"], "meta_prob": meta_out["meta_label_prob"],
        }, index=X.index)
        buys = scored[(scored["direction"] == 1) & (scored["meta_act"])]
        if "atr_14_pct" in candidates.columns:
            buys = buys.assign(atr_14_pct=candidates.loc[buys.index, "atr_14_pct"])
        apply_daily_entries(portfolio, buys, sector_map, prices_today, d, tracker, entry_context, n_positions)

    for i, d in enumerate(trading_dates):
        parquet = FEATURES_DAILY_DIR / f"{d}.parquet"
        if not parquet.exists():
            log.warning("Missing feature parquet for %s — skipping", d)
            skipped_dates += 1
            continue

        fm = pd.read_parquet(parquet)
        if "ticker" not in fm.columns:
            log.warning("Feature matrix for %s has no ticker column", d)
            continue
        fm = fm.set_index("ticker")

        prices_today = price_lookup.loc[d].to_dict() if d in price_lookup.index.get_level_values(0) else {}
        if not prices_today:
            continue

        for ticker in portfolio.positions:
            if ticker in prices_today:
                portfolio.update_peak(ticker, prices_today[ticker])
        portfolio.record_equity(d, prices_today)

        _apply_exits(d, prices_today)
        _apply_entries(d, fm, prices_today)

        if (i + 1) % 10 == 0 or i == 0:
            log.info(
                "Day %d/%d (%s): %d open positions, equity=₹%.0f",
                i + 1, len(trading_dates), d, len(portfolio.positions), portfolio.total_equity(prices_today),
            )

    df_trades = portfolio.trades_df
    equity_curve = portfolio.equity_curve

    if df_trades.empty:
        log.warning("No closed trades generated — check if models need retraining or feature columns mismatch")
        return {"trades": 0, "sharpe": None, "cagr": None}

    daily_returns = equity_curve["equity"].pct_change().dropna() if not equity_curve.empty else pd.Series(dtype=float)
    sharpe = float(daily_returns.mean() / daily_returns.std() * np.sqrt(252)) if daily_returns.std() > 0 else 0.0

    total_days = len(trading_dates)
    years = total_days / 252
    final_equity = float(equity_curve["equity"].iloc[-1]) if not equity_curve.empty else INITIAL_CAPITAL
    cagr = float((final_equity / INITIAL_CAPITAL) ** (1 / years) - 1) if years > 0 else 0.0

    win_rate = float((df_trades["pnl_pct"] > 0).mean())
    total_pnl = float(df_trades["pnl_inr"].sum())
    # exit_reason here is PortfolioSimulator's generic action reason
    # (exit_model_urgent/exit_model_reduce) — the real EXIT_TYPES vocabulary
    # (target_achieved, thesis_broken, ...) is written per-trade into the
    # output_dir CSV logs by tracker.log_trade(), not summarized here.
    portfolio_action_counts = df_trades["exit_reason"].value_counts().to_dict() if "exit_reason" in df_trades.columns else {}

    report = {
        "simulation_type": "historical_paper_trading",
        "exit_policy": exit_policy_name,
        "from_date": str(trading_dates[0]),
        "to_date": str(trading_dates[-1]),
        "trading_days": len(trading_dates),
        "skipped_dates": skipped_dates,
        "n_positions": n_positions,
        "initial_capital_inr": INITIAL_CAPITAL,
        "final_equity_inr": round(final_equity, 2),
        "total_pnl_inr": round(total_pnl, 2),
        "total_trades": len(df_trades),
        "win_rate_pct": round(win_rate * 100, 2),
        "sharpe_ratio": round(sharpe, 4),
        "cagr_pct": round(cagr * 100, 2),
        "portfolio_action_counts": portfolio_action_counts,
        "model": SIGNAL_MODEL_NAME + "_current",
        "meta_model": META_MODEL_NAME + "_current",
        "output_dir": str(output_dir),
    }

    report_path = output_dir / f"paper_trading_sim_{trading_dates[0]}_{trading_dates[-1]}.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    log.info("=" * 60)
    log.info("PAPER TRADING SIMULATION COMPLETE (exit_policy=%s)", exit_policy_name)
    log.info("  Period     : %s → %s (%d days)", trading_dates[0], trading_dates[-1], len(trading_dates))
    log.info("  Trades     : %d", len(df_trades))
    log.info("  Win rate   : %.1f%%", win_rate * 100)
    log.info("  Total P&L  : ₹%.0f", total_pnl)
    log.info("  Sharpe     : %.4f", sharpe)
    log.info("  CAGR       : %.2f%%", cagr * 100)
    log.info("  Closed-trade CSVs : %s", output_dir)
    log.info("  Report             : %s", report_path)
    log.info("=" * 60)

    return report


def main():
    parser = argparse.ArgumentParser(description="Historical paper trading simulation")
    parser.add_argument(
        "--from-date", default="2007-01-03",
        help="Start date (YYYY-MM-DD). Earliest available: 2007-01-03 (default)"
    )
    parser.add_argument("--days", type=int, default=90, help="Number of trading days to simulate (default: 90)")
    parser.add_argument("--n-positions", type=int, default=N_POSITIONS, help="Max concurrent positions (default: 10)")
    parser.add_argument(
        "--exit-policy", choices=["rule_based", "model"], default="rule_based",
        help="rule_based: mechanical target/stop/max-hold bootstrap policy (pass 1). "
             "model: a real trained ExitSignalModel (pass 2+, requires one to already exist).",
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()

    from_date = date.fromisoformat(args.from_date)
    earliest = date(2007, 1, 3)
    if from_date < earliest:
        log.warning("Feature store starts %s — adjusting from-date from %s", earliest, from_date)
        from_date = earliest

    run_simulation(
        from_date=from_date,
        n_days=args.days,
        n_positions=args.n_positions,
        output_dir=Path(args.output_dir),
        exit_policy_name=args.exit_policy,
    )


if __name__ == "__main__":
    main()
