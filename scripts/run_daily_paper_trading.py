#!/usr/bin/env python3
"""
scripts/run_daily_paper_trading.py

Phase: 3.x (Automated Daily Paper Trading)
Specs: SPEC-BT-002, SPEC-MODEL-002, SPEC-OBS-004
Owner: ml_signal_engine / exit
Consumers: ingestion/scheduler/daily_pipeline.py (step_paper_trade),
    dashboard/static (Paper Trading screen, reads back via the
    datastore/api/routers/paper_trading.py router)

The forward-live paper trading bot. Run once per NSE trading day, after the
rest of the daily pipeline has written today's ml_signals/ml_forensic rows.
Unlike scripts/run_paper_trading_sim.py (historical bootstrap replay over
pre-loaded feature parquets), this script does NOT recompute any model —
it reads back what the pipeline already wrote via the DataStore API
(/api/v1/signals/ml/top_buys, /api/v1/signals/ml/{ticker}/{date},
/api/v1/ohlcv/_bulk) and only executes the resulting portfolio mechanics
(shared with run_paper_trading_sim.py via
systems/ml_signal_engine/inference/paper_trading_step.py).

This is the one and only script (outside of an operator's manual
dashboard --log-trade) allowed to write into paper_trading/executions/ —
the directory Phase 3 Gate 7 counts (>=90 distinct dated CSVs) as genuine
forward-time paper trading days. To make that count honest — a CSV for
date D exists if and only if this bot genuinely ran on D — every run logs
at least one row for `run_date`:
  - every new entry is logged immediately as an open row (no exit fields),
    so a buy-only day still produces a CSV;
  - every position that fully closes is logged by apply_daily_exits()
    (paper_trading_step.py), same as the historical sim;
  - if a run does neither (no signals cleared the gates, nothing to
    exit), a single heartbeat row is logged so the day still counts as a
    real bot run, not a synthetic trade.

Portfolio state (cash, open positions, equity curve) persists across runs
in paper_trading/portfolio_state.json (backtest/portfolio_state.py) —
each invocation is a separate process, so this is the only way positions
survive across day boundaries.

Usage:
    python3 scripts/run_daily_paper_trading.py                     # today (IST), rule_based exits
    python3 scripts/run_daily_paper_trading.py --date 2026-06-29
    python3 scripts/run_daily_paper_trading.py --n-positions 10 --exit-policy model
"""

import argparse
import json
import logging
import sys
from datetime import date as date_type
from pathlib import Path
from typing import Dict, List, Optional

import httpx
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from backtest.engine import EXIT_CONTEXT_COLUMNS
from backtest.portfolio import PortfolioSimulator
from backtest.portfolio_state import load_portfolio_state, save_portfolio_state
from config.settings import DATASTORE_API_BASE_URL, PAPER_TRADING_REQUIRE_APPROVAL
from config.timezone import now_ist
from config.universe import load_universe_raw
from datastore.api.utils.file_lock import locked_file
from scripts.paper_trading_tracker import PaperTradingTracker
from systems.ml_signal_engine.inference.paper_trading_step import (
    apply_daily_entries,
    apply_daily_exits,
    propose_daily_entries,
    propose_daily_exits,
)
from systems.ml_signal_engine.models.exit.exit_signal import ExitSignalModel
from systems.ml_signal_engine.models.exit.rule_based_exit_policy import (
    MAX_HOLD_DAYS,
    TARGET_PCT,
    RuleBasedExitPolicy,
    exit_criterion_text,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

PORTFOLIO_STATE_PATH = Path("paper_trading/portfolio_state.json")
EXECUTIONS_DIR = Path("paper_trading/executions")
PENDING_DIR = Path("paper_trading/pending")
MODELS_DIR = Path("datastore/models")

SIGNAL_MODEL_NAME = "signal_5d"
META_MODEL_NAME = "meta_labeler"
PND_MODEL_NAME = "pnd_detector"
EXIT_MODEL_NAME = "exit_signal"
N_POSITIONS = 10
INITIAL_CAPITAL = 10_000_000  # 1 crore INR
MOMENTUM_LOOKBACK_DAYS = 95  # calendar days, comfortably covers 63 trading days
CANDIDATE_BUFFER = 50  # top_buys fetch size before meta/direction filtering down to n_positions
HEARTBEAT_TICKER = "_HEARTBEAT_"

# Longer-horizon signal models the bot never trades (its own exit policy caps
# every position at MAX_HOLD_DAYS=21) but which are trained and scored daily
# regardless — surfaced as a read-only watchlist alongside the tradeable
# pending queue, purely so live conviction on these horizons is visible on
# the dashboard. Not gated by meta_labeler/pnd (that gate is specific to
# signal_5d's trade decision) and never accept/reject-able.
WATCHLIST_MODELS = ["signal_21d", "signal_63d"]
WATCHLIST_N = 10


def _load_exit_policy(exit_policy: str):
    if exit_policy == "rule_based":
        log.info("Exit policy: RuleBasedExitPolicy")
        return RuleBasedExitPolicy()
    if exit_policy == "model":
        model_path = MODELS_DIR / EXIT_MODEL_NAME / f"{EXIT_MODEL_NAME}_current.pkl"
        if not model_path.exists():
            raise FileNotFoundError(
                f"--exit-policy model requires a trained ExitSignalModel at {model_path} — "
                "none exists yet. Run with --exit-policy rule_based until enough closed "
                "trades accumulate to train one."
            )
        log.info("Exit policy: trained ExitSignalModel (%s)", model_path)
        model = ExitSignalModel()
        model.load(str(model_path))
        return model
    raise ValueError(f"Unknown exit_policy: {exit_policy}")


def _fetch_prices(client: httpx.Client, api_base_url: str, run_date: date_type) -> Dict[str, float]:
    resp = client.get(f"{api_base_url}/api/v1/ohlcv/_bulk", params={"from": str(run_date), "to": str(run_date)})
    resp.raise_for_status()
    rows = resp.json()
    return {r["ticker"]: r["close"] for r in rows}


def _fetch_pnd_scores(client: httpx.Client, api_base_url: str, run_date: date_type, tickers: List[str]) -> Dict[str, float]:
    scores: Dict[str, float] = {}
    for t in tickers:
        resp = client.get(f"{api_base_url}/api/v1/signals/ml/{t}/{run_date}")
        if resp.status_code != 200:
            continue
        for row in resp.json():
            if row.get("model_name") == PND_MODEL_NAME and row.get("pnd_score") is not None:
                scores[t] = float(row["pnd_score"])
    return scores


def _fetch_momentum(client: httpx.Client, api_base_url: str, run_date: date_type, tickers: List[str]) -> Dict[str, float]:
    """Real 63-trading-day close-to-close momentum, computed from OHLCV history
    (not stored anywhere downstream of daily_inference.py, so re-derived here
    for just the handful of currently-held tickers — same proxy backtest/
    engine.py's BacktestEngine._build_momentum and run_paper_trading_sim.py's
    _build_momentum use)."""
    momentum: Dict[str, float] = {}
    from_date = run_date.toordinal() - MOMENTUM_LOOKBACK_DAYS
    from_date = date_type.fromordinal(from_date)
    for t in tickers:
        resp = client.get(
            f"{api_base_url}/api/v1/ohlcv/{t}", params={"from": str(from_date), "to": str(run_date)}
        )
        if resp.status_code != 200:
            continue
        rows = resp.json().get("data", [])
        if len(rows) < 64:
            continue
        closes = [r["close"] for r in rows]
        if closes[-64] > 0:
            momentum[t] = closes[-1] / closes[-64] - 1
    return momentum


CANDIDATE_COLUMNS = [
    "buy_prob", "model_name", "meta_label_prob", "q10_return", "q50_return", "q90_return",
]


def _fetch_buy_candidates(
    client: httpx.Client, api_base_url: str, run_date: date_type, n_positions: int
) -> pd.DataFrame:
    """Top buy-signal tickers for today, filtered to signal_direction == 'buy'
    and meta-labeler-approved (meta_label == 'act') — mirrors
    run_paper_trading_sim.py's `scored[(direction == 1) & meta_act]` gate,
    sourced from already-written ml_signals rows instead of recomputing the
    models. Excludes P&D-blocked tickers (already enforced by /top_buys).

    Carries signal_5d's quantile forecast (q10/q50/q90_return) and the
    meta_labeler's own act-probability (meta_prob) through onto each
    candidate — logged at entry time (see log_trade() call in
    run_daily_paper_trading) so that a live 'did meta_labeler's real-time
    'act' call actually pay off' comparison is possible later, not just a
    backtest-fold estimate (systems/ml_signal_engine/models/signal/
    meta_labeler.py is currently validated only against historical folds)."""
    resp = client.get(
        f"{api_base_url}/api/v1/signals/ml/top_buys/{run_date}",
        params={"n": CANDIDATE_BUFFER, "model_name": SIGNAL_MODEL_NAME},
    )
    resp.raise_for_status()
    rows = resp.json()

    approved = []
    for row in rows:
        if row.get("signal_direction") != "buy":
            continue
        meta_resp = client.get(f"{api_base_url}/api/v1/signals/ml/{row['ticker']}/{run_date}")
        if meta_resp.status_code != 200:
            continue
        meta_rows = {r["model_name"]: r for r in meta_resp.json()}
        meta_row = meta_rows.get(META_MODEL_NAME)
        if meta_row is None or meta_row.get("meta_label") != "act":
            continue
        approved.append({
            "ticker": row["ticker"],
            "buy_prob": row["buy_prob"],
            "model_name": SIGNAL_MODEL_NAME,
            "meta_label_prob": meta_row.get("meta_prob"),
            "q10_return": row.get("q10_return"),
            "q50_return": row.get("q50_return"),
            "q90_return": row.get("q90_return"),
        })

    if not approved:
        return pd.DataFrame(columns=CANDIDATE_COLUMNS)

    # ML24 (2026-07-11): new BUY candidates only — gates recommendation
    # surfacing, not existing open positions (those are left alone even if
    # a held ticker later falls below the ADTV floor).
    from config.training_universe import filter_recommendable

    approved_df = pd.DataFrame(approved)
    approved_df = filter_recommendable(approved_df)
    if approved_df.empty:
        return pd.DataFrame(columns=CANDIDATE_COLUMNS)
    return approved_df.set_index("ticker").head(max(n_positions * 3, n_positions))


def _fetch_horizon_watchlist(
    client: httpx.Client, api_base_url: str, run_date: date_type
) -> Dict[str, List[Dict]]:
    """Top-N buy-signal tickers today for each of WATCHLIST_MODELS
    (signal_21d, signal_63d) — read-only observability, never traded or
    accept/reject-able. Written to paper_trading/pending/{date}_watchlist.json
    alongside (but structurally separate from) the tradeable pending queue."""
    watchlist: Dict[str, List[Dict]] = {}
    for model_name in WATCHLIST_MODELS:
        resp = client.get(
            f"{api_base_url}/api/v1/signals/ml/top_buys/{run_date}",
            params={"n": WATCHLIST_N, "model_name": model_name},
        )
        if resp.status_code != 200:
            watchlist[model_name] = []
            continue
        rows = [r for r in resp.json() if r.get("signal_direction") == "buy"]
        watchlist[model_name] = [
            {
                "ticker": r["ticker"],
                "buy_prob": r.get("buy_prob"),
                "q10_return": r.get("q10_return"),
                "q50_return": r.get("q50_return"),
                "q90_return": r.get("q90_return"),
            }
            for r in rows
        ]
    return watchlist


def _build_held_context(
    portfolio: PortfolioSimulator,
    run_date: date_type,
    prices: Dict[str, float],
    pnd_scores: Dict[str, float],
    momentum: Dict[str, float],
) -> pd.DataFrame:
    held = [t for t in portfolio.positions if t in prices]
    if not held:
        return pd.DataFrame()

    rows = []
    for t in held:
        pos = portfolio.positions[t]
        price = prices[t]
        days_held = max((pd.Timestamp(run_date) - pd.Timestamp(pos.entry_date)).days, 0)
        rows.append(
            {
                "ticker": t,
                "entry_price": pos.entry_price,
                "days_held": float(days_held),
                "unrealised_pnl_pct": (price - pos.entry_price) / pos.entry_price,
                "days_to_next_earnings": float("nan"),
                "drawdown_from_peak": (price - pos.peak_price) / pos.peak_price if pos.peak_price else 0.0,
                "momentum_3m": momentum.get(t, 0.0),
                "pnd_score": pnd_scores.get(t, 0.0),
                "hmm_regime": float("nan"),
                "atr_pct": pos.entry_atr_pct if pos.entry_atr_pct is not None else float("nan"),
            }
        )
    return pd.DataFrame(rows).set_index("ticker")[EXIT_CONTEXT_COLUMNS]


def _discard_stale_pending(run_date: date_type) -> None:
    """SPEC-PT-003: a pending-proposal file from a prior date is built off
    stale signals — discard (don't execute) it at the start of today's run
    rather than letting it silently accumulate or get accepted against
    today's prices under yesterday's reasoning. Today's own watchlist file
    (run_date}_watchlist.json, WATCHLIST_MODELS) is regenerated wholesale
    later in this same run, so it's also a keep, not a discard."""
    if not PENDING_DIR.exists():
        return
    today_stems = {str(run_date), f"{run_date}_watchlist"}
    for path in PENDING_DIR.glob("*.json"):
        if path.stem not in today_stems:
            log.info("Discarding stale pending-actions file %s (superseded by today's run)", path)
            path.unlink()


def run_daily_paper_trading(
    run_date: date_type,
    n_positions: int = N_POSITIONS,
    exit_policy_name: str = "rule_based",
    api_base_url: Optional[str] = None,
    require_approval: bool = PAPER_TRADING_REQUIRE_APPROVAL,
) -> Dict:
    api_base_url = api_base_url or DATASTORE_API_BASE_URL
    exit_policy = _load_exit_policy(exit_policy_name)

    if require_approval:
        _discard_stale_pending(run_date)

    with locked_file(PORTFOLIO_STATE_PATH):
        portfolio = load_portfolio_state(PORTFOLIO_STATE_PATH)
        if portfolio is None:
            log.info("No existing portfolio state — starting fresh with ₹%.0f", INITIAL_CAPITAL)
            portfolio = PortfolioSimulator(
                initial_capital=INITIAL_CAPITAL, sizing_mode="equal_weight", n_target_positions=n_positions,
            )

        universe = load_universe_raw()
        sector_map = dict(zip(universe["ticker"], universe["sector"].fillna("UNKNOWN")))
        name_map = dict(zip(universe["ticker"], universe["company_name"].fillna("")))

        tracker = PaperTradingTracker(logs_dir=str(EXECUTIONS_DIR))
        entry_context: Dict[str, Dict] = {}
        newly_bought: set = set()
        pending_actions: List[Dict] = []

        with httpx.Client(timeout=30.0) as client:
            prices = _fetch_prices(client, api_base_url, run_date)
            if not prices:
                log.warning("No OHLCV prices for %s — nothing to do today", run_date)

            for t in portfolio.positions:
                if t in prices:
                    portfolio.update_peak(t, prices[t])
            portfolio.record_equity(run_date, prices)

            held = list(portfolio.positions.keys())
            pnd_scores = _fetch_pnd_scores(client, api_base_url, run_date, held)
            momentum = _fetch_momentum(client, api_base_url, run_date, held)
            held_context = _build_held_context(portfolio, run_date, prices, pnd_scores, momentum)
            candidates = _fetch_buy_candidates(client, api_base_url, run_date, n_positions)
            # ml_signals rows can predate the ETF exclusion filter (bhavcopy/universe
            # changes don't retroactively clean already-written signal rows) — drop
            # anything not in today's ETF-free universe before proposing entries.
            candidates = candidates[candidates.index.isin(sector_map)]
            watchlist = _fetch_horizon_watchlist(client, api_base_url, run_date)
            for model_name in watchlist:
                watchlist[model_name] = [
                    row for row in watchlist[model_name] if row["ticker"] in sector_map
                ]

            if require_approval:
                # Compute candidates but don't execute — SPEC-PT-003 review/approve
                # flow. A human accepts/rejects each via the API; this run only
                # records the equity mark-to-market above and proposes actions.
                pending_actions = propose_daily_exits(portfolio, exit_policy, held_context, name_map=name_map)
                pending_actions += propose_daily_entries(
                    candidates, sector_map, prices, n_positions, list(portfolio.positions.keys()),
                    name_map=name_map,
                )
            else:
                apply_daily_exits(portfolio, exit_policy, held_context, prices, run_date, tracker, entry_context)
                before = set(portfolio.positions.keys())
                apply_daily_entries(
                    portfolio, candidates, sector_map, prices, run_date, tracker, entry_context, n_positions,
                )
                newly_bought = set(portfolio.positions.keys()) - before

        position_meta = {}
        for t in newly_bought:
            pos = portfolio.positions[t]

            def _cand(col: str):
                if t not in candidates.index or col not in candidates.columns:
                    return None
                value = candidates.loc[t, col]
                return None if pd.isna(value) else value

            buy_prob = float(_cand("buy_prob")) if _cand("buy_prob") is not None else None
            tracker.log_trade(
                date=str(run_date),
                ticker=t,
                signal_type="BUY",
                entry_price=pos.entry_price,
                quantity=pos.quantity,
                entry_time=entry_context.get(t, {}).get("entry_time", "09:15:00"),
                model_name=_cand("model_name"),
                buy_prob=buy_prob,
                meta_label_prob=_cand("meta_label_prob"),
                q10_return=_cand("q10_return"),
                q50_return=_cand("q50_return"),
                q90_return=_cand("q90_return"),
            )
            target_date = run_date.toordinal() + MAX_HOLD_DAYS
            position_meta[t] = {
                "buy_prob_entry": buy_prob,
                "target_price": round(pos.entry_price * (1 + TARGET_PCT), 2),
                "target_date": str(date_type.fromordinal(target_date)),
                "exit_criterion": exit_criterion_text(pos.entry_price),
            }

        if pending_actions:
            PENDING_DIR.mkdir(parents=True, exist_ok=True)
            rows = [
                {"action_id": f"{run_date}_{a['ticker']}_{a['action_type']}", "date": str(run_date), "status": "pending", **a}
                for a in pending_actions
            ]
            (PENDING_DIR / f"{run_date}.json").write_text(json.dumps(rows, indent=2))
            log.info("Wrote %d pending action(s) for %s — awaiting accept/reject via the API", len(rows), run_date)

        if any(watchlist.values()):
            PENDING_DIR.mkdir(parents=True, exist_ok=True)
            (PENDING_DIR / f"{run_date}_watchlist.json").write_text(
                json.dumps({"date": str(run_date), "models": watchlist}, indent=2)
            )
            log.info(
                "Wrote horizon watchlist for %s: %s",
                run_date, {m: len(rows) for m, rows in watchlist.items()},
            )

        day_log = EXECUTIONS_DIR / f"{run_date}.csv"
        if not day_log.exists():
            log.info("No entries/exits today — logging heartbeat row so Gate 7 still counts %s", run_date)
            tracker.log_trade(
                date=str(run_date), ticker=HEARTBEAT_TICKER, signal_type="NONE",
                entry_price=0.0, quantity=0, entry_time="00:00:00",
            )

        save_portfolio_state(portfolio, PORTFOLIO_STATE_PATH, as_of_date=str(run_date), position_meta=position_meta)

    equity = portfolio.total_equity(prices) if prices else portfolio.cash
    log.info(
        "Daily paper trading %s done: %d open positions, %d new buys, %d pending action(s), equity=₹%.0f",
        run_date, len(portfolio.positions), len(newly_bought), len(pending_actions), equity,
    )
    return {
        "date": str(run_date),
        "open_positions": len(portfolio.positions),
        "new_buys": len(newly_bought),
        "pending_actions": len(pending_actions),
        "equity": equity,
    }


def main():
    parser = argparse.ArgumentParser(description="Automated daily paper trading bot (forward-live)")
    parser.add_argument("--date", default=None, help="Run date YYYY-MM-DD (default: today IST)")
    parser.add_argument("--n-positions", type=int, default=N_POSITIONS, help="Max concurrent positions")
    parser.add_argument(
        "--exit-policy", choices=["rule_based", "model"], default="rule_based",
        help="rule_based: mechanical target/stop/max-hold policy (default). "
             "model: a real trained ExitSignalModel (requires one to already exist).",
    )
    parser.add_argument("--api", default=None, help=f"DataStore API base URL (default: {DATASTORE_API_BASE_URL})")
    parser.add_argument(
        "--auto-execute", action="store_true",
        help="Bypass SPEC-PT-003 review/approve and auto-execute immediately, overriding "
             f"config.settings.PAPER_TRADING_REQUIRE_APPROVAL (currently {PAPER_TRADING_REQUIRE_APPROVAL}).",
    )
    args = parser.parse_args()

    run_date = date_type.fromisoformat(args.date) if args.date else now_ist().date()

    run_daily_paper_trading(
        run_date=run_date, n_positions=args.n_positions, exit_policy_name=args.exit_policy, api_base_url=args.api,
        require_approval=False if args.auto_execute else PAPER_TRADING_REQUIRE_APPROVAL,
    )


if __name__ == "__main__":
    main()
