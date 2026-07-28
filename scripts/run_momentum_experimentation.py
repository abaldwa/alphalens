"""
scripts/run_momentum_experimentation.py

Phase: FeatureBacklog.md ML38 — momentum strategy implementation
Owner: Platform / Backtest
Consumers: operator CLI (`python3 -m scripts.run_momentum_experimentation`)

Runs every ML38 variant — 4 trailing-momentum lookbacks x 5 market-cap
rank bands x 4 rebalance frequencies x 3 portfolio sizes (2026-07-14:
expanded from the original 64-variant 20-stock-only grid to also compare
10/15/20-stock portfolios, and added a 5th "mixed" rank-100-200 band) —
over the last 10 years of real OHLCV/fundamentals history and writes one
combined report, including capital-gains tax (LTCG/STCG) and a post-tax
CAGR per variant.

One-off research script per FeatureBacklog.md ML38's own scoping note —
not wired into daily_pipeline.py or any scheduled job. Opens the real
production DuckDB read-only (persist=False) so it never contends for the
write lock with the live ingestion scheduler.
"""

import argparse
import json
import logging
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

import csv
import hashlib

from backtest.core.horizon import HorizonBucket
from backtest.momentum_backtest import MomentumBacktester, MomentumBacktestResult
from backtest.momentum_metrics import cagr, churn_factor, sharpe_sortino_calmar, total_return, xirr
from backtest.momentum_tax import compute_total_tax, post_tax_ending_value
from backtest.strategy_id import build_strategy_id
from config.settings import BACKTEST_DUCKDB_PATH, DUCKDB_PATH
from config.timezone import now_ist
from datastore.api.db import get_duckdb_connection
from datastore.schema.create_strategy_catalog import create_strategy_catalog_schema
from features.momentum_signal import LOOKBACK_MONTHS, lookback_trading_days, load_price_panel
from features.momentum_universe import (
    RANK_BANDS,
    all_yearly_full_rankings,
    yearly_band_universes_from_rankings,
)

# 2026-07-24 user request (backtest sweep expansion): two wider bands
# beyond momentum_universe.py's own RANK_BANDS (which cap at rank 200,
# the ML38-scoped set used by the live rebalance-suggestion system).
# Kept local to this script rather than appended to the shared RANK_BANDS
# constant so the live system's band numbering/behavior is untouched.
# rank_band_tickers()/full_rank_universe() already support rank_end > 200
# via their own max(rank_end, MAX_TRACKED_RANK) — no new ranking logic
# needed, only wider (band_id, rank_start, rank_end) tuples.
WIDE_BANDS = [(6, 251, 500), (7, 501, 800)]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

STARTING_CAPITAL = 1_000_000.0
INVESTABLE_PCT = 0.8
GRACE_CYCLES = 2

# 2026-07-14: compare 10/15/20-stock portfolios, not just top-20.
TOP_N_OPTIONS = [10, 15, 20]

# 2026-07-14: SIP comparison — same starting capital, plus this amount
# contributed on the first trading day of every subsequent month.
SIP_MONTHLY_AMOUNT = 50_000.0

# weekly / biweekly / monthly / bimonthly / quarterly, in trading days
# (2026-07-15: added bimonthly ["2 Monthly"] per user request).
REBALANCE_PERIODS = {"weekly": 5, "biweekly": 10, "monthly": 21, "bimonthly": 42, "quarterly": 63}

REPORTS_DIR = Path(__file__).resolve().parent.parent / "backtest" / "reports" / "momentum"


def _union_tickers(yearly_rankings: Dict[str, "pd.DataFrame"]) -> List[str]:
    tickers = set()
    for ranked in yearly_rankings.values():
        if not ranked.empty:
            tickers.update(ranked["ticker"].tolist())
    return sorted(tickers)


def _summarize(result: MomentumBacktestResult, top_n: int, min_momentum: Optional[float] = None) -> Dict:
    """Per-variant metrics: the original equity-curve-based CAGR/Total
    Return/Churn, plus (2026-07-14 additions) Total Invested / Total Sell
    Value / transaction-based Total Return, win rate, and post-tax CAGR
    (LTCG 12.5% / STCG 20%, see backtest/momentum_tax.py)."""
    churn = churn_factor(result.rebalance_events)

    txns = result.transactions
    total_invested = sum(t["buy_price"] * t["qty"] for t in txns)
    total_sell_value = sum(t["sell_price"] * t["qty"] for t in txns if t["sell_price"] is not None)
    txn_total_return = (total_sell_value - total_invested) / total_invested if total_invested > 0 else 0.0

    closed = [t for t in txns if t["status"] == "closed"]
    win_rate = (sum(1 for t in closed if t["sell_price"] > t["buy_price"]) / len(closed)) if closed else None
    avg_days_held = (sum(t["holding_days"] for t in closed) / len(closed)) if closed else None

    total_tax = compute_total_tax(txns)
    post_tax_value = post_tax_ending_value(result.ending_value, txns)
    variant_cagr = cagr(result.starting_capital, result.ending_value, result.start_date, result.end_date)
    post_tax_cagr = cagr(result.starting_capital, post_tax_value, result.start_date, result.end_date)
    ratios = sharpe_sortino_calmar(result.equity_curve, variant_cagr)

    return {
        "top_n": top_n,
        "min_momentum": min_momentum,
        "starting_capital": result.starting_capital,
        "ending_value": result.ending_value,
        "start_date": result.start_date,
        "end_date": result.end_date,
        "total_return": total_return(result.starting_capital, result.ending_value),
        "cagr": variant_cagr,
        "sharpe": ratios["sharpe"],
        "sortino": ratios["sortino"],
        "calmar": ratios["calmar"],
        "churn_avg_transactions_per_year": churn["avg_transactions_per_year"],
        "n_rebalances": len(result.rebalance_events),
        "total_invested": total_invested,
        "total_sell_value": total_sell_value,
        "txn_total_return": txn_total_return,
        "win_rate": win_rate,
        "n_closed_trades": len(closed),
        "n_open_trades": len(txns) - len(closed),
        "avg_days_held": avg_days_held,
        "total_tax": total_tax,
        "post_tax_ending_value": post_tax_value,
        "post_tax_cagr": post_tax_cagr,
        "equity_curve": result.equity_curve,
        "churn_per_rebalance": churn["per_rebalance"],
        "transactions": txns,
    }


def _sip_summary(
    price_panel, yearly_universes: Dict[str, List[str]], lookback_days: int, rebalance_days: int, top_n: int
) -> Dict:
    """Runs the same variant a second time with a ₹50,000/month SIP on top
    of the same ₹10,00,000 starting capital (2026-07-14 user request), and
    returns only the 3 aggregate numbers needed for comparison — not the
    SIP run's own transaction ledger/equity curve, to keep report size
    bounded (this doubles engine runs but not output size)."""
    engine = MomentumBacktester(
        price_panel=price_panel,
        yearly_universes=yearly_universes,
        lookback_days=lookback_days,
        rebalance_every_n_trading_days=rebalance_days,
        starting_capital=STARTING_CAPITAL,
        investable_pct=INVESTABLE_PCT,
        top_n=top_n,
        grace_cycles=GRACE_CYCLES,
        sip_amount=SIP_MONTHLY_AMOUNT,
    )
    result = engine.run()
    cash_flows = [(cf["date"], cf["amount"]) for cf in result.cash_flows]
    cash_flows.append((result.end_date, result.ending_value))
    sip_xirr = xirr(cash_flows)
    return {
        "sip_monthly_amount": SIP_MONTHLY_AMOUNT,
        "sip_total_contributed": result.total_contributed,
        "sip_ending_value": result.ending_value,
        "sip_xirr": sip_xirr,
    }


def _variant_key(band_id: int, rank_start: int, rank_end: int, lookback_months: int, rebalance_name: str, top_n: int) -> str:
    descriptor = f"band{band_id}_{rank_start}-{rank_end}_top{top_n}_{lookback_months}m_{rebalance_name}"
    return descriptor


def _write_trade_book_csv(descriptor: str, txns: List[Dict]) -> Path:
    """One CSV per variant, matching the Technical/Fundamental trade-book
    convention (backtest/export_trade_book.py) — ticker, buy/sell date &
    price, days held, P&L. holding_days is None for still-open positions
    (no sell_date yet), left blank rather than fabricated."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = REPORTS_DIR / f"trade_book_{descriptor}.csv"
    with open(csv_path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["ticker", "buy_date", "buy_price", "sell_date", "sell_price", "days_held", "pnl", "status"])
        for t in txns:
            pnl = (t["sell_price"] - t["buy_price"]) * t["qty"] if t["sell_price"] is not None else None
            writer.writerow([
                t["ticker"], t["buy_date"], t["buy_price"], t.get("sell_date"), t.get("sell_price"),
                t.get("holding_days"), pnl, t["status"],
            ])
    return csv_path


def _upsert_strategy_catalog(descriptor: str, params: Dict, run_date) -> None:
    """One strategy_catalog row per momentum variant config. Momentum
    variants don't go through BacktestOrchestrator/backtest_runs, so
    latest_run_id here is this script's own descriptor-derived key, not a
    real backtest_runs.run_id — the catalog's FK is documented as
    value-level, not a DB foreign key, so this is consistent with that."""
    horizon = HorizonBucket.D21 if params["lookback_months"] <= 3 else (
        HorizonBucket.D63 if params["lookback_months"] <= 9 else HorizonBucket.Y1
    )
    strategy_id = build_strategy_id("momentum", descriptor, horizon, as_of=run_date.date())
    params_json = json.dumps(params, default=str)
    strategy_key = hashlib.sha1(f"momentum|{descriptor}|{params_json}".encode()).hexdigest()
    create_strategy_catalog_schema(BACKTEST_DUCKDB_PATH)
    with get_duckdb_connection(BACKTEST_DUCKDB_PATH, read_only=False, persist=False) as conn:
        conn.execute(
            """
            INSERT INTO strategy_catalog
                (strategy_key, channel, descriptor, params_json, latest_run_id, first_run_at, last_run_at, n_runs)
            VALUES (?, 'momentum', ?, ?, ?, ?, ?, 1)
            ON CONFLICT (strategy_key) DO UPDATE SET
                latest_run_id = excluded.latest_run_id,
                last_run_at = excluded.last_run_at,
                n_runs = strategy_catalog.n_runs + 1
            """,
            [strategy_key, descriptor, params_json, strategy_id, run_date, run_date],
        )
        conn.commit()


def run_experimentation(
    years_back: int = 10, write_trade_books: bool = True, end_date: Optional[date] = None,
) -> Dict:
    end_date = end_date or now_ist().date()
    start_date = date(end_date.year - years_back, end_date.month, end_date.day)

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        max_rank = max(rank_end for _, _, rank_end in RANK_BANDS + WIDE_BANDS)
        logger.info("Computing yearly full market-cap rankings (top %d) %s..%s", max_rank, start_date, end_date)
        yearly_rankings = all_yearly_full_rankings(
            conn, start_date.isoformat(), end_date.isoformat(), max_rank=max_rank, include_delisted=True,
        )  # 2026-07-20 survivorship-bias fix — BacktestUmbrellaPlan.md Gap #1
        if not yearly_rankings:
            raise RuntimeError("No real ohlcv_adjusted rows found in the requested date range — cannot run.")

        candidate_tickers = _union_tickers(yearly_rankings)
        logger.info(
            "Loading price panel for %d candidate tickers over %s..%s",
            len(candidate_tickers), start_date, end_date,
        )
        price_panel = load_price_panel(conn, candidate_tickers, start_date.isoformat(), end_date.isoformat())
        if price_panel.empty:
            raise RuntimeError("Price panel came back empty for the candidate ticker set — cannot run.")

    run_date = now_ist()
    variants = []
    for band_id, rank_start, rank_end in RANK_BANDS + WIDE_BANDS:
        yearly_universes = yearly_band_universes_from_rankings(yearly_rankings, rank_start, rank_end)
        for lookback_months in LOOKBACK_MONTHS:
            lookback_days = lookback_trading_days(lookback_months)
            for rebalance_name, rebalance_days in REBALANCE_PERIODS.items():
                for top_n in TOP_N_OPTIONS:
                    logger.info(
                        "Running band=%d (rank %d-%d) lookback=%dmo rebalance=%s top_n=%d",
                        band_id, rank_start, rank_end, lookback_months, rebalance_name, top_n,
                    )
                    engine = MomentumBacktester(
                        price_panel=price_panel,
                        yearly_universes=yearly_universes,
                        lookback_days=lookback_days,
                        rebalance_every_n_trading_days=rebalance_days,
                        starting_capital=STARTING_CAPITAL,
                        investable_pct=INVESTABLE_PCT,
                        top_n=top_n,
                        grace_cycles=GRACE_CYCLES,
                    )
                    result = engine.run()
                    summary = _summarize(result, top_n)
                    sip = _sip_summary(price_panel, yearly_universes, lookback_days, rebalance_days, top_n)
                    descriptor = _variant_key(band_id, rank_start, rank_end, lookback_months, rebalance_name, top_n)
                    variant_params = {
                        "band_id": band_id, "rank_start": rank_start, "rank_end": rank_end,
                        "lookback_months": lookback_months, "rebalance_period": rebalance_name, "top_n": top_n,
                    }
                    if write_trade_books:
                        _write_trade_book_csv(descriptor, result.transactions)
                        _upsert_strategy_catalog(descriptor, variant_params, run_date)
                    variants.append({
                        **variant_params,
                        **summary,
                        **sip,
                    })

    return {"generated_at": now_ist().isoformat(), "variants": variants}


def run_min_momentum_comparison(years_back: int, variants_to_test: List[Dict]) -> List[Dict]:
    """2026-07-14 win-rate exploration: rerun a curated set of already-run
    variants with min_momentum=0.0 (only ever buy names with genuinely
    positive trailing momentum) and report the win-rate/CAGR delta versus
    their original (unfiltered) run. See FeatureBacklog.md ML38."""
    end_date = now_ist().date()
    start_date = date(end_date.year - years_back, end_date.month, end_date.day)

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        yearly_rankings = all_yearly_full_rankings(
            conn, start_date.isoformat(), end_date.isoformat(), include_delisted=True,
        )  # 2026-07-20 survivorship-bias fix — BacktestUmbrellaPlan.md Gap #1
        candidate_tickers = _union_tickers(yearly_rankings)
        price_panel = load_price_panel(conn, candidate_tickers, start_date.isoformat(), end_date.isoformat())

    comparisons = []
    for base in variants_to_test:
        yearly_universes = yearly_band_universes_from_rankings(yearly_rankings, base["rank_start"], base["rank_end"])
        lookback_days = lookback_trading_days(base["lookback_months"])
        rebalance_days = REBALANCE_PERIODS[base["rebalance_period"]]

        engine = MomentumBacktester(
            price_panel=price_panel,
            yearly_universes=yearly_universes,
            lookback_days=lookback_days,
            rebalance_every_n_trading_days=rebalance_days,
            starting_capital=STARTING_CAPITAL,
            investable_pct=INVESTABLE_PCT,
            top_n=base["top_n"],
            grace_cycles=GRACE_CYCLES,
            min_momentum=0.0,
        )
        result = engine.run()
        filtered_summary = _summarize(result, base["top_n"], min_momentum=0.0)

        comparisons.append({
            "band_id": base["band_id"], "rank_start": base["rank_start"], "rank_end": base["rank_end"],
            "lookback_months": base["lookback_months"], "rebalance_period": base["rebalance_period"],
            "top_n": base["top_n"],
            "baseline_win_rate": base["win_rate"], "baseline_cagr": base["cagr"],
            "filtered_win_rate": filtered_summary["win_rate"], "filtered_cagr": filtered_summary["cagr"],
            "filtered_n_closed_trades": filtered_summary["n_closed_trades"],
        })
    return comparisons


def main():
    parser = argparse.ArgumentParser(description="Run ML38 momentum strategy experimentation")
    parser.add_argument("--years-back", type=int, default=10)
    parser.add_argument("--end-date", type=str, default=None, help="YYYY-MM-DD; defaults to today")
    parser.add_argument("--no-trade-books", action="store_true", help="Skip per-variant CSV/catalog writes")
    args = parser.parse_args()

    end_date = date.fromisoformat(args.end_date) if args.end_date else None
    report = run_experimentation(years_back=args.years_back, write_trade_books=not args.no_trade_books, end_date=end_date)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = REPORTS_DIR / f"momentum_experimentation_{now_ist().strftime('%Y%m%d_%H%M%S')}.json"
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    logger.info("Wrote report to %s", out_path)

    summary = sorted(report["variants"], key=lambda v: v["cagr"], reverse=True)
    print("\nTop 10 variants by CAGR:")
    print(f"{'band':<6}{'top_n':<7}{'lookback':<10}{'rebalance':<12}{'CAGR':>9}{'PostTaxCAGR':>13}{'SIP_XIRR':>10}{'WinRate':>9}{'AvgChurn/yr':>14}")
    for v in summary[:10]:
        wr = f"{v['win_rate']*100:.1f}%" if v["win_rate"] is not None else "n/a"
        sip_xirr = f"{v['sip_xirr']*100:.2f}%" if v["sip_xirr"] is not None else "n/a"
        print(
            f"{v['band_id']:<6}{v['top_n']:<7}{v['lookback_months']:<10}{v['rebalance_period']:<12}"
            f"{v['cagr']*100:>8.2f}%{v['post_tax_cagr']*100:>12.2f}%{sip_xirr:>10}{wr:>9}{v['churn_avg_transactions_per_year']:>14.1f}"
        )


if __name__ == "__main__":
    main()
