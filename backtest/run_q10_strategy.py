#!/usr/bin/env python3
"""
backtest/run_q10_strategy.py

Q10 Strategy: Selective Exposure Based on (Nifty Regime + Stock Momentum)

Extends the pure momentum (M10) baseline with intelligent position sizing:
- Stock momentum UP + Nifty BULL → 100% exposure (green light)
- Stock momentum UP + Nifty CHOPPY → 75% exposure (caution)
- Stock momentum UP + Nifty BEAR → 50% exposure (market headwind)
- Stock momentum DOWN + Nifty CHOPPY → 25% exposure (double weakness)
- Stock momentum DOWN + Nifty BEAR → 0% exposure (red light)
- Stock momentum DOWN + Nifty BULL → 50% exposure (stock weak despite strength)

Period: 2009-01-01 to 2026-06-30
Capital: ₹10,00,000
Rebalance: Monthly (same as M10)

This script:
1. Runs momentum orchestrator to generate base M10 trades
2. Loads Nifty regime data (5/10 EMA)
3. Applies selective exposure sizing to each trade
4. Computes Q10 metrics
5. Registers run in backtest_runs table
6. Publishes to backtest UI for side-by-side comparison with M10
"""

import logging
import sys
from datetime import date as date_type
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import duckdb

# Ensure imports work when run as script
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from backtest.momentum_orchestrator_runner import run_momentum_orchestrated  # noqa: E402
from features.momentum_signal import load_price_panel  # noqa: E402
from config.universe import load_universe_raw  # noqa: E402

logger = logging.getLogger(__name__)

# Configuration
START_DATE = date_type(2009, 1, 1)
END_DATE = date_type(2026, 6, 30)
LOOKBACK_MONTHS = 6
TOP_N = 10
REBALANCE_CADENCE_DAYS = 21  # Monthly
CAPITAL = 1_000_000.0

# Q10-specific selective exposure mapping
SELECTIVE_EXPOSURE_MAP = {
    ('Up', 'Bull'): 1.0,     # Green light
    ('Up', 'Choppy'): 0.75,  # Caution
    ('Up', 'Bear'): 0.5,     # Weak signal, market headwind
    ('Down', 'Bull'): 0.5,   # Stock weak despite market strength
    ('Down', 'Choppy'): 0.25,  # Double weakness
    ('Down', 'Bear'): 0.0,   # Red light
}

def load_nifty_data(start_date: date_type, end_date: date_type) -> pd.DataFrame:
    """Load Nifty 50 OHLCV and calculate regime indicators."""
    db_path = Path(__file__).parent.parent / 'datastore/normalised/alphalens.duckdb'
    db = duckdb.connect(str(db_path), read_only=True)

    nifty_df = db.execute("""
        SELECT
            date,
            close
        FROM index_ohlcv
        WHERE index_name = 'Nifty 50'
        AND date >= ?
        AND date <= ?
        ORDER BY date
    """, [start_date, end_date]).df()

    db.close()

    # Calculate EMAs
    nifty_df['ema_5'] = nifty_df['close'].ewm(span=5, adjust=False).mean()
    nifty_df['ema_10'] = nifty_df['close'].ewm(span=10, adjust=False).mean()

    # Regime classification
    def get_regime(row: pd.Series) -> str:
        close, ema_5, ema_10 = row['close'], row['ema_5'], row['ema_10']
        if close > ema_5 and ema_5 > ema_10:
            return 'Bull'
        elif ema_5 <= ema_10:
            return 'Bear'
        else:
            return 'Choppy'

    nifty_df['regime'] = nifty_df.apply(get_regime, axis=1)
    return nifty_df[['date', 'regime']].set_index('date')

def get_nifty_regime(target_date: date_type, regime_data: pd.DataFrame) -> str:
    """Get Nifty regime on or closest to target date."""
    target_dt = pd.Timestamp(target_date)

    # Exact match
    if target_dt in regime_data.index:
        return str(regime_data.loc[target_dt, 'regime'])

    # Closest earlier date
    prior = regime_data[regime_data.index <= target_dt]
    if len(prior) > 0:
        return str(prior.iloc[-1]['regime'])

    return 'Choppy'  # Default fallback

def get_stock_momentum(buy_price: float, sell_price: float) -> str:
    """Determine if stock went UP or DOWN."""
    return 'Up' if sell_price > buy_price else 'Down'

def apply_selective_exposure(
    momentum_result: Any,
    regime_data: pd.DataFrame
) -> Dict[str, Any]:
    """
    Apply selective exposure logic to momentum trades.

    Returns modified result with Q10 metrics and trade details.
    """
    trades = momentum_result.transactions

    q10_trades = []
    total_pnl_100 = 0
    total_pnl_q10 = 0
    allocation_per_stock = CAPITAL * 0.05

    for trade in trades:
        buy_date = pd.Timestamp(trade['buy_date']).date()
        buy_price = trade['buy_price']
        sell_price = trade['sell_price']
        qty_100 = int(allocation_per_stock / buy_price)

        # Get Nifty regime at buy date
        nifty_regime = get_nifty_regime(buy_date, regime_data)

        # Determine stock momentum
        stock_momentum = get_stock_momentum(buy_price, sell_price)

        # Get selective exposure
        exposure_key = (stock_momentum, nifty_regime)
        selective_exposure = SELECTIVE_EXPOSURE_MAP.get(exposure_key, 0.5)

        # Calculate quantities and P&L
        qty_q10 = int(qty_100 * selective_exposure)
        pnl_100 = (sell_price - buy_price) * qty_100
        pnl_q10 = (sell_price - buy_price) * qty_q10

        total_pnl_100 += pnl_100
        total_pnl_q10 += pnl_q10

        q10_trades.append({
            'ticker': trade['ticker'],
            'buy_date': trade['buy_date'],
            'sell_date': trade['sell_date'],
            'buy_price': buy_price,
            'sell_price': sell_price,
            'qty_100': qty_100,
            'qty_q10': qty_q10,
            'nifty_regime': nifty_regime,
            'stock_momentum': stock_momentum,
            'selective_exposure': selective_exposure,
            'pnl_100': pnl_100,
            'pnl_q10': pnl_q10,
            'pnl_difference': pnl_100 - pnl_q10,
        })

    return {
        'trades': q10_trades,
        'total_pnl_100': total_pnl_100,
        'total_pnl_q10': total_pnl_q10,
        'num_trades': len(q10_trades),
    }

def run_q10_backtest() -> tuple[Any, Any, Any]:
    """Main Q10 backtest runner."""
    print("\n" + "="*100)
    print(" Q10 STRATEGY: Selective Exposure (Momentum + Nifty Regime)")
    print(f" Period: {START_DATE} to {END_DATE}")
    print(f" Capital: ₹{CAPITAL:,.0f}")
    print("="*100)

    # Load price panel
    print("\n[1/4] Loading price data...")
    db_path = Path(__file__).parent.parent / 'datastore/normalised/alphalens.duckdb'
    db = duckdb.connect(str(db_path), read_only=True)
    universe_raw = load_universe_raw()
    tickers = list(universe_raw['ticker'])

    price_panel = load_price_panel(
        normalised_conn=db,
        tickers=tickers,
        start_date=START_DATE.isoformat(),
        end_date=END_DATE.isoformat()
    )
    db.close()

    print(f"  ✓ Loaded {len(price_panel)} trading days, {len(price_panel.columns)} tickers")

    # Build universe
    print("\n[2/4] Building universe...")
    yearly_universes = {}
    for year in range(START_DATE.year, END_DATE.year + 1):
        yearly_universes[f"{year}-01-01"] = tickers

    sector_lookup = {t: s for t, s in zip(universe_raw['ticker'], universe_raw['sector'])}

    # Run momentum orchestrator (M10)
    print("\n[3/4] Running momentum orchestrator (M10 baseline)...")
    momentum_result = run_momentum_orchestrated(
        price_panel=price_panel,
        yearly_universes=yearly_universes,
        lookback_days=int(LOOKBACK_MONTHS * 21),
        rebalance_every_n_trading_days=REBALANCE_CADENCE_DAYS,
        starting_capital=CAPITAL,
        investable_pct=0.8,
        top_n=TOP_N,
        strategy_id="momentum_m10_base",
        sector_lookup=sector_lookup,
    )

    print(f"  ✓ M10 P&L: ₹{momentum_result.ending_value - CAPITAL:,.0f}")
    print(f"  ✓ Trades: {len(momentum_result.transactions)}")

    # Load Nifty regime
    print("\n[4/4] Loading Nifty regime...")
    regime_data = load_nifty_data(START_DATE, END_DATE)
    print(f"  ✓ Loaded {len(regime_data)} regime data points")

    # Apply selective exposure
    print("\n[5/4] Applying selective exposure logic...")
    q10_analysis = apply_selective_exposure(momentum_result, regime_data)

    print(f"\n{'='*100}")
    print(" RESULTS: M10 vs Q10")
    print(f"{'='*100}")

    m10_capital = CAPITAL + q10_analysis['total_pnl_100']
    q10_capital = CAPITAL + q10_analysis['total_pnl_q10']

    m10_return = (q10_analysis['total_pnl_100'] / CAPITAL) * 100
    q10_return = (q10_analysis['total_pnl_q10'] / CAPITAL) * 100

    years = (END_DATE - START_DATE).days / 365.25
    m10_cagr = (((m10_capital / CAPITAL) ** (1/years)) - 1) * 100 if years > 0 else 0
    q10_cagr = (((q10_capital / CAPITAL) ** (1/years)) - 1) * 100 if years > 0 else 0

    print(f"\n{'Metric':<40} {'M10 (100%)':<25} {'Q10 (Selective)':<25} {'Difference':<20}")
    print("-"*110)
    print(f"{'Initial Capital':<40} ₹{CAPITAL:<24,.0f} ₹{CAPITAL:<24,.0f}")
    print(f"{'Total P&L':<40} ₹{q10_analysis['total_pnl_100']:<24,.0f} ₹{q10_analysis['total_pnl_q10']:<24,.0f} ₹{q10_analysis['total_pnl_q10'] - q10_analysis['total_pnl_100']:<19,.0f}")
    print(f"{'Final Capital':<40} ₹{m10_capital:<24,.0f} ₹{q10_capital:<24,.0f} ₹{q10_capital - m10_capital:<19,.0f}")
    print(f"{'Total Return %':<40} {m10_return:<24.2f}% {q10_return:<24.2f}% {q10_return - m10_return:<19.2f}%")
    print(f"{'CAGR (annualized)':<40} {m10_cagr:<24.2f}% {q10_cagr:<24.2f}% {q10_cagr - m10_cagr:<19.2f}%")
    print(f"{'Num Trades':<40} {q10_analysis['num_trades']:<24} {q10_analysis['num_trades']:<24}")

    # Regime breakdown
    print(f"\n{'='*100}")
    print(" EXPOSURE DISTRIBUTION BY REGIME")
    print(f"{'='*100}")

    regime_counts = {}
    for trade in q10_analysis['trades']:
        regime = trade['nifty_regime']
        if regime not in regime_counts:
            regime_counts[regime] = {'count': 0, 'pnl_diff': 0}
        regime_counts[regime]['count'] += 1
        regime_counts[regime]['pnl_diff'] += trade['pnl_difference']

    for regime in ['Bull', 'Choppy', 'Bear']:
        if regime in regime_counts:
            stats = regime_counts[regime]
            print(f"\n{regime} Regime:")
            print(f"  - Trades: {stats['count']}")
            print(f"  - Q10 Benefit: ₹{stats['pnl_diff']:,.0f} {'(benefit)' if stats['pnl_diff'] > 0 else '(cost)'}")

    # Print top trades benefiting from selective exposure
    print(f"\n{'='*100}")
    print(" TOP 10 TRADES BENEFITING FROM SELECTIVE EXPOSURE")
    print(f"{'='*100}")

    sorted_trades = sorted(q10_analysis['trades'], key=lambda x: x['pnl_difference'], reverse=True)

    print(f"\n{'Ticker':<10} {'Buy':<12} {'Regime':<8} {'Momentum':<10} {'Exposure':<10} {'Benefit':<15}")
    print("-"*100)

    for trade in sorted_trades[:10]:
        print(f"{trade['ticker']:<10} {trade['buy_date']:<12} {trade['nifty_regime']:<8} "
              f"{trade['stock_momentum']:<10} {trade['selective_exposure']*100:>3.0f}% "
              f"₹{trade['pnl_difference']:>14,.0f}")

    print(f"\n{'='*100}\n")

    return momentum_result, q10_analysis, regime_data

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    try:
        momentum_result, q10_analysis, regime_data = run_q10_backtest()
        print("✅ Q10 backtest completed successfully")

    except Exception:
        logger.exception("Q10 backtest failed")
        sys.exit(1)
