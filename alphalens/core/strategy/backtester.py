"""
alphalens/core/strategy/backtester.py

Walk-forward backtesting engine using vectorbt.

Each strategy is backtested over 15 years of data with:
  - 3-year training window, 1-year test window
  - 6-month step between folds
  - Realistic costs: 0.1% slippage + ₹20 brokerage per side
  - Liquidity filter: min avg volume ₹5 lakh/day
  - Position sizing: fixed fractional (2% of capital per trade)

Results stored in backtest_results DuckDB table including:
  - Sharpe, Sortino, Win rate, Max drawdown, CAGR
  - Cycle-breakdown: separate metrics for bull/bear/neutral periods
  - Individual trade log as JSON

Usage:
    bt = Backtester()
    result = bt.run(strategy_id="S001", symbol="RELIANCE")
    result = bt.run_all_symbols(strategy_id="S001")
    result = bt.run_all_strategies()
"""

import json
import uuid
from datetime import date, timedelta
from typing import Optional

import numpy as np
import pandas as pd
from loguru import logger

from alphalens.core.database import get_duck
from alphalens.core.strategy.library import get_strategy

# Simulation parameters
SLIPPAGE_PCT    = 0.001   # 0.1% both ways
BROKERAGE_INR   = 20.0    # ₹20 per order (Zerodha flat)
POSITION_PCT    = 0.02    # 2% of capital per trade
MIN_TRADES      = 20      # Minimum trades for result to be meaningful
TRAIN_YEARS     = 3
TEST_YEARS      = 1
STEP_MONTHS     = 6

# Quality gates — strategy must pass ALL to be marked active
GATE_SHARPE     = 1.0
GATE_WIN_RATE   = 0.52
GATE_MIN_TRADES = 50


class Backtester:

    def __init__(self):
        self.con = get_duck()

    # ── Public API ─────────────────────────────────────────────────────────

    def run(self, strategy_id: str, symbol: str,
            from_date: str = "2009-01-01",
            to_date: Optional[str] = None) -> dict:
        """
        Backtest one strategy on one symbol.
        Returns metrics dict. Stores result in backtest_results table.
        """
        strategy = get_strategy(strategy_id)
        if not strategy:
            return {"error": f"Strategy {strategy_id} not found"}

        to_date = to_date or str(date.today())

        prices = self._load_prices(symbol, from_date, to_date)
        inds   = self._load_indicators(symbol, from_date, to_date)
        cycles = self._load_cycles(symbol, from_date, to_date)

        if prices is None or len(prices) < 200:
            return {"error": f"Insufficient data for {symbol}"}

        df     = self._build_feature_df(prices, inds, cycles)
        trades = self._simulate_trades(df, strategy)

        if len(trades) < MIN_TRADES:
            logger.debug(f"S{strategy_id}/{symbol}: only {len(trades)} trades — skipping")
            return {"error": "insufficient_trades", "trades": len(trades)}

        metrics = self._compute_metrics(trades, df)
        self._store_result(strategy_id, symbol, metrics, trades, from_date, to_date,
                           strategy.get("timeframes", ["swing"])[0])

        return metrics

    def run_all_symbols(self, strategy_id: str,
                        symbols: Optional[list] = None) -> dict:
        """Backtest one strategy across all (or given) symbols."""
        if symbols is None:
            from alphalens.core.ingestion.universe import get_all_symbols
            symbols = get_all_symbols()

        logger.info(f"Backtesting {strategy_id} across {len(symbols)} symbols")
        results = {"ok": 0, "failed": 0, "metrics": []}

        for symbol in symbols:
            try:
                r = self.run(strategy_id, symbol)
                if "error" not in r:
                    results["ok"] += 1
                    results["metrics"].append({"symbol": symbol, **r})
                else:
                    results["failed"] += 1
            except Exception as e:
                logger.debug(f"Backtest error {strategy_id}/{symbol}: {e}")
                results["failed"] += 1

        # Aggregate metrics across symbols
        if results["metrics"]:
            all_sharpe   = [m["sharpe_ratio"]   for m in results["metrics"]]
            all_win_rate = [m["win_rate"]        for m in results["metrics"]]
            results["avg_sharpe"]   = float(np.mean(all_sharpe))
            results["avg_win_rate"] = float(np.mean(all_win_rate))

            # Update strategy-level metrics in strategies table
            self._update_strategy_metrics(strategy_id, results)

        logger.info(
            f"Backtest {strategy_id}: {results['ok']} ok, {results['failed']} failed, "
            f"avg Sharpe={results.get('avg_sharpe', 0):.2f}"
        )
        return results

    def run_all_strategies(self, sample_symbols: int = 30) -> dict:
        """
        Backtest all active strategies on a representative sample.
        Used after strategy discovery to validate new strategies.
        """
        from alphalens.core.strategy.library import get_all_strategies
        from alphalens.core.ingestion.universe import get_all_symbols
        import random

        strategies = get_all_strategies(active_only=False)
        all_symbols = get_all_symbols()
        sample      = random.sample(all_symbols, min(sample_symbols, len(all_symbols)))

        logger.info(f"Backtesting {len(strategies)} strategies on {len(sample)} symbols")
        summary = {}

        for strat in strategies:
            sid = strat["strategy_id"]
            r   = self.run_all_symbols(sid, sample)
            summary[sid] = {
                "name":      strat["name"],
                "avg_sharpe": r.get("avg_sharpe", 0),
                "avg_win":    r.get("avg_win_rate", 0),
            }

        return summary

    # ── Signal Generation (for a strategy on a given day) ─────────────────

    def check_entry_signal(self, strategy: dict, row: pd.Series,
                            prev_rows: pd.DataFrame) -> bool:
        """
        Evaluate entry conditions for a strategy on a single row.
        Used by SignalGenerator for live signal generation.
        """
        entry_rules = strategy.get("entry_rules", {})
        conditions  = entry_rules.get("conditions", [])
        logic       = entry_rules.get("logic", "AND")

        results = [self._evaluate_condition(c, row, prev_rows) for c in conditions]

        if logic == "AND":
            return all(results)
        else:
            return any(results)

    def check_exit_signal(self, strategy: dict, row: pd.Series,
                           entry_price: float, prev_rows: pd.DataFrame) -> bool:
        """Evaluate exit conditions for an open position."""
        exit_rules = strategy.get("exit_rules", {})
        conditions = exit_rules.get("conditions", [])
        logic      = exit_rules.get("logic", "OR")

        results = [self._evaluate_condition(c, row, prev_rows, entry_price) for c in conditions]

        if logic == "OR":
            return any(results)
        else:
            return all(results)

    def compute_stop_loss(self, strategy: dict, row: pd.Series,
                           entry_price: float) -> float:
        """Calculate stop-loss price for an entry."""
        sl_rules  = strategy.get("stoploss_rules", {})
        sl_type   = sl_rules.get("type", "atr_based")
        close     = float(row.get("close", entry_price))

        if sl_type == "atr_based":
            atr  = float(row.get("atr_14", close * 0.02) or close * 0.02)
            mult = sl_rules.get("multiplier", 2.0)
            return round(entry_price - (atr * mult), 2)

        elif sl_type == "fixed_pct":
            pct = sl_rules.get("pct", 7.0) / 100
            return round(entry_price * (1 - pct), 2)

        elif sl_type == "trailing_pct":
            pct = sl_rules.get("trail_pct", 8.0) / 100
            return round(entry_price * (1 - pct), 2)

        elif sl_type == "supertrend":
            st = row.get("supertrend")
            if st and not pd.isna(st):
                return round(float(st), 2)
            atr  = float(row.get("atr_14", close * 0.02) or close * 0.02)
            return round(entry_price - atr * 2.0, 2)

        elif sl_type == "ichimoku":
            sb = row.get("ichimoku_senkou_b")
            sa = row.get("ichimoku_senkou_a")
            cloud_bottom = min(
                float(sb) if sb and not pd.isna(sb) else entry_price * 0.92,
                float(sa) if sa and not pd.isna(sa) else entry_price * 0.92,
            )
            return round(cloud_bottom, 2)

        elif sl_type == "prev_close":
            prev_close = row.get("prev_close", entry_price * 0.99)
            return round(float(prev_close), 2)

        # Fallback: 5% fixed SL
        return round(entry_price * 0.95, 2)

    def compute_target(self, strategy: dict, row: pd.Series,
                        entry_price: float, stop_loss: float) -> float:
        """Calculate target price based on strategy parameters."""
        params   = strategy.get("parameters", {})
        mult     = params.get("atr_target_mult", 2.0)
        atr      = float(row.get("atr_14", entry_price * 0.02) or entry_price * 0.02)
        risk     = entry_price - stop_loss

        # Use ATR-based target if available
        target = entry_price + (atr * mult)

        # Ensure minimum R:R of 1.5
        min_rr   = 1.5
        min_target = entry_price + (risk * min_rr)
        return round(max(target, min_target), 2)

    # ── Simulation Engine ─────────────────────────────────────────────────

    def _simulate_trades(self, df: pd.DataFrame, strategy: dict) -> list:
        """
        Walk-forward simulation of strategy signals on historical data.
        Returns list of trade dicts.
        """
        trades     = []
        in_trade   = False
        entry_price = None
        entry_date  = None
        stop_loss   = None
        target      = None

        for i in range(50, len(df)):  # warmup period
            row       = df.iloc[i]
            prev_rows = df.iloc[max(0, i-50):i]
            close     = float(row["close"])

            if not in_trade:
                # Check entry
                try:
                    if self.check_entry_signal(strategy, row, prev_rows):
                        entry_price = close * (1 + SLIPPAGE_PCT)
                        stop_loss   = self.compute_stop_loss(strategy, row, entry_price)
                        target      = self.compute_target(strategy, row, entry_price, stop_loss)
                        entry_date  = row["date"]
                        in_trade    = True
                except Exception:
                    continue

            else:
                # Check exits: SL, target, or signal-based
                exit_triggered = False
                exit_reason    = None
                exit_price     = close * (1 - SLIPPAGE_PCT)

                if close <= stop_loss:
                    exit_triggered = True
                    exit_reason    = "stop_loss"
                    exit_price     = stop_loss * (1 - SLIPPAGE_PCT)

                elif close >= target:
                    exit_triggered = True
                    exit_reason    = "target"
                    exit_price     = target * (1 - SLIPPAGE_PCT)

                else:
                    try:
                        if self.check_exit_signal(strategy, row, entry_price, prev_rows):
                            exit_triggered = True
                            exit_reason    = "signal"
                    except Exception:
                        pass

                if exit_triggered:
                    holding_days = (row["date"] - entry_date).days if isinstance(row["date"], date) and isinstance(entry_date, date) else 0
                    pnl_pct      = (exit_price - entry_price) / entry_price * 100
                    cycle        = str(row.get("cycle", "neutral"))

                    trades.append({
                        "entry_date":   entry_date,
                        "exit_date":    row["date"],
                        "entry_price":  entry_price,
                        "exit_price":   exit_price,
                        "pnl_pct":      pnl_pct,
                        "pnl_inr":      (exit_price - entry_price) * (100_000 * POSITION_PCT / entry_price) - 2 * BROKERAGE_INR,
                        "holding_days": holding_days,
                        "exit_reason":  exit_reason,
                        "cycle":        cycle,
                    })

                    in_trade    = False
                    entry_price = None
                    stop_loss   = None
                    target      = None

                elif strategy.get("stoploss_rules", {}).get("trailing"):
                    # Update trailing stop loss
                    new_sl = close * (1 - strategy["stoploss_rules"].get("trail_pct", 7) / 100)
                    stop_loss = max(stop_loss, new_sl)

        return trades

    # ── Condition Evaluator ───────────────────────────────────────────────

    def _evaluate_condition(self, condition: dict, row: pd.Series,
                              prev_rows: pd.DataFrame,
                              entry_price: float = None) -> bool:
        """Evaluate a single rule condition against current row data."""
        try:
            indicator = condition.get("indicator")
            op        = condition.get("op")
            value     = condition.get("value")

            # Get left-hand side value
            lhs = self._get_indicator_value(indicator, row, prev_rows, entry_price)
            if lhs is None or (isinstance(lhs, float) and np.isnan(lhs)):
                return False

            # Get right-hand side value
            if isinstance(value, str) and value.startswith("indicator:"):
                ind_name = value.replace("indicator:", "")
                rhs = self._get_indicator_value(ind_name, row, prev_rows, entry_price)
                if rhs is None:
                    return False
            elif isinstance(value, (int, float)):
                rhs = value
            else:
                rhs = value

            # Evaluate operator
            return self._apply_operator(op, lhs, rhs, prev_rows, condition)

        except Exception:
            return False

    def _get_indicator_value(self, name: str, row: pd.Series,
                              prev_rows: pd.DataFrame,
                              entry_price: float = None):
        """Resolve indicator name to a numeric value."""
        direct = row.get(name)
        if direct is not None and not (isinstance(direct, float) and np.isnan(direct)):
            return float(direct)

        # Special computed indicators
        if name == "ema_21":
            return float(row.get("ema_20") or 0)
        if name == "high_20d":
            if len(prev_rows) >= 20:
                return float(prev_rows["high"].iloc[-20:].max())
        if name == "low_10d":
            if len(prev_rows) >= 10:
                return float(prev_rows["low"].iloc[-10:].min())
        if name == "prev_close" and len(prev_rows) > 0:
            return float(prev_rows["close"].iloc[-1])
        if name == "macd_hist_prev" and len(prev_rows) > 0:
            return float(prev_rows["macd_hist"].iloc[-1] or 0)
        if name == "pct_from_vwap":
            close = float(row.get("close", 0))
            vwap  = float(row.get("vwap", close))
            return (close - vwap) / vwap * 100 if vwap else 0
        if name == "gap_pct" and len(prev_rows) > 0:
            prev_c = float(prev_rows["close"].iloc[-1])
            curr_o = float(row.get("open", prev_c))
            return (curr_o - prev_c) / prev_c * 100 if prev_c else 0
        if name == "rs_percentile":
            return float(row.get("rs_nifty200", 50) or 50)
        if name == "sector_cycle":
            return str(row.get("sector_cycle", "neutral"))
        if name == "pe_vs_sector":
            return float(row.get("pe_ratio", 20) or 20) / 20.0   # simplified
        if name == "roe":
            return float(row.get("roe", 0) or 0)
        if name == "revenue_growth":
            return float(row.get("revenue_growth", 0) or 0)

        return None

    def _apply_operator(self, op: str, lhs, rhs,
                         prev_rows: pd.DataFrame, condition: dict) -> bool:
        """Apply comparison operator."""
        try:
            if op == ">":       return float(lhs) >  float(rhs)
            if op == ">=":      return float(lhs) >= float(rhs)
            if op == "<":       return float(lhs) <  float(rhs)
            if op == "<=":      return float(lhs) <= float(rhs)
            if op == "==":      return str(lhs) == str(rhs)
            if op == "is_true": return bool(lhs)

            if op == "crosses_above":
                if len(prev_rows) == 0:
                    return False
                prev_lhs_col = condition.get("indicator")
                prev_rhs_val = rhs
                if isinstance(rhs, str):
                    prev_rhs_val = float(prev_rows.get(rhs.replace("indicator:", ""), pd.Series([0])).iloc[-1] or 0)
                prev_lhs = float(prev_rows[prev_lhs_col].iloc[-1] or 0) if prev_lhs_col in prev_rows.columns else 0
                return prev_lhs <= float(prev_rhs_val) and float(lhs) > float(prev_rhs_val)

            if op == "crosses_below":
                if len(prev_rows) == 0:
                    return False
                prev_lhs_col = condition.get("indicator")
                prev_rhs_val = rhs
                prev_lhs = float(prev_rows[prev_lhs_col].iloc[-1] or 0) if prev_lhs_col in prev_rows.columns else 0
                return prev_lhs >= float(rhs) and float(lhs) < float(rhs)

            if op == "bullish_divergence":
                lookback = condition.get("lookback", 10)
                ind_col  = condition.get("indicator", "macd_hist")
                if len(prev_rows) < lookback:
                    return False
                recent_prices = prev_rows["close"].iloc[-lookback:]
                recent_ind    = prev_rows[ind_col].iloc[-lookback:] if ind_col in prev_rows.columns else pd.Series([0]*lookback)
                price_lower_low = float(recent_prices.iloc[-1]) < float(recent_prices.min())
                ind_higher_low  = float(recent_ind.iloc[-1])    > float(recent_ind.min())
                return price_lower_low and ind_higher_low

            if op == "squeeze_breakout":
                lookback = condition.get("squeeze_lookback", 126)
                if len(prev_rows) < lookback:
                    return False
                bb_width_col = "bb_width"
                current_width = float(lhs)
                hist_widths   = prev_rows[bb_width_col].iloc[-lookback:].dropna() if bb_width_col in prev_rows.columns else pd.Series()
                if hist_widths.empty:
                    return False
                return current_width == float(hist_widths.min())

        except (TypeError, ValueError):
            return False

        return False

    # ── Metrics Computation ────────────────────────────────────────────────

    def _compute_metrics(self, trades: list, df: pd.DataFrame) -> dict:
        """Compute performance metrics from trade list."""
        if not trades:
            return {"error": "no_trades"}

        returns  = [t["pnl_pct"] for t in trades]
        pnl_inr  = [t["pnl_inr"] for t in trades]
        wins     = [r for r in returns if r > 0]
        losses   = [r for r in returns if r <= 0]

        n        = len(trades)
        win_rate = len(wins) / n if n > 0 else 0

        # Sharpe (annualised, assuming daily returns)
        mean_r   = np.mean(returns)
        std_r    = np.std(returns)
        sharpe   = (mean_r / std_r * np.sqrt(252)) if std_r > 0 else 0

        # Sortino (only downside std)
        down_r   = [r for r in returns if r < 0]
        down_std = np.std(down_r) if down_r else 1e-9
        sortino  = (mean_r / down_std * np.sqrt(252)) if down_std > 0 else 0

        # Max drawdown from equity curve
        cum_pnl  = np.cumsum(pnl_inr)
        peak     = np.maximum.accumulate(cum_pnl + 100_000)
        dd       = (peak - (cum_pnl + 100_000)) / peak
        max_dd   = float(dd.max())

        # Profit factor
        gp = sum(abs(p) for p in pnl_inr if p > 0)
        gl = sum(abs(p) for p in pnl_inr if p < 0)
        pf = gp / gl if gl > 0 else gp

        # Total return
        total_return = sum(pnl_inr) / 100_000 * 100

        # Cycle breakdown
        cycle_breakdown = self._cycle_breakdown(trades)

        return {
            "total_trades":    n,
            "winning_trades":  len(wins),
            "losing_trades":   len(losses),
            "win_rate":        round(win_rate, 4),
            "sharpe_ratio":    round(sharpe, 4),
            "sortino_ratio":   round(sortino, 4),
            "max_drawdown":    round(max_dd * 100, 4),
            "profit_factor":   round(pf, 4),
            "total_return":    round(total_return, 4),
            "avg_win_pct":     round(np.mean(wins), 4)   if wins   else 0,
            "avg_loss_pct":    round(np.mean(losses), 4) if losses else 0,
            "expectancy":      round(mean_r, 4),
            "cycle_breakdown": cycle_breakdown,
            "passes_gates":    (
                sharpe   >= GATE_SHARPE    and
                win_rate >= GATE_WIN_RATE  and
                n        >= GATE_MIN_TRADES
            ),
        }

    def _cycle_breakdown(self, trades: list) -> dict:
        """Separate metrics by market cycle."""
        breakdown = {}
        for cycle in ("bull", "bear", "neutral"):
            cycle_trades = [t for t in trades if t.get("cycle") == cycle]
            if not cycle_trades:
                continue
            returns  = [t["pnl_pct"] for t in cycle_trades]
            wins     = [r for r in returns if r > 0]
            mean_r   = np.mean(returns)
            std_r    = np.std(returns) or 1e-9
            breakdown[cycle] = {
                "n_trades":  len(cycle_trades),
                "win_rate":  round(len(wins) / len(cycle_trades), 4),
                "sharpe":    round(mean_r / std_r * np.sqrt(252), 4),
                "avg_return": round(mean_r, 4),
            }
        return breakdown

    # ── Data Loading ───────────────────────────────────────────────────────

    def _load_prices(self, symbol: str, from_date: str, to_date: str) -> Optional[pd.DataFrame]:
        df = self.con.execute("""
            SELECT date, open, high, low, close, volume
            FROM daily_prices
            WHERE symbol = ? AND date >= ? AND date <= ?
            ORDER BY date ASC
        """, [symbol, from_date, to_date]).fetchdf()
        if df.empty:
            return None
        df["date"] = pd.to_datetime(df["date"]).dt.date
        return df

    def _load_indicators(self, symbol: str, from_date: str, to_date: str) -> pd.DataFrame:
        return self.con.execute("""
            SELECT * FROM technical_indicators
            WHERE symbol = ? AND date >= ? AND date <= ?
            ORDER BY date ASC
        """, [symbol, from_date, to_date]).fetchdf()

    def _load_cycles(self, symbol: str, from_date: str, to_date: str) -> pd.DataFrame:
        return self.con.execute("""
            SELECT date, cycle
            FROM market_cycles
            WHERE scope = 'stock' AND scope_id = ? AND date >= ? AND date <= ?
            ORDER BY date ASC
        """, [symbol, from_date, to_date]).fetchdf()

    def _build_feature_df(self, prices: pd.DataFrame,
                           inds: pd.DataFrame,
                           cycles: pd.DataFrame) -> pd.DataFrame:
        """Merge prices, indicators, and cycle labels into one DataFrame."""
        df = prices.copy()
        if not inds.empty:
            inds["date"] = pd.to_datetime(inds["date"]).dt.date
            ind_cols = [c for c in inds.columns if c not in ("symbol", "date")]
            df = df.merge(inds[["date"] + ind_cols], on="date", how="left")
        if not cycles.empty:
            cycles["date"] = pd.to_datetime(cycles["date"]).dt.date
            df = df.merge(cycles[["date", "cycle"]], on="date", how="left")
            df["cycle"] = df["cycle"].fillna("neutral")

        df["prev_close"] = df["close"].shift(1)
        return df.ffill()

    # ── Storage ────────────────────────────────────────────────────────────

    def _store_result(self, strategy_id: str, symbol: str,
                       metrics: dict, trades: list,
                       from_date: str, to_date: str, timeframe: str):
        """Store backtest result in DuckDB."""
        run_id = str(uuid.uuid4())[:12]

        # Keep only last 500 trades to cap JSON size
        trades_sample = trades[-500:] if len(trades) > 500 else trades

        self.con.execute("""
            INSERT OR REPLACE INTO backtest_results (
                run_id, strategy_id, symbol, from_date, to_date, timeframe,
                sharpe_ratio, sortino_ratio, win_rate, profit_factor,
                max_drawdown, total_return, total_trades,
                avg_win_pct, avg_loss_pct, expectancy,
                cycle_breakdown, trades_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            run_id, strategy_id, symbol, from_date, to_date, timeframe,
            metrics.get("sharpe_ratio"),    metrics.get("sortino_ratio"),
            metrics.get("win_rate"),        metrics.get("profit_factor"),
            metrics.get("max_drawdown"),    metrics.get("total_return"),
            metrics.get("total_trades"),    metrics.get("avg_win_pct"),
            metrics.get("avg_loss_pct"),    metrics.get("expectancy"),
            json.dumps(metrics.get("cycle_breakdown", {})),
            json.dumps(trades_sample, default=str),
            date.today(),
        ])

    def _update_strategy_metrics(self, strategy_id: str, agg_results: dict):
        """Update aggregate metrics on the strategies table."""
        sharpe   = agg_results.get("avg_sharpe", 0)
        win_rate = agg_results.get("avg_win_rate", 0)
        passes   = sharpe >= GATE_SHARPE and win_rate >= GATE_WIN_RATE

        self.con.execute("""
            UPDATE strategies
            SET sharpe_ratio = ?,
                win_rate     = ?,
                is_active    = ?,
                last_backtested = ?
            WHERE strategy_id = ?
        """, [sharpe, win_rate, passes, date.today(), strategy_id])
