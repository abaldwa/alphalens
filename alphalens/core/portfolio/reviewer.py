"""
alphalens/core/portfolio/reviewer.py

Portfolio review engine — runs at 9:30 AM, 3:00 PM, and 6:30 PM.

Reviews:
  9:30 AM  — Gap analysis: gap-up/gap-down for portfolio stocks + intraday setups
  3:00 PM  — Pre-close: intraday exit check, SL update to breakeven
  6:30 PM  — EOD: update all targets/SLs, generate tomorrow's watchlist
  Monthly  — Investment portfolio: ML recommendation per holding
  Ongoing  — 10% drawdown monitor for long-term holdings
"""

from datetime import date, datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
from loguru import logger

from alphalens.core.database import get_duck, get_sqlite, PortfolioHolding
from alphalens.core.portfolio.manager import PortfolioManager
from alphalens.core.strategy.backtester import Backtester
from alphalens.core.strategy.library import get_strategy
from alphalens.core.cycle.context import get_cycle_context
from config.settings import settings


class PortfolioReviewer:

    def __init__(self):
        self.con   = get_duck()
        self.pm    = PortfolioManager()
        self.bt    = Backtester()

    # ── 9:30 AM Review ────────────────────────────────────────────────────

    def run_gap_analysis(self) -> dict:
        """
        9:30 AM — Gap analysis for all portfolio stocks and top watchlist stocks.

        Returns:
          gap_ups:          stocks with gap-up > 1% (portfolio + watchlist)
          gap_downs:        stocks with gap-down < -1%
          intraday_signals: top 3 intraday setups for today
        """
        holdings = self.pm.get_holdings()
        symbols  = list({h["symbol"] for h in holdings})

        # Add top watchlist symbols
        watchlist_symbols = self._get_watchlist_symbols("intraday", limit=20)
        all_symbols = list(set(symbols + watchlist_symbols))

        gap_ups   = []
        gap_downs = []

        for symbol in all_symbols:
            gap = self._compute_gap(symbol)
            if gap is None:
                continue
            if gap["gap_pct"] >= 1.0:
                gap_ups.append(gap)
            elif gap["gap_pct"] <= -1.0:
                gap_downs.append(gap)

        gap_ups.sort(key=lambda x: x["gap_pct"], reverse=True)
        gap_downs.sort(key=lambda x: x["gap_pct"])

        # Top intraday setups from watchlist
        intraday_signals = self._get_top_intraday_setups(3)

        return {
            "gap_ups":          gap_ups,
            "gap_downs":        gap_downs,
            "intraday_signals": intraday_signals,
            "reviewed_at":      datetime.now(),
        }

    # ── 3:00 PM Review ────────────────────────────────────────────────────

    def run_preclose_intraday_check(self) -> list:
        """
        3:00 PM — Check all open intraday positions.

        For each intraday holding:
          - If in profit: update SL to breakeven
          - If target not hit: alert + suggest exit by 3:15 PM
          - If SL breached: exit alert

        Returns list of alert dicts.
        """
        holdings = self.pm.get_holdings(timeframe="intraday")
        prices   = self._get_current_prices([h["symbol"] for h in holdings])
        alerts   = []

        for holding in holdings:
            symbol  = holding["symbol"]
            current = prices.get(symbol, holding["avg_cost"])
            pnl_pct = (current / holding["avg_cost"] - 1) * 100 if holding["avg_cost"] else 0

            alert = {
                "symbol":    symbol,
                "holding_id": holding["holding_id"],
                "avg_cost":  holding["avg_cost"],
                "current":   current,
                "pnl_pct":   pnl_pct,
                "target":    holding.get("target"),
                "stop_loss": holding.get("stop_loss"),
            }

            if current <= (holding.get("stop_loss") or 0):
                alert["action"] = "EXIT_NOW"
                alert["reason"] = "Stop loss breached"

            elif current < holding["avg_cost"]:
                # In loss — update SL to breakeven if we're close
                alert["action"] = "REVIEW_EXIT"
                alert["reason"] = f"Target not reached. Consider exiting by 3:15 PM"
                alert["new_sl"] = holding["avg_cost"]
                # Update SL to breakeven
                self.pm.update_targets(holding["holding_id"], new_stop_loss=holding["avg_cost"])

            elif pnl_pct >= 0.5:
                # In profit — trail SL to lock in gains
                trail_sl = current * 0.995   # 0.5% below current
                alert["action"] = "TRAILING_SL_UPDATED"
                alert["reason"] = f"In profit {pnl_pct:+.1f}% — SL trailed to ₹{trail_sl:.2f}"
                alert["new_sl"] = round(trail_sl, 2)
                self.pm.update_targets(holding["holding_id"], new_stop_loss=round(trail_sl, 2))

            else:
                alert["action"] = "MONITOR"
                alert["reason"] = f"P&L: {pnl_pct:+.1f}% — monitor till 3:15 PM"

            alerts.append(alert)

        return alerts

    # ── 6:30 PM EOD Review ────────────────────────────────────────────────

    def run_eod_review(self) -> dict:
        """
        6:30 PM EOD — Update all portfolio targets and stop-losses.

        For each open holding:
          1. Get latest ATR-based SL and target
          2. Apply trailing SL if in profit
          3. Update holding in SQLite
          4. Return summary of changes
        """
        holdings = self.pm.get_holdings()
        prices   = self._get_current_prices([h["symbol"] for h in holdings])
        updates  = []

        for holding in holdings:
            symbol   = holding["symbol"]
            current  = prices.get(symbol, holding["avg_cost"])
            strategy = get_strategy(holding.get("strategy_id", "")) if holding.get("strategy_id") else None

            if strategy is None:
                continue

            # Load latest indicators
            inds = self._load_latest_indicators(symbol)
            if not inds:
                continue

            row = pd.Series({**inds, "close": current})

            # Recompute ATR-based SL
            new_sl     = self.bt.compute_stop_loss(strategy, row, holding["avg_cost"])
            new_target = self.bt.compute_target(strategy, row, holding["avg_cost"], new_sl)

            # Trailing: only move SL up (never down)
            old_sl   = holding.get("stop_loss") or 0
            final_sl = max(old_sl, new_sl)

            if abs(final_sl - old_sl) > 0.01 or abs(new_target - (holding.get("target") or 0)) > 1:
                self.pm.update_targets(
                    holding["holding_id"],
                    new_target    = round(new_target, 2),
                    new_stop_loss = round(final_sl, 2),
                )
                updates.append({
                    "symbol":      symbol,
                    "old_sl":      old_sl,
                    "new_sl":      final_sl,
                    "new_target":  new_target,
                    "current":     current,
                })

        logger.info(f"EOD review: {len(updates)} positions updated")
        return {"updated": len(updates), "details": updates}

    # ── Monthly Investment Review ─────────────────────────────────────────

    def run_monthly_investment_review(self) -> dict:
        """
        1st of month — Full review of all long-term investment holdings.

        For each holding:
          1. Fetch latest price and fundamentals
          2. Run ML recommendation (Hold / Average Down / Exit)
          3. Recompute target and stop-loss based on current price
          4. Return full report for email dispatch
        """
        holdings = self.pm.get_holdings(timeframe="long_term")
        prices   = self._get_current_prices([h["symbol"] for h in holdings])
        ctx      = get_cycle_context()
        results  = []

        for holding in holdings:
            symbol   = holding["symbol"]
            current  = prices.get(symbol, holding["avg_cost"])
            pnl      = (current - holding["avg_cost"]) * holding["qty"]
            pnl_pct  = (current / holding["avg_cost"] - 1) * 100 if holding["avg_cost"] else 0

            # ML recommendation
            rec, reason = self._ml_investment_recommendation(
                holding, current, ctx
            )

            # Recompute target/SL
            inds = self._load_latest_indicators(symbol)
            if inds:
                row        = pd.Series({**inds, "close": current})
                strategy   = get_strategy(holding.get("strategy_id") or "S010")
                if strategy:
                    new_sl     = self.bt.compute_stop_loss(strategy, row, holding["avg_cost"])
                    new_target = self.bt.compute_target(strategy, row, holding["avg_cost"], new_sl)
                    self.pm.update_targets(
                        holding["holding_id"],
                        new_target    = round(new_target, 2),
                        new_stop_loss = round(max(new_sl, holding.get("stop_loss") or 0), 2),
                    )
                else:
                    new_target = current * 1.15
                    new_sl     = current * 0.87

            fund = self._load_fundamentals(symbol)

            results.append({
                "symbol":          symbol,
                "qty":             holding["qty"],
                "avg_cost":        holding["avg_cost"],
                "current_price":   current,
                "pnl":             pnl,
                "pnl_pct":         pnl_pct,
                "target":          round(new_target, 2) if inds else None,
                "stop_loss":       round(new_sl, 2) if inds else None,
                "holding_days":    (date.today() - holding["entry_date"]).days if holding.get("entry_date") else 0,
                "tax_type":        "LTCG" if (holding.get("entry_date") and (date.today() - holding["entry_date"]).days > 365) else "STCG",
                "recommendation":  rec,
                "reason":          reason,
                "fundamentals":    fund,
            })

        logger.info(f"Monthly review: {len(results)} holdings reviewed")
        return {"holdings": results, "reviewed_at": datetime.now()}

    # ── Drawdown Monitor ──────────────────────────────────────────────────

    def check_drawdown_alerts(self) -> list:
        """
        Check all long-term holdings for 10% drawdown below avg cost.
        Returns list of alert dicts with ML recommendations.
        """
        threshold = get_config("drawdown_alert_pct", 0.10)  # default 10%
        holdings  = self.pm.get_holdings(timeframe="long_term")
        prices    = self._get_current_prices([h["symbol"] for h in holdings])
        alerts    = []
        ctx       = get_cycle_context()

        for holding in holdings:
            symbol   = holding["symbol"]
            current  = prices.get(symbol, holding["avg_cost"])
            drawdown = (current - holding["avg_cost"]) / holding["avg_cost"]

            if drawdown <= -threshold:
                rec, reason = self._ml_investment_recommendation(
                    holding, current, ctx
                )
                alerts.append({
                    "symbol":         symbol,
                    "avg_cost":       holding["avg_cost"],
                    "current_price":  current,
                    "drawdown_pct":   drawdown * 100,
                    "recommendation": rec,
                    "reason":         reason,
                    "holding_id":     holding["holding_id"],
                })

        return alerts

    # ── ML Recommendation ─────────────────────────────────────────────────

    def _ml_investment_recommendation(self, holding: dict,
                                        current_price: float,
                                        ctx) -> tuple:
        """
        Generate ML-based recommendation for an investment holding.
        Returns (recommendation_str, reason_str).

        Logic:
          EXIT if:
            - Stock cycle = bear AND market cycle = bear
            - Price < 200 DMA AND RSI < 40
            - P&L < -15%
          AVERAGE DOWN if:
            - Stock fundamentals strong (ROE > 15%)
            - Market cycle = neutral or bull
            - Drawdown 10-20% (buying opportunity)
          HOLD otherwise
        """
        symbol    = holding["symbol"]
        avg_cost  = holding["avg_cost"]
        pnl_pct   = (current_price / avg_cost - 1) * 100 if avg_cost else 0
        drawdown  = pnl_pct

        inds  = self._load_latest_indicators(symbol)
        fund  = self._load_fundamentals(symbol)
        cycle = ctx.get_stock_cycle(symbol).get("cycle", "neutral")

        rsi   = float(inds.get("rsi_14", 50) or 50) if inds else 50
        above_200 = (
            float(inds.get("close", 0) or 0) > float(inds.get("sma_200", 0) or 0)
            if inds else True
        )
        roe   = float(fund.get("roe",  0) or 0) if fund else 0

        # ── EXIT conditions ────────────────────────────────────────────
        if drawdown < -15:
            return "EXIT", (
                f"Loss exceeds 15% ({drawdown:.1f}%). "
                f"Capital preservation: book loss and redeploy."
            )

        if cycle == "bear" and ctx.market_cycle == "bear":
            return "EXIT", (
                f"Both stock ({cycle}) and market ({ctx.market_cycle}) in bear cycle. "
                f"High risk of further decline. Exit and wait for cycle reversal."
            )

        if not above_200 and rsi < 40 and ctx.market_cycle == "bear":
            return "EXIT", (
                f"Price below 200 DMA with RSI={rsi:.0f} in a bear market. "
                f"Trend strongly negative."
            )

        # ── AVERAGE DOWN conditions ────────────────────────────────────
        if (drawdown >= -20 and drawdown <= -10 and
            roe >= 15 and
            ctx.market_cycle in ("neutral", "bull") and
            above_200):
            return "AVERAGE_DOWN", (
                f"Quality stock (ROE={roe:.1f}%) down {abs(drawdown):.1f}% — "
                f"temporary correction in a {ctx.market_cycle} market. "
                f"Consider averaging down to reduce cost basis."
            )

        # ── HOLD ───────────────────────────────────────────────────────
        return "HOLD", (
            f"Hold position. Market: {ctx.market_cycle}, "
            f"Stock cycle: {cycle}, RSI: {rsi:.0f}, "
            f"P&L: {pnl_pct:+.1f}%"
        )

    # ── Helpers ───────────────────────────────────────────────────────────

    def _compute_gap(self, symbol: str) -> Optional[dict]:
        """Compute gap-up/gap-down for a symbol."""
        df = self.con.execute("""
            SELECT date, open, close FROM daily_prices
            WHERE symbol = ? ORDER BY date DESC LIMIT 2
        """, [symbol]).fetchdf()

        if len(df) < 2:
            return None

        prev_close = float(df.iloc[1]["close"])
        today_open = float(df.iloc[0]["open"])
        gap_pct    = (today_open - prev_close) / prev_close * 100 if prev_close else 0

        return {
            "symbol":     symbol,
            "prev_close": prev_close,
            "open":       today_open,
            "gap_pct":    round(gap_pct, 2),
        }

    def _get_top_intraday_setups(self, n: int) -> list:
        """Return top N intraday watchlist items by confidence."""
        with get_sqlite() as session:
            from alphalens.core.database import Watchlist
            items = session.query(Watchlist).filter(
                Watchlist.timeframe == "intraday",
                Watchlist.is_active == True,
                Watchlist.signal_type == "buy",
            ).order_by(Watchlist.confidence.desc()).limit(n).all()

            return [
                {
                    "rank":     i + 1,
                    "symbol":   w.symbol,
                    "entry":    w.suggested_entry,
                    "target":   w.target_price,
                    "sl":       w.stop_loss,
                    "conf":     w.confidence,
                    "strategy": w.strategy_id,
                }
                for i, w in enumerate(items)
            ]

    def _get_watchlist_symbols(self, timeframe: str, limit: int = 20) -> list:
        with get_sqlite() as session:
            from alphalens.core.database import Watchlist
            items = session.query(Watchlist.symbol).filter(
                Watchlist.timeframe == timeframe,
                Watchlist.is_active == True,
            ).limit(limit).all()
            return [r[0] for r in items]

    def _get_current_prices(self, symbols: list) -> dict:
        if not symbols:
            return {}
        ph  = ", ".join(["?"] * len(symbols))
        rows = self.con.execute(f"""
            SELECT symbol, close FROM daily_prices
            WHERE (symbol, date) IN (
                SELECT symbol, MAX(date) FROM daily_prices
                WHERE symbol IN ({ph}) GROUP BY symbol
            )
        """, symbols).fetchall()
        return {r[0]: float(r[1]) for r in rows}

    def _load_latest_indicators(self, symbol: str) -> Optional[dict]:
        row = self.con.execute("""
            SELECT * FROM technical_indicators
            WHERE symbol = ? ORDER BY date DESC LIMIT 1
        """, [symbol]).fetchdf()
        if row.empty:
            return None
        return row.iloc[0].to_dict()

    def _load_fundamentals(self, symbol: str) -> Optional[dict]:
        row = self.con.execute("""
            SELECT * FROM fundamentals
            WHERE symbol = ? ORDER BY period_end DESC LIMIT 1
        """, [symbol]).fetchdf()
        if row.empty:
            return None
        return row.iloc[0].to_dict()
