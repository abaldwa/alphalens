"""
alphalens/core/portfolio/pnl.py

P&L tracking — computes and snapshots booked and notional P&L.

Tracks:
  - Booked P&L:   realised gains/losses from closed trades
  - Notional P&L: unrealised gains/losses on open positions
  - Total P&L:    booked + notional
  - STCG/LTCG breakdown for tax planning
  - Per-timeframe breakdown
  - Benchmark comparison: vs Nifty200 buy-and-hold

Daily snapshots are stored in pnl_snapshots SQLite table.

Usage:
    pnl = PnlTracker()
    summary = pnl.get_summary()           # current P&L snapshot
    pnl.take_snapshot()                   # store today's snapshot
    history = pnl.get_history(days=90)    # DataFrame of daily P&L
"""

from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
from loguru import logger

from alphalens.core.database import (
    get_duck, get_sqlite,
    PortfolioHolding, ClosedTrade, PnlSnapshot
)
from config.settings import settings


class PnlTracker:

    def __init__(self):
        self.con = get_duck()

    # ── Summary ────────────────────────────────────────────────────────────

    def get_summary(self) -> dict:
        """
        Compute current P&L summary across all timeframes.
        Returns booked + notional + total + STCG/LTCG breakdown.
        """
        booked   = self._compute_booked_pnl()
        notional = self._compute_notional_pnl()

        total_booked   = sum(v["booked_pnl"]   for v in booked.values())
        total_notional = sum(v["notional_pnl"]  for v in notional.values())

        # STCG / LTCG split
        tax_breakdown = self._compute_tax_breakdown()

        # Capital utilisation
        invested  = sum(v["invested_capital"] for v in notional.values())
        total_cap = sum(settings.capital_config.values())
        cash_avail = total_cap - invested

        # Per-timeframe merge
        per_tf = {}
        for tf in ("intraday", "swing", "medium", "long_term"):
            per_tf[tf] = {
                "booked_pnl":    booked.get(tf, {}).get("booked_pnl", 0),
                "notional_pnl":  notional.get(tf, {}).get("notional_pnl", 0),
                "total_pnl":     booked.get(tf, {}).get("booked_pnl", 0) + notional.get(tf, {}).get("notional_pnl", 0),
                "open_positions": notional.get(tf, {}).get("open_positions", 0),
                "invested":      notional.get(tf, {}).get("invested_capital", 0),
            }

        return {
            "as_of":               date.today(),
            "total_booked_pnl":    round(total_booked, 2),
            "total_notional_pnl":  round(total_notional, 2),
            "total_pnl":           round(total_booked + total_notional, 2),
            "total_pnl_pct":       round((total_booked + total_notional) / total_cap * 100, 2) if total_cap else 0,
            "invested_capital":    round(invested, 2),
            "cash_available":      round(cash_avail, 2),
            "capital_utilisation": round(invested / total_cap * 100, 2) if total_cap else 0,
            "tax_breakdown":       tax_breakdown,
            "by_timeframe":        per_tf,
        }

    def take_snapshot(self):
        """Store today's P&L snapshot in SQLite."""
        summary = self.get_summary()
        today   = date.today()

        with get_sqlite() as session:
            # Total snapshot
            total_snap = PnlSnapshot(
                snapshot_date    = today,
                timeframe        = "total",
                booked_pnl       = summary["total_booked_pnl"],
                notional_pnl     = summary["total_notional_pnl"],
                total_pnl        = summary["total_pnl"],
                portfolio_value  = summary["invested_capital"] + summary["total_notional_pnl"],
                invested_capital = summary["invested_capital"],
                cash_available   = summary["cash_available"],
                open_positions   = sum(v["open_positions"] for v in summary["by_timeframe"].values()),
                closed_trades    = self._count_closed_trades_today(),
            )
            session.merge(total_snap)

            # Per-timeframe snapshots
            for tf, data in summary["by_timeframe"].items():
                tf_snap = PnlSnapshot(
                    snapshot_date    = today,
                    timeframe        = tf,
                    booked_pnl       = data["booked_pnl"],
                    notional_pnl     = data["notional_pnl"],
                    total_pnl        = data["total_pnl"],
                    portfolio_value  = data["invested"] + data["notional_pnl"],
                    invested_capital = data["invested"],
                    open_positions   = data["open_positions"],
                )
                session.merge(tf_snap)

        logger.debug(f"P&L snapshot taken: total_pnl=₹{summary['total_pnl']:,.0f}")

    def get_history(self, timeframe: str = "total",
                     days: int = 90) -> pd.DataFrame:
        """Return P&L history as a DataFrame for charting."""
        from_date = date.today() - timedelta(days=days)

        with get_sqlite() as session:
            rows = session.query(PnlSnapshot).filter(
                PnlSnapshot.timeframe     == timeframe,
                PnlSnapshot.snapshot_date >= from_date,
            ).order_by(PnlSnapshot.snapshot_date).all()

            return pd.DataFrame([{
                "date":           r.snapshot_date,
                "booked_pnl":     r.booked_pnl or 0,
                "notional_pnl":   r.notional_pnl or 0,
                "total_pnl":      r.total_pnl or 0,
                "portfolio_value": r.portfolio_value or 0,
            } for r in rows])

    def get_closed_trades_summary(self,
                                   timeframe: str = None,
                                   from_date: date = None,
                                   to_date: date = None) -> dict:
        """Summarise all closed trades with STCG/LTCG breakdown."""
        with get_sqlite() as session:
            q = session.query(ClosedTrade)
            if timeframe:
                q = q.filter(ClosedTrade.timeframe == timeframe)
            if from_date:
                q = q.filter(ClosedTrade.exit_date >= from_date)
            if to_date:
                q = q.filter(ClosedTrade.exit_date <= to_date)

            trades = q.all()

            if not trades:
                return {"trades": 0, "total_pnl": 0, "stcg": 0, "ltcg": 0}

            total_pnl = sum(t.booked_pnl or 0 for t in trades)
            stcg_pnl  = sum(t.booked_pnl or 0 for t in trades if t.tax_type == "STCG")
            ltcg_pnl  = sum(t.booked_pnl or 0 for t in trades if t.tax_type == "LTCG")
            winners   = [t for t in trades if (t.booked_pnl or 0) > 0]
            losers    = [t for t in trades if (t.booked_pnl or 0) <= 0]

            return {
                "trades":       len(trades),
                "winners":      len(winners),
                "losers":       len(losers),
                "win_rate":     round(len(winners) / len(trades), 4) if trades else 0,
                "total_pnl":    round(total_pnl, 2),
                "stcg_pnl":     round(stcg_pnl, 2),
                "ltcg_pnl":     round(ltcg_pnl, 2),
                "avg_win":      round(sum(t.booked_pnl_pct or 0 for t in winners) / len(winners), 2) if winners else 0,
                "avg_loss":     round(sum(t.booked_pnl_pct or 0 for t in losers)  / len(losers),  2) if losers  else 0,
                "trades_list":  [self._trade_to_dict(t) for t in sorted(trades, key=lambda x: x.exit_date or date.today(), reverse=True)],
            }

    # ── Private ────────────────────────────────────────────────────────────

    def _compute_booked_pnl(self) -> dict:
        """Sum of all closed trade P&L, grouped by timeframe."""
        with get_sqlite() as session:
            rows = session.query(
                ClosedTrade.timeframe, ClosedTrade.booked_pnl
            ).all()

            result = {tf: {"booked_pnl": 0} for tf in ("intraday", "swing", "medium", "long_term")}
            for tf, pnl in rows:
                if tf in result:
                    result[tf]["booked_pnl"] += pnl or 0
            return result

    def _compute_notional_pnl(self) -> dict:
        """Unrealised P&L on open holdings at current market price."""
        with get_sqlite() as session:
            holdings = session.query(PortfolioHolding).filter(
                PortfolioHolding.is_active == True
            ).all()

        if not holdings:
            return {tf: {"notional_pnl": 0, "open_positions": 0, "invested_capital": 0}
                    for tf in ("intraday", "swing", "medium", "long_term")}

        symbols = [h.symbol for h in holdings]
        prices  = self._get_current_prices(symbols)

        result = {tf: {"notional_pnl": 0, "open_positions": 0, "invested_capital": 0}
                  for tf in ("intraday", "swing", "medium", "long_term")}

        for h in holdings:
            tf      = h.timeframe
            if tf not in result:
                continue
            current   = prices.get(h.symbol, h.avg_cost)
            notional  = (current - h.avg_cost) * h.qty
            invested  = h.avg_cost * h.qty

            result[tf]["notional_pnl"]   += notional
            result[tf]["open_positions"] += 1
            result[tf]["invested_capital"] += invested

        return result

    def _compute_tax_breakdown(self) -> dict:
        """Compute STCG/LTCG from closed trades + notional on open holdings."""
        with get_sqlite() as session:
            closed = session.query(ClosedTrade).all()
            stcg_realised = sum(t.booked_pnl or 0 for t in closed if t.tax_type == "STCG" and (t.booked_pnl or 0) > 0)
            ltcg_realised = sum(t.booked_pnl or 0 for t in closed if t.tax_type == "LTCG" and (t.booked_pnl or 0) > 0)
            stcg_loss     = sum(t.booked_pnl or 0 for t in closed if t.tax_type == "STCG" and (t.booked_pnl or 0) < 0)
            ltcg_loss     = sum(t.booked_pnl or 0 for t in closed if t.tax_type == "LTCG" and (t.booked_pnl or 0) < 0)

        return {
            "stcg_gains":  round(stcg_realised, 2),
            "stcg_losses": round(stcg_loss, 2),
            "ltcg_gains":  round(ltcg_realised, 2),
            "ltcg_losses": round(ltcg_loss, 2),
            "net_stcg":    round(stcg_realised + stcg_loss, 2),
            "net_ltcg":    round(ltcg_realised + ltcg_loss, 2),
            "estimated_stcg_tax": round(max(0, (stcg_realised + stcg_loss)) * 0.15, 2),  # 15% STCG
            "estimated_ltcg_tax": round(max(0, (ltcg_realised + ltcg_loss - 100_000)) * 0.10, 2),  # 10% on gains > ₹1L
        }

    def _get_current_prices(self, symbols: list) -> dict:
        if not symbols:
            return {}
        ph   = ", ".join(["?"] * len(symbols))
        rows = self.con.execute(f"""
            SELECT symbol, close FROM daily_prices
            WHERE (symbol, date) IN (
                SELECT symbol, MAX(date) FROM daily_prices
                WHERE symbol IN ({ph}) GROUP BY symbol
            )
        """, symbols).fetchall()
        return {r[0]: float(r[1]) for r in rows}

    def _count_closed_trades_today(self) -> int:
        with get_sqlite() as session:
            return session.query(ClosedTrade).filter(
                ClosedTrade.exit_date == date.today()
            ).count()

    @staticmethod
    def _trade_to_dict(t: ClosedTrade) -> dict:
        return {
            "symbol":       t.symbol,
            "timeframe":    t.timeframe,
            "qty":          t.qty,
            "entry_date":   str(t.entry_date),
            "entry_price":  t.entry_price,
            "exit_date":    str(t.exit_date),
            "exit_price":   t.exit_price,
            "booked_pnl":   round(t.booked_pnl or 0, 2),
            "pnl_pct":      round(t.booked_pnl_pct or 0, 2),
            "holding_days": t.holding_days,
            "tax_type":     t.tax_type,
            "exit_reason":  t.exit_reason,
        }
