"""
alphalens/core/portfolio/manager.py

Portfolio slot manager — tracks available capacity and capital allocation
across the 4 timeframes, enforces slot limits, and computes position sizes.

Key responsibilities:
  - Count open positions per timeframe
  - Determine if a new signal can be added (slot available)
  - Calculate position size (capital / slots, capped at per-trade max)
  - When portfolio is full: run ExitAdvisor for 3-perspective recommendation
  - Track booked and notional P&L

Usage:
    pm = PortfolioManager()
    capacity = pm.get_capacity("swing")       # {"available": 2, "used": 3, "max": 5}
    can_add  = pm.can_add("swing")            # True / False
    size     = pm.position_size("swing", 450) # 2222 shares (₹1L / ₹450)
    pm.open_position(holding_dict)
    pm.close_position(holding_id, exit_price, reason)
"""

from datetime import date, datetime
from typing import Optional

from loguru import logger

from alphalens.core.database import (
    get_sqlite, get_config, PortfolioHolding, ClosedTrade, SignalLog
)


class PortfolioManager:

    def __init__(self):
        self._slot_limits   = self._load_slot_limits()
        self._capital_alloc = self._load_capital_alloc()

    # ── Capacity ───────────────────────────────────────────────────────────

    def get_capacity(self, timeframe: str) -> dict:
        """Return slot capacity info for a timeframe."""
        used  = self._count_open_positions(timeframe)
        max_s = self._slot_limits.get(timeframe, 5)
        return {
            "timeframe": timeframe,
            "used":      used,
            "max":       max_s,
            "available": max(0, max_s - used),
            "pct_full":  used / max_s if max_s > 0 else 1.0,
        }

    def get_all_capacity(self) -> dict:
        """Return capacity for all 4 timeframes."""
        return {
            tf: self.get_capacity(tf)
            for tf in ("intraday", "swing", "medium", "long_term")
        }

    def can_add(self, timeframe: str) -> bool:
        """True if there is at least one open slot in this timeframe."""
        cap = self.get_capacity(timeframe)
        return cap["available"] > 0

    # ── Position Sizing ───────────────────────────────────────────────────

    def position_size(self, timeframe: str, stock_price: float,
                       risk_pct: float = 0.02) -> dict:
        """
        Calculate position size for a new entry.

        Uses fixed fractional sizing:
          - Allocate risk_pct of timeframe capital per trade
          - Round down to whole shares
          - Cap at max_per_trade capital

        Returns dict with qty, value_inr, capital_used_pct
        """
        capital        = self._capital_alloc.get(timeframe, 500_000)
        slots          = self._slot_limits.get(timeframe, 5)
        per_slot_cap   = capital / slots
        position_value = min(per_slot_cap, capital * risk_pct)

        if stock_price <= 0:
            return {"qty": 0, "value_inr": 0, "capital_used_pct": 0}

        qty   = max(1, int(position_value / stock_price))
        value = qty * stock_price

        return {
            "qty":              qty,
            "value_inr":        round(value, 2),
            "capital_used_pct": round(value / capital * 100, 2) if capital > 0 else 0,
            "per_slot_capital": round(per_slot_cap, 2),
        }

    # ── Position Lifecycle ────────────────────────────────────────────────

    def open_position(self, symbol: str, timeframe: str,
                       qty: int, avg_cost: float,
                       target: float, stop_loss: float,
                       strategy_id: str = None,
                       notes: str = None) -> int:
        """Add a new holding to the portfolio. Returns holding_id."""
        with get_sqlite() as session:
            holding = PortfolioHolding(
                symbol          = symbol,
                timeframe       = timeframe,
                qty             = qty,
                avg_cost        = avg_cost,
                entry_date      = date.today(),
                strategy_id     = strategy_id,
                current_target  = target,
                current_stop_loss = stop_loss,
                source          = "system",
                is_active       = True,
                notes           = notes,
                created_at      = datetime.now(),
                updated_at      = datetime.now(),
            )
            session.add(holding)
            session.flush()
            holding_id = holding.holding_id
            logger.info(f"Opened position: {symbol} [{timeframe}] qty={qty} @₹{avg_cost:.2f}")
            return holding_id

    def close_position(self, holding_id: int, exit_price: float,
                        reason: str = "manual") -> dict:
        """
        Close a portfolio position and create a ClosedTrade record.
        Returns trade summary dict.
        """
        with get_sqlite() as session:
            holding = session.get(PortfolioHolding, holding_id)
            if not holding:
                return {"error": "holding_not_found"}

            holding_days = (date.today() - holding.entry_date).days if holding.entry_date else 0
            pnl          = (exit_price - holding.avg_cost) * holding.qty
            pnl_pct      = (exit_price / holding.avg_cost - 1) * 100 if holding.avg_cost else 0
            tax_type     = "LTCG" if holding_days > 365 else "STCG"

            trade = ClosedTrade(
                symbol         = holding.symbol,
                timeframe      = holding.timeframe,
                qty            = holding.qty,
                entry_date     = holding.entry_date,
                entry_price    = holding.avg_cost,
                exit_date      = date.today(),
                exit_price     = exit_price,
                booked_pnl     = pnl,
                booked_pnl_pct = pnl_pct,
                holding_days   = holding_days,
                tax_type       = tax_type,
                strategy_id    = holding.strategy_id,
                exit_reason    = reason,
                created_at     = datetime.now(),
            )
            session.add(trade)

            # Mark holding as inactive
            holding.is_active   = False
            holding.updated_at  = datetime.now()

            logger.info(
                f"Closed position: {holding.symbol} [{holding.timeframe}] "
                f"exit=₹{exit_price:.2f} PnL=₹{pnl:.0f} ({pnl_pct:+.1f}%) "
                f"reason={reason} tax={tax_type}"
            )

            return {
                "symbol":      holding.symbol,
                "pnl":         pnl,
                "pnl_pct":     pnl_pct,
                "holding_days": holding_days,
                "tax_type":    tax_type,
                "exit_reason": reason,
            }

    def update_targets(self, holding_id: int,
                        new_target: float = None,
                        new_stop_loss: float = None) -> bool:
        """Update target and/or stop-loss for an open position."""
        with get_sqlite() as session:
            holding = session.get(PortfolioHolding, holding_id)
            if not holding:
                return False
            if new_target is not None:
                holding.current_target = new_target
            if new_stop_loss is not None:
                holding.current_stop_loss = new_stop_loss
            holding.last_reviewed_at = datetime.now()
            holding.updated_at       = datetime.now()
            return True

    # ── Portfolio Queries ─────────────────────────────────────────────────

    def get_holdings(self, timeframe: str = None,
                      symbol: str = None) -> list:
        """Return all open holdings, optionally filtered."""
        with get_sqlite() as session:
            q = session.query(PortfolioHolding).filter(PortfolioHolding.is_active == True)
            if timeframe:
                q = q.filter(PortfolioHolding.timeframe == timeframe)
            if symbol:
                q = q.filter(PortfolioHolding.symbol == symbol)
            holdings = q.all()

            result = []
            for h in holdings:
                result.append({
                    "holding_id":   h.holding_id,
                    "symbol":       h.symbol,
                    "timeframe":    h.timeframe,
                    "qty":          h.qty,
                    "avg_cost":     h.avg_cost,
                    "entry_date":   h.entry_date,
                    "target":       h.current_target,
                    "stop_loss":    h.current_stop_loss,
                    "strategy_id":  h.strategy_id,
                    "source":       h.source,
                    "notes":        h.notes,
                    "last_reviewed": h.last_reviewed_at,
                })
            return result

    def get_holding(self, holding_id: int) -> Optional[dict]:
        """Return a single holding by ID."""
        with get_sqlite() as session:
            h = session.get(PortfolioHolding, holding_id)
            if not h:
                return None
            return {
                "holding_id":   h.holding_id,
                "symbol":       h.symbol,
                "timeframe":    h.timeframe,
                "qty":          h.qty,
                "avg_cost":     h.avg_cost,
                "entry_date":   h.entry_date,
                "target":       h.current_target,
                "stop_loss":    h.current_stop_loss,
                "strategy_id":  h.strategy_id,
            }

    def get_symbols_in_portfolio(self) -> set:
        """Return set of all symbols currently in portfolio (any timeframe)."""
        holdings = self.get_holdings()
        return {h["symbol"] for h in holdings}

    # ── Exit Advisor ──────────────────────────────────────────────────────

    def suggest_exit_candidate(self, timeframe: str, new_signal: dict) -> dict:
        """
        When portfolio is full, suggest which existing holding to exit.
        Returns 3-perspective analysis:
          1. ML score perspective  (weakest signal score)
          2. Tax perspective       (prefer STCG exits to avoid LTCG clock)
          3. P&L perspective       (lock profits on largest winner)
        """
        holdings    = self.get_holdings(timeframe=timeframe)
        if not holdings:
            return {"error": "no_holdings"}

        new_sym = new_signal.get("symbol")
        prices  = self._get_current_prices([h["symbol"] for h in holdings])

        candidates = []
        for h in holdings:
            current_price = prices.get(h["symbol"], h["avg_cost"])
            pnl_pct       = (current_price / h["avg_cost"] - 1) * 100 if h["avg_cost"] else 0
            holding_days  = (date.today() - h["entry_date"]).days if h["entry_date"] else 0

            candidates.append({
                **h,
                "current_price": current_price,
                "pnl_pct":       pnl_pct,
                "pnl_inr":       (current_price - h["avg_cost"]) * h["qty"],
                "holding_days":  holding_days,
                "tax_type":      "LTCG" if holding_days > 365 else "STCG",
                "signal_score":  self._get_signal_score(h["symbol"], timeframe),
            })

        # ── Perspective 1: Weakest ML signal ──────────────────────────
        ml_candidate = min(candidates, key=lambda x: x.get("signal_score", 0))

        # ── Perspective 2: Tax efficiency (exit STCG if approaching 1yr)
        # Prefer to exit STCG holdings near 365 days (if selling, do it at STCG vs near-LTCG)
        # OR prefer to keep holdings that just turned LTCG (no immediate tax urgency)
        def tax_score(c):
            d = c["holding_days"]
            if d > 365:
                return 0   # LTCG — prefer to keep (low tax urgency)
            if d >= 330:
                return 3   # About to turn LTCG — consider booking before if losing
            return 1       # STCG — normal
        tax_candidate = max(candidates, key=lambda x: tax_score(x) + (x["pnl_pct"] < 0) * 2)

        # ── Perspective 3: Lock profits on biggest winner ──────────────
        profit_candidate = max(candidates, key=lambda x: x.get("pnl_pct", 0))

        return {
            "new_signal_symbol": new_sym,
            "timeframe":         timeframe,
            "perspectives": {
                "ml_score": {
                    "symbol":    ml_candidate["symbol"],
                    "reason":    f"Weakest current signal score ({ml_candidate.get('signal_score', 0):.2f}). "
                                 f"P&L: {ml_candidate['pnl_pct']:+.1f}%",
                    "holding":   ml_candidate,
                },
                "tax_efficiency": {
                    "symbol":    tax_candidate["symbol"],
                    "reason":    self._tax_reason(tax_candidate),
                    "holding":   tax_candidate,
                },
                "pnl_lock": {
                    "symbol":    profit_candidate["symbol"],
                    "reason":    f"Lock in largest profit: {profit_candidate['pnl_pct']:+.1f}% "
                                 f"(₹{profit_candidate['pnl_inr']:+,.0f}). "
                                 f"Tax: {profit_candidate['tax_type']}",
                    "holding":   profit_candidate,
                },
            },
        }

    # ── Config Reload ─────────────────────────────────────────────────────

    def reload_config(self):
        """Reload slot limits and capital from config table."""
        self._slot_limits   = self._load_slot_limits()
        self._capital_alloc = self._load_capital_alloc()

    # ── Private ───────────────────────────────────────────────────────────

    def _count_open_positions(self, timeframe: str) -> int:
        with get_sqlite() as session:
            return session.query(PortfolioHolding).filter(
                PortfolioHolding.timeframe == timeframe,
                PortfolioHolding.is_active == True
            ).count()

    def _load_slot_limits(self) -> dict:
        return {
            "intraday":  get_config("intraday_slots",  3),
            "swing":     get_config("swing_slots",     5),
            "medium":    get_config("medium_slots",    8),
            "long_term": get_config("longterm_slots",  15),
        }

    def _load_capital_alloc(self) -> dict:
        return {
            "intraday":  get_config("intraday_capital",  250_000),
            "swing":     get_config("swing_capital",     500_000),
            "medium":    get_config("medium_capital",    750_000),
            "long_term": get_config("longterm_capital",  1_000_000),
        }

    def _get_current_prices(self, symbols: list) -> dict:
        """Get latest close price for a list of symbols from DuckDB."""
        if not symbols:
            return {}
        from alphalens.core.database import get_duck
        con   = get_duck()
        ph    = ", ".join(["?"] * len(symbols))
        rows  = con.execute(f"""
            SELECT symbol, close FROM daily_prices
            WHERE (symbol, date) IN (
                SELECT symbol, MAX(date) FROM daily_prices
                WHERE symbol IN ({ph}) GROUP BY symbol
            )
        """, symbols).fetchall()
        return {r[0]: float(r[1]) for r in rows}

    def _get_signal_score(self, symbol: str, timeframe: str) -> float:
        """Get latest signal confidence for a symbol from signals_log."""
        with get_sqlite() as session:
            sig = session.query(SignalLog).filter(
                SignalLog.symbol    == symbol,
                SignalLog.timeframe == timeframe,
                SignalLog.is_active == True,
            ).order_by(SignalLog.generated_at.desc()).first()
            return sig.confidence or 0.5 if sig else 0.5

    def _tax_reason(self, holding: dict) -> str:
        days = holding.get("holding_days", 0)
        pnl  = holding.get("pnl_pct", 0)
        if days > 365:
            return f"LTCG holding ({days} days). Tax-efficient to exit now."
        if days >= 330:
            return f"Approaching LTCG ({365-days} days to go). If in loss, book STCG now."
        return f"STCG holding ({days} days). P&L: {pnl:+.1f}%"
