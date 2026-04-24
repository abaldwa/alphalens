"""
alphalens/core/corporate_actions/adjuster.py

Corporate actions handling — splits, bonus, dividends.

Workflow:
  1. Scraper fetches corp actions from NSE or manual entry
  2. Store in corporate_actions table
  3. On ex-date, run adjuster
  4. Adjust historical prices retroactively
  5. Recalculate indicators
  6. Adjust open positions avg_cost
  7. Adjust pending triggers trigger_price/buy_below_price
  8. Flag affected backtests as stale

Actions supported:
  - Stock split (e.g. 1:2 = 1 becomes 2)
  - Bonus issue (e.g. 1:1 = 1 bonus for each 1 held)
  - Dividend (cash, no price adjust but capture event)
  - Rights (if data available)
"""

import json
from datetime import datetime, date
from typing import Optional

import numpy as np
import pandas as pd
from loguru import logger

from alphalens.core.database import get_duck, get_sqlite


class CorporateActionAdjuster:
    """Handle corporate actions and price adjustments."""

    def __init__(self):
        self.con = get_duck()
        self._ensure_tables()

    def _ensure_tables(self):
        """Create corporate_actions table if not exists."""
        # DuckDB table
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS corporate_actions (
                action_id          TEXT PRIMARY KEY,
                symbol             TEXT NOT NULL,
                action_type        TEXT NOT NULL,
                ex_date            DATE NOT NULL,
                record_date        DATE,
                ratio              REAL,
                cash_amount        REAL,
                adjustment_factor  REAL NOT NULL,
                source             TEXT,
                raw_payload        TEXT,
                processed          INTEGER DEFAULT 0,
                created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    # ── Record Corporate Action ───────────────────────────────────────────

    def record_action(
        self,
        symbol: str,
        action_type: str,
        ex_date: date,
        ratio: float = None,
        cash_amount: float = None,
        record_date: date = None,
        source: str = "manual",
        raw_payload: dict = None,
    ) -> str:
        """
        Record a corporate action.

        Args:
            symbol: Stock symbol
            action_type: "split" | "bonus" | "dividend" | "rights"
            ex_date: Ex-date
            ratio: For split/bonus (e.g. 1:2 split = 0.5, 1:1 bonus = 1.0)
            cash_amount: For dividends
            record_date: Record date
            source: "nse" | "bse" | "manual"
            raw_payload: Original data

        Returns:
            action_id
        """
        import uuid

        action_id = f"{symbol}_{action_type}_{ex_date}_{uuid.uuid4().hex[:8]}"

        # Compute adjustment factor
        adj_factor = self._compute_adjustment_factor(action_type, ratio, cash_amount)

        self.con.execute("""
            INSERT INTO corporate_actions (
                action_id, symbol, action_type, ex_date, record_date,
                ratio, cash_amount, adjustment_factor, source, raw_payload,
                processed, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
        """, [
            action_id, symbol, action_type, ex_date, record_date,
            ratio, cash_amount, adj_factor, source,
            json.dumps(raw_payload) if raw_payload else None,
            datetime.now()
        ])

        logger.info(
            f"Recorded corp action: {symbol} {action_type} ex_date={ex_date} "
            f"ratio={ratio} adj_factor={adj_factor:.6f}"
        )

        return action_id

    def _compute_adjustment_factor(self, action_type: str, ratio: float, cash_amount: float) -> float:
        """
        Compute adjustment factor for historical prices.

        Formula:
          - Split 1:N → adj_factor = N+1 (e.g. 1:2 → 3, 1:10 → 11)
          - Bonus 1:M → adj_factor = M+1 (e.g. 1:1 → 2, 1:2 → 3)
          - Dividend → adj_factor = 1.0 (no price adjustment, capture event only)

        Adjusted price = raw_price / adj_factor (applied retroactively before ex_date)
        """
        if action_type in ("split", "bonus"):
            if ratio is None:
                raise ValueError(f"{action_type} requires ratio")
            return 1 + ratio  # e.g. 1:1 bonus = 1+1 = 2

        elif action_type == "dividend":
            return 1.0  # No price adjustment for dividends

        elif action_type == "rights":
            # Rights are complex; simplified: treat like bonus if ratio given
            return 1 + (ratio if ratio else 0)

        else:
            raise ValueError(f"Unknown action_type: {action_type}")

    # ── Apply Corporate Action ────────────────────────────────────────────

    def apply_action(self, action_id: str) -> dict:
        """
        Apply a corporate action.

        Steps:
          1. Load action details
          2. Adjust historical prices (before ex_date)
          3. Recalculate technical indicators
          4. Adjust open portfolio positions
          5. Adjust pending trigger prices
          6. Mark backtests stale
          7. Set processed = 1

        Returns:
            {"success": bool, "affected_rows": int, ...}
        """
        action = self.con.execute("""
            SELECT symbol, action_type, ex_date, adjustment_factor, processed
            FROM corporate_actions WHERE action_id = ?
        """, [action_id]).fetchone()

        if not action:
            return {"success": False, "error": "action_not_found"}

        symbol, action_type, ex_date, adj_factor, processed = action

        if processed:
            logger.warning(f"Action {action_id} already processed, skipping")
            return {"success": False, "error": "already_processed"}

        logger.info(
            f"Applying corp action: {action_id} {symbol} {action_type} "
            f"ex_date={ex_date} adj_factor={adj_factor:.6f}"
        )

        # ── Step 1: Adjust historical prices ──────────────────────────────

        # For splits/bonus, adjust OHLC backward from ex_date
        if action_type in ("split", "bonus"):
            affected = self._adjust_historical_prices(symbol, ex_date, adj_factor)
            logger.info(f"  Adjusted {affected} historical price rows")
        else:
            affected = 0

        # ── Step 2: Recalculate indicators ────────────────────────────────

        # Delete and recompute indicators for this symbol
        self.con.execute("DELETE FROM technical_indicators WHERE symbol = ?", [symbol])
        logger.info(f"  Deleted indicators for {symbol}, will be recomputed on next run")

        # ── Step 3: Adjust open portfolio positions ───────────────────────

        adjusted_positions = self._adjust_portfolio_positions(symbol, adj_factor, action_type)
        logger.info(f"  Adjusted {adjusted_positions} open positions")

        # ── Step 4: Adjust pending triggers ───────────────────────────────

        adjusted_triggers = self._adjust_pending_triggers(symbol, adj_factor)
        logger.info(f"  Adjusted {adjusted_triggers} pending triggers")

        # ── Step 5: Mark backtests stale ──────────────────────────────────

        # For now, we just log; could delete or flag
        logger.info(f"  Backtests for {symbol} are now stale and should be re-run")

        # ── Step 6: Mark as processed ─────────────────────────────────────

        self.con.execute("""
            UPDATE corporate_actions
            SET processed = 1
            WHERE action_id = ?
        """, [action_id])

        return {
            "success": True,
            "action_id": action_id,
            "symbol": symbol,
            "action_type": action_type,
            "affected_price_rows": affected,
            "adjusted_positions": adjusted_positions,
            "adjusted_triggers": adjusted_triggers,
        }

    def _adjust_historical_prices(self, symbol: str, ex_date: date, adj_factor: float) -> int:
        """
        Adjust all historical OHLC prices before ex_date.

        Formula:
          adjusted_price = raw_price / adj_factor

        Returns count of affected rows.
        """
        # Get all prices before ex_date
        df = self.con.execute("""
            SELECT date, open, high, low, close
            FROM daily_prices
            WHERE symbol = ? AND date < ?
            ORDER BY date
        """, [symbol, ex_date]).fetchdf()

        if df.empty:
            return 0

        # Apply adjustment
        df["open"]  = df["open"]  / adj_factor
        df["high"]  = df["high"]  / adj_factor
        df["low"]   = df["low"]   / adj_factor
        df["close"] = df["close"] / adj_factor

        # Update back to DB
        for _, row in df.iterrows():
            self.con.execute("""
                UPDATE daily_prices
                SET open = ?, high = ?, low = ?, close = ?
                WHERE symbol = ? AND date = ?
            """, [
                float(row["open"]), float(row["high"]),
                float(row["low"]),  float(row["close"]),
                symbol, row["date"]
            ])

        return len(df)

    def _adjust_portfolio_positions(self, symbol: str, adj_factor: float, action_type: str) -> int:
        """
        Adjust open positions for splits/bonus.

        For split/bonus:
          - qty *= adj_factor
          - avg_cost /= adj_factor
          - Economics preserved: qty * avg_cost unchanged
        """
        if action_type == "dividend":
            return 0  # No position adjustment for dividends

        with get_sqlite() as session:
            from alphalens.core.database import PortfolioHolding
            holdings = session.query(PortfolioHolding).filter(
                PortfolioHolding.symbol == symbol,
                PortfolioHolding.is_active == True,
            ).all()

            for h in holdings:
                old_qty  = h.qty
                old_cost = h.avg_cost

                h.qty      = int(h.qty * adj_factor)
                h.avg_cost = h.avg_cost / adj_factor

                # Adjust target and SL too
                if h.current_target:
                    h.current_target = h.current_target / adj_factor
                if h.current_stop_loss:
                    h.current_stop_loss = h.current_stop_loss / adj_factor

                h.updated_at = datetime.now()

                logger.debug(
                    f"  Position adjusted: {symbol} qty {old_qty}→{h.qty}, "
                    f"cost ₹{old_cost:.2f}→₹{h.avg_cost:.2f}"
                )

            session.commit()
            return len(holdings)

    def _adjust_pending_triggers(self, symbol: str, adj_factor: float) -> int:
        """Adjust pending trigger prices for splits/bonus."""
        with get_sqlite() as session:
            rows = session.execute("""
                UPDATE signal_triggers
                SET trigger_price = trigger_price / ?,
                    buy_below_price = buy_below_price / ?,
                    current_price = CASE WHEN current_price IS NOT NULL
                                    THEN current_price / ? ELSE NULL END,
                    updated_at = ?
                WHERE symbol = ? AND status IN ('pending', 'eligible')
            """, (adj_factor, adj_factor, adj_factor, datetime.now(), symbol))
            return rows.rowcount

    # ── Get Corporate Actions ─────────────────────────────────────────────

    def get_actions(
        self,
        symbol: str = None,
        processed: bool = None,
        from_date: date = None,
        to_date: date = None,
    ) -> list:
        """
        Query corporate actions.

        Args:
            symbol: Filter by symbol
            processed: True/False/None for all
            from_date, to_date: Filter by ex_date range

        Returns:
            List of action dicts
        """
        where = []
        params = []

        if symbol:
            where.append("symbol = ?")
            params.append(symbol)

        if processed is not None:
            where.append("processed = ?")
            params.append(1 if processed else 0)

        if from_date:
            where.append("ex_date >= ?")
            params.append(from_date)

        if to_date:
            where.append("ex_date <= ?")
            params.append(to_date)

        where_clause = " AND ".join(where) if where else "1=1"

        rows = self.con.execute(f"""
            SELECT action_id, symbol, action_type, ex_date, record_date,
                   ratio, cash_amount, adjustment_factor, source, processed, created_at
            FROM corporate_actions
            WHERE {where_clause}
            ORDER BY ex_date DESC, created_at DESC
        """, params).fetchall()

        return [
            {
                "action_id":         r[0],
                "symbol":            r[1],
                "action_type":       r[2],
                "ex_date":           r[3],
                "record_date":       r[4],
                "ratio":             r[5],
                "cash_amount":       r[6],
                "adjustment_factor": r[7],
                "source":            r[8],
                "processed":         bool(r[9]),
                "created_at":        r[10],
            }
            for r in rows
        ]

    # ── Get Affected Holdings/Triggers ────────────────────────────────────

    def get_impact_summary(self, action_id: str) -> dict:
        """
        Get impact summary for a corporate action.

        Returns:
            {
                "action": {...},
                "affected_positions": [...],
                "affected_triggers": [...],
                "price_row_count": int,
            }
        """
        action = self.con.execute("""
            SELECT symbol, action_type, ex_date, adjustment_factor
            FROM corporate_actions WHERE action_id = ?
        """, [action_id]).fetchone()

        if not action:
            return {"error": "action_not_found"}

        symbol, action_type, ex_date, adj_factor = action

        # Count affected price rows
        price_count = self.con.execute("""
            SELECT COUNT(*) FROM daily_prices
            WHERE symbol = ? AND date < ?
        """, [symbol, ex_date]).fetchone()[0]

        # Get affected positions
        with get_sqlite() as session:
            from alphalens.core.database import PortfolioHolding
            positions = session.query(PortfolioHolding).filter(
                PortfolioHolding.symbol == symbol,
                PortfolioHolding.is_active == True,
            ).all()

            pos_list = [
                {
                    "holding_id":  p.holding_id,
                    "qty":         p.qty,
                    "avg_cost":    p.avg_cost,
                    "new_qty":     int(p.qty * adj_factor),
                    "new_cost":    p.avg_cost / adj_factor,
                }
                for p in positions
            ]

        # Get affected triggers
        with get_sqlite() as session:
            triggers = session.execute("""
                SELECT trigger_id, trigger_price, buy_below_price
                FROM signal_triggers
                WHERE symbol = ? AND status IN ('pending', 'eligible')
            """, (symbol,)).fetchall()

            trig_list = [
                {
                    "trigger_id":         t[0],
                    "trigger_price":      t[1],
                    "buy_below_price":    t[2],
                    "new_trigger_price":  t[1] / adj_factor,
                    "new_buy_below":      t[2] / adj_factor,
                }
                for t in triggers
            ]

        return {
            "action": {
                "action_id":   action_id,
                "symbol":      symbol,
                "action_type": action_type,
                "ex_date":     ex_date,
                "adj_factor":  adj_factor,
            },
            "price_row_count":     price_count,
            "affected_positions":  pos_list,
            "affected_triggers":   trig_list,
        }
