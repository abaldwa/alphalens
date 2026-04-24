"""
alphalens/core/signals/trigger_manager.py

Trigger-price intelligence system.

Problem: Strategy fires → crowd buys → price spikes → mean reverts.
Solution: 2-step signal model.

Step 1: Signal fires → write to trigger_candidates (NOT immediate buy)
Step 2: Daily checker → when price hits buy-below threshold → eligible
Step 3: User validates → strategy still valid? → Confirm buy

Database: signal_triggers table (SQLite)
Workflow:
  - SignalGenerator writes to signal_triggers with status='pending'
  - TriggerChecker runs every 6 hours, updates status to 'eligible' when price met
  - UI shows all eligible candidates
  - User clicks "Validate" → re-evaluates strategy conditions
  - User confirms → creates portfolio position
"""

import json
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
from loguru import logger

from alphalens.core.database import get_duck, get_sqlite, get_config
from alphalens.core.strategy.backtester import Backtester
from alphalens.core.strategy.library import get_strategy


# Default buy-below thresholds (configurable)
DEFAULT_THRESHOLDS = {
    "intraday":  0.010,  # 1.0%
    "swing":     0.015,  # 1.5%
    "medium":    0.030,  # 3.0%
    "long_term": 0.050,  # 5.0%
}

# Expiry periods (trading days)
DEFAULT_EXPIRY = {
    "intraday":  5,
    "swing":     10,
    "medium":    30,
    "long_term": 90,
}


class TriggerManager:
    """Manage trigger-price candidates and eligibility checking."""

    def __init__(self):
        self.con = get_duck()
        self.bt  = Backtester()
        self._ensure_table()

    def _ensure_table(self):
        """Create signal_triggers table if not exists."""
        with get_sqlite() as session:
            session.execute("""
                CREATE TABLE IF NOT EXISTS signal_triggers (
                    trigger_id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol             TEXT NOT NULL,
                    strategy_id        TEXT NOT NULL,
                    timeframe          TEXT NOT NULL,
                    trigger_date       DATE NOT NULL,
                    trigger_price      REAL NOT NULL,
                    buy_below_pct      REAL NOT NULL,
                    buy_below_price    REAL NOT NULL,
                    current_price      REAL,
                    distance_pct       REAL,
                    strategy_snapshot  TEXT,
                    market_regime      TEXT,
                    expiry_date        DATE,
                    status             TEXT DEFAULT 'pending',
                    validation_state   TEXT,
                    manual_override    INTEGER DEFAULT 0,
                    notes              TEXT,
                    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            session.execute("CREATE INDEX IF NOT EXISTS idx_triggers_status ON signal_triggers(status)")
            session.execute("CREATE INDEX IF NOT EXISTS idx_triggers_symbol_tf ON signal_triggers(symbol, timeframe)")

    # ── Create Trigger Candidate ──────────────────────────────────────────

    def create_trigger(
        self,
        symbol: str,
        strategy_id: str,
        timeframe: str,
        trigger_price: float,
        strategy_snapshot: dict,
        market_regime: str = "neutral",
    ) -> int:
        """
        Create a new trigger candidate.

        Args:
            symbol: Stock symbol
            strategy_id: Strategy ID that fired
            timeframe: intraday/swing/medium/long_term
            trigger_price: Price at which strategy fired
            strategy_snapshot: Full JSON of all indicator values at trigger
            market_regime: bull/bear/neutral

        Returns:
            trigger_id
        """
        # Get buy-below threshold
        threshold = get_config(f"trigger_discount_{timeframe}", DEFAULT_THRESHOLDS.get(timeframe, 0.03))
        buy_below_price = trigger_price * (1 - threshold)

        # Get expiry period
        expiry_days = get_config(f"trigger_expiry_{timeframe}", DEFAULT_EXPIRY.get(timeframe, 30))
        expiry_date = date.today() + timedelta(days=expiry_days)

        with get_sqlite() as session:
            result = session.execute("""
                INSERT INTO signal_triggers (
                    symbol, strategy_id, timeframe, trigger_date, trigger_price,
                    buy_below_pct, buy_below_price, current_price, market_regime,
                    expiry_date, strategy_snapshot, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
            """, (
                symbol, strategy_id, timeframe, date.today(), trigger_price,
                threshold, buy_below_price, trigger_price, market_regime,
                expiry_date, json.dumps(strategy_snapshot), datetime.now(), datetime.now()
            ))
            trigger_id = result.lastrowid

        logger.info(
            f"Created trigger: {symbol} [{timeframe}] @ ₹{trigger_price:.2f}, "
            f"buy-below @ ₹{buy_below_price:.2f} ({threshold*100:.1f}% discount), "
            f"expires {expiry_date}"
        )
        return trigger_id

    # ── Check Eligibility (Daily Job) ─────────────────────────────────────

    def check_all_pending(self) -> dict:
        """
        Check all pending triggers against current prices.
        Updates status to 'eligible' when price <= buy_below_price.
        Marks as 'expired' when past expiry_date.

        Returns summary dict.
        """
        with get_sqlite() as session:
            pending = session.execute("""
                SELECT trigger_id, symbol, timeframe, buy_below_price, expiry_date
                FROM signal_triggers
                WHERE status = 'pending'
            """).fetchall()

        if not pending:
            return {"pending": 0, "eligible": 0, "expired": 0}

        symbols = list(set(row[1] for row in pending))
        prices  = self._get_current_prices(symbols)

        eligible_count = 0
        expired_count  = 0

        for trigger_id, symbol, timeframe, buy_below, expiry in pending:
            current = prices.get(symbol)
            if current is None:
                continue

            distance_pct = (current - buy_below) / buy_below * 100 if buy_below > 0 else 0

            # Check expiry
            if expiry and date.today() > expiry:
                with get_sqlite() as session:
                    session.execute("""
                        UPDATE signal_triggers
                        SET status = 'expired', updated_at = ?
                        WHERE trigger_id = ?
                    """, (datetime.now(), trigger_id))
                expired_count += 1
                continue

            # Check if price met buy-below threshold
            if current <= buy_below:
                with get_sqlite() as session:
                    session.execute("""
                        UPDATE signal_triggers
                        SET status = 'eligible',
                            current_price = ?,
                            distance_pct = ?,
                            updated_at = ?
                        WHERE trigger_id = ?
                    """, (current, distance_pct, datetime.now(), trigger_id))
                eligible_count += 1
                logger.info(
                    f"Trigger #{trigger_id} {symbol} [{timeframe}] → ELIGIBLE "
                    f"(current ₹{current:.2f} <= buy-below ₹{buy_below:.2f})"
                )
            else:
                # Update current price for monitoring
                with get_sqlite() as session:
                    session.execute("""
                        UPDATE signal_triggers
                        SET current_price = ?,
                            distance_pct = ?,
                            updated_at = ?
                        WHERE trigger_id = ?
                    """, (current, distance_pct, datetime.now(), trigger_id))

        logger.info(
            f"Trigger check complete: {len(pending)} pending, "
            f"{eligible_count} newly eligible, {expired_count} expired"
        )

        return {
            "pending":  len(pending) - eligible_count - expired_count,
            "eligible": eligible_count,
            "expired":  expired_count,
        }

    # ── Validate Strategy Conditions ──────────────────────────────────────

    def validate_trigger(self, trigger_id: int) -> dict:
        """
        Re-validate strategy conditions using latest data.

        Returns:
            {
                "valid": bool,
                "trigger_id": int,
                "symbol": str,
                "strategy_id": str,
                "rule_checks": [{rule, passed, reason}, ...],
                "capital_fit": bool,
                "sector_fit": bool,
                "position_size": {...},
            }
        """
        with get_sqlite() as session:
            row = session.execute("""
                SELECT symbol, strategy_id, timeframe, trigger_price, buy_below_price,
                       current_price, strategy_snapshot, market_regime
                FROM signal_triggers
                WHERE trigger_id = ?
            """, (trigger_id,)).fetchone()

        if not row:
            return {"valid": False, "error": "trigger_not_found"}

        symbol, strategy_id, timeframe, trigger_price, buy_below_price, current_price, snapshot_json, regime = row

        strategy = get_strategy(strategy_id)
        if not strategy:
            return {"valid": False, "error": "strategy_not_found"}

        # Load latest data
        latest_inds = self._load_latest_indicators(symbol)
        if not latest_inds:
            return {"valid": False, "error": "no_data"}

        latest_prices = self._load_recent_prices(symbol, 60)
        if latest_prices is None or len(latest_prices) < 10:
            return {"valid": False, "error": "insufficient_price_data"}

        row_series = self._build_signal_row(latest_inds, latest_prices, symbol)
        prev_rows  = latest_prices

        # Re-evaluate entry conditions
        rule_checks = self._evaluate_all_entry_conditions(strategy, row_series, prev_rows)
        all_passed  = all(r["passed"] for r in rule_checks)

        # Check capital fit
        from alphalens.core.capital.allocator import CapitalAllocator
        allocator = CapitalAllocator()
        position_size = allocator.calculate_position_size(
            strategy_id, timeframe, current_price or buy_below_price, confidence=1.0
        )
        capital_fit = position_size.get("qty", 0) > 0

        # Check sector exposure
        sector_check = allocator.check_sector_exposure(symbol, position_size.get("value_inr", 0))
        sector_fit   = sector_check.get("allowed", True)

        # Store validation state
        validation = {
            "timestamp": datetime.now().isoformat(),
            "all_passed": all_passed,
            "rule_checks": rule_checks,
            "capital_fit": capital_fit,
            "sector_fit": sector_fit,
        }

        with get_sqlite() as session:
            session.execute("""
                UPDATE signal_triggers
                SET validation_state = ?, updated_at = ?
                WHERE trigger_id = ?
            """, (json.dumps(validation), datetime.now(), trigger_id))

        return {
            "valid":         all_passed and capital_fit and sector_fit,
            "trigger_id":    trigger_id,
            "symbol":        symbol,
            "strategy_id":   strategy_id,
            "timeframe":     timeframe,
            "trigger_price": trigger_price,
            "buy_below_price": buy_below_price,
            "current_price": current_price,
            "rule_checks":   rule_checks,
            "capital_fit":   capital_fit,
            "sector_fit":    sector_fit,
            "sector_check":  sector_check,
            "position_size": position_size,
        }

    def _evaluate_all_entry_conditions(self, strategy: dict, row: pd.Series, prev_rows: pd.DataFrame) -> list:
        """Evaluate each entry condition individually and return pass/fail per rule."""
        entry_rules = strategy.get("entry_rules", {})
        if isinstance(entry_rules, str):
            try:
                entry_rules = json.loads(entry_rules)
            except Exception:
                return []

        conditions = entry_rules.get("conditions", [])
        results = []

        for i, cond in enumerate(conditions):
            try:
                passed = self.bt._evaluate_condition(cond, row, prev_rows)
                results.append({
                    "rule_num": i + 1,
                    "indicator": cond.get("indicator"),
                    "operator": cond.get("op"),
                    "value": cond.get("value"),
                    "passed": bool(passed),
                    "reason": self._describe_condition(cond, row, passed),
                })
            except Exception as e:
                results.append({
                    "rule_num": i + 1,
                    "indicator": cond.get("indicator"),
                    "passed": False,
                    "reason": f"Error: {e}",
                })

        return results

    def _describe_condition(self, cond: dict, row: pd.Series, passed: bool) -> str:
        """Generate human-readable description of condition result."""
        ind  = cond.get("indicator", "")
        op   = cond.get("op", "")
        val  = cond.get("value")
        
        actual = row.get(ind)
        if actual is None or (isinstance(actual, float) and pd.isna(actual)):
            return f"{ind} missing"

        if isinstance(val, str) and val.startswith("indicator:"):
            comp_ind = val.replace("indicator:", "")
            comp_val = row.get(comp_ind)
            return f"{ind}={actual:.2f} {op} {comp_ind}={comp_val:.2f} → {'✓' if passed else '✗'}"
        else:
            return f"{ind}={actual:.2f} {op} {val} → {'✓' if passed else '✗'}"

    # ── Get Triggers for UI ───────────────────────────────────────────────

    def get_triggers(
        self,
        status: str = "all",
        timeframe: str = None,
        limit: int = 100,
    ) -> list:
        """
        Get trigger candidates for display.

        Args:
            status: "all" | "pending" | "eligible" | "bought" | "expired" | "invalidated"
            timeframe: Filter by timeframe
            limit: Max results

        Returns:
            List of trigger dicts
        """
        where = []
        params = []

        if status != "all":
            where.append("status = ?")
            params.append(status)

        if timeframe:
            where.append("timeframe = ?")
            params.append(timeframe)

        where_clause = " AND ".join(where) if where else "1=1"

        with get_sqlite() as session:
            rows = session.execute(f"""
                SELECT trigger_id, symbol, strategy_id, timeframe, trigger_date,
                       trigger_price, buy_below_price, current_price, distance_pct,
                       status, expiry_date, validation_state, notes
                FROM signal_triggers
                WHERE {where_clause}
                ORDER BY trigger_date DESC, trigger_id DESC
                LIMIT ?
            """, params + [limit]).fetchall()

        results = []
        for r in rows:
            days_old = (date.today() - r[4]).days if r[4] else 0
            val_state = json.loads(r[11]) if r[11] else {}
            
            results.append({
                "trigger_id":      r[0],
                "symbol":          r[1],
                "strategy_id":     r[2],
                "timeframe":       r[3],
                "trigger_date":    r[4],
                "trigger_price":   r[5],
                "buy_below_price": r[6],
                "current_price":   r[7],
                "distance_pct":    r[8],
                "status":          r[9],
                "expiry_date":     r[10],
                "validation_state": val_state,
                "notes":           r[12],
                "days_old":        days_old,
            })

        return results

    # ── Confirm Buy ───────────────────────────────────────────────────────

    def confirm_buy(self, trigger_id: int, override_price: float = None, reason: str = None) -> dict:
        """
        User confirms buy after validation.
        Creates portfolio position and marks trigger as 'bought'.

        Args:
            trigger_id: Trigger to execute
            override_price: Optional manual entry price (if not using current)
            reason: Optional reason for override

        Returns:
            {
                "success": bool,
                "holding_id": int,
                "position": {...},
            }
        """
        # Get trigger details
        with get_sqlite() as session:
            row = session.execute("""
                SELECT symbol, strategy_id, timeframe, buy_below_price, current_price
                FROM signal_triggers
                WHERE trigger_id = ? AND status = 'eligible'
            """, (trigger_id,)).fetchone()

        if not row:
            return {"success": False, "error": "trigger_not_found_or_not_eligible"}

        symbol, strategy_id, timeframe, buy_below, current = row
        entry_price = override_price if override_price else (current if current else buy_below)

        # Calculate position size
        from alphalens.core.capital.allocator import CapitalAllocator
        allocator = CapitalAllocator()
        pos_size  = allocator.calculate_position_size(strategy_id, timeframe, entry_price, confidence=1.0)

        if pos_size.get("qty", 0) < 1:
            return {"success": False, "error": "insufficient_capital_for_position"}

        # Compute target and SL
        strategy  = get_strategy(strategy_id)
        latest    = self._load_latest_indicators(symbol)
        row_data  = pd.Series({**latest, "close": entry_price})
        stop_loss = self.bt.compute_stop_loss(strategy, row_data, entry_price)
        target    = self.bt.compute_target(strategy, row_data, entry_price, stop_loss)

        # Create portfolio position
        from alphalens.core.portfolio.manager import PortfolioManager
        pm = PortfolioManager()
        holding_id = pm.open_position(
            symbol      = symbol,
            timeframe   = timeframe,
            qty         = pos_size["qty"],
            avg_cost    = entry_price,
            target      = target,
            stop_loss   = stop_loss,
            strategy_id = strategy_id,
            notes       = f"Trigger #{trigger_id}" + (f" | {reason}" if reason else ""),
        )

        # Mark trigger as bought
        with get_sqlite() as session:
            session.execute("""
                UPDATE signal_triggers
                SET status = 'bought',
                    manual_override = ?,
                    notes = ?,
                    updated_at = ?
                WHERE trigger_id = ?
            """, (1 if override_price else 0, reason or "", datetime.now(), trigger_id))

        logger.info(
            f"Trigger #{trigger_id} executed: {symbol} [{timeframe}] "
            f"{pos_size['qty']} shares @ ₹{entry_price:.2f} → Holding #{holding_id}"
        )

        return {
            "success":    True,
            "holding_id": holding_id,
            "trigger_id": trigger_id,
            "position":   pos_size,
            "target":     target,
            "stop_loss":  stop_loss,
        }

    # ── Cancel / Invalidate ───────────────────────────────────────────────

    def cancel_trigger(self, trigger_id: int, reason: str = "manual"):
        """Cancel a pending/eligible trigger."""
        with get_sqlite() as session:
            session.execute("""
                UPDATE signal_triggers
                SET status = 'cancelled', notes = ?, updated_at = ?
                WHERE trigger_id = ? AND status IN ('pending', 'eligible')
            """, (reason, datetime.now(), trigger_id))
        logger.info(f"Trigger #{trigger_id} cancelled: {reason}")

    def invalidate_trigger(self, trigger_id: int):
        """Mark trigger as invalidated (strategy no longer valid)."""
        with get_sqlite() as session:
            session.execute("""
                UPDATE signal_triggers
                SET status = 'invalidated', updated_at = ?
                WHERE trigger_id = ?
            """, (datetime.now(), trigger_id))
        logger.info(f"Trigger #{trigger_id} invalidated")

    # ── Helpers ────────────────────────────────────────────────────────────

    def _get_current_prices(self, symbols: list) -> dict:
        if not symbols:
            return {}
        ph  = ", ".join(["?"] * len(symbols))
        rows = self.con.execute(f"""
            SELECT symbol, close FROM daily_prices
            WHERE (symbol, date) IN (
                SELECT symbol, MAX(date) FROM daily_prices WHERE symbol IN ({ph}) GROUP BY symbol
            )
        """, symbols).fetchall()
        return {r[0]: float(r[1]) for r in rows}

    def _load_latest_indicators(self, symbol: str) -> dict:
        row = self.con.execute("""
            SELECT * FROM technical_indicators
            WHERE symbol = ? ORDER BY date DESC LIMIT 1
        """, [symbol]).fetchdf()
        if row.empty:
            return {}
        return row.iloc[0].to_dict()

    def _load_recent_prices(self, symbol: str, n: int) -> Optional[pd.DataFrame]:
        df = self.con.execute("""
            SELECT date, open, high, low, close, volume
            FROM daily_prices WHERE symbol = ?
            ORDER BY date DESC LIMIT ?
        """, [symbol, n]).fetchdf()
        if df.empty:
            return None
        df["date"] = pd.to_datetime(df["date"]).dt.date
        return df.sort_values("date").reset_index(drop=True)

    def _build_signal_row(self, inds: dict, prices: pd.DataFrame, symbol: str) -> pd.Series:
        from alphalens.core.cycle.context import get_cycle_context
        ctx = get_cycle_context()
        latest_price = prices.iloc[-1].to_dict() if not prices.empty else {}
        row_data = {**inds, **latest_price}
        row_data["prev_close"] = float(prices.iloc[-2]["close"]) if len(prices) > 1 else row_data.get("close", 0)
        row_data["market_cycle"] = ctx.market_cycle
        return pd.Series(row_data)
