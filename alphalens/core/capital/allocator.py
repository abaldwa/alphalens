"""
alphalens/core/capital/allocator.py

Capital allocation engine with configurable strategy/timeframe ratios.

Workflow:
  1. User configures total capital + reserve %
  2. User sets strategy allocation ratios (what % to each strategy)
  3. User sets timeframe allocation ratios (intraday/swing/med/long)
  4. When signal fires, allocator computes exact share qty

Features:
  - Multiple allocation modes: equal-weight, ratio-based, volatility-scaled
  - Sector exposure limits
  - Per-stock capital cap
  - Slippage + brokerage + fees model
  - Real-time what-if calculator
"""

import json
from datetime import datetime
from typing import Optional

import numpy as np
from loguru import logger

from alphalens.core.database import get_duck, get_sqlite, get_config, set_config


class CapitalAllocator:
    """Smart capital allocation with strategy ratios + position sizing."""

    def __init__(self):
        self.con = get_duck()
        self._load_config()

    def _load_config(self):
        """Load all capital config from database."""
        self.total_capital    = get_config("total_capital", 2_500_000)
        self.reserve_pct      = get_config("reserve_cash_pct", 0.10)
        self.max_per_stock    = get_config("max_capital_per_stock", 200_000)
        self.max_sector_pct   = get_config("max_sector_exposure_pct", 0.25)
        self.slippage_pct     = get_config("slippage_pct", 0.001)
        self.brokerage_flat   = get_config("brokerage_flat_inr", 20.0)
        
        # Load strategy ratios (JSON dict)
        ratios_json = get_config("strategy_allocation_ratios", None)
        if ratios_json:
            try:
                self.strategy_ratios = json.loads(ratios_json) if isinstance(ratios_json, str) else ratios_json
            except Exception:
                self.strategy_ratios = {}
        else:
            self.strategy_ratios = {}

        # Load timeframe ratios
        tf_json = get_config("timeframe_allocation_ratios", None)
        if tf_json:
            try:
                self.timeframe_ratios = json.loads(tf_json) if isinstance(tf_json, str) else tf_json
            except Exception:
                self.timeframe_ratios = {"intraday": 0.10, "swing": 0.20, "medium": 0.30, "long_term": 0.40}
        else:
            self.timeframe_ratios = {"intraday": 0.10, "swing": 0.20, "medium": 0.30, "long_term": 0.40}

    def reload(self):
        """Reload config from DB (call after settings page save)."""
        self._load_config()

    # ── Position Sizing ────────────────────────────────────────────────────

    def calculate_position_size(
        self,
        strategy_id: str,
        timeframe: str,
        entry_price: float,
        confidence: float = 1.0,
        mode: str = "ratio",
    ) -> dict:
        """
        Calculate exact share quantity for a position.

        Args:
            strategy_id: Strategy identifier
            timeframe: intraday/swing/medium/long_term
            entry_price: Entry price per share
            confidence: Signal confidence (0-1), used to scale position
            mode: "ratio" | "equal_weight" | "volatility_scaled"

        Returns:
            {
                "qty": int,
                "value_inr": float,
                "capital_used_pct": float,
                "capital_available": float,
                "fees_total": float,
                "slippage_inr": float,
                "effective_entry": float,
            }
        """
        if entry_price <= 0:
            return {"qty": 0, "value_inr": 0, "error": "invalid_price"}

        # Step 1: Determine strategy allocation
        if mode == "ratio" and self.strategy_ratios:
            strat_ratio = self.strategy_ratios.get(strategy_id, 0)
            if strat_ratio == 0:
                # Fallback to timeframe ratio
                strat_capital = self.total_capital * self.timeframe_ratios.get(timeframe, 0.20)
            else:
                strat_capital = self.total_capital * strat_ratio
        else:
            # Equal weight or fallback
            strat_capital = self.total_capital * self.timeframe_ratios.get(timeframe, 0.20)

        # Step 2: Apply reserve cash
        usable_capital = strat_capital * (1 - self.reserve_pct)

        # Step 3: Cap per position
        available_capital = self._get_available_capital(strategy_id, timeframe)
        per_position_cap  = min(usable_capital, self.max_per_stock, available_capital)

        # Step 4: Apply confidence scaling
        position_value = per_position_cap * confidence

        # Step 5: Compute fees
        slippage_inr = position_value * self.slippage_pct
        brokerage    = self.brokerage_flat
        fees_total   = slippage_inr + brokerage

        # Step 6: Effective capital for shares
        net_capital = position_value - fees_total
        if net_capital <= 0:
            return {"qty": 0, "value_inr": 0, "error": "insufficient_after_fees"}

        # Step 7: Calculate shares (floor to whole shares)
        effective_entry = entry_price * (1 + self.slippage_pct)
        qty = int(net_capital / effective_entry)

        if qty < 1:
            return {"qty": 0, "value_inr": 0, "error": "insufficient_for_1_share"}

        actual_value = qty * effective_entry

        return {
            "qty":               qty,
            "value_inr":         round(actual_value, 2),
            "capital_used_pct":  round(actual_value / self.total_capital * 100, 2),
            "capital_available": round(available_capital, 2),
            "fees_total":        round(fees_total, 2),
            "slippage_inr":      round(slippage_inr, 2),
            "brokerage_inr":     brokerage,
            "effective_entry":   round(effective_entry, 2),
            "strategy_capital":  round(strat_capital, 2),
            "mode":              mode,
        }

    def _get_available_capital(self, strategy_id: str, timeframe: str) -> float:
        """Get remaining capital for this strategy after existing positions."""
        with get_sqlite() as session:
            from alphalens.core.database import PortfolioHolding
            # Sum all active holdings for this strategy
            holdings = session.query(PortfolioHolding).filter(
                PortfolioHolding.strategy_id == strategy_id,
                PortfolioHolding.is_active == True,
            ).all()

            deployed = sum(h.qty * h.avg_cost for h in holdings)

        strat_ratio   = self.strategy_ratios.get(strategy_id, self.timeframe_ratios.get(timeframe, 0.20))
        strat_capital = self.total_capital * strat_ratio
        return max(0, strat_capital - deployed)

    # ── Sector Exposure Check ──────────────────────────────────────────────

    def check_sector_exposure(self, symbol: str, proposed_value: float) -> dict:
        """
        Check if adding this position would breach sector exposure limit.

        Returns:
            {
                "allowed": bool,
                "sector": str,
                "current_exposure_pct": float,
                "new_exposure_pct": float,
                "limit_pct": float,
            }
        """
        sector = self._get_symbol_sector(symbol)
        if not sector:
            return {"allowed": True, "sector": "Unknown"}

        # Get current sector exposure
        with get_sqlite() as session:
            from alphalens.core.database import PortfolioHolding
            holdings = session.query(PortfolioHolding).filter(
                PortfolioHolding.is_active == True
            ).all()

        sector_value = 0
        for h in holdings:
            h_sector = self._get_symbol_sector(h.symbol)
            if h_sector == sector:
                sector_value += h.qty * h.avg_cost

        current_pct = sector_value / self.total_capital if self.total_capital > 0 else 0
        new_pct     = (sector_value + proposed_value) / self.total_capital if self.total_capital > 0 else 0

        allowed = new_pct <= self.max_sector_pct

        return {
            "allowed":             allowed,
            "sector":              sector,
            "current_exposure_pct": round(current_pct * 100, 2),
            "new_exposure_pct":    round(new_pct * 100, 2),
            "limit_pct":           self.max_sector_pct * 100,
        }

    def _get_symbol_sector(self, symbol: str) -> Optional[str]:
        """Get sector for a symbol."""
        row = self.con.execute(
            "SELECT sector FROM nifty200_stocks WHERE symbol = ?", [symbol]
        ).fetchone()
        return row[0] if row else None

    # ── Capital Summary ────────────────────────────────────────────────────

    def get_capital_summary(self) -> dict:
        """
        Return full capital allocation summary.

        Shows:
          - Total capital
          - Reserve cash
          - Deployed capital (by strategy, by timeframe, by sector)
          - Available capital
          - Unutilized capital
        """
        with get_sqlite() as session:
            from alphalens.core.database import PortfolioHolding
            holdings = session.query(PortfolioHolding).filter(
                PortfolioHolding.is_active == True
            ).all()

        # Deployed capital by strategy
        by_strategy = {}
        by_timeframe = {"intraday": 0, "swing": 0, "medium": 0, "long_term": 0}
        by_sector = {}

        for h in holdings:
            value = h.qty * h.avg_cost
            
            # By strategy
            sid = h.strategy_id or "unknown"
            by_strategy[sid] = by_strategy.get(sid, 0) + value

            # By timeframe
            tf = h.timeframe or "swing"
            by_timeframe[tf] = by_timeframe.get(tf, 0) + value

            # By sector
            sector = self._get_symbol_sector(h.symbol) or "Unknown"
            by_sector[sector] = by_sector.get(sector, 0) + value

        total_deployed = sum(by_strategy.values())
        cash_available = self.total_capital - total_deployed
        reserve_cash   = self.total_capital * self.reserve_pct

        return {
            "total_capital":      self.total_capital,
            "reserve_cash":       reserve_cash,
            "deployed_capital":   total_deployed,
            "cash_available":     cash_available,
            "utilization_pct":    (total_deployed / self.total_capital * 100) if self.total_capital > 0 else 0,
            "by_strategy":        by_strategy,
            "by_timeframe":       by_timeframe,
            "by_sector":          {k: round(v, 2) for k, v in by_sector.items()},
            "sector_exposure_pct": {k: round(v / self.total_capital * 100, 2) for k, v in by_sector.items()},
        }

    # ── Configuration Methods ──────────────────────────────────────────────

    def set_strategy_ratios(self, ratios: dict):
        """
        Set strategy allocation ratios.

        Args:
            ratios: {"S001": 0.10, "S002": 0.15, ...}
                    Must sum to <= 1.0
        """
        total = sum(ratios.values())
        if total > 1.0:
            raise ValueError(f"Strategy ratios sum to {total:.2f}, must be <= 1.0")
        
        set_config("strategy_allocation_ratios", json.dumps(ratios))
        self.strategy_ratios = ratios
        logger.info(f"Updated strategy allocation ratios: {len(ratios)} strategies")

    def set_timeframe_ratios(self, ratios: dict):
        """
        Set timeframe allocation ratios.

        Args:
            ratios: {"intraday": 0.10, "swing": 0.20, "medium": 0.30, "long_term": 0.40}
                    Must sum to 1.0
        """
        total = sum(ratios.values())
        if abs(total - 1.0) > 0.01:
            raise ValueError(f"Timeframe ratios sum to {total:.2f}, must equal 1.0")
        
        set_config("timeframe_allocation_ratios", json.dumps(ratios))
        self.timeframe_ratios = ratios
        logger.info(f"Updated timeframe allocation ratios")

    def set_total_capital(self, amount: float):
        """Update total capital."""
        if amount <= 0:
            raise ValueError("Total capital must be > 0")
        set_config("total_capital", amount)
        self.total_capital = amount
        logger.info(f"Updated total capital: ₹{amount:,.0f}")
