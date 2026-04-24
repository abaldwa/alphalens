"""
alphalens/core/cycle/context.py

In-memory singleton for the current market cycle state.
Updated once after each EOD classification run.
Dashboard reads from here — O(1) without hitting DuckDB.

Usage:
    ctx = get_cycle_context()
    ctx.market_cycle          # "bull" | "bear" | "neutral"
    ctx.market_confidence     # 0.82
    ctx.sector_cycles         # {"IT": "bull", "Financials": "neutral", ...}
    ctx.get_stock_cycle("RELIANCE")  # {"cycle": "bull", "confidence": 0.77}
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

from loguru import logger


@dataclass
class CycleContext:
    market_cycle:      str   = "neutral"
    market_confidence: float = 0.0
    sector_cycles:     dict  = field(default_factory=dict)
    stock_cycles:      dict  = field(default_factory=dict)
    classified_at:     Optional[datetime] = None
    classified_date:   Optional[date]     = None

    def get_stock_cycle(self, symbol: str) -> dict:
        return self.stock_cycles.get(symbol, {"cycle": "neutral", "confidence": 0.0})

    def get_sector_cycle(self, sector: str) -> dict:
        return self.sector_cycles.get(sector, {"cycle": "neutral", "confidence": 0.0})

    def get_signal_threshold(self, base_thresholds: dict) -> float:
        """Return signal confidence threshold based on market cycle."""
        return base_thresholds.get(self.market_cycle, base_thresholds.get("neutral", 0.75))

    def is_stale(self, max_age_hours: int = 25) -> bool:
        """Returns True if classification is older than max_age_hours."""
        if self.classified_at is None:
            return True
        age = (datetime.now() - self.classified_at).total_seconds() / 3600
        return age > max_age_hours

    def to_dict(self) -> dict:
        return {
            "market_cycle":      self.market_cycle,
            "market_confidence": self.market_confidence,
            "sector_cycles":     self.sector_cycles,
            "classified_at":     self.classified_at.isoformat() if self.classified_at else None,
            "classified_date":   str(self.classified_date) if self.classified_date else None,
        }

    def summary_line(self) -> str:
        """One-line summary for logging / Telegram."""
        bull_sectors = [s for s, v in self.sector_cycles.items() if v.get("cycle") == "bull"]
        bear_sectors = [s for s, v in self.sector_cycles.items() if v.get("cycle") == "bear"]
        return (
            f"Market: {self.market_cycle.upper()} ({self.market_confidence:.0%}) | "
            f"Bull sectors: {len(bull_sectors)} | Bear sectors: {len(bear_sectors)}"
        )


# ── Global singleton ───────────────────────────────────────────────────────

_cycle_context = CycleContext()


def get_cycle_context() -> CycleContext:
    """Return the global cycle context singleton."""
    return _cycle_context


def update_cycle_context(classifier_results: dict):
    """
    Update the singleton from CycleClassifier.classify_all_and_store() output.
    Called after each EOD classification run.
    """
    global _cycle_context

    market_result = classifier_results.get("market", {})
    sector_results = classifier_results.get("sectors", {})
    stock_results  = classifier_results.get("stocks", {})

    _cycle_context = CycleContext(
        market_cycle      = market_result.get("cycle", "neutral"),
        market_confidence = market_result.get("confidence", 0.0),
        sector_cycles     = {k: v for k, v in sector_results.items()},
        stock_cycles      = {k: v for k, v in stock_results.items()},
        classified_at     = datetime.now(),
        classified_date   = date.today(),
    )

    logger.info(f"Cycle context updated: {_cycle_context.summary_line()}")


def load_cycle_context_from_db():
    """
    Bootstrap cycle context from DB on startup
    (in case we restart mid-day after a classification run).
    """
    global _cycle_context
    from alphalens.core.database import get_duck

    con = get_duck()
    today = date.today()

    # Market
    row = con.execute("""
        SELECT cycle, confidence FROM market_cycles
        WHERE scope = 'market' AND scope_id IS NULL
        ORDER BY date DESC LIMIT 1
    """).fetchone()
    market_cycle = row[0] if row else "neutral"
    market_conf  = row[1] if row else 0.0

    # Sectors
    sectors = con.execute("""
        SELECT scope_id, cycle, confidence FROM market_cycles
        WHERE scope = 'sector' AND date = (
            SELECT MAX(date) FROM market_cycles WHERE scope = 'sector'
        )
    """).fetchall()
    sector_cycles = {r[0]: {"cycle": r[1], "confidence": r[2]} for r in sectors if r[0]}

    # Stocks (latest date)
    stocks = con.execute("""
        SELECT scope_id, cycle, confidence FROM market_cycles
        WHERE scope = 'stock' AND date = (
            SELECT MAX(date) FROM market_cycles WHERE scope = 'stock'
        )
    """).fetchall()
    stock_cycles = {r[0]: {"cycle": r[1], "confidence": r[2]} for r in stocks if r[0]}

    _cycle_context = CycleContext(
        market_cycle      = market_cycle,
        market_confidence = market_conf,
        sector_cycles     = sector_cycles,
        stock_cycles      = stock_cycles,
        classified_at     = datetime.now(),
        classified_date   = today,
    )

    logger.info(f"Cycle context loaded from DB: {_cycle_context.summary_line()}")
    logger.info(f"  Stocks loaded: {len(stock_cycles)}")
