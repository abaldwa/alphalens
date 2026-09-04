"""
Trading Calendar Helpers — generic date-arithmetic utilities, factored out
of strategy files (explicit user instruction, 2026-09-04: "the purpose of
Strategy is to generate trades and nothing more... everything should be
pure-computed or available for the strategy"). Resolving "N trading
sessions before this date" is infrastructure any strategy could need, not
strategy-specific business logic — it was previously embedded directly as
a raw SQL query inside strategies/r03_jt_skipmonth.py.
"""

from typing import Any


def offset_trading_date(conn: Any, as_of_date: str, skip_days: int) -> str:
    """
    Resolve the trading date `skip_days` sessions before as_of_date (the
    Jegadeesh-Titman "skip-month" rule's date arithmetic — R03 ranks as of
    this offset date, not as_of_date itself, to avoid short-term reversal
    contaminating the momentum signal).
    """
    row = conn.execute(
        """
        SELECT date FROM (
            SELECT DISTINCT date FROM ohlcv_adjusted
            WHERE date <= ?
            ORDER BY date DESC
            LIMIT 1 OFFSET ?
        )
        """,
        [as_of_date, skip_days],
    ).fetchone()
    if row is None:
        raise ValueError(f"Not enough trading history before {as_of_date} to skip {skip_days} days")
    return str(row[0])
