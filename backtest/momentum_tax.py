"""
backtest/momentum_tax.py

Phase: FeatureBacklog.md ML38 — momentum strategy implementation
Owner: Platform / Backtest
Consumers: scripts/run_momentum_experimentation.py

Indian equity capital-gains tax on ML38's momentum backtest transaction
ledger (MomentumBacktester's `transactions`, both realized/closed trades
and still-open ones — MomentumBacktester already marks open positions to
the final backtest date's price, so both kinds carry a real (or
mark-to-market) sell_price + holding_days).

2026-07-14 user-specified rates (post the FY2024-25 capital-gains regime):
  - STCG (short-term, held < 365 days): 20% of the gain.
  - LTCG (long-term, held >= 365 days): 12.5% of the gain.

Simplifications, stated explicitly rather than silently assumed:
  - Losses are not tax-credited/offset against gains elsewhere in the
    portfolio (real Indian tax law allows capital-loss set-off/carry-
    forward) — this computes gross tax on winning trades only, which is a
    conservative (higher) estimate of tax owed, not a full tax-optimal
    simulation.
  - The real LTCG regime has a ₹1.25 lakh/year exemption on long-term
    gains; not modeled here (every rupee of LTCG gain is taxed at 12.5%)
    — again conservative, since real tax owed would be somewhat lower.
  - Still-open positions at the end of the backtest are treated as if
    sold on the final date at the mark-to-market price already recorded
    on that transaction, for the purpose of reporting a single post-tax
    number — real unrealized gains are not actually taxed until sold, so
    this is a reporting convenience, not a claim those shares were
    actually liquidated.
"""

from typing import Any, Dict, List

# [2026-08-18] The rates and the LTCG threshold are the TAX REGIME, and a
# regime can only be declared once. They lived here AND in core/tax.py as two
# identical literals, so a rate change applied to one would silently leave the
# other taxing at the old regime -- and features/momentum_strategy.py, which
# runs on the LIVE momentum path, imported them from here rather than from the
# engine that every other channel taxes through. core/tax.py is now the single
# declaration; these re-exports keep this module's existing importers working
# while it is retired with MomentumBacktester (ML40-2.2/2.3).
from backtest.core.tax import LTCG_HOLDING_DAYS, LTCG_RATE, STCG_RATE  # noqa: F401


def compute_transaction_tax(txn: Dict[str, Any]) -> float:
    """Tax owed (INR) on one transaction's gain, 0 if the trade lost money
    or has no sell_price (shouldn't happen — MomentumBacktester always
    marks open positions to the final date's price when one exists)."""
    if txn["sell_price"] is None:
        return 0.0
    gain = (txn["sell_price"] - txn["buy_price"]) * txn["qty"]
    if gain <= 0:
        return 0.0
    rate = LTCG_RATE if txn["holding_days"] >= LTCG_HOLDING_DAYS else STCG_RATE
    tax: float = gain * rate
    return tax


def compute_total_tax(transactions: List[Dict[str, Any]]) -> float:
    return sum(compute_transaction_tax(t) for t in transactions)


def post_tax_ending_value(ending_value: float, transactions: List[Dict[str, Any]]) -> float:
    """Ending portfolio value net of capital-gains tax on every
    transaction's gain (see module docstring for the simplifications)."""
    return ending_value - compute_total_tax(transactions)
