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

from typing import Dict, List

STCG_RATE = 0.20
LTCG_RATE = 0.125
LTCG_HOLDING_DAYS = 365


def compute_transaction_tax(txn: Dict) -> float:
    """Tax owed (INR) on one transaction's gain, 0 if the trade lost money
    or has no sell_price (shouldn't happen — MomentumBacktester always
    marks open positions to the final date's price when one exists)."""
    if txn["sell_price"] is None:
        return 0.0
    gain = (txn["sell_price"] - txn["buy_price"]) * txn["qty"]
    if gain <= 0:
        return 0.0
    rate = LTCG_RATE if txn["holding_days"] >= LTCG_HOLDING_DAYS else STCG_RATE
    return gain * rate


def compute_total_tax(transactions: List[Dict]) -> float:
    return sum(compute_transaction_tax(t) for t in transactions)


def post_tax_ending_value(ending_value: float, transactions: List[Dict]) -> float:
    """Ending portfolio value net of capital-gains tax on every
    transaction's gain (see module docstring for the simplifications)."""
    return ending_value - compute_total_tax(transactions)
