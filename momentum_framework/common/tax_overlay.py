"""
Post-Tax Returns Overlay — FY-netted LTCG/STCG tax computed as a POST-
PROCESSING step on a completed backtest's realized trades, NOT deducted
from cash during simulation (contrast with transaction costs, which ARE
real cash outflows deducted in Portfolio._sell() — see
backtesting/portfolio.py's module docstring for why the two are split).

Reuses backtest/core/tax.py's canonical FY-netted engine UNCHANGED (LTCG
12.5%, STCG 20%, holding >= 365 days = LTCG, asymmetric loss set-off
rules — the same engine the legacy system uses), applied here to
momentum_framework's own trade_log shape.

Explicit user instruction 2026-09-06: "We also calculate Pre-Tax and
Post-Tax Returns" — the fields this module produces are ADDITIONAL,
reported alongside the existing pre-tax metrics (BacktestResult.metrics),
never replacing them. CAGR/Sharpe/MaxDD/equity curve stay pre-tax
bookkeeping, exactly as before this change.
"""

from datetime import date as date_type
from typing import Any, Dict, List

import pandas as pd

from backtest.core.tax import Transaction, post_tax_ending_value, total_tax


def build_transactions_from_trade_log(trade_log: List[Dict[str, Any]]) -> List[Transaction]:
    """
    One Transaction per realized "sell" entry in `trade_log` — Portfolio.
    _sell() already records buy_date/buy_price/sell_date/sell_price on
    every sell entry for exactly this purpose (2026-09-06). "buy" entries
    and any still-open position at the end of the run produce no
    Transaction — matches tax.py's own "only realized (closed)
    transactions are taxable" convention (an open position is not
    mark-to-market taxed until actually sold).
    """
    transactions = []
    for entry in trade_log:
        if entry.get("action") != "sell":
            continue
        transactions.append(Transaction(
            ticker=entry["ticker"],
            buy_date=_to_date(entry["buy_date"]),
            sell_date=_to_date(entry["sell_date"]),
            buy_price=entry["buy_price"],
            sell_price=entry["sell_price"],
            quantity=entry["shares"],
        ))
    return transactions


def _to_date(value: Any) -> date_type:
    if isinstance(value, date_type):
        return value
    result: date_type = pd.Timestamp(value).date()
    return result


def compute_post_tax_metrics(
    trade_log: List[Dict[str, Any]],
    initial_capital: float,
    pre_tax_ending_value: float,
    years: float,
) -> Dict[str, Any]:
    """
    post_tax_* fields to merge into a backtest's metrics dict:
    - post_tax_total_tax_inr: total FY-netted tax paid across the whole run
    - post_tax_ending_value: pre_tax_ending_value net of that tax
    - post_tax_cagr: CAGR using post_tax_ending_value over the SAME
      `initial_capital`/`years` the pre-tax CAGR used (MetricsCalculator.
      _cagr's formula, replicated here rather than imported to avoid a
      circular import between metrics/ and this module) — so the two
      numbers are a fair apples-to-apples comparison, only the ending
      value differs.

    No trades (empty/never-realized run) -> zero tax, post-tax == pre-tax.
    """
    transactions = build_transactions_from_trade_log(trade_log)
    if not transactions:
        return {
            "post_tax_total_tax_inr": 0.0,
            "post_tax_ending_value": pre_tax_ending_value,
            "post_tax_cagr": 0.0,
        }

    tax_paid = total_tax(transactions)
    ending_value = post_tax_ending_value(pre_tax_ending_value, transactions)
    cagr = 0.0
    if years > 0 and initial_capital > 0 and ending_value > 0:
        cagr = (ending_value / initial_capital) ** (1 / years) - 1

    return {
        "post_tax_total_tax_inr": tax_paid,
        "post_tax_ending_value": ending_value,
        "post_tax_cagr": float(cagr),
    }
