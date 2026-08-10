"""
datastore/api/portfolio_nav.py

Phase: FeatureBacklog.md ML38 — momentum strategy consolidation (2026-08-09)
Owner: Platform / Backend
Consumers: datastore/api/routers/portfolios.py

NAV (Net Asset Value) computation for the generic Portfolio module
(datastore/schema/create_normalised.py's `portfolios`/`portfolio_cash_flows`
tables). A portfolio's NAV as of any date is:

    nav = cash_balance + holdings_value

cash_balance = external capital in/out (portfolio_cash_flows) net of what's
been spent on / recovered from trades tagged to this portfolio via
momentum_trades.portfolio_id (buys are cash out, sells are cash in).

holdings_value = mark-to-market value of every still-open (unsold as of
that date) tagged trade, using the last real close price on or before
that date — same "real price, never fabricated" convention every other
valuation helper in this codebase follows (see
systems/damodaran_valuation/valuation_engine.py::_load_current_price,
which this intentionally does not import from — this module stays
decoupled from the Damodaran valuation domain; the query itself is a
trivial 6-line duplicate, not worth a cross-domain dependency).

Only momentum_trades is wired in for Phase 0 (my_holdings also carries a
portfolio_id column per the same migration, but isn't queried here yet --
add a symmetric branch once a real cross-channel use case needs it).
"""

from typing import Any, Dict, List, Optional, Tuple

from backtest.momentum_metrics import xirr


def _load_last_close(conn: Any, ticker: str, as_of_date: str) -> Optional[float]:
    row = conn.execute(
        "SELECT close FROM ohlcv_adjusted WHERE ticker = ? AND date <= ? ORDER BY date DESC LIMIT 1",
        [ticker, as_of_date],
    ).fetchone()
    return float(row[0]) if row and row[0] is not None else None


def compute_nav(conn: Any, portfolio_id: int, as_of_date: str) -> Dict[str, Any]:
    """Returns {nav, cash_balance, holdings_value, total_contributed,
    total_withdrawn, xirr}. `xirr` is None if fewer than 2 real external
    cash flows exist yet (can't bracket a rate — see backtest.momentum_metrics.xirr).
    """
    cash_flow_rows = conn.execute(
        "SELECT date, amount FROM portfolio_cash_flows WHERE portfolio_id = ? AND date <= ? ORDER BY date",
        [portfolio_id, as_of_date],
    ).fetchall()
    external_cash_net = sum(amount for _, amount in cash_flow_rows)
    total_contributed = sum(amount for _, amount in cash_flow_rows if amount > 0)
    total_withdrawn = -sum(amount for _, amount in cash_flow_rows if amount < 0)

    trade_rows = conn.execute(
        """
        SELECT ticker, qty, purchase_price, purchase_date, sell_price, sale_date
        FROM momentum_trades
        WHERE portfolio_id = ? AND purchase_date <= ?
        """,
        [portfolio_id, as_of_date],
    ).fetchall()

    trade_cash_net = 0.0
    holdings_value = 0.0
    for ticker, qty, purchase_price, purchase_date, sell_price, sale_date in trade_rows:
        if qty is None or purchase_price is None:
            continue
        trade_cash_net -= qty * purchase_price  # buy: cash out
        sold_by_as_of = sale_date is not None and str(sale_date) <= as_of_date
        if sold_by_as_of and sell_price is not None:
            trade_cash_net += qty * sell_price  # sell: cash in
        else:
            # still open as of as_of_date: mark-to-market, never fabricated
            # -- a ticker with no real close on/before as_of_date (e.g.
            # delisted with no further data) contributes 0, not a guess.
            price = _load_last_close(conn, ticker, as_of_date)
            if price is not None:
                holdings_value += qty * price

    cash_balance = external_cash_net + trade_cash_net
    nav = cash_balance + holdings_value

    xirr_flows: List[Tuple[str, float]] = [(str(d), -amount) for d, amount in cash_flow_rows]
    xirr_flows.append((as_of_date, nav))
    rate = xirr(xirr_flows) if len(xirr_flows) >= 2 and total_contributed > 0 else None

    return {
        "nav": nav,
        "cash_balance": cash_balance,
        "holdings_value": holdings_value,
        "total_contributed": total_contributed,
        "total_withdrawn": total_withdrawn,
        "xirr": rate,
    }
