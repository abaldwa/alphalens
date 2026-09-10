"""
Portfolio — minimal position/cash tracker for native orchestrator
execution.

Deliberately simple relative to backtest/core/portfolio.py (no tax
lots): this is the smallest correct simulator that can (a) execute a
strategy's target basket at a day's close price and (b) mark the book to
market on every trading day so R08/R09's update_portfolio_equity() has
real numbers to work with.

TRANSACTION COSTS wired in 2026-09-06 (explicit user instruction: "we
need to include Transaction Costs. We also calculate Pre-Tax and
Post-Tax Returns"). Reuses backtest/costs.py::IndianTransactionCosts
UNCHANGED — same rate table, same round-trip-at-entry-turnover
simplification the legacy engine already uses and this framework's
trade-by-trade parity effort is checked against (see
backtest/core/portfolio.py::_close(), which this mirrors exactly: the
FULL round-trip cost is deducted from SELL proceeds, computed off the
ENTRY price/quantity/ADTV, not split across both legs). Real cash outflow
— this changes CAGR/Sharpe/MaxDD/equity curve directly, same as it would
in live trading.

TAX is a separate, POST-PROCESSING overlay — NOT deducted from cash
during simulation (contrast with costs, above). Reuses
backtest/core/tax.py's canonical FY-netted LTCG/STCG engine unchanged.
Reported as parallel post_tax_* fields alongside the existing pre-tax
metrics (BacktestResult.metrics), not a replacement — CAGR/Sharpe/MaxDD/
equity curve stay pre-tax bookkeeping, exactly as before this change.
Computed once from the completed trade_log's realized round-trips at the
END of run_native() (see orchestrator.py), which is what "layered on"
meant in this docstring before 2026-09-06 — see
momentum_framework/common/tax_overlay.py for the FY-netting call.

CORRECTNESS FIX 2026-09-04: the original version of this file executed
signals literally — a repeated "buy" for an already-held ticker was
treated as a fresh purchase funded from whatever cash happened to be
idle, and a ticker missing from a rebalance's signal list was never
sold (no strategy but R07 emits explicit sell signals). Concretely: R01
on Nifty 50, 2023, monthly rebalance, reported a 30.5% CAGR that turned
out to be "buy 5 stocks in January, do almost nothing for the rest of
the year" — because month 2 onward had near-zero idle cash to deploy,
so the portfolio was never actually rotating. Caught because the user
found the number implausible and asked; verified by inspecting real
rebalance() output (Feb signals correctly dropped 3 January names and
added 3 new ones — the STRATEGY was always correct, only the Portfolio's
execution semantics were wrong).

FIX: rebalance_to_target() replaces execute(). The set of tickers
carrying a "buy" action in one rebalance call IS the complete desired
portfolio composition for that period — this matches the legacy
adapter's own stated model ("this period's top_n is List 2, what we
hold is List 1 — anything held and no longer in List 2 is sold; anything
in List 2 and not held is bought"), applied generically instead of only
inside R07. Anything currently held but NOT in this period's target is
sold; anything in target but not held is bought at an equal-weight (x
size_multiplier) share of total portfolio value; anything in both is
left untouched (no wash trade) — matching R07's original comment that
"already-held survivors keep whatever size they entered at."
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import logging

from backtest.costs import IndianTransactionCosts
from momentum_framework.backtesting.adapter import Signal

logger = logging.getLogger(__name__)


@dataclass
class Position:
    ticker: str
    shares: float
    entry_price: float
    entry_date: str
    #: ADTV (Rs crore) at entry, used to select the slippage rate at exit
    #: (IndianTransactionCosts._slippage_pct) — None when `conn` wasn't
    #: passed to rebalance_to_target() (falls back to the non-small-cap
    #: default rate, same as the legacy engine's own Optional[adtv_cr]).
    entry_adtv_cr: Optional[float] = None


class Portfolio:
    """Cash + positions, marked to market daily. No leverage: a
    size_multiplier > 1.0 is honored as intended relative exposure
    within the buy basket, but total spend is always capped at
    available cash — orders are scaled down proportionally, never
    over-filled on margin."""

    def __init__(self, initial_capital: float, costs: Optional[IndianTransactionCosts] = None):
        self.cash = initial_capital
        self.initial_capital = initial_capital
        self.positions: Dict[str, Position] = {}
        self.trade_log: List[Dict[str, Any]] = []
        # Real Indian-equity round-trip cost model (STT, exchange, SEBI,
        # stamp, GST, ADTV-scaled slippage) — see module docstring. Default
        # brokerage_pct=0.0 (zero-brokerage discount broker), same default
        # backtest/core/portfolio.py uses.
        self.costs = costs or IndianTransactionCosts()

    def market_value(self, prices: Dict[str, float]) -> float:
        """Total portfolio value (cash + mark-to-market positions) at
        today's close. A held ticker missing from `prices` (delisted,
        no data that day) keeps its LAST KNOWN value rather than being
        dropped to zero — a real data gap, not a real loss."""
        value = self.cash
        for ticker, pos in self.positions.items():
            price = prices.get(ticker, pos.entry_price)
            value += pos.shares * price
        return value

    def rebalance_to_target(
        self, signals: List[Signal], prices: Dict[str, float], as_of_date: str, conn: Any = None,
    ) -> None:
        """
        `signals`' "buy"-action tickers define the COMPLETE target
        portfolio for this period (see module docstring). Any explicit
        "sell"/"forced_close" signals are honored too (e.g. a strategy
        forcing an exit outside the normal target diff), but are
        redundant with — never required for — a ticker's absence from
        the buy set.

        `conn` (added 2026-09-06, optional, default None): a DuckDB
        connection used ONLY to look up each new position's ADTV at entry
        (common/liquidity.py::compute_adtv_cr), stored on Position and
        consulted at exit to pick the small-cap vs default slippage rate
        (see IndianTransactionCosts._slippage_pct). Omitting it (None) is
        fully backward compatible — every existing caller/test that
        doesn't pass conn gets entry_adtv_cr=None, which
        IndianTransactionCosts already treats as "use the default
        (non-small-cap) slippage rate," never an error.
        """
        target: Dict[str, float] = {
            s.ticker: (s.size_multiplier or 1.0) for s in signals if s.action == "buy"
        }
        # PORTFOLIO-LEVEL scalar (R08/R09 — see Signal's docstring for why
        # this must be separate from size_multiplier, which gets normalized
        # across the buy set below and would silently cancel out a uniform
        # value). Every buy signal in one rebalance carries the SAME
        # exposure_multiplier by construction (it's a book-wide scalar, not
        # a per-ticker one) — any single buy signal's value represents it.
        exposure_multipliers = [s.exposure_multiplier for s in signals if s.action == "buy"]
        exposure = exposure_multipliers[0] if exposure_multipliers else 1.0
        explicit_sells = {s.ticker for s in signals if s.action in ("sell", "forced_close")}

        held = set(self.positions.keys())
        to_sell = (held - target.keys()) | (explicit_sells & held)
        to_buy = target.keys() - held
        # target ∩ held, minus anything explicitly force-sold: left untouched.

        # Circuit-lock exclusion, sell side (2026-09-06, user instruction:
        # "skip the sell, keep holding until it unlocks"). A locked ticker
        # was never actually sellable at today's close — high==low, no
        # intraday range, no fill possible — so executing `_sell()` on one
        # anyway (the prior behavior) was a phantom fill Portfolio has no
        # business recording. Skipped ONLY this rebalance: `to_sell` is
        # recomputed fresh from `held`/`target` every call, so a still-
        # locked position is naturally reconsidered (and sold once it
        # unlocks) at the next rebalance, not permanently stuck. Applies
        # uniformly to organic rotation-out AND explicit sell/forced_close
        # signals — no strategy currently emits forced_close for a reason
        # that should override a genuine circuit lock.
        if conn is not None and to_sell:
            from momentum_framework.common.liquidity import get_circuit_locked_tickers_auto
            locked_held = get_circuit_locked_tickers_auto(conn, list(to_sell), as_of_date)
            if locked_held:
                to_sell = to_sell - locked_held

        for ticker in to_sell:
            self._sell(ticker, prices, as_of_date)

        if not to_buy:
            return

        total_value = self.market_value(prices)
        # Normalize across the FULL target basket (held + to_buy), not just
        # to_buy — held positions are left untouched (no wash trade, see
        # module docstring) but still occupy a share of total_weight. Bug
        # fixed 2026-09-06 (Category B1): normalizing over to_buy alone made
        # a single rotating-in ticker's weight collapse to 1.0 regardless of
        # its actual computed weight, silently defeating R09/R14/R16's
        # position sizing whenever only one ticker rotated per rebalance —
        # verified bit-for-bit identical to equal-weight R01 in that case.
        total_weight = sum(target.values())
        if total_weight <= 0:
            return

        requested = {t: total_value * (target[t] / total_weight) * exposure for t in to_buy}
        total_requested = sum(requested.values())
        scale = 1.0
        if total_requested > self.cash and total_requested > 0:
            scale = self.cash / total_requested

        # Batched ONE query for every ticker bought this rebalance, not one
        # per ticker — same reasoning as every other per-rebalance DB call
        # in this codebase (see common/signals.py's cache docstring).
        adtv_by_ticker: Dict[str, float] = {}
        if conn is not None and to_buy:
            from momentum_framework.common.liquidity import compute_adtv_cr_auto
            adtv_by_ticker = compute_adtv_cr_auto(conn, list(to_buy), as_of_date).to_dict()

        for ticker in to_buy:
            price = prices.get(ticker)
            if price is None or price <= 0:
                continue  # no price data today — skip, reconsidered next rebalance
            alloc = requested[ticker] * scale
            shares = alloc / price
            if shares <= 0:
                continue
            self.cash -= shares * price
            self.positions[ticker] = Position(
                ticker=ticker, shares=shares, entry_price=price, entry_date=as_of_date,
                entry_adtv_cr=adtv_by_ticker.get(ticker),
            )
            self.trade_log.append({
                "date": as_of_date, "ticker": ticker, "action": "buy",
                "shares": shares, "price": price, "value": shares * price,
            })

    def _sell(self, ticker: str, prices: Dict[str, float], as_of_date: str) -> None:
        pos = self.positions.pop(ticker, None)
        if pos is None:
            return
        price = prices.get(ticker, pos.entry_price)
        gross_proceeds = pos.shares * price
        # FULL round-trip cost (both legs) deducted here at exit, computed
        # off ENTRY price/quantity/ADTV — mirrors backtest/core/portfolio.py
        # ::_close() exactly (see module docstring). entry_price/shares are
        # always > 0 by construction (enforced at buy time above), so this
        # never hits compute_roundtrip_cost()'s price/quantity ValueError.
        cost = self.costs.compute_roundtrip_cost(pos.entry_price, pos.shares, pos.entry_adtv_cr)
        proceeds = gross_proceeds - cost
        entry_basis = pos.shares * pos.entry_price
        self.cash += proceeds
        self.trade_log.append({
            "date": as_of_date, "ticker": ticker, "action": "sell",
            "shares": pos.shares, "price": price, "value": proceeds,
            "pnl": proceeds - entry_basis,
            "cost_inr": cost,
            # Fields needed to build a tax.py::Transaction post-hoc (see
            # common/tax_overlay.py) without re-deriving them from a flat
            # buy/sell trade_log scan — this Position already has them.
            "buy_date": pos.entry_date, "buy_price": pos.entry_price,
            "sell_date": as_of_date, "sell_price": price,
        })
