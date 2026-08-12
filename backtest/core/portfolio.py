"""
backtest/core/portfolio.py

Phase: Unified Backtest & Paper Trading Umbrella, Phase 1
Owner: Platform / Backtest
Consumers: backtest/core/engine.py (once refactored), every channel
adapter, backtest/walk_forward, backtest/paper_trading

StrategyPortfolio: the one portfolio implementation every channel/mode
uses, replacing today's split between backtest/portfolio.py's
PortfolioSimulator (global MAX_POSITION_PCT/MAX_SECTOR_PCT, no SIP) and
backtest/momentum_backtest.py's bespoke in-memory cash-flow/SIP tracking
(no shared sizing gate, no ADTV cap).

What's new here vs. both predecessors:
  - Position/sector caps come from backtest.core.horizon's
    HorizonSizingPolicy (per horizon bucket), not a single global
    constant — this is the "standard way of doing... Position tracking"
    the user asked for.
  - SIP support (BacktestUmbrellaPlan.md Truthful Review: today only
    MomentumBacktester has it) — generalized from
    momentum_backtest.py's _monthly_injection_dates() pattern, so
    Technical/Fundamental/ML strategies get lump-sum-or-SIP for free.
  - A hard ADTV-based position-size cap (Truthful Review Gap #6): today
    adtv_cr only feeds cost/slippage in IndianTransactionCosts, never a
    rejection gate. A strategy can no longer "buy" more than a
    configurable fraction of a stock's trailing average daily traded
    value.
  - Every closed trade is exposed as a backtest.core.tax.Transaction via
    tax_transactions(), so core/tax.py's FY-netted engine can compute
    the strategy's tax cash flows without re-deriving buy/sell dates
    from Trade objects at every call site.
  - Every StrategyPortfolio instance is scoped to exactly one strategy's
    capital base — no pooled-capital concept, per the confirmed
    "each Strategy would run its own backtest... on a Strategy Capital
    base" decision (BacktestUmbrellaPlan.md Context).

Reuses backtest.portfolio.Position/Trade (dataclasses only — no logic
imported) and backtest.costs.IndianTransactionCosts unchanged.
"""

import logging
from dataclasses import dataclass
from datetime import date as date_type
from typing import Any, Dict, List, Optional

import pandas as pd

from backtest.core.horizon import HorizonBucket, HorizonSizingPolicy, sizing_for
from backtest.core.tax import Transaction as TaxTransaction
from backtest.costs import IndianTransactionCosts
from backtest.portfolio import Position, Trade
from config.settings import MIN_ADT_INR

logger = logging.getLogger(__name__)

DEFAULT_ADTV_CAP_FRACTION = 0.10  # a position may not exceed 10% of trailing ADTV — Truthful Review Gap #6


@dataclass
class SipConfig:
    amount: float
    cadence: str = "monthly"  # only "monthly" supported today — matches the user's stated example

    def __post_init__(self) -> None:
        if self.amount <= 0:
            raise ValueError("SIP amount must be positive")
        if self.cadence != "monthly":
            raise ValueError("only monthly SIP cadence is currently supported")


@dataclass
class AnnualResetConfig:
    """capital_mode="annual_reset" — the user's third performance measure
    (2026-08-12).

    Intent, in the user's words: "Suppose I start the year with 10,00,000 for
    every strategy. At the end of the year, I take all profits out after paying
    the taxes out and leave the capital in there. If there has been a loss in
    the Financial Year, I will pour the deficit into the strategy and start the
    year again with 10,00,000."

    This is a genuinely different simulation, not a re-scoring of the lump run.
    Position sizing is a function of equity (see `position_size`, capped by
    `self.cash`), and `can_buy` rejects when cash is short or integer-share
    rounding gives qty 0 — so a portfolio held near a constant Rs 10L base takes
    DIFFERENT trades from one that compounded to Rs 50L, not the same trades at
    different sizes.

    The awkward case, agreed with the user 2026-08-12, is the NORMAL case: a
    template holding 10-20 equal-weight positions is near-fully invested at
    31 March, so most of the year's gain is unrealised and cash is small. You
    then cannot withdraw down to Rs 10L without selling positions the strategy
    never signalled an exit for. The agreed rule keeps the strategy honest and
    lets the base drift instead:

        withdraw = min(realised_after_tax, cash, equity - base_capital)
        top_up   = base_capital - equity            (only when equity < base)

    so profits are taken out only to the extent they were actually BOOKED and
    are actually LIQUID, exactly as a real fully-invested investor would find.
    Losses are always topped back up to `base_capital`, which is always possible.

    Consequence that must be reported, never hidden: opening capital is NOT
    Rs 10L every year. It drifts above it in good years. Every FY's actual
    opening capital is recorded in `fy_ledger` and must be shown alongside the
    return, or the number reads as a fixed-base return and overstates the
    strategy.
    """

    base_capital: float = 1_000_000.0
    # LTCG regime the withdrawal's tax is computed under. Defaults match
    # backtest/core/tax.py's constants (the 12.5% / no-exemption engine
    # default); the sweep runs one job per regime.
    ltcg_rate: float = 0.125
    ltcg_exemption: float = 0.0
    regime_label: str = "engine_default"

    def __post_init__(self) -> None:
        if self.base_capital <= 0:
            raise ValueError("annual-reset base_capital must be positive")
        if not (0.0 <= self.ltcg_rate < 1.0):
            raise ValueError("ltcg_rate must be in [0, 1)")
        if self.ltcg_exemption < 0:
            raise ValueError("ltcg_exemption must be non-negative")


class StrategyPortfolio:
    def __init__(
        self,
        initial_capital: float,
        horizon_bucket: HorizonBucket,
        sizing_overrides: Optional[dict] = None,
        n_target_positions: int = 10,
        costs: Optional[IndianTransactionCosts] = None,
        sip: Optional[SipConfig] = None,
        annual_reset: Optional["AnnualResetConfig"] = None,
        adtv_cap_fraction: float = DEFAULT_ADTV_CAP_FRACTION,
    ) -> None:
        if initial_capital <= 0:
            raise ValueError("initial_capital must be positive")
        if n_target_positions <= 0:
            raise ValueError("n_target_positions must be positive")
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.horizon_bucket = horizon_bucket
        self.sizing: HorizonSizingPolicy = sizing_for(horizon_bucket, sizing_overrides)
        self.n_target_positions = n_target_positions
        self.costs = costs or IndianTransactionCosts()
        self.sip = sip
        self.adtv_cap_fraction = adtv_cap_fraction

        self.positions: Dict[str, Position] = {}
        self.trades: List[Trade] = []
        self._equity_curve: List[Dict] = []
        self._cash_position_series: List[Dict] = []
        self.cash_flows: List[Dict] = [{"date": None, "amount": -initial_capital}]  # date stamped by caller on first record
        self.total_contributed = initial_capital
        self._sip_injection_dates: Optional[List[pd.Timestamp]] = None
        self._sip_injection_idx = 0

        # ----- capital_mode="annual_reset" (2026-08-12) -----
        # All inert when annual_reset is None, which is the lump/sip default —
        # the existing paths must stay bit-identical.
        self.annual_reset = annual_reset
        self._annual_reset_dates: Optional[List[pd.Timestamp]] = None
        self._annual_reset_idx = 0
        self.fy_ledger: List[Dict] = []
        # Post-tax total actually taken out (the real cash flow), and the
        # gross-of-tax equivalent for comparison — see apply_due_annual_reset.
        self.total_withdrawn = 0.0
        self.total_withdrawn_pretax = 0.0
        # Opening capital of the FY currently in progress. Seeded with the
        # run's initial capital; each reset stamps the next year's value.
        self._current_fy_opening_capital = initial_capital

    # ===== SIP injection (generalized from momentum_backtest.py's _monthly_injection_dates) =====
    def _monthly_injection_dates(self, trading_days: pd.DatetimeIndex) -> List[pd.Timestamp]:
        """First trading day of every calendar month in the run, excluding the
        first month (whose contribution is initial_capital itself)."""
        seen_months = set()
        dates: List[pd.Timestamp] = []
        for d in trading_days:
            key = (d.year, d.month)
            if key not in seen_months:
                seen_months.add(key)
                dates.append(d)
        return dates[1:]

    def prime_sip_schedule(self, trading_days: pd.DatetimeIndex) -> None:
        """Call once at run start (after the trading-day calendar for the run
        is known) if self.sip is set. Also stamps the initial-capital cash
        flow with the run's actual first date, since __init__ doesn't know it."""
        if trading_days is not None and len(trading_days) > 0:
            self.cash_flows[0]["date"] = str(pd.Timestamp(trading_days[0]).date())
        if self.sip is not None:
            self._sip_injection_dates = self._monthly_injection_dates(trading_days)
            self._sip_injection_idx = 0

    # ===== capital_mode="annual_reset" (2026-08-12) =====
    def _fy_start_dates(self, trading_days: pd.DatetimeIndex) -> List[pd.Timestamp]:
        """First trading day of every Indian FY (1 Apr - 31 Mar) in the run,
        EXCLUDING the FY the run starts in — that year opens on
        `initial_capital` and needs no adjustment."""
        seen_fys = set()
        dates: List[pd.Timestamp] = []
        for d in trading_days:
            # FY label = the calendar year the FY started in.
            fy = d.year if d.month >= 4 else d.year - 1
            if fy not in seen_fys:
                seen_fys.add(fy)
                dates.append(d)
        return dates[1:]

    def prime_annual_reset_schedule(self, trading_days: pd.DatetimeIndex) -> None:
        """Call once at run start, alongside prime_sip_schedule. No-op unless
        capital_mode="annual_reset"."""
        if self.annual_reset is None:
            return
        if trading_days is None or len(trading_days) == 0:
            return
        self._annual_reset_dates = self._fy_start_dates(trading_days)
        self._annual_reset_idx = 0
        self._current_fy_opening_capital = self.annual_reset.base_capital

    def _realised_tax_transactions_for_fy(self, fy_end: date_type) -> List[TaxTransaction]:
        """Closed trades whose SELL landed in the FY closing on `fy_end`.
        Sell-date drives the FY, matching core/tax.group_by_financial_year."""
        from backtest.core.tax import financial_year_end

        return [t for t in self.tax_transactions() if financial_year_end(t.sell_date) == fy_end]

    def apply_due_annual_reset(self, as_of_date, prices: Dict[str, float]) -> None:
        """At each FY boundary: withdraw booked-and-liquid profit after tax, or
        top the base back up after a losing year. Positions are never touched —
        see AnnualResetConfig's docstring for why the base is allowed to drift
        upward instead of forcing liquidations.

        Needs `prices` (unlike the SIP equivalent) because the decision is made
        against mark-to-market equity, not cash alone.
        """
        if self.annual_reset is None or self._annual_reset_dates is None:
            return

        from backtest.core.tax import fy_net_tax_with_regime

        cfg = self.annual_reset
        as_of = pd.Timestamp(as_of_date)

        while (
            self._annual_reset_idx < len(self._annual_reset_dates)
            and self._annual_reset_dates[self._annual_reset_idx] <= as_of
        ):
            reset_date = self._annual_reset_dates[self._annual_reset_idx]
            self._annual_reset_idx += 1

            # The FY that just CLOSED.
            #
            # [BUG FIX 2026-08-12, caught by the pre-sweep smoke test] This used
            # to be financial_year_end(reset_date - 1 day). That breaks whenever
            # 1 April is a weekend/holiday: the first trading day is then 2 or
            # 3 April, so `reset_date - 1 day` still lands INSIDE the new FY and
            # financial_year_end returns the NEXT close. The 17-year ledger came
            # out with 2013-03-31 / 2019-03-31 / 2024-03-31 duplicated and
            # 2012 / 2017 / 2023 missing — and because the label drives
            # _realised_tax_transactions_for_fy, the mislabelled years pulled the
            # wrong realised-P&L bucket and withdrew the wrong amount.
            #
            # reset_date is by construction the first trading day of the NEW FY
            # (April or later), so the FY that just closed ends on 31 March of
            # that same calendar year. Derive it directly rather than by walking
            # back a day into calendar ambiguity.
            rd = reset_date.date()
            closed_fy_end = date_type(rd.year if rd.month >= 4 else rd.year - 1, 3, 31)

            equity_before = self.total_equity(prices)
            opening_capital = self._current_fy_opening_capital

            fy_txns = self._realised_tax_transactions_for_fy(closed_fy_end)
            realised = sum(t.gain for t in fy_txns)
            tax = fy_net_tax_with_regime(
                fy_txns, ltcg_rate=cfg.ltcg_rate, ltcg_exemption=cfg.ltcg_exemption,
            )
            realised_after_tax = realised - tax

            # [2026-08-12, user request] Report the withdrawal both gross and
            # net of tax. Only ONE of these can actually move cash — the
            # post-tax figure does, because that is what the investor really
            # gets to take out. `withdrawn_pretax` is the same calculation with
            # the tax line removed, reported so the tax drag on withdrawals is
            # visible per year. It is an informational figure within a post-tax
            # simulation, NOT a separate pre-tax run: had the pre-tax amount
            # actually been withdrawn, the following year would have started on
            # less capital and taken different trades.
            headroom = max(0.0, equity_before - cfg.base_capital)
            withdrawn_pretax = (
                max(0.0, min(realised, self.cash, headroom))
                if equity_before >= cfg.base_capital else 0.0
            )

            withdrawn = 0.0
            topped_up = 0.0
            if equity_before < cfg.base_capital:
                # Losing year: always fundable, so the base is genuinely restored.
                topped_up = cfg.base_capital - equity_before
                self.cash += topped_up
                self.total_contributed += topped_up
                self.cash_flows.append({"date": str(reset_date.date()), "amount": -topped_up})
            elif realised_after_tax > 0:
                # Take out only what was booked AND is liquid AND is genuinely
                # above the base. Any of the three can bind; usually `cash` does.
                withdrawn = min(realised_after_tax, self.cash, equity_before - cfg.base_capital)
                withdrawn = max(withdrawn, 0.0)
                if withdrawn > 0:
                    self.cash -= withdrawn
                    self.total_withdrawn += withdrawn
                    self.cash_flows.append({"date": str(reset_date.date()), "amount": withdrawn})
            self.total_withdrawn_pretax += withdrawn_pretax

            equity_after = self.total_equity(prices)

            self.fy_ledger.append({
                "fy_end": str(closed_fy_end),
                "opening_capital": round(opening_capital, 2),
                "closing_equity": round(equity_before, 2),
                "realised_pnl": round(realised, 2),
                "tax": round(tax, 2),
                "realised_after_tax": round(realised_after_tax, 2),
                # Gross vs net withdrawal (user request 2026-08-12). Only
                # `withdrawn` moved cash; `withdrawn_pretax` shows what would
                # have left before tax, so the annual tax drag is explicit.
                "withdrawn_pretax": round(withdrawn_pretax, 2),
                "withdrawn": round(withdrawn, 2),
                "withdrawal_tax_drag": round(withdrawn_pretax - withdrawn, 2),
                "topped_up": round(topped_up, 2),
                "opening_capital_next": round(equity_after, 2),
                # The honest return for the year: growth of the capital the year
                # actually STARTED with, before any boundary adjustment.
                "return_on_opening_pct": (
                    round(100.0 * (equity_before - opening_capital) / opening_capital, 4)
                    if opening_capital > 0 else None
                ),
                "regime": cfg.regime_label,
                # Makes the drift visible per row rather than only in aggregate.
                "base_capital": cfg.base_capital,
                "opened_above_base": bool(equity_after > cfg.base_capital + 1.0),
            })

            self._current_fy_opening_capital = equity_after

            if self.cash < -1e-6:
                raise AssertionError(
                    f"annual_reset produced negative cash ({self.cash:.2f}) at {reset_date.date()} — "
                    "withdrawal should be capped by available cash"
                )

    def apply_due_sip_injections(self, as_of_date) -> None:
        """Apply every SIP contribution due on or before as_of_date. Contributions
        between rebalances sit in cash, unused, until the next rebalance deploys
        them — same treatment as momentum_backtest.py's existing behavior."""
        if self.sip is None or self._sip_injection_dates is None:
            return
        as_of = pd.Timestamp(as_of_date)
        while (
            self._sip_injection_idx < len(self._sip_injection_dates)
            and self._sip_injection_dates[self._sip_injection_idx] <= as_of
        ):
            injection_date = self._sip_injection_dates[self._sip_injection_idx]
            self.cash += self.sip.amount
            self.total_contributed += self.sip.amount
            self.cash_flows.append({"date": str(injection_date.date()), "amount": -self.sip.amount})
            self._sip_injection_idx += 1

    # ===== Position sizing (horizon-bucket-driven, per HorizonSizingPolicy) =====
    def position_size(
        self, price: float, portfolio_value: float, adtv_cr: Optional[float] = None,
        weight_multiplier: float = 1.0,
    ) -> int:
        """
        Share quantity for a new position, capped by:
          - sizing.max_position_pct of portfolio_value (horizon-bucket-driven, not global)
          - available cash
          - adtv_cap_fraction of trailing ADTV, if adtv_cr is supplied (Truthful
            Review Gap #6 — a hard rejection cap, not just a cost-model input)

        weight_multiplier scales ONLY the equal-weight slot (2026-08-05,
        Momentum volume-weighted sizing — mirrors MomentumBacktester's
        `investable_per_slot * volume_weights[ticker]`), never the
        max_position_pct / cash / ADTV caps, which stay hard ceilings a
        strategy's weighting scheme cannot talk its way past. 1.0 (the
        default, used by every other channel) is exactly today's behavior.
        """
        if price <= 0:
            raise ValueError("price must be positive")
        if portfolio_value < 0:
            raise ValueError("portfolio_value must be non-negative")
        if weight_multiplier <= 0:
            raise ValueError("weight_multiplier must be positive")

        max_position_inr = portfolio_value * self.sizing.max_position_pct
        equal_weight_inr = (portfolio_value / self.n_target_positions) * weight_multiplier
        target_inr = min(equal_weight_inr, max_position_inr, self.cash)

        if adtv_cr is not None and adtv_cr > 0:
            adtv_inr = adtv_cr * 1e7  # crores -> INR
            target_inr = min(target_inr, adtv_inr * self.adtv_cap_fraction)

        return max(int(target_inr / price), 0)

    def sector_exposure_pct(self, sector: str, prices: Dict[str, float]) -> float:
        equity = self.total_equity(prices)
        if equity <= 0:
            return 0.0
        sector_value = sum(
            pos.quantity * prices.get(t, pos.entry_price) for t, pos in self.positions.items() if pos.sector == sector
        )
        return sector_value / equity

    def can_buy(
        self, ticker: str, sector: str, price: float, prices: Dict[str, float], adtv_cr: Optional[float] = None,
        weight_multiplier: float = 1.0,
    ) -> bool:
        if ticker in self.positions:
            return False
        # [BUG FIX, 6th fundamental-strategies review, item 3] MIN_ADT_INR
        # was previously only used as a post-hoc audit floor
        # (integrity_checker.py::check_06_liquidity) and as a SIZE cap here
        # (adtv_cap_fraction, position_size above) — a ticker below the
        # floor could still be bought at a smaller size rather than being
        # excluded outright, so garp/turnaround (and any other
        # adapter/orchestrator-driven strategy sharing this StrategyPortfolio)
        # structurally traded sub-floor-liquidity names. A real ADTV value
        # (not None — missing data is the separate, already-tracked
        # "no_adtv_data_position_sized_uncapped" case) below MIN_ADT_INR now
        # hard-rejects the trade entirely, mirroring backtest/engine.py's
        # existing `_apply_entries` hard-reject for the technical channel
        # (`illiquid = adtv_at_entry.index[... adtv_at_entry*1e7 < MIN_ADT_INR]`).
        if adtv_cr is not None and adtv_cr * 1e7 < MIN_ADT_INR:
            return False
        equity = self.total_equity(prices)
        qty = self.position_size(price, equity, adtv_cr, weight_multiplier)
        if qty <= 0:
            return False
        turnover = price * qty
        if turnover > self.cash:
            return False
        sector_value = sum(
            pos.quantity * prices.get(t, pos.entry_price) for t, pos in self.positions.items() if pos.sector == sector
        )
        if equity > 0 and (sector_value + turnover) / equity > self.sizing.max_sector_pct:
            return False
        return True

    def buy(
        self, ticker: str, sector: str, price: float, date, prices: Dict[str, float],
        adtv_cr: Optional[float] = None, entry_atr_pct: Optional[float] = None,
        template: Optional[str] = None, pillar: Optional[str] = None,
        market_cap_rank: Optional[int] = None,
        entry_feature_vector: Optional[Dict[str, Any]] = None,
        weight_multiplier: float = 1.0,
    ) -> Optional[Position]:
        if not self.can_buy(ticker, sector, price, prices, adtv_cr, weight_multiplier):
            return None
        equity = self.total_equity(prices)
        qty = self.position_size(price, equity, adtv_cr, weight_multiplier)
        turnover = price * qty
        self.cash -= turnover
        position = Position(
            ticker=ticker, sector=sector, entry_date=date, entry_price=price, quantity=qty,
            entry_atr_pct=entry_atr_pct, template=template, pillar=pillar,
            entry_market_cap_rank=market_cap_rank, entry_adtv_cr=adtv_cr,
            entry_feature_vector=entry_feature_vector,
        )
        self.positions[ticker] = position
        return position

    def sell(
        self, ticker: str, price: float, date, reason: str = "signal", adtv_cr: Optional[float] = None,
    ) -> Optional[Trade]:
        """Enforces the horizon bucket's min_holding_days floor (Truthful Review
        applies here too) unless reason indicates a forced exit (delisting/merger
        close-out, or an exit-model urgent signal) — a non-urgent sell before the
        floor is a no-op, matching the Standard Backtesting Algorithm's intent
        that horizon buckets set real holding-period discipline, not just a label."""
        position = self.positions.get(ticker)
        if position is None:
            return None
        if reason not in ("forced_close", "exit_model_urgent"):
            holding_days = (pd.Timestamp(date) - pd.Timestamp(position.entry_date)).days
            if holding_days < self.sizing.min_holding_days:
                return None
        self.positions.pop(ticker, None)
        return self._close(position, position.quantity, price, date, reason, adtv_cr)

    def force_close(self, ticker: str, price: float, date, reason: str = "forced_close") -> Optional[Trade]:
        """Delisting/merger reconciliation hook (Truthful Review Gap #4) — always
        allowed regardless of min_holding_days, since the position is disappearing
        whether the strategy wants to exit or not."""
        position = self.positions.pop(ticker, None)
        if position is None:
            return None
        return self._close(position, position.quantity, price, date, reason, None)

    def _close(self, position: Position, qty: int, price: float, date, reason: str, adtv_cr, partial: bool = False) -> Trade:
        cost = self.costs.compute_roundtrip_cost(position.entry_price, qty, adtv_cr)
        proceeds = price * qty - cost
        entry_basis = position.entry_price * qty
        pnl_inr = proceeds - entry_basis
        pnl_pct = pnl_inr / entry_basis if entry_basis else 0.0
        self.cash += proceeds
        if partial:
            position.quantity -= qty
        trade = Trade(
            ticker=position.ticker, entry_date=position.entry_date, exit_date=date,
            entry_price=position.entry_price, exit_price=price, quantity=qty,
            pnl_inr=pnl_inr, pnl_pct=pnl_pct, cost_inr=cost, exit_reason=reason,
            entry_market_cap_rank=position.entry_market_cap_rank,
            adtv_cr=position.entry_adtv_cr,
            entry_feature_vector=position.entry_feature_vector,
        )
        self.trades.append(trade)
        return trade

    def total_equity(self, prices: Dict[str, float]) -> float:
        positions_value = sum(pos.quantity * prices.get(t, pos.entry_price) for t, pos in self.positions.items())
        return self.cash + positions_value

    def record_equity(self, date, prices: Dict[str, float]) -> None:
        self._equity_curve.append({"date": date, "equity": self.total_equity(prices)})
        self._cash_position_series.append({"date": date, "cash": self.cash})

    @property
    def equity_curve(self) -> pd.Series:
        df = pd.DataFrame(self._equity_curve)
        if df.empty:
            return pd.Series(dtype=float)
        return pd.Series(df["equity"].values, index=pd.to_datetime(df["date"]))

    @property
    def cash_position_series(self) -> List[Dict]:
        return self._cash_position_series

    @property
    def trades_df(self) -> pd.DataFrame:
        return pd.DataFrame([t.__dict__ for t in self.trades])

    def tax_transactions(self) -> List[TaxTransaction]:
        """Every closed trade as a backtest.core.tax.Transaction, for
        core/tax.py's FY-netted tax engine. Only realized (closed) trades
        produce a taxable event — open positions at run-end are excluded
        here (core/tax.py's module docstring: no mark-to-market tax accrual)."""
        out = []
        for t in self.trades:
            buy_date = t.entry_date if isinstance(t.entry_date, date_type) else pd.Timestamp(t.entry_date).date()
            sell_date = t.exit_date if isinstance(t.exit_date, date_type) else pd.Timestamp(t.exit_date).date()
            out.append(TaxTransaction(
                ticker=t.ticker, buy_date=buy_date, sell_date=sell_date,
                buy_price=t.entry_price, sell_price=t.exit_price, quantity=t.quantity,
            ))
        return out
