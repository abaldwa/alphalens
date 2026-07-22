"""
backtest/core/engine.py

Phase: Unified Backtest & Paper Trading Umbrella, Phase 1
Owner: Platform / Backtest
Consumers: backtest/adapters/*.py (technical, fundamental, momentum —
Phase 2; ml_adapter.py wraps backtest/engine.py instead, see below),
backtest/walk_forward/day_driver.py (Phase 2.5), backtest/paper_trading
(Phase 5)

The shared orchestrator every channel's adapter plugs into, implementing
the Standard Backtesting Algorithm (BacktestUmbrellaPlan.md): point-in-
time universe construction, rebalance-date iteration, signal generation,
corporate-action/delisting reconciliation, horizon-bucket-driven position
sizing, SIP cash-flow injection, cost-aware execution, and standardized
metrics — the same loop regardless of channel.

Built NET-NEW rather than extracted from backtest/engine.py's
BacktestEngine (confirmed 2026-07-20, "backtest/engine.py: wrap, don't
refactor" — see BacktestUmbrellaPlan.md): that module is left completely
untouched. adapters/ml_adapter.py (Phase 2) wraps BacktestEngine as a
StrategyAdapter-conforming black box instead of this orchestrator driving
its internals directly.

No-Mock-Data Policy: this module never fabricates a price or feature
value. When price_lookup returns None for a ticker/date, that
ticker/date is EXCLUDED from the run and recorded in the returned
BacktestRunResult.data_gaps — never interpolated or defaulted.
"""

import logging
from dataclasses import dataclass, field
from datetime import date as date_type
from typing import Any, Callable, Dict, List, Literal, Optional, Protocol

import pandas as pd

from backtest.core.horizon import HorizonBucket, sizing_for
from backtest.core.metrics import compute_metrics
from backtest.core.portfolio import SipConfig, StrategyPortfolio
from backtest.portfolio import Position
from backtest.core.regime_breakdown import compute_regime_breakdown
from backtest.core.run_context import BacktestRun, BacktestRunResult
from backtest.core.tax import fy_tax_cash_flows

logger = logging.getLogger(__name__)

SignalAction = str  # "buy" | "sell" | "forced_close" | "hold"


@dataclass(frozen=True)
class Signal:
    ticker: str
    action: SignalAction
    sector: str = "Unknown"
    conviction: float = 0.0  # higher = prioritized first when capital is constrained
    adtv_cr: Optional[float] = None


class StrategyAdapter(Protocol):
    """The contract every channel adapter implements (BacktestUmbrellaPlan.md
    Architecture section). ml_adapter.py implements this by delegating to the
    existing, unmodified backtest/engine.py::BacktestEngine internally."""

    channel: str

    def generate_signals(self, universe: List[str], as_of_date: date_type, horizon_bucket: HorizonBucket) -> List[Signal]:
        ...

    def feature_vector(self, ticker: str, as_of_date: date_type) -> Dict[str, Any]:
        ...


PriceLookup = Callable[[str, date_type], Optional[float]]
UniverseProvider = Callable[[date_type], List[str]]
SectorLookup = Callable[[str], str]
IsDelistedCheck = Callable[[str, date_type], bool]


@dataclass(frozen=True)
class CorporateActionEvent:
    """One MERGER/SPINOFF event affecting a held ticker as of a given date
    (BacktestUmbrellaPlan.md Truthful Review Gap #4 fix, 2026-07-20).

    successor_ticker/swap_ratio are a documented EXTENSION POINT, not
    exercised logic today: no real ingestion pipeline in this codebase
    currently populates a successor ticker or swap ratio for a MERGER/
    SPINOFF corporate_actions row (corporate_actions.action_type is
    free-text VARCHAR with no MERGER/SPINOFF values produced by any real
    scraper as of this fix) — building stock-swap modeling against data
    that doesn't exist would be fabrication, not a fix. When both are
    real and present, BacktestOrchestrator swaps the position into the
    successor ticker at the given ratio; when either is missing (the
    only case reachable with real data today), it force-closes at the
    last known real price instead — the plan's own documented "mark-to-
    last-price-and-close" policy option.
    """

    action_type: str  # "MERGER" | "SPINOFF"
    successor_ticker: Optional[str] = None
    swap_ratio: Optional[float] = None


CorporateActionLookup = Callable[[str, date_type], Optional[CorporateActionEvent]]


@dataclass
class DataGap:
    ticker: str
    as_of_date: date_type
    reason: str


@dataclass
class RefitEvent:
    as_of_date: date_type
    model_version: str


@dataclass
class OrchestratorConfig:
    trading_days: pd.DatetimeIndex
    universe_provider: UniverseProvider
    price_lookup: PriceLookup
    sector_lookup: SectorLookup = field(default=lambda ticker: "Unknown")
    is_delisted: Optional[IsDelistedCheck] = None
    corporate_action_lookup: Optional[CorporateActionLookup] = None
    rebalance_cadence_days: Optional[int] = None  # None -> use horizon_bucket's default
    refit_cadence_days: Optional[int] = None  # Walk-Forward retrain cadence; None -> never refit (plain backtest)
    # REV17 (2026-07-21 review): signals generated at as_of_date were always
    # filled at that SAME day's own close — an undocumented, silent
    # simplification that overstates fill quality (the signal couldn't
    # actually have been acted on until the price was already known).
    # Default unchanged (same_day_close) for full backward compatibility;
    # "next_day_open" is the explicit, tested alternative this review asked
    # for, so the convention is now a decided, visible choice rather than a
    # silent one — see _resolve_execution_date below.
    execution_timing: Literal["same_day_close", "next_day_open"] = "same_day_close"


class BacktestOrchestrator:
    """
    Runs one BacktestRun end-to-end against one adapter, per the Standard
    Backtesting Algorithm. Stateless across runs — construct fresh (or
    reuse) per call to run(); all mutable state lives in the
    StrategyPortfolio created inside run().
    """

    def __init__(self, feature_log_writer=None, regime_conn=None, regime_index_name: str = "Nifty 500") -> None:
        """feature_log_writer: optional backtest.core.feature_log.FeatureLogWriter.
        None is valid — orchestration/metrics tests that don't need a live
        DuckDB connection can omit it; production callers always supply one.

        regime_conn: optional read-only DuckDB connection to config.settings.
        DUCKDB_PATH (the normalised-schema DB market_regimes lives in — a
        DIFFERENT file from BACKTEST_DUCKDB_PATH, so this is deliberately a
        second connection, not the same one feature_log_writer uses). When
        given, the result's regime_breakdown is populated
        (backtest/core/regime_breakdown.py); when None (the default), it's
        left empty — regime breakdown is opt-in, not required for every run.
        """
        self._feature_log_writer = feature_log_writer
        self._regime_conn = regime_conn
        self._regime_index_name = regime_index_name

    def run(self, run: BacktestRun, adapter: StrategyAdapter, config: OrchestratorConfig) -> BacktestRunResult:
        if run.channel != adapter.channel:
            raise ValueError(f"run.channel={run.channel!r} does not match adapter.channel={adapter.channel!r}")

        sizing = sizing_for(run.horizon_bucket)
        cadence = config.rebalance_cadence_days or sizing.default_rebalance_cadence_days
        rebalance_dates = config.trading_days[::cadence]
        if len(rebalance_dates) == 0:
            raise ValueError("no rebalance dates in the supplied trading_days for this cadence")

        sip = SipConfig(amount=run.sip_amount) if run.capital_mode == "sip" and run.sip_amount else None
        portfolio = StrategyPortfolio(
            initial_capital=run.initial_capital, horizon_bucket=run.horizon_bucket, sip=sip,
        )
        portfolio.prime_sip_schedule(config.trading_days)

        data_gaps: List[DataGap] = []
        distinct_tickers: List[str] = []
        refit_log: List[RefitEvent] = []
        refit_dates = (
            set(config.trading_days[:: config.refit_cadence_days])
            if config.refit_cadence_days else set()
        )

        for as_of_date in rebalance_dates:
            as_of = as_of_date.date() if hasattr(as_of_date, "date") else as_of_date

            # Walk-Forward retraining (Phase 2.5, BacktestUmbrellaPlan.md "Walk-Forward
            # Module"): only called for adapters that implement an optional refit()
            # method — none of Phase 2's adapters need one today (their signals are
            # already point-in-time-pure recomputations, not fitted models), but this
            # is the hook a future ML-style adapter plugs a retrain step into.
            if as_of_date in refit_dates and hasattr(adapter, "refit"):
                model_version = adapter.refit(as_of)
                refit_log.append(RefitEvent(as_of_date=as_of, model_version=str(model_version)))

            portfolio.apply_due_sip_injections(as_of)

            # Corporate-action/delisting reconciliation BEFORE new sizing (Standard
            # Backtesting Algorithm step 3b, Truthful Review Gap #4) — always runs,
            # regardless of what the adapter's signals say this period.
            #
            # MERGER/SPINOFF checked FIRST, separately from delisting: a merged/
            # spun-off company may never appear in delisted_companies at all (it
            # didn't fail or get suspended, it stopped existing as a distinct
            # security for a completely different reason), so this must not be
            # folded into the is_delisted branch below.
            if config.corporate_action_lookup is not None:
                for ticker in list(portfolio.positions.keys()):
                    event = config.corporate_action_lookup(ticker, as_of)
                    if event is None or event.action_type not in ("MERGER", "SPINOFF"):
                        continue
                    successor_price = (
                        config.price_lookup(event.successor_ticker, as_of)
                        if event.successor_ticker else None
                    )
                    if event.successor_ticker and event.swap_ratio and successor_price is not None:
                        # Real swap data available: close the original and open the
                        # successor position at the disclosed ratio — see
                        # CorporateActionEvent's docstring; unexercised with today's
                        # real data (no ingestion source populates these fields yet).
                        old_position = portfolio.positions.get(ticker)
                        quantity = old_position.quantity if old_position else 0
                        # swap_ratio = new shares received per 1 old share; the
                        # value received per old share is therefore
                        # successor_price * swap_ratio, not successor_price alone.
                        portfolio.force_close(ticker, successor_price * event.swap_ratio, as_of, reason=f"{event.action_type.lower()}_swap")
                        new_quantity = int(quantity * event.swap_ratio)
                        if new_quantity > 0:
                            portfolio.positions[event.successor_ticker] = Position(
                                ticker=event.successor_ticker,
                                sector=config.sector_lookup(event.successor_ticker),
                                quantity=new_quantity,
                                entry_price=successor_price,
                                entry_date=as_of,
                            )
                    else:
                        # No real successor/ratio data (the only reachable case
                        # today) — the plan's documented "mark-to-last-price-and-
                        # close" policy: realize P&L at the last known real price,
                        # release capital, never fabricate a swap.
                        price = config.price_lookup(ticker, as_of)
                        if price is None:
                            data_gaps.append(DataGap(ticker, as_of, f"{event.action_type.lower()}_and_no_close_price"))
                            continue
                        portfolio.force_close(ticker, price, as_of, reason=f"{event.action_type.lower()}_forced_close")

            if config.is_delisted is not None:
                for ticker in list(portfolio.positions.keys()):
                    if config.is_delisted(ticker, as_of):
                        price = config.price_lookup(ticker, as_of)
                        if price is None:
                            data_gaps.append(DataGap(ticker, as_of, "delisted_and_no_close_price"))
                            continue
                        portfolio.force_close(ticker, price, as_of, reason="forced_close")

            universe = config.universe_provider(as_of)
            signals = adapter.generate_signals(universe, as_of, run.horizon_bucket)

            prices: Dict[str, float] = {}
            for ticker in set(list(portfolio.positions.keys()) + [s.ticker for s in signals]):
                price = config.price_lookup(ticker, as_of)
                if price is not None:
                    prices[ticker] = price
                elif ticker in portfolio.positions:
                    data_gaps.append(DataGap(ticker, as_of, "no_price_marking_open_position_at_last_known_price"))

            for signal in signals:
                self._log_feature(run.run_id, signal.ticker, as_of, run.horizon_bucket, adapter, signal.action)
                distinct_tickers.append(signal.ticker)

            # REV17 (2026-07-21 review): a signal generated at as_of used to
            # always fill at that SAME day's own close — overstating fill
            # quality, since the signal couldn't actually have been acted on
            # until that price was already known. execution_timing="same_day_close"
            # (default) preserves this exact prior behavior; "next_day_open"
            # fills at the NEXT trading day's price_lookup value instead (this
            # engine has one generic per-adapter price_lookup, not a separate
            # open/close pair, so "next_day_open" means "priced at the next
            # trading day", whatever convention that adapter's price_lookup
            # itself uses). Position-sizing equity valuation (`prices` above)
            # deliberately stays as_of-priced — sizing is a decision made with
            # information known at signal time, only the FILL is delayed.
            execution_date = as_of
            if config.execution_timing == "next_day_open":
                later_dates = config.trading_days[config.trading_days > as_of_date]
                if len(later_dates) > 0:
                    next_date = later_dates[0]
                    execution_date = next_date.date() if hasattr(next_date, "date") else next_date
                else:
                    data_gaps.append(
                        DataGap(
                            "__execution_timing__", as_of,
                            "next_day_open_unavailable_at_last_rebalance_fell_back_to_same_day_close",
                        )
                    )

            fill_prices: Dict[str, float] = {}
            if signals:
                fill_tickers = {s.ticker for s in signals}
                if execution_date == as_of:
                    fill_prices = {t: prices[t] for t in fill_tickers if t in prices}
                else:
                    for ticker in fill_tickers:
                        price = config.price_lookup(ticker, execution_date)
                        if price is not None:
                            fill_prices[ticker] = price

            # sells before buys, so freed cash is available for the same rebalance's buys
            for signal in sorted((s for s in signals if s.action == "sell"), key=lambda s: -s.conviction):
                if signal.ticker not in fill_prices:
                    data_gaps.append(DataGap(signal.ticker, execution_date, "no_price_for_sell_signal"))
                    continue
                portfolio.sell(
                    signal.ticker, fill_prices[signal.ticker], execution_date, reason="signal", adtv_cr=signal.adtv_cr,
                )

            for signal in sorted((s for s in signals if s.action == "buy"), key=lambda s: -s.conviction):
                if signal.ticker not in fill_prices:
                    data_gaps.append(DataGap(signal.ticker, execution_date, "no_price_for_buy_signal"))
                    continue
                if signal.adtv_cr is None:
                    # 2026-07-20 (Truthful Review Gap #6): core/portfolio.py's
                    # position_size() only enforces the ADTV hard cap when
                    # adtv_cr is provided — silently skipping it otherwise.
                    # Recording this as a visible data_gap (not just sizing
                    # uncapped without comment) so a channel/strategy that
                    # never populates Signal.adtv_cr shows up honestly in
                    # results instead of looking like the cap was checked
                    # and passed.
                    data_gaps.append(DataGap(signal.ticker, execution_date, "no_adtv_data_position_sized_uncapped"))
                portfolio.buy(
                    signal.ticker, config.sector_lookup(signal.ticker), fill_prices[signal.ticker], execution_date,
                    prices, adtv_cr=signal.adtv_cr,
                )

            portfolio.record_equity(as_of, prices)

        if self._feature_log_writer is not None:
            self._feature_log_writer.flush()

        return self._finalize(
            run, portfolio, data_gaps, distinct_tickers, config.trading_days, refit_log,
            execution_timing=config.execution_timing,
        )

    def _log_feature(self, run_id, ticker, as_of, horizon_bucket, adapter: StrategyAdapter, action: str) -> None:
        if self._feature_log_writer is None:
            return
        self._feature_log_writer.record(
            run_id=run_id, ticker=ticker, as_of_date=as_of, horizon_bucket=horizon_bucket,
            feature_vector=adapter.feature_vector(ticker, as_of), decision_taken=action,
        )

    def _finalize(
        self, run: BacktestRun, portfolio: StrategyPortfolio, data_gaps: List[DataGap],
        distinct_tickers: List[str], trading_days: pd.DatetimeIndex, refit_log: Optional[List[RefitEvent]] = None,
        execution_timing: str = "same_day_close",
    ) -> BacktestRunResult:
        tax_flows = fy_tax_cash_flows(portfolio.tax_transactions())
        cash_flows = [(cf["date"], cf["amount"]) for cf in portfolio.cash_flows] + [
            (d.isoformat(), amt) for d, amt in tax_flows
        ]
        # Tax is a real cash outflow — deduct it from the equity curve's final value too,
        # not just from the XIRR cash-flow series, so final_capital reflects it.
        total_tax = -sum(amt for _, amt in tax_flows)
        equity_curve = portfolio.equity_curve
        if len(equity_curve) and total_tax:
            equity_curve = equity_curve.copy()
            equity_curve.iloc[-1] = equity_curve.iloc[-1] - total_tax

        trade_pnls = [t.pnl_inr for t in portfolio.trades]
        trade_values = [t.entry_price * t.quantity for t in portfolio.trades]
        start_date = trading_days[0].date() if hasattr(trading_days[0], "date") else trading_days[0]
        end_date = trading_days[-1].date() if hasattr(trading_days[-1], "date") else trading_days[-1]

        metrics = compute_metrics(
            equity_curve=equity_curve, cash_flows=cash_flows, trade_pnls=trade_pnls,
            trade_values=trade_values, distinct_tickers=distinct_tickers,
            start_date=start_date, end_date=end_date, total_contributed=portfolio.total_contributed,
            cash_position_series=portfolio.cash_position_series,
        )

        regime_breakdown: List[Dict[str, Any]] = []
        if self._regime_conn is not None:
            from dataclasses import asdict as _asdict

            from systems.regime.regime_store import list_regime_segments

            segments = list_regime_segments(
                self._regime_conn, self._regime_index_name, start_date=start_date, end_date=end_date
            )
            regime_breakdown = [
                _asdict(row)
                for row in compute_regime_breakdown(equity_curve, portfolio.trades, start_date, end_date, segments)
            ]

        from dataclasses import asdict
        return BacktestRunResult(
            run=run,
            metrics=asdict(metrics),
            data_gaps=[{"ticker": g.ticker, "as_of_date": g.as_of_date.isoformat(), "reason": g.reason} for g in data_gaps],
            refit_log=[{"as_of_date": r.as_of_date.isoformat(), "model_version": r.model_version} for r in (refit_log or [])],
            execution_timing=execution_timing,
            regime_breakdown=regime_breakdown,
        )
