"""
backtest/momentum_orchestrator_runner.py

Phase: H4 -- retire MomentumBacktester (UnifiedGeneratorRefactorPlan.md §7)
Owner: Platform / Backtest
Consumers: scripts/run_momentum_*.py, scripts/run_band*.py,
systems/copilot/backtest_bridge.py

Drop-in replacement for backtest/momentum_backtest.py::MomentumBacktester
for the research/sweep scripts that construct it directly against an
already-loaded in-memory price_panel. Those scripts are NOT repointed onto
backtest/run_orchestrator_backtest.py::run_orchestrator_backtest() --
that is the DB-integrated single-run production path (re-fetches OHLCV,
holds exclusive_backtest_lock, writes backtest_runs) and is the wrong tool
for a loop running dozens-to-hundreds of lightweight parameter variants
against data already loaded once.

This module drives the SAME generator every production run and every
other channel uses -- backtest.adapters.momentum_adapter.MomentumAdapter
through backtest.core.engine.BacktestOrchestrator, the exact pattern
scripts/compare_momentum_simulation_paths.py (ML40-2.1) proved out -- and
reshapes the output into a MomentumOrchestratorResult exposing the same
field names/shapes MomentumBacktestResult did, so callers (cagr(),
churn_factor(), trade_quality_metrics(), etc., now living in
backtest/core/metrics.py and backtest/core/tax.py -- see H4) needed no
rewrite beyond the construction call itself.

Known, documented differences from MomentumBacktester (never silently
papered over -- No-Mock-Data Policy):

  - Seven knobs no longer exist on MomentumAdapter, per the 2026-08-18 user
    decision that pure-play momentum is a plain rank rotation
    (UnifiedGeneratorRefactorPlan.md §19): grace_cycles, exit_rank,
    max_pct_of_adtv, trailing_stop_pct, per_ticker_hmm_regime,
    disable_hmm_regimes, min_momentum, volume_weighted, momentum_panel
    (the momentum score is always recomputed from price_panel; a
    precomputed override panel is not accepted). Passing any of them
    raises TypeError -- a caller that meant to sweep one of these fails
    loudly instead of silently reporting a flat, meaningless result.
  - `transactions` covers CLOSED trades only. BacktestOrchestrator's trade
    log (backtest/core/engine.py::_write_trade_log) records closed trades;
    still-open positions at run end are not included, so every
    transaction dict's "status" is "closed". MomentumBacktestResult also
    included open positions; that view is not available here.
  - "sell_rank" is always None: the shared trade log records
    entry_market_cap_rank (rank at BUY time) only, never the rank a
    position had when it was sold.
  - `equity_curve` entries use the old "total_value" key (so
    rolling_window_returns/income_mode_summary need no change) but the
    VALUES are now real DAILY mark-to-market points (one per trading day)
    rather than one point per rebalance -- a strict improvement, not a
    behaviour loss.
  - `tax_payments` / `capital_resets` are read from
    BacktestRunResult.tax_ledger / .fy_ledger (StrategyPortfolio's own tax
    engine, backtest/core/tax.py) rather than momentum_tax.py. Only
    populated when withhold_fy_tax=True / annual_capital_reset_target is
    set, same as before.
"""

from __future__ import annotations

import logging
from datetime import date as date_type
from typing import Any, Dict, List, Literal, Optional, Set, cast

import pandas as pd

from backtest.adapters.momentum_adapter import MomentumAdapter
from backtest.core.engine import BacktestOrchestrator, OrchestratorConfig
from backtest.core.run_context import BacktestRun, BacktestRunResult
from backtest.core.tax import Transaction as TaxTransaction
from backtest.strategy_id import default_horizon_for_momentum

logger = logging.getLogger(__name__)


def to_core_transactions(transactions: List[Dict[str, Any]]) -> List[TaxTransaction]:
    """Closed transactions from a MomentumOrchestratorResult, as
    backtest/core/tax.py::Transaction objects -- for callers that used to
    pass MomentumBacktestResult.transactions straight into
    backtest.momentum_tax.compute_total_tax/post_tax_ending_value and now
    call backtest.core.tax.total_tax/post_tax_ending_value instead (H4:
    momentum_tax.py is deleted)."""
    out = []
    for t in transactions:
        if t["status"] != "closed" or t["sell_date"] is None or t["sell_price"] is None:
            continue
        out.append(TaxTransaction(
            ticker=t["ticker"],
            buy_date=pd.Timestamp(t["buy_date"]).date(),
            sell_date=pd.Timestamp(t["sell_date"]).date(),
            buy_price=t["buy_price"],
            sell_price=t["sell_price"],
            quantity=t["qty"],
        ))
    return out


class MomentumOrchestratorResult:
    """Field-compatible stand-in for backtest/momentum_backtest.py's
    MomentumBacktestResult -- see module docstring for the documented
    differences."""

    def __init__(
        self,
        equity_curve: List[Dict[str, Any]],
        rebalance_events: List[Dict[str, Any]],
        transactions: List[Dict[str, Any]],
        starting_capital: float,
        ending_value: float,
        start_date: str,
        end_date: str,
        cash_flows: List[Dict[str, Any]],
        total_contributed: float,
        total_signals: int,
        tax_payments: List[Dict[str, Any]],
        capital_resets: List[Dict[str, Any]],
        run_result: BacktestRunResult,
    ) -> None:
        self.equity_curve = equity_curve
        self.rebalance_events = rebalance_events
        self.transactions = transactions
        self.starting_capital = starting_capital
        self.ending_value = ending_value
        self.start_date = start_date
        self.end_date = end_date
        self.cash_flows = cash_flows
        self.total_contributed = total_contributed
        self.total_signals = total_signals
        self.tax_payments = tax_payments
        self.capital_resets = capital_resets
        # The raw BacktestRunResult, for callers that want the shared
        # compute_metrics() output (cagr/sharpe/sortino/calmar/win_rate)
        # directly rather than recomputing it via backtest/core/metrics.py
        # helpers keyed off starting_capital/ending_value.
        self.run_result = run_result


def _sector_map() -> Dict[str, str]:
    from config.universe import load_universe_raw

    raw = load_universe_raw()
    return {t: s for t, s in zip(raw["ticker"], raw["sector"]) if s is not None}


def run_momentum_orchestrated(
    price_panel: pd.DataFrame,
    yearly_universes: Dict[str, List[str]],
    lookback_days: int,
    rebalance_every_n_trading_days: int,
    rebalance_offset_days: int = 0,
    starting_capital: float = 1_000_000.0,
    investable_pct: float = 0.8,
    top_n: int = 20,
    strategy_id: str = "momentum_orchestrated_probe",
    sip_amount: Optional[float] = None,
    downtrend_filter_pct: Optional[float] = None,
    downtrend_lookback_days: int = 20,
    volume_panel: Optional[pd.DataFrame] = None,
    min_adtv_cr: Optional[float] = None,
    adtv_lookback_days: int = 20,
    circuit_band_pct: Optional[float] = None,
    approximation_flags: Optional[Dict[str, Dict[str, bool]]] = None,
    exclude_approximated_mcap: bool = False,
    regime_conn: Optional[Any] = None,
    regime_index_name: str = "Nifty 500",
    regime_method: Optional[str] = None,
    disable_buys_in_regime: Optional[Set[str]] = None,
    orthogonalize_vs_size_beta: bool = False,
    market_cap_panel: Optional[pd.DataFrame] = None,
    beta_map: Optional[Dict[str, float]] = None,
    quality_scores: Optional[Dict[str, Dict[str, float]]] = None,
    quality_gate: Optional[Dict[str, float]] = None,
    costs: Optional[Any] = None,
    sector_lookup: Optional[Dict[str, str]] = None,
    withhold_fy_tax: bool = False,
    annual_capital_reset_target: Optional[float] = None,
    rank_start: Optional[int] = None,
    yearly_rank_lookup: Optional[Dict[str, Dict[str, int]]] = None,
    persist_signals: bool = False,
) -> MomentumOrchestratorResult:
    """Run one momentum strategy through BacktestOrchestrator +
    MomentumAdapter -- the same generator run_orchestrator_backtest.py's
    momentum branch drives -- and return a MomentumBacktestResult-shaped
    object. See module docstring for the documented shape differences.

    lookback_days is in TRADING days (matches MomentumBacktester's own
    convention, unlike MomentumAdapter's lookback_months) -- converted here
    so callers built around the old constructor need no unit change.
    """
    if costs is not None:
        logger.warning(
            "run_momentum_orchestrated: `costs` is accepted for signature "
            "compatibility but ignored -- BacktestOrchestrator prices "
            "costs via backtest/costs.py internally through "
            "StrategyPortfolio, not an injected IndianTransactionCosts."
        )

    lookback_months = max(1, round(lookback_days / 21))
    # BacktestOrchestrator computes rebalance dates as
    # config.trading_days[::cadence] (backtest/core/engine.py) -- slicing
    # off the first rebalance_offset_days trading days shifts which
    # absolute dates the schedule lands on by exactly that many days,
    # reproducing MomentumBacktester.rebalance_offset_days' effect. The
    # window's start therefore moves forward by rebalance_offset_days
    # trading days too (a documented side effect, not hidden).
    trading_days = price_panel.index[rebalance_offset_days:] if rebalance_offset_days else price_panel.index
    start = pd.Timestamp(trading_days.min()).date()
    end = pd.Timestamp(trading_days.max()).date()

    resolved_sector_lookup = sector_lookup if sector_lookup is not None else _sector_map()

    def universe_provider(as_of: date_type) -> List[str]:
        keys = sorted(k for k in yearly_universes if k <= as_of.isoformat())
        return list(yearly_universes[keys[-1]]) if keys else []

    def price_lookup(ticker: str, as_of: date_type) -> Optional[float]:
        if ticker not in price_panel.columns:
            return None
        window = price_panel[ticker].loc[: pd.Timestamp(as_of)].dropna()
        return float(window.iloc[-1]) if len(window) else None

    adapter = MomentumAdapter(
        price_panel=price_panel,
        top_n=top_n,
        lookback_months=lookback_months,
        sector_lookup=resolved_sector_lookup,
        volume_panel=volume_panel,
        adtv_lookback_days=adtv_lookback_days,
        min_adtv_cr=min_adtv_cr,
        circuit_band_pct=circuit_band_pct,
        downtrend_filter_pct=downtrend_filter_pct,
        downtrend_lookback_days=downtrend_lookback_days,
        quality_scores=quality_scores,
        quality_gate=quality_gate,
        regime_conn=regime_conn,
        regime_index_name=regime_index_name,
        regime_method=regime_method,
        disable_buys_in_regime=disable_buys_in_regime,
        orthogonalize_vs_size_beta=orthogonalize_vs_size_beta,
        market_cap_panel=market_cap_panel,
        beta_map=beta_map,
        exclude_approximated_mcap=exclude_approximated_mcap,
        approximation_flags=approximation_flags,
        rank_start=rank_start,
        yearly_rank_lookup=yearly_rank_lookup,
    )

    capital_mode: Literal["lump", "sip", "annual_reset"] = "annual_reset" if annual_capital_reset_target is not None else (
        "sip" if sip_amount else "lump"
    )
    config = OrchestratorConfig(
        trading_days=trading_days,
        sector_lookup=lambda ticker: resolved_sector_lookup.get(ticker, "Unknown"),
        universe_provider=universe_provider,
        price_lookup=price_lookup,
        rebalance_cadence_days=rebalance_every_n_trading_days,
        # Momentum is periodic (see OrchestratorConfig.exit_policy_cadence
        # docstring) -- every real momentum run uses "rebalance", matching
        # run_orchestrator_backtest.py::_exit_policy_cadence_for.
        exit_policy_cadence="rebalance",
        # [2026-08-18 user decision] fully invested, no sector cap; slot
        # size is 1/top_n -- matches
        # run_orchestrator_backtest.py::_sizing_overrides_for("momentum", ...).
        sizing_overrides={"max_position_pct": investable_pct / top_n, "max_sector_pct": 1.0},
        persist_signals=persist_signals,
    )

    run = BacktestRun(
        run_id=f"{strategy_id}-{pd.Timestamp.utcnow().isoformat()}",
        channel="momentum",
        strategy_id=strategy_id,
        horizon_bucket=default_horizon_for_momentum(lookback_months),
        mode="backtest",
        universe_spec="momentum_rank_band",
        start_date=start,
        end_date=end,
        capital_mode=capital_mode,
        initial_capital=starting_capital,
        sip_amount=sip_amount,
        annual_reset_ltcg_rate=None if annual_capital_reset_target is None else 0.125,
    )

    result = BacktestOrchestrator().run(run, adapter, config)

    transactions: List[Dict[str, Any]] = []
    if result.trade_log_path:
        try:
            log = pd.read_csv(result.trade_log_path)
        except Exception:
            logger.exception("run_momentum_orchestrated: failed to read trade log %s", result.trade_log_path)
            log = pd.DataFrame()
        for row in log.itertuples(index=False):
            buy_date = str(getattr(row, "buy_date"))[:10]
            sale_date = getattr(row, "sale_date", None)
            sell_date = None if pd.isna(sale_date) or str(sale_date) in ("", "nan") else str(sale_date)[:10]
            holding_days = None
            if sell_date is not None:
                holding_days = (pd.Timestamp(sell_date) - pd.Timestamp(buy_date)).days
            stock_rank = getattr(row, "stock_rank", None)
            transactions.append({
                "ticker": getattr(row, "ticker"),
                "buy_date": buy_date,
                "buy_price": float(getattr(row, "buy_price")),
                "sell_date": sell_date,
                "sell_price": None if pd.isna(getattr(row, "sale_price")) else float(getattr(row, "sale_price")),
                # "qty" (not "quantity") -- matches MomentumBacktestResult's
                # own key, which backtest/core/tax.py's dict-based helpers
                # and several scripts' t["qty"] accesses depend on.
                "qty": int(getattr(row, "qty")),
                "status": "closed",
                # stock_rank is Any from the trade log and may be NaN, "" or
                # None; the guards above cover all three, so the cast states
                # what they already established rather than widening it.
                "buy_momentum_rank": (
                    None if stock_rank is None or pd.isna(stock_rank) or stock_rank == ""
                    else int(cast(Any, stock_rank))
                ),
                # trade log only carries entry rank, never exit rank -- see module docstring
                "sell_momentum_rank": None,
                "trade_cagr": None,  # not recomputed here; callers use .get("trade_cagr")
                "exit_reason": getattr(row, "exit_reason", None),
                "holding_days": holding_days,
            })

    # rebalance_events (old shape: [{"date","n_bought","n_sold"}]) is not
    # carried on BacktestRunResult, but it is faithfully reconstructable
    # (not fabricated) from the real per-trade buy/sell dates already in
    # `transactions` -- a rebalance date is any date something was bought
    # or sold.
    n_bought_by_date: Dict[str, int] = {}
    n_sold_by_date: Dict[str, int] = {}
    for t in transactions:
        n_bought_by_date[t["buy_date"]] = n_bought_by_date.get(t["buy_date"], 0) + 1
        if t["sell_date"] is not None:
            n_sold_by_date[t["sell_date"]] = n_sold_by_date.get(t["sell_date"], 0) + 1
    rebalance_events = [
        {"date": d, "n_bought": n_bought_by_date.get(d, 0), "n_sold": n_sold_by_date.get(d, 0)}
        for d in sorted(set(n_bought_by_date) | set(n_sold_by_date))
    ]

    equity_curve = [
        {"date": pt["date"], "total_value": pt["equity"]} for pt in (result.equity_curve or [])
    ]
    ending_value = float(equity_curve[-1]["total_value"]) if equity_curve else starting_capital

    tax_payments = [
        {
            "date": row["fy_end"],
            "fy_label": row["fy_end"],
            "tax_due": row["assessed"],
            "tax_paid": row["paid"],
        }
        for row in (result.tax_ledger or [])
    ] if withhold_fy_tax else []

    capital_resets = [
        {
            "date": row["fy_end"],
            "fy_label": row["fy_end"],
            "pre_reset_value": row["closing_equity"],
            "withdrawal": row["withdrawn"],
            "injection": row["topped_up"],
        }
        for row in (result.fy_ledger or [])
    ] if annual_capital_reset_target is not None else []

    # SIP injection dates are a pure function of the trading-day calendar
    # (first trading day of every calendar month after the first), the same
    # rule backtest/core/portfolio.py::StrategyPortfolio._monthly_injection_
    # dates and the old MomentumBacktester._monthly_injection_dates both use
    # -- so cash_flows is reconstructable exactly, not approximated, without
    # needing the portfolio object itself (not carried on BacktestRunResult).
    cash_flows: List[Dict[str, Any]] = [{"date": start.isoformat(), "amount": -starting_capital}]
    total_contributed = starting_capital
    if sip_amount:
        seen_months = set()
        injection_dates = []
        for d in trading_days:
            key = (d.year, d.month)
            if key not in seen_months:
                seen_months.add(key)
                injection_dates.append(d)
        for d in injection_dates[1:]:  # first month's contribution is starting_capital itself
            cash_flows.append({"date": pd.Timestamp(d).date().isoformat(), "amount": -sip_amount})
            total_contributed += sip_amount

    metrics = result.metrics or {}
    total_signals = int(metrics.get("n_trades") or len(transactions))

    return MomentumOrchestratorResult(
        equity_curve=equity_curve,
        rebalance_events=rebalance_events,
        transactions=transactions,
        starting_capital=starting_capital,
        ending_value=ending_value,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        cash_flows=cash_flows,
        total_contributed=total_contributed,
        total_signals=total_signals,
        tax_payments=tax_payments,
        capital_resets=capital_resets,
        run_result=result,
    )
