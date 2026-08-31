"""
backtest/core/metrics.py

Phase: Unified Backtest & Paper Trading Umbrella, Phase 1
Owner: Platform / Backtest
Consumers: backtest/core/engine.py (once refactored), every channel
adapter, datastore/api/routers/backtest_reports.py (once migrated)

The single metrics module every channel/adapter reports through, closing
the methodology mismatch between backtest/engine.py (trading-day-
annualized CAGR) and backtest/momentum_metrics.py (calendar/365.25 CAGR):
per the 2026-07-20 user-confirmed decision, calendar/365.25 CAGR + XIRR
are canonical everywhere; trading-day-annualized CAGR is kept only as a
secondary/legacy field (`cagr_trading_day_legacy`) for backward
comparability with existing ML `/ml-backtest` reports.

DEFINES xirr()/churn_factor()/return_population_zscores() (Phase H1,
2026-08-18 — they used to live in backtest/momentum_metrics.py and be
imported from here, which had the shared module depending on a
channel-specific one). The bisection-based XIRR is
channel-agnostic — it operates on a plain cash-flow list, no
momentum-specific assumptions).
"""

import logging
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import numpy as np
import pandas as pd


TRADING_DAYS_PER_YEAR = 252

# [H4, 2026-08-18] trade_cagr, moved here verbatim from backtest/momentum_
# backtest.py (deleted with MomentumBacktester) -- channel-agnostic despite
# living there originally, and still needed by backtest/export_trade_book.py.
# [DATA QUALITY, 2026-08-02] Dedicated log for trade_cagr's OverflowError
# guard (see its docstring) -- these trades' extreme price ratios are a
# real-data-corruption signal (an un-smoothed adj_factor discontinuity at a
# corporate action), not just a numeric edge case, so every occurrence is
# recorded here for later data-quality triage rather than silently dropped
# alongside the None return value.
DATA_QUALITY_ANOMALY_LOG_PATH = Path(__file__).resolve().parent.parent.parent / "logs" / "data_quality_anomalies.log"
_anomaly_logger = logging.getLogger("backtest.data_quality_anomalies")
if not _anomaly_logger.handlers:
    DATA_QUALITY_ANOMALY_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    _handler = logging.FileHandler(DATA_QUALITY_ANOMALY_LOG_PATH, mode="a")
    _handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    _anomaly_logger.addHandler(_handler)
    _anomaly_logger.setLevel(logging.WARNING)
    _anomaly_logger.propagate = False


def trade_cagr(
    buy_price: float, sell_price: Optional[float], holding_days: Optional[int],
    *, ticker: Optional[str] = None, buy_date: Optional[str] = None,
    sell_date: Optional[str] = None, run_id: Optional[str] = None,
) -> Optional[float]:
    """Per-trade annualized price gain: (sell/buy)^(365.25/holding_days) - 1.

    None for a still-open position (sell_price/holding_days not yet known)
    or a same-day round-trip (holding_days <= 0 makes annualizing
    meaningless -- division by a near-zero exponent denominator blows up).

    ticker/buy_date/sell_date/run_id are optional, log-only context (not
    used in the calculation) so an OverflowError anomaly can be traced back
    to the actual trade without changing this function's return contract
    for existing positional callers.
    """
    if sell_price is None or holding_days is None or holding_days <= 0 or buy_price <= 0:
        return None
    try:
        return float((sell_price / buy_price) ** (365.25 / holding_days) - 1)
    except OverflowError:
        _anomaly_logger.warning(
            "trade_cagr_overflow ticker=%s run_id=%s buy_date=%s sell_date=%s "
            "buy_price=%s sell_price=%s ratio=%s holding_days=%s",
            ticker, run_id, buy_date, sell_date, buy_price, sell_price,
            (sell_price / buy_price) if buy_price else None, holding_days,
        )
        return None


# ---------------------------------------------------------------------------
# Channel-agnostic primitives (Phase H1, 2026-08-18)
#
# These three lived in backtest/momentum_metrics.py and were IMPORTED here,
# which had the shared metrics module depending on a channel-specific one --
# and on the specific module ML40-2.3 retires with MomentumBacktester. They
# were never momentum-specific: xirr is money-weighted return for any
# irregular cash-flow series, churn_factor summarises any rebalance log, and
# return_population_zscores is outlier detection over any trade population.
# Only the module's NAME said momentum.
#
# momentum_metrics.py now re-exports them so the ~13 scripts/run_momentum_*.py
# keep working until H4 repoints them.
# ---------------------------------------------------------------------------


def xirr(cash_flows: List[Tuple[str, float]]) -> Optional[float]:
    """
    Money-weighted annual rate of return for a series of irregularly-timed
    cash flows (2026-07-14, added for the SIP comparison — plain CAGR only
    works for a single lump-sum in/out; a monthly SIP needs XIRR since
    each contribution has its own date).

    cash_flows : [(date_str, amount), ...] — contributions negative
        (money leaving the investor's pocket), the final value positive
        (money that would come back if liquidated on that date). Order
        doesn't matter internally but the first entry's date is used as
        the discounting anchor.

    Returns
    -------
    float or None — None if a rate can't be bracketed (e.g. all cash
    flows are the same sign, so there's no real rate that zeroes the
    NPV), rather than raising or guessing a value.
    """
    if len(cash_flows) < 2:
        raise ValueError("xirr needs at least 2 cash flows")
    dates = [pd.Timestamp(d) for d, _ in cash_flows]
    amounts = [a for _, a in cash_flows]
    anchor = min(dates)

    def npv(rate: float) -> float:
        total: float = sum(a / (1.0 + rate) ** ((d - anchor).days / 365.0) for d, a in zip(dates, amounts))
        return total

    lo, hi = -0.9999, 10.0
    f_lo, f_hi = npv(lo), npv(hi)
    if f_lo == 0:
        return lo
    if f_hi == 0:
        return hi
    if (f_lo > 0) == (f_hi > 0):
        return None  # same sign at both ends: can't bracket a root

    for _ in range(200):
        mid = (lo + hi) / 2.0
        f_mid = npv(mid)
        if abs(f_mid) < 1e-6:
            return mid
        if (f_lo > 0) == (f_mid > 0):
            lo, f_lo = mid, f_mid
        else:
            hi, f_hi = mid, f_mid
    return (lo + hi) / 2.0


def _xirr_cash_flows(
    cash_flows: Sequence[Tuple[str, float]],
    end_date: "DateLike",
    ending_value: float,
) -> List[Tuple[str, float]]:
    """The investor-perspective flow series XIRR actually needs.

    `cash_flows` carries what went IN (initial capital, SIP injections, both
    negative) and what came OUT before the run ended (annual-reset
    withdrawals, positive). It does not carry the liquidation at the end,
    because the portfolio does not "pay" it -- it is simply what the book is
    worth on the last day. Without it there is no positive flow to discount
    against and the rate is meaningless, so it is appended here rather than
    left to each of the four call sites to remember.

    A non-positive ending value is passed through as-is: a wiped-out book is
    a real (-100%-ish) outcome, and xirr() returns None when it cannot
    bracket a root.
    """
    flows = [(str(d), float(a)) for d, a in cash_flows]
    flows.append((str(end_date), float(ending_value)))
    return flows


def churn_factor(rebalance_events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    rebalance_events: list of dicts, one per rebalance, each with at least
    {"date": iso date str, "n_bought": int, "n_sold": int}.

    Returns
    -------
    dict with:
      per_rebalance: [{"date", "n_bought", "n_sold", "n_transactions"}, ...]
      avg_transactions_per_year: float — mean of each calendar year's total
        (bought + sold) transaction count across the run (the "average
        number of stocks bought+sold through the year" ML38 asked for).
    """
    per_rebalance = [
        {
            "date": event["date"],
            "n_bought": event["n_bought"],
            "n_sold": event["n_sold"],
            "n_transactions": event["n_bought"] + event["n_sold"],
        }
        for event in rebalance_events
    ]
    if not per_rebalance:
        return {"per_rebalance": [], "avg_transactions_per_year": 0.0}

    df = pd.DataFrame(per_rebalance)
    df["year"] = pd.to_datetime(df["date"]).dt.year
    per_year = df.groupby("year")["n_transactions"].sum()
    avg_per_year = float(per_year.mean())

    return {"per_rebalance": per_rebalance, "avg_transactions_per_year": avg_per_year}


_NEAR_ZERO_STD = 1e-9


def return_population_zscores(
    returns_pct: Sequence[Optional[float]], outlier_threshold: float = 3.0,
) -> Dict[str, Any]:
    """Channel-agnostic core of the outlier-detection math: given a list of
    per-trade % returns (None entries pass through as None, e.g. a trade
    with no realized/unrealized return yet), returns each entry's z-score
    plus population summary stats. Extracted from trade_quality_metrics()
    (2026-08-01) so backtest/core/metrics.py::compute_metrics can reuse the
    exact same statistics against backtest.portfolio.Trade's own pnl_pct
    (Technical/Fundamental/Momentum-via-orchestrator channels) instead of
    reimplementing it against a differently-shaped transaction dict.

    Returns
    -------
    dict with:
      zscores               — list, same length/order as returns_pct; None
          where the input was None or the population (<3 non-None values)
          is too small for a meaningful std.
      n_outliers             — count with |zscore| > outlier_threshold.
      max_abs_zscore         — the single most extreme |zscore|, or None.
    """
    valid = [r for r in returns_pct if r is not None]
    mean = float(np.mean(valid)) if len(valid) >= 3 else None
    std = float(np.std(valid)) if len(valid) >= 3 else None

    zscores: List[Optional[float]] = []
    n_outliers = 0
    max_abs_z = None
    for r in returns_pct:
        if r is None or mean is None or std is None or std == 0:
            zscores.append(None)
            continue
        z = (r - mean) / std
        zscores.append(z)
        if max_abs_z is None or abs(z) > max_abs_z:
            max_abs_z = abs(z)
        if abs(z) > outlier_threshold:
            n_outliers += 1

    return {"zscores": zscores, "n_outliers": n_outliers, "max_abs_zscore": max_abs_z}

# Anything pd.Timestamp() accepts as a single date, which is what every
# date-ish parameter below is immediately fed to (callers pass ISO strings,
# datetime.date/datetime and pandas Timestamps interchangeably today).
DateLike = Union[str, date, pd.Timestamp]


@dataclass
class BacktestMetrics:
    cagr: Optional[float]  # calendar/365.25 basis — primary
    cagr_trading_day_legacy: Optional[float]  # trading-day-annualized — secondary, for ML report continuity
    xirr: Optional[float]  # money-weighted, handles lump-sum and SIP uniformly
    final_capital: float
    total_contributed: float  # initial capital + all SIP injections (for SIP mode); == initial_capital for lump-sum
    max_drawdown: float
    win_rate: Optional[float]
    profit_factor: Optional[float]
    sharpe: Optional[float]  # 2026-07-26 (REV6 wiring): annualized daily-return Sharpe, rf=0 — deflated_sharpe_ratio's required input; None with < 2 return observations or zero volatility
    sortino: Optional[float]
    sortino_none_reason: Optional[str]  # REV19: why sortino is None, when it is
    calmar: Optional[float]
    calmar_none_reason: Optional[str]  # None when a real value was computed
    n_distinct_tickers_traded: int
    turnover_ratio: Optional[float]
    n_trades: int
    benchmark_cagr: Optional[float]  # None + benchmark_status flagged if fold predates 2023-07 (index_ohlcv gap)
    excess_return: Optional[float]
    benchmark_status: str  # "ok" | "insufficient_benchmark_history"
    # A98: WHICH index the comparison was against. Without it, two runs'
    # excess returns look comparable when they may be measured against
    # different yardsticks — and a report cannot honestly label its own
    # benchmark column. None only for runs predating this field.
    benchmark_index_name: Optional[str] = None
    # [T13, 2026-08-13] Consistency and trade-quality metrics that only
    # Momentum produced. Computed in-engine here so every channel reports the
    # same figures under the same definitions -- previously Technical could
    # get them only post-hoc from trade-book CSVs, under a different
    # definition, which is worse than not having them because the columns line
    # up and invite an invalid comparison.
    #
    # rolling_returns: {"3y": {min_cagr, median_cagr, max_cagr,
    # positive_share, n_windows}, ...} -- all FRACTIONS, all ANNUALISED.
    rolling_returns: Dict[str, Any] = field(default_factory=dict)
    # [{"fy_label": "FY2021", "return_pct": 0.18, "partial": false}, ...]
    fy_returns: List[Dict[str, Any]] = field(default_factory=list)
    churn_per_year: Optional[float] = None
    # Per-trade outcomes, not rates -- never annualise these.
    avg_winner_pct: Optional[float] = None
    avg_loser_pct: Optional[float] = None
    # [A86, 2026-08-13] Which basis `cagr` above is stated on: "post_tax" when
    # the run paid capital-gains tax as an annual cash outflow, "pre_tax"
    # otherwise. Reading a CAGR without knowing this is how a post-tax figure
    # gets compared with a pre-tax one and the difference is read as skill.
    tax_basis: str = "pre_tax"
    total_tax_paid: Optional[float] = None
    # The OTHER basis, reconstructed from the same run rather than re-simulated
    # (see reconstruct_pre_tax_curve for what that does and does not assume).
    # Both bases from one execution is the point of A86: Momentum previously
    # needed two full runs and Technical computed tax post-hoc on the trade
    # book, so the two channels' "post-tax" numbers were not the same measure.
    cagr_other_basis: Optional[float] = None
    cash_position_series: List[Dict[str, Any]] = field(default_factory=list)  # [{"date":..., "cash":...}, ...]
    # A90: the mark-to-market portfolio value per recorded date, [{"date":...,
    # "equity":...}, ...]. StrategyPortfolio has always computed this
    # (portfolio.record_equity -> total_equity = cash + positions at market),
    # but until now only the CASH half was carried into the result, so every
    # consumer that wanted an equity curve either had nothing to draw or drew
    # the cash series and mislabelled it. Cash alone is not the curve: a fully
    # invested portfolio shows near-zero cash while its equity compounds, so
    # plotting cash reads as a strategy that lost everything on day one.
    #
    # Persisted alongside cash_position_series rather than replacing it —
    # they answer different questions (how much is deployed vs. what is it
    # all worth), and the annual-reset ledger already reads the cash half.
    equity_curve_series: List[Dict[str, Any]] = field(default_factory=list)
    avg_days_held: Optional[float] = None  # mean (exit_date - entry_date).days across closed trades; None if n_trades == 0
    # 2026-08-01 (Technical-strategy Momentum-parity reporting) — n_trades
    # above is closed-only (len(trade_pnls)); total_trades additionally
    # counts still-open positions, matching Momentum's n_closed_trades vs.
    # total_trades distinction (backtest/momentum_metrics.py::trade_quality_metrics).
    total_trades: Optional[int] = None
    avg_trade_duration_days: Optional[float] = None  # mean holding-period across ALL trades (open + closed)
    n_outlier_trades: Optional[int] = None  # count with |return z-score| > 3 among this run's own closed trades
    max_abs_return_zscore: Optional[float] = None
    # Annualised stdev of periodic returns, as a fraction. See annualised_volatility.
    volatility: Optional[float] = None


def calendar_cagr(
    starting_capital: float, ending_value: float, start_date: DateLike, end_date: DateLike,
) -> Optional[float]:
    """Primary CAGR basis (calendar/365.25) — correct under both lump-sum and SIP
    when compared against XIRR, since it doesn't assume trading-day density."""
    if starting_capital <= 0:
        return None
    # [BUG FIX 2026-08-18] A non-positive ending value makes the base of the
    # fractional power negative, and Python returns a COMPLEX number from a
    # function declared Optional[float] -- which then flows into the report and
    # fails serialisation rather than reporting "no meaningful CAGR". The
    # sibling trading_day_cagr has always guarded exactly this case
    # (`if total_return <= 0: return None`); this brings the two into line.
    # Dormant in practice: a long-only book with no leverage cannot end
    # net-negative. Guarded anyway, because the failure mode is a wrong TYPE
    # escaping into a report, not a wrong number.
    if ending_value <= 0:
        return None
    years: float = (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days / 365.25
    if years <= 0:
        return None
    cagr_value: float = (ending_value / starting_capital) ** (1.0 / years) - 1.0
    return cagr_value


def trading_day_cagr(equity_curve: pd.Series) -> Optional[float]:
    """Legacy basis matching backtest/engine.py's existing _cagr_sharpe_from_equity
    methodology — trading-day-count annualized. Kept only for comparability with
    pre-refactor ML report output; never treat as primary."""
    if len(equity_curve) < 2 or equity_curve.iloc[0] <= 0:
        return None
    n_days = len(equity_curve) - 1
    if n_days <= 0:
        return None
    total_return: float = equity_curve.iloc[-1] / equity_curve.iloc[0]
    if total_return <= 0:
        return None
    years = n_days / TRADING_DAYS_PER_YEAR
    cagr_value: float = total_return ** (1.0 / years) - 1.0
    return cagr_value


def max_drawdown(equity_curve: pd.Series) -> float:
    if len(equity_curve) == 0:
        return 0.0
    running_max = equity_curve.cummax()
    drawdown = (equity_curve - running_max) / running_max
    return float(drawdown.min())


_TRADING_DAYS_PER_YEAR = 252

# A degenerate (no-trade / flat-equity) run's std/drawdown comes out as
# float rounding noise (e.g. 1e-16), not exact zero — every `== 0`
# division-by-zero guard below needs this tolerance instead, or the
# guard silently doesn't fire and a mean-noise/std-noise ratio gets
# reported as if it were a real metric (found 2026-07-26 auditing the
# B4 technical template: 0 trades, non-null Sharpe of -0.32).


def infer_periods_per_year(index: Any) -> float:
    """Observations per year implied by a curve's OWN date spacing.

    Sharpe and Sortino annualize by sqrt(periods per year), so the factor has
    to match the curve being measured. A daily equity curve is 252; a WEEKLY
    momentum rebalance curve is ~52, and annualizing it at 252 overstates the
    ratio by sqrt(252/52) ~ 2.2x. That mismatch is the entire reason
    momentum_metrics.py grew a second Sharpe implementation instead of calling
    this module's -- core's hardcoded 252 was correct for the daily curves the
    orchestrator produces and wrong for every other cadence.

    Inferring from real spacing rather than taking a declared cadence means a
    curve with gaps (holidays, a suspended strategy) annualizes on what
    actually happened, and no caller has to keep a cadence constant in sync
    with the data it describes.

    Falls back to the daily constant when there is too little to measure --
    two observations cannot establish a cadence.

    NOTE it returns ~261 for a business-daily curve, not 252: it measures
    calendar spacing, and a year holds ~261 weekdays before NSE holidays are
    removed. That is why sharpe_ratio still DEFAULTS to the 252 constant --
    switching daily curves to inference would move every published Sharpe by
    ~2% for no gain. Use this where the cadence is genuinely not daily.
    """
    stamps = pd.to_datetime(pd.Index(index))
    if len(stamps) < 3:
        return float(TRADING_DAYS_PER_YEAR)
    span_days = (stamps[-1] - stamps[0]).days
    if span_days <= 0:
        return float(TRADING_DAYS_PER_YEAR)
    return float((len(stamps) - 1) / (span_days / 365.25))


def annualised_volatility(
    returns: pd.Series, periods_per_year: Optional[float] = None
) -> Optional[float]:
    """Annualised standard deviation of periodic returns, as a fraction.

    [A98 gap, 2026-08-19] The Risk screen has always had a Volatility column
    and BacktestMetrics has never had the field behind it, so every row
    rendered an em dash. It is the denominator Sharpe already divides by --
    the engine was computing it and throwing it away -- and it is the number
    that says whether a 0.63 Sharpe came from a calm 12%/yr book or a wild
    45%/yr one, which Sharpe alone cannot.

    None (not zero) below two observations or on a flat curve, matching
    sharpe_ratio: an unmeasurable volatility and a genuinely riskless one
    are different facts.
    """
    if len(returns) < 2:
        return None
    std = returns.std()
    if pd.isna(std) or std < _NEAR_ZERO_STD:
        return None
    ppy = _TRADING_DAYS_PER_YEAR if periods_per_year is None else periods_per_year
    return float(std * (ppy**0.5))


def sharpe_ratio(
    returns: pd.Series, periods_per_year: Optional[float] = None
) -> Optional[float]:
    """Annualized daily-return Sharpe (rf=0). None with < 2 return
    observations or zero volatility (division-by-zero guard) — matches
    sortino_ratio's None-on-insufficient-data convention. 2026-07-26
    (REV6 wiring): the required scalar input to overfit_checks.
    deflated_sharpe_ratio; BacktestMetrics had no Sharpe field before this.

    Uses _NEAR_ZERO_STD (not `== 0`) since a degenerate (no-trade, flat
    equity) run's std comes out as float noise like 1e-16, not exact
    zero — an exact-equality guard let a meaningless mean-noise/std-noise
    ratio (e.g. -0.32 on a 0-trade run) through as if it were a real
    Sharpe (found 2026-07-26 auditing the B4 technical template)).

    periods_per_year defaults to the 252-day daily basis this function has
    always used, so every existing caller is unchanged. Pass
    infer_periods_per_year(curve.index) for a curve that is not daily --
    per-rebalance momentum curves in particular."""
    if len(returns) < 2:
        return None
    std = returns.std()
    if pd.isna(std) or std < _NEAR_ZERO_STD:
        return None
    ppy = _TRADING_DAYS_PER_YEAR if periods_per_year is None else periods_per_year
    return float(returns.mean() / std * (ppy**0.5))


def sortino_ratio(
    returns: pd.Series, periods_per_year: float = TRADING_DAYS_PER_YEAR,
) -> Tuple[Optional[float], Optional[str]]:
    """Like Sharpe but only penalizes downside deviation — recommended addition
    (BacktestUmbrellaPlan.md Truthful Review #8) since small/mid-cap Indian
    equity strategies have fat left tails that Sharpe alone under-penalizes.

    Returns (value, none_reason) — REV19 (2026-07-21 review): a bare `None`
    doesn't distinguish "genuinely no downside, excellent run" from "too few
    observations to compute a real ratio"; none_reason makes that explicit
    for any caller aggregating many runs.
    """
    if len(returns) < 2:
        return None, "insufficient_returns"
    downside = returns[returns < 0]
    if len(downside) == 0:
        return None, "no_downside_periods"
    downside_std = downside.std()
    if np.isnan(downside_std) or downside_std < _NEAR_ZERO_STD:
        return None, "zero_downside_std"
    mean_return = returns.mean()
    return float((mean_return * periods_per_year) / (downside_std * np.sqrt(periods_per_year))), None


def calmar_ratio(cagr_value: Optional[float], mdd: float) -> Tuple[Optional[float], Optional[str]]:
    """Returns (value, none_reason) — see sortino_ratio's docstring for why."""
    if cagr_value is None:
        return None, "no_cagr"
    if abs(mdd) < _NEAR_ZERO_STD:
        return None, "zero_or_undefined_drawdown"
    return cagr_value / abs(mdd), None


def win_rate_and_profit_factor(trade_pnls: List[float]) -> Tuple[Optional[float], Optional[float]]:
    if not trade_pnls:
        return None, None
    wins = [p for p in trade_pnls if p > 0]
    losses = [p for p in trade_pnls if p < 0]
    win_rate = len(wins) / len(trade_pnls)
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else None
    return win_rate, profit_factor


def turnover_ratio(trade_values: List[float], avg_portfolio_value: float) -> Optional[float]:
    """Sum of (buy value + sell value) across the run / average portfolio value —
    the second of the two churn definitions pinned down in BacktestUmbrellaPlan.md
    Truthful Review #11 (n_distinct_tickers_traded is the other; both are reported)."""
    if avg_portfolio_value <= 0:
        return None
    return sum(trade_values) / avg_portfolio_value


def benchmark_metrics(
    strategy_cagr: Optional[float], benchmark_equity_curve: Optional[pd.Series],
    start_date: DateLike, end_date: DateLike, index_ohlcv_min_date: Optional[date] = None,
) -> Tuple[Optional[float], Optional[float], str]:
    """
    Returns (benchmark_cagr, excess_return, status).

    The 2026-07-20 note here recorded index_ohlcv as holding real Nifty
    history only from 2023-07-03, and this function hard-coded that date as
    a floor: any run starting earlier got (None, None,
    "insufficient_benchmark_history") REGARDLESS of whether real benchmark
    data covered it.

    [BUG FIX 2026-08-08] That floor is stale. index_ohlcv now holds Nifty
    500 continuously from 2012-03-13 — 3,549 distinct trading dates with no
    gap wider than 7 days (verified directly against the table). The cutoff
    was therefore silently nulling benchmark_cagr/excess_return for every
    run starting before mid-2023: a 5-year 2021-start Technical sweep
    reported NO benchmark comparison at all, on 46 strategies, even though
    the full Nifty 500 series for that window was sitting in the DB.

    The cutoff was also redundant as a safety mechanism. The only source of
    benchmark_equity_curve is BacktestEngine._build_benchmark_curve(), which
    already returns None when the index has no real overlap with the run's
    dates, and the None branch below already reports
    "insufficient_benchmark_history". Real coverage is thus decided by the
    data itself rather than by a date constant that has to be maintained by
    hand — so the default is now None (no floor), and CLAUDE.md Absolute
    Rule 6 still holds: absent real index data, this returns None, never a
    synthetic benchmark.

    index_ohlcv_min_date is retained (default None = no floor) so a caller
    with a genuine reason to assert a coverage floor can still pass one.
    """
    if benchmark_equity_curve is None:
        return None, None, "insufficient_benchmark_history"
    if index_ohlcv_min_date is not None and pd.Timestamp(start_date) < pd.Timestamp(index_ohlcv_min_date):
        return None, None, "insufficient_benchmark_history"
    bench_cagr = calendar_cagr(
        benchmark_equity_curve.iloc[0], benchmark_equity_curve.iloc[-1], start_date, end_date
    )
    if bench_cagr is None or strategy_cagr is None:
        return bench_cagr, None, "insufficient_benchmark_history"
    return bench_cagr, strategy_cagr - bench_cagr, "ok"


# ---------------------------------------------------------------------------
# T13 -- consistency and trade-quality metrics, in-engine
#
# Momentum computed these; the orchestrator channels (Technical, Fundamental,
# ML) did not, so a Technical strategy could only ever be compared with a
# Momentum one on the handful of metrics both happened to produce. They were
# available for Technical only post-hoc, from trade-book CSVs, using a
# DIFFERENT definition -- which is worse than not having them, because the
# columns line up and invite a comparison that is not valid.
#
# The definitions below deliberately match backtest/momentum_metrics.py:
# calendar-stepped rolling windows reporting ANNUALISED CAGR. Everything here
# is a FRACTION (0.243 = 24.3%/yr), matching the rest of BacktestMetrics --
# the Technical comparison report uses percentages, and that boundary is
# exactly where a unit error becomes a wrong number on a screen.
# ---------------------------------------------------------------------------


#: Window lengths reported for every run. 2/3/4/5 matches what the Technical
#: comparison report and the Momentum dynamic report already publish, so the
#: two remain directly comparable.
ROLLING_WINDOW_YEARS = (2, 3, 4, 5)


def rolling_window_cagrs(
    equity_curve: pd.Series, window_years: float, step_months: int = 3,
) -> List[float]:
    """Every window_years-long window's ANNUALISED return, as fractions.

    Window starts step by CALENDAR time, not row index. Stepping by index is a
    bug this project has already shipped once: a quarterly-rebalance curve has
    ~63 trading days between rows, so index stepping under-sampled by ~60x and
    produced 1-2 windows over a 16-year backtest.

    The tail shorter than one full window is dropped rather than padded. A
    partial window is not a short-dated return, it is no return at all, and
    extrapolating one would invent the strategy's most recent performance --
    the figure a reader trusts most.
    """
    if equity_curve is None or len(equity_curve) < 2 or window_years <= 0:
        return []
    curve = equity_curve.dropna().sort_index()
    if len(curve) < 2:
        return []

    window_delta = pd.DateOffset(years=int(window_years))
    step_delta = pd.DateOffset(months=step_months)

    out: List[float] = []
    cursor = curve.index[0]
    last = curve.index[-1]
    while cursor <= last:
        starts = curve[curve.index >= cursor]
        if starts.empty:
            break
        start_ts, start_value = starts.index[0], float(starts.iloc[0])
        ends = curve[curve.index >= start_ts + window_delta]
        if ends.empty:
            break
        end_ts, end_value = ends.index[0], float(ends.iloc[0])
        if start_value > 0 and end_value > 0:
            years = (end_ts - start_ts).days / 365.25
            if years > 0:
                out.append((end_value / start_value) ** (1.0 / years) - 1.0)
        cursor = cursor + step_delta
    return out


def rolling_window_summary(
    equity_curve: pd.Series, window_years: float, step_months: int = 3,
) -> Dict[str, Optional[float]]:
    """min/median/max annualised return across every rolling window, plus the
    share that were positive -- the consistency question ("does this work
    repeatedly?") rather than the headline one ("what did it return?")."""
    values = rolling_window_cagrs(equity_curve, window_years, step_months)
    if not values:
        return {
            "min_cagr": None, "median_cagr": None, "max_cagr": None,
            "positive_share": None, "n_windows": 0,
        }
    return {
        "min_cagr": float(min(values)),
        "median_cagr": float(np.median(values)),
        "max_cagr": float(max(values)),
        "positive_share": sum(1 for v in values if v > 0) / len(values),
        "n_windows": len(values),
    }


def financial_year_label(ts: DateLike) -> str:
    """Indian financial year: 1 April - 31 March. FY2021 starts 2020-04-01."""
    ts = pd.Timestamp(ts)
    return f"FY{ts.year + 1}" if ts.month >= 4 else f"FY{ts.year}"


def fy_returns(equity_curve: pd.Series) -> List[Dict[str, Any]]:
    """Per-financial-year return as a FRACTION, from the equity curve.

    A year's return is measured from the equity at the previous FY's close, so
    consecutive years chain to the whole-period return. The first FY is
    measured from the curve's own start, which makes it a PARTIAL year unless
    the run happens to begin on 1 April -- flagged as such, because a stub
    period presented as a year drags every "share of positive years" figure.
    """
    if equity_curve is None or len(equity_curve) < 2:
        return []
    curve = equity_curve.dropna().sort_index()
    if len(curve) < 2:
        return []

    frame = pd.DataFrame({"equity": curve.values}, index=pd.to_datetime(curve.index))
    frame["fy"] = [financial_year_label(ts) for ts in frame.index]

    out: List[Dict[str, Any]] = []
    prev_close: Optional[float] = None
    first_fy = frame["fy"].iloc[0]
    for fy, group in frame.groupby("fy", sort=True):
        open_value = prev_close if prev_close is not None else float(group["equity"].iloc[0])
        close_value = float(group["equity"].iloc[-1])
        ret = (close_value / open_value - 1.0) if open_value > 0 else None
        out.append({
            "fy_label": fy,
            "return_pct": ret,
            # True when the run started mid-year, so this is not a full year's
            # performance and must not be counted as one.
            "partial": bool(fy == first_fy and frame.index[0].month != 4),
        })
        prev_close = close_value
    return out


def reconstruct_pre_tax_curve(
    equity_curve: pd.Series, tax_ledger: Optional[List[Dict[str, Any]]],
) -> Optional[pd.Series]:
    """The equity curve this run would have had if tax were never charged.

    Tax leaves the portfolio as a dated cash outflow, so adding each payment
    back from its payment date onwards recovers the pre-tax path exactly --
    for the SAME sequence of trades.

    WHAT THIS DOES NOT DO: re-simulate. A portfolio that kept the tax money
    would have had more cash and might have taken positions this run could not
    afford, so the reconstruction is the honest lower bound on a pre-tax run,
    not a substitute for one. It is reported as `cagr_other_basis` rather than
    as an independent result for exactly that reason -- the alternative,
    running the whole backtest twice, doubles every sweep's cost to answer a
    question this arithmetic answers to within position-sizing effects.
    """
    if equity_curve is None or len(equity_curve) == 0 or not tax_ledger:
        return None
    payments = [
        (pd.Timestamp(row.get("fy_end")), float(row.get("paid") or 0.0))
        for row in tax_ledger
        if row.get("paid") and row.get("fy_end")
    ]
    if not payments:
        return None
    curve = equity_curve.copy()
    add_back = pd.Series(0.0, index=curve.index)
    for when, amount in payments:
        add_back.loc[add_back.index >= when] += amount
    return curve + add_back


def apply_tax_to_curve(
    equity_curve: pd.Series, tax_ledger: Optional[List[Dict[str, Any]]],
) -> Optional[pd.Series]:
    """The mirror of reconstruct_pre_tax_curve: the curve a PRE-tax run would
    have had if each FY's tax had been paid on the day it fell due.

    [FIX 2026-08-19] A pre-tax run used to get its post-tax basis by
    subtracting the whole run's cumulative FY tax from the LAST point of the
    curve. Over a 17-year, 58-trades-a-year book that lump is a large
    multiple of any single year's tax, so the result was not a post-tax
    figure at all -- it dragged one strategy's reported CAGR from 19.0%/yr
    down to 4.9%/yr and made the "pre-tax" column read LOWER than the
    post-tax one. Tax is an annual event; subtracting it from its own
    payment date onwards is what the post-tax path already does.

    Same caveat as the reconstruction in the other direction: this does not
    re-simulate. A book that had paid tax each year would have had less cash
    and might have skipped positions, so this is the honest upper bound on a
    post-tax run, not a substitute for one.
    """
    if equity_curve is None or len(equity_curve) == 0 or not tax_ledger:
        return None
    payments = [
        (pd.Timestamp(row.get("fy_end")), float(row.get("paid") or 0.0))
        for row in tax_ledger
        if row.get("paid") and row.get("fy_end")
    ]
    if not payments:
        return None
    curve = equity_curve.copy()
    take_out = pd.Series(0.0, index=curve.index)
    for when, amount in payments:
        take_out.loc[take_out.index >= when] += amount
    return curve - take_out


def churn_per_year(
    n_trades: Optional[int], start_date: DateLike, end_date: DateLike,
) -> Optional[float]:
    """Round-trips per year -- what the strategy costs to run, and the figure
    that decides whether a pre-tax edge survives contact with STCG."""
    if not n_trades:
        return None
    years = (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days / 365.25
    return None if years <= 0 else float(n_trades) / years


def avg_winner_loser(
    trade_returns_pct: Optional[List[float]],
) -> Tuple[Optional[float], Optional[float]]:
    """Mean return of winning and of losing trades, as fractions.

    These are TRADE OUTCOMES, not rates: a 4% gain over a three-day hold is
    4%, not 380%/yr. Annualising them is meaningless and the rate rule in
    AGENTS.md explicitly exempts them.
    """
    if not trade_returns_pct:
        return None, None
    wins = [r for r in trade_returns_pct if r is not None and r > 0]
    losses = [r for r in trade_returns_pct if r is not None and r < 0]
    return (
        float(np.mean(wins)) if wins else None,
        float(np.mean(losses)) if losses else None,
    )



def _equity_curve_to_series(equity_curve: Optional[pd.Series]) -> List[Dict[str, Any]]:
    """pd.Series(index=date, value=equity) -> [{"date": "YYYY-MM-DD", "equity": float}].

    NaNs are dropped rather than zero-filled: a date the portfolio could not
    be marked on is missing data, and zero-filling it draws a cliff to zero
    and back, which reads as a total loss that never happened.
    """
    if equity_curve is None or len(equity_curve) == 0:
        return []
    out: List[Dict[str, Any]] = []
    for idx, value in equity_curve.items():
        if value is None or not np.isfinite(float(value)):
            continue
        out.append({
            "date": idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx),
            "equity": round(float(value), 2),
        })
    return out


def compute_metrics(
    equity_curve: pd.Series,
    # [(date_str, amount), ...] — what the INVESTOR paid in (negative) and
    # took out (positive) DURING the run: initial capital, SIP injections,
    # annual-reset withdrawals. Do NOT include the closing liquidation; it is
    # appended from the equity curve by _xirr_cash_flows, so that every caller
    # gets it and none can double it.
    cash_flows: List[Tuple[str, float]],
    trade_pnls: List[float],
    trade_values: List[float],
    distinct_tickers: List[str],
    start_date: DateLike, end_date: DateLike,
    total_contributed: float,
    benchmark_equity_curve: Optional[pd.Series] = None,
    cash_position_series: Optional[List[Dict[str, Any]]] = None,
    holding_days: Optional[List[float]] = None,
    trade_returns_pct: Optional[List[float]] = None,
    n_open_positions: int = 0,
    holding_days_all: Optional[List[float]] = None,
    benchmark_index_name: Optional[str] = None,
    # A86: the run's tax ledger, so both bases come from one execution.
    tax_ledger: Optional[List[Dict[str, Any]]] = None,
    deduct_tax_annually: bool = False,
) -> BacktestMetrics:
    """
    Single entry point every adapter's backtest/walk-forward run calls once
    at the end of a run to produce the standardized metrics record (CAGR,
    XIRR, final capital, churn, win rate, max drawdown, cash position —
    the exact set the user specified, plus Sortino/Calmar per Truthful
    Review #8).

    trade_returns_pct : optional (2026-08-01) — per-closed-trade % return
        (e.g. [t.pnl_pct * 100 for t in portfolio.trades], net-of-cost —
        deliberately NOT recomputed from raw buy/sell prices the way
        Momentum's trade_quality_metrics does, since Trade.pnl_pct already
        reflects real transaction costs). Feeds n_outlier_trades/
        max_abs_return_zscore via the same population-z-score math
        Momentum's trade-quality outlier detection uses
        (return_population_zscores, defined above) — a second,
        independent defense against fabricated/stale-price trades, same
        motivation as the Momentum fix. None (omit) leaves those fields
        None, unaffected for any caller not yet passing it.
    n_open_positions : still-open position count at run end — added to
        len(trade_pnls) for total_trades (open + closed), since n_trades
        itself stays closed-only for backward compatibility.
    holding_days_all : optional — holding-period days across BOTH closed
        trades and still-open positions (as-of-run-end for the latter),
        feeding avg_trade_duration_days. Falls back to `holding_days`
        (closed-only, same value as avg_days_held) when omitted — no
        behavior change for any existing caller.
    """
    starting_capital = equity_curve.iloc[0] if len(equity_curve) else 0.0
    ending_value = equity_curve.iloc[-1] if len(equity_curve) else 0.0
    returns = equity_curve.pct_change().dropna() if len(equity_curve) > 1 else pd.Series(dtype=float)

    cagr_value = calendar_cagr(starting_capital, ending_value, start_date, end_date)

    # [FIX 2026-08-19] XIRR was computed on the CONTRIBUTION side only: the
    # liquidation inflow at end_date was never appended, so the solver was
    # asked for the rate that zeroes a series of outflows plus a handful of
    # FY tax events. That has no economic meaning -- it returned None when
    # every flow shared a sign, and an arbitrary negative root when the tax
    # events happened to bracket one (a run with a 21.4%/yr CAGR reported
    # XIRR of -3.3%/yr). The terminal value is what the investor would get
    # back on that date, and XIRR is undefined without it.
    xirr_flows = _xirr_cash_flows(cash_flows, end_date, float(ending_value))
    xirr_value = xirr(xirr_flows) if len(xirr_flows) >= 2 else None
    mdd = max_drawdown(equity_curve)
    win_rate, profit_factor = win_rate_and_profit_factor(trade_pnls)
    bench_cagr, excess_return, bench_status = benchmark_metrics(
        cagr_value, benchmark_equity_curve, start_date, end_date
    )
    sharpe_value = sharpe_ratio(returns)
    volatility_value = annualised_volatility(returns)
    sortino_value, sortino_reason = sortino_ratio(returns)
    calmar_value, calmar_reason = calmar_ratio(cagr_value, mdd)

    if trade_returns_pct:
        z_result = return_population_zscores(trade_returns_pct)
        n_outlier_trades = z_result["n_outliers"]
        max_abs_return_zscore = z_result["max_abs_zscore"]
    else:
        n_outlier_trades = None
        max_abs_return_zscore = None

    _avg_win, _avg_loss = avg_winner_loser(trade_returns_pct)

    # A86 -- state the basis, and supply the other one from the same run.
    _tax_basis = "post_tax" if deduct_tax_annually else "pre_tax"
    _total_tax = (
        sum(float(r.get("paid") or 0.0) for r in tax_ledger) if tax_ledger else None
    )
    # A86 both ways round. A pre-tax run's other basis is its post-tax path,
    # which until 2026-08-19 was not computed at all here -- the engine
    # instead docked the whole run's tax off the final equity point and
    # reported THAT as the headline, so `cagr` on a pre-tax run was neither
    # basis. See apply_tax_to_curve.
    _other_curve = (
        reconstruct_pre_tax_curve(equity_curve, tax_ledger)
        if deduct_tax_annually
        else apply_tax_to_curve(equity_curve, tax_ledger)
    )
    _other_cagr = (
        calendar_cagr(
            float(_other_curve.iloc[0]), float(_other_curve.iloc[-1]), start_date, end_date
        )
        if _other_curve is not None and len(_other_curve) >= 2
        else None
    )

    return BacktestMetrics(
        cagr=cagr_value,
        cagr_trading_day_legacy=trading_day_cagr(equity_curve),
        xirr=xirr_value,
        final_capital=float(ending_value),
        total_contributed=total_contributed,
        max_drawdown=mdd,
        win_rate=win_rate,
        profit_factor=profit_factor,
        sharpe=sharpe_value,
        volatility=volatility_value,
        sortino=sortino_value,
        sortino_none_reason=sortino_reason,
        calmar=calmar_value,
        calmar_none_reason=calmar_reason,
        n_distinct_tickers_traded=len(set(distinct_tickers)),
        turnover_ratio=turnover_ratio(trade_values, float(equity_curve.mean()) if len(equity_curve) else 0.0),
        n_trades=len(trade_pnls),
        benchmark_cagr=bench_cagr,
        excess_return=excess_return,
        benchmark_status=bench_status,
        benchmark_index_name=benchmark_index_name,
        rolling_returns={
            f"{w}y": rolling_window_summary(equity_curve, w)
            for w in ROLLING_WINDOW_YEARS
        },
        fy_returns=fy_returns(equity_curve),
        churn_per_year=churn_per_year(len(trade_pnls), start_date, end_date),
        avg_winner_pct=_avg_win,
        avg_loser_pct=_avg_loss,
        tax_basis=_tax_basis,
        total_tax_paid=_total_tax,
        cagr_other_basis=_other_cagr,
        cash_position_series=cash_position_series or [],
        # Built from the same `equity_curve` this function already receives
        # and measures everything else off, so the persisted series can never
        # disagree with the CAGR/drawdown/Sharpe computed beside it.
        equity_curve_series=_equity_curve_to_series(equity_curve),
        avg_days_held=(float(np.mean(holding_days)) if holding_days else None),
        total_trades=len(trade_pnls) + n_open_positions,
        avg_trade_duration_days=(
            float(np.mean(holding_days_all)) if holding_days_all
            else (float(np.mean(holding_days)) if holding_days else None)
        ),
        n_outlier_trades=n_outlier_trades,
        max_abs_return_zscore=max_abs_return_zscore,
    )


__all__ = [
    "BacktestMetrics", "calendar_cagr", "trading_day_cagr", "max_drawdown", "sharpe_ratio", "sortino_ratio",
    "calmar_ratio", "win_rate_and_profit_factor", "turnover_ratio", "benchmark_metrics",
    "compute_metrics", "churn_factor", "xirr", "infer_periods_per_year",
    "total_return", "cagr", "sharpe_sortino_calmar", "win_rate", "avg_winner_return_pct",
    "avg_loser_return_pct", "rolling_window_returns", "rolling_window_summary_from_capital_curve",
    "income_mode_summary", "trade_quality_metrics", "trade_cagr",
]


# ---------------------------------------------------------------------------
# [H4, 2026-08-18, UnifiedGeneratorRefactorPlan.md] Momentum-specific metric
# helpers, moved here verbatim from backtest/momentum_metrics.py (deleted
# with MomentumBacktester) -- see that module's original docstring for
# provenance (FeatureBacklog.md ML38). These operate on
# starting_capital/ending_value scalars and MomentumBacktestResult-shaped
# dict lists, NOT on the pd.Series equity curves the rest of this module
# uses -- a genuinely different calling convention, not a duplicate, which
# is why rolling_window_summary above and this section's
# rolling_window_summary_from_capital_curve coexist under different names
# rather than colliding.
# ---------------------------------------------------------------------------


def total_return(starting_capital: float, ending_value: float) -> float:
    """Net-of-cost total return over the whole run, e.g. 0.42 = +42%."""
    if starting_capital <= 0:
        raise ValueError("starting_capital must be positive")
    if ending_value <= 0:
        raise ValueError("ending_value must be positive to annualize")
    return (ending_value / starting_capital) - 1.0


def cagr(starting_capital: float, ending_value: float, start_date: str, end_date: str) -> float:
    """Compounded annual growth rate over the real elapsed calendar time
    between start_date and end_date."""
    if starting_capital <= 0:
        raise ValueError("starting_capital must be positive")
    if ending_value <= 0:
        raise ValueError("ending_value must be positive to annualize")
    years = (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days / 365.25
    if years <= 0:
        raise ValueError("end_date must be after start_date")
    return float((ending_value / starting_capital) ** (1.0 / years) - 1.0)


def sharpe_sortino_calmar(
    equity_curve: List[Dict[str, Any]], cagr_value: Optional[float],
) -> Dict[str, Optional[float]]:
    """Sharpe/Sortino/Calmar for a MomentumBacktestResult-shaped equity
    curve ([{"date","total_value"}], one row per rebalance/day -- NOT a
    pd.Series) -- computed straight from the equity curve every sweep/
    experimentation run already has, no fresh backtest needed.

    Infers real periods-per-year from the curve's own average calendar-day
    spacing (NOT a hardcoded 252) so a weekly/biweekly/monthly/bimonthly/
    quarterly momentum rebalance schedule annualizes correctly -- see
    tests/quality/test_one_measurement_layer.py's docstring for why a
    hardcoded 252 was the entire reason a second Sharpe implementation
    was ever written (2.46 vs 1.12 on the same weekly returns).

    cagr_value : this variant's already-computed calendar/365.25 CAGR
        (core.metrics.cagr) -- reused for Calmar rather than recomputed,
        for consistency with the CAGR already reported elsewhere in the
        same variant dict.
    """
    if len(equity_curve) < 3:
        return {"sharpe": None, "sortino": None, "calmar": None}

    dates = pd.to_datetime([e["date"] for e in equity_curve])
    values = np.array([e["total_value"] for e in equity_curve], dtype=float)

    period_returns = pd.Series(values[1:] / values[:-1] - 1.0)
    period_returns = period_returns[np.isfinite(period_returns)]
    if len(period_returns) < 2:
        return {"sharpe": None, "sortino": None, "calmar": None}

    span_days = (dates[-1] - dates[0]).days
    periods_per_year = (len(dates) - 1) / max(span_days / 365.25, 1e-9)

    std = period_returns.std()
    sharpe = (
        float(period_returns.mean() / std * (periods_per_year**0.5))
        if pd.notna(std) and std > _NEAR_ZERO_STD else None
    )

    downside = period_returns[period_returns < 0]
    if len(downside) == 0:
        sortino = None
    else:
        downside_std = downside.std()
        sortino = (
            float((period_returns.mean() * periods_per_year) / (downside_std * (periods_per_year**0.5)))
            if pd.notna(downside_std) and downside_std > _NEAR_ZERO_STD else None
        )

    running_max = np.maximum.accumulate(values)
    drawdown = (values - running_max) / running_max
    mdd = float(drawdown.min()) if len(drawdown) else 0.0
    calmar = (cagr_value / abs(mdd)) if (cagr_value is not None and abs(mdd) > _NEAR_ZERO_STD) else None

    return {"sharpe": sharpe, "sortino": sortino, "calmar": calmar, "max_drawdown": mdd}


def win_rate(transactions: List[Dict[str, Any]]) -> Optional[float]:
    """Fraction of CLOSED transactions that sold above their buy price.

    transactions : dicts with at least {"status", "buy_price", "sell_price"}.
    Open positions (status != "closed") are excluded. None if there are no
    closed transactions to judge.
    """
    closed = [t for t in transactions if t["status"] == "closed"]
    if not closed:
        return None
    wins = sum(1 for t in closed if t["sell_price"] is not None and t["sell_price"] > t["buy_price"])
    return wins / len(closed)


def _closed_trade_returns_pct(transactions: List[Dict[str, Any]]) -> List[float]:
    """Simple (non-annualized) per-trade % return for every closed
    transaction with real buy/sell prices."""
    out = []
    for t in transactions:
        if t["status"] != "closed" or t["sell_price"] is None or not t["buy_price"]:
            continue
        out.append((t["sell_price"] / t["buy_price"] - 1.0) * 100.0)
    return out


def avg_winner_return_pct(transactions: List[Dict[str, Any]]) -> Optional[float]:
    """Mean simple % return across closed trades that sold ABOVE their buy
    price. None if there are no winning closed trades."""
    winners = [r for r in _closed_trade_returns_pct(transactions) if r > 0]
    return sum(winners) / len(winners) if winners else None


def avg_loser_return_pct(transactions: List[Dict[str, Any]]) -> Optional[float]:
    """Mean simple % return across closed trades that sold AT/BELOW their
    buy price. Reported as a negative number. None if there are no losing
    closed trades."""
    losers = [r for r in _closed_trade_returns_pct(transactions) if r <= 0]
    return sum(losers) / len(losers) if losers else None


def rolling_window_returns(
    equity_curve: List[Dict[str, Any]], window_years: int, step_months: int = 3,
) -> List[Dict[str, Any]]:
    """Every window_years-long rolling-window CAGR from a
    MomentumBacktestResult-shaped equity curve ([{"date","total_value"}]),
    window start stepped every step_months (calendar time, not row index)."""
    if len(equity_curve) < 2:
        return []
    df = pd.DataFrame(equity_curve)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    window_delta = pd.DateOffset(years=window_years)
    step_delta = pd.DateOffset(months=step_months)

    out = []
    cursor = df["date"].iloc[0]
    last_date = df["date"].iloc[-1]
    while cursor <= last_date:
        start_candidates = df[df["date"] >= cursor]
        if start_candidates.empty:
            break
        start_row = start_candidates.iloc[0]
        start_date, start_value = start_row["date"], start_row["total_value"]

        target_end = start_date + window_delta
        end_candidates = df[df["date"] >= target_end]
        if end_candidates.empty:
            break
        end_row = end_candidates.iloc[0]
        end_date, end_value = end_row["date"], end_row["total_value"]
        if start_value > 0 and end_value > 0:
            years = (end_date - start_date).days / 365.25
            cagr_pct = ((end_value / start_value) ** (1.0 / years) - 1.0) * 100.0
            out.append({
                "window_start": start_date.date().isoformat(),
                "window_end": end_date.date().isoformat(),
                "cagr_pct": cagr_pct,
            })
        cursor = cursor + step_delta
    return out


def rolling_window_summary_from_capital_curve(
    equity_curve: List[Dict[str, Any]], window_years: int,
) -> Dict[str, Optional[float]]:
    """min/median/max CAGR across all rolling window_years windows, from a
    MomentumBacktestResult-shaped equity curve -- see rolling_window_summary
    above for the pd.Series-based equivalent every other channel uses."""
    windows = rolling_window_returns(equity_curve, window_years)
    if not windows:
        return {"min_cagr_pct": None, "median_cagr_pct": None, "max_cagr_pct": None, "n_windows": 0}
    values = [w["cagr_pct"] for w in windows]
    return {
        "min_cagr_pct": min(values),
        "median_cagr_pct": float(np.median(values)),
        "max_cagr_pct": max(values),
        "n_windows": len(values),
    }


def income_mode_summary(
    capital_resets: List[Dict[str, Any]], target_capital: float,
) -> Dict[str, Optional[float]]:
    """Headline figures for annual_capital_reset_target ("income mode").
    capital_resets: [{"date","fy_label","pre_reset_value","withdrawal",
    "injection"}], one entry per FY boundary."""
    n_years = len(capital_resets)
    if n_years == 0 or target_capital <= 0:
        return {
            "total_withdrawn": None, "total_injected": None,
            "avg_annual_yield_pct": None, "years_survived_pct": None, "n_years": 0,
        }
    total_withdrawn = sum(c["withdrawal"] for c in capital_resets)
    total_injected = sum(c["injection"] for c in capital_resets)
    net_per_year = [(c["withdrawal"] - c["injection"]) / target_capital * 100.0 for c in capital_resets]
    n_survived = sum(1 for c in capital_resets if c["withdrawal"] > 0)
    return {
        "total_withdrawn": total_withdrawn,
        "total_injected": total_injected,
        "avg_annual_yield_pct": sum(net_per_year) / n_years,
        "years_survived_pct": n_survived / n_years * 100.0,
        "n_years": n_years,
    }


def trade_quality_metrics(
    transactions: List[Dict[str, Any]], zscore_outlier_threshold: float = 3.0,
) -> Dict[str, Any]:
    """Data-quality / summary stats across the FULL transaction ledger
    (open + closed), plus a per-trade return z-score used to auto-flag
    price artifacts.

    Mutates each transaction dict in place, adding "return_pct" and
    "return_zscore".
    """
    total_trades = len(transactions)
    durations = [t["holding_days"] for t in transactions if t.get("holding_days") is not None]
    avg_trade_duration_days = (sum(durations) / len(durations)) if durations else None

    returns: List[Optional[float]] = []
    for t in transactions:
        if t.get("sell_price") is not None and t.get("buy_price"):
            r = (t["sell_price"] / t["buy_price"] - 1) * 100
        else:
            r = None
        t["return_pct"] = r
        returns.append(r)

    z_result = return_population_zscores(returns, zscore_outlier_threshold)
    for t, z in zip(transactions, z_result["zscores"]):
        t["return_zscore"] = z

    return {
        "total_trades": total_trades,
        "avg_trade_duration_days": avg_trade_duration_days,
        "n_outlier_trades": z_result["n_outliers"],
        "max_abs_return_zscore": z_result["max_abs_zscore"],
    }


def aggregate_by_sector(
    holdings: List[Dict[str, Any]],
    sector_lookup: Optional[Dict[str, str]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Aggregate holding-level metrics by sector.

    Per Phase 11, this enables sector-breakdown reporting for R11/R12 backtest results.

    Args:
        holdings: List of holding dicts (from backtest results), each with
                 'ticker', 'entry_date', 'exit_date', 'return_pct', etc.
        sector_lookup: Optional dict mapping ticker -> sector (e.g., {"INFY": "IT"}).
                      If None, all holdings are grouped under a default "Unknown" sector.

    Returns:
        Dict keyed by sector, with aggregated metrics (avg return, trade count, etc.).
    """
    if not holdings or sector_lookup is None:
        # Fallback: return empty dict or aggregate all as "Unknown"
        all_returns = [h.get("return_pct") for h in holdings if h.get("return_pct") is not None]
        if all_returns:
            return {
                "Unknown": {
                    "avg_return_pct": float(np.mean(all_returns)),
                    "n_trades": len(holdings),
                }
            }
        return {}

    result: Dict[str, Dict[str, Any]] = {}
    for holding in holdings:
        ticker = holding.get("ticker")
        sector = sector_lookup.get(ticker, "Unknown") if ticker else "Unknown"

        if sector not in result:
            result[sector] = {"n_trades": 0, "returns": []}

        result[sector]["n_trades"] += 1
        ret = holding.get("return_pct")
        if ret is not None:
            result[sector]["returns"].append(ret)

    # Compute aggregates
    for sector in result:
        returns = result[sector]["returns"]
        result[sector]["avg_return_pct"] = float(np.mean(returns)) if returns else None
        result[sector]["max_return_pct"] = float(np.max(returns)) if returns else None
        result[sector]["min_return_pct"] = float(np.min(returns)) if returns else None
        del result[sector]["returns"]  # Clean up temp list

    return result


def aggregate_by_regime(
    daily_returns: pd.Series,
    regime_labels: Optional[pd.Series] = None,
) -> Dict[str, Dict[str, Any]]:
    """Aggregate daily returns by regime (bull/bear/crash).

    Per Phase 11, this enables regime-breakdown reporting for R11/R12 backtest results.

    Args:
        daily_returns: pd.Series of daily portfolio returns, indexed by date.
        regime_labels: Optional pd.Series of regime labels (e.g., "bull", "bear", "crash"),
                      indexed by date. If None, all returns are grouped under "Unknown".

    Returns:
        Dict keyed by regime, with aggregated metrics (avg daily return, volatility, etc.).
    """
    if daily_returns.empty:
        return {}

    if regime_labels is None or regime_labels.empty:
        return {
            "Unknown": {
                "avg_daily_return": float(daily_returns.mean()),
                "volatility": float(daily_returns.std()),
                "n_days": len(daily_returns),
            }
        }

    result: Dict[str, Dict[str, Any]] = {}
    for regime in regime_labels.unique():
        mask = regime_labels == regime
        regime_returns = daily_returns[mask]

        if len(regime_returns) == 0:
            continue

        result[regime] = {
            "avg_daily_return": float(regime_returns.mean()),
            "volatility": float(regime_returns.std()),
            "n_days": len(regime_returns),
            "cumulative_return": float((1 + regime_returns).prod() - 1),
        }

    return result
