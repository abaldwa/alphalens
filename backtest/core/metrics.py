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

Reuses backtest/momentum_metrics.py's xirr()/churn_factor() rather than
reimplementing them (that module's bisection-based XIRR is
channel-agnostic already — it operates on a plain cash-flow list, no
momentum-specific assumptions).
"""

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

from backtest.momentum_metrics import churn_factor, return_population_zscores, xirr

TRADING_DAYS_PER_YEAR = 252

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
_NEAR_ZERO_STD = 1e-9


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
        if row.get("paid")
    ]
    if not payments:
        return None
    curve = equity_curve.copy()
    add_back = pd.Series(0.0, index=curve.index)
    for when, amount in payments:
        add_back.loc[add_back.index >= when] += amount
    return curve + add_back


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
    cash_flows: List[Tuple[str, float]],  # [(date_str, amount), ...] incl. initial capital, SIP, tax outflows
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
        (backtest.momentum_metrics.return_population_zscores) — a second,
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
    xirr_value = xirr(cash_flows) if len(cash_flows) >= 2 else None
    mdd = max_drawdown(equity_curve)
    win_rate, profit_factor = win_rate_and_profit_factor(trade_pnls)
    bench_cagr, excess_return, bench_status = benchmark_metrics(
        cagr_value, benchmark_equity_curve, start_date, end_date
    )
    sharpe_value = sharpe_ratio(returns)
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
    _other_curve = (
        reconstruct_pre_tax_curve(equity_curve, tax_ledger)
        if deduct_tax_annually
        else None
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
]
