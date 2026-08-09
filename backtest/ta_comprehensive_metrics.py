"""
backtest/ta_comprehensive_metrics.py

Owner: Platform / Backtest
Consumers: backtest/ta_comparison_report.py, scripts/run_ta_5year_backtest.py

The 5-year TA strategy comparison asks for figures the orchestrator's own
metrics block does not carry: returns bucketed by INDIAN TRADING YEAR
(1 Apr - 31 Mar), post-tax returns under the Indian capital-gains regime,
signals generated per month, and the average number of stocks held. All
four are derived here from artifacts the run already writes — the enriched
trade book CSV (backtest/export_trade_book.py) plus the report JSON's
cash_position_series — never recomputed from a re-simulation and never
estimated where a real number exists.

Why the trade book and not an equity curve: backtest_runs persists no
daily equity series (only metrics + the cash series), so a mark-to-market
yearly return would require re-pricing every open position — i.e. a second
simulation. Every figure below is instead computed on REALIZED trades,
which the trade book records exactly (buy/sale date, price, qty, pnl). The
consequence is stated explicitly in each function's docstring and carried
into the report output as `basis: "realized"` so a reader never mistakes a
realized-P&L year for a mark-to-market one.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import date as date_type, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

# Indian capital-gains regime applied to equity delivery trades, as of
# FY2025-26 (the last trading year in this comparison window):
#   - Short Term (holding <= 365 days): 20% flat on the gain (Sec 111A).
#   - Long Term  (holding  > 365 days): 12.5% on gains above a ₹1.25L
#     per-financial-year exemption (Sec 112A).
# The user's brief specified 20% STCG and "10% over ₹1L" LTCG — the older
# pre-Budget-2024 LTCG numbers. Both are computed and reported side by side
# rather than silently picking one, since which applies depends on the
# assessment year being modelled.
STCG_RATE = 0.20
LTCG_REGIMES = {
    # label: (rate, per-year exemption in INR)
    "ltcg_10pct_1L": (0.10, 100_000.0),
    "ltcg_12_5pct_1_25L": (0.125, 125_000.0),
}
LONG_TERM_HOLDING_DAYS = 365


def trading_year(d: date_type) -> str:
    """Indian trading/financial year label for a date: 1 Apr - 31 Mar,
    e.g. 2021-06-10 -> "FY2021-22", 2022-02-10 -> "FY2021-22"."""
    start = d.year if d.month >= 4 else d.year - 1
    return f"FY{start}-{str(start + 1)[-2:]}"


def load_trade_book(path: Path) -> pd.DataFrame:
    """Reads a run's trade log CSV into a typed frame. Only CLOSED trades
    (a real sale_date) are returned — an open-at-end position has no
    realized P&L and must not be counted as a zero-return trade."""
    df = pd.read_csv(path)
    for col in ("buy_date", "sale_date"):
        df[col] = pd.to_datetime(df[col], errors="coerce")
    df = df[df["sale_date"].notna() & df["buy_date"].notna()].copy()
    df["holding_days"] = (df["sale_date"] - df["buy_date"]).dt.days
    df["buy_value"] = df["qty"] * df["buy_price"]
    df["sale_value"] = df["qty"] * df["sale_price"]
    return df.reset_index(drop=True)


@dataclass
class YearBucket:
    trading_year: str
    n_trades: int
    realized_pnl_inr: float
    invested_inr: float
    return_pct: Optional[float]
    win_rate: Optional[float]
    avg_holding_days: Optional[float]


def yearly_returns(trades: pd.DataFrame) -> List[YearBucket]:
    """Realized P&L bucketed by the trading year the position was EXITED
    in (sale_date), with return_pct expressed against the capital actually
    deployed in that year's closed trades (sum of buy_value). A trade
    opened in FY2021-22 and closed in FY2022-23 belongs entirely to
    FY2022-23 — realized-basis accounting, see the module docstring."""
    if trades.empty:
        return []
    buckets: List[YearBucket] = []
    trades = trades.assign(_fy=[trading_year(d.date()) for d in trades["sale_date"]])
    for fy, grp in trades.groupby("_fy", sort=True):
        invested = float(grp["buy_value"].sum())
        pnl = float(grp["pnl_inr"].sum())
        buckets.append(
            YearBucket(
                trading_year=fy,
                n_trades=int(len(grp)),
                realized_pnl_inr=pnl,
                invested_inr=invested,
                return_pct=(pnl / invested) if invested > 0 else None,
                win_rate=float((grp["pnl_inr"] > 0).mean()),
                avg_holding_days=float(grp["holding_days"].mean()),
            )
        )
    return buckets


@dataclass
class TaxResult:
    regime: str
    short_term_gain_inr: float
    long_term_gain_inr: float
    stcg_tax_inr: float
    ltcg_tax_inr: float
    total_tax_inr: float
    pre_tax_pnl_inr: float
    post_tax_pnl_inr: float
    per_year: Dict[str, Dict[str, float]] = field(default_factory=dict)


def tax_liability(trades: pd.DataFrame, regime: str = "ltcg_12_5pct_1_25L") -> TaxResult:
    """Indian capital-gains tax on the realized trade set.

    Computed PER FINANCIAL YEAR (not on the whole 5-year total), because
    the LTCG exemption is a per-year allowance — pooling five years into
    one calculation would grant the exemption once instead of five times
    and overstate the liability. Losses are set off within their own
    bucket and year (short-term losses against short-term gains, long-term
    against long-term), which is the ordinary Indian treatment; a net loss
    in a year produces zero tax for that bucket and is NOT carried forward
    into the next year — carry-forward is a filing-level election this
    backtest has no basis to assume.
    """
    if regime not in LTCG_REGIMES:
        raise ValueError(f"unknown regime {regime!r} — one of {list(LTCG_REGIMES)}")
    ltcg_rate, ltcg_exemption = LTCG_REGIMES[regime]

    per_year: Dict[str, Dict[str, float]] = {}
    st_total = lt_total = stcg_tax = ltcg_tax = 0.0
    if not trades.empty:
        tagged = trades.assign(_fy=[trading_year(d.date()) for d in trades["sale_date"]])
        for fy, grp in tagged.groupby("_fy", sort=True):
            is_long = grp["holding_days"] > LONG_TERM_HOLDING_DAYS
            st_gain = float(grp.loc[~is_long, "pnl_inr"].sum())
            lt_gain = float(grp.loc[is_long, "pnl_inr"].sum())
            st_tax = max(st_gain, 0.0) * STCG_RATE
            lt_tax = max(lt_gain - ltcg_exemption, 0.0) * ltcg_rate
            per_year[fy] = {
                "short_term_gain_inr": st_gain,
                "long_term_gain_inr": lt_gain,
                "stcg_tax_inr": st_tax,
                "ltcg_tax_inr": lt_tax,
                "total_tax_inr": st_tax + lt_tax,
            }
            st_total += st_gain
            lt_total += lt_gain
            stcg_tax += st_tax
            ltcg_tax += lt_tax

    pre_tax = st_total + lt_total
    return TaxResult(
        regime=regime,
        short_term_gain_inr=st_total,
        long_term_gain_inr=lt_total,
        stcg_tax_inr=stcg_tax,
        ltcg_tax_inr=ltcg_tax,
        total_tax_inr=stcg_tax + ltcg_tax,
        pre_tax_pnl_inr=pre_tax,
        post_tax_pnl_inr=pre_tax - (stcg_tax + ltcg_tax),
    )


def holdings_profile(trades: pd.DataFrame) -> Dict[str, Any]:
    """Average/peak concurrent open positions, from the trade book's
    [buy_date, sale_date) intervals. Averaged over CALENDAR days spanned
    by the run (not trading days) — the trade book carries no calendar, so
    a trading-day denominator would have to be fabricated; the calendar
    average is a true, slightly conservative figure and is labelled as
    such in the output key name."""
    if trades.empty:
        return {"avg_concurrent_positions_calendar": None, "peak_concurrent_positions": None, "n_days_spanned": 0}
    start = trades["buy_date"].min().date()
    end = trades["sale_date"].max().date()
    delta = defaultdict(int)
    for row in trades.itertuples(index=False):
        delta[row.buy_date.date()] += 1
        delta[row.sale_date.date()] -= 1
    held = 0
    total = 0
    peak = 0
    day = start
    changes = dict(delta)
    n_days = (end - start).days + 1
    for _ in range(n_days):
        held += changes.get(day, 0)
        peak = max(peak, held)
        total += held
        day += timedelta(days=1)
    return {
        "avg_concurrent_positions_calendar": total / n_days if n_days else None,
        "peak_concurrent_positions": peak,
        "n_days_spanned": n_days,
    }


def signals_per_month(trades: pd.DataFrame) -> Dict[str, Any]:
    """Entry signals ACTED ON per month (one closed trade = one filled
    entry). This counts fills, not raw screener matches — a match the
    portfolio had no cash to take is not in the trade book. Named and
    reported as `entries_per_month` for exactly that reason."""
    if trades.empty:
        return {"avg_entries_per_month": None, "by_month": {}}
    months = trades["buy_date"].dt.to_period("M").astype(str)
    counts = months.value_counts().sort_index()
    span_months = trades["buy_date"].dt.to_period("M").nunique()
    return {
        "avg_entries_per_month": float(counts.sum() / span_months) if span_months else None,
        "by_month": {k: int(v) for k, v in counts.items()},
    }


def exit_reason_breakdown(trades: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    """Which exit rule actually closed each trade (stop / target / max-hold
    / rotation), with that reason's own hit rate and mean P&L — the direct
    read on whether a style's exit parameters did what they were meant to."""
    if trades.empty or "exit_reason" not in trades.columns:
        return {}
    out: Dict[str, Dict[str, float]] = {}
    for reason, grp in trades.groupby(trades["exit_reason"].fillna("unknown")):
        out[str(reason)] = {
            "n_trades": int(len(grp)),
            "share": float(len(grp) / len(trades)),
            "avg_pnl_pct": float(grp["pnl_pct"].mean()),
            "win_rate": float((grp["pnl_inr"] > 0).mean()),
        }
    return out


ROLLING_WINDOW_YEARS = (2, 3, 4, 5)


def rolling_returns(
    equity_curve: List[Dict[str, Any]], windows: tuple = ROLLING_WINDOW_YEARS,
) -> Dict[str, Any]:
    """Rolling N-year returns over the run's daily mark-to-market equity
    curve, for each N in `windows`.

    Unlike everything else in this module, this is TIME-WEIGHTED and
    mark-to-market, not realized: it compares portfolio value on date D to
    value on D minus N years, so open positions count at their marked value.
    That is the only honest way to answer "what would 3 years starting
    anywhere in this run have returned" — a realized-P&L version would
    silently omit whatever was still held at each window's end.

    Every trading day that has a real bar N years earlier starts one window
    (a daily-overlapping population, standard for rolling-return analysis).
    Windows are anchored on CALENDAR dates offset by N years, resolved to
    the nearest earlier trading day, so a "3-year return" spans three real
    calendar years regardless of holiday placement.

    Returns per-window: count, min/median/max, mean, and the share of
    windows that were positive — plus the annualized (CAGR-equivalent)
    median, since a raw 5-year total return isn't comparable to a 2-year one.
    Windows shorter than N years produce an empty result for that N rather
    than a partial-window figure.
    """
    if not equity_curve or len(equity_curve) < 2:
        return {}
    series = pd.Series(
        [float(p["equity"]) for p in equity_curve],
        index=pd.DatetimeIndex([pd.Timestamp(p["date"]) for p in equity_curve]),
    ).sort_index()

    out: Dict[str, Any] = {}
    for years in windows:
        # For each end date, the value N years before it — reindex with
        # method="ffill" resolves an offset landing on a holiday/weekend
        # back to the most recent real trading day, never interpolating.
        start_targets = series.index - pd.DateOffset(years=years)
        start_values = series.reindex(start_targets, method="ffill")
        valid = start_targets >= series.index[0]
        rets = []
        for end_value, start_value, ok in zip(series.to_numpy(), start_values.to_numpy(), valid):
            if not ok or start_value is None or pd.isna(start_value) or start_value <= 0:
                continue
            rets.append(end_value / start_value - 1.0)
        key = f"{years}y"
        if not rets:
            out[key] = {"n_windows": 0}
            continue
        s = pd.Series(rets)
        median = float(s.median())
        out[key] = {
            "n_windows": int(len(s)),
            "min": float(s.min()),
            "median": median,
            "max": float(s.max()),
            "mean": float(s.mean()),
            "positive_share": float((s > 0).mean()),
            # Annualized equivalent of the median window, so 2y/3y/4y/5y
            # figures sit on one comparable scale.
            "median_annualized": float((1.0 + median) ** (1.0 / years) - 1.0),
        }
    return out


def compute_comprehensive_metrics(
    report: Dict[str, Any], trade_log_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Everything above, assembled for ONE orchestrator run report.

    `report` is a parsed backtest/reports/orchestrator_*.json. The trade
    log defaults to the path the report itself recorded, so a caller
    normally passes only the report."""
    path = Path(trade_log_path or report.get("trade_log_path") or "")
    if not path.exists():
        raise FileNotFoundError(f"trade log not found for this run: {path}")
    trades = load_trade_book(path)

    run = report.get("run", {})
    metrics = report.get("metrics", {})
    initial_capital = float(run.get("initial_capital") or 0.0)
    realized_pnl = float(trades["pnl_inr"].sum()) if not trades.empty else 0.0

    taxes = {name: asdict(tax_liability(trades, regime=name)) for name in LTCG_REGIMES}

    return {
        "run_id": run.get("run_id"),
        "template_name": (run.get("config") or {}).get("template_name"),
        "strategy_id": run.get("strategy_id"),
        "start_date": run.get("start_date"),
        "end_date": run.get("end_date"),
        "initial_capital": initial_capital,
        "basis": "realized",
        "engine_metrics": {
            k: metrics.get(k)
            for k in (
                "cagr", "xirr", "final_capital", "max_drawdown", "sharpe", "sortino",
                "calmar", "win_rate", "profit_factor", "n_trades", "turnover_ratio",
                "benchmark_cagr", "excess_return", "avg_days_held", "n_distinct_tickers_traded",
            )
        },
        "closed_trades": int(len(trades)),
        "realized_pnl_inr": realized_pnl,
        "realized_return_on_capital": (realized_pnl / initial_capital) if initial_capital else None,
        "avg_holding_days": float(trades["holding_days"].mean()) if not trades.empty else None,
        "median_holding_days": float(trades["holding_days"].median()) if not trades.empty else None,
        "yearly": [asdict(b) for b in yearly_returns(trades)],
        # Empty for a run predating the equity_curve field on
        # BacktestRunResult (2026-08-08) — reported as absent, never faked
        # from realized P&L, which would not be the same quantity.
        "rolling": rolling_returns(report.get("equity_curve") or []),
        "taxes": taxes,
        "holdings": holdings_profile(trades),
        "entries": signals_per_month(trades),
        "exit_reasons": exit_reason_breakdown(trades),
    }
