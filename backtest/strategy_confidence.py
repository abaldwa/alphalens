"""
backtest/strategy_confidence.py

Phase: post-Phase-3 (Technical Analysis Screener confidence rework)
Owner: Platform / Backtest
Consumers: systems/technical_analysis/screener/outcomes.py (first caller);
           intended for future reuse by momentum (ML38) and ML signal
           models — any strategy that emits (date, ticker, direction)
           signals can plug into this without bespoke evaluation code.

Replaces a rejected ad hoc "did price touch a resistance/support line"
win/loss computation (see git history of
systems/technical_analysis/screener/outcomes.py) with a general strategy
confidence evaluator built entirely on infrastructure this repo already
has, per a 6-agent model review (ml-rigor, domain-expert,
backend-data-engineer, skeptic-tester, backtest-reviewer, product-owner)
that unanimously rejected the level-touch approach.

Design, in one paragraph: a "win" is a cost-adjusted forward net return
that clears a threshold (default > 0%), not a price touching an arbitrary
level — this both matches what a real trade means and removes the
level-selection artifact that made the rejected version structurally
unable to score breakout signals. Win rate is reported as a Wilson score
interval, not a bare percentage, and "sample size" for that interval is
the count of independent trading DATES a strategy fired on, not the
(much larger, cross-sectionally correlated) count of signal rows — firing
on 500 tickers on the same day is one observation of "what the market did
that day," not 500. Every win rate is reported alongside a baseline
(unconditional random-buy) win rate computed the same way, over the same
dates/horizon/costs, so a strategy's number is interpretable as "edge over
market beta," not an absolute. Results are additionally split by market
regime (reusing the existing hmm_market classification already written to
ml_signals — see datastore/api/routers/regime.py) since a strategy that
only "wins" in one regime is showing beta, not skill. Finally, a strategy
is only presented as VALIDATED once it clears a minimum independent-date
sample, has been observed across >=2 regimes, and its edge survives a
Deflated Sharpe Ratio correction for how many strategies were compared
side by side (backtest/overfit_checks.py::deflated_sharpe_ratio, built for
exactly this "20+ configurations tested" scenario, previously never wired
to a production caller).

Integrity-check scoping note: backtest/integrity_checker.py's
BacktestIntegrityChecker is built around walk-forward MODEL TRAINING
backtests (fold splits, HPO datasets, PIT feature columns) — most of its
10 checks don't apply to a signal-outcome evaluation like this one (there
is no train/test fold, no HPO, no ML feature matrix). Rather than
invent fold/HPO context that doesn't reflect anything real about this
evaluation just to satisfy checks that don't semantically apply here
(which would be worse than not running them), this module calls only
the two checks that DO apply directly —
check_05_costs (a real transaction cost was applied) and check_06_liquidity
(a real ADTV floor was applied) — and skips the rest by design, not by
omission. If either applicable check fails, the tier is forced to
INSUFFICIENT_DATA and no win-rate number is shown.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from backtest.core.metrics import calmar_ratio, max_drawdown, sortino_ratio
from backtest.costs import IndianTransactionCosts
from backtest.integrity_checker import BacktestIntegrityChecker
from backtest.overfit_checks import deflated_sharpe_ratio
from config.settings import (
    CONFIDENCE_DSR_THRESHOLD,
    CONFIDENCE_MIN_DATES_PER_REGIME,
    CONFIDENCE_MIN_INDEPENDENT_DATES,
    MIN_ADT_INR,
)

logger = logging.getLogger(__name__)

TIER_INSUFFICIENT = "INSUFFICIENT_DATA"
TIER_PRELIMINARY = "PRELIMINARY"
TIER_VALIDATED = "VALIDATED"
REGIME_UNKNOWN = "unknown"
REGIME_ALL = "ALL"


@dataclass(frozen=True)
class SignalEvent:
    date: "pd.Timestamp"
    ticker: str
    strategy_id: str
    direction: str = "long"  # "long" | "short"


@dataclass
class ConfidenceResult:
    strategy_id: str
    regime: str  # REGIME_ALL for the pooled result, else a regime label
    n_signals: int
    n_independent_dates: int
    wins: int
    losses: int
    pending: int
    win_rate: Optional[float]
    wilson_lo: Optional[float]
    wilson_hi: Optional[float]
    baseline_win_rate: Optional[float]
    delta_vs_baseline: Optional[float]
    deflated_sharpe: Optional[float]
    sortino: Optional[float] = None
    calmar: Optional[float] = None
    tier: str = TIER_INSUFFICIENT
    reasons: List[str] = field(default_factory=list)
    per_regime: Dict[str, "ConfidenceResult"] = field(default_factory=dict)


def wilson_interval(wins: int, n: int, z: float = 1.96) -> "tuple[float, float]":
    """Wilson score interval for a binomial proportion — well-behaved at
    small n and near 0/1, unlike a naive normal-approximation interval."""
    if n <= 0:
        return (0.0, 0.0)
    phat = wins / n
    denom = 1 + z * z / n
    center = phat + z * z / (2 * n)
    margin = z * ((phat * (1 - phat) / n + z * z / (4 * n * n)) ** 0.5)
    lo = (center - margin) / denom
    hi = (center + margin) / denom
    return max(0.0, lo), min(1.0, hi)


def _sortino_calmar_from_returns(dated_returns: "pd.Series") -> "tuple[Optional[float], Optional[float]]":
    """Sortino/Calmar computed straight from real, already-recorded signal
    outcomes (strategy_confidence_outcomes.net_return_pct, aggregated to
    one mean-return-per-independent-date series — the exact same series
    deflated_sharpe above is computed from) — 2026-07-27 user request:
    "is it possible to calculate these ratios without running the
    backtest?" Yes here, since every input is a real, already-resolved
    trade outcome already sitting in the DB; no new backtest run needed.

    Reuses backtest/core/metrics.py's shared sortino_ratio/calmar_ratio
    (same _NEAR_ZERO_STD-guarded implementations the unified orchestrator
    path uses) rather than a third hand-rolled copy. Calmar needs a
    cumulative equity curve, built by compounding this per-date mean
    return series in chronological order — a real proxy (every value is a
    real realized net_return_pct), not a synthetic one; annualized via the
    same calendar-day span the series' own dates cover."""
    dated_returns = dated_returns.dropna().sort_index()
    if len(dated_returns) < 2:
        return None, None
    sortino, _sortino_reason = sortino_ratio(dated_returns)

    equity = (1.0 + dated_returns).cumprod()
    mdd = max_drawdown(equity)
    span_days = (dated_returns.index[-1] - dated_returns.index[0]).days
    years = max(span_days / 365.25, 1e-9)
    ending_value = float(equity.iloc[-1])
    cagr_equiv = ending_value ** (1.0 / years) - 1.0 if ending_value > 0 else None
    calmar, _calmar_reason = calmar_ratio(cagr_equiv, mdd)
    return sortino, calmar


def compute_forward_net_return(
    entry_price: float,
    exit_price: float,
    direction: str,
    quantity: int = 100,
    adtv_cr: Optional[float] = None,
    costs: Optional[IndianTransactionCosts] = None,
) -> "tuple[float, float]":
    """Returns (gross_return_pct, net_return_pct) for one round trip."""
    costs = costs or IndianTransactionCosts()
    if direction == "long":
        gross = (exit_price - entry_price) / entry_price
    elif direction == "short":
        gross = (entry_price - exit_price) / entry_price
    else:
        raise ValueError(f"direction must be 'long' or 'short', got {direction!r}")
    cost_pct = costs.compute_roundtrip_cost_pct(entry_price, quantity, adtv_cr)
    return gross, gross - cost_pct


def _build_ticker_index(ohlcv_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    df = ohlcv_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    return {str(t): g.sort_values("date").reset_index(drop=True) for t, g in df.groupby("ticker")}


def _regime_series(regime_df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if regime_df is None or regime_df.empty:
        return None
    df = regime_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)


def _regime_as_of(regime_idx: Optional[pd.DataFrame], sig_date: np.datetime64) -> str:
    if regime_idx is None:
        return REGIME_UNKNOWN
    dates = regime_idx["date"].to_numpy()
    idx = np.searchsorted(dates, sig_date, side="right") - 1
    if idx < 0:
        return REGIME_UNKNOWN
    regime = regime_idx.iloc[idx]["hmm_regime"]
    # [BUG FIX, 2026-07-28 model-review item 3] hmm_regime is NaN for any
    # feature parquet built with compute_hmm=False (matrix_builder.py's own
    # documented "recommended" flag for a full historical backfill).
    # str(nan) == "nan", which is NOT REGIME_UNKNOWN ("unknown"), so these
    # rows used to silently pool into a fabricated "nan" regime bucket and
    # get treated as real regime data for confidence-per-regime stats
    # instead of being excluded like genuinely unknown regimes are.
    if pd.isna(regime):
        return REGIME_UNKNOWN
    return str(regime)


def _evaluate_one(
    ticker: str,
    sig_date: pd.Timestamp,
    direction: str,
    hist_by_ticker: Dict[str, pd.DataFrame],
    horizon_days: int,
    win_threshold_pct: float,
    quantity: int,
    costs: IndianTransactionCosts,
    adtv_lookup: Optional[Dict[str, float]],
) -> Optional[dict[str, Any]]:
    """Entry = next trading day's open after sig_date; exit = close
    `horizon_days` trading days after entry. Returns None if this ticker
    has no OHLCV history at all (can't evaluate, not the same as pending)."""
    g = hist_by_ticker.get(ticker)
    if g is None or g.empty:
        return None
    dates = g["date"].to_numpy()
    np_sig_date = np.datetime64(sig_date)
    entry_idx = np.searchsorted(dates, np_sig_date, side="right")
    if entry_idx >= len(g):
        return {"outcome": "pending", "entry_price": None, "exit_price": None,
                "gross_return_pct": None, "cost_pct": None, "net_return_pct": None,
                "outcome_date": None}

    exit_idx = entry_idx + horizon_days - 1
    if exit_idx >= len(g):
        return {"outcome": "pending", "entry_price": float(g.iloc[entry_idx]["open"]),
                "exit_price": None, "gross_return_pct": None, "cost_pct": None,
                "net_return_pct": None, "outcome_date": None}

    entry_price = float(g.iloc[entry_idx]["open"])
    exit_row = g.iloc[exit_idx]
    exit_price = float(exit_row["close"])
    adtv_cr = adtv_lookup.get(ticker) if adtv_lookup else None
    gross_pct, net_pct = compute_forward_net_return(entry_price, exit_price, direction, quantity, adtv_cr, costs)
    cost_pct = gross_pct - net_pct
    outcome = "win" if net_pct > win_threshold_pct else "loss"
    return {
        "outcome": outcome, "entry_price": entry_price, "exit_price": exit_price,
        "gross_return_pct": gross_pct, "cost_pct": cost_pct, "net_return_pct": net_pct,
        "outcome_date": str(exit_row["date"].date()),
    }


def _assign_tier(
    n_independent_dates: int,
    regime_date_counts: Dict[str, int],
    integrity_ok: bool,
    wilson_lo: Optional[float],
    baseline_win_rate: Optional[float],
    deflated_sharpe: Optional[float],
) -> "tuple[str, List[str]]":
    reasons: List[str] = []
    if not integrity_ok:
        return TIER_INSUFFICIENT, ["failed applicable integrity checks (costs/liquidity not properly modeled)"]
    if n_independent_dates < CONFIDENCE_MIN_INDEPENDENT_DATES:
        return TIER_INSUFFICIENT, [
            f"only {n_independent_dates} independent trading dates "
            f"(need >= {CONFIDENCE_MIN_INDEPENDENT_DATES})"
        ]

    regimes_with_enough_dates = [r for r, n in regime_date_counts.items()
                                  if r != REGIME_UNKNOWN and n >= CONFIDENCE_MIN_DATES_PER_REGIME]
    if len(regimes_with_enough_dates) < 2:
        reasons.append(
            f"only {len(regimes_with_enough_dates)} regime(s) with >= "
            f"{CONFIDENCE_MIN_DATES_PER_REGIME} dates each (need >= 2 to rule out single-regime beta)"
        )
        return TIER_PRELIMINARY, reasons

    if deflated_sharpe is None or deflated_sharpe < CONFIDENCE_DSR_THRESHOLD:
        reasons.append(
            f"deflated Sharpe {deflated_sharpe!r} below threshold {CONFIDENCE_DSR_THRESHOLD} "
            "(doesn't survive multiple-comparison correction)"
        )
        return TIER_PRELIMINARY, reasons

    if wilson_lo is None or baseline_win_rate is None or wilson_lo <= baseline_win_rate:
        reasons.append("Wilson lower bound does not clearly exceed the random-buy baseline")
        return TIER_PRELIMINARY, reasons

    return TIER_VALIDATED, ["meets sample size, multi-regime, DSR, and baseline requirements"]


def compute_baseline(
    ohlcv_df: pd.DataFrame,
    dates: List[pd.Timestamp],
    *,
    horizon_days: int = 5,
    win_threshold_pct: float = 0.0,
    quantity: int = 100,
    sample_size: int = 200,
    random_state: int = 42,
    costs: Optional[IndianTransactionCosts] = None,
) -> "tuple[int, int]":
    """Unconditional random-buy control: for each date, sample up to
    `sample_size` tickers with OHLCV data on/before that date and evaluate
    the same long/horizon/cost/threshold rule any real strategy is scored
    with. Returns (wins, total) so callers can combine with wilson_interval."""
    costs = costs or IndianTransactionCosts()
    hist_by_ticker = _build_ticker_index(ohlcv_df)
    rng = np.random.default_rng(random_state)
    all_tickers = sorted(hist_by_ticker.keys())

    wins = 0
    total = 0
    for d in sorted(set(pd.Timestamp(d) for d in dates)):
        sample = rng.choice(all_tickers, size=min(sample_size, len(all_tickers)), replace=False)
        for ticker in sample:
            result = _evaluate_one(ticker, d, "long", hist_by_ticker, horizon_days, win_threshold_pct, quantity, costs, None)
            if result is None or result["outcome"] == "pending":
                continue
            total += 1
            if result["outcome"] == "win":
                wins += 1
    return wins, total


def evaluate_signals(
    signals: List[SignalEvent],
    ohlcv_df: pd.DataFrame,
    *,
    regime_df: Optional[pd.DataFrame] = None,
    horizon_days: int = 5,
    win_threshold_pct: float = 0.0,
    n_strategies_compared: int = 1,
    quantity: int = 100,
    adtv_lookup: Optional[Dict[str, float]] = None,
    applied_min_adt_inr: Optional[float] = None,
    baseline_dates: Optional[List[pd.Timestamp]] = None,
    baseline_sample_size: int = 200,
) -> Dict[str, "ConfidenceResult"]:
    """The one entry point every caller (TA screener, future momentum/ML
    signal callers) uses. Groups `signals` by strategy_id, evaluates each
    against real forward OHLCV under a uniform cost-adjusted win rule, and
    returns a tiered confidence result per strategy (with a nested
    per-regime breakdown), keyed by strategy_id.

    Returns per-signal detail rows via the module-level `_last_detail_rows`
    convention is intentionally NOT used — callers that need to persist
    per-signal rows should call `evaluate_signals_with_detail` instead.
    """
    results, _ = evaluate_signals_with_detail(
        signals, ohlcv_df, regime_df=regime_df, horizon_days=horizon_days,
        win_threshold_pct=win_threshold_pct, n_strategies_compared=n_strategies_compared,
        quantity=quantity, adtv_lookup=adtv_lookup, applied_min_adt_inr=applied_min_adt_inr,
        baseline_dates=baseline_dates, baseline_sample_size=baseline_sample_size,
    )
    return results


def evaluate_signals_with_detail(
    signals: List[SignalEvent],
    ohlcv_df: pd.DataFrame,
    *,
    regime_df: Optional[pd.DataFrame] = None,
    horizon_days: int = 5,
    win_threshold_pct: float = 0.0,
    n_strategies_compared: int = 1,
    quantity: int = 100,
    adtv_lookup: Optional[Dict[str, float]] = None,
    applied_min_adt_inr: Optional[float] = None,
    baseline_dates: Optional[List[pd.Timestamp]] = None,
    baseline_sample_size: int = 200,
) -> "tuple[Dict[str, ConfidenceResult], pd.DataFrame]":
    """Same as evaluate_signals but also returns the per-signal detail
    DataFrame (one row per input SignalEvent, with entry/exit/outcome) for
    persistence into strategy_confidence_outcomes."""
    costs = IndianTransactionCosts()
    hist_by_ticker = _build_ticker_index(ohlcv_df)
    regime_idx = _regime_series(regime_df)
    applied_min_adt_inr = applied_min_adt_inr if applied_min_adt_inr is not None else MIN_ADT_INR

    detail_df = _compute_detail_rows(
        signals, hist_by_ticker, regime_idx, horizon_days, win_threshold_pct, quantity, costs, adtv_lookup,
    )

    # Baseline: reuse the same real dates the signals actually fired on
    # (or an explicit override), same horizon/cost rule.
    baseline_dates = baseline_dates if baseline_dates is not None else (
        detail_df["date"].unique().tolist() if not detail_df.empty else []
    )
    baseline_wins, baseline_total = (0, 0)
    if baseline_dates:
        baseline_wins, baseline_total = compute_baseline(
            ohlcv_df, baseline_dates, horizon_days=horizon_days, win_threshold_pct=win_threshold_pct,
            quantity=quantity, sample_size=baseline_sample_size, costs=costs,
        )
    baseline_win_rate = (baseline_wins / baseline_total) if baseline_total > 0 else None

    applied_cost_pct = costs.compute_roundtrip_cost_pct(1000.0, quantity)
    integrity_checker = BacktestIntegrityChecker(
        applied_roundtrip_cost_pct=applied_cost_pct, applied_min_adt_inr=applied_min_adt_inr,
    )
    cost_check = integrity_checker.check_05_costs()
    liquidity_check = integrity_checker.check_06_liquidity()
    integrity_ok = cost_check.passed and liquidity_check.passed
    if not integrity_ok:
        logger.warning(
            "strategy_confidence integrity checks failed: costs=%s (%s), liquidity=%s (%s)",
            cost_check.passed, cost_check.detail, liquidity_check.passed, liquidity_check.detail,
        )

    results = build_confidence_results(detail_df, baseline_win_rate, integrity_ok, n_strategies_compared)
    return results, detail_df


def build_confidence_results(
    detail_df: pd.DataFrame,
    baseline_win_rate: Optional[float],
    integrity_ok: bool,
    n_strategies_compared: int,
) -> Dict[str, ConfidenceResult]:
    """Aggregates a full (possibly chunk-assembled) detail DataFrame into a
    tiered ConfidenceResult per strategy_id, with a nested per-regime
    breakdown. Split out from evaluate_signals_with_detail so
    evaluate_signals_chunked can build detail incrementally across chunks
    (persisting each chunk to disk as it goes) while still running this
    aggregation step exactly once, over the complete detail set."""
    results: Dict[str, ConfidenceResult] = {}
    if detail_df.empty:
        return results

    for strategy_id, sdf in detail_df.groupby("strategy_id"):
        strategy_id_str = str(strategy_id)
        pooled = _build_confidence_result(
            strategy_id_str, REGIME_ALL, sdf, baseline_win_rate, integrity_ok,
            n_strategies_compared, regime_date_counts=_regime_date_counts(sdf),
        )
        pooled.per_regime = {
            str(regime): _build_confidence_result(
                strategy_id_str, str(regime), rdf, baseline_win_rate, integrity_ok,
                n_strategies_compared, regime_date_counts=_regime_date_counts(sdf),
            )
            for regime, rdf in sdf.groupby("regime")
        }
        results[strategy_id_str] = pooled

    return results


def _compute_detail_rows(
    signals: List[SignalEvent],
    hist_by_ticker: Dict[str, pd.DataFrame],
    regime_idx: Optional[pd.DataFrame],
    horizon_days: int,
    win_threshold_pct: float,
    quantity: int,
    costs: IndianTransactionCosts,
    adtv_lookup: Optional[Dict[str, float]],
) -> pd.DataFrame:
    """Per-signal entry/exit/outcome evaluation, cached by (ticker, date,
    direction) since multiple strategies commonly share the same signal-day."""
    eval_cache: Dict[tuple[Any, ...], Optional[dict[str, Any]]] = {}
    detail_rows: List[dict[str, Any]] = []

    for sig in signals:
        sig_date = pd.Timestamp(sig.date)
        cache_key = (sig.ticker, sig_date, sig.direction)
        if cache_key not in eval_cache:
            eval_cache[cache_key] = _evaluate_one(
                sig.ticker, sig_date, sig.direction, hist_by_ticker, horizon_days,
                win_threshold_pct, quantity, costs, adtv_lookup,
            )
        result = eval_cache[cache_key]
        if result is None:
            continue
        regime = _regime_as_of(regime_idx, np.datetime64(sig_date))
        detail_rows.append({
            "date": sig_date, "ticker": sig.ticker, "strategy_id": sig.strategy_id,
            "direction": sig.direction, "regime": regime, **result,
        })

    return pd.DataFrame(detail_rows)


def evaluate_signals_chunked(
    signals: List[SignalEvent],
    ohlcv_df: pd.DataFrame,
    conn: Any,
    *,
    regime_df: Optional[pd.DataFrame] = None,
    horizon_days: int = 5,
    win_threshold_pct: float = 0.0,
    n_strategies_compared: int = 1,
    quantity: int = 100,
    adtv_lookup: Optional[Dict[str, float]] = None,
    applied_min_adt_inr: Optional[float] = None,
    baseline_sample_size: int = 200,
    chunk_size_dates: int = 20,
    on_chunk_persisted: Optional[Any] = None,
) -> Dict[str, ConfidenceResult]:
    """Same evaluation as evaluate_signals_with_detail, but persists detail
    rows to `strategy_confidence_outcomes` chunk-by-chunk (grouped by
    signal date, `chunk_size_dates` trading dates per chunk) as it computes
    them, instead of holding the entire multi-million-row detail set in
    memory and writing it in one shot at the very end. This matters for
    multi-year backfills: a kill/crash partway through leaves real,
    queryable partial progress in the DB rather than nothing, and progress
    is visible in the table's row count while the run is still going.

    CRITICAL memory note: an earlier version of this function persisted
    each chunk to disk correctly but ALSO kept every chunk's raw per-signal
    detail rows in a Python list for the final summary aggregation — so by
    the last chunk it was holding the entire multi-million-row detail set
    in memory anyway, identical to not chunking at all. This OOM-killed a
    real 20-year/19M-row run (2026-07-19, confirmed via dmesg: anon-rss
    7.17GB at kill time). Fixed by reducing each chunk to a compact
    per-(strategy_id, regime, date) aggregate immediately after persisting
    it (`_aggregate_chunk_for_summary`) and discarding the raw rows — the
    accumulated aggregate is bounded by strategies x dates (tens of
    thousands of rows) regardless of how many signal rows or tickers
    contributed to it, and produces IDENTICAL win-rate/Wilson/DSR numbers
    since those statistics only ever depended on per-date win/loss/pending
    counts and per-date mean net return, never on individual ticker rows.
    `on_chunk_persisted(i, n_chunks, n_rows_in_chunk)` is called after each
    chunk's write, for progress logging by the caller.
    """
    costs = IndianTransactionCosts()
    hist_by_ticker = _build_ticker_index(ohlcv_df)
    regime_idx = _regime_series(regime_df)
    applied_min_adt_inr = applied_min_adt_inr if applied_min_adt_inr is not None else MIN_ADT_INR

    dates_sorted = sorted({pd.Timestamp(s.date) for s in signals})
    chunks = [dates_sorted[i:i + chunk_size_dates] for i in range(0, len(dates_sorted), chunk_size_dates)]
    signals_by_date: Dict[pd.Timestamp, List[SignalEvent]] = {}
    for s in signals:
        signals_by_date.setdefault(pd.Timestamp(s.date), []).append(s)

    agg_chunks: List[pd.DataFrame] = []
    baseline_dates: List[pd.Timestamp] = []
    for i, chunk_dates in enumerate(chunks, 1):
        chunk_signals = [s for d in chunk_dates for s in signals_by_date[d]]
        chunk_detail = _compute_detail_rows(
            chunk_signals, hist_by_ticker, regime_idx, horizon_days, win_threshold_pct, quantity, costs, adtv_lookup,
        )
        n_rows = persist_detail(conn, chunk_detail)
        if not chunk_detail.empty:
            baseline_dates.extend(chunk_detail["date"].unique().tolist())
            agg_chunks.append(_aggregate_chunk_for_summary(chunk_detail))
        del chunk_detail  # free the raw per-signal rows now that they're persisted + aggregated
        if on_chunk_persisted is not None:
            on_chunk_persisted(i, len(chunks), n_rows)

    agg_df = pd.concat(agg_chunks, ignore_index=True) if agg_chunks else pd.DataFrame()

    baseline_wins, baseline_total = (0, 0)
    if baseline_dates:
        baseline_wins, baseline_total = compute_baseline(
            ohlcv_df, baseline_dates, horizon_days=horizon_days, win_threshold_pct=win_threshold_pct,
            quantity=quantity, sample_size=baseline_sample_size, costs=costs,
        )
    baseline_win_rate = (baseline_wins / baseline_total) if baseline_total > 0 else None

    applied_cost_pct = costs.compute_roundtrip_cost_pct(1000.0, quantity)
    integrity_checker = BacktestIntegrityChecker(
        applied_roundtrip_cost_pct=applied_cost_pct, applied_min_adt_inr=applied_min_adt_inr,
    )
    cost_check = integrity_checker.check_05_costs()
    liquidity_check = integrity_checker.check_06_liquidity()
    integrity_ok = cost_check.passed and liquidity_check.passed
    if not integrity_ok:
        logger.warning(
            "strategy_confidence integrity checks failed: costs=%s (%s), liquidity=%s (%s)",
            cost_check.passed, cost_check.detail, liquidity_check.passed, liquidity_check.detail,
        )

    return build_confidence_results_from_agg(agg_df, baseline_win_rate, integrity_ok, n_strategies_compared)


def _aggregate_chunk_for_summary(chunk_detail_df: pd.DataFrame) -> pd.DataFrame:
    """Collapses one chunk's raw per-signal detail rows to one row per
    (strategy_id, regime, date) — the smallest representation that still
    lets build_confidence_results_from_agg reproduce identical win-rate/
    Wilson/DSR numbers, since none of those statistics depend on which (or
    how many) tickers fired a signal on a given date, only on that date's
    win/loss/pending counts and mean net return."""
    cols = ["strategy_id", "regime", "date", "n", "wins", "losses", "pending", "mean_net_return"]
    if chunk_detail_df.empty:
        return pd.DataFrame(columns=cols)

    grouped = chunk_detail_df.groupby(["strategy_id", "regime", "date"])
    agg = grouped.agg(
        n=("outcome", "size"),
        wins=("outcome", lambda s: int((s == "win").sum())),
        losses=("outcome", lambda s: int((s == "loss").sum())),
        pending=("outcome", lambda s: int((s == "pending").sum())),
    ).reset_index()

    decided = chunk_detail_df[chunk_detail_df["outcome"].isin(["win", "loss"])]
    mean_ret = (
        decided.groupby(["strategy_id", "regime", "date"])["net_return_pct"]
        .mean().rename("mean_net_return").reset_index()
    )
    agg = agg.merge(mean_ret, on=["strategy_id", "regime", "date"], how="left")
    return agg[cols]


def build_confidence_results_from_agg(
    agg_df: pd.DataFrame,
    baseline_win_rate: Optional[float],
    integrity_ok: bool,
    n_strategies_compared: int,
) -> Dict[str, ConfidenceResult]:
    """Same aggregation as build_confidence_results, but from the compact
    per-(strategy_id, regime, date) table _aggregate_chunk_for_summary
    produces, instead of raw per-signal rows — see evaluate_signals_chunked's
    docstring for why this bound on memory matters at multi-million-row scale."""
    results: Dict[str, ConfidenceResult] = {}
    if agg_df.empty:
        return results

    for strategy_id, sdf in agg_df.groupby("strategy_id"):
        strategy_id_str = str(strategy_id)
        pooled = _build_confidence_result_from_agg(
            strategy_id_str, REGIME_ALL, sdf, baseline_win_rate, integrity_ok,
            n_strategies_compared, regime_date_counts=_regime_date_counts_agg(sdf),
        )
        pooled.per_regime = {
            str(regime): _build_confidence_result_from_agg(
                strategy_id_str, str(regime), rdf, baseline_win_rate, integrity_ok,
                n_strategies_compared, regime_date_counts=_regime_date_counts_agg(sdf),
            )
            for regime, rdf in sdf.groupby("regime")
        }
        results[strategy_id_str] = pooled

    return results


def _regime_date_counts_agg(sdf: pd.DataFrame) -> Dict[str, int]:
    decided = sdf[(sdf["wins"] + sdf["losses"]) > 0]
    return {str(k): v for k, v in decided.groupby("regime").size().to_dict().items()}  # one row per date already


def _build_confidence_result_from_agg(
    strategy_id: str,
    regime: str,
    sdf: pd.DataFrame,
    baseline_win_rate: Optional[float],
    integrity_ok: bool,
    n_strategies_compared: int,
    regime_date_counts: Dict[str, int],
) -> ConfidenceResult:
    decided = sdf[(sdf["wins"] + sdf["losses"]) > 0]
    wins = int(decided["wins"].sum())
    losses = int(decided["losses"].sum())
    pending = int(sdf["pending"].sum())
    n_independent_dates = len(decided)  # one row per date already
    n = wins + losses

    win_rate = (wins / n) if n > 0 else None
    wilson_lo, wilson_hi = wilson_interval(wins, n) if n > 0 else (None, None)
    delta_vs_baseline = (win_rate - baseline_win_rate) if (win_rate is not None and baseline_win_rate is not None) else None

    deflated_sharpe = None
    sortino = None
    calmar = None
    if n_independent_dates >= 2:
        daily_returns = decided["mean_net_return"].dropna()
        if len(daily_returns) >= 2 and daily_returns.std(ddof=1) > 0:
            sharpe = float(daily_returns.mean() / daily_returns.std(ddof=1))
            deflated_sharpe = deflated_sharpe_ratio(sharpe, max(n_strategies_compared, 1), len(daily_returns))
        dated_returns = decided.set_index("date")["mean_net_return"].dropna()
        sortino, calmar = _sortino_calmar_from_returns(dated_returns)

    tier, reasons = _assign_tier(
        n_independent_dates, regime_date_counts, integrity_ok, wilson_lo, baseline_win_rate, deflated_sharpe,
    )

    return ConfidenceResult(
        strategy_id=strategy_id, regime=regime, n_signals=int(sdf["n"].sum()),
        n_independent_dates=n_independent_dates, wins=wins, losses=losses, pending=pending,
        win_rate=win_rate, wilson_lo=wilson_lo, wilson_hi=wilson_hi,
        baseline_win_rate=baseline_win_rate, delta_vs_baseline=delta_vs_baseline,
        deflated_sharpe=deflated_sharpe, sortino=sortino, calmar=calmar, tier=tier, reasons=reasons,
    )


def _regime_date_counts(sdf: pd.DataFrame) -> Dict[str, int]:
    decided = sdf[sdf["outcome"].isin(["win", "loss"])]
    return {str(k): v for k, v in decided.groupby("regime")["date"].nunique().to_dict().items()}


def _build_confidence_result(
    strategy_id: str,
    regime: str,
    sdf: pd.DataFrame,
    baseline_win_rate: Optional[float],
    integrity_ok: bool,
    n_strategies_compared: int,
    regime_date_counts: Dict[str, int],
) -> ConfidenceResult:
    decided = sdf[sdf["outcome"].isin(["win", "loss"])]
    wins = int((decided["outcome"] == "win").sum())
    losses = int((decided["outcome"] == "loss").sum())
    pending = int((sdf["outcome"] == "pending").sum())
    n_independent_dates = int(decided["date"].nunique())
    n = wins + losses

    win_rate = (wins / n) if n > 0 else None
    wilson_lo, wilson_hi = wilson_interval(wins, n) if n > 0 else (None, None)
    delta_vs_baseline = (win_rate - baseline_win_rate) if (win_rate is not None and baseline_win_rate is not None) else None

    deflated_sharpe = None
    sortino = None
    calmar = None
    if n_independent_dates >= 2:
        daily_returns = decided.groupby("date")["net_return_pct"].mean()
        if daily_returns.std(ddof=1) > 0:
            sharpe = float(daily_returns.mean() / daily_returns.std(ddof=1))
            deflated_sharpe = deflated_sharpe_ratio(
                sharpe, max(n_strategies_compared, 1), len(daily_returns), returns=daily_returns
            )
        sortino, calmar = _sortino_calmar_from_returns(daily_returns)

    tier, reasons = _assign_tier(
        n_independent_dates, regime_date_counts, integrity_ok, wilson_lo, baseline_win_rate, deflated_sharpe,
    )

    return ConfidenceResult(
        strategy_id=strategy_id, regime=regime, n_signals=len(sdf),
        n_independent_dates=n_independent_dates, wins=wins, losses=losses, pending=pending,
        win_rate=win_rate, wilson_lo=wilson_lo, wilson_hi=wilson_hi,
        baseline_win_rate=baseline_win_rate, delta_vs_baseline=delta_vs_baseline,
        deflated_sharpe=deflated_sharpe, sortino=sortino, calmar=calmar, tier=tier, reasons=reasons,
    )


def create_outcomes_table(conn: Any) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_confidence_outcomes (
            date DATE NOT NULL,
            ticker VARCHAR NOT NULL,
            strategy_id VARCHAR NOT NULL,
            direction VARCHAR NOT NULL,
            entry_price DOUBLE,
            exit_price DOUBLE,
            quantity INTEGER,
            gross_return_pct DOUBLE,
            cost_pct DOUBLE,
            net_return_pct DOUBLE,
            outcome VARCHAR NOT NULL,
            outcome_date DATE,
            regime VARCHAR,
            evaluated_at TIMESTAMP NOT NULL,
            PRIMARY KEY (date, ticker, strategy_id, direction)
        )
        """
    )


def create_summary_table(conn: Any) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_confidence_summary (
            strategy_id VARCHAR NOT NULL,
            regime VARCHAR NOT NULL,
            n_signals INTEGER,
            n_independent_dates INTEGER,
            wins INTEGER,
            losses INTEGER,
            pending INTEGER,
            win_rate DOUBLE,
            wilson_lo DOUBLE,
            wilson_hi DOUBLE,
            baseline_win_rate DOUBLE,
            delta_vs_baseline DOUBLE,
            deflated_sharpe DOUBLE,
            tier VARCHAR NOT NULL,
            reasons VARCHAR,
            computed_at TIMESTAMP NOT NULL,
            PRIMARY KEY (strategy_id, regime)
        )
        """
    )
    # 2026-07-27: Sortino/Calmar, computed from the same real per-signal
    # net_return_pct outcomes deflated_sharpe already uses — added after
    # this table already existed in real deployments, so CREATE TABLE IF
    # NOT EXISTS alone wouldn't reach it (same ALTER TABLE ADD COLUMN IF
    # NOT EXISTS pattern datastore/schema/create_backtest.py uses).
    conn.execute("ALTER TABLE strategy_confidence_summary ADD COLUMN IF NOT EXISTS sortino DOUBLE")
    conn.execute("ALTER TABLE strategy_confidence_summary ADD COLUMN IF NOT EXISTS calmar DOUBLE")


def persist_detail(conn: Any, detail_df: pd.DataFrame) -> int:
    """Bulk upsert via a registered DataFrame + INSERT...SELECT...ON CONFLICT,
    NOT conn.executemany() with one parameterized statement per row — the
    same ~250x-slower-per-row pattern measured in
    systems/technical_analysis/alerts/daily_alert_checker.py's _BULK_UPSERT_SQL,
    which matters here because detail_df can be millions of rows for a
    multi-year backfill (vs. ~5,000 rows/day for the live case)."""
    if detail_df.empty:
        return 0
    create_outcomes_table(conn)
    batch_df = detail_df[[
        "date", "ticker", "strategy_id", "direction", "entry_price", "exit_price",
        "gross_return_pct", "cost_pct", "net_return_pct", "outcome", "outcome_date", "regime",
    ]].copy()
    batch_df["quantity"] = 100
    conn.register("_strategy_confidence_detail_batch", batch_df)
    try:
        conn.execute(
            """
            INSERT INTO strategy_confidence_outcomes
                (date, ticker, strategy_id, direction, entry_price, exit_price, quantity,
                 gross_return_pct, cost_pct, net_return_pct, outcome, outcome_date, regime, evaluated_at)
            SELECT date, ticker, strategy_id, direction, entry_price, exit_price, quantity,
                   gross_return_pct, cost_pct, net_return_pct, outcome, outcome_date, regime, now()
            FROM _strategy_confidence_detail_batch
            ON CONFLICT (date, ticker, strategy_id, direction) DO UPDATE SET
                entry_price = excluded.entry_price, exit_price = excluded.exit_price,
                gross_return_pct = excluded.gross_return_pct, cost_pct = excluded.cost_pct,
                net_return_pct = excluded.net_return_pct, outcome = excluded.outcome,
                outcome_date = excluded.outcome_date, regime = excluded.regime,
                evaluated_at = excluded.evaluated_at
            """
        )
    finally:
        conn.unregister("_strategy_confidence_detail_batch")
    return len(batch_df)


def persist_summary(conn: Any, results: Dict[str, ConfidenceResult]) -> int:
    """Row count here is small (strategies x regimes, low hundreds at
    most) so executemany's per-row overhead doesn't matter — kept as-is
    for readability, unlike persist_detail's bulk rewrite.

    Deletes each strategy's existing rows before inserting the fresh set,
    rather than relying purely on ON CONFLICT DO UPDATE. This matters
    because the per-regime rows a run produces aren't a fixed, known-ahead
    set — e.g. an 'unknown' bucket can legitimately disappear entirely once
    regime history is backfilled and every date resolves to a real regime.
    ON CONFLICT alone only ever updates or inserts; it never removes a
    regime row that the current run no longer produces, so a stale 'unknown'
    row from a prior run (before real regime data existed) would silently
    keep reporting an outdated confidence tier forever. Confirmed live
    (2026-07-19): A4's 'unknown' row lingered with computed_at from an
    earlier partial run after a full recompute produced zero unknown-regime
    dates for it."""
    create_summary_table(conn)
    rows = []
    for res in results.values():
        for r in [res, *res.per_regime.values()]:
            rows.append((
                r.strategy_id, r.regime, r.n_signals, r.n_independent_dates,
                r.wins, r.losses, r.pending, r.win_rate, r.wilson_lo, r.wilson_hi,
                r.baseline_win_rate, r.delta_vs_baseline, r.deflated_sharpe, r.sortino, r.calmar, r.tier,
                "; ".join(r.reasons),
            ))
    if not rows:
        return 0

    strategy_ids = list(results.keys())
    placeholders = ",".join("?" * len(strategy_ids))
    conn.execute(f"DELETE FROM strategy_confidence_summary WHERE strategy_id IN ({placeholders})", strategy_ids)

    conn.executemany(
        """
        INSERT INTO strategy_confidence_summary
            (strategy_id, regime, n_signals, n_independent_dates, wins, losses, pending,
             win_rate, wilson_lo, wilson_hi, baseline_win_rate, delta_vs_baseline,
             deflated_sharpe, sortino, calmar, tier, reasons, computed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
        ON CONFLICT (strategy_id, regime) DO UPDATE SET
            n_signals = excluded.n_signals, n_independent_dates = excluded.n_independent_dates,
            wins = excluded.wins, losses = excluded.losses, pending = excluded.pending,
            win_rate = excluded.win_rate, wilson_lo = excluded.wilson_lo, wilson_hi = excluded.wilson_hi,
            baseline_win_rate = excluded.baseline_win_rate, delta_vs_baseline = excluded.delta_vs_baseline,
            deflated_sharpe = excluded.deflated_sharpe, sortino = excluded.sortino, calmar = excluded.calmar,
            tier = excluded.tier, reasons = excluded.reasons,
            computed_at = excluded.computed_at
        """,
        rows,
    )
    return len(rows)
