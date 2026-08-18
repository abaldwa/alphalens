"""
scripts/run_momentum_filter_overlays.py

Phase: FeatureBacklog.md ML38 — momentum strategy robustness overlays
Owner: Platform / Backtest
Consumers: operator CLI (`python3 -m scripts.run_momentum_filter_overlays`)

2026-07-27 user request: run the 7 previously-optional (default-off)
MomentumBacktester filters — liquidity floor (min_adtv_cr), ADTV-capped
position sizing (max_pct_of_adtv), a circuit-lock proxy (circuit_band_pct),
a short-term downtrend filter (downtrend_filter_pct), regime-conditional
buy-disabling (regime_series/disable_in_regimes), size/beta-orthogonalized
momentum (orthogonalize_vs_size_beta), and Piotroski/Beneish quality-gating
(quality_scores/quality_gate) — see backtest/momentum_backtest.py's
MomentumBacktester.__init__ docstring for what each one does. Reuses
scripts/run_momentum_experimentation.py's same 7-band x 4-lookback x
5-rebalance x 3-top_n grid (420 configs) so every overlay's variants line
up 1:1 with that script's already-published baseline (no-filter) numbers
for comparison — this script does NOT re-run the baseline itself.

Data sources for the filters that need auxiliary panels (all real, no
synthetic/stub data per this repo's zero-stub policy):
  - volume_panel: features.momentum_signal.load_volume_panel (real
    ohlcv_adjusted volume) — required for min_adtv_cr/max_pct_of_adtv.
  - market_cap_panel: price_panel x a STATIC per-ticker shares_outstanding
    (each ticker's earliest real fundamentals.shares_outstanding row —
    same "known, accepted approximation" already used by
    features.momentum_universe.market_cap_snapshot for pre-2024 dates,
    applied here as one flat series for the whole panel rather than
    recomputed per rebalance, since MomentumBacktester only needs it for
    a same-day cross-sectional regression, not a rank-band cutoff).
  - beta_map: stock_master.sector -> scripts.download_damodaran_datasets.
    SECTOR_UNLEVERED_BETAS (falls back to "Default"=0.90 for any sector
    string that isn't an exact key match).
  - regime_series: backtest/reports/momentum/momentum_yoy.duckdb's
    benchmark_index(nifty_500) table, real NSE closes, run through
    features.regime_signal.compute_realized_vol_regime. That table only
    has real history from 2023-07-03 (the same accepted index_ohlcv gap
    documented elsewhere in this codebase) — regime-conditional buying is
    therefore only actually exercised over the last ~3 years of the
    10-year window; MomentumBacktester's own "never disable on missing
    regime" convention handles the earlier years correctly (no
    fabricated regime labels).
  - quality_scores: the most recent real ml_forensic row per ticker
    (piotroski_f/beneish_m), applied as ONE static snapshot across the
    whole 10-year window — ml_forensic has no historical backfill (real
    rows only exist from 2026-06 onward in this DB), so a true
    point-in-time quality gate isn't possible yet. This is a stated,
    documented approximation (same "known-limitation, not fabricated
    data" pattern as market_cap_panel above), not a claim of PIT
    correctness.

SIP is skipped here (summary-only comparison against the existing
baseline, matching scripts/run_momentum_downtrend_filter_comparison.py's
precedent) to keep the 7 filters x 420 configs = 2,940 backtests
tractable.
"""

import argparse
import json
import logging
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from backtest.momentum_orchestrator_runner import run_momentum_orchestrated
from backtest.core.metrics import cagr, churn_factor, sharpe_sortino_calmar, trade_quality_metrics
from backtest.core.tax import post_tax_ending_value_from_dicts as post_tax_ending_value
from config.settings import DUCKDB_PATH, MAX_ORDER_VS_ADTV, MIN_ADTV_CR
from config.timezone import now_ist
from datastore.api.db import get_duckdb_connection
from features.momentum_signal import LOOKBACK_MONTHS, lookback_trading_days, load_price_panel, load_volume_panel
from features.momentum_universe import RANK_BANDS, all_yearly_full_rankings, yearly_band_universes_from_rankings
from features.regime_signal import HIGH_VOL, compute_realized_vol_regime
from scripts.download_damodaran_datasets import SECTOR_UNLEVERED_BETAS

# Per-ticker HMM regime (bearish/sideways/bullish) from daily feature parquets
HMM_REGIME_BEARISH = 0.0
HMM_REGIME_SIDEWAYS = 1.0
HMM_REGIME_BULLISH = 2.0

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# 2026-07-29: 201-250 band added to match run_momentum_experimentation.py
WIDE_BANDS = [(8, 201, 250), (6, 251, 500), (7, 501, 800)]

STARTING_CAPITAL = 1_000_000.0
INVESTABLE_PCT = 0.8
GRACE_CYCLES = 2  # [H4, 2026-08-18] vestigial -- MomentumAdapter has no grace_cycles knob (§19)
TOP_N_OPTIONS = [10, 15, 20]
REBALANCE_PERIODS = {"weekly": 5, "biweekly": 10, "monthly": 21, "bimonthly": 42, "quarterly": 63}

CIRCUIT_BAND_PCT = 0.20
DOWNTREND_FILTER_PCT = 0.05
DOWNTREND_LOOKBACK_DAYS = 20
QUALITY_GATE = {"min_f_score": 4, "max_m_score": -1.78}

REPORTS_DIR = Path(__file__).resolve().parent.parent / "backtest" / "reports" / "momentum"
MOMENTUM_YOY_DB = REPORTS_DIR / "momentum_yoy.duckdb"


def _union_tickers(yearly_rankings) -> List[str]:
    tickers = set()
    for ranked in yearly_rankings.values():
        if not ranked.empty:
            tickers.update(ranked["ticker"].tolist())
    return sorted(tickers)


def _load_static_shares_outstanding(conn, tickers: List[str]) -> Dict[str, float]:
    """ticker -> earliest real fundamentals.shares_outstanding — the same
    fallback market_cap_snapshot() uses for pre-2024 dates, applied here
    as one flat value per ticker (see module docstring)."""
    if not tickers:
        return {}
    placeholders = ",".join("?" for _ in tickers)
    rows = conn.execute(
        f"""
        SELECT ticker, shares_outstanding FROM (
            SELECT ticker, shares_outstanding,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY announcement_date ASC) AS rn
            FROM fundamentals
            WHERE ticker IN ({placeholders}) AND shares_outstanding IS NOT NULL
        ) WHERE rn = 1
        """,
        tickers,
    ).fetchall()
    return {t: s for t, s in rows if s is not None and s > 0}


def _build_market_cap_panel(price_panel: pd.DataFrame, shares_map: Dict[str, float]) -> pd.DataFrame:
    cols = [t for t in price_panel.columns if t in shares_map]
    if not cols:
        return pd.DataFrame(index=price_panel.index)
    shares = pd.Series({t: shares_map[t] for t in cols})
    return price_panel[cols].mul(shares, axis=1) / 1e7  # INR crore, matching momentum_universe.py's convention


def _load_beta_map(conn, tickers: List[str]) -> Dict[str, float]:
    if not tickers:
        return {}
    placeholders = ",".join("?" for _ in tickers)
    rows = conn.execute(
        f"SELECT ticker, sector FROM stock_master WHERE ticker IN ({placeholders})", tickers,
    ).fetchall()
    default_beta = SECTOR_UNLEVERED_BETAS["Default"]
    return {t: SECTOR_UNLEVERED_BETAS.get(sector, default_beta) for t, sector in rows}


def _load_regime_series() -> Optional[pd.Series]:
    """Real Nifty 500 close series -> realized-vol regime labels. None if
    momentum_yoy.duckdb / its benchmark_index table isn't present (this
    script still runs, regime_conditional just becomes a full no-op —
    never fabricated)."""
    if not MOMENTUM_YOY_DB.exists():
        logger.warning("momentum_yoy.duckdb not found at %s — regime_conditional filter will be a no-op", MOMENTUM_YOY_DB)
        return None
    with get_duckdb_connection(MOMENTUM_YOY_DB, persist=False, read_only=True) as conn:
        rows = conn.execute(
            "SELECT date, close FROM benchmark_index WHERE index_name = 'nifty_500' ORDER BY date"
        ).fetchall()
    if not rows:
        return None
    idx = pd.DatetimeIndex([r[0] for r in rows])
    closes = pd.Series([r[1] for r in rows], index=idx)
    return compute_realized_vol_regime(closes)


def _load_quality_scores(tickers: List[str]) -> Dict[str, Dict[str, float]]:
    """Most recent real ml_forensic row per ticker -> {f_score, m_score}
    (see module docstring for why this is one static snapshot, not a
    PIT series)."""
    from config.settings import SIGNALS_DUCKDB_PATH

    if not tickers:
        return {}
    placeholders = ",".join("?" for _ in tickers)
    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
        rows = conn.execute(
            f"""
            SELECT ticker, piotroski_f, beneish_m FROM (
                SELECT ticker, piotroski_f, beneish_m,
                       ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
                FROM ml_forensic WHERE ticker IN ({placeholders})
            ) WHERE rn = 1
            """,
            tickers,
        ).fetchall()
    return {
        t: {"f_score": f, "m_score": m}
        for t, f, m in rows
        if f is not None or m is not None
    }


def _load_per_ticker_hmm_regime(
    tickers: List[str], start_date: str, end_date: str
) -> Dict[str, pd.DataFrame]:
    """Load per-ticker HMM regime (bearish=0, sideways=1, bullish=2) from daily
    feature parquets for the given ticker list and date range.

    Returns a dict of ticker -> DataFrame with index=date, columns=['hmm_regime'].
    Missing tickers/dates get NaN (never fabricated).
    """
    from config.settings import FEATURES_DAILY_DIR

    if not tickers:
        return {}

    logger.info("Loading per-ticker HMM regime from %s", FEATURES_DAILY_DIR)
    regime_by_ticker = {}
    ticker_set = set(tickers)

    parquets = sorted(FEATURES_DAILY_DIR.glob("*.parquet"))
    if not parquets:
        logger.warning("No feature parquets found at %s", FEATURES_DAILY_DIR)
        return {}

    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)
    relevant_parquets = [p for p in parquets if start_ts <= pd.Timestamp(p.stem) <= end_ts]

    for p in relevant_parquets:
        date_str = p.stem
        df = pd.read_parquet(p, columns=["ticker", "hmm_regime"])
        df = df[df["ticker"].isin(ticker_set) & df["hmm_regime"].notna()]
        if df.empty:
            continue
        for _, row in df.iterrows():
            ticker = row["ticker"]
            if ticker not in regime_by_ticker:
                regime_by_ticker[ticker] = []
            regime_by_ticker[ticker].append({"date": date_str, "hmm_regime": row["hmm_regime"]})

    # Convert to DataFrames with DatetimeIndex
    out = {}
    for ticker, rows in regime_by_ticker.items():
        if not rows:
            continue
        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date").sort_index()
        out[ticker] = df[["hmm_regime"]]

    logger.info("Loaded per-ticker HMM regime for %d tickers", len(out))
    return out


def _run_variant(
    filter_name: str, kwargs: Dict, price_panel: pd.DataFrame, yearly_universes: Dict,
    lookback_days: int, rebalance_days: int, top_n: int,
):
    return run_momentum_orchestrated(
        price_panel=price_panel,
        yearly_universes=yearly_universes,
        lookback_days=lookback_days,
        rebalance_every_n_trading_days=rebalance_days,
        starting_capital=STARTING_CAPITAL,
        investable_pct=INVESTABLE_PCT,
        top_n=top_n,
        **kwargs,
    )


def run_overlays(years_back: int = 10) -> Dict:
    end_date = now_ist().date()
    start_date = date(end_date.year - years_back, end_date.month, end_date.day)

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        yearly_rankings = all_yearly_full_rankings(
            conn, start_date.isoformat(), end_date.isoformat(), max_rank=800, include_delisted=True,
        )
        candidate_tickers = _union_tickers(yearly_rankings)
        logger.info("Loading price/volume panels for %d candidate tickers", len(candidate_tickers))
        price_panel = load_price_panel(conn, candidate_tickers, start_date.isoformat(), end_date.isoformat())
        volume_panel = load_volume_panel(conn, candidate_tickers, start_date.isoformat(), end_date.isoformat())
        shares_map = _load_static_shares_outstanding(conn, candidate_tickers)
        beta_map = _load_beta_map(conn, candidate_tickers)

    market_cap_panel = _build_market_cap_panel(price_panel, shares_map)
    regime_series = _load_regime_series()
    quality_scores = _load_quality_scores(candidate_tickers)
    logger.info(
        "Auxiliary data ready: shares_outstanding=%d tickers, beta_map=%d tickers, "
        "market_cap_panel cols=%d, regime_series=%s, quality_scores=%d tickers",
        len(shares_map), len(beta_map), market_cap_panel.shape[1],
        "none" if regime_series is None else f"{len(regime_series)} rows",
        len(quality_scores),
    )

    # [H4, 2026-08-18] Two of the original 7 filters are dropped:
    #   - adtv_capped_sizing (max_pct_of_adtv): deprecated off MomentumAdapter
    #     entirely by the 2026-08-18 user decision (§19).
    #   - regime_conditional: MomentumAdapter conditions on regime via a live
    #     regime_conn (DB connection), not a precomputed regime_series/
    #     disable_in_regimes pair -- same limitation noted in
    #     run_momentum_dynamic_report.py::_build_strategies, not plumbed
    #     through here either.
    # regime_series is still loaded/logged above for visibility into what
    # would be available, even though no filter below consumes it.
    filters: Dict[str, Dict] = {
        "liquidity_floor": {"volume_panel": volume_panel, "min_adtv_cr": MIN_ADTV_CR},
        "circuit_lock_proxy": {"circuit_band_pct": CIRCUIT_BAND_PCT},
        "downtrend_filter": {
            "downtrend_filter_pct": DOWNTREND_FILTER_PCT, "downtrend_lookback_days": DOWNTREND_LOOKBACK_DAYS,
        },
        "size_beta_orthogonalized": {
            "orthogonalize_vs_size_beta": True, "market_cap_panel": market_cap_panel, "beta_map": beta_map,
        },
        "quality_gated": {"quality_scores": quality_scores, "quality_gate": QUALITY_GATE},
    }

    variants = []
    for filter_name, filter_kwargs in filters.items():
        for band_id, rank_start, rank_end in RANK_BANDS + WIDE_BANDS:
            yearly_universes = yearly_band_universes_from_rankings(yearly_rankings, rank_start, rank_end)
            for lookback_months in LOOKBACK_MONTHS:
                lookback_days = lookback_trading_days(lookback_months)
                for rebalance_name, rebalance_days in REBALANCE_PERIODS.items():
                    for top_n in TOP_N_OPTIONS:
                        logger.info(
                            "filter=%s band=%d lookback=%dmo rebalance=%s top_n=%d",
                            filter_name, band_id, lookback_months, rebalance_name, top_n,
                        )
                        result = _run_variant(
                            filter_name, filter_kwargs, price_panel, yearly_universes,
                            lookback_days, rebalance_days, top_n,
                        )
                        churn = churn_factor(result.rebalance_events)
                        post_tax_value = post_tax_ending_value(result.ending_value, result.transactions)
                        closed = [t for t in result.transactions if t["status"] == "closed"]
                        win_rate = (
                            sum(1 for t in closed if t["sell_price"] is not None and t["sell_price"] > t["buy_price"])
                            / len(closed) if closed else None
                        )
                        avg_days_held = (
                            sum(t["holding_days"] for t in closed) / len(closed) if closed else None
                        )
                        trade_quality = trade_quality_metrics(result.transactions)
                        variant_cagr = cagr(
                            result.starting_capital, result.ending_value, result.start_date, result.end_date
                        )
                        ratios = sharpe_sortino_calmar(result.equity_curve, variant_cagr)
                        variants.append({
                            "filter": filter_name,
                            "band_id": band_id, "rank_start": rank_start, "rank_end": rank_end,
                            "lookback_months": lookback_months, "rebalance_period": rebalance_name, "top_n": top_n,
                            "cagr": variant_cagr,
                            "sharpe": ratios["sharpe"],
                            "sortino": ratios["sortino"],
                            "calmar": ratios["calmar"],
                            "post_tax_cagr": cagr(
                                result.starting_capital, post_tax_value, result.start_date, result.end_date
                            ),
                            "churn_avg_transactions_per_year": churn["avg_transactions_per_year"],
                            "win_rate": win_rate,
                            "n_closed_trades": len(closed),
                            "n_open_trades": len(result.transactions) - len(closed),
                            "avg_days_held": avg_days_held,
                            "total_trades": trade_quality["total_trades"],
                            "avg_trade_duration_days": trade_quality["avg_trade_duration_days"],
                            "n_outlier_trades": trade_quality["n_outlier_trades"],
                            "max_abs_return_zscore": trade_quality["max_abs_return_zscore"],
                        })

    return {
        "generated_at": now_ist().isoformat(),
        "filters": list(filters.keys()),
        "filter_params": {
            "liquidity_floor": {"min_adtv_cr": MIN_ADTV_CR},
            "adtv_capped_sizing": {"max_pct_of_adtv": MAX_ORDER_VS_ADTV},
            "circuit_lock_proxy": {"circuit_band_pct": CIRCUIT_BAND_PCT},
            "downtrend_filter": {
                "downtrend_filter_pct": DOWNTREND_FILTER_PCT, "downtrend_lookback_days": DOWNTREND_LOOKBACK_DAYS,
            },
            "regime_conditional": {"disable_in_regimes": [HIGH_VOL], "regime_data_from": "2023-07-03"},
            "size_beta_orthogonalized": {"beta_source": "Damodaran SECTOR_UNLEVERED_BETAS"},
            "quality_gated": QUALITY_GATE,
        },
        "variants": variants,
    }


def main():
    parser = argparse.ArgumentParser(description="ML38 momentum filter-overlay robustness sweep")
    parser.add_argument("--years-back", type=int, default=10)
    args = parser.parse_args()

    report = run_overlays(years_back=args.years_back)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = REPORTS_DIR / f"momentum_filter_overlays_{now_ist().strftime('%Y%m%d_%H%M%S')}.json"
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    logger.info("Wrote report to %s (%d variants)", out_path, len(report["variants"]))


if __name__ == "__main__":
    main()
