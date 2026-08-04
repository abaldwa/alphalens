"""
scripts/backfill_deferred_advanced_technical.py

Phase: 3.1 (Advanced Technical Features)
Owner: Platform / Features
Consumers: systems/ml_signal_engine (bulk model inputs)

Fills in the 17 `advanced_technical` features the live daily pipeline now
skips by default (`ingestion/scheduler/daily_pipeline.py::step_compute_
features`'s `advanced_technical_used_only=True` default, added 2026-08-04 —
see FeatureBacklog.md). Of the 18 advanced_technical features (wavelet
decomposition, entropy, fractal dimension, fracdiff, Lyapunov, RQA), only
`hurst_exp_21d` is ever referenced downstream by name (screener templates);
the other 17 exist solely as bulk ML-model inputs, yet are the expensive,
per-row loop-based part of the daily feature build. This script runs
asynchronously, off the live pipeline's critical path, and closes that gap
for any date whose Parquet is still missing them.

Historical 2016-2026 dates are unaffected by the used_only default change —
already fully backfilled with all 18 features (FeatureBacklog.md A74). This
script only needs to run for NEW dates going forward.

Ticker scope: the FULL universe (config.universe.get_tickers_for_feature_
engineering(), ~2,314 non-ETF tickers) — NOT the Technical backtest sweep's
800-ticker ADTV-capped subset (`--max-tickers 800` is a
backtest/run_orchestrator_backtest.py-only concept, unrelated to feature-
store ticker coverage). This keeps the full feature store — which ML
models train against — complete.

Idempotent: a date whose Parquet already has a non-NaN wavelet_trend value
is skipped (same "already covers this range" convention
scripts/precompute_technical_screener_matches.py uses).

Merges ONLY the 17 deferred columns into each date's existing Parquet —
never touches the other ~80 columns already correctly populated by the
fast daily path.

Usage
-----
    # Single date
    .venv/bin/python3 scripts/backfill_deferred_advanced_technical.py \\
        --start-date 2026-08-04

    # Date range
    .venv/bin/python3 scripts/backfill_deferred_advanced_technical.py \\
        --start-date 2026-08-01 --end-date 2026-08-04

    # Background run with live progress log
    nohup .venv/bin/python3 scripts/backfill_deferred_advanced_technical.py \\
        --start-date 2026-08-01 --end-date 2026-08-04 \\
        > logs/backfill_deferred_advanced_technical.log 2>&1 &
"""

import argparse
import logging
import os
from datetime import date as date_type, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# All advanced_technical columns except hurst_exp_21d — the 17 the fast
# daily path leaves NaN and this script fills back in.
DEFERRED_COLUMNS = [
    "wavelet_trend", "wavelet_noise", "wavelet_energy_ratio", "wavelet_regime_signal",
    "hurst_exp_63d",
    "approx_entropy_21d", "sample_entropy_21d", "permutation_entropy_21d",
    "spectral_entropy", "fractal_dimension",
    "fracdiff_d_optimal", "fracdiff_price", "fracdiff_volume",
    "lyapunov_exponent_proxy", "rqa_rec_rate", "time_series_complexity", "nonlinear_trend_strength",
]


def _already_covers(parquet_path: Path) -> bool:
    """A date's Parquet already has the deferred columns filled in if any
    row has a non-NaN wavelet_trend — the fast daily path always leaves
    every deferred column NaN under advanced_technical_used_only=True, so
    a single non-NaN value proves this date was already backfilled."""
    if not parquet_path.exists():
        return False
    try:
        existing = pd.read_parquet(parquet_path, columns=["wavelet_trend"])
    except Exception:
        return False
    return bool(existing["wavelet_trend"].notna().any())


def backfill_one_date(target_date: date_type, client) -> bool:
    """Returns True if the date's Parquet was updated, False if skipped
    (already covered, no existing Parquet to merge into, or no OHLCV)."""
    from config.settings import FEATURES_DAILY_DIR

    parquet_path = FEATURES_DAILY_DIR / f"{target_date.isoformat()}.parquet"
    if not parquet_path.exists():
        logger.warning(
            f"backfill_deferred_advanced_technical: no existing Parquet for {target_date} — "
            "skipping (run the main daily pipeline for this date first)"
        )
        return False
    if _already_covers(parquet_path):
        logger.info(f"backfill_deferred_advanced_technical: {target_date} already covered, skipping")
        return False

    from config.universe import get_tickers_for_feature_engineering
    from features.advanced_technical import compute_advanced_technical_features
    from features.matrix_builder import LOOKBACK_CALENDAR_DAYS, _fetch_ohlcv_panel

    tickers = get_tickers_for_feature_engineering()
    ts = pd.Timestamp(target_date)
    from_date = (ts - pd.Timedelta(days=LOOKBACK_CALENDAR_DAYS)).to_pydatetime()
    to_date = ts.to_pydatetime()

    bulk_panel = None
    bulk_loader = getattr(client, "get_ohlcv_bulk", None)
    if callable(bulk_loader):
        try:
            bulk_panel = bulk_loader(from_date, to_date)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                f"backfill_deferred_advanced_technical: bulk OHLCV fetch failed for "
                f"{target_date}, falling back to per-ticker: {exc}"
            )

    ohlcv_panel = _fetch_ohlcv_panel(client, tickers, from_date, to_date, _bulk_panel=bulk_panel)
    if ohlcv_panel.empty:
        logger.warning(f"backfill_deferred_advanced_technical: no OHLCV for {target_date} — skipping")
        return False

    full_adv_tech = compute_advanced_technical_features(ohlcv_panel, all_rows=False, used_only=False)
    # all_rows=False fills only the LAST row per ticker (the target date's)
    # with real values, but still returns one row per date in the whole
    # lookback window — filter to the target date before merging, same
    # pattern features/matrix_builder.py's _compute_one_chunk_panels uses
    # (adv_tech[adv_tech["date"] == target_date]).
    full_adv_tech_today = full_adv_tech[full_adv_tech["date"] == ts]
    deferred = full_adv_tech_today[["ticker"] + DEFERRED_COLUMNS]

    existing = pd.read_parquet(parquet_path)
    original_columns = list(existing.columns)
    merged = existing.drop(columns=DEFERRED_COLUMNS, errors="ignore").merge(deferred, on="ticker", how="left")
    merged = merged[original_columns]

    tmp_path = parquet_path.with_suffix(".parquet.tmp")
    try:
        merged.to_parquet(tmp_path, index=False)
        os.replace(tmp_path, parquet_path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise

    logger.info(
        f"backfill_deferred_advanced_technical: {target_date} updated "
        f"({len(deferred)} tickers x {len(DEFERRED_COLUMNS)} deferred columns merged)"
    )
    return True


def run_backfill(start_date: date_type, end_date: date_type, client=None) -> dict:
    from datastore.client import DataStoreClient

    client = client or DataStoreClient()
    d = start_date
    n_updated = n_skipped = 0
    while d <= end_date:
        if backfill_one_date(d, client):
            n_updated += 1
        else:
            n_skipped += 1
        d += timedelta(days=1)
    logger.info(
        f"backfill_deferred_advanced_technical: complete — {n_updated} updated, {n_skipped} skipped "
        f"({start_date} .. {end_date})"
    )
    return {"n_updated": n_updated, "n_skipped": n_skipped}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fill in the 17 deferred advanced_technical features the fast daily pipeline path leaves NaN"
    )
    parser.add_argument("--start-date", type=date_type.fromisoformat, required=True)
    parser.add_argument(
        "--end-date", type=date_type.fromisoformat, default=None, help="Default: same as --start-date"
    )
    args = parser.parse_args()
    end_date: Optional[date_type] = args.end_date or args.start_date
    run_backfill(args.start_date, end_date)


if __name__ == "__main__":
    main()
