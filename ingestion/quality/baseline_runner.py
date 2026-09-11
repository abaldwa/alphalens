"""
ingestion/quality/baseline_runner.py

Phase: 0.6 (Data Quality & Observability)
Specs: SPEC-PIPE-005, SPEC-DS-007
Owner: Platform / Ingestion
Consumers: operator (manual, run after the OHLCV backfill is complete)

One-time/periodic operator script: loads ~2 years of feature history from
Store 3's per-date Parquets (FEATURES_DAILY_DIR), restricted to
CORE_TECHNICAL_FEATURES (the same 77-column pool daily_inference.py's PSI
check restricts itself to — features/technical.py), and calls
ingestion.quality.drift_monitor.PSIMonitor.compute_baseline() to produce
datastore/features/baseline/stats_baseline.pkl — the reference distribution
ingestion.quality.drift_monitor.PSIMonitor.check_drift() compares each new
day against (SPEC-PIPE-005).

NOTE on data source (2026-09-11 fix): an earlier version of this file read
only 3 derived columns (return_1d, volume, delivery_pct) from
ohlcv_adjusted, as a deliberate Phase-0.6 stand-in for when
features/matrix_builder.py didn't exist yet. That module now exists and
CORE_TECHNICAL_FEATURES has grown to 77 real technical features, but the
baseline was never regenerated against it — check_drift() ended up
restricted to the 1 of those 77 columns the stale 3-feature baseline
happened to cover (delivery_pct), tripping PSI_MIN_MONITORED_FEATURES=10
and running the drift comparison on essentially one noisy series (see
BuildLog 2026-09-11: run_models halting on flip-flopping delivery_pct PSI
readings that were really measuring baseline staleness, not real drift —
same root cause as commit fe3c7231's 2026-08-14 fix, whose non_null_share
coverage-shift safety net never activated because this pickle predates
that field). Now reads CORE_TECHNICAL_FEATURES directly from the feature
store instead of re-deriving a subset from raw OHLCV, so the baseline
covers the same pool the drift check evaluates and non_null_share is
populated for every one of them.
"""

import argparse
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Union

import pandas as pd

from config.settings import FEATURES_DAILY_DIR
from config.timezone import now_ist
from features.technical import CORE_TECHNICAL_FEATURES
from ingestion.quality.drift_monitor import PSIMonitor

logger = logging.getLogger(__name__)

BASELINE_WINDOW_YEARS = 2  # "Compute PSI baseline: load 2 years of existing data"


def load_feature_history(
    features_dir: Optional[Union[str, Path]] = None,
    end_date: Optional[date] = None,
    years: int = BASELINE_WINDOW_YEARS,
) -> pd.DataFrame:
    """
    Load CORE_TECHNICAL_FEATURES history from the per-date feature Parquets
    covering the last `years` years.

    Parameters
    ----------
    features_dir : str, optional
        Defaults to config.settings.FEATURES_DAILY_DIR.
    end_date : date, optional
        Defaults to today.
    years : int
        Lookback window (default 2 — "load 2 years of existing data").

    Returns
    -------
    pd.DataFrame
        Columns: date, ticker, + whichever CORE_TECHNICAL_FEATURES are
        present in the stored Parquets. One row per (date, ticker).

    Spec References
    ----------------
    SPEC-PIPE-005.

    PIT Assumptions
    ----------------
    None — each date's Parquet is that date's own already-final, PIT-correct
    feature snapshot (features/matrix_builder.py).

    Raises
    ------
    FileNotFoundError
        If no feature Parquets are found in the requested window — the
        feature backfill (compute_features) must run first.
    """

    end_date = end_date or now_ist().date()
    start_date = end_date - timedelta(days=365 * years)
    resolved_dir = Path(features_dir) if features_dir is not None else FEATURES_DAILY_DIR

    frames = []
    for path in sorted(resolved_dir.glob("*.parquet")):
        try:
            file_date = date.fromisoformat(path.stem)
        except ValueError:
            continue
        if not (start_date <= file_date <= end_date):
            continue
        df = pd.read_parquet(path)
        cols = ["date", "ticker"] + [c for c in CORE_TECHNICAL_FEATURES if c in df.columns]
        frames.append(df[cols])

    if not frames:
        raise FileNotFoundError(
            f"No feature Parquets found in {resolved_dir} for {start_date}..{end_date}. "
            "Run the feature backfill (compute_features) first — see "
            "ingestion/quality/baseline_runner.py's module docstring."
        )

    history = pd.concat(frames, ignore_index=True)
    logger.info(
        f"Loaded feature history ({start_date}..{end_date}): "
        f"{len(history)} rows, {history['ticker'].nunique()} tickers, "
        f"{len(history.columns) - 2} feature(s)"
    )
    return history


def run(
    features_dir: Optional[Union[str, Path]] = None,
    end_date: Optional[date] = None,
    years: int = BASELINE_WINDOW_YEARS,
) -> Dict[str, Any]:
    """
    Load feature history, and compute + persist the PSI baseline.

    Returns
    -------
    dict
        The baseline dict returned by PSIMonitor.compute_baseline()
        ({feature_name: {'bin_edges': ..., 'baseline_pct': ...}}).

    Spec References
    ----------------
    SPEC-PIPE-005.

    Raises
    ------
    FileNotFoundError
        See load_feature_history().
    """
    history = load_feature_history(features_dir=features_dir, end_date=end_date, years=years)
    matrix = history.drop(columns=["date", "ticker"])
    monitor = PSIMonitor()
    baseline: Dict[str, Any] = monitor.compute_baseline(matrix)
    return baseline


def main() -> None:
    """CLI entry point: `python -m ingestion.quality.baseline_runner`."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(
        description="Compute the PSI drift baseline from 2 years of OHLCV history "
        "(must run after the OHLCV backfill is complete)."
    )
    parser.add_argument("--years", type=int, default=BASELINE_WINDOW_YEARS)
    args = parser.parse_args()

    baseline = run(years=args.years)
    print(f"PSI baseline computed for {len(baseline)} features.", flush=True)


if __name__ == "__main__":
    main()
