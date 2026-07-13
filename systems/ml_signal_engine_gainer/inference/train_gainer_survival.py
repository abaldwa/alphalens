"""
systems/ml_signal_engine_gainer/inference/train_gainer_survival.py

GAINER EXPERIMENT (development phase, FeatureBacklog.md ML33, 2026-07-13
user-authorized) — standalone training entry point for the small
RandomSurvivalForest "first_touch_day" survival head
(models/signal/gainer_survival_head.py), for the 21d/63d gainer signal
targets ONLY (not 6d — see that module's docstring for why; not
signal_5d/21d/63d production models, not MultiBagger).

**Development-phase scope**: this script trains the RSF head end-to-end
and prints diagnostics (training_samples, event_rate, concordance_index,
sample survival-curve predictions) to prove the approach works. It does
NOT save anything into datastore/models/ or GAINER_MODELS_DIR's
registry.json, and it is NOT wired into any scheduler/cron/systemd job —
per the user's own instruction, scheduling is a separate, explicit
follow-up step once this development output has been reviewed.

Reuses read-only, non-experimental infra already used by
train_gainer_signals.py (OHLCV/benchmark loaders, CORE_TECHNICAL_FEATURES
computation, checkpointed chunk loading, PnD panel scoring) — this file
does not modify train_gainer_signals.py or any production
ml_signal_engine module.

Usage
-----
    python -m systems.ml_signal_engine_gainer.inference.train_gainer_survival \\
        --horizon 21 --lookback-days 400 --ticker-chunk-size 50 \\
        --tickers RELIANCE,TCS,INFY,HDFCBANK,ICICIBANK,SBIN,ITC,LT,AXISBANK,KOTAKBANK

(a small explicit --tickers list + short --lookback-days is the
"small/quick sample, prove it works end-to-end" development mode; omit
both for a full-universe run once reviewed.)
"""

import argparse
import logging
from typing import Dict, List, Optional

import pandas as pd

from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from features.technical import CORE_TECHNICAL_FEATURES, compute_technical_features
from systems.ml_signal_engine.inference.train_all_phase1 import load_benchmark_from_db
from systems.ml_signal_engine_gainer.inference.checkpoint_utils import (
    checkpoint_path, load_all_checkpoints, load_checkpoint, save_checkpoint, ticker_chunks,
)
from systems.ml_signal_engine_gainer.inference.train_gainer_signals import (
    DEFAULT_TICKER_CHUNK_SIZE,
    GAINER_SIGNAL_TARGETS,
    GainerSignalTarget,
)
from systems.ml_signal_engine_gainer.models.multibagger.multibagger_model import _score_pnd_panel
from systems.ml_signal_engine_gainer.models.signal.gainer_survival_head import GainerSurvivalHead
from systems.ml_signal_engine_gainer.training.labeling import FixedPercentLabeler

logger = logging.getLogger(__name__)

# ML33: only 21d/63d — the backlog row's own feasibility assessment found
# the 6d window too short for day-level timing to change a trading
# decision (low value), while 21d/63d are "plausible."
SURVIVAL_TARGETS: List[GainerSignalTarget] = [
    t for t in GAINER_SIGNAL_TARGETS if t.horizon_days in (21, 63)
]


def _build_survival_dataset_from_chunk(
    ohlcv_chunk: pd.DataFrame, benchmark: pd.DataFrame, target: GainerSignalTarget, pnd_scores_chunk: pd.Series,
) -> pd.DataFrame:
    """Same feature computation as train_gainer_signals.py's
    _build_gainer_training_dataset_from_chunk, but keeps first_touch_day
    (dropped by that function since the classifier doesn't need it)."""
    features = compute_technical_features(ohlcv_chunk, benchmark)

    labeler = FixedPercentLabeler(horizon_days=target.horizon_days, target_pct=target.target_pct)
    merged = ohlcv_chunk.assign(pnd_score=pnd_scores_chunk.to_numpy())
    merged["pnd_block"] = merged["pnd_score"].fillna(0) > 40

    label_df = labeler.label_panel(merged, close_col="close", ticker_col="ticker", pnd_block_col="pnd_block")

    combined = features.copy()
    combined["_label"] = label_df["label"].to_numpy()
    combined["_first_touch_day"] = label_df["first_touch_day"].to_numpy()
    combined = combined.dropna(subset=["_label"]).reset_index(drop=True)
    return combined


def _build_survival_dataset_chunked(
    target: GainerSignalTarget,
    benchmark: pd.DataFrame,
    lookback_days: int,
    db_path=None,
    tickers: Optional[list] = None,
    ticker_chunk_size: int = DEFAULT_TICKER_CHUNK_SIZE,
) -> pd.DataFrame:
    """Chunked + checkpointed, same pattern as train_gainer_signals.py's
    _build_gainer_training_dataset_chunked, under a distinct checkpoint
    stage tag ("survival_...") so it never collides with that function's
    own checkpoints for the same target/lookback."""
    db_path = db_path or DUCKDB_PATH
    stage = f"survival_h{target.horizon_days}_t{target.target_pct}"

    with get_duckdb_connection(db_path, read_only=True, persist=False) as conn:
        if tickers:
            all_tickers = list(tickers)
        else:
            all_tickers = conn.execute(
                "SELECT DISTINCT ticker FROM ohlcv_adjusted WHERE date >= CURRENT_DATE - INTERVAL (?) DAY",
                [lookback_days],
            ).df()["ticker"].tolist()
        if not all_tickers:
            raise RuntimeError("ohlcv_adjusted has no tickers in the requested lookback window")

        chunk_list = list(ticker_chunks(all_tickers, chunk_size=ticker_chunk_size))
        logger.info(
            "%s (survival): %d tickers in %d chunks of <=%d", target.name, len(all_tickers), len(chunk_list), ticker_chunk_size,
        )

        for chunk_idx, chunk_tickers in enumerate(chunk_list):
            ckpt = checkpoint_path(f"{target.name}_survival", stage, str(chunk_idx))
            cached = load_checkpoint(ckpt)
            if cached is not None:
                logger.info("chunk %d/%d: loaded from checkpoint (%d rows)", chunk_idx + 1, len(chunk_list), len(cached))
                continue

            ohlcv_chunk = conn.execute(
                """
                SELECT date, ticker, open, high, low, close, volume,
                       COALESCE(delivery_pct, 0.0) AS delivery_pct
                FROM ohlcv_adjusted
                WHERE date >= CURRENT_DATE - INTERVAL (?) DAY
                  AND ticker = ANY(?)
                ORDER BY ticker, date
                """,
                [lookback_days, chunk_tickers],
            ).df()
            if ohlcv_chunk.empty:
                save_checkpoint(pd.DataFrame(), ckpt)
                continue
            ohlcv_chunk["date"] = pd.to_datetime(ohlcv_chunk["date"])
            ohlcv_chunk = ohlcv_chunk.sort_values(["ticker", "date"]).reset_index(drop=True)

            pnd_scores_chunk = _score_pnd_panel(ohlcv_chunk)
            combined_chunk = _build_survival_dataset_from_chunk(ohlcv_chunk, benchmark, target, pnd_scores_chunk)
            save_checkpoint(combined_chunk, ckpt)
            logger.info("chunk %d/%d done: %d rows from %d tickers", chunk_idx + 1, len(chunk_list), len(combined_chunk), len(chunk_tickers))

        combined = load_all_checkpoints(f"{target.name}_survival", stage)

    if combined.empty:
        raise RuntimeError(f"{target.name}: no training rows survived chunked processing (survival dataset)")
    return combined.sort_values(["ticker", "date"]).reset_index(drop=True)


def train_gainer_survival_variant(
    target: GainerSignalTarget,
    benchmark: pd.DataFrame,
    lookback_days: int,
    db_path=None,
    tickers: Optional[list] = None,
    ticker_chunk_size: int = DEFAULT_TICKER_CHUNK_SIZE,
    n_estimators: int = 100,
    seed: int = 42,
) -> Dict:
    """Trains one GainerSurvivalHead on the full (non-split) dataset —
    development-phase proof-of-concept, not a held-out-validated release
    candidate (no val split, no save-to-registry, matching this script's
    documented development-only scope)."""
    combined = _build_survival_dataset_chunked(
        target, benchmark, lookback_days, db_path=db_path, tickers=tickers, ticker_chunk_size=ticker_chunk_size,
    )

    X = combined[CORE_TECHNICAL_FEATURES]
    head = GainerSurvivalHead(n_estimators=n_estimators, random_state=seed)
    diagnostics = head.fit(X, combined["_first_touch_day"], combined["_label"], horizon_days=target.horizon_days)

    sample_days = sorted({1, target.horizon_days // 2, target.horizon_days})
    sample_preds = head.predict_survival_at_days(X.head(5), days=sample_days)

    logger.info(
        "%s survival head trained: %d rows, event_rate=%.3f, concordance=%.3f",
        target.name, diagnostics["training_samples"], diagnostics["event_rate"], diagnostics["concordance_index"],
    )
    logger.info("Sample survival-curve predictions (first 5 rows):\n%s", sample_preds.to_string())

    return {"model": head, "diagnostics": diagnostics, "sample_predictions": sample_preds, "training_rows": len(combined)}


def train_all_gainer_survival(
    lookback_days: int = 400,
    db_path=None,
    tickers: Optional[list] = None,
    ticker_chunk_size: int = DEFAULT_TICKER_CHUNK_SIZE,
    n_estimators: int = 100,
    seed: int = 42,
    targets: Optional[List[GainerSignalTarget]] = None,
) -> Dict[str, Dict]:
    """targets defaults to SURVIVAL_TARGETS (both 21d and 63d); pass a
    filtered list (e.g. from the --horizon CLI flag) to train only one."""
    targets = targets if targets is not None else SURVIVAL_TARGETS
    db_path = db_path or DUCKDB_PATH
    with get_duckdb_connection(db_path, read_only=True, persist=False) as conn:
        dates_df = conn.execute(
            "SELECT DISTINCT date FROM ohlcv_adjusted WHERE date >= CURRENT_DATE - INTERVAL (?) DAY ORDER BY date",
            [lookback_days],
        ).df()
    if dates_df.empty:
        raise RuntimeError("ohlcv_adjusted is empty — run ingestion/backfill_runner.py first")
    dates = pd.DatetimeIndex(pd.to_datetime(dates_df["date"]))
    benchmark = load_benchmark_from_db(dates=dates, db_path=db_path)

    results = {}
    for target in targets:
        results[target.name] = train_gainer_survival_variant(
            target, benchmark, lookback_days, db_path=db_path, tickers=tickers,
            ticker_chunk_size=ticker_chunk_size, n_estimators=n_estimators, seed=seed,
        )
    return results


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="ML33 development: train the gainer 21d/63d RSF survival head")
    parser.add_argument("--horizon", type=int, choices=[21, 63], default=None, help="Restrict to one horizon; omit for both")
    parser.add_argument("--lookback-days", type=int, default=400)
    parser.add_argument("--ticker-chunk-size", type=int, default=DEFAULT_TICKER_CHUNK_SIZE)
    parser.add_argument("--tickers", type=str, default=None, help="Comma-separated ticker list, for a small/quick dev sample")
    parser.add_argument("--n-estimators", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    tickers = [t.strip() for t in args.tickers.split(",")] if args.tickers else None
    targets = SURVIVAL_TARGETS if args.horizon is None else [t for t in SURVIVAL_TARGETS if t.horizon_days == args.horizon]

    results = train_all_gainer_survival(
        lookback_days=args.lookback_days, tickers=tickers, ticker_chunk_size=args.ticker_chunk_size,
        n_estimators=args.n_estimators, seed=args.seed, targets=targets,
    )

    for name, res in results.items():
        print(f"{name}: {res['diagnostics']}")


if __name__ == "__main__":
    main()
