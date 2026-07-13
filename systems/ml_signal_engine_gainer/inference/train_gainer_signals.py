"""
systems/ml_signal_engine_gainer/inference/train_gainer_signals.py

GAINER EXPERIMENT (not used by production; does not touch datastore/models/
signal_5d|21d|63d/ or datastore/models/registry.json).

Trains the 3 short-horizon "gainer" models (5%/6d, 10%/21d, 20%/63d) using
FixedPercentLabeler (fixed single-touch % target, no ATR scaling — see
training/labeling.py) instead of production's ATR-scaled TripleBarrierLabeler,
so results can be compared side by side against signal_5d/21d/63d.

Reuses read-only, non-experimental infra from production: OHLCV/benchmark
loaders and CORE_TECHNICAL_FEATURES computation (features/technical.py),
and the cached PnDDetector artifact for circuit/P&D exclusion (same
_score_pnd_panel pattern as multibagger_model.py) — none of this is part
of what's being experimented on, only the labeling scheme and the model
instances are new/copied.
"""

import argparse
import json
import logging
from typing import Dict, List, NamedTuple

import pandas as pd

from config.settings import DUCKDB_PATH, MODELS_DIR
from config.timezone import now_ist
from datastore.api.db import get_duckdb_connection
from features.technical import CORE_TECHNICAL_FEATURES, compute_technical_features
from systems.ml_signal_engine.inference.train_all_phase1 import load_benchmark_from_db
from systems.ml_signal_engine_gainer.inference.checkpoint_utils import (
    checkpoint_path, load_all_checkpoints, load_checkpoint, save_checkpoint, ticker_chunks,
)
from systems.ml_signal_engine_gainer.models.multibagger.multibagger_model import _score_pnd_panel
from systems.ml_signal_engine_gainer.models.signal.gainer_signal_models import (
    GainerSignal6DModel,
    GainerSignal21DModel,
    GainerSignal63DModel,
)
from systems.ml_signal_engine_gainer.training.labeling import FixedPercentLabeler
from systems.ml_signal_engine_gainer.training.walk_forward import WalkForwardValidator

logger = logging.getLogger(__name__)

GAINER_MODELS_DIR = MODELS_DIR / "_gainer_experiment"
MODEL_VERSION_DATE_FORMAT = "%Y%m%d"
DEFAULT_TICKER_CHUNK_SIZE = 150


class GainerSignalTarget(NamedTuple):
    name: str
    horizon_days: int
    target_pct: float
    model_cls: type


GAINER_SIGNAL_TARGETS: List[GainerSignalTarget] = [
    GainerSignalTarget("gainer_signal_6d", 6, 0.05, GainerSignal6DModel),
    GainerSignalTarget("gainer_signal_21d", 21, 0.10, GainerSignal21DModel),
    GainerSignalTarget("gainer_signal_63d", 63, 0.20, GainerSignal63DModel),
]


def _build_gainer_training_dataset_from_chunk(
    ohlcv_chunk: pd.DataFrame, benchmark: pd.DataFrame, target: GainerSignalTarget, pnd_scores_chunk: pd.Series,
) -> pd.DataFrame:
    """One combined DataFrame for a single ticker chunk: date, ticker,
    CORE_TECHNICAL_FEATURES, _label (0/1 -> mapped to BaseSignalModel's
    {0, 1}, i.e. HOLD/BUY only), _return (realized forward max return,
    for the near-miss-magnitude metric AND the quantile-regression target)."""
    features = compute_technical_features(ohlcv_chunk, benchmark)

    labeler = FixedPercentLabeler(horizon_days=target.horizon_days, target_pct=target.target_pct)
    merged = ohlcv_chunk.assign(pnd_score=pnd_scores_chunk.to_numpy())
    merged["pnd_block"] = merged["pnd_score"].fillna(0) > 40  # PND_FLAG_THRESHOLD, same convention as multibagger

    label_df = labeler.label_panel(merged, close_col="close", ticker_col="ticker", pnd_block_col="pnd_block")

    combined = features.copy()
    combined["_label"] = label_df["label"].to_numpy()
    combined["_return"] = label_df["max_return"].to_numpy()
    combined = combined.dropna(subset=["_label", "_return"]).reset_index(drop=True)
    return combined


def _build_gainer_training_dataset_chunked(
    target: GainerSignalTarget,
    benchmark: pd.DataFrame,
    lookback_days: int,
    db_path=None,
    tickers: list = None,
    ticker_chunk_size: int = DEFAULT_TICKER_CHUNK_SIZE,
) -> pd.DataFrame:
    """
    CHUNKED + CHECKPOINTED (addresses the frequent-persistence and
    moderated-chunk-size requirements — same pattern as
    multibagger_model.py's load_multibagger_training_data_from_db):
    processes the universe in ticker chunks (default 150/chunk), so at
    most one chunk's OHLCV + technical features + labels is ever
    memory-resident; each chunk's finished rows are persisted to a
    parquet checkpoint immediately, and a re-run resumes from whatever
    chunks already finished instead of recomputing them.
    """
    db_path = db_path or DUCKDB_PATH
    stage = f"h{target.horizon_days}_t{target.target_pct}"

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
            "%s: %d tickers in %d chunks of <=%d", target.name, len(all_tickers), len(chunk_list), ticker_chunk_size,
        )

        for chunk_idx, chunk_tickers in enumerate(chunk_list):
            ckpt = checkpoint_path(target.name, stage, str(chunk_idx))
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
            combined_chunk = _build_gainer_training_dataset_from_chunk(ohlcv_chunk, benchmark, target, pnd_scores_chunk)
            save_checkpoint(combined_chunk, ckpt)
            logger.info("chunk %d/%d done: %d rows from %d tickers", chunk_idx + 1, len(chunk_list), len(combined_chunk), len(chunk_tickers))

        combined = load_all_checkpoints(target.name, stage)

    if combined.empty:
        raise RuntimeError(f"{target.name}: no training rows survived chunked processing")
    return combined.sort_values(["ticker", "date"]).reset_index(drop=True)


def _save_model_gainer(model, name: str, run_date, registry: Dict, metadata_extra=None):
    model_dir = GAINER_MODELS_DIR / name
    model_dir.mkdir(parents=True, exist_ok=True)
    version = run_date.strftime(MODEL_VERSION_DATE_FORMAT)
    versioned_path = model_dir / f"{name}_v{version}_fold0.pkl"
    current_path = model_dir / f"{name}_current.pkl"
    model.save(str(versioned_path))
    model.save(str(current_path))
    meta = model.metadata() if hasattr(model, "metadata") else {}
    meta = {**meta, **(metadata_extra or {})}
    meta["saved_path"] = str(versioned_path)
    meta["saved_at"] = run_date.isoformat()
    registry[name] = meta
    logger.info(f"Saved {name} -> {versioned_path}")
    return versioned_path


def _write_gainer_registry(registry: Dict) -> None:
    GAINER_MODELS_DIR.mkdir(parents=True, exist_ok=True)
    registry_path = GAINER_MODELS_DIR / "registry.json"
    existing = {}
    if registry_path.exists():
        try:
            existing = json.loads(registry_path.read_text())
        except (json.JSONDecodeError, OSError):
            existing = {}
    existing.update({k: (v if not hasattr(v, "isoformat") else v.isoformat()) for k, v in registry.items()})
    registry_path.write_text(json.dumps(existing, indent=2, default=str))
    logger.info("Updated gainer-experiment model registry: %s", registry_path)


def train_gainer_signal_variant(
    target: GainerSignalTarget,
    benchmark: pd.DataFrame,
    lookback_days: int,
    db_path=None,
    tickers: list = None,
    ticker_chunk_size: int = DEFAULT_TICKER_CHUNK_SIZE,
    optuna_trials: int = 20,
    save: bool = True,
    seed: int = 42,
) -> Dict:
    run_date = now_ist()
    combined = _build_gainer_training_dataset_chunked(
        target, benchmark, lookback_days, db_path=db_path, tickers=tickers, ticker_chunk_size=ticker_chunk_size,
    )

    validator = WalkForwardValidator(n_folds=2)
    n_folds_data = combined["date"].dt.year.nunique() - 1
    if n_folds_data < 1:
        train_df, val_df = validator.get_train_validation_split(combined, val_fraction=0.3)
    else:
        folds = validator.split_data(combined, n_folds=min(2, n_folds_data))
        train_fold, _test_fold = folds[0]
        train_df, val_df = validator.get_train_validation_split(train_fold, val_fraction=0.2)

    model = target.model_cls(optuna_trials=optuna_trials, random_state=seed)
    diag = model.train_full(
        train_df[CORE_TECHNICAL_FEATURES], train_df["_label"],
        val_df[CORE_TECHNICAL_FEATURES], val_df["_label"],
        returns_train=train_df["_return"], returns_val=val_df["_return"],
    )
    logger.info(f"{target.name} trained: {diag['thresholds']}")

    registry: Dict = {}
    if save:
        _save_model_gainer(
            model, target.name, run_date, registry,
            metadata_extra={"diagnostics": diag, "horizon_days": target.horizon_days, "target_pct": target.target_pct},
        )
        _write_gainer_registry(registry)

    return {"registry": registry, "diagnostics": diag, "model": model, "val_df": val_df}


def train_all_gainer_signals(
    lookback_days: int = 1260, optuna_trials: int = 20, save: bool = True, seed: int = 42, db_path=None,
    tickers: list = None, ticker_chunk_size: int = DEFAULT_TICKER_CHUNK_SIZE,
) -> Dict[str, Dict]:
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
    for target in GAINER_SIGNAL_TARGETS:
        results[target.name] = train_gainer_signal_variant(
            target, benchmark, lookback_days, db_path=db_path, tickers=tickers,
            ticker_chunk_size=ticker_chunk_size, optuna_trials=optuna_trials, save=save, seed=seed,
        )
    return results


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="[GAINER EXPERIMENT] Train 5%/6d, 10%/21d, 20%/63d fixed-pct signal models")
    parser.add_argument("--lookback-days", type=int, default=1260)
    parser.add_argument("--optuna-trials", type=int, default=20)
    args = parser.parse_args()

    results = train_all_gainer_signals(lookback_days=args.lookback_days, optuna_trials=args.optuna_trials)
    for name, result in results.items():
        diag = result["diagnostics"]
        print(f"\n{name}: thresholds={diag['thresholds']}, val_f1={diag['val_f1_per_class']}")


if __name__ == "__main__":
    main()
