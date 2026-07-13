"""
systems/ml_signal_engine_gainer/inference/checkpoint_utils.py

GAINER EXPERIMENT: shared chunking + frequent-persistence helpers for all
6 gainer model training pipelines. Addresses two explicit requirements:

1. Feature engineering / training-data / model artifacts must be
   persisted frequently so an OOM kill mid-run doesn't lose completed
   work — each ticker CHUNK's fully-processed (featured, labeled,
   deduped, subsampled) rows are written to its own parquet file as
   soon as that chunk finishes, instead of holding the whole universe's
   intermediate DataFrames in memory and only writing once at the end.
2. Chunk sizes are moderated (DEFAULT_TICKER_CHUNK_SIZE) so at most one
   chunk's OHLCV + features + labels are ever resident in memory at
   once, not the full ~2,300-ticker universe simultaneously — this is
   the same class of OOM this repo has already hit in production (see
   multibagger_model.py's backlog #26/#27 RSF memory notes and the
   signal_63d OOM in logs/retrain_signal_63d_ml24_*.log).

Resumability: if a chunk's checkpoint file already exists on disk, it is
loaded instead of recomputed — a crashed/OOM-killed run can be re-invoked
and will skip every chunk that already finished.
"""

import logging
from pathlib import Path
from typing import Iterator, List, Optional, Sequence

import pandas as pd

from config.settings import MODELS_DIR

logger = logging.getLogger(__name__)

CHECKPOINT_ROOT = MODELS_DIR / "_gainer_experiment" / "checkpoints"
DEFAULT_TICKER_CHUNK_SIZE = 150


def ticker_chunks(tickers: Sequence[str], chunk_size: int = DEFAULT_TICKER_CHUNK_SIZE) -> Iterator[List[str]]:
    """Split a ticker list into moderate-size chunks (default 150 tickers/chunk)
    so OHLCV + feature computation for at most one chunk is ever memory-resident."""
    tickers = sorted(tickers)
    for i in range(0, len(tickers), chunk_size):
        yield tickers[i:i + chunk_size]


def checkpoint_path(pipeline: str, stage: str, chunk_key: str) -> Path:
    """pipeline: e.g. 'multibagger_2x_12m', 'gainer_signal_6d'.
    stage: e.g. 'labeled_features'.
    chunk_key: e.g. a hash or index identifying the ticker chunk."""
    d = CHECKPOINT_ROOT / pipeline / stage
    d.mkdir(parents=True, exist_ok=True)
    return d / f"chunk_{chunk_key}.parquet"


def load_checkpoint(path: Path) -> Optional[pd.DataFrame]:
    if path.exists():
        try:
            return pd.read_parquet(path)
        except Exception as exc:
            logger.warning("load_checkpoint: failed to read %s (%s) — will recompute", path, exc)
            return None
    return None


def save_checkpoint(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".parquet.tmp")
    df.to_parquet(tmp_path, index=False)
    tmp_path.replace(path)  # atomic rename — a partially-written file never looks "done"
    logger.info("save_checkpoint: %s (%d rows)", path, len(df))


def load_all_checkpoints(pipeline: str, stage: str) -> pd.DataFrame:
    """Concatenate every chunk checkpoint for (pipeline, stage) from disk
    (not from any in-memory accumulator) — used once all chunks are done."""
    d = CHECKPOINT_ROOT / pipeline / stage
    if not d.exists():
        return pd.DataFrame()
    parts = [pd.read_parquet(p) for p in sorted(d.glob("chunk_*.parquet"))]
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)
