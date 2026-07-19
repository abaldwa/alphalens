"""
scripts/backfill_hmm_regime.py

Backfills historical market-wide HMM regime labels (ml_signals, ticker='MARKET',
model_name='hmm_market') so backtest/strategy_confidence.py's regime segmentation
has real regime history to work with, instead of ~12 real rows (2026-07-02 onward)
against ~4,800 signal dates.

Why this ISN'T just "call predict_regime() once for every historical date":
the production model (systems/ml_signal_engine/inference/train_all_phase1.py)
is fit ONCE on a single trailing window and reused for daily inference until the
next scheduled retrain (config.settings.DEFAULT_TRAINING_INTERVAL_DAYS, 28 days).
Naively fitting one HMM on the ENTIRE 2006-2026 NIFTYBEES history and decoding
every historical date with it would leak later-history statistics (state means/
covariances derived from data that didn't exist yet) into early "historical"
regime labels — exactly the kind of look-ahead bias
backtest/integrity_checker.py exists to catch elsewhere in this repo.

Instead this script WALKS FORWARD, replaying production's own retrain cadence
historically: refit a fresh HMMRegimeDetector every `--refit-interval-days`
using only NIFTYBEES observables on/before that refit date (trailing
`--lookback-days`), then decode only the dates in the following block with
that fixed model — the same "fit on the past, apply going forward" contract
the live system already uses, just replayed date-by-date instead of only at
today's cutover.

Idempotent (ml_signals upserts ON CONFLICT), skips dates already present in
ml_signals for model_name='hmm_market' by default.

Usage:
    python -m scripts.backfill_hmm_regime
    python -m scripts.backfill_hmm_regime --start-date 2010-01-01
    python -m scripts.backfill_hmm_regime --force   # re-decode already-covered dates
"""
from __future__ import annotations

import argparse
import logging
import time
from datetime import date

import numpy as np
import pandas as pd

from config.settings import DEFAULT_TRAINING_INTERVAL_DAYS, DUCKDB_PATH, SIGNALS_DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from systems.ml_signal_engine.models.hmm.regime_detector import (
    HMMRegimeDetector,
    MIN_OBSERVATIONS,
    compute_hmm_observables,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

HMM_MARKET_TICKER = "MARKET"
HMM_MODEL_NAME = "hmm_market"
MODEL_VERSION = "backfill-1.0"
REGIME_RANK_NAMES = {0.0: "bearish", 1.0: "sideways", 2.0: "volatile", 3.0: "bullish"}

# Matches train_all_phase1.py's default trailing-fit window (~5 years).
DEFAULT_LOOKBACK_DAYS = 1260

_CREATE_ML_SIGNALS_MINIMAL = """
CREATE TABLE IF NOT EXISTS ml_signals (
    date DATE NOT NULL,
    ticker VARCHAR NOT NULL,
    model_name VARCHAR NOT NULL,
    model_version VARCHAR NOT NULL,
    hmm_regime VARCHAR,
    hmm_regime_prob DOUBLE,
    hmm_stability DOUBLE,
    PRIMARY KEY (date, ticker, model_name)
)
"""

_BULK_UPSERT_SQL = """
INSERT INTO ml_signals (date, ticker, model_name, model_version, hmm_regime, hmm_regime_prob, hmm_stability)
SELECT date, ticker, model_name, model_version, hmm_regime, hmm_regime_prob, hmm_stability
FROM _hmm_regime_upsert_batch
ON CONFLICT (date, ticker, model_name) DO UPDATE SET
    model_version = excluded.model_version,
    hmm_regime = excluded.hmm_regime,
    hmm_regime_prob = excluded.hmm_regime_prob,
    hmm_stability = excluded.hmm_stability
"""


def _load_nifty_observables(start: str, end: str) -> pd.DataFrame:
    """Real NIFTYBEES OHLCV (proxy used by train_all_phase1.py), plus the 5
    HMM observable columns computed once, causally (rolling/shift only look
    backward), from config.settings.DUCKDB_PATH."""
    with get_duckdb_connection(DUCKDB_PATH, persist=False, read_only=True) as conn:
        ohlcv = conn.execute(
            """
            SELECT date, open, high, low, close, volume
            FROM ohlcv_adjusted WHERE ticker = 'NIFTYBEES' AND date <= ?
            ORDER BY date
            """,
            [end],
        ).fetchdf()
    if ohlcv.empty:
        raise RuntimeError("No NIFTYBEES OHLCV found in ohlcv_adjusted — cannot backfill HMM regime")
    ohlcv["ticker"] = "NIFTYBEES"
    obs = compute_hmm_observables(ohlcv)
    return obs[obs["date"] >= pd.Timestamp(start) - pd.Timedelta(days=DEFAULT_LOOKBACK_DAYS)].reset_index(drop=True)


def _existing_hmm_dates() -> set:
    SIGNALS_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
        tables = [r[0] for r in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'ml_signals'"
        ).fetchall()]
        if not tables:
            return set()
        return {
            str(r[0]) for r in conn.execute(
                "SELECT DISTINCT date FROM ml_signals WHERE model_name = ?", [HMM_MODEL_NAME]
            ).fetchall()
        }


def _decode_one_block(detector: HMMRegimeDetector, decode_window: pd.DataFrame) -> pd.DataFrame:
    regimes, probs = detector.predict_regime(decode_window)
    stability = probs.max(axis=1) if probs is not None else pd.Series(np.nan, index=regimes.index)

    rows = []
    for d, rank, stab in zip(decode_window["date"], regimes, stability):
        if pd.isna(rank):
            continue
        rows.append({
            "date": pd.Timestamp(d).date(),
            "ticker": HMM_MARKET_TICKER,
            "model_name": HMM_MODEL_NAME,
            "model_version": MODEL_VERSION,
            "hmm_regime": REGIME_RANK_NAMES.get(float(rank), str(rank)),
            "hmm_regime_prob": float(stab) if pd.notna(stab) else None,
            "hmm_stability": float(stab) if pd.notna(stab) else None,
        })
    return pd.DataFrame(rows)


def _walk_forward_decode(
    obs: pd.DataFrame, start: str, end: str, refit_interval_days: int, lookback_days: int,
    *, on_block_decoded=None,
) -> pd.DataFrame:
    """Refits every `refit_interval_days` on the trailing `lookback_days` of
    obs on/before the refit date, then decodes the following block with that
    fixed model — see module docstring for why this (not a single all-history
    fit) is the leakage-safe replay of production's retrain cadence.

    Each block's rows are handed to `on_block_decoded(block_df)` as soon as
    they're decoded (if given), so a caller can persist them immediately —
    the walk-forward loop itself is the expensive part (one HMM fit every
    `refit_interval_days`, ~170 fits for a 20-year backfill), so holding
    every block in memory until the whole 20-year decode finishes before
    writing anything (as an earlier version of this script did) meant a
    kill partway through lost all progress, not just the in-flight block."""
    all_dates = obs["date"].sort_values().unique()
    target_dates = all_dates[(all_dates >= np.datetime64(start)) & (all_dates <= np.datetime64(end))]
    if len(target_dates) == 0:
        return pd.DataFrame()

    refit_dates = target_dates[::refit_interval_days]  # always includes target_dates[0] (slice starts at index 0)

    block_dfs = []
    for i, refit_date in enumerate(refit_dates):
        block_end = refit_dates[i + 1] if i + 1 < len(refit_dates) else target_dates[-1] + np.timedelta64(1, "D")
        block_mask = (target_dates >= refit_date) & (target_dates < block_end)
        block_dates = target_dates[block_mask]
        if len(block_dates) == 0:
            continue

        train_window = obs[
            (obs["date"] <= pd.Timestamp(refit_date))
            & (obs["date"] > pd.Timestamp(refit_date) - pd.Timedelta(days=lookback_days))
        ]
        detector = HMMRegimeDetector()
        try:
            detector.fit(train_window)
        except ValueError as exc:
            logger.debug("HMM backfill: refit at %s skipped (%s), block left undecoded", refit_date, exc)
            continue

        decode_window = obs[obs["date"].isin(block_dates)]
        block_df = _decode_one_block(detector, decode_window)
        if on_block_decoded is not None:
            on_block_decoded(i + 1, len(refit_dates), block_df)
        block_dfs.append(block_df)

    return pd.concat(block_dfs, ignore_index=True) if block_dfs else pd.DataFrame()


def _persist(conn, batch_df: pd.DataFrame) -> int:
    """Bulk register()+INSERT...SELECT...ON CONFLICT, not per-row executemany
    — see backtest/strategy_confidence.py::persist_detail's docstring for the
    ~250x measured difference this matters for at multi-thousand-row scale."""
    if batch_df.empty:
        return 0
    conn.execute(_CREATE_ML_SIGNALS_MINIMAL)
    conn.register("_hmm_regime_upsert_batch", batch_df)
    try:
        conn.execute(_BULK_UPSERT_SQL)
    finally:
        conn.unregister("_hmm_regime_upsert_batch")
    return len(batch_df)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", default="2007-01-03")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD, default today")
    parser.add_argument("--refit-interval-days", type=int, default=DEFAULT_TRAINING_INTERVAL_DAYS,
                         help=f"Trading-day refit cadence (default matches production: {DEFAULT_TRAINING_INTERVAL_DAYS})")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--force", action="store_true", help="Re-decode dates already present in ml_signals")
    args = parser.parse_args()

    end_date = args.end_date or date.today().isoformat()
    start_date = args.start_date

    obs = _load_nifty_observables(start_date, end_date)
    if obs[obs["date"] >= pd.Timestamp(start_date)].dropna(subset=["daily_return"]).shape[0] < MIN_OBSERVATIONS:
        logger.warning("Not enough NIFTYBEES history before %s to fit an HMM (need >= %d observations)",
                        start_date, MIN_OBSERVATIONS)
        return

    existing = set() if args.force else _existing_hmm_dates()

    logger.info(
        "backfill_hmm_regime: decoding %s to %s, refit every %d trading dates, %d-day lookback",
        start_date, end_date, args.refit_interval_days, args.lookback_days,
    )

    start_time = time.monotonic()
    total_written = 0

    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=False) as conn:
        def _persist_block(block_num: int, n_blocks: int, block_df: pd.DataFrame) -> None:
            nonlocal total_written
            if existing:
                block_df = block_df[~block_df["date"].astype(str).isin(existing)]
            n = _persist(conn, block_df)
            total_written += n
            elapsed = time.monotonic() - start_time
            logger.info("  [%d/%d] refit block persisted (%d rows), elapsed=%.0fs", block_num, n_blocks, n, elapsed)

        decoded = _walk_forward_decode(
            obs, start_date, end_date, args.refit_interval_days, args.lookback_days,
            on_block_decoded=_persist_block,
        )

    if decoded.empty:
        logger.warning("backfill_hmm_regime: no regime rows decoded")
        return

    logger.info(
        "backfill_hmm_regime: done. %d dates decoded, %d rows written, %.0fs elapsed",
        decoded["date"].nunique(), total_written, time.monotonic() - start_time,
    )


if __name__ == "__main__":
    main()
