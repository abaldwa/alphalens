"""
systems/ml_signal_engine/inference/score_multibagger.py

Phase: 2.6 (Phase 2 Data Source Integration)
Specs: SPEC-MODEL-001, SPEC-UI-003
Owner: ml_signal_engine / inference
Consumers: operator CLI (`python3 -m systems.ml_signal_engine.inference.score_multibagger`)

Computes M-08's MultibaggerModel output (mb_probability, mb_tier,
mb_archetype, survival curves) for real tickers from their real OHLCV
history, and writes each result through the DataStore API's POST
/api/v1/signals/ml/multibagger/write — the previously-missing link
between P2.4's MultibaggerModel (systems/ml_signal_engine/models/
multibagger/) and the ml_multibagger table (existed in schema since
P0.2, fed datastore/api/routers/watchlist.py's stub, but had no writer
until this script — see watchlist.py's P2.6 module docstring).

Same "train once at scoring time" pattern score_forensic.py uses for
M-10: trains a fresh MultibaggerModel on real OHLCV via
multibagger_model.load_multibagger_training_data_from_db() (see that
module's docstring and BuildLog.md "Real data sourcing — Multibagger") —
no separate persisted model artifact exists yet.

Institutional/governance enrichment (mf_snapshot, governance_snapshot —
compute_multibagger_features' optional params) is intentionally omitted
here: wiring those in means duplicating matrix_builder.py's MF-holdings/
governance panel-fetch machinery in a second place for marginal feature
coverage gain; institutional_accumulation_flag and the few
governance-snapshot-driven features stay honest NaN in this script's
output, same documented-gap discipline as everywhere else in this
codebase (the underlying compute_multibagger_features() call is
unaffected — these are simply optional args left at their None default).
"""

import argparse
import logging
from typing import Dict, List, Optional

import pandas as pd

from config.timezone import now_ist
from config.universe import load_universe_raw
from datastore.client import DataStoreClient
from features.multibagger import MULTIBAGGER_FEATURES, compute_multibagger_features
from features.technical import BENCHMARK_TICKERS
from systems.ml_signal_engine.models.multibagger.multibagger_model import (
    MultibaggerModel,
    load_multibagger_training_data_from_db,
)

logger = logging.getLogger(__name__)

_LOOKBACK_DAYS = 760  # >= 756 trading days per SPEC-MODEL-001 (~3 calendar years of buffer)


def _fetch_ohlcv_panel(client: DataStoreClient, tickers: List[str], as_of) -> pd.DataFrame:
    """Long-format OHLCV panel (date, ticker, open, high, low, close, volume) for `tickers`."""
    from_date = as_of - pd.Timedelta(days=_LOOKBACK_DAYS)
    frames = []
    for ticker in tickers:
        try:
            rows = client.get_ohlcv(ticker, from_date, as_of)
        except Exception as exc:
            logger.warning(f"OHLCV fetch failed for {ticker}: {exc}")
            continue
        if rows:
            df = pd.DataFrame(rows)
            df["ticker"] = ticker
            frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["date", "ticker", "open", "high", "low", "close", "volume"])
    panel = pd.concat(frames, ignore_index=True)
    panel["date"] = pd.to_datetime(panel["date"])
    return panel


def _fetch_benchmark_wide(client: DataStoreClient, as_of) -> Optional[pd.DataFrame]:
    """Wide-format date + {name}_close panel for compute_multibagger_features' relative-strength inputs."""
    from_date = as_of - pd.Timedelta(days=_LOOKBACK_DAYS)
    frames = {}
    for name, ticker in BENCHMARK_TICKERS.items():
        try:
            rows = client.get_ohlcv(ticker, from_date, as_of)
        except Exception as exc:
            logger.warning(f"Benchmark OHLCV fetch failed for {ticker}: {exc}")
            continue
        if rows:
            df = pd.DataFrame(rows)[["date", "close"]].rename(columns={"close": f"{name}_close"})
            df["date"] = pd.to_datetime(df["date"])
            frames[name] = df
    if not frames:
        return None
    wide = None
    for df in frames.values():
        wide = df if wide is None else wide.merge(df, on="date", how="outer")
    return wide.sort_values("date") if wide is not None else None


def score_universe(
    tickers: List[str],
    client: Optional[DataStoreClient] = None,
    model: Optional[MultibaggerModel] = None,
    write: bool = True,
) -> Dict[str, bool]:
    """
    Score every ticker in `tickers` and (if write=True) upsert each
    result via POST /api/v1/signals/ml/multibagger/write.

    Parameters
    ----------
    tickers : list of str
    client : DataStoreClient, optional
    model : MultibaggerModel, optional
        Injected (already-trained) for testability/reuse; defaults to
        training a fresh one via load_multibagger_training_data_from_db().
    write : bool

    Returns
    -------
    dict
        ticker -> True if scoring (+ write, if requested) succeeded,
        False if it failed (including "no OHLCV history at all" —
        distinct from a real error, but still not scoreable). One bad
        ticker never aborts the batch.
    """
    client = client or DataStoreClient()
    if model is None:
        X_train, y_train, duration, event, groups, _pnd = load_multibagger_training_data_from_db()
        model = MultibaggerModel()
        model.train_full(X_train, y_train, duration, event, groups=groups)

    as_of = pd.Timestamp(now_ist().date())
    run_date = as_of.date()

    universe = load_universe_raw()
    sector_map = dict(zip(universe["ticker"], universe["sector"]))

    ohlcv_panel = _fetch_ohlcv_panel(client, tickers, as_of)
    benchmark_wide = _fetch_benchmark_wide(client, as_of)

    if ohlcv_panel.empty:
        logger.warning("No OHLCV data for any requested ticker — nothing to score")
        return {t: False for t in tickers}

    features = compute_multibagger_features(ohlcv_panel, benchmark_wide, sector_map)
    latest_per_ticker = features.sort_values("date").groupby("ticker").tail(1).set_index("ticker")

    results: Dict[str, bool] = {}
    for ticker in tickers:
        if ticker not in latest_per_ticker.index:
            results[ticker] = False
            continue
        try:
            X = latest_per_ticker.loc[[ticker], MULTIBAGGER_FEATURES]
            scored = model.predict_full(X)
            row = scored.iloc[0]

            if write:
                client.write_multibagger_score(
                    {
                        "date": run_date.isoformat(),
                        "ticker": ticker,
                        "mb_probability": _none_if_nan(row["mb_probability"]),
                        "mb_tier": row["mb_tier"],
                        "mb_archetype": row["mb_archetype"],
                        "survival_6m": _none_if_nan(row["mb_survival_6m"]),
                        "survival_12m": _none_if_nan(row["mb_survival_12m"]),
                        "survival_18m": _none_if_nan(row["mb_survival_18m"]),
                        "survival_24m": _none_if_nan(row["mb_survival_24m"]),
                        "survival_36m": _none_if_nan(row["mb_survival_36m"]),
                    }
                )
            results[ticker] = True
        except Exception as exc:
            logger.warning(f"score_multibagger failed for {ticker}: {exc}")
            results[ticker] = False

    return results


def _none_if_nan(value) -> Optional[float]:
    return None if pd.isna(value) else float(value)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="P2.6: score the universe's multibagger probability and write ml_multibagger"
    )
    parser.add_argument("--tickers", help="Comma-separated ticker list (default: full universe)")
    parser.add_argument("--limit", type=int, default=None, help="Cap the number of tickers scored")
    parser.add_argument("--no-write", action="store_true", help="Score only, skip the API writes")
    args = parser.parse_args()

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    else:
        from config.universe import get_tickers

        tickers = get_tickers()
    if args.limit:
        tickers = tickers[: args.limit]

    print(f"Scoring {len(tickers)} tickers (write={not args.no_write})...", flush=True)
    results = score_universe(tickers, write=not args.no_write)
    n_ok = sum(1 for ok in results.values() if ok)
    print(f"Done: {n_ok}/{len(tickers)} succeeded.", flush=True)
    failed = [t for t, ok in results.items() if not ok]
    if failed:
        print(f"Failed: {failed}", flush=True)


if __name__ == "__main__":
    main()
