"""
systems/ml_signal_engine/inference/score_forensic.py

Phase: 2.6 (Phase 2 Data Source Integration)
Specs: SPEC-MODEL-009, SPEC-MODEL-010
Owner: ml_signal_engine / inference
Consumers: operator CLI (`python3 -m systems.ml_signal_engine.inference.score_forensic`)

Computes M-09/M-10's full 4-layer forensic risk score (classical 20% +
ML fraud 40% + anomaly 20% + governance 20% — forensic_ml.py's
COMPOSITE_WEIGHTS) for real tickers and writes each result through the
DataStore API's POST /api/v1/signals/ml/forensic/write — the
previously-missing link between P2.5's forensic models
(systems/ml_signal_engine/models/forensic/) and the ml_forensic table
(which existed in schema since P0.2 but had no writer until this script).

Same "anchor on the real archive, train once at scoring time" pattern
P2.5's forensic_ml.py already validated end-to-end (KNOWN_FRAUD_ARCHIVE /
KNOWN_CLEAN_ARCHIVE plus real DB-sourced clean tickers via
load_forensic_training_data_from_db — see that module's docstring and
BuildLog.md "Real data sourcing — Forensic ML"). No separate persisted
model artifact / retrain script exists for M-10 yet (out of this
prompt's scope; ForensicMLModel trains in well under a second, so
retraining once per scoring run is cheap and always reflects the
current archive + universe).

Honest data-coverage note: the live screener.py scraper does not capture
current_assets/current_liabilities/fcf/capex/gross_profit (documented gap,
see features/forensic_classical.py's module docstring) — Altman Z-Score
and several Group B/D classical/ML features are therefore NaN for every
real ticker scored by this script today, not a bug in this script. The
ML fraud-probability layer (LightGBM/XGBoost on all 84 features, NaN-
native) and the governance layer (real shareholding/promoter-pledge data)
still produce a real, non-degenerate forensic_composite even when the
classical layer is entirely NaN — forensic_classical_composite's own
renormalization (features/forensic_classical.py's
compute_forensic_classical_scores -> forensic_classical_composite)
excludes missing layers from the weighted average rather than treating
a NaN classical layer as zero risk.
"""

import argparse
import logging
from typing import Dict, List, Optional

import pandas as pd

from config.timezone import now_ist
from datastore.client import DataStoreClient
from features.forensic_classical import compute_forensic_classical_scores
from systems.ml_signal_engine.models.forensic.forensic_ml import (
    FORENSIC_ML_FEATURES,
    ForensicMLModel,
    compute_forensic_ml_features,
    compute_governance_score,
    load_forensic_training_data_from_db,
)

logger = logging.getLogger(__name__)


def score_universe(
    tickers: List[str],
    client: Optional[DataStoreClient] = None,
    model: Optional[ForensicMLModel] = None,
    write: bool = True,
) -> Dict[str, bool]:
    """
    Score every ticker in `tickers` and (if write=True) upsert each
    result via POST /api/v1/signals/ml/forensic/write.

    Parameters
    ----------
    tickers : list of str
    client : DataStoreClient, optional
        Injected for testability; defaults to a real DataStoreClient.
    model : ForensicMLModel, optional
        Injected (already-trained) for testability/reuse across calls;
        defaults to training a fresh one via
        load_forensic_training_data_from_db(client, clean_tickers=tickers).
    write : bool
        If True (default), upserts via client.write_forensic_score. If
        False, scores only (used by tests).

    Returns
    -------
    dict
        ticker -> True if scoring (+ write, if requested) succeeded,
        False if it failed. One bad ticker never aborts the batch (same
        per-ticker isolation as every other batch scraper/scorer in this
        codebase).
    """
    client = client or DataStoreClient()
    if model is None:
        X_train, y_train = load_forensic_training_data_from_db(client=client, clean_tickers=tickers)
        model = ForensicMLModel()
        model.train_full(X_train, y_train)

    as_of = now_ist()
    run_date = as_of.date()
    results: Dict[str, bool] = {}

    for ticker in tickers:
        try:
            ml_features = compute_forensic_ml_features(client, ticker, as_of)
            classical = compute_forensic_classical_scores(client, ticker, as_of)
            governance_score = compute_governance_score(ml_features)

            X = pd.DataFrame([ml_features])[FORENSIC_ML_FEATURES]
            classical_series = pd.Series([classical["forensic_classical_composite"]])
            governance_series = pd.Series([governance_score])
            full = model.predict_full(X, classical_series, governance_series)
            row = full.iloc[0]

            if write:
                client.write_forensic_score(
                    {
                        "date": run_date.isoformat(),
                        "ticker": ticker,
                        "beneish_m": classical["m_score"],
                        "altman_z": classical["z_score"],
                        "piotroski_f": classical["f_score"],
                        "ohlson_o": classical["o_score"],
                        "dechow_f": classical["dechow_f_score"],
                        "sloan_accrual": classical["sloan_accrual"],
                        "benford_mad": classical["benford_mad"],
                        "forensic_composite": _none_if_nan(row["forensic_composite"]),
                        "forensic_flag": bool(row["blocked"]),
                        "forensic_flag_label": row["flag"],
                        "forensic_ml_prob": _none_if_nan(row["ml_fraud_probability"]),
                    }
                )
            results[ticker] = True
        except Exception as exc:
            logger.warning(f"score_forensic failed for {ticker}: {exc}")
            results[ticker] = False

    return results


def _none_if_nan(value) -> Optional[float]:
    return None if pd.isna(value) else float(value)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="P2.6: score the universe's forensic risk and write ml_forensic")
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
