"""
systems/ml_signal_engine_gainer/inference/report_multibagger_picks.py

GAINER EXPERIMENT: for each trained multibagger variant, scores the most
recent (ticker, date) snapshot per ticker, ranks by mb_probability, and
reports the top-20 picks with:
  - tradeability: ADV (avg daily traded value, INR) vs the user-specified
    liquidity floor, and the round-trip transaction cost % (backtest/costs.py's
    IndianTransactionCosts) at that ADV — so a pick with real signal but
    unrealistic trading frictions is flagged, not silently included.
  - a "confirmed in training data" flag: whether this ticker actually
    achieved the variant's target multiple within the training window
    (event=1) — a DATA-DERIVED sanity check, not a hardcoded list of
    "known multibaggers" (which would risk stating unverified market-
    history claims); this cross-references the model's own labeled
    outcomes, which is the only historically-grounded source available
    in this environment.
"""

import argparse
import json
import logging
from typing import Dict, List

import pandas as pd

from backtest.costs import IndianTransactionCosts
from config.settings import DUCKDB_PATH, MODELS_DIR
from config.universe import load_universe_raw
from datastore.api.db import get_duckdb_connection
from features.multibagger import MULTIBAGGER_FEATURES, compute_multibagger_features
from systems.ml_signal_engine_gainer.inference.checkpoint_utils import load_all_checkpoints
from systems.ml_signal_engine_gainer.inference.train_multibagger import MULTIBAGGER_TARGETS
from systems.ml_signal_engine_gainer.models.multibagger.multibagger_model import MultibaggerModel

logger = logging.getLogger(__name__)

GAINER_MODELS_DIR = MODELS_DIR / "_gainer_experiment"
TOP_N = 20


def _latest_snapshot_features(lookback_days: int, tickers: List[str], db_path=None) -> pd.DataFrame:
    """Real OHLCV -> real multibagger features, one row per ticker at its most recent date."""
    db_path = db_path or DUCKDB_PATH
    with get_duckdb_connection(db_path, read_only=True, persist=False) as conn:
        ohlcv = conn.execute(
            """
            SELECT date, ticker, open, high, low, close, volume,
                   COALESCE(delivery_pct, 0.0) AS delivery_pct
            FROM ohlcv_adjusted
            WHERE date >= CURRENT_DATE - INTERVAL (?) DAY
              AND ticker = ANY(?)
            ORDER BY ticker, date
            """,
            [lookback_days, tickers],
        ).df()
    if ohlcv.empty:
        raise RuntimeError("no OHLCV rows for the given tickers/lookback")
    ohlcv["date"] = pd.to_datetime(ohlcv["date"])
    ohlcv = ohlcv.sort_values(["ticker", "date"]).reset_index(drop=True)

    try:
        universe = load_universe_raw()
        sector_map = dict(zip(universe["ticker"], universe["sector"]))
    except Exception:
        sector_map = {}

    features_df = compute_multibagger_features(ohlcv, sector_map=sector_map)
    features_df["date"] = pd.to_datetime(features_df["date"])
    latest = features_df.sort_values(["ticker", "date"]).groupby("ticker", as_index=False).tail(1)
    return latest.reset_index(drop=True)


def _confirmed_tickers(target_name: str, stage_glob_prefix: str) -> set:
    """Tickers with event=1 anywhere in this variant's own checkpointed training labels
    — the data-derived 'this really happened in our history' cross-check."""
    from systems.ml_signal_engine_gainer.inference.checkpoint_utils import CHECKPOINT_ROOT

    base = CHECKPOINT_ROOT / target_name
    if not base.exists():
        return set()
    confirmed = set()
    for stage_dir in base.iterdir():
        if not stage_dir.is_dir() or not stage_dir.name.startswith(stage_glob_prefix):
            continue
        df = load_all_checkpoints(target_name, stage_dir.name)
        if not df.empty and "event" in df.columns:
            confirmed |= set(df.loc[df["event"] == 1, "ticker"].unique())
    return confirmed


def generate_report(
    tickers: List[str], adv_lookup: Dict[str, float], liquidity_floor_inr: float = 10_000_000,
    lookback_days: int = 1260, db_path=None,
) -> Dict:
    """
    Parameters
    ----------
    tickers : list[str]
    adv_lookup : dict[ticker -> average daily traded value, INR]
    liquidity_floor_inr : float
        Minimum ADV (INR) for a pick to count as tradeable. Default 1e7
        (Rs 1 crore/day, per user decision — NOTE this differs from
        config.settings.MIN_ADT_INR, which is Rs 10 lakh; both are
        reported so the discrepancy is visible, not silently resolved).
    """
    costs = IndianTransactionCosts()
    latest_features = _latest_snapshot_features(lookback_days=lookback_days, tickers=tickers, db_path=db_path)

    report = {}
    for target in MULTIBAGGER_TARGETS:
        model_path = GAINER_MODELS_DIR / target.name / f"{target.name}_current.pkl"
        if not model_path.exists():
            logger.warning(f"{target.name}: no saved model at {model_path} — skipped")
            continue
        model = MultibaggerModel()
        model.load(str(model_path))

        X = latest_features[MULTIBAGGER_FEATURES]
        proba = model.predict(X)
        scored = latest_features[["date", "ticker"]].assign(mb_probability=proba.to_numpy())
        scored = scored.sort_values("mb_probability", ascending=False).head(TOP_N).reset_index(drop=True)

        confirmed = _confirmed_tickers(target.name, "stride")

        picks = []
        for _, row in scored.iterrows():
            adv = adv_lookup.get(row["ticker"])
            adv_cr = (adv / 1e7) if adv is not None else None
            roundtrip_pct = costs.compute_roundtrip_cost_pct(price=1000.0, quantity=100, adtv_cr=adv_cr) if adv_cr else None
            picks.append({
                "ticker": row["ticker"],
                "as_of_date": str(row["date"].date()),
                "mb_probability": round(float(row["mb_probability"]), 4),
                "adv_inr": adv,
                "adv_inr_cr": round(adv_cr, 2) if adv_cr is not None else None,
                "tradeable": bool(adv is not None and adv >= liquidity_floor_inr),
                "roundtrip_cost_pct": round(roundtrip_pct, 4) if roundtrip_pct is not None else None,
                "confirmed_multibagger_in_training_history": row["ticker"] in confirmed,
            })
        report[target.name] = {
            "liquidity_floor_inr": liquidity_floor_inr,
            "min_adt_inr_setting": 1_000_000,  # config.settings.MIN_ADT_INR, for comparison
            "n_tradeable": sum(1 for p in picks if p["tradeable"]),
            "n_confirmed_in_history": sum(1 for p in picks if p["confirmed_multibagger_in_training_history"]),
            "picks": picks,
        }
    return report


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="[GAINER EXPERIMENT] MultiBagger top-20 picks + tradeability report")
    parser.add_argument("--tickers-csv", type=str, required=True, help="CSV with columns ticker, avg_turnover")
    parser.add_argument("--liquidity-floor-inr", type=float, default=10_000_000)
    parser.add_argument("--lookback-days", type=int, default=1260)
    parser.add_argument("--out", type=str, default="/tmp/multibagger_picks_report.json")
    args = parser.parse_args()

    df = pd.read_csv(args.tickers_csv)
    tickers = df["ticker"].tolist()
    adv_lookup = dict(zip(df["ticker"], df["avg_turnover"]))

    report = generate_report(tickers, adv_lookup, liquidity_floor_inr=args.liquidity_floor_inr, lookback_days=args.lookback_days)
    with open(args.out, "w") as f:
        json.dump(report, f, indent=2, default=str)
    logger.info(f"Report saved to {args.out}")

    for name, r in report.items():
        print(f"\n=== {name} === tradeable {r['n_tradeable']}/{TOP_N}, confirmed-in-history {r['n_confirmed_in_history']}/{TOP_N}")
        for p in r["picks"][:10]:
            print(f"  {p['ticker']:12s} proba={p['mb_probability']:.3f} adv_cr={p['adv_inr_cr']} tradeable={p['tradeable']} confirmed={p['confirmed_multibagger_in_training_history']}")


if __name__ == "__main__":
    main()
