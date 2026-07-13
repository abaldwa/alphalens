"""
scripts/backtest_realized_hitrate.py

ML24 follow-up (2026-07-11): realized-outcome validation harness for the
CURRENT production models (signal_5d/21d/63d, MultiBagger) — no retraining
happens anywhere in this script. For a fixed ticker universe (today's
recommendable set) and many historical point-in-time evaluation dates, this
scores each model exactly as daily_inference.py/score_multibagger.py would
have that day, then walks forward on real OHLCV to check whether the
recommended move actually happened:

  - signal_5d:  +5% touched intraday within the next 5-6 trading days
  - signal_21d: +10% touched intraday within the next ~23 trading days
  - signal_63d: +15% touched intraday within the next 63 trading days
  - MultiBagger: 2x/3x/5x within 12/24/36 months

"Touched" means the rolling max of daily HIGH crossed the threshold
anywhere in the window, not the close-to-close return at the end of the
window (user's explicit correction, 2026-07-11).

Every recorded "buy" recommendation also captures its SHAP top-5 feature
attribution (reusing daily_inference.py's `_compute_shap_top5`, restricted
to buy-flagged tickers only to bound the added cost) — the user wants this
per-signal SHAP data to fine-tune the models, not just an aggregate
hit-rate number.

Reuses features.matrix_builder.build_feature_matrix (already PIT-correct
for arbitrary historical dates) and features.backfill_cache.BackfillDataCache
(pre-loads fundamentals/shareholding/corp-actions ONCE per ticker instead of
once per eval date — this is what makes a 200-daily-date run tractable;
without it this would repeat ~15-20M HTTP calls, see that module's docstring).
"""

import argparse
import json
import logging
from datetime import date as date_type
from pathlib import Path
from typing import Dict, List, Optional

import duckdb
import pandas as pd

from config.settings import DUCKDB_PATH, MODELS_DIR
from config.training_universe import load_current_training_universe
from datastore.client import DataStoreClient
from features.backfill_cache import BackfillDataCache
from features.matrix_builder import build_feature_matrix
from features.multibagger import MULTIBAGGER_FEATURES
from systems.ml_signal_engine.inference.daily_inference import _compute_shap_top5
from systems.ml_signal_engine.models.multibagger.multibagger_model import MultibaggerModel
from systems.ml_signal_engine.models.signal.base_signal_model import CLASS_NAMES
from systems.ml_signal_engine.models.signal.signal_5d import Signal5DModel
from systems.ml_signal_engine.models.signal.signal_21d import Signal21DModel
from systems.ml_signal_engine.models.signal.signal_63d import Signal63DModel

logger = logging.getLogger(__name__)

SIGNAL_HORIZONS = {
    "signal_5d": {"cls": Signal5DModel, "horizon_days": 6, "threshold": 0.05},
    "signal_21d": {"cls": Signal21DModel, "horizon_days": 23, "threshold": 0.10},
    "signal_63d": {"cls": Signal63DModel, "horizon_days": 63, "threshold": 0.15},
}
MULTIBAGGER_THRESHOLDS = [(12, 2.0), (24, 3.0), (36, 5.0)]  # (months, multiple)
TRADING_DAYS_PER_MONTH = 21


def _load_model(cls, name: str):
    path = MODELS_DIR / name / f"{name}_current.pkl"
    model = cls()
    model.load(str(path))
    return model


def load_trading_days(con: "duckdb.DuckDBPyConnection") -> pd.DatetimeIndex:
    rows = con.execute("SELECT DISTINCT date FROM ohlcv_adjusted ORDER BY date").df()["date"]
    return pd.DatetimeIndex(pd.to_datetime(rows))


def load_ohlcv_wide(con: "duckdb.DuckDBPyConnection", tickers: List[str]) -> Dict[str, pd.DataFrame]:
    """ticker -> DataFrame[open, high, close] indexed by date, full history, one bulk query."""
    placeholders = ",".join(["?"] * len(tickers))
    df = con.execute(
        f"SELECT ticker, date, open, high, close FROM ohlcv_adjusted WHERE ticker IN ({placeholders}) "
        "ORDER BY ticker, date",
        tickers,
    ).df()
    df["date"] = pd.to_datetime(df["date"])
    out: Dict[str, pd.DataFrame] = {}
    for ticker, g in df.groupby("ticker"):
        out[ticker] = g.set_index("date")[["open", "high", "close"]]
    return out


def pick_signal_eval_dates(trading_days: pd.DatetimeIndex, horizon_days: int, n_dates: int) -> List[pd.Timestamp]:
    """Last n_dates trading days that still have a full forward window (horizon_days
    plus 1 for next-day-open entry) available before the latest OHLCV date."""
    usable = trading_days[: len(trading_days) - horizon_days - 1]
    return list(usable[-n_dates:])


def _checkpoint_path(records_dir: Path, key: str) -> Path:
    return records_dir / f"{key}.checkpoint.jsonl"


def _append_checkpoint(records_dir: Path, key: str, new_records: list) -> None:
    """Append newly-produced records for one eval date to a per-key jsonl file
    immediately, so an OOM kill mid-run only loses the in-flight eval date
    instead of the entire multi-hour run (nothing was persisted before this;
    see module docstring / 2026-07-11 harness build)."""
    if not new_records:
        return
    records_dir.mkdir(parents=True, exist_ok=True)
    with open(_checkpoint_path(records_dir, key), "a") as f:
        for rec in new_records:
            f.write(json.dumps(rec, default=str) + "\n")


def evaluate_all_signal_horizons(
    horizon_keys: List[str],
    universe: List[str],
    client: DataStoreClient,
    cache: BackfillDataCache,
    trading_days: pd.DatetimeIndex,
    ohlcv: Dict[str, pd.DataFrame],
    n_dates: int,
    limit_dates: Optional[int],
    records_dir: Path,
) -> Dict[str, dict]:
    """
    Scores all requested signal_5d/21d/63d models against ONE shared
    build_feature_matrix() call per evaluation date, instead of each
    horizon independently rebuilding the same date's features (3x fewer
    feature builds — the dominant cost of this harness even with
    BackfillDataCache's per-ticker caching, since FNO/OHLCV-bulk/universe
    resolution still happen per date).

    Uses the strictest (longest) horizon's eligible-date window as the
    shared date set, so every requested horizon gets the same `n_dates`
    evaluation dates and a full forward window is guaranteed for all of them.
    """
    strictest_horizon_days = max(SIGNAL_HORIZONS[k]["horizon_days"] for k in horizon_keys)
    eval_dates = pick_signal_eval_dates(trading_days, strictest_horizon_days, n_dates)
    if limit_dates:
        eval_dates = eval_dates[-limit_dates:]

    models = {k: _load_model(SIGNAL_HORIZONS[k]["cls"], k) for k in horizon_keys}
    records: Dict[str, list] = {k: [] for k in horizon_keys}

    for i, T in enumerate(eval_dates):
        logger.info(f"[signals] eval date {i + 1}/{len(eval_dates)}: {T.date()}")
        try:
            fm = build_feature_matrix(
                date=T.strftime("%Y-%m-%d"), tickers=universe, client=client,
                save=False, compute_hmm=False, data_cache=cache,
            )
        except Exception as exc:
            logger.warning(f"[signals] feature build failed for {T.date()}: {exc}")
            continue
        eligible = fm.set_index("ticker")
        t_idx = trading_days.get_loc(T)
        entry_date = trading_days[t_idx + 1]

        for horizon_key in horizon_keys:
            cfg = SIGNAL_HORIZONS[horizon_key]
            model = models[horizon_key]
            try:
                proba = model.predict_signals(eligible)
                direction = model.predict(eligible)
            except Exception as exc:
                logger.warning(f"[{horizon_key}] scoring failed for {T.date()}: {exc}")
                continue

            forward_dates = trading_days[t_idx + 1: t_idx + 1 + cfg["horizon_days"]]

            buy_tickers = [t for t in eligible.index if CLASS_NAMES[int(direction.loc[t])] == "buy"]
            shap_top5: Dict[str, str] = {}
            if buy_tickers:
                try:
                    shap_top5 = _compute_shap_top5(model, eligible.loc[buy_tickers], direction.loc[buy_tickers])
                except Exception as exc:
                    logger.warning(f"[{horizon_key}] SHAP computation failed for {T.date()}, left null ({exc})")

            date_records = []
            for ticker in buy_tickers:
                buy_prob = float(proba.loc[ticker, "signal_buy_prob"])
                t_ohlcv = ohlcv.get(ticker)
                if t_ohlcv is None or entry_date not in t_ohlcv.index:
                    continue
                entry_price = t_ohlcv.loc[entry_date, "open"]
                if pd.isna(entry_price) or entry_price <= 0:
                    continue
                window = t_ohlcv.reindex(forward_dates)["high"]
                if window.dropna().empty:
                    continue
                max_return = (window.max() - entry_price) / entry_price
                hit = bool(max_return >= cfg["threshold"])
                days_to_hit = None
                if hit:
                    touch_dates = window.index[(window - entry_price) / entry_price >= cfg["threshold"]]
                    if len(touch_dates):
                        days_to_hit = int((touch_dates[0] - entry_date).days)
                date_records.append({
                    "eval_date": T.date().isoformat(), "ticker": ticker, "buy_prob": buy_prob,
                    "entry_price": float(entry_price),
                    "max_return": float(max_return) if pd.notna(max_return) else None,
                    "hit": hit, "days_to_hit": days_to_hit,
                    "shap_top5_json": shap_top5.get(ticker),
                })
            records[horizon_key].extend(date_records)
            _append_checkpoint(records_dir, horizon_key, date_records)

    out = {}
    for horizon_key in horizon_keys:
        result_df = pd.DataFrame(records[horizon_key])
        out[horizon_key] = {
            "summary": _summarize_signal(result_df, horizon_key, SIGNAL_HORIZONS[horizon_key]["threshold"]),
            "records": result_df,
        }
    return out


def _summarize_shap_drivers(df: pd.DataFrame, min_n: int = 5) -> Optional[list]:
    """
    Break down hit-rate by each 'buy' recommendation's #1 SHAP feature — the
    actionable "SHAP gap" view for fine-tuning: which features are actually
    driving buy calls, and do calls driven by a given feature hit their
    target more or less often than average? Features with fewer than min_n
    recommendations are dropped (too noisy to draw a conclusion from).
    """
    import json as _json

    def _top_feature(shap_json):
        if not shap_json:
            return None
        try:
            parsed = _json.loads(shap_json)
            return parsed[0]["feature"] if parsed else None
        except Exception:
            return None

    if "shap_top5_json" not in df.columns:
        return None
    work = df.copy()
    work["top_feature"] = work["shap_top5_json"].apply(_top_feature)
    work = work.dropna(subset=["top_feature"])
    if work.empty:
        return None
    grouped = work.groupby("top_feature").agg(n=("hit", "size"), hit_rate=("hit", "mean")).reset_index()
    grouped = grouped[grouped["n"] >= min_n].sort_values("n", ascending=False)
    return grouped.to_dict("records") if not grouped.empty else None


def _summarize_signal(df: pd.DataFrame, name: str, threshold: float) -> dict:
    if df.empty:
        return {"model": name, "n": 0, "hit_rate": None, "threshold": threshold}
    df = df.copy()
    df["days_to_hit"] = pd.to_numeric(df["days_to_hit"], errors="coerce")
    decile = None
    try:
        df["decile"] = pd.qcut(df["buy_prob"], 10, labels=False, duplicates="drop")
        decile = (
            df.groupby("decile").agg(n=("hit", "size"), hit_rate=("hit", "mean"))
            .reset_index().to_dict("records")
        )
    except Exception:
        pass
    return {
        "model": name, "n": int(len(df)), "hit_rate": float(df["hit"].mean()), "threshold": threshold,
        "median_days_to_hit": float(df.loc[df["hit"], "days_to_hit"].median()) if df["hit"].any() else None,
        "decile_breakdown": decile,
        "shap_top_driver_breakdown": _summarize_shap_drivers(df),
    }


def evaluate_multibagger(
    universe: List[str],
    client: DataStoreClient,
    cache: BackfillDataCache,
    trading_days: pd.DatetimeIndex,
    ohlcv: Dict[str, pd.DataFrame],
    n_quarters: int,
    limit_dates: Optional[int],
    records_dir: Path,
) -> dict:
    model = _load_model(MultibaggerModel, "multibagger")

    all_idx = list(range(0, len(trading_days) - 1, 63))  # quarterly cadence, entry needs next trading day
    eval_idx = all_idx[-n_quarters:]
    if limit_dates:
        eval_idx = eval_idx[-limit_dates:]

    records = []
    for j, t_idx in enumerate(eval_idx):
        T = trading_days[t_idx]
        logger.info(f"[multibagger] eval date {j + 1}/{len(eval_idx)}: {T.date()}")
        try:
            fm = build_feature_matrix(
                date=T.strftime("%Y-%m-%d"), tickers=universe, client=client,
                save=False, compute_hmm=False, data_cache=cache,
            )
        except Exception as exc:
            logger.warning(f"[multibagger] feature build failed for {T.date()}: {exc}")
            continue
        eligible = fm.set_index("ticker")
        try:
            scored = model.predict_full(eligible[MULTIBAGGER_FEATURES])
        except Exception as exc:
            logger.warning(f"[multibagger] scoring failed for {T.date()}: {exc}")
            continue

        entry_date = trading_days[t_idx + 1]
        max_forward_idx = min(t_idx + 1 + 36 * TRADING_DAYS_PER_MONTH, len(trading_days) - 1)
        forward_dates_full = trading_days[t_idx + 1: max_forward_idx + 1]
        forward_available_days = len(forward_dates_full)

        date_records = []
        for ticker in scored.index:
            t_ohlcv = ohlcv.get(ticker)
            if t_ohlcv is None or entry_date not in t_ohlcv.index:
                continue
            entry_price = t_ohlcv.loc[entry_date, "close"]
            if pd.isna(entry_price) or entry_price <= 0:
                continue
            window = t_ohlcv.reindex(forward_dates_full)["high"]
            rec = {
                "eval_date": T.date().isoformat(), "ticker": ticker,
                "mb_probability": float(scored.loc[ticker, "mb_probability"]),
                "mb_tier": scored.loc[ticker, "mb_tier"], "entry_price": float(entry_price),
            }
            for months, multiple in MULTIBAGGER_THRESHOLDS:
                horizon_days = months * TRADING_DAYS_PER_MONTH
                complete = forward_available_days >= horizon_days
                sub_window = window.iloc[:horizon_days]
                cum_return = (
                    (sub_window.max() - entry_price) / entry_price if not sub_window.dropna().empty else None
                )
                hit = bool(cum_return is not None and cum_return >= (multiple - 1))
                rec[f"hit_{months}m"] = hit if complete else None
                rec[f"complete_{months}m"] = complete
            date_records.append(rec)
        records.extend(date_records)
        _append_checkpoint(records_dir, "multibagger", date_records)

    result_df = pd.DataFrame(records)
    return {"summary": _summarize_multibagger(result_df), "records": result_df}


def _summarize_multibagger(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"model": "multibagger", "n": 0}
    out = {"model": "multibagger", "n": int(len(df))}
    for months, multiple in MULTIBAGGER_THRESHOLDS:
        complete_df = df[df[f"complete_{months}m"] == True]  # noqa: E712
        out[f"{months}m_{multiple}x"] = {
            "n_complete": int(len(complete_df)),
            "hit_rate": float(complete_df[f"hit_{months}m"].mean()) if len(complete_df) else None,
            "n_provisional": int((df[f"complete_{months}m"] == False).sum()),  # noqa: E712
        }
    return out


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="Realized-outcome hit-rate validation for the current production ML signal models. "
        "No retraining happens here — this only scores already-trained artifacts at "
        "historical dates and checks the real forward price action."
    )
    parser.add_argument("--models", default="signal_5d,signal_21d,signal_63d,multibagger",
                         help="Comma-separated subset to run")
    parser.add_argument("--eval-days", type=int, default=200,
                         help="Number of daily eval dates for signal_5d/21d/63d (default 200)")
    parser.add_argument("--eval-quarters", type=int, default=12,
                         help="Number of quarterly eval dates for MultiBagger (default 12 = 3 years)")
    parser.add_argument("--limit-dates", type=int, default=None,
                         help="Smoke-test: only run the most recent N eval dates per model")
    parser.add_argument("--universe-size", type=int, default=None,
                         help="Cap the fixed ticker universe (debug/speed only)")
    parser.add_argument("--cache-workers", type=int, default=8,
                         help="Thread count for the one-time fundamentals/shareholding pre-load")
    parser.add_argument("--out", default=None,
                         help="Output JSON path (default: backtest/reports/realized_hitrate_<today>.json)")
    args = parser.parse_args()

    models_to_run = set(args.models.split(","))

    universe = load_current_training_universe()
    if args.universe_size:
        universe = universe[: args.universe_size]
    logger.info(f"Fixed evaluation universe: {len(universe)} tickers")

    client = DataStoreClient()
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    trading_days = load_trading_days(con)
    logger.info(f"Trading calendar: {trading_days[0].date()} .. {trading_days[-1].date()} ({len(trading_days)} days)")

    logger.info("Bulk-loading forward OHLCV for hit-rate checks (one-time query)...")
    ohlcv = load_ohlcv_wide(con, universe)

    to_date_ts = pd.Timestamp(trading_days[-1])
    logger.info("Pre-loading fundamentals/shareholding/corp-actions cache (one-time per ticker — "
                "this is what makes the multi-date loop below tractable)...")
    cache = BackfillDataCache(client, universe, to_date=to_date_ts.to_pydatetime(), n_workers=args.cache_workers)

    out_path = Path(args.out) if args.out else Path("backtest/reports") / f"realized_hitrate_{date_type.today().isoformat()}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    records_dir = out_path.parent / f"realized_hitrate_records_{date_type.today().isoformat()}"
    records_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Checkpointing per-eval-date records incrementally to {records_dir}/*.checkpoint.jsonl")

    results = {}
    signal_keys = [k for k in ("signal_5d", "signal_21d", "signal_63d") if k in models_to_run]
    if signal_keys:
        results.update(evaluate_all_signal_horizons(
            signal_keys, universe, client, cache, trading_days, ohlcv, args.eval_days, args.limit_dates,
            records_dir,
        ))
    if "multibagger" in models_to_run:
        results["multibagger"] = evaluate_multibagger(
            universe, client, cache, trading_days, ohlcv, args.eval_quarters, args.limit_dates,
            records_dir,
        )

    serializable = {k: v["summary"] for k, v in results.items()}
    out_path.write_text(json.dumps(serializable, indent=2, default=str))

    # Full per-signal records (incl. shap_top5_json) — the raw material for
    # fine-tuning, not just the aggregate summary above. Also supersedes the
    # incremental *.checkpoint.jsonl files written during the run.
    for k, v in results.items():
        records_df = v.get("records")
        if records_df is not None and not records_df.empty:
            records_df.to_parquet(records_dir / f"{k}.parquet", index=False)

    print("\n=== Realized-Outcome Hit-Rate Summary ===")
    for k, v in serializable.items():
        print(f"\n{k}:\n{json.dumps(v, indent=2, default=str)}")
    print(f"\nPer-signal records (incl. SHAP) written to {records_dir}/")
    print(f"\nFull report written to {out_path}")


if __name__ == "__main__":
    main()
