"""scripts/diagnose_signal_quantile_agreement.py

ML24 diagnostic (2026-07-11): quantifies whether the buy_prob/q50_return
divergence seen on LGEINDIA (signal_63d: buy_prob=0.84, q50_return=-0.018)
is a rare, individually-explainable edge case or a systemic calibration
problem, before deciding whether a model-level fix is warranted.

For each of signal_5d/signal_21d/signal_63d, reports:
  - overall sign-agreement rate (share of buy_prob>0.5 rows with q50>0)
  - Pearson correlation between buy_prob and q50_return
  - agreement rate broken down by buy_prob decile

Read-only against the real signals.duckdb (persist=False, read_only=True —
shares the file safely with the running API per datastore/api/db.py).
"""

import pandas as pd

from config.settings import SIGNALS_DUCKDB_PATH
from datastore.api.db import get_duckdb_connection

MODELS = ["signal_5d", "signal_21d", "signal_63d"]


def main() -> None:
    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, read_only=True, persist=False) as conn:
        for model_name in MODELS:
            df = conn.execute(
                """
                SELECT date, ticker, buy_prob, q50_return
                FROM ml_signals
                WHERE model_name = ? AND buy_prob IS NOT NULL AND q50_return IS NOT NULL
                """,
                [model_name],
            ).fetchdf()

            print(f"\n=== {model_name} ===")
            print(f"rows with both buy_prob and q50_return: {len(df)}")
            if df.empty:
                continue

            high_conf = df[df["buy_prob"] > 0.5]
            agree = (high_conf["q50_return"] > 0).mean() if len(high_conf) else float("nan")
            print(f"buy_prob>0.5 rows: {len(high_conf)}")
            print(f"sign-agreement rate (q50>0 | buy_prob>0.5): {agree:.4f}")

            corr = df["buy_prob"].corr(df["q50_return"])
            print(f"Pearson corr(buy_prob, q50_return): {corr:.4f}")

            df["decile"] = pd.qcut(df["buy_prob"], 10, labels=False, duplicates="drop")
            decile_stats = df.groupby("decile").apply(
                lambda g: pd.Series({
                    "n": len(g),
                    "buy_prob_range": f"{g['buy_prob'].min():.3f}-{g['buy_prob'].max():.3f}",
                    "pct_q50_positive": (g["q50_return"] > 0).mean(),
                    "mean_q50": g["q50_return"].mean(),
                }),
                include_groups=False,
            )
            print(decile_stats.to_string())

            severe = df[(df["buy_prob"] > 0.7) & (df["q50_return"] < -0.01)]
            print(f"\nSevere-divergence rows (buy_prob>0.7 AND q50<-1%): {len(severe)}")
            if len(severe):
                print(severe.sort_values("buy_prob", ascending=False).head(15).to_string(index=False))


if __name__ == "__main__":
    main()
