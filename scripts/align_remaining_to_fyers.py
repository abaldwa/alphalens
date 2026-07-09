"""
Align the 102 remaining #32 tickers (left untouched during the NSE-verified
triage) directly to Fyers, without waiting for individual NSE confirmation of
each corporate action's exact type/ratio.

For each ticker: use the already-detected candidate ex-date and empirical
price factor (our/Fyers close ratio jump) from the batch-detection CSVs.
Record a BONUS row in corporate_actions as a placeholder note ("a bonus has
been announced") since the true action type (split/bonus/rights/demerger/etc)
was not independently confirmed for these — then apply the empirical
multiplicative rescale directly to ohlcv_adjusted (open/high/low/close,
adj_factor) for all rows before the ex-date, same mechanism used for the
RIGHTS corrections earlier. This works regardless of factor direction
(unlike the BONUS-ratio formula in price_adjuster.py, which assumes
factor < 1), so no adjust_for_corporate_actions() re-run is needed or wanted
here — it would double-apply on collision tickers that already have a
pre-existing action at/near the same date.

Usage:
    python3 scripts/align_remaining_to_fyers.py [--dry-run]
"""
import argparse
import sys

import duckdb
import pandas as pd

sys.path.insert(0, ".")
from config.settings import DUCKDB_PATH

TICKERS_JSON = "/tmp/remaining_104.json"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    import json
    remaining = set(json.load(open(TICKERS_JSON)))

    conn0 = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    already_done = set(conn0.execute(
        "SELECT ticker FROM corporate_actions_validation WHERE action_type='BONUS' AND validation_status='unchecked'"
    ).fetchdf().ticker)
    conn0.close()
    already_done.add("BRITANNIA")  # rescaled in a crashed prior run; only validation bookkeeping remained
    remaining = remaining - already_done
    print(f"Skipping {len(already_done)} already-aligned tickers from a prior run")

    dfs = []
    for i in range(1, 10):
        dfs.append(pd.read_csv(f"reconstruction_review_batch{i}.csv"))
    all_df = pd.concat(dfs, ignore_index=True)
    rem_df = all_df[all_df.ticker.isin(remaining) & (all_df.status == "ok")].copy()

    print(f"{len(rem_df)} tickers to align (of {len(remaining)} remaining; "
          f"{len(remaining) - len(rem_df)} skipped, no usable jump data)")

    conn = duckdb.connect(str(DUCKDB_PATH), read_only=args.dry_run)

    applied = []
    for row in rem_df.itertuples():
        ticker = row.ticker
        ex_date = row.candidate_ex_date
        factor = row.implied_factor
        if pd.isna(factor) or factor <= 0:
            print(f"SKIP {ticker}: invalid factor {factor}")
            continue

        placeholder_ratio = round(max(1.0 / factor - 1.0, 0.0001), 4)
        details = (
            "Bonus announced (empirical Fyers price-alignment, 2026-07-06 "
            "#32 reconciliation; exact action type/ratio not independently "
            "confirmed against NSE — see FutureDevelopment.md #32)"
        )

        if args.dry_run:
            print(f"{ticker}: ex_date={ex_date} factor={factor:.4f} "
                  f"placeholder_ratio={placeholder_ratio}")
            continue

        exists = conn.execute(
            "SELECT count(*) FROM corporate_actions WHERE ticker=? AND ex_date=? AND action_type='BONUS'",
            [ticker, ex_date],
        ).fetchone()[0]
        if exists:
            print(f"  ({ticker}: BONUS row already exists at {ex_date}, skipping insert, applying rescale only)")
        else:
            conn.execute(
                "INSERT INTO corporate_actions (ticker, ex_date, action_type, "
                "ratio, announcement_date, record_date, details) "
                "VALUES (?, ?, 'BONUS', ?, NULL, NULL, ?)",
                [ticker, ex_date, placeholder_ratio, details],
            )
        conn.execute(
            """UPDATE ohlcv_adjusted
               SET open=open*?, high=high*?, low=low*?, close=close*?,
                   adj_factor=adj_factor*?
               WHERE ticker=? AND date < ?""",
            [factor, factor, factor, factor, factor, ticker, ex_date],
        )
        val_exists = conn.execute(
            "SELECT count(*) FROM corporate_actions_validation WHERE ticker=? AND ex_date=? AND action_type='BONUS'",
            [ticker, ex_date],
        ).fetchone()[0]
        if val_exists:
            conn.execute(
                "UPDATE corporate_actions_validation SET validation_status='unchecked' "
                "WHERE ticker=? AND ex_date=? AND action_type='BONUS'",
                [ticker, ex_date],
            )
        else:
            conn.execute(
                "INSERT INTO corporate_actions_validation "
                "(ticker, ex_date, action_type, validation_status) "
                "VALUES (?, ?, 'BONUS', 'unchecked')",
                [ticker, ex_date],
            )
        applied.append((ticker, ex_date, factor))
        print(f"aligned {ticker} @ {ex_date} factor={factor:.4f}")

    conn.close()
    print(f"\n{len(applied)} tickers aligned.")


if __name__ == "__main__":
    main()
