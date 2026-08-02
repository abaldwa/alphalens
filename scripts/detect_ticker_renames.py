"""
scripts/detect_ticker_renames.py

Phase: A20 follow-up (2026-07-30)
Owner: Data Layer / Ops

Analysis-only, writes nothing to the DB — same discipline as
scripts/detect_missing_split_reconstruction.py. Scans ohlcv_adjusted for
candidate ticker-rename/identity-split pairs: an old ticker whose series
ends right where a new ticker's series begins, with price continuity
across the boundary — the exact signature found for TATAMOTORS -> TMPV
(Tata Motors' Oct 2025 demerger; NSE's Corporate Actions API retroactively
files the company's entire 20-year history under the new symbol, but our
own ohlcv_adjusted kept the two as separate, disjoint ticker series, so
ticker-string joins against corporate_actions silently miss the old
symbol's history).

Cross-checks each candidate pair against config.universe.get_isin_to_ticker_map()
as a second, independent signal when available.

Output is a review CSV only. A follow-up, explicitly-approved step would
add a ticker_aliases table for check_corporate_actions_coverage and
similar ticker-keyed joins to consult — not an automatic OHLCV-series
merge (too high a blast radius for every downstream feature/model/
universe consumer to do unreviewed).

Usage
-----
    .venv/bin/python3 scripts/detect_ticker_renames.py
    .venv/bin/python3 scripts/detect_ticker_renames.py --gap-days 10 --price-tolerance-pct 10
    .venv/bin/python3 scripts/detect_ticker_renames.py --out my_review.csv
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

DEFAULT_GAP_DAYS = 5
DEFAULT_PRICE_TOLERANCE_PCT = 10.0
# Tight by design: the overlap detector is testing near-identical price
# (the same company, two ticker strings), not merely correlated
# magnitude — TATAMOTORS/TMPV's real match came in at 0.4% median diff.
# A loose tolerance here matched 40,000+ pairs in testing (coincidental
# closeness among thousands of tickers), making the review CSV useless.
DEFAULT_OVERLAP_TOLERANCE_PCT = 2.0
MIN_TRADING_DAYS = 250  # ~1 year — filters out noise from thin/short-lived series


def find_candidate_renames(conn, gap_days: int, price_tolerance_pct: float) -> pd.DataFrame:
    series = conn.execute(
        """
        SELECT ticker, count(*) AS n_days, min(date) AS first_date, max(date) AS last_date
        FROM ohlcv_adjusted
        GROUP BY ticker
        HAVING count(*) >= ?
        """,
        [MIN_TRADING_DAYS],
    ).df()
    if series.empty:
        return pd.DataFrame()

    last_close = conn.execute(
        """
        SELECT o.ticker, o.close AS last_close
        FROM ohlcv_adjusted o
        INNER JOIN (SELECT ticker, max(date) AS last_date FROM ohlcv_adjusted GROUP BY ticker) m
                ON o.ticker = m.ticker AND o.date = m.last_date
        """
    ).df()
    first_close = conn.execute(
        """
        SELECT o.ticker, o.close AS first_close
        FROM ohlcv_adjusted o
        INNER JOIN (SELECT ticker, min(date) AS first_date FROM ohlcv_adjusted GROUP BY ticker) m
                ON o.ticker = m.ticker AND o.date = m.first_date
        """
    ).df()

    series = series.merge(last_close, on="ticker").merge(first_close, on="ticker")
    series["first_date"] = pd.to_datetime(series["first_date"])
    series["last_date"] = pd.to_datetime(series["last_date"])

    rows = []
    for old in series.itertuples():
        window_end = old.last_date + pd.Timedelta(days=gap_days)
        candidates = series[
            (series["ticker"] != old.ticker)
            & (series["first_date"] > old.last_date)
            & (series["first_date"] <= window_end)
        ]
        for new in candidates.itertuples():
            if old.last_close and old.last_close > 0:
                price_gap_pct = abs(new.first_close - old.last_close) / old.last_close * 100
            else:
                price_gap_pct = None

            gap_days_actual = (new.first_date - old.last_date).days
            rows.append(
                {
                    "old_ticker": old.ticker,
                    "new_ticker": new.ticker,
                    "old_last_date": old.last_date.date(),
                    "new_first_date": new.first_date.date(),
                    "gap_days": gap_days_actual,
                    "old_last_close": old.last_close,
                    "new_first_close": new.first_close,
                    "price_gap_pct": price_gap_pct,
                    "old_trading_days": old.n_days,
                    "new_trading_days": new.n_days,
                    "within_price_tolerance": (
                        price_gap_pct is not None and price_gap_pct <= price_tolerance_pct
                    ),
                }
            )

    return pd.DataFrame(rows)


def find_overlapping_duplicate_series(
    conn, price_tolerance_pct: float, recent_window_days: int = 10, stopped_buffer_days: int = 10
) -> pd.DataFrame:
    """
    Catches the TATAMOTORS/TMPV shape of duplicate: unlike a clean
    sequential handoff (find_candidate_renames), TMPV's series started in
    2005 and ran the whole time IN PARALLEL with TATAMOTORS (2006 to
    2025-10-23) — the two overlap for 20 years, they don't hand off. Full-
    history return correlation between the pair is misleadingly low
    (0.17) because TMPV's price gets step-adjusted at each of ITS OWN
    known corporate actions while TATAMOTORS (a stale, no-longer-updated
    duplicate) never does — so a generic correlation test dilutes across
    two decades of accumulated step-mismatches. What stays reliable is
    the LAST `recent_window_days` of the overlap, right before the old
    (stopped) ticker's final date: both series are close to their
    "current" scale there (few/no pending unadjusted corporate actions in
    a short recent window), so a simple price-level closeness check on
    that tail works even though full-history correlation doesn't.

    Only considers "stopped" tickers (last_date more than
    `stopped_buffer_days` before the DB's global max date) as the old
    side of a pair, and only currently-active tickers (last_date within
    `stopped_buffer_days` of the global max) as the new side — otherwise
    two unrelated tickers that both happened to stop trading on the same
    day would spuriously match.
    """
    global_max_date = conn.execute("SELECT max(date) FROM ohlcv_adjusted").fetchone()[0]
    stopped_cutoff = pd.Timestamp(global_max_date) - pd.Timedelta(days=stopped_buffer_days)

    series = conn.execute(
        """
        SELECT ticker, count(*) AS n_days, min(date) AS first_date, max(date) AS last_date
        FROM ohlcv_adjusted
        GROUP BY ticker
        HAVING count(*) >= ?
        """,
        [MIN_TRADING_DAYS],
    ).df()
    if series.empty:
        return pd.DataFrame()
    series["first_date"] = pd.to_datetime(series["first_date"])
    series["last_date"] = pd.to_datetime(series["last_date"])

    stopped = series[series["last_date"] < stopped_cutoff]
    active = series[series["last_date"] >= stopped_cutoff]
    if stopped.empty or active.empty:
        return pd.DataFrame()

    rows = []
    for old in stopped.itertuples():
        # Fetch a wider calendar pool (a corporate action like a demerger
        # can make prices genuinely diverge right up until its ex_date —
        # see TATAMOTORS/TMPV, ~40% apart until 2025-10-14, <1% apart
        # after; a fixed calendar window can straddle that boundary and
        # wash out a real match). Then, per candidate, use only the
        # trailing `recent_window_days` TRADING ROWS immediately before
        # old.last_date — a row-count window that self-adjusts to sit
        # entirely after any such transition, instead of a calendar
        # window that might not.
        pool_start = old.last_date - pd.Timedelta(days=recent_window_days * 4)
        overlap = conn.execute(
            """
            SELECT a.ticker AS candidate_ticker, a.date, a.close AS candidate_close, o.close AS old_close
            FROM ohlcv_adjusted a
            JOIN (
                SELECT date, close FROM ohlcv_adjusted WHERE ticker = ? AND date BETWEEN ? AND ?
            ) o ON a.date = o.date
            WHERE a.ticker != ?
              AND a.date BETWEEN ? AND ?
            """,
            [old.ticker, pool_start.date(), old.last_date.date(), old.ticker, pool_start.date(), old.last_date.date()],
        ).df()
        if overlap.empty:
            continue

        overlap = overlap[overlap["candidate_ticker"].isin(set(active["ticker"]))]
        if overlap.empty:
            continue

        overlap = overlap.sort_values("date").groupby("candidate_ticker").tail(recent_window_days)
        overlap["pct_diff"] = (overlap["candidate_close"] - overlap["old_close"]).abs() / overlap["old_close"] * 100
        by_candidate = overlap.groupby("candidate_ticker").agg(
            median_pct_diff=("pct_diff", "median"),
            n_overlap_days=("pct_diff", "size"),
        )
        min_overlap_days = max(5, recent_window_days - 2)
        matches = by_candidate[
            (by_candidate["median_pct_diff"] <= price_tolerance_pct)
            & (by_candidate["n_overlap_days"] >= min_overlap_days)
        ]
        for new_ticker, m in matches.iterrows():
            rows.append(
                {
                    "old_ticker": old.ticker,
                    "new_ticker": new_ticker,
                    "old_last_date": old.last_date.date(),
                    "new_first_date": None,  # overlapping, not a handoff — new ticker predates/coexists
                    "gap_days": None,
                    "old_last_close": None,
                    "new_first_close": None,
                    "price_gap_pct": round(m["median_pct_diff"], 4),
                    "old_trading_days": old.n_days,
                    "new_trading_days": int(active.loc[active["ticker"] == new_ticker, "n_days"].iloc[0]),
                    "within_price_tolerance": True,
                    "detection_method": "overlapping_series",
                }
            )

    return pd.DataFrame(rows)


def _isin_check(df: pd.DataFrame) -> pd.DataFrame:
    try:
        from config.universe import get_isin_to_ticker_map
    except Exception as exc:  # noqa: BLE001
        print(f"Could not load ISIN map ({exc}) — skipping ISIN cross-check.")
        df["isin_current_ticker"] = None
        return df

    isin_to_ticker = get_isin_to_ticker_map()
    ticker_to_isin = {t: isin for isin, t in isin_to_ticker.items()}

    def _resolve(row):
        # If either side of the pair is the ISIN map's CURRENT ticker for
        # its own ISIN, and the other side shares no ISIN entry (likely
        # delisted/renamed-away, so absent from the current-only map),
        # that's consistent with a rename — not proof, just corroboration.
        new_isin = ticker_to_isin.get(row["new_ticker"])
        old_isin = ticker_to_isin.get(row["old_ticker"])
        if new_isin and not old_isin:
            return "new_ticker_has_isin_old_does_not (consistent with rename)"
        if old_isin and new_isin and old_isin == new_isin:
            return "same_isin (strong rename signal)"
        return None

    df["isin_note"] = df.apply(_resolve, axis=1)
    return df


def _confidence(row) -> str:
    if row["within_price_tolerance"] and row["isin_note"] == "same_isin (strong rename signal)":
        return "high"
    if row["within_price_tolerance"]:
        return "medium"
    return "low"


def main() -> None:
    parser = argparse.ArgumentParser(description="Detect candidate ticker-rename/identity-split pairs")
    parser.add_argument("--gap-days", type=int, default=DEFAULT_GAP_DAYS)
    parser.add_argument("--price-tolerance-pct", type=float, default=DEFAULT_PRICE_TOLERANCE_PCT)
    parser.add_argument("--overlap-tolerance-pct", type=float, default=DEFAULT_OVERLAP_TOLERANCE_PCT)
    parser.add_argument("--out", default="ticker_rename_review.csv")
    args = parser.parse_args()

    from config.settings import DUCKDB_PATH
    from datastore.api.db import get_duckdb_connection

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        handoff_df = find_candidate_renames(conn, args.gap_days, args.price_tolerance_pct)
        if not handoff_df.empty:
            handoff_df["detection_method"] = "sequential_handoff"
        overlap_df = find_overlapping_duplicate_series(conn, args.overlap_tolerance_pct)

    non_empty = [d for d in (handoff_df, overlap_df) if not d.empty]
    if not non_empty:
        print("No candidate rename pairs found.")
        return
    df = pd.concat(non_empty, ignore_index=True) if len(non_empty) > 1 else non_empty[0]

    df = _isin_check(df)
    df["confidence"] = df.apply(_confidence, axis=1)
    df = df.sort_values(["confidence", "price_gap_pct"], ascending=[False, True])
    df.to_csv(args.out, index=False)

    print(f"Wrote {len(df)} candidate pair(s) to {args.out}")
    print(df["confidence"].value_counts())
    high = df[df["confidence"] == "high"]
    if not high.empty:
        print("\nHigh-confidence pairs:")
        print(high[["old_ticker", "new_ticker", "gap_days", "price_gap_pct"]].to_string(index=False))


if __name__ == "__main__":
    main()
