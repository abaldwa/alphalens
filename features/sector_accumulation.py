"""
features/sector_accumulation.py

Phase: FeatureBacklog.md ML29 — sector accumulation detection
Owner: Platform / Features
Consumers: datastore/api/routers/sector_accumulation.py, dashboard Sector
Rotation screen (a new "Sector Accumulation" section)

Daily per-sector accumulation metric:

    accumulation_score(sector, date) =
        sum_over_constituent_stocks(delivery_pct/100 * volume)  [that date]
        --------------------------------------------------------------
        sum_over_constituent_stocks(shares_outstanding)  [that stock's
        own most recent PIT-eligible fundamentals row as of that date]

i.e. delivery-weighted volume (a standard "genuine accumulation, not
intraday churn" proxy — delivery_qty is NSE's own settled-delivery
figure) relative to how large the sector actually is in share-count
terms, tracked day over day, to surface sectors under constant steady
accumulation.

**Sector's total outstanding shares = the simple sum of each
constituent stock's own shares_outstanding** (2026-07-13 user decision,
FeatureBacklog.md ML29 row) — e.g. Stock A 100,000 + Stock B 200,000 =
300,000 for a 2-stock sector. Not weighted, not market-cap-based.

All inputs are real: ohlcv_adjusted (volume, delivery_pct) and
fundamentals (shares_outstanding, PIT-gated on announcement_date per
SPEC-PIPE-003) from the normalised DuckDB (config.settings.DUCKDB_PATH),
config.universe.load_universe() for sector membership. A stock missing
shares_outstanding as of a given date is simply excluded from that
date's denominator (and its own delivery-volume contribution) rather
than backfilled with a guess (CLAUDE.md Absolute Rule 6) — so a sector
with zero fully-known constituents on a date is absent from that date's
results, not shown with a fabricated 0/0.

Connections are passed in explicitly, matching features/sector_rotation.py's
existing dependency-injection convention (SPEC-SOLID-005).
"""

import logging
from typing import Any, List, Optional

import pandas as pd

from config.universe import load_universe

logger = logging.getLogger(__name__)

DEFAULT_LOOKBACK_DAYS = 90


def _latest_shares_outstanding_asof(
    normalised_conn: Any, tickers: List[str]
) -> pd.DataFrame:
    """Every real fundamentals row (ticker, announcement_date,
    shares_outstanding) for these tickers, sorted for an asof-merge —
    PIT correctness is enforced by the caller doing a <=-date asof join
    on announcement_date, never quarter_end_date (SPEC-PIPE-003)."""
    if not tickers:
        return pd.DataFrame(columns=["ticker", "announcement_date", "shares_outstanding"])
    placeholders = ",".join("?" for _ in tickers)
    df = normalised_conn.execute(
        f"""
        SELECT ticker, announcement_date, shares_outstanding
        FROM fundamentals
        WHERE ticker IN ({placeholders}) AND shares_outstanding IS NOT NULL
        ORDER BY ticker, announcement_date
        """,
        list(tickers),
    ).fetch_df()
    if not df.empty:
        df["announcement_date"] = pd.to_datetime(df["announcement_date"])
    return df


def _ohlcv_delivery_volume(
    normalised_conn: Any,
    tickers: List[str],
    start_date: Optional[str],
    end_date: Optional[str],
) -> pd.DataFrame:
    """Real (date, ticker, volume, delivery_pct) rows from ohlcv_adjusted
    for these tickers, optionally date-bounded."""
    if not tickers:
        return pd.DataFrame(columns=["date", "ticker", "volume", "delivery_pct"])
    placeholders = ",".join("?" for _ in tickers)
    where = f"ticker IN ({placeholders})"
    params: List[Any] = list(tickers)
    if start_date is not None:
        where += " AND date >= ?"
        params.append(start_date)
    if end_date is not None:
        where += " AND date <= ?"
        params.append(end_date)
    df = normalised_conn.execute(
        f"SELECT date, ticker, volume, delivery_pct FROM ohlcv_adjusted WHERE {where} ORDER BY ticker, date",
        params,
    ).fetch_df()
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df


def compute_sector_accumulation(
    normalised_conn: Any,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> pd.DataFrame:
    """
    Daily accumulation_score per sector over [start_date, end_date]
    (defaults to the trailing `lookback_days` calendar days ending on the
    latest real ohlcv_adjusted date).

    Returns
    -------
    pd.DataFrame
        Columns: date, sector, accumulation_score, delivery_volume,
        sector_shares_outstanding, n_stocks_included (constituents with
        real shares_outstanding data on that date — the ones actually
        summed into sector_shares_outstanding/accumulation_score).
        Empty if no real ohlcv_adjusted rows exist for the universe.
    """
    universe = load_universe()
    if universe.empty:
        return pd.DataFrame(
            columns=["date", "sector", "accumulation_score", "delivery_volume", "sector_shares_outstanding", "n_stocks_included"]
        )
    tickers = universe["ticker"].tolist()

    if end_date is None:
        latest = normalised_conn.execute(
            "SELECT MAX(date) FROM ohlcv_adjusted WHERE ticker IN ({})".format(
                ",".join("?" for _ in tickers)
            ),
            tickers,
        ).fetchone()[0]
        if latest is None:
            return pd.DataFrame(
                columns=["date", "sector", "accumulation_score", "delivery_volume", "sector_shares_outstanding", "n_stocks_included"]
            )
        end_date = pd.Timestamp(latest).date().isoformat()
    if start_date is None:
        start_date = (pd.Timestamp(end_date) - pd.Timedelta(days=lookback_days)).date().isoformat()

    ohlcv = _ohlcv_delivery_volume(normalised_conn, tickers, start_date, end_date)
    if ohlcv.empty:
        return pd.DataFrame(
            columns=["date", "sector", "accumulation_score", "delivery_volume", "sector_shares_outstanding", "n_stocks_included"]
        )

    fundamentals = _latest_shares_outstanding_asof(normalised_conn, tickers)

    # PIT asof-join: each ohlcv row picks up the most recent
    # shares_outstanding whose announcement_date <= that row's date, per
    # ticker. merge_asof requires both frames sorted by the join key
    # within each group.
    if fundamentals.empty:
        ohlcv["shares_outstanding"] = pd.NA
    else:
        ohlcv = ohlcv.sort_values(["ticker", "date"])
        fundamentals = fundamentals.sort_values(["ticker", "announcement_date"])
        ohlcv = pd.merge_asof(
            ohlcv,
            fundamentals.rename(columns={"announcement_date": "date"}),
            on="date",
            by="ticker",
            direction="backward",
        )

    ticker_to_sector = dict(zip(universe["ticker"], universe["sector"]))
    ohlcv["sector"] = ohlcv["ticker"].map(ticker_to_sector)
    ohlcv = ohlcv.dropna(subset=["sector"])

    # A stock only contributes (to both numerator and denominator) on a
    # date where it has a real shares_outstanding figure AND real
    # volume/delivery_pct — no partial/guessed contribution.
    known = ohlcv.dropna(subset=["shares_outstanding", "volume", "delivery_pct"]).copy()
    if known.empty:
        return pd.DataFrame(
            columns=["date", "sector", "accumulation_score", "delivery_volume", "sector_shares_outstanding", "n_stocks_included"]
        )

    known["delivery_volume"] = known["delivery_pct"] / 100.0 * known["volume"]

    grouped = known.groupby(["date", "sector"]).agg(
        delivery_volume=("delivery_volume", "sum"),
        sector_shares_outstanding=("shares_outstanding", "sum"),
        n_stocks_included=("ticker", "nunique"),
    ).reset_index()
    grouped["accumulation_score"] = grouped["delivery_volume"] / grouped["sector_shares_outstanding"]
    grouped["date"] = grouped["date"].dt.date.astype(str)
    grouped = grouped.sort_values(["date", "accumulation_score"], ascending=[True, False]).reset_index(drop=True)
    return grouped[["date", "sector", "accumulation_score", "delivery_volume", "sector_shares_outstanding", "n_stocks_included"]]


def sector_accumulation_drilldown(
    normalised_conn: Any,
    sector: str,
    date_str: str,
) -> pd.DataFrame:
    """
    Per-stock breakdown backing one (sector, date) accumulation_score
    cell — the numerator/denominator contribution of each constituent
    stock, for a dashboard drill-down click.

    Returns
    -------
    pd.DataFrame
        Columns: ticker, volume, delivery_pct, delivery_volume,
        shares_outstanding, contribution_pct (this stock's share of the
        sector's total delivery_volume on that date). Only stocks with
        real data for that date are included (same no-guess rule as
        compute_sector_accumulation).
    """
    universe = load_universe()
    tickers = universe.loc[universe["sector"] == sector, "ticker"].tolist()
    if not tickers:
        return pd.DataFrame(columns=["ticker", "volume", "delivery_pct", "delivery_volume", "shares_outstanding", "contribution_pct"])

    ohlcv = _ohlcv_delivery_volume(normalised_conn, tickers, date_str, date_str)
    if ohlcv.empty:
        return pd.DataFrame(columns=["ticker", "volume", "delivery_pct", "delivery_volume", "shares_outstanding", "contribution_pct"])

    fundamentals = _latest_shares_outstanding_asof(normalised_conn, tickers)
    if fundamentals.empty:
        ohlcv["shares_outstanding"] = pd.NA
    else:
        ohlcv = ohlcv.sort_values(["ticker", "date"])
        fundamentals = fundamentals.sort_values(["ticker", "announcement_date"])
        ohlcv = pd.merge_asof(
            ohlcv,
            fundamentals.rename(columns={"announcement_date": "date"}),
            on="date",
            by="ticker",
            direction="backward",
        )

    known = ohlcv.dropna(subset=["shares_outstanding", "volume", "delivery_pct"]).copy()
    if known.empty:
        return pd.DataFrame(columns=["ticker", "volume", "delivery_pct", "delivery_volume", "shares_outstanding", "contribution_pct"])

    known["delivery_volume"] = known["delivery_pct"] / 100.0 * known["volume"]
    total_delivery_volume = known["delivery_volume"].sum()
    known["contribution_pct"] = (
        known["delivery_volume"] / total_delivery_volume * 100.0 if total_delivery_volume else 0.0
    )
    known = known.sort_values("delivery_volume", ascending=False).reset_index(drop=True)
    return known[["ticker", "volume", "delivery_pct", "delivery_volume", "shares_outstanding", "contribution_pct"]]
