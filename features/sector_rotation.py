"""
features/sector_rotation.py

Phase: FutureDevelopment #25 (ML12 steps 4-6 — daily sector rotation report)
Owner: Platform / Features
Consumers: datastore/api/routers/sector_rotation.py, dashboard sector rotation screen

Trailing-21-trading-day relative strength per sector (config/
sector_index_map.py's SECTOR_INDEX_MAP) vs. Nifty 500, ranked; for each
in-favor sector, joins back to the latest real ml_signals/ml_multibagger
rows to surface that sector's top-ranked stocks.

All inputs are real: index_ohlcv (ingestion/scrapers/nse_indices.py,
DUCKDB_PATH) for sector-index OHLC, config.universe.load_universe() for
sector membership, ml_signals/ml_multibagger (SIGNALS_DUCKDB_PATH) for
per-ticker model output. No synthetic fallback for any of these — an
index/sector with too little index_ohlcv history to compute a trailing-21d
return is simply excluded from the ranking (CLAUDE.md Absolute Rule 6),
not backfilled with a guessed value.

Connections are passed in explicitly (not opened internally) so callers —
the API router and unit tests alike — control which DuckDB file/mode is
used, matching this project's existing dependency-injection convention
(see features/matrix_builder.py's module docstring, SPEC-SOLID-005).
"""

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from config.sector_index_map import SECTOR_INDEX_MAP, sectors_for_index
from config.universe import load_universe

logger = logging.getLogger(__name__)

TRAILING_WINDOW_DAYS = 21
BENCHMARK_INDEX_NAME = "Nifty 500"
# Minimum distinct trading days of index_ohlcv history required to compute
# a real trailing-21d return (need at least TRAILING_WINDOW_DAYS + 1 closes).
MIN_INDEX_ROWS = TRAILING_WINDOW_DAYS + 1
DEFAULT_TOP_N_STOCKS = 5

# ML28 (2026-07-13): relative-strength horizons surfaced alongside the
# original trailing-21d figure (kept as the default/primary ranking metric
# for backward compatibility with the existing report/dashboard sort).
RS_HORIZONS: Dict[str, int] = {"1d": 1, "5d": 5, "21d": 21, "63d": 63}
# Longest horizon also sets how much index_ohlcv history the sparkline
# trend series covers (one point per trading day over the window).
SPARKLINE_WINDOW_DAYS = max(RS_HORIZONS.values())


def _return_over_window(closes: pd.Series, window_days: int) -> Optional[float]:
    """Trailing `window_days`-trading-day close-to-close return, or None if
    there isn't enough real history (no synthetic fill)."""
    closes = closes.dropna()
    if len(closes) < window_days + 1:
        return None
    first = closes.iloc[-(window_days + 1)]
    last = closes.iloc[-1]
    if first is None or pd.isna(first) or first == 0:
        return None
    return float(last / first - 1)


def _trailing_return(closes: pd.Series) -> Optional[float]:
    """Trailing TRAILING_WINDOW_DAYS-trading-day close-to-close return, or
    None if there isn't enough real history (no synthetic fill)."""
    return _return_over_window(closes, TRAILING_WINDOW_DAYS)


def _sparkline_series(closes: pd.Series, window_days: int = SPARKLINE_WINDOW_DAYS) -> List[float]:
    """Last `window_days` (+1 anchor) real closes, rebased to a
    percent-change-from-first-point series (first point = 0.0) for cheap
    client-side sparkline rendering. Empty list if there isn't enough real
    history — no synthetic fill (CLAUDE.md Absolute Rule 6)."""
    closes = closes.dropna()
    if len(closes) < 2:
        return []
    tail = closes.iloc[-(window_days + 1):]
    base = tail.iloc[0]
    if base is None or pd.isna(base) or base == 0:
        return []
    return [float(c / base - 1) for c in tail.tolist()]


def _sector_market_cap_cr(
    normalised_conn: Any, universe: pd.DataFrame, as_of_date: Optional[str]
) -> Dict[str, float]:
    """
    ML28 — total real market cap (INR crore) per sector, summed over each
    constituent's own market cap: latest real ohlcv_adjusted close
    (as of `as_of_date`, or overall latest if None) times the most recent
    real fundamentals.shares_outstanding as of that same close date
    (PIT asof-join on announcement_date, never quarter_end_date —
    SPEC-PIPE-003, same pattern as features/sector_accumulation.py's
    _latest_shares_outstanding_asof). A stock only contributes if both a
    real close and a real shares_outstanding figure exist — no guessed
    fallback (CLAUDE.md Absolute Rule 6). Sectors with zero contributing
    constituents are simply absent from the returned dict.
    """
    tickers = universe["ticker"].tolist() if not universe.empty else []
    if not tickers:
        return {}

    placeholders = ",".join("?" for _ in tickers)
    close_where = f"ticker IN ({placeholders})"
    close_params: List[Any] = list(tickers)
    if as_of_date is not None:
        close_where += " AND date <= ?"
        close_params.append(as_of_date)
    closes = normalised_conn.execute(
        f"""
        SELECT ticker, date, close FROM ohlcv_adjusted
        WHERE {close_where}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) = 1
        """,
        close_params,
    ).fetch_df()
    if closes.empty:
        return {}
    closes["date"] = pd.to_datetime(closes["date"])

    fundamentals = normalised_conn.execute(
        f"""
        SELECT ticker, announcement_date, shares_outstanding
        FROM fundamentals
        WHERE ticker IN ({placeholders}) AND shares_outstanding IS NOT NULL
        ORDER BY ticker, announcement_date
        """,
        list(tickers),
    ).fetch_df()
    if fundamentals.empty:
        return {}
    fundamentals["announcement_date"] = pd.to_datetime(fundamentals["announcement_date"])

    closes = closes.sort_values(["ticker", "date"])
    fundamentals = fundamentals.sort_values(["ticker", "announcement_date"])
    merged = pd.merge_asof(
        closes,
        fundamentals.rename(columns={"announcement_date": "date"}),
        on="date",
        by="ticker",
        direction="backward",
    )
    merged = merged.dropna(subset=["close", "shares_outstanding"])
    if merged.empty:
        return {}
    merged["market_cap_cr"] = merged["close"] * merged["shares_outstanding"] / 1e7

    ticker_to_sector = dict(zip(universe["ticker"], universe["sector"]))
    merged["sector"] = merged["ticker"].map(ticker_to_sector)
    merged = merged.dropna(subset=["sector"])
    if merged.empty:
        return {}
    return merged.groupby("sector")["market_cap_cr"].sum().to_dict()


def compute_index_relative_strength(
    normalised_conn: Any, as_of_date: Optional[str] = None
) -> pd.DataFrame:
    """
    Trailing-21d return per tracked sector index (SECTOR_INDEX_MAP values)
    and Nifty 500, ranked by relative strength (sector return - Nifty 500
    return) descending.

    Parameters
    ----------
    normalised_conn : duckdb connection
        Open connection to DUCKDB_PATH (holds index_ohlcv).
    as_of_date : str, optional
        "YYYY-MM-DD" — restrict to index_ohlcv rows on or before this
        date. Defaults to the latest date index_ohlcv has for any tracked
        index.

    Returns
    -------
    pd.DataFrame
        Columns: sector, index_name, as_of_date, trailing_21d_return,
        nifty500_trailing_21d_return, relative_strength, rank.
        One row per SECTOR_INDEX_MAP entry whose index has enough real
        history; sectors without enough history are simply absent.
    """
    tracked_index_names = sorted(set(SECTOR_INDEX_MAP.values()) | {BENCHMARK_INDEX_NAME})
    placeholders = ",".join("?" for _ in tracked_index_names)
    params: List[Any] = list(tracked_index_names)
    where = f"index_name IN ({placeholders})"
    if as_of_date is not None:
        where += " AND date <= ?"
        params.append(as_of_date)

    df = normalised_conn.execute(
        f"SELECT date, index_name, close FROM index_ohlcv WHERE {where} ORDER BY index_name, date", params
    ).fetch_df()

    if df.empty:
        logger.warning("compute_index_relative_strength: no index_ohlcv rows for tracked indices")
        return pd.DataFrame(
            columns=[
                "sector", "index_name", "as_of_date", "trailing_21d_return",
                "nifty500_trailing_21d_return", "relative_strength", "rank",
            ]
        )

    resolved_as_of = pd.Timestamp(df["date"].max()).date().isoformat()

    nifty_rows = df[df["index_name"] == BENCHMARK_INDEX_NAME].sort_values("date")
    nifty_return = _trailing_return(nifty_rows["close"])
    nifty_returns_by_horizon = {
        h: _return_over_window(nifty_rows["close"], w) for h, w in RS_HORIZONS.items()
    }
    nifty_spark = _sparkline_series(nifty_rows["close"])

    records: List[Dict[str, Any]] = []
    for index_name in sorted(set(SECTOR_INDEX_MAP.values())):
        rows = df[df["index_name"] == index_name].sort_values("date")
        idx_return = _trailing_return(rows["close"])
        if idx_return is None or nifty_return is None:
            continue

        rs_by_horizon: Dict[str, Optional[float]] = {}
        for h, w in RS_HORIZONS.items():
            idx_h = _return_over_window(rows["close"], w)
            nifty_h = nifty_returns_by_horizon[h]
            rs_by_horizon[h] = None if idx_h is None or nifty_h is None else idx_h - nifty_h

        spark = _sparkline_series(rows["close"])

        for sector in sectors_for_index(index_name):
            records.append(
                {
                    "sector": sector,
                    "index_name": index_name,
                    "as_of_date": resolved_as_of,
                    "trailing_21d_return": idx_return,
                    "nifty500_trailing_21d_return": nifty_return,
                    "relative_strength": idx_return - nifty_return,
                    "rs_1d": rs_by_horizon.get("1d"),
                    "rs_5d": rs_by_horizon.get("5d"),
                    "rs_21d": rs_by_horizon.get("21d"),
                    "rs_63d": rs_by_horizon.get("63d"),
                    "sparkline": spark,
                    "nifty500_sparkline": nifty_spark,
                }
            )

    columns = [
        "sector", "index_name", "as_of_date", "trailing_21d_return",
        "nifty500_trailing_21d_return", "relative_strength",
        "rs_1d", "rs_5d", "rs_21d", "rs_63d", "sparkline", "nifty500_sparkline",
    ]
    result = pd.DataFrame(records, columns=columns)
    if result.empty:
        return result.assign(rank=pd.Series(dtype="int"), sector_market_cap_cr=pd.Series(dtype="float"))

    # `rank` stays relative-strength-based (unchanged, existing consumers
    # rely on it) — computed before the ML28 market-cap reordering below so
    # it still reflects "how in-favor is this sector" regardless of size.
    result = result.sort_values("relative_strength", ascending=False).reset_index(drop=True)
    result["rank"] = result.index + 1

    # ML28 — "ordered by market cap": each sector's real aggregate market
    # cap (sum of its constituents' own market cap, PIT-safe — see
    # _sector_market_cap_cr). Sectors with no computable market cap (no
    # real ohlcv_adjusted/fundamentals overlap for any constituent) sort
    # last rather than being guessed at.
    try:
        universe = load_universe()
    except Exception:
        universe = pd.DataFrame(columns=["ticker", "sector"])
    market_caps = _sector_market_cap_cr(normalised_conn, universe, as_of_date) if not universe.empty else {}
    result["sector_market_cap_cr"] = result["sector"].map(market_caps)
    result = result.sort_values(
        "sector_market_cap_cr", ascending=False, na_position="last"
    ).reset_index(drop=True)
    return result


def _latest_signal_rows(signals_conn: Any, table: str, tickers: List[str], as_of_date: Optional[str]) -> pd.DataFrame:
    """Latest real row per ticker (date <= as_of_date, or overall latest if None) from ml_signals/ml_multibagger."""
    if not tickers:
        return pd.DataFrame()
    placeholders = ",".join("?" for _ in tickers)
    date_filter = "AND date <= ?" if as_of_date is not None else ""
    params: List[Any] = list(tickers)
    if as_of_date is not None:
        params.append(as_of_date)
    query = f"""
        SELECT * FROM {table}
        WHERE ticker IN ({placeholders}) {date_filter}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) = 1
    """
    return signals_conn.execute(query, params).fetch_df()


def top_stocks_for_sector(
    signals_conn: Any, sector: str, as_of_date: Optional[str] = None, top_n: int = DEFAULT_TOP_N_STOCKS,
) -> pd.DataFrame:
    """
    Real latest ml_signals + ml_multibagger rows for every ticker in this
    sector (config.universe.load_universe()), ranked by buy_prob descending
    (model_name='signal_5d' preferred — same convention as
    dashboard/static/ml Daily Insights uses) then mb_probability, top_n rows.
    """
    universe = load_universe()
    tickers = universe.loc[universe["sector"] == sector, "ticker"].tolist()
    if not tickers:
        return pd.DataFrame()

    signals = _latest_signal_rows(signals_conn, "ml_signals", tickers, as_of_date)
    if not signals.empty and "model_name" in signals.columns:
        preferred = signals[signals["model_name"] == "signal_5d"]
        signals = preferred if not preferred.empty else signals

    multibagger = _latest_signal_rows(signals_conn, "ml_multibagger", tickers, as_of_date)

    if signals.empty and multibagger.empty:
        return pd.DataFrame()

    cols_signals = [c for c in ["ticker", "date", "signal_direction", "buy_prob", "meta_label", "exit_urgency"] if c in signals.columns]
    cols_mb = [c for c in ["ticker", "mb_probability", "mb_tier", "mb_archetype"] if c in multibagger.columns]

    merged = signals[cols_signals] if not signals.empty else pd.DataFrame({"ticker": tickers})
    if not multibagger.empty:
        merged = merged.merge(multibagger[cols_mb], on="ticker", how="outer")

    sort_cols = [c for c in ["buy_prob", "mb_probability"] if c in merged.columns]
    if sort_cols:
        merged = merged.sort_values(sort_cols, ascending=False, na_position="last")
    merged = merged.head(top_n).reset_index(drop=True)
    if "date" in merged.columns:
        # JSON/pydantic-safe: a bare pandas Timestamp round-trips oddly
        # through pydantic's json serializer (observed: a spurious "float
        # can't be interpreted as an integer" TypeError) — stringify here,
        # once, rather than leaving every caller (the API router, tests) to
        # rediscover the same serialization gotcha.
        merged["date"] = merged["date"].apply(lambda d: d.date().isoformat() if pd.notna(d) else None)
    return merged


def compute_sector_rotation_report(
    normalised_conn: Any, signals_conn: Any, as_of_date: Optional[str] = None, top_n_stocks: int = DEFAULT_TOP_N_STOCKS,
) -> Dict[str, Any]:
    """
    Full daily sector rotation report: ranked sectors by trailing-21d
    relative strength vs Nifty 500, each with its real top-N stocks by
    ml_signals buy_prob / ml_multibagger probability.

    Returns
    -------
    dict
        {"as_of_date": ..., "sectors": [ {sector, index_name, rank,
        trailing_21d_return, nifty500_trailing_21d_return,
        relative_strength, top_stocks: [...]}, ... ]}
    """
    ranked = compute_index_relative_strength(normalised_conn, as_of_date)
    if ranked.empty:
        return {"as_of_date": None, "sectors": []}

    resolved_as_of = ranked["as_of_date"].iloc[0]
    sectors_out = []
    for _, row in ranked.iterrows():
        top_stocks_df = top_stocks_for_sector(signals_conn, row["sector"], as_of_date=resolved_as_of, top_n=top_n_stocks)
        sectors_out.append(
            {
                "sector": row["sector"],
                "index_name": row["index_name"],
                "rank": int(row["rank"]),
                "trailing_21d_return": row["trailing_21d_return"],
                "nifty500_trailing_21d_return": row["nifty500_trailing_21d_return"],
                "relative_strength": row["relative_strength"],
                "rs_1d": row.get("rs_1d"),
                "rs_5d": row.get("rs_5d"),
                "rs_21d": row.get("rs_21d"),
                "rs_63d": row.get("rs_63d"),
                "sparkline": list(row["sparkline"]) if isinstance(row.get("sparkline"), (list, tuple)) else [],
                "nifty500_sparkline": list(row["nifty500_sparkline"]) if isinstance(row.get("nifty500_sparkline"), (list, tuple)) else [],
                "sector_market_cap_cr": row.get("sector_market_cap_cr") if pd.notna(row.get("sector_market_cap_cr")) else None,
                "top_stocks": top_stocks_df.to_dict(orient="records"),
            }
        )

    return {"as_of_date": resolved_as_of, "sectors": sectors_out}
