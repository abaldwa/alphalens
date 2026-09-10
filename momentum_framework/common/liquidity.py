"""
Liquidity & Circuit-Lock — ADTV computation and circuit-lock detection,
both computed directly from ohlcv_adjusted (no separate table needed;
verified 2026-09-04 against datastore/normalised/alphalens.duckdb's
schema — high/low/volume are all present there).

Used by:
  - R07 (circuit-lock check before trading a ticker — legacy's
    `_is_circuit_locked`)
  - R12 (ADTV-quintile liquidity interaction — the "+ Liquidity" half of
    its name, see strategies/r12_reversal_1mo.py)
  - Any future strategy needing an ADTV floor or circuit-lock filter.

Circuit-lock signature (ported from backtest/run_orchestrator_backtest.py's
comment on its own circuit-lock proxy, 2026-09-04): a real session with
high == low on a day that actually traded (volume > 0) is the
unambiguous signature of the band being hit and held for the whole
session — no intraday range means no fill was possible. A flat bar with
zero volume is a carried-forward price on a non-trading day, not a lock.
"""

from typing import Any, List, Optional, Set
import pandas as pd

ADTV_LOOKBACK_DAYS_DEFAULT = 20


def compute_adtv_cr(conn: Any, tickers: List[str], as_of_date: str,
                     lookback_days: int = ADTV_LOOKBACK_DAYS_DEFAULT) -> pd.Series:
    """
    Average Daily Traded Value (in crores) per ticker, over the trailing
    `lookback_days` sessions ending on/before as_of_date.
    ADTV_cr = mean(close * volume) / 1e7 (1 crore = 1e7).
    A ticker with no rows in the window is DROPPED, not defaulted to 0 —
    zero liquidity and unknown liquidity are different things.
    """
    if not tickers:
        return pd.Series(dtype=float)

    placeholders = ",".join("?" for _ in tickers)
    df = conn.execute(
        f"""
        SELECT ticker, date, close, volume
        FROM (
            SELECT ticker, date, close, volume,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
            FROM ohlcv_adjusted
            WHERE ticker IN ({placeholders}) AND date <= ?
        )
        WHERE rn <= ?
        """,
        list(tickers) + [as_of_date, lookback_days],
    ).fetch_df()

    if df.empty:
        return pd.Series(dtype=float)

    df["traded_value_cr"] = (df["close"] * df["volume"]) / 1e7
    return df.groupby("ticker")["traded_value_cr"].mean()


def get_circuit_locked_tickers(conn: Any, tickers: List[str], as_of_date: str) -> Set[str]:
    """
    Tickers that were circuit-locked (high == low, volume > 0) ON
    as_of_date specifically — a ticker locked on a rebalance date is
    unfillable at that close and should be skipped this rebalance, not
    treated as a normal trade.
    """
    if not tickers:
        return set()

    placeholders = ",".join("?" for _ in tickers)
    df = conn.execute(
        f"""
        SELECT ticker FROM ohlcv_adjusted
        WHERE ticker IN ({placeholders}) AND date = ?
          AND high = low AND volume > 0
        """,
        list(tickers) + [as_of_date],
    ).fetch_df()
    return set(df["ticker"].astype(str)) if not df.empty else set()


def get_circuit_locked_tickers_cached(tickers: List[str], as_of_date: str) -> Set[str]:
    """
    Same result as get_circuit_locked_tickers(), but reads from the
    pre-materialized circuit_lock_snapshots table (momentum_framework/
    cache/universe_cache.duckdb, built once by
    scripts/build_liquidity_cache.py) instead of scanning ohlcv_adjusted
    live on every call.

    WHY THIS EXISTS (2026-09-06): this check runs on EVERY rebalance date
    for EVERY job (added this session for Category B8's circuit-lock
    exclusion, both buy-side in orchestrator.py and sell-side in
    portfolio.py) — for a 5-day-cadence job over the full 2009-2026
    window that's ~880 live queries, measured at ~7ms each. Across a
    1,116-job campaign that adds up to real wall-clock time for zero
    benefit, since the underlying fact ("was ticker X locked on date D")
    never changes between jobs or campaigns — same principle as
    common/momentum_rank_cache.py's existing rank cache, applied here.

    Falls back to a live query (via get_circuit_locked_tickers, which
    needs a real prod connection this function doesn't have) is NOT done
    here — a caller needing that fallback should check
    momentum_rank_cache.CACHE_DB_PATH.exists() itself and choose which
    function to call, same pattern common/signals.py already uses for the
    rank cache.
    """
    if not tickers:
        return set()
    from momentum_framework.common.momentum_rank_cache import get_thread_cache_connection
    conn = get_thread_cache_connection()
    placeholders = ",".join("?" for _ in tickers)
    df = conn.execute(
        f"SELECT ticker FROM circuit_lock_snapshots WHERE ticker IN ({placeholders}) AND date = ?",
        list(tickers) + [as_of_date],
    ).fetch_df()
    return set(df["ticker"].astype(str)) if not df.empty else set()


def compute_adtv_cr_cached(tickers: List[str], as_of_date: str) -> pd.Series:
    """
    Same result as compute_adtv_cr() (default 20-day lookback, inclusive
    of as_of_date — see that function's docstring), but reads from the
    pre-materialized adtv_snapshots table instead of a live windowed
    aggregation over ohlcv_adjusted on every call. See
    get_circuit_locked_tickers_cached()'s docstring for why this matters
    at campaign scale — same reasoning, this function is ~42ms/call live.
    Verified 2026-09-06: matches compute_adtv_cr() to floating-point
    precision on real spot-checked tickers/dates.
    """
    if not tickers:
        return pd.Series(dtype=float)
    from momentum_framework.common.momentum_rank_cache import get_thread_cache_connection
    conn = get_thread_cache_connection()
    placeholders = ",".join("?" for _ in tickers)
    df = conn.execute(
        f"SELECT ticker, adtv_cr FROM adtv_snapshots WHERE ticker IN ({placeholders}) AND date = ?",
        list(tickers) + [as_of_date],
    ).fetch_df()
    if df.empty:
        return pd.Series(dtype=float)
    return df.set_index("ticker")["adtv_cr"]


def get_circuit_locked_tickers_auto(conn: Any, tickers: List[str], as_of_date: str) -> Set[str]:
    """
    Prefers the pre-materialized cache (get_circuit_locked_tickers_cached);
    falls back to the live query (get_circuit_locked_tickers) on ANY cache
    error — missing cache file, missing table (e.g. built before
    scripts/build_liquidity_cache.py existed), stale schema, etc. Same
    graceful-degradation convention common/signals.py already uses for the
    momentum-rank cache: never let a cache miss/error become a hard
    failure, only a slower correct answer. Use this at call sites instead
    of picking one function by hand.
    """
    if not tickers:
        return set()
    try:
        return get_circuit_locked_tickers_cached(tickers, as_of_date)
    except Exception:
        return get_circuit_locked_tickers(conn, tickers, as_of_date)


def compute_adtv_cr_auto(conn: Any, tickers: List[str], as_of_date: str) -> pd.Series:
    """Same fallback convention as get_circuit_locked_tickers_auto(), for ADTV."""
    if not tickers:
        return pd.Series(dtype=float)
    try:
        return compute_adtv_cr_cached(tickers, as_of_date)
    except Exception:
        return compute_adtv_cr(conn, tickers, as_of_date)


def filter_tradeable(conn: Any, tickers: List[str], as_of_date: str,
                      min_adtv_cr: Optional[float] = None,
                      adtv_lookback_days: int = ADTV_LOOKBACK_DAYS_DEFAULT) -> List[str]:
    """
    `tickers` minus circuit-locked names on as_of_date, minus any below
    `min_adtv_cr` (if supplied). Order-preserving. This is the combined
    filter a strategy applies to a candidate list right before trading it.
    """
    if not tickers:
        return []

    locked = get_circuit_locked_tickers(conn, tickers, as_of_date)
    survivors = [t for t in tickers if t not in locked]

    if min_adtv_cr is not None and survivors:
        adtv = compute_adtv_cr(conn, survivors, as_of_date, adtv_lookback_days)
        survivors = [t for t in survivors if adtv.get(t, 0.0) >= min_adtv_cr]

    return survivors


def liquidity_quintile_universe(conn: Any, tickers: List[str], as_of_date: str,
                                 quintile: int,
                                 adtv_lookback_days: int = ADTV_LOOKBACK_DAYS_DEFAULT) -> List[str]:
    """
    `tickers` restricted to ONE ADTV quintile (1 = least liquid fifth of
    `tickers` by trailing ADTV, 5 = most liquid fifth) — R12's "interaction
    of the reversal signal with liquidity quintiles" (spec 7.12): rank
    candidates within `tickers` by liquidity, keep only the requested
    fifth, and the caller applies its own ranking signal within that
    subset. Ties in pd.qcut fall to the same quintile, so band sizes may
    vary slightly, especially in small universes.
    """
    if quintile not in (1, 2, 3, 4, 5):
        raise ValueError(f"quintile must be 1-5, got {quintile}")
    if not tickers:
        return []

    adtv = compute_adtv_cr(conn, tickers, as_of_date, adtv_lookback_days)
    adtv = adtv.dropna()
    if len(adtv) < 5:
        # Too few tickers with measurable ADTV to form 5 quintiles — no
        # arbitrary split; return nothing rather than a misleading bucket.
        return []

    import pandas as pd
    labels = pd.qcut(adtv, 5, labels=[1, 2, 3, 4, 5], duplicates="drop")
    return [str(t) for t in labels[labels == quintile].index.tolist()]
