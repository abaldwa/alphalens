"""
Band-Attached Benchmark Equity Curves

Explicit user instruction (2026-09-04): "Index Equity Curve is to be
attached to a band and not to a strategy." Both R07's crash detection
and R09's regime detection need a real market-index equity curve — this
module is the ONE place that resolves band_id -> benchmark index ->
price series, so R07 and R09 (and anything else band-scoped in the
future) share the identical benchmark for a given band rather than each
strategy instance being handed one ad hoc.

Verified against datastore/normalised/alphalens.duckdb::index_ohlcv
(2026-09-04): every mapping below except M7 resolves to a real
`index_name` with full 2006-2026 history.
"""

from typing import Any, Dict, Optional
import pandas as pd

#: band_id -> index_ohlcv.index_name. M13 -> "Nifty 500" per explicit
#: user instruction. M7 has NO resolved mapping — "Nifty Midcap 250"
#: (common/universe.py's MBand.nifty_benchmark string for M7) does not
#: exist in index_ohlcv; only Midcap 50/100/150 do. Flagged, not guessed
#: — resolve_benchmark_index() raises for band_id=7 until this is decided.
BAND_BENCHMARK_INDEX: Dict[int, str] = {
    2: "Nifty 50",
    4: "Nifty Midcap 150",
    9: "Nifty Smallcap 250",
    10: "Nifty Smallcap 250",
    12: "Nifty Microcap 250",
    13: "Nifty 500",
}

UNRESOLVED_BANDS = frozenset({7})  # see module comment — no matching index_ohlcv entry


def resolve_benchmark_index(band_id: int) -> str:
    """The index_ohlcv.index_name attached to `band_id`. Raises for
    unmapped/unresolved bands rather than silently picking a substitute."""
    if band_id in UNRESOLVED_BANDS:
        raise ValueError(
            f"band_id={band_id} (M7) has no resolved benchmark index — "
            f"\"Nifty Midcap 250\" is not present in index_ohlcv (only "
            f"Midcap 50/100/150 are). Needs a user decision: which real "
            f"index should M7 use? See common/benchmark.py's module docstring."
        )
    if band_id not in BAND_BENCHMARK_INDEX:
        raise ValueError(f"band_id={band_id} has no benchmark index mapping at all")
    return BAND_BENCHMARK_INDEX[band_id]


def load_benchmark_equity_curve(
    band_id: int,
    conn: Any,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.Series:
    """
    Loads the close-price series (as a pd.Series indexed by date) for
    `band_id`'s attached benchmark index from `index_ohlcv`. This is what
    R07's crash detector and R09's regime detector both consume — same
    band, same benchmark, never two different series for one band.
    """
    index_name = resolve_benchmark_index(band_id)

    query = "SELECT date, close FROM index_ohlcv WHERE index_name = ?"
    params = [index_name]
    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)
    query += " ORDER BY date"

    df = conn.execute(query, params).fetch_df()
    if df.empty:
        return pd.Series(dtype=float)

    df["date"] = pd.to_datetime(df["date"])
    return df.set_index("date")["close"]
