"""
ingestion/adjust/price_adjuster.py

Phase: 0.4 (Data Ingestion Scrapers)
Specs: SPEC-PIPE-002, SPEC-SCHED-010
Owner: Platform / Ingestion
Consumers: ingestion/scheduler, datastore/normalised

Applies retroactive corporate-action price adjustments to ohlcv_adjusted,
driven by the corporate_actions ledger — both DuckDB tables in Store 2
(SPEC-DS-007). MUST be idempotent (SPEC-PIPE-002): re-running for a ticker
that is already correctly adjusted is a no-op, verified by recomputing the
target cumulative adj_factor from the full corporate_actions history and
comparing it against each row's stored adj_factor, rather than blindly
re-multiplying ratios (which would NOT be idempotent).

NOTE on API_SPEC.md: that doc types `conn` as sqlite3.Connection. The
project's normalised store (ohlcv_adjusted, corporate_actions) lives in
DuckDB, not SQLite (SPEC-DS-007: "DuckDB for analytical stores ... SQLite
only for transactional stores"). This module accepts a DuckDB connection
(duckdb.DuckDBPyConnection); the parameter is left untyped at the
signature level so callers aren't forced to import duckdb just to satisfy
a type hint.

NOTE on adjustment direction: this task's instructions ("SPLIT: multiply
all pre-ex prices by 1/ratio") and 08_specifications.md's SPEC-PIPE-002
("SPLIT: pre-ex prices x ratio") give opposite directions for SPLIT. The
financially correct direction is 1/ratio: a 1-for-2 split (ratio=2) means
1 old share becomes 2 new shares, so a pre-split price of 100 must become
50 to be comparable to post-split per-share prices -- 100 * (1/2) = 50.
This module follows 1/ratio (this task's instruction, and the
standard/correct convention); 08_specifications.md's wording for SPLIT
appears to be an error and should be corrected there.
"""

import logging
from typing import List, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

MAX_CONTINUITY_GAP_PCT = 1.0  # SPEC-PIPE-002: < 1% gap at ex-date after adjustment
ADJ_FACTOR_TOLERANCE = 1e-9


def _action_factor(action_type: str, ratio: float, ticker: str) -> float:
    """
    Per-action multiplicative price factor (SPEC-PIPE-002).

    SPLIT: 1/ratio. BONUS: 1/(1+ratio). Unknown action types contribute no
    adjustment (factor 1.0) and are logged — never silently misapplied.
    """
    if action_type == "SPLIT":
        return 1.0 / ratio
    if action_type == "BONUS":
        return 1.0 / (1.0 + ratio)
    logger.warning(f"{ticker}: unknown corporate action_type '{action_type}' — ignoring (factor 1.0)")
    return 1.0


def adjust_for_corporate_actions(conn, ticker: str) -> None:
    """
    Apply all retroactive price adjustments for a ticker, idempotently.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Connection to the normalised store (ohlcv_adjusted,
        corporate_actions tables).
    ticker : str

    Returns
    -------
    None
        Updates ohlcv_adjusted in place.

    Spec References
    ----------------
    SPEC-PIPE-002: idempotent corporate-action adjustment; SPLIT -> 1/ratio,
        BONUS -> 1/(1+ratio); post-adjustment continuity check < 1% gap.
    SPEC-SCHED-010: atomic write — the bulk UPDATE is a single SQL
        statement, which DuckDB executes as one transaction.

    PIT Assumptions
    ----------------
    None — corporate_actions and ohlcv_adjusted are both already
    PIT-correct inputs; this function only transforms price magnitudes,
    never row availability.

    Raises
    ------
    None — unknown action types and missing data are logged and skipped,
    not raised, since a malformed single action should not abort
    adjustment of an otherwise-valid ticker.
    """
    actions_df = conn.execute(
        "SELECT ex_date, action_type, ratio FROM corporate_actions "
        "WHERE ticker = ? ORDER BY ex_date",
        [ticker],
    ).df()

    if actions_df.empty:
        logger.info(f"{ticker}: no corporate actions — nothing to adjust")
        return

    ohlcv_df = conn.execute(
        "SELECT date, open, high, low, close, adj_factor FROM ohlcv_adjusted "
        "WHERE ticker = ? ORDER BY date",
        [ticker],
    ).df()

    if ohlcv_df.empty:
        logger.info(f"{ticker}: no OHLCV rows — nothing to adjust")
        return

    actions_df["factor"] = [
        _action_factor(row.action_type, row.ratio, ticker) for row in actions_df.itertuples()
    ]

    ex_dates = pd.to_datetime(actions_df["ex_date"]).to_numpy()
    factors = actions_df["factor"].to_numpy(dtype="float64")
    row_dates = pd.to_datetime(ohlcv_df["date"]).to_numpy()

    # affects[i, j] = True iff action j's ex_date is strictly after row i's
    # date -- only actions that happen AFTER a row must adjust that row.
    affects = row_dates[:, None] < ex_dates[None, :]
    target_factor = np.exp((affects * np.log(factors)[None, :]).sum(axis=1))

    current_factor = ohlcv_df["adj_factor"].fillna(1.0).to_numpy(dtype="float64")
    needs_update = ~np.isclose(current_factor, target_factor, atol=ADJ_FACTOR_TOLERANCE)

    if not needs_update.any():
        logger.info(f"{ticker}: already correctly adjusted — idempotent no-op")
        check_price_continuity(conn, ticker, actions_df["ex_date"].tolist())
        return

    # Undo whatever adjustment is currently baked into the stored
    # ("ohlcv_adjusted") prices, then apply the freshly computed target —
    # this is what makes repeated calls converge rather than compound.
    rescale = target_factor / current_factor

    updated = ohlcv_df.loc[needs_update, ["date"]].copy()
    for col in ("open", "high", "low", "close"):
        updated[col] = ohlcv_df.loc[needs_update, col].to_numpy() * rescale[needs_update]
    updated["adj_factor"] = target_factor[needs_update]

    conn.register("_price_adjuster_updates", updated)
    try:
        conn.execute(
            """
            UPDATE ohlcv_adjusted
            SET open = u.open, high = u.high, low = u.low, close = u.close,
                adj_factor = u.adj_factor
            FROM _price_adjuster_updates u
            WHERE ohlcv_adjusted.ticker = ? AND ohlcv_adjusted.date = u.date
            """,
            [ticker],
        )
    finally:
        conn.unregister("_price_adjuster_updates")

    logger.info(f"{ticker}: adjusted {int(needs_update.sum())} rows for {len(actions_df)} corporate action(s)")
    check_price_continuity(conn, ticker, actions_df["ex_date"].tolist())


def get_adjustment_factor(conn, ticker: str, as_of_date: str) -> float:
    """
    Return the cumulative adjustment factor for a ticker as of a date.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
    ticker : str
    as_of_date : str
        "YYYY-MM-DD".

    Returns
    -------
    float
        adj_factor stored for (ticker, as_of_date). raw_price =
        adjusted_price / adj_factor.

    Spec References
    ----------------
    SPEC-PIPE-002

    PIT Assumptions
    ----------------
    None.

    Raises
    ------
    ValueError
        If no ohlcv_adjusted row exists for (ticker, as_of_date).
    """
    row = conn.execute(
        "SELECT adj_factor FROM ohlcv_adjusted WHERE ticker = ? AND date = ?",
        [ticker, as_of_date],
    ).fetchone()
    if row is None:
        raise ValueError(f"No ohlcv_adjusted row for {ticker} on {as_of_date}")
    return row[0]


def check_price_continuity(conn, ticker: str, ex_dates: List, max_gap_pct: float = MAX_CONTINUITY_GAP_PCT) -> bool:
    """
    Verify adjusted-price continuity at each corporate action's ex_date.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
    ticker : str
    ex_dates : list
        Ex-dates to check (corporate_actions.ex_date values).
    max_gap_pct : float
        Maximum allowed |pct change| in adjusted close from the last
        trading day before ex_date to ex_date itself.

    Returns
    -------
    bool
        True if every ex_date's gap is below max_gap_pct (or there is
        nothing to check); False if any ex_date violates it.

    Spec References
    ----------------
    SPEC-PIPE-002: "Post-adjustment: price continuity at ex-date < 1% gap."

    PIT Assumptions
    ----------------
    None.

    Raises
    ------
    None — violations are logged as warnings (a data-quality signal, not a
    hard validation gate: a genuine market move coinciding with an ex_date
    is possible).
    """
    df = conn.execute(
        "SELECT date, close FROM ohlcv_adjusted WHERE ticker = ? ORDER BY date",
        [ticker],
    ).df()
    if df.empty:
        return True

    dates = pd.to_datetime(df["date"]).to_numpy()
    closes = df["close"].to_numpy(dtype="float64")

    all_ok = True
    for ex_date in ex_dates:
        ex_date_np = pd.Timestamp(ex_date).to_datetime64()
        before_idx = np.searchsorted(dates, ex_date_np) - 1
        on_or_after_idx = np.searchsorted(dates, ex_date_np)
        if before_idx < 0 or on_or_after_idx >= len(dates):
            continue

        prev_close = closes[before_idx]
        ex_close = closes[on_or_after_idx]
        if prev_close == 0:
            continue

        gap_pct = abs(ex_close - prev_close) / prev_close * 100
        if gap_pct >= max_gap_pct:
            all_ok = False
            logger.warning(
                f"{ticker}: price continuity check failed at {ex_date} "
                f"({gap_pct:.2f}% gap, threshold {max_gap_pct}%)"
            )

    return all_ok
