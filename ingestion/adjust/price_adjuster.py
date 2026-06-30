"""
ingestion/adjust/price_adjuster.py

Phase: 0.4 (built) / 3.5 (dividend + volume + audit-table design)
Specs: SPEC-PIPE-002, SPEC-SCHED-010
Owner: Platform / Ingestion
Consumers: ingestion/scheduler, datastore/normalised

Applies retroactive corporate-action price and volume adjustments to
ohlcv_adjusted, driven by the corporate_actions ledger.

BACKWARD ADJUSTMENT (industry standard):
    Historical prices and volumes are rewritten so they are on the same
    per-share basis as today's prices. Today's NSE-reported price is always
    preserved unchanged; only rows that predate a CA's ex_date are modified.

AUDIT TABLE DESIGN (SPEC-PIPE-002):
    Before modifying any row, the adjuster captures original NSE values in
    ohlcv_ca_audit using ON CONFLICT semantics:
      - raw_* columns: first write wins (DO NOTHING on conflict) — original
        NSE price is preserved forever regardless of how many times the
        adjuster re-runs or new CAs arrive.
      - adj_factor / vol_adj_factor: always updated to the latest applied
        factors so the audit row is self-contained.

    Stocks with no corporate actions: adj_factor=1.0, vol_adj_factor=1.0,
    NO ohlcv_ca_audit entry (raw == adjusted, nothing to audit).

RESTORE A TICKER (SQL):
    UPDATE ohlcv_adjusted o
    SET open=a.raw_open, high=a.raw_high, low=a.raw_low, close=a.raw_close,
        volume=a.raw_volume, delivery_qty=a.raw_delivery_qty,
        adj_factor=1.0, vol_adj_factor=1.0
    FROM ohlcv_ca_audit a
    WHERE o.date=a.date AND o.ticker=a.ticker AND a.ticker='RELIANCE';

ACTION TYPE HANDLING:
    SPLIT    price_factor = 1/ratio            vol_factor = ratio
    BONUS    price_factor = 1/(1+ratio)        vol_factor = 1+ratio
    DIVIDEND price_factor = 1-(div/raw_close)  vol_factor = 1.0
    Others   price_factor = 1.0               vol_factor = 1.0

DIVIDEND RAW_CLOSE LOOKUP:
    COALESCE(ohlcv_ca_audit.raw_close,
             ohlcv_adjusted.close / GREATEST(adj_factor, 1e-12))
    Exact when the "day before" row has an audit entry (already adjusted);
    correct-within-float when it does not (adj_factor=1.0 → raw = close).

IDEMPOTENCY:
    Raw NSE values are recovered from current_adjusted / adj_factor.
    Rows whose (adj_factor, vol_adj_factor) already match the target are
    skipped. Result is identical on every call.

DELIVERY_PCT / TURNOVER:
    delivery_pct (= delivery_qty / volume) and turnover (price × volume)
    are invariant under the same-factor multiplication — NOT modified.
"""

import logging
from typing import List, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

MAX_CONTINUITY_GAP_PCT = 1.0   # SPEC-PIPE-002
ADJ_FACTOR_TOLERANCE   = 1e-9
_GUARD_ZERO            = 1e-12  # prevents division-by-zero in factor recovery


def _dividend_price_factor(conn, ticker: str, ex_date: str, dividend: float) -> float:
    """
    factor = 1 - (dividend / raw_close_on_last_trading_day_before_ex_date)

    raw_close = COALESCE(audit.raw_close, ohlcv.close / adj_factor)
    Returns 1.0 (no-op) if the reference price is unavailable or the
    computed factor would be invalid.
    """
    row = conn.execute(
        """
        SELECT COALESCE(
            a.raw_close,
            o.close / GREATEST(o.adj_factor, ?)
        ) AS raw_close
        FROM ohlcv_adjusted o
        LEFT JOIN ohlcv_ca_audit a
               ON a.date = o.date AND a.ticker = o.ticker
        WHERE o.ticker = ?
          AND o.date   < ?
          AND o.close  > 0
        ORDER BY o.date DESC
        LIMIT 1
        """,
        [_GUARD_ZERO, ticker, ex_date],
    ).fetchone()

    if row is None or row[0] is None or row[0] <= 0:
        logger.warning(
            f"{ticker}: no valid raw_close before {ex_date} "
            f"(dividend={dividend}) — skipping dividend adjustment"
        )
        return 1.0

    close_before = float(row[0])
    if dividend >= close_before:
        logger.warning(
            f"{ticker}: dividend {dividend} >= close_before {close_before} "
            f"at {ex_date} — extraordinary/data error, skipping"
        )
        return 1.0

    factor = 1.0 - (dividend / close_before)
    if factor <= 0:
        logger.warning(f"{ticker}: dividend factor {factor:.6f} at {ex_date} invalid — skipping")
        return 1.0

    return factor


def _action_factors(
    conn, ticker: str, action_type: str, ratio: float, ex_date: str
) -> Tuple[float, float]:
    """
    Return (price_factor, vol_factor) for a single corporate action.
    Both default to (1.0, 1.0) for unrecognised or non-adjustable types.
    """
    if action_type == "SPLIT":
        if ratio <= 0:
            logger.warning(f"{ticker}: SPLIT ratio={ratio} invalid at {ex_date}")
            return 1.0, 1.0
        return 1.0 / ratio, float(ratio)

    if action_type == "BONUS":
        if ratio <= 0:
            logger.warning(f"{ticker}: BONUS ratio={ratio} invalid at {ex_date}")
            return 1.0, 1.0
        return 1.0 / (1.0 + ratio), 1.0 + ratio

    if action_type == "DIVIDEND":
        if ratio <= 0:
            logger.debug(f"{ticker}: DIVIDEND ratio=0 at {ex_date} — skipping")
            return 1.0, 1.0
        return _dividend_price_factor(conn, ticker, ex_date, ratio), 1.0

    logger.debug(f"{ticker}: {action_type} — no price/volume adjustment")
    return 1.0, 1.0


def adjust_for_corporate_actions(conn, ticker: str) -> None:
    """
    Apply all retroactive price and volume adjustments for a ticker, idempotently.

    Steps
    -----
    1. Read corporate_actions for this ticker (ordered by ex_date).
    2. Compute (price_factor, vol_factor) per action.
    3. Vectorise cumulative factor: rows × actions affects matrix.
    4. Skip rows whose stored factors already equal the target.
    5. Recover original NSE values: raw = current / adj_factor.
    6. INSERT into ohlcv_ca_audit (preserve raw_* on conflict; update factors).
    7. UPDATE ohlcv_adjusted with new adjusted values.
    8. Post-adjustment price-continuity check.

    Spec References: SPEC-PIPE-002, SPEC-SCHED-010
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
        """
        SELECT date, open, high, low, close, volume, delivery_qty,
               adj_factor, vol_adj_factor
        FROM ohlcv_adjusted
        WHERE ticker = ?
        ORDER BY date
        """,
        [ticker],
    ).df()

    if ohlcv_df.empty:
        logger.info(f"{ticker}: no OHLCV rows — nothing to adjust")
        return

    # Per-action factors -------------------------------------------------------
    p_factors, v_factors = [], []
    for row in actions_df.itertuples():
        pf, vf = _action_factors(conn, ticker, row.action_type, row.ratio, str(row.ex_date))
        p_factors.append(pf)
        v_factors.append(vf)

    actions_df["price_factor"] = p_factors
    actions_df["vol_factor"]   = v_factors

    valid = (np.array(p_factors) > 0) & (np.array(v_factors) > 0)
    if not valid.all():
        bad = actions_df.loc[~valid, "ex_date"].tolist()
        logger.warning(f"{ticker}: dropping {(~valid).sum()} degenerate action(s) at {bad}")
        actions_df = actions_df[valid].reset_index(drop=True)
        if actions_df.empty:
            return

    # Vectorised cumulative factor computation ---------------------------------
    # affects[i, j] = True  iff  row_date[i] < ex_date[j]
    ex_dates  = pd.to_datetime(actions_df["ex_date"]).to_numpy()
    p_arr     = actions_df["price_factor"].to_numpy(dtype="float64")
    v_arr     = actions_df["vol_factor"].to_numpy(dtype="float64")
    row_dates = pd.to_datetime(ohlcv_df["date"]).to_numpy()

    affects         = row_dates[:, None] < ex_dates[None, :]
    target_price_adj = np.exp((affects * np.log(p_arr)[None, :]).sum(axis=1))
    target_vol_adj   = np.exp((affects * np.log(v_arr)[None, :]).sum(axis=1))

    # Idempotency gate ---------------------------------------------------------
    cur_p = ohlcv_df["adj_factor"].fillna(1.0).to_numpy(dtype="float64")
    cur_v = ohlcv_df["vol_adj_factor"].fillna(1.0).to_numpy(dtype="float64")

    price_needs  = ~np.isclose(cur_p, target_price_adj, atol=ADJ_FACTOR_TOLERANCE)
    vol_needs    = ~np.isclose(cur_v, target_vol_adj,   atol=ADJ_FACTOR_TOLERANCE)
    needs_update = price_needs | vol_needs

    if not needs_update.any():
        logger.info(f"{ticker}: already correctly adjusted — idempotent no-op")
        check_price_continuity(conn, ticker, actions_df["ex_date"].tolist())
        return

    # Build staging data -------------------------------------------------------
    mask  = needs_update
    tp    = target_price_adj[mask]
    tv    = target_vol_adj[mask]
    adj_f = cur_p[mask].clip(min=_GUARD_ZERO)
    vol_f = cur_v[mask].clip(min=_GUARD_ZERO)

    stage = ohlcv_df.loc[mask, [
        "date", "open", "high", "low", "close", "volume", "delivery_qty",
    ]].copy()

    # Recover original NSE values: raw = current_adjusted / current_factor.
    # First run (adj_factor=1.0): raw = current, which IS the NSE price.
    # Re-run: ON CONFLICT keeps already-stored raw_* values; these computed
    # values are only used for the first INSERT and then ignored.
    stage["raw_open"]  = stage["open"].to_numpy()  / adj_f
    stage["raw_high"]  = stage["high"].to_numpy()  / adj_f
    stage["raw_low"]   = stage["low"].to_numpy()   / adj_f
    stage["raw_close"] = stage["close"].to_numpy() / adj_f
    stage["raw_volume"] = np.round(
        stage["volume"].to_numpy(dtype="float64") / vol_f
    ).astype("int64")

    dq     = stage["delivery_qty"]
    has_dq = dq.notna()
    raw_dq = pd.array([pd.NA] * len(stage), dtype="Int64")
    if has_dq.any():
        raw_dq[has_dq.to_numpy()] = np.round(
            dq[has_dq].astype(float).to_numpy() / vol_f[has_dq.to_numpy()]
        ).astype("int64")
    stage["raw_delivery_qty"] = raw_dq

    # New adjusted values: new = raw × target_factor
    stage["new_open"]  = stage["raw_open"].to_numpy()  * tp
    stage["new_high"]  = stage["raw_high"].to_numpy()  * tp
    stage["new_low"]   = stage["raw_low"].to_numpy()   * tp
    stage["new_close"] = stage["raw_close"].to_numpy() * tp
    stage["new_volume"] = np.round(
        stage["raw_volume"].to_numpy(dtype="float64") * tv
    ).astype("int64")

    new_dq = pd.array([pd.NA] * len(stage), dtype="Int64")
    if has_dq.any():
        new_dq[has_dq.to_numpy()] = np.round(
            stage["raw_delivery_qty"][has_dq].astype(float).to_numpy()
            * tv[has_dq.to_numpy()]
        ).astype("int64")
    stage["new_delivery_qty"] = new_dq

    stage["new_adj_factor"]     = tp
    stage["new_vol_adj_factor"] = tv
    stage["ticker_col"]         = ticker

    # Step A: INSERT into ohlcv_ca_audit ---------------------------------------
    # raw_* columns preserved on conflict (first NSE value wins forever).
    # adj_factor / vol_adj_factor updated to reflect the latest applied state.
    conn.register("_pa_audit", stage[[
        "date", "ticker_col",
        "raw_open", "raw_high", "raw_low", "raw_close",
        "raw_volume", "raw_delivery_qty",
        "new_adj_factor", "new_vol_adj_factor",
    ]])
    try:
        conn.execute(
            """
            INSERT INTO ohlcv_ca_audit
                (date, ticker,
                 raw_open, raw_high, raw_low, raw_close,
                 raw_volume, raw_delivery_qty,
                 adj_factor, vol_adj_factor)
            SELECT u.date, u.ticker_col,
                   u.raw_open, u.raw_high, u.raw_low, u.raw_close,
                   CAST(u.raw_volume AS BIGINT),
                   CAST(u.raw_delivery_qty AS BIGINT),
                   u.new_adj_factor, u.new_vol_adj_factor
            FROM _pa_audit u
            ON CONFLICT (date, ticker) DO UPDATE SET
                adj_factor     = excluded.adj_factor,
                vol_adj_factor = excluded.vol_adj_factor
            """
        )
    finally:
        conn.unregister("_pa_audit")

    # Step B: UPDATE ohlcv_adjusted --------------------------------------------
    conn.register("_pa_updates", stage[[
        "date", "ticker_col",
        "new_open", "new_high", "new_low", "new_close",
        "new_volume", "new_delivery_qty",
        "new_adj_factor", "new_vol_adj_factor",
    ]])
    try:
        conn.execute(
            """
            UPDATE ohlcv_adjusted
            SET open           = u.new_open,
                high           = u.new_high,
                low            = u.new_low,
                close          = u.new_close,
                volume         = CAST(u.new_volume AS BIGINT),
                delivery_qty   = CAST(u.new_delivery_qty AS BIGINT),
                adj_factor     = u.new_adj_factor,
                vol_adj_factor = u.new_vol_adj_factor
            FROM _pa_updates u
            WHERE ohlcv_adjusted.ticker = u.ticker_col
              AND ohlcv_adjusted.date   = u.date
            """
        )
    finally:
        conn.unregister("_pa_updates")

    logger.info(
        f"{ticker}: adjusted {int(needs_update.sum())} rows "
        f"(price: {int(price_needs.sum())}, vol: {int(vol_needs.sum())}) "
        f"across {len(actions_df)} corporate action(s)"
    )
    check_price_continuity(conn, ticker, actions_df["ex_date"].tolist())


def get_adjustment_factor(conn, ticker: str, as_of_date: str) -> float:
    """
    Return cumulative price adj factor for (ticker, as_of_date).
    raw_price = adjusted_price / adj_factor.
    Raises ValueError if no row exists.
    """
    row = conn.execute(
        "SELECT adj_factor FROM ohlcv_adjusted WHERE ticker = ? AND date = ?",
        [ticker, as_of_date],
    ).fetchone()
    if row is None:
        raise ValueError(f"No ohlcv_adjusted row for {ticker} on {as_of_date}")
    return float(row[0])


def get_vol_adjustment_factor(conn, ticker: str, as_of_date: str) -> float:
    """
    Return cumulative volume adj factor for (ticker, as_of_date).
    raw_volume = adjusted_volume / vol_adj_factor.
    Raises ValueError if no row exists.
    """
    row = conn.execute(
        "SELECT vol_adj_factor FROM ohlcv_adjusted WHERE ticker = ? AND date = ?",
        [ticker, as_of_date],
    ).fetchone()
    if row is None:
        raise ValueError(f"No ohlcv_adjusted row for {ticker} on {as_of_date}")
    return float(row[0])


def check_price_continuity(
    conn,
    ticker: str,
    ex_dates: List,
    max_gap_pct: float = MAX_CONTINUITY_GAP_PCT,
) -> bool:
    """
    Verify adjusted-price continuity at each corporate action's ex_date.

    |close[ex_date] − close[day_before]| / close[day_before] < max_gap_pct.
    Violations are WARNINGs — genuine market moves on ex_date can legitimately
    exceed the threshold; this is a data-quality signal, not a hard gate.

    Spec References: SPEC-PIPE-002
    """
    df = conn.execute(
        "SELECT date, close FROM ohlcv_adjusted WHERE ticker = ? ORDER BY date",
        [ticker],
    ).df()
    if df.empty:
        return True

    dates  = pd.to_datetime(df["date"]).to_numpy()
    closes = df["close"].to_numpy(dtype="float64")
    all_ok = True

    for ex_date in ex_dates:
        ex_dt           = pd.Timestamp(ex_date).to_datetime64()
        before_idx      = np.searchsorted(dates, ex_dt) - 1
        on_or_after_idx = np.searchsorted(dates, ex_dt)

        if before_idx < 0 or on_or_after_idx >= len(dates):
            continue

        prev_close = closes[before_idx]
        ex_close   = closes[on_or_after_idx]
        if prev_close == 0:
            continue

        gap_pct = abs(ex_close - prev_close) / prev_close * 100
        if gap_pct >= max_gap_pct:
            all_ok = False
            logger.warning(
                f"{ticker}: continuity check at {ex_date}: "
                f"{gap_pct:.2f}% gap (threshold {max_gap_pct}%)"
            )

    return all_ok
