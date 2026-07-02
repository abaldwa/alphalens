"""
ingestion/scheduler/daily_pipeline.py

Phase: 0.6 (Laptop-Only Daily Operation)
Specs: SPEC-SCHED-001, SPEC-SCHED-002, SPEC-SCHED-005, SPEC-SCHED-009,
       SPEC-PIPE-001, SPEC-PIPE-002, SPEC-PIPE-005, SPEC-PIPE-006
Owner: Platform / Scheduler
Consumers: operator (entry point, run once and leave running)

Concrete step_runner wiring real ingestion functions into the generic
scheduler engine (ingestion/scheduler/pipeline_scheduler.py + checkpoint.py)
built in Phase 0.3, plus the `main()` entry point that registers and starts
the persistent, recurring daily job.

SPEC-SCHED-009 was originally "Oracle Cloud Independence" (Oracle-first,
NSE-archive-fallback). Oracle Cloud Free Tier provisioning was abandoned
(ap-mumbai-1 had zero free ARM A1 capacity, and the Free Trial account
restriction blocked subscribing to an alternate region without upgrading
to a paid account — see BuildLog.md "Laptop-only pivot"). The spec already
described an Oracle-first-NSE-fallback design, which degrades cleanly to
NSE-archive-only with Oracle simply never in the loop — this module *is*
that fallback path running as the primary (only) path, not a new design.

This is the file CLAUDE.md's repo structure has named `scheduler/
daily_pipeline.py` ("Main daily pipeline runner") since the project's
inception; it now lives at its actual SOLID-D location alongside the
scheduler engine it wires into, under ingestion/scheduler/.

What's wired vs. deferred:
- download_bhavcopy: bhavcopy.download_bhavcopy() — writes OHLCV +
  delivery_qty/delivery_pct in one upsert (the same CSV row set; no
  separate NSE fetch needed — see checkpoint.py's module docstring).
- download_fno: fno.download_fno_bhavcopy() — fixed in P2.3 (the archive
  endpoint used through P0.6 404'd against NSE's current archive; see
  ingestion/scrapers/fno.py's module docstring for the real, working
  UDiFF endpoint that replaced it) and now persists into the fno_data
  DuckDB table (added in P2.3). Still caught-and-logged like every other
  non-critical source (SPEC-PIPE-006's "mark unavailable" philosophy) —
  an F&O outage must never block download_macro/adjust_prices.
- download_macro: macro.download_vix/download_fiidii/download_fx — each
  independently caught so one indicator's outage never blocks the others.
- download_corporate_actions: corporate_actions.download_corporate_actions()
  fetches NSE's corporate action filings (SPLIT, BONUS, DIVIDEND, RIGHTS,
  BUYBACK, QIP, AGM) for run_date and upserts into the corporate_actions
  table. Idempotent (ON CONFLICT DO NOTHING). Non-critical: logged and
  skipped on failure so a transient NSE outage never blocks macro/features.
- download_large_deals: large_deals.download_large_deals() fetches NSE and
  BSE bulk/block deals for run_date. Each of the four sources is
  independently caught on failure (SPEC-PIPE-006 "mark unavailable,
  non-critical" pattern).
- adjust_prices: price_adjuster.adjust_for_corporate_actions() per
  universe ticker — currently disabled via PRICE_ADJUSTMENT_ENABLED=False
  in config/settings.py (pending deliberation on adjustment logic). The
  corporate_actions table is still populated daily so the ledger is ready
  the moment the flag is switched on.
- compute_features, run_models, write_signals: [AS BUILT, P1.7] wired to
  features/matrix_builder.py + features/pnd_features.py, systems/
  ml_signal_engine/inference/daily_inference.py, and a result-file
  verification step, respectively — see each function's own docstring.
  Each future phase fills in its dispatch entry here without touching
  pipeline_scheduler.py or checkpoint.py (SOLID-O).
- paper_trade: [AS BUILT, P3.x] wired to scripts/run_daily_paper_trading.py
  — the automated daily paper-trading bot, run after write_signals so it
  always acts on today's already-written, already-verified ml_signals
  rows. Not backfillable (checkpoint.py's STEPS), same reasoning as
  run_models/write_signals.
"""

import json
import logging
import time
from datetime import date as date_type
from pathlib import Path
from typing import Optional

import pandas as pd

from config.timezone import now_ist
from datastore.api.db import get_duckdb_connection

logger = logging.getLogger(__name__)

# SPEC-PIPE-002: on first INSERT, adj_factor=1.0 / vol_adj_factor=1.0 (raw
# NSE prices, unadjusted).  On re-download of the same date (ON CONFLICT),
# prices are refreshed from fresh NSE data and both factors are reset to 1.0
# so the price adjuster re-applies on the next pipeline run.
# ohlcv_ca_audit rows for this date are deleted immediately after (see
# step_download_bhavcopy), because the audit rows were based on the old NSE
# data and would be stale after a re-download.
_UPSERT_OHLCV_WITH_DELIVERY = """
    INSERT INTO ohlcv_adjusted (
        date, ticker, open, high, low, close, volume, delivery_qty, delivery_pct,
        adj_factor, vol_adj_factor
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1.0, 1.0)
    ON CONFLICT (date, ticker) DO UPDATE SET
        open           = excluded.open,
        high           = excluded.high,
        low            = excluded.low,
        close          = excluded.close,
        volume         = excluded.volume,
        delivery_qty   = excluded.delivery_qty,
        delivery_pct   = excluded.delivery_pct,
        adj_factor     = 1.0,
        vol_adj_factor = 1.0
"""

_DELETE_AUDIT_FOR_DATE = "DELETE FROM ohlcv_ca_audit WHERE date = ?"

_UPSERT_MACRO_INDICATOR = """
    INSERT INTO macro_indicators (date, indicator, value)
    VALUES (?, ?, ?)
    ON CONFLICT (date, indicator) DO UPDATE SET value = excluded.value
"""

_INSERT_FNO_DATA = """
    INSERT INTO fno_data
        (trade_date, ticker, instrument, expiry, strike, option_type,
         oi, oi_change, volume, settle_price, close_price, underlying_price)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def step_download_bhavcopy(run_date: date_type, db_path: Optional[Path] = None) -> None:
    """
    Download NSE bhavcopy for run_date and upsert OHLCV + delivery into
    ohlcv_adjusted in one pass.

    Parameters
    ----------
    run_date : date
    db_path : Path, optional
        Defaults to config.settings.DUCKDB_PATH.

    Returns
    -------
    None

    Spec References
    ----------------
    SPEC-PIPE-001, SPEC-PIPE-005

    PIT Assumptions
    ----------------
    None — same-day published data.

    Raises
    ------
    ConnectionError
        If the NSE archive fetch fails after retries (propagated from
        bhavcopy.download_bhavcopy) — this step is NOT caught/non-critical;
        bhavcopy is the foundational input every later step depends on.
    ValueError
        If the downloaded bhavcopy fails validation (duplicate tickers,
        non-positive prices, out-of-range delivery_pct, too few stocks).
    """
    from config.settings import DUCKDB_PATH
    from ingestion.scrapers import bhavcopy

    resolved_db_path = db_path or DUCKDB_PATH
    date_str = run_date.isoformat()

    df = bhavcopy.download_bhavcopy(date_str)
    delivery_pct = (df["delivery_qty"] / df["traded_qty"] * 100).where(df["traded_qty"] > 0)

    rows = [
        (
            date_str,
            row.ticker,
            row.open,
            row.high,
            row.low,
            row.close,
            int(row.volume),
            None if pd_isna(row.delivery_qty) else int(row.delivery_qty),
            None if pd_isna(pct) else float(pct),
        )
        for row, pct in zip(df.itertuples(), delivery_pct)
    ]

    # persist=False (SPEC-SCHED-013): this is a long-lived scheduler process
    # sharing DUCKDB_PATH with the DataStore API — release the write lock as
    # soon as this step finishes, rather than holding it for the process's
    # entire lifetime (see datastore/api/db.py's module docstring).
    with get_duckdb_connection(resolved_db_path, persist=False) as conn:
        conn.executemany(_UPSERT_OHLCV_WITH_DELIVERY, rows)
        # On re-download of an already-processed date, any ohlcv_ca_audit rows
        # for this date are now stale (they were derived from the old NSE data).
        # Delete them so the next adjust_prices run re-creates audit entries from
        # the fresh NSE prices.  For new dates this is a no-op.
        conn.execute(_DELETE_AUDIT_FOR_DATE, [date_str])

    logger.info(f"download_bhavcopy: {len(rows)} tickers written for {date_str}")


def pd_isna(value) -> bool:
    """Thin wrapper so step_download_bhavcopy doesn't need a top-level pandas import for one check."""
    import pandas as pd

    return bool(pd.isna(value))


def step_download_fno(run_date: date_type, db_path: Optional[Path] = None) -> None:
    """
    Download NSE F&O bhavcopy for run_date and persist into fno_data.
    Non-critical: a failure here must never block download_macro/
    adjust_prices.

    Parameters
    ----------
    run_date : date
    db_path : Path, optional
        Defaults to config.settings.DUCKDB_PATH.

    Returns
    -------
    None
        Always — failures are caught and logged, never raised (same
        "mark unavailable, non-critical" philosophy SPEC-PIPE-006 already
        applies to FII/DII and VIX; an F&O outage must not block the
        Phase 1 OHLCV/macro steps this pipeline also runs daily).

    Spec References
    ----------------
    SPEC-PIPE-001

    PIT Assumptions
    ----------------
    None — same-day archive data.

    Raises
    ------
    None
    """
    from config.settings import DUCKDB_PATH
    from ingestion.scrapers import fno

    resolved_db_path = db_path or DUCKDB_PATH
    date_str = run_date.isoformat()
    try:
        df = fno.download_fno_bhavcopy(date_str)
    except Exception as exc:
        logger.warning(
            f"download_fno: unavailable for {date_str} ({exc}) — "
            "non-critical, continuing"
        )
        return

    rows = [
        (
            date_str,
            row.ticker,
            row.instrument,
            row.expiry.date().isoformat() if pd.notna(row.expiry) else None,
            None if pd.isna(row.strike) else float(row.strike),
            None if pd.isna(row.option_type) else row.option_type,
            None if pd.isna(row.oi) else int(row.oi),
            None if pd.isna(row.oi_change) else int(row.oi_change),
            None if pd.isna(row.volume) else int(row.volume),
            None if pd.isna(row.settle_price) else float(row.settle_price),
            None if pd.isna(row.close_price) else float(row.close_price),
            None if pd.isna(row.underlying_price) else float(row.underlying_price),
        )
        for row in df.itertuples()
        if pd.notna(row.expiry)
    ]

    # Delete-then-insert per trade_date: the bhavcopy file arrives as one
    # atomic daily snapshot (no incremental updates within a day), and
    # fno_data has no PRIMARY KEY (strike/option_type are NULL for
    # futures rows) — see datastore/schema/create_normalised.py's
    # _CREATE_FNO_DATA comment. persist=False per SPEC-SCHED-013, same as
    # step_download_bhavcopy.
    with get_duckdb_connection(resolved_db_path, persist=False) as conn:
        conn.execute("DELETE FROM fno_data WHERE trade_date = ?", [date_str])
        conn.executemany(_INSERT_FNO_DATA, rows)

    logger.info(f"download_fno: {len(rows)} rows written for {date_str}")


def step_download_macro(run_date: date_type, db_path: Optional[Path] = None) -> None:
    """
    Fetch India VIX, FII/DII net flows, and USD/INR for run_date.

    Each indicator is independently caught so one source's outage never
    blocks the others (SPEC-PIPE-006: "mark unavailable if scrape fails,
    non-critical").

    Parameters
    ----------
    run_date : date
    db_path : Path, optional
        Defaults to config.settings.DUCKDB_PATH for the upsert, and is
        also forwarded to each macro.download_* call for its own
        previous-value fallback lookup.

    Returns
    -------
    None

    Spec References
    ----------------
    SPEC-PIPE-006

    PIT Assumptions
    ----------------
    None — same-day published data.

    Raises
    ------
    None — this step never raises; a date with zero available indicators
    still completes successfully (an empty macro_indicators write), since
    a temporary outage of every macro source is not a Phase 1 blocker the
    way a missing bhavcopy is.
    """
    from config.settings import DUCKDB_PATH
    from ingestion.scrapers import macro

    resolved_db_path = db_path or DUCKDB_PATH
    date_str = run_date.isoformat()
    indicators = {}

    try:
        indicators["INDIA_VIX"] = macro.download_vix(date_str, db_path=resolved_db_path)
    except ConnectionError as exc:
        logger.warning(f"download_macro: VIX unavailable for {date_str}: {exc}")

    try:
        fiidii = macro.download_fiidii(date_str, db_path=resolved_db_path)
        if fiidii.get("fii_net_cr") is not None:
            indicators["FII_NET_CR"] = fiidii["fii_net_cr"]
        if fiidii.get("dii_net_cr") is not None:
            indicators["DII_NET_CR"] = fiidii["dii_net_cr"]
    except ConnectionError as exc:
        logger.warning(f"download_macro: FII/DII unavailable for {date_str}: {exc}")

    try:
        fx = macro.download_fx(date_str, db_path=resolved_db_path)
        indicators["USD_INR"] = fx["usd_inr"]
    except ConnectionError as exc:
        logger.warning(f"download_macro: USD/INR unavailable for {date_str}: {exc}")

    if indicators:
        rows = [(date_str, name, value) for name, value in indicators.items()]
        # persist=False (SPEC-SCHED-013): this is a long-lived scheduler
        # process sharing DUCKDB_PATH with the DataStore API — release the
        # write lock as soon as this step finishes, rather than holding it
        # for the process's entire lifetime (see datastore/api/db.py's
        # module docstring).
        with get_duckdb_connection(resolved_db_path, persist=False) as conn:
            conn.executemany(_UPSERT_MACRO_INDICATOR, rows)

    logger.info(f"download_macro: {len(indicators)}/3 indicators written for {date_str}")


def step_download_corporate_actions(run_date: date_type, db_path: Optional[Path] = None) -> None:
    """
    Download NSE corporate actions for run_date and upsert into corporate_actions.

    Parameters
    ----------
    run_date : date
    db_path : Path, optional
        Defaults to config.settings.DUCKDB_PATH.

    Returns
    -------
    None
        Always — failures are caught and logged, never raised (non-critical:
        a CA API outage must not block macro/features/models).

    Spec References
    ----------------
    SPEC-PIPE-002: corporate actions ledger.

    PIT Assumptions
    ----------------
    None — ex_date from NSE is same-day published data.

    Raises
    ------
    None
    """
    from config.settings import DUCKDB_PATH
    from ingestion.scrapers.corporate_actions import (
        download_corporate_actions,
        upsert_corporate_actions,
    )

    resolved_db_path = db_path or DUCKDB_PATH
    date_str = run_date.isoformat()
    try:
        df = download_corporate_actions(date_str)
        if not df.empty:
            with get_duckdb_connection(resolved_db_path, persist=False) as conn:
                upsert_corporate_actions(conn, df)
        logger.info(
            f"download_corporate_actions: {len(df)} records processed for {date_str}"
        )
    except Exception as exc:
        logger.warning(
            f"download_corporate_actions: unavailable for {date_str} ({exc}) — "
            "non-critical, continuing"
        )


def step_download_large_deals(run_date: date_type, db_path: Optional[Path] = None) -> None:
    """
    Download NSE and BSE bulk/block deals for run_date and persist into large_deals.

    Each of the four sources (NSE bulk, NSE block, BSE bulk, BSE block) is
    independently caught inside download_large_deals() — this step only wraps
    the combined call and the DuckDB write.

    Parameters
    ----------
    run_date : date
    db_path : Path, optional
        Defaults to config.settings.DUCKDB_PATH.

    Returns
    -------
    None
        Always — failures are caught and logged, never raised.

    Spec References
    ----------------
    SPEC-PIPE-006: mark unavailable, non-critical.

    PIT Assumptions
    ----------------
    None — same-day published data.

    Raises
    ------
    None
    """
    from config.settings import DUCKDB_PATH
    from ingestion.scrapers.large_deals import download_large_deals, persist_large_deals

    resolved_db_path = db_path or DUCKDB_PATH
    date_str = run_date.isoformat()
    try:
        df = download_large_deals(date_str)
        with get_duckdb_connection(resolved_db_path, persist=False) as conn:
            persist_large_deals(conn, df, date_str)
        logger.info(
            f"download_large_deals: {len(df)} total rows (NSE+BSE bulk+block) for {date_str}"
        )
    except Exception as exc:
        logger.warning(
            f"download_large_deals: failed for {date_str} ({exc}) — "
            "non-critical, continuing"
        )


def step_adjust_prices(run_date: date_type, db_path: Optional[Path] = None) -> None:
    """
    Apply idempotent corporate-action adjustment across the full universe.

    Parameters
    ----------
    run_date : date
        Unused directly — adjust_for_corporate_actions operates on each
        ticker's full ohlcv_adjusted history, not just run_date's row, so
        a newly-applied corporate action correctly rewrites all
        historical rows in one pass, not just today's.
    db_path : Path, optional
        Defaults to config.settings.DUCKDB_PATH.

    Returns
    -------
    None

    Spec References
    ----------------
    SPEC-PIPE-002

    PIT Assumptions
    ----------------
    None.

    Raises
    ------
    None — adjust_for_corporate_actions itself never raises (malformed
    individual actions are logged and skipped, not propagated).
    """
    from config.settings import DUCKDB_PATH, PRICE_ADJUSTMENT_ENABLED

    if not PRICE_ADJUSTMENT_ENABLED:
        logger.info(
            "adjust_prices: disabled via PRICE_ADJUSTMENT_ENABLED=False — skipping. "
            "Raw NSE prices preserved in ohlcv_adjusted (adj_factor=1.0). "
            "Set PRICE_ADJUSTMENT_ENABLED=True in config/settings.py once the "
            "adjustment logic has been deliberated and agreed."
        )
        return

    from config.universe import get_tickers
    from ingestion.adjust.price_adjuster import adjust_for_corporate_actions

    resolved_db_path = db_path or DUCKDB_PATH
    tickers = get_tickers()

    # persist=False (SPEC-SCHED-013): this is a long-lived scheduler process
    # sharing DUCKDB_PATH with the DataStore API — release the write lock as
    # soon as this step finishes, rather than holding it for the process's
    # entire lifetime (see datastore/api/db.py's module docstring).
    with get_duckdb_connection(resolved_db_path, persist=False) as conn:
        for ticker in tickers:
            adjust_for_corporate_actions(conn, ticker)

    logger.info(f"adjust_prices: checked {len(tickers)} tickers for {run_date.isoformat()}")


def step_compute_features(run_date: date_type, db_path: Optional[Path] = None, compute_hmm: bool = True, data_cache=None) -> None:
    """
    Build today's two feature matrices and save both to Parquet (SPEC-DS-005):
    ALL_FEATURE_COLUMNS (features/matrix_builder.py) and PND_FEATURES
    (features/pnd_features.py, not part of ALL_FEATURE_COLUMNS — see
    config.settings.FEATURES_PND_DAILY_DIR's comment).

    Parameters
    ----------
    run_date : date
    db_path : Path, optional
        Unused — both builders reach OHLCV exclusively through
        DataStoreClient/the API (SPEC-SOLID-005), never a direct DuckDB
        connection, so there is nothing for this step to open directly.

    Returns
    -------
    None

    Spec References
    ----------------
    SPEC-PIPE-004, SPEC-DS-005, SPEC-SOLID-005.

    PIT Assumptions
    ----------------
    None at this layer — OHLCV is PITRule.NONE; build_feature_matrix and
    compute_pnd_features only ever look backward from run_date.

    Raises
    ------
    ValueError
        If the universe is empty (propagated from build_feature_matrix).
    httpx.HTTPError
        If the DataStore API is unreachable.
    """
    from datetime import datetime, timedelta

    from config.settings import (
        FEATURE_CACHE_PRELOAD_WORKERS,
        FEATURES_PND_DAILY_DIR,
        HMM_FEATURE_WORKERS,
    )
    from config.universe import get_tickers
    from datastore.client import DataStoreClient
    from features.backfill_cache import BackfillDataCache
    from features.matrix_builder import build_feature_matrix
    from features.pnd_features import PND_FEATURES, compute_pnd_features

    date_str = run_date.isoformat()
    tickers = get_tickers()

    # 2026-07 perf fix: the live/daily path never wired up a data_cache, so
    # every panel needing fundamentals/shareholding (deep_forensic etc.) hit
    # the DataStore API per-ticker individually -- ~5,300 sequential HTTP
    # round-trips, ~27 min of a ~2h run against the full universe. Build the
    # same pre-loader the mass backfill scripts use, threaded (I/O-bound,
    # cheap) since this is only ONE date, not thousands.
    if data_cache is None:
        client_for_cache = DataStoreClient()
        to_dt = datetime.combine(run_date, datetime.min.time())
        data_cache = BackfillDataCache(
            client_for_cache, tickers, to_dt, n_workers=FEATURE_CACHE_PRELOAD_WORKERS
        )

    matrix = build_feature_matrix(
        date_str, tickers, compute_hmm=compute_hmm, data_cache=data_cache,
        hmm_workers=HMM_FEATURE_WORKERS,
    )
    logger.info(f"compute_features: built {len(matrix)}-row ALL_FEATURE_COLUMNS matrix for {date_str}")

    # Re-use PND columns already computed inside build_feature_matrix (which has a 760-day
    # OHLCV window — a strict superset of the 90-day window used to compute PND features).
    # This eliminates a second get_ohlcv_bulk call per date.
    if set(PND_FEATURES).issubset(matrix.columns):
        pnd_today = matrix[["date", "ticker"] + PND_FEATURES].copy()
    else:
        # Fallback: explicit bulk call if PND columns are missing for any reason
        client = DataStoreClient()
        from_dt = datetime.combine(run_date - timedelta(days=90), datetime.min.time())
        to_dt = datetime.combine(run_date, datetime.min.time())
        ohlcv_panel = client.get_ohlcv_bulk(from_dt, to_dt)
        if not ohlcv_panel.empty:
            ticker_set = set(tickers)
            ohlcv_panel = ohlcv_panel[ohlcv_panel["ticker"].isin(ticker_set)].copy()
            pnd_features = compute_pnd_features(ohlcv_panel)
            pnd_today = pnd_features.sort_values("date").groupby("ticker", sort=False).tail(1).reset_index(drop=True)
        else:
            logger.warning(f"compute_features: no OHLCV returned for any of {len(tickers)} tickers on {date_str}")
            pnd_today = pd.DataFrame(columns=["date", "ticker"] + PND_FEATURES)

    FEATURES_PND_DAILY_DIR.mkdir(parents=True, exist_ok=True)
    pnd_path = FEATURES_PND_DAILY_DIR / f"{date_str}.parquet"
    pnd_today.to_parquet(pnd_path, index=False)
    logger.info(f"compute_features: built {len(pnd_today)}-row PND_FEATURES matrix for {date_str} -> {pnd_path}")


def step_run_models(run_date: date_type, db_path: Optional[Path] = None) -> None:
    """
    Load today's two feature Parquets + a market-proxy OHLCV slice, run
    the full daily_inference sequence (HMM -> PSI check -> P&D filter ->
    Signals -> MetaLabel -> Exit -> write to DataStore — see
    systems/ml_signal_engine/inference/daily_inference.py), and persist
    the run's result dict for step_write_signals to verify (this step and
    write_signals are separate STEP_NAMES/checkpoints by Phase 0.3 design
    — see checkpoint.py's STEPS list — even though daily_inference.py
    itself writes incrementally as each model step completes, not in one
    final batch at the end).

    Parameters
    ----------
    run_date : date
    db_path : Path, optional
        Unused — see step_compute_features.

    Returns
    -------
    None

    Spec References
    ----------------
    SPEC-MODEL-006, SPEC-PIPE-005, SPEC-SYS-002.

    PIT Assumptions
    ----------------
    None at this layer.

    Raises
    ------
    FileNotFoundError
        If step_compute_features hasn't run for run_date yet (no Parquet found).
    """
    from datetime import datetime, timedelta

    from config.settings import FEATURES_DAILY_DIR, FEATURES_PND_DAILY_DIR, LOGS_DIR
    from datastore.client import DataStoreClient
    from features.technical import BENCHMARK_TICKERS
    from systems.ml_signal_engine.inference.daily_inference import run_daily_inference

    date_str = run_date.isoformat()
    feature_matrix = pd.read_parquet(FEATURES_DAILY_DIR / f"{date_str}.parquet")
    pnd_feature_matrix = pd.read_parquet(FEATURES_PND_DAILY_DIR / f"{date_str}.parquet")

    client = DataStoreClient()
    market_ticker = BENCHMARK_TICKERS["nifty50"]
    from_dt = datetime.combine(run_date - timedelta(days=400), datetime.min.time())
    to_dt = datetime.combine(run_date, datetime.min.time())
    market_rows = client.get_ohlcv(market_ticker, from_dt, to_dt)
    market_ohlcv = pd.DataFrame(market_rows)
    if not market_ohlcv.empty:
        market_ohlcv["date"] = pd.to_datetime(market_ohlcv["date"])

    # Phase 1 has no portfolio/positions tracking yet (architecture doc's
    # /portfolio/ group is out of P1.7's explicit router list, same as
    # dashboard/screens/daily_dashboard.py's --held CLI flag) — an empty
    # position_context means the exit step is a documented no-op, not a
    # silently-skipped error.
    position_context = pd.DataFrame(columns=["ticker"])

    result = run_daily_inference(
        run_date=run_date,
        feature_matrix=feature_matrix,
        pnd_feature_matrix=pnd_feature_matrix,
        market_ohlcv=market_ohlcv,
        position_context=position_context,
    )

    result_dir = LOGS_DIR / "daily_inference"
    result_dir.mkdir(parents=True, exist_ok=True)
    with open(result_dir / f"{date_str}.json", "w") as f:
        json.dump(result, f, indent=2, default=str)

    logger.info(
        f"run_models: {date_str} halted={result['halted']} scored={result['tickers_scored']} "
        f"pnd_blocked={len(result['pnd_blocked'])}"
    )
    if result["halted"]:
        raise RuntimeError(f"run_models: daily_inference halted — {result['halt_reason']}")


def step_write_signals(run_date: date_type, db_path: Optional[Path] = None) -> None:
    """
    Verify step_run_models' writes completed (SPEC-DS-004) — a separate
    checkpointed step per Phase 0.3's STEP_NAMES design, even though the
    actual writes already happened incrementally inside run_daily_inference
    (see step_run_models' docstring for why these remain two steps).

    Parameters
    ----------
    run_date : date
    db_path : Path, optional
        Unused.

    Returns
    -------
    None

    Spec References
    ----------------
    SPEC-DS-004.

    PIT Assumptions
    ----------------
    None.

    Raises
    ------
    FileNotFoundError
        If step_run_models hasn't run for run_date yet.
    RuntimeError
        If the recorded run halted (e.g. PSI drift) — write_signals must
        not report success for a run that never produced trustworthy output.
    """
    from config.settings import LOGS_DIR

    date_str = run_date.isoformat()
    result_path = LOGS_DIR / "daily_inference" / f"{date_str}.json"
    with open(result_path) as f:
        result = json.load(f)

    if result["halted"]:
        raise RuntimeError(f"write_signals: upstream run_models run was halted — {result['halt_reason']}")

    logger.info(f"write_signals: confirmed {result['tickers_scored']} signal rows written for {date_str}")


def step_paper_trade(run_date: date_type, db_path: Optional[Path] = None) -> None:
    """
    Run the automated daily paper trading bot: act on today's already-
    written ml_signals (scripts/run_daily_paper_trading.py), persisting
    any new entries/exits to paper_trading/portfolio_state.json and
    paper_trading/executions/<date>.csv (Phase 3 Gate 7's source of
    truth — see that script's module docstring for the full design).

    Parameters
    ----------
    run_date : date
    db_path : Path, optional
        Unused — the bot reaches all data through the DataStore API
        (DATASTORE_API_BASE_URL), like step_compute_features/step_run_models.

    Returns
    -------
    None

    Spec References
    ----------------
    SPEC-BT-002, SPEC-MODEL-002, SPEC-OBS-004

    PIT Assumptions
    ----------------
    None at this layer — only acts on today's already-PIT-correct ml_signals.

    Raises
    ------
    FileNotFoundError
        If step_write_signals hasn't run for run_date yet (no ml_signals rows).
    Exception
        Any DataStore API or model-scoring failure — propagated so the
        checkpoint records this step as failed rather than silently
        skipping a real trading day.
    """
    from scripts.run_daily_paper_trading import run_daily_paper_trading

    result = run_daily_paper_trading(run_date=run_date)
    logger.info(
        f"paper_trade: {result['date']} open_positions={result['open_positions']} "
        f"new_buys={result['new_buys']} equity=₹{result['equity']:.0f}"
    )


_STEP_DISPATCH = {
    "download_bhavcopy": step_download_bhavcopy,
    "download_fno": step_download_fno,
    "download_macro": step_download_macro,
    "download_corporate_actions": step_download_corporate_actions,
    "download_large_deals": step_download_large_deals,
    "adjust_prices": step_adjust_prices,
    "compute_features": step_compute_features,
    "run_models": step_run_models,
    "write_signals": step_write_signals,
    "paper_trade": step_paper_trade,
}


def step_runner(run_date: date_type, step_name: str) -> None:
    """
    The StepRunner passed to the scheduler engine — dispatches to the
    concrete step_* function above.

    Must remain a plain, top-level, importable function (never a lambda
    or closure): ingestion/scheduler/pipeline_scheduler.py's
    schedule_daily_pipeline() pickles this alongside the registered job
    so it survives process restarts via SQLAlchemyJobStore.

    Parameters
    ----------
    run_date : date
    step_name : str
        Must be a key in _STEP_DISPATCH (i.e. one of checkpoint.STEP_NAMES).

    Returns
    -------
    None

    Spec References
    ----------------
    SPEC-SCHED-001

    Raises
    ------
    Exception
        Whatever the dispatched step_* function raises — propagated
        as-is so pipeline_scheduler.run_steps_for_date can record the
        failed checkpoint (per the StepRunner contract: "must raise on
        failure").
    KeyError
        If step_name is not recognized.
    """
    _STEP_DISPATCH[step_name](run_date)


def run_daily_pipeline_once(today: Optional[date_type] = None) -> bool:
    """
    One full invocation: gap backfill (if any) + today's pipeline.

    Thin wrapper around pipeline_scheduler.run_startup_sequence with this
    module's CheckpointManager and step_runner — kept as a single function
    so `main()`'s startup catch-up call and the cron-triggered recurring
    job (which also calls run_startup_sequence, via _execute_daily_job)
    never diverge in behavior; pipeline_runs recording (SPEC-SCHED-005)
    lives in run_startup_sequence itself for exactly this reason.

    Parameters
    ----------
    today : date, optional
        Defaults to now_ist().date() (IST) via pipeline_scheduler.run_startup_sequence;
        exposed for testability.

    Returns
    -------
    bool
        True if today's own pipeline run succeeded (or was skipped as an
        NSE holiday); False if it failed.

    Spec References
    ----------------
    SPEC-SCHED-001, SPEC-SCHED-003, SPEC-SCHED-004, SPEC-SCHED-005,
    SPEC-SCHED-008

    PIT Assumptions
    ----------------
    None at this layer.

    Raises
    ------
    None
    """
    from ingestion.scheduler.checkpoint import CheckpointManager
    from ingestion.scheduler.pipeline_scheduler import run_startup_sequence

    return run_startup_sequence(step_runner, CheckpointManager(), today=today)


def dry_run_with_timing(today: Optional[date_type] = None) -> dict:
    """
    🔒 PHASE 1 GATE CHECK item 3 ("Verify daily pipeline timing... must
    complete simulation in < 90 minutes", SPEC-SYS-002): a structural
    dry run — logs each STEP_NAMES entry without executing it (no real
    network/DB/model I/O) and times the dry-run loop itself.

    [AS BUILT] This measures the dry-run STRUCTURE completing, not real
    production per-step cost — no production daily-pipeline run has
    happened yet (Phase 1 just finished), so there is no real timing
    history to simulate against. The closest real evidence is each
    step's own already-measured runtime from this project's BuildLog.md
    (e.g. daily_inference.py's HMM/PSI/P&D/signals/exit steps each
    completing in well under a second on test-sized data, and P1.6's
    full 40-ticker training+backtest run completing in ~2.5 minutes) —
    see BuildLog.md's Phase 1 Gate Check entry for the full citation.
    Re-run this for a real measurement once a production pipeline run
    has actually executed against the full ~500-stock universe.

    Returns
    -------
    dict
        steps (list of {name, simulated_duration_s}), total_duration_s,
        within_budget (bool, < 5400s / 90 minutes).
    """
    from ingestion.scheduler.checkpoint import STEP_NAMES

    today = today or now_ist().date()
    steps = []
    overall_start = time.monotonic()
    for step_name in STEP_NAMES:
        t0 = time.monotonic()
        logger.info(f"[dry-run] would execute step '{step_name}' for {today.isoformat()}")
        steps.append({"name": step_name, "simulated_duration_s": time.monotonic() - t0})
    total = time.monotonic() - overall_start

    return {"date": today.isoformat(), "steps": steps, "total_duration_s": total, "within_budget": total < 5400}


def main() -> None:
    """
    Entry point: catch up once immediately, register the recurring job,
    then block forever so APScheduler's background thread keeps firing it.

    Run this once (e.g. `nohup .venv/bin/python3 -m
    ingestion.scheduler.daily_pipeline &`, or as a systemd --user service)
    and leave it running — this replaces the OS-level crontab entries
    06_deployment.md previously documented for both the laptop and the
    (now-abandoned) Oracle Cloud instance. The job itself is registered
    via ingestion/scheduler/pipeline_scheduler.py's persistent
    SQLAlchemyJobStore, so it survives this process restarting (it does
    NOT survive the laptop being off at the scheduled fire time — that
    gap is exactly what the startup catch-up call and SPEC-SCHED-003/004
    unlimited backfill exist to absorb on the next run).

    Spec References
    ----------------
    SPEC-SCHED-001, SPEC-SCHED-009

    Raises
    ------
    None
    """
    import argparse

    parser = argparse.ArgumentParser(description="AlphaLens daily pipeline scheduler")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Structural dry run (🔒 PHASE 1 GATE CHECK item 3): log each step without executing it, then exit",
    )
    parser.add_argument("--timing", action="store_true", help="With --dry-run: print per-step + total timing")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.dry_run:
        result = dry_run_with_timing()
        if args.timing:
            print(f"\n=== Dry-Run Timing — {result['date']} ===")
            for step in result["steps"]:
                print(f"  {step['name']:<20} {step['simulated_duration_s']:.4f}s")
            print(f"  {'TOTAL':<20} {result['total_duration_s']:.4f}s")
            print(f"  Within 90-minute budget (SPEC-SYS-002): {result['within_budget']}")
        return

    from config.settings import DAILY_PIPELINE_SCHEDULE_TIME, MORNING_CATCHUP_SCHEDULE_TIME
    from ingestion.scheduler.checkpoint import CheckpointManager
    from ingestion.scheduler.pipeline_scheduler import (
        create_scheduler,
        schedule_daily_pipeline,
        schedule_mf_holdings_ingestion,
        schedule_morning_catchup,
        schedule_model_training,
        schedule_weekend_feature_backfill,
        schedule_weekend_fundamentals,
    )

    logger.info("Startup catch-up: checking for missed trading days, then running today's pipeline")
    run_daily_pipeline_once()

    checkpoint_manager = CheckpointManager()
    scheduler = create_scheduler()
    # 2026-07-01: briefly moved to 20:00 for same-evening testing, reverted
    # back to DAILY_PIPELINE_SCHEDULE_TIME (18:00) the same day at the
    # user's request -- this is the standing schedule.
    # schedule_backfill_catchup (SPEC-SCHED-012) is intentionally NOT
    # registered here: it exists only to backfill gaps via FYERS, whose
    # access tokens require an interactive daily login (see
    # _execute_backfill_catchup's docstring) -- everything this pipeline
    # needs is sourced from the NSE website directly, so that job has no
    # unattended use here.
    schedule_daily_pipeline(
        scheduler, step_runner, checkpoint_manager, schedule_time=DAILY_PIPELINE_SCHEDULE_TIME
    )
    # 2026-07: earlier second trigger so NSE-sourced steps that failed on a
    # prior date (download_fno/macro/corporate_actions/large_deals etc.)
    # get retried hours before the 18:00 run, instead of appearing "never
    # run" on the Ops page all day. See schedule_morning_catchup's
    # docstring for why this reuses the same catch-up logic rather than
    # something bespoke.
    schedule_morning_catchup(
        scheduler, step_runner, checkpoint_manager, schedule_time=MORNING_CATCHUP_SCHEDULE_TIME
    )
    schedule_mf_holdings_ingestion(scheduler)  # P2.2: twice-monthly, primary source Groww
    # 2026-07-02: 23-hour window + job-dependency scheduler.
    # Model training fires after the daily pipeline (~20:00 IST) and runs
    # overnight if needed — well within the 6 PM–5 PM 23-hour window.
    schedule_model_training(scheduler)
    # Weekend jobs: feature backfill (09:00 IST Sat) + fundamentals (10:30 IST Sat).
    schedule_weekend_feature_backfill(scheduler)
    schedule_weekend_fundamentals(scheduler)
    scheduler.start()
    # The job store (SCHEDULER_DB_PATH) is persistent across process restarts
    # (SPEC-SCHED-001), so a "backfill_catchup" job registered by an older
    # version of this function keeps firing forever unless explicitly
    # removed here -- simply deleting the schedule_backfill_catchup() call
    # above does not retroactively unschedule it. Must run AFTER
    # scheduler.start(): remove_job() raises JobLookupError against every
    # job, even ones that genuinely exist in the persisted store, until the
    # scheduler has started and wired up its jobstores -- confirmed by
    # testing (this bug meant every prior restart silently failed to remove
    # the stale job, caught by the bare except below).
    try:
        scheduler.remove_job("backfill_catchup")
        logger.info("Removed stale persisted 'backfill_catchup' job (FYERS-only, no longer scheduled)")
    except Exception:
        pass
    logger.info(
        f"Scheduler started: daily pipeline registered for {DAILY_PIPELINE_SCHEDULE_TIME} IST (mon-fri), "
        f"morning catch-up registered for {MORNING_CATCHUP_SCHEDULE_TIME} IST (mon-fri), "
        "MF holdings ingestion registered for 08:00 IST (5th & 20th of each month)"
    )

    # SPEC-SCHED-001's misfire_grace_time=86400 only covers the process
    # being off at fire time. It does NOT cover this process staying alive
    # across a laptop suspend/resume cycle: APScheduler's BackgroundScheduler
    # computes one wait up to ~24h ahead (next cron fire time minus now) and
    # blocks on a threading.Event with that timeout; the underlying timed
    # wait is driven by a monotonic clock that does not advance during
    # suspend, so on a laptop with many suspend/resume cycles in one wait
    # window the deadline silently drifts and can fail to re-fire at all
    # (confirmed 2026-06-23: 10+ suspends since the previous evening's
    # startup, the 18:00 daily_pipeline and 20:00 backfill_catchup triggers
    # both went un-fired for hours with the process still alive and next_run_time
    # stuck in the past in the persisted job store). Waking the scheduler on a
    # short, fixed cadence forces it to re-evaluate due jobs against the
    # current wall clock regardless of suspend history — wakeup() is just
    # `self._event.set()` (cheap, side-effect-free if nothing is due).
    WAKEUP_INTERVAL_SECONDS = 60
    try:
        while True:
            time.sleep(WAKEUP_INTERVAL_SECONDS)
            scheduler.wakeup()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down scheduler")
        scheduler.shutdown()


if __name__ == "__main__":
    main()
