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
  universe ticker — enabled via PRICE_ADJUSTMENT_ENABLED=True in
  config/settings.py. The corporate_actions table is populated daily so
  the ledger stays current for this step.
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

_UPSERT_INDEX_OHLCV = """
    INSERT INTO index_ohlcv (date, index_name, open, high, low, close, volume)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (date, index_name) DO UPDATE SET
        open   = excluded.open,
        high   = excluded.high,
        low    = excluded.low,
        close  = excluded.close,
        volume = excluded.volume
"""

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

    2026-07-29 (user decision): promoted from non-critical to critical.
    Through 2026-07 a scrape failure here was caught and swallowed so the
    checkpoint was always marked 'success' even when nothing was written —
    which is exactly why 6 trading days (07-02, 07-07, 07-08, 07-22, 07-23,
    07-28) went missing for weeks without anyone noticing or anything
    retrying them: `download_fno` is `is_backfillable: True` in
    checkpoint.py's STEPS list, but a checkpoint already marked 'success'
    is never retried. Now the scrape failure propagates so the checkpoint
    is honestly marked 'failed', which the existing gap-backfill mechanism
    (run_backfill/run_morning_catchup_sequence) will automatically retry on
    a later run until NSE actually serves that date's bhavcopy. The DB
    write is still wrapped separately below so a lock-conflict on write
    doesn't get confused with a genuine scrape/data outage.

    `publish_and_snapshot` depends_on ["download_fno", "adjust_prices"]
    (checkpoint.py), so on a day this fails, only that day's snapshot step
    is skipped (not aborted) until a later backfill run succeeds — it does
    not block download_macro/adjust_prices/compute_features, which have no
    dependency on this step.

    2026-07-30 (A56 follow-up, user-reported): promoting this to critical
    (above) meant every live 18:00 attempt started failing routinely, not
    exceptionally — NSE simply hasn't published that day's F&O bhavcopy
    yet at 18:00 most days, so this was raising (and being logged as a
    'failed' checkpoint) for a timing reason, not a real outage. Rather
    than reverting to critical=False (which reintroduces the exact A34
    silent-miss bug this step was promoted to fix) or leaving the noisy
    same-day failure in place, a live (run_date == today) attempt before
    config.settings.FNO_MIN_ATTEMPT_TIME now defers cleanly instead of
    attempting the scrape at all — no raise, no rows written, nothing
    misleading recorded as either success or failure at this point in the
    day. The real attempt happens via
    ingestion.scheduler.pipeline_scheduler.schedule_fno_late_catchup, a
    dedicated job at FNO_LATE_CATCHUP_SCHEDULE_TIME (21:00 IST default) —
    by then NSE has routinely published the day's bhavcopy, so a failure
    there is a genuine one worth surfacing critically. A backfill/catch-up
    call for a PAST date (run_date != today) is never deferred by this
    check — only "attempting today, too early" is a no-op; everything
    else behaves exactly as before.

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
    SPEC-PIPE-001

    PIT Assumptions
    ----------------
    None — same-day archive data.

    Raises
    ------
    Exception
        Propagated from fno.download_fno_bhavcopy() or the DB write on
        failure — this step is now critical (see docstring above) and the
        checkpoint is marked 'failed' so it gets retried on backfill.
        Never raises for the "too early today" deferral case (see above).
    """
    from config.settings import DUCKDB_PATH, FNO_MIN_ATTEMPT_TIME
    from config.timezone import now_ist
    from ingestion.scrapers import fno

    resolved_db_path = db_path or DUCKDB_PATH
    date_str = run_date.isoformat()

    now = now_ist()
    if run_date == now.date():
        cutoff_hour, cutoff_minute = (int(p) for p in FNO_MIN_ATTEMPT_TIME.split(":"))
        if (now.hour, now.minute) < (cutoff_hour, cutoff_minute):
            logger.info(
                f"download_fno: {date_str} is today and before {FNO_MIN_ATTEMPT_TIME} IST — "
                "NSE's F&O bhavcopy is routinely not yet published this early; deferring to "
                "schedule_fno_late_catchup instead of attempting (and routinely failing) now."
            )
            return

    df = fno.download_fno_bhavcopy(date_str)

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


def step_download_index_ohlcv(run_date: date_type, db_path: Optional[Path] = None) -> None:
    """
    Download NSE's indices-close archive for run_date and upsert Nifty
    50/500 + tracked sector indices into index_ohlcv. Non-critical: feeds
    the sector-rotation report and the backtest benchmark curve, neither
    of which is on the critical path for signal generation.

    Parameters
    ----------
    run_date : date
    db_path : Path, optional
        Defaults to config.settings.DUCKDB_PATH.

    Returns
    -------
    None
        Always — failures are caught and logged (SPEC-PIPE-006 "mark
        unavailable, non-critical" pattern), same as step_download_fno.

    Raises
    ------
    None
    """
    from config.settings import DUCKDB_PATH
    from ingestion.scrapers import nse_indices

    resolved_db_path = db_path or DUCKDB_PATH
    date_str = run_date.isoformat()
    try:
        df = nse_indices.download_index_ohlcv(date_str)

        rows = [
            (
                date_str,
                row.index_name,
                None if pd.isna(row.open) else float(row.open),
                None if pd.isna(row.high) else float(row.high),
                None if pd.isna(row.low) else float(row.low),
                None if pd.isna(row.close) else float(row.close),
                None if pd.isna(row.volume) else int(row.volume),
            )
            for row in df.itertuples()
        ]

        # persist=False per SPEC-SCHED-013, same as step_download_bhavcopy.
        with get_duckdb_connection(resolved_db_path, persist=False) as conn:
            conn.executemany(_UPSERT_INDEX_OHLCV, rows)
    except Exception as exc:
        # 2026-07-09 (A31): the DB write (schema-missing Catalog Error,
        # cross-process DuckDB lock conflict) previously escaped this
        # try/except entirely, failing the whole step even though this
        # step is documented as always-non-critical — only the scraper
        # fetch was guarded. Widened to cover the write too, matching
        # step_download_fno/step_download_macro's "mark unavailable,
        # never raise" contract for this step.
        logger.warning(
            f"download_index_ohlcv: unavailable for {date_str} ({exc}) — "
            "non-critical, continuing"
        )
        return

    logger.info(f"download_index_ohlcv: {len(rows)} indices written for {date_str}")


def step_download_macro(run_date: date_type, db_path: Optional[Path] = None) -> None:
    """
    18:00 STEPS entry — as of 2026-07, a no-op placeholder.

    Through 2026-07 this fetched India VIX, FII/DII net flows, and USD/INR
    for run_date. Those three were moved to step_download_macro_morning,
    fired at 07:30 IST instead (backlog #1/#2/#3 "Morning Catch-Up
    redesign", Sub-task C) — a deliberate PIT shift: capturing them
    pre-market on trading day D means the value stored under date=D is D's
    own pre-open snapshot (in practice, D-1's last published close, since
    VIX/FII-DII/USD-INR for D itself doesn't exist until D's own market
    close) rather than the previous same-day-close capture, which was
    actually a same-day lookahead (fetching D's own closing VIX and
    storing it as D's own feature, before D's trades had even happened).

    Kept as a no-op (rather than removed from checkpoint.py's STEPS list)
    since STEPS/pipeline_runs/tests reference "download_macro" by name in
    many places already — removing the STEPS entry entirely is a larger,
    unrelated migration. Always succeeds.

    Parameters
    ----------
    run_date : date
    db_path : Path, optional
        Unused (kept for step_runner signature compatibility).

    Returns
    -------
    None

    Spec References
    ----------------
    SPEC-PIPE-006

    PIT Assumptions
    ----------------
    None — see step_download_macro_morning, which now owns this concern.

    Raises
    ------
    None
    """
    logger.info(
        f"download_macro: no-op for {run_date.isoformat()} — VIX/FII-DII/USD-INR/global "
        "indices now captured by step_download_macro_morning at 07:30 IST (see docstring)"
    )


def step_download_macro_morning(run_date: date_type, db_path: Optional[Path] = None) -> None:
    """
    07:30 IST morning-catchup macro capture: India VIX, FII/DII net flows,
    USD/INR, and global index snapshots (Nasdaq Composite, Dow Jones,
    S&P 500, Nikkei 225, Hang Seng).

    Each indicator is independently caught so one source's outage never
    blocks the others (SPEC-PIPE-006: "mark unavailable if scrape fails,
    non-critical") — same pattern step_download_macro used through 2026-07.

    Called once per calendar day for `run_date` = "today" from
    ingestion.scheduler.pipeline_scheduler._execute_morning_catchup_job —
    NOT part of checkpoint.py's STEPS list, so it is never retried against
    older gap-backfill dates the way the STEPS-listed steps are; it only
    ever runs for the current trading day, each morning.

    Parameters
    ----------
    run_date : date
        The "today" the morning-catchup job is firing for.
    db_path : Path, optional
        Defaults to config.settings.DUCKDB_PATH for the upsert, and is
        also forwarded to each macro.download_* call for its own
        previous-value fallback lookup.

    Returns
    -------
    None

    Spec References
    ----------------
    SPEC-PIPE-006. 2026-07 backlog #1/#2/#3 (Morning Catch-Up redesign),
    Sub-tasks B and C.

    PIT Assumptions
    ----------------
    Deliberately captured BEFORE run_date's own NSE/global market
    sessions open (07:30 IST). Each macro.download_* call attempts a
    live fetch for `run_date` itself; since that day's own close doesn't
    exist yet at 07:30, NSE/Yahoo return no data for "today" and each
    function's existing retry-then-fallback-to-previous-value path
    (SPEC-PIPE-006) kicks in, yielding the most recent prior close — which
    is then stored under macro_indicators.date = run_date. This is
    intentional (see step_download_macro's docstring): it associates each
    trading day with the macro snapshot that was actually knowable at
    that day's own open, with zero same-day lookahead.

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
        logger.warning(f"download_macro_morning: VIX unavailable for {date_str}: {exc}")

    try:
        fiidii = macro.download_fiidii(date_str, db_path=resolved_db_path)
        if fiidii.get("fii_net_cr") is not None:
            indicators["FII_NET_CR"] = fiidii["fii_net_cr"]
        if fiidii.get("dii_net_cr") is not None:
            indicators["DII_NET_CR"] = fiidii["dii_net_cr"]
    except ConnectionError as exc:
        logger.warning(f"download_macro_morning: FII/DII unavailable for {date_str}: {exc}")

    try:
        fx = macro.download_fx(date_str, db_path=resolved_db_path)
        indicators["USD_INR"] = fx["usd_inr"]
    except ConnectionError as exc:
        logger.warning(f"download_macro_morning: USD/INR unavailable for {date_str}: {exc}")

    for name, fetch_fn, key in (
        ("NASDAQ_COMPOSITE", macro.download_nasdaq, "nasdaq_composite"),
        ("DOW_JONES", macro.download_dow, "dow_jones"),
        ("SP500", macro.download_sp500, "sp500"),
        ("NIKKEI_225", macro.download_nikkei, "nikkei_225"),
        ("HANG_SENG", macro.download_hangseng, "hang_seng"),
        ("DXY", macro.download_dxy, "dxy"),
        # 2026-07-28: download_crude_oil/download_gold existed (Yahoo-sourced,
        # real data, working previous-value fallback) but were never actually
        # called anywhere in the pipeline — same class of gap as the
        # 2026-07-07 bond-yields fix above. features/*'s crude_oil_price/
        # gold_price were 100% NaN for every ticker/date purely from this
        # missing wire-up.
        ("CRUDE_OIL", macro.download_crude_oil, "crude_oil_price"),
        ("GOLD", macro.download_gold, "gold_price"),
    ):
        try:
            result = fetch_fn(date_str, db_path=resolved_db_path)
            indicators[name] = result[key]
        except ConnectionError as exc:
            logger.warning(f"download_macro_morning: {name} unavailable for {date_str}: {exc}")

    # 2026-07-07: download_bond_yields existed (FRED-sourced, real data,
    # working PIT-safe forward-fill) but was never actually called anywhere
    # in the pipeline — features/macro_features.py's yield_10yr/
    # yield_spread_10yr_2yr were 100% NaN for every ticker/date as a
    # result, purely from this missing wire-up, not a data-availability
    # problem. FRED India yield series are monthly, not daily, so this is
    # cheap to call every morning alongside the other macro indicators.
    try:
        yields = macro.download_bond_yields(date_str, db_path=resolved_db_path)
        indicators["YIELD_10YR"] = yields["yield_10yr"]
        indicators["YIELD_3M"] = yields["yield_3m"]
    except ConnectionError as exc:
        logger.warning(f"download_macro_morning: bond yields unavailable for {date_str}: {exc}")

    # 2026-07-07 (follow-up): cement_dispatches_growth/power_consumption_growth
    # now have a real free source (DPIIT ICI workbook, monthly) — see
    # ingestion/scrapers/macro_real_economy.py's module docstring. Written to
    # its own macro_real_economy.parquet (long-format, PIT availability_date
    # per row), NOT the macro_indicators DuckDB table the rest of this
    # function writes to, since features/real_economy_macro.py's
    # load_real_economy_macro() already reads that specific Parquet schema —
    # matching it avoids a second, redundant storage format. Idempotent: the
    # source only updates monthly, upsert dedupes on (feature_name,
    # reference_month_end), so calling this daily is cheap and safe.
    try:
        from ingestion.scrapers.macro_real_economy import upsert_macro_real_economy_parquet

        n_written = upsert_macro_real_economy_parquet(date_str)
        logger.info(f"download_macro_morning: {n_written} real-economy macro row(s) updated for {date_str}")
    except ConnectionError as exc:
        logger.warning(f"download_macro_morning: real-economy macro (cement/power) unavailable for {date_str}: {exc}")

    # 2026-07-07: real NSE Corporate Announcements feed (material-event
    # categories only — see ingestion/scrapers/nse_corporate_announcements.py's
    # module docstring). Fetches a 2-day window (run_date-1 .. run_date) each
    # morning, not just run_date, because filings often land late evening
    # (verified: real filings at 22:07 IST the same day) — the morning-catchup
    # job runs at 07:30 IST, so yesterday's post-close filings need this
    # overlap to be captured; the upsert (keyed on NSE's own seq_id) makes
    # re-fetching the same window on consecutive days a safe no-op for
    # already-seen rows.
    try:
        from datetime import timedelta as _timedelta

        from ingestion.scrapers.nse_corporate_announcements import (
            download_corporate_announcements,
            upsert_corporate_announcements,
        )

        window_start = (run_date - _timedelta(days=1)).isoformat()
        ann_df = download_corporate_announcements(window_start, date_str)
        if not ann_df.empty:
            with get_duckdb_connection(resolved_db_path, persist=False) as conn:
                n_ann = upsert_corporate_announcements(conn, ann_df)
            logger.info(f"download_macro_morning: {n_ann} corporate announcement(s) upserted for {window_start}..{date_str}")
        else:
            logger.info(f"download_macro_morning: 0 material corporate announcements for {window_start}..{date_str}")
    except ConnectionError as exc:
        logger.warning(f"download_macro_morning: corporate announcements unavailable for {date_str}: {exc}")

    if indicators:
        rows = [(date_str, name, value) for name, value in indicators.items()]
        # persist=False (SPEC-SCHED-013): this is a long-lived scheduler
        # process sharing DUCKDB_PATH with the DataStore API — release the
        # write lock as soon as this step finishes, rather than holding it
        # for the process's entire lifetime (see datastore/api/db.py's
        # module docstring).
        with get_duckdb_connection(resolved_db_path, persist=False) as conn:
            conn.executemany(_UPSERT_MACRO_INDICATOR, rows)

    logger.info(f"download_macro_morning: {len(indicators)}/10 indicators written for {date_str}")


def step_download_corporate_actions(run_date: date_type, db_path: Optional[Path] = None) -> None:
    """
    Download NSE corporate actions for run_date and upsert into corporate_actions.

    2026-07-30 (user decision): promoted from non-critical to critical,
    mirroring step_download_fno's 2026-07-29 fix. A scrape failure here
    used to be caught and swallowed so the checkpoint was always marked
    'success' even when nothing was written — and since
    download_corporate_actions is is_backfillable: True in checkpoint.py's
    STEPS list, a checkpoint already marked 'success' is never retried, so
    a missed day's corporate actions (SPLIT/BONUS ratios needed by
    adjust_prices, and by data_integrity_check's own corporate-action
    cross-check) would silently never be filled in. Now the scrape failure
    propagates so the checkpoint is honestly marked 'failed', which the
    existing gap-backfill mechanism (run_backfill/run_morning_catchup_sequence)
    will automatically retry on a later run until NSE actually serves that
    date's corporate actions. data_integrity_check now also depends_on this
    step (checkpoint.py STEPS) so it never evaluates a day's data against a
    corporate_actions table that's missing that same day's actions.

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
    SPEC-PIPE-002: corporate actions ledger.

    PIT Assumptions
    ----------------
    None — ex_date from NSE is same-day published data.

    Raises
    ------
    Exception
        Propagated from corporate_actions.download_corporate_actions() or
        the DB write on failure — this step is now critical (see docstring
        above) and the checkpoint is marked 'failed' so it gets retried on
        backfill.
    """
    from config.settings import DUCKDB_PATH
    from ingestion.scrapers.corporate_actions import (
        download_corporate_actions,
        upsert_corporate_actions,
    )

    resolved_db_path = db_path or DUCKDB_PATH
    date_str = run_date.isoformat()

    df = download_corporate_actions(date_str)
    if not df.empty:
        with get_duckdb_connection(resolved_db_path, persist=False) as conn:
            upsert_corporate_actions(conn, df)
    logger.info(
        f"download_corporate_actions: {len(df)} records processed for {date_str}"
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


def step_attribute_bulk_deals(run_date: date_type, db_path: Optional[Path] = None) -> None:
    """
    Net same-day wash trades and attribute large_deals rows to investor_family
    for run_date, writing bulk_deal_positions.

    Deterministic post-processing of that day's own large_deals rows — no
    model inference, so safe to backfill (unlike run_models/write_signals).

    Parameters
    ----------
    run_date : date
    db_path : Path, optional
        Defaults to config.settings.DUCKDB_PATH.

    Returns
    -------
    None
        Always — failures are caught and logged, never raised (same
        non-critical philosophy as step_download_large_deals).

    Raises
    ------
    None
    """
    from config.settings import DUCKDB_PATH
    from ingestion.scrapers.bulk_deal_attribution import attribute_bulk_deals

    resolved_db_path = db_path or DUCKDB_PATH
    date_str = run_date.isoformat()
    try:
        with get_duckdb_connection(resolved_db_path, persist=False) as conn:
            written = attribute_bulk_deals(conn, run_date)
        logger.info(f"attribute_bulk_deals: {written} family/ticker/deal_type positions for {date_str}")
    except Exception as exc:
        logger.warning(f"attribute_bulk_deals: failed for {date_str} ({exc}) — non-critical, continuing")


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

    # Just-in-time DB lock hold (2026-07-10 lock-hold-time remediation): the
    # write connection used to wrap this whole loop over the *entire*
    # universe (thousands of tickers), holding DuckDB's single-writer lock
    # for the full scan even though adjust_for_corporate_actions() itself
    # early-returns a no-op for any ticker with zero corporate_actions rows
    # — which is nearly all of them on a typical day. Pre-filter with a
    # cheap read-only query first (no lock needed — read_only=True), so the
    # write connection below is only opened for, and only held for the
    # duration of, the small subset of tickers that actually have work to
    # do. Falls back to the full universe if the read-only probe itself
    # fails, so a transient read error degrades to the old (safe, just
    # slower) behavior rather than silently skipping tickers.
    try:
        with get_duckdb_connection(resolved_db_path, persist=False, read_only=True) as ro_conn:
            actionable = {
                row[0] for row in ro_conn.execute(
                    "SELECT DISTINCT ticker FROM corporate_actions"
                ).fetchall()
            }
        tickers_to_adjust = [t for t in tickers if t in actionable]
    except Exception as exc:
        logger.warning(
            f"adjust_prices: could not pre-filter tickers with corporate actions ({exc}) "
            "— falling back to checking the full universe"
        )
        tickers_to_adjust = tickers

    # persist=False (SPEC-SCHED-013): this is a long-lived scheduler process
    # sharing DUCKDB_PATH with the DataStore API — release the write lock as
    # soon as this step finishes, rather than holding it for the process's
    # entire lifetime (see datastore/api/db.py's module docstring). Scoped
    # to only the actionable tickers (see above) so the lock is held for
    # just the work that actually needs it, not a full-universe scan.
    if tickers_to_adjust:
        with get_duckdb_connection(resolved_db_path, persist=False) as conn:
            for ticker in tickers_to_adjust:
                adjust_for_corporate_actions(conn, ticker)

    logger.info(
        f"adjust_prices: {len(tickers_to_adjust)}/{len(tickers)} tickers had corporate "
        f"actions to check for {run_date.isoformat()}"
    )


def step_compute_features(
    run_date: date_type,
    db_path: Optional[Path] = None,
    compute_hmm: bool = True,
    data_cache=None,
    panel_workers: Optional[int] = None,
    staged_panel=None,
    skip_slow_categories: bool = False,
) -> None:
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
    panel_workers : int, optional
        Forwarded to build_feature_matrix's panel_workers (default: None,
        which falls back to config.settings.PANEL_COMPUTE_WORKERS). See
        features/matrix_builder.py::_compute_chunked_ticker_independent_panels
        for the parallelization pattern this controls.
    staged_panel : pd.DataFrame, optional
        [2026-07-29] Forwarded to build_feature_matrix's `staged_panel`
        (default: None, unchanged live-daily-path behavior). Only
        scripts/feature_backfill.py passes this — see
        features/panel_staging.py for what it is and why.
    skip_slow_categories : bool
        [2026-07-31] Forwarded to build_feature_matrix's
        `skip_slow_categories` (default: False, unchanged behavior). Only
        scripts/feature_backfill.py's --skip-slow-categories flag sets
        this True — see that flag's help text / build_feature_matrix's
        docstring for what it skips and why.

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
        PANEL_COMPUTE_WORKERS,
    )

    if panel_workers is None:
        panel_workers = PANEL_COMPUTE_WORKERS
    from config.universe import get_tickers
    from datastore.client import DataStoreClient
    from features.backfill_cache import BackfillDataCache
    from features.matrix_builder import build_feature_matrix
    from features.pnd_features import PND_FEATURES, compute_pnd_features

    # 2026-07-10 (A52): this step's fundamentals/forensic panels depend on the
    # DataStore API for every ticker. A transient outage here used to be
    # swallowed by the panel builders' per-ticker except-blocks and silently
    # written as permanent NaN (see features/fundamental.py's A52 fix) — block
    # on API health first so an outage fails this step loudly instead.
    _wait_for_datastore_api()

    date_str = run_date.isoformat()
    tickers = get_tickers()

    # 2026-07 perf fix: the live/daily path never wired up a data_cache, so
    # every panel needing fundamentals/shareholding (deep_forensic etc.) hit
    # the DataStore API per-ticker individually -- ~5,300 sequential HTTP
    # round-trips, ~27 min of a ~2h run against the full universe. Build the
    # same pre-loader the mass backfill scripts use, threaded (I/O-bound,
    # cheap) since this is only ONE date, not thousands.
    if data_cache is None and not skip_slow_categories:
        # BackfillDataCache pre-loads fundamentals/shareholding/corp_actions
        # for the categories skip_slow_categories disables entirely below —
        # building it here would be pure wasted I/O when they're skipped.
        client_for_cache = DataStoreClient()
        to_dt = datetime.combine(run_date, datetime.min.time())
        data_cache = BackfillDataCache(
            client_for_cache, tickers, to_dt, n_workers=FEATURE_CACHE_PRELOAD_WORKERS
        )

    matrix = build_feature_matrix(
        date_str, tickers, compute_hmm=compute_hmm, data_cache=data_cache,
        hmm_workers=HMM_FEATURE_WORKERS, panel_workers=panel_workers,
        staged_panel=staged_panel, skip_slow_categories=skip_slow_categories,
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


# AF-2 (#9): regimes in which the daily HMM step (_step_hmm in
# systems/ml_signal_engine/inference/daily_inference.py, REGIME_RANK_NAMES)
# genuinely justify an empty top_buys list — a bearish or high-volatility
# regime legitimately producing zero buy-quality signals is a distinct,
# expected state, not the same thing as run_models/write_signals silently
# breaking (the actual 2026-06-23..2026-07-02 incident this gate exists
# for). "sideways"/"bullish" regimes with zero buys are NOT given this
# pass — those are exactly the implausible case that went unnoticed for
# 10 trading days.
_NO_BUY_REGIMES = {"bearish", "volatile"}

# 80% of MIN_STOCKS_FOR_INFERENCE (config/settings.py, SPEC-SYS-003) — a
# hard floor on how many tickers' signal_5d rows must exist for run_date
# before the day's output is considered plausible at all. MIN_STOCKS_FOR_
# INFERENCE itself is the gate daily_inference.py's own P&D/HMM steps use
# to decide whether to run at all; this reuses the same number rather than
# inventing a second, potentially-inconsistent threshold.
_SANITY_MIN_SIGNAL_ROWS_FRACTION = 0.8

# Features whose upstream data sources (MF holdings disclosures, corporate
# governance filings, macro releases, XBRL-derived working-capital ratios,
# etc.) have proven impossible to source reliably on a daily cadence — see
# 2026-07-08 sanity_check failure. Check 3 below would otherwise fail every
# run over these, permanently blocking write_signals-dependent output
# (paper_trade) on data availability we do not control. Exempted from the
# all-NaN floor; still computed and stored as usual when a source is
# available, just not used as a pipeline-failure signal when it isn't.
# A54 (2026-07-10): removed intangibles_growth, capex_to_assets,
# noncash_assets_ratio — these read real, populated NSE XBRL columns
# (intangibles_growth was a lookup-key bug, now fixed in deep_forensic.py;
# the other two were always correctly wired) and should be measured the
# same as any other partial-coverage NSE XBRL field, not silenced.
# audit_qualification_flag/goodwill_ratio were never added here (also
# correctly wired to real data) — left out on purpose, not an oversight.
# Added benford_mad after fixing forensic_classical.py's panel-level
# blanket except-to-NaN bug (same class as A52) — its remaining nulls are
# legitimate new-listing warmup (needs >=5 quarterly revenue values).
_SANITY_KNOWN_SPARSE_COLUMNS = {
    "inventory_days", "receivable_days", "payable_days", "cash_conversion_cycle",
    "mf_pct", "mf_change_qoq", "mf_total_holding_change_1m", "mf_sip_inflow_proxy",
    "days_to_record_date", "buyback_price_spread", "buyback_acceptance_estimated",
    "index_inclusion_days", "dividend_yield_vs_fd_rate", "qip_dilution_impact",
    "iv_compression_flag", "gst_collection_growth", "pmi_manufacturing",
    "pmi_services", "iip_growth", "auto_monthly_sales_growth",
    "rail_freight_growth", "upi_transaction_growth", "bank_credit_growth",
    "contingent_liability_ratio", "subsidiary_count", "loans_to_related",
    "off_balance_sheet_proxy", "salary_to_pat",
    "rpt_intensity", "auditor_change_flag", "cfo_tenure_months",
    "board_independence", "director_resignation_count_4q",
    "whistle_blower_policy", "gst_revenue_divergence", "peer_outlier_score",
    "tax_rate_anomaly", "benford_mad",
    # [2026-08-01] receivable_days/inventory_days (base columns) were already
    # exempted above, but these derived _change columns were missed when
    # added — same root cause (ingestion/scrapers/screener.py hardcodes the
    # base fields to None at scrape time, so any delta computed from them is
    # structurally NaN too). dilution_3y shares the same "source doesn't
    # reliably provide this" character. See DataModelAudit.md priority #1.
    "receivable_days_change", "inventory_days_change", "dilution_3y",
}


def step_sanity_check(run_date: date_type, db_path: Optional[Path] = None) -> None:
    """
    AF-2 (#9): hard floors on run_date's own already-written output,
    checked immediately after step_write_signals.

    run_models silently produced no real signals for 10 consecutive
    trading days (2026-06-23 to 2026-07-02) before a user noticed by
    coincidence — checkpoint.py only ever tracked "did each step raise",
    never "was the output actually plausible". This step re-reads that
    day's own ml_signals rows, top_buys, and feature Parquet and raises if
    any hard floor is violated, so the checkpoint records the day as
    failed (not silently "success") and it surfaces loudly on the Ops
    page instead of only being caught by chance.

    Checks (all against run_date's own output only):
    1. signal_5d row count in ml_signals >= 80% of MIN_STOCKS_FOR_INFERENCE.
    2. top_buys is non-empty, OR the day's decoded market regime is a
       recognized "legitimately no buys" state (_NO_BUY_REGIMES) — a
       distinct, expected state, not silent emptiness.
    3. run_date's ALL_FEATURE_COLUMNS Parquet has no column that is 100%
       NaN across every row (a whole feature silently failing to compute
       is exactly the kind of thing that let the real incident go
       unnoticed), excluding _SANITY_KNOWN_SPARSE_COLUMNS — features whose
       sources are known to be unreliable/unsourceable day-to-day and
       whose emptiness is therefore not a signal of breakage.

    Parameters
    ----------
    run_date : date
    db_path : Path, optional
        Unused — signals are read via SIGNALS_DUCKDB_PATH, features via
        FEATURES_DAILY_DIR, both from config.settings like the steps
        above.

    Returns
    -------
    None

    Spec References
    ----------------
    SPEC-SYS-003, SPEC-MODEL-006.

    Raises
    ------
    RuntimeError
        If any hard floor is violated — logged at CRITICAL first (the
        loudest logging level used anywhere in this codebase, reserved
        for exactly this "impossible to miss" case) so it stands out from
        the routine WARNING-level "source unavailable, continuing" noise
        every other step emits, then propagated so
        pipeline_scheduler.run_steps_for_date records this checkpoint (and
        therefore this day) as failed.
    """
    from config.settings import FEATURES_DAILY_DIR, MIN_STOCKS_FOR_INFERENCE, SIGNALS_DUCKDB_PATH

    date_str = run_date.isoformat()
    problems = []

    # --- Check 1: ml_signals row count floor ---------------------------
    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False, read_only=True) as conn:
        signal_row_count = conn.execute(
            "SELECT COUNT(*) FROM ml_signals WHERE date = ? AND model_name = 'signal_5d' "
            "AND buy_prob IS NOT NULL",
            [date_str],
        ).fetchone()[0]

        pnd_blocked_tickers = {
            r[0] for r in conn.execute(
                "SELECT ticker FROM ml_signals WHERE date = ? AND model_name = 'pnd_detector' "
                "AND pnd_block = TRUE",
                [date_str],
            ).fetchall()
        }
        if pnd_blocked_tickers:
            top_buy_count = conn.execute(
                f"""
                SELECT COUNT(*) FROM ml_signals
                WHERE date = ? AND model_name = 'signal_5d' AND buy_prob IS NOT NULL
                  AND ticker NOT IN ({",".join("?" for _ in pnd_blocked_tickers)})
                """,
                [date_str, *pnd_blocked_tickers],
            ).fetchone()[0]
        else:
            top_buy_count = signal_row_count

        regime_row = conn.execute(
            "SELECT hmm_regime FROM ml_signals WHERE date = ? AND ticker = 'MARKET' "
            "AND model_name = 'hmm_market' ORDER BY date DESC LIMIT 1",
            [date_str],
        ).fetchone()
        regime = regime_row[0] if regime_row else None

    min_required = int(MIN_STOCKS_FOR_INFERENCE * _SANITY_MIN_SIGNAL_ROWS_FRACTION)
    if signal_row_count < min_required:
        problems.append(
            f"ml_signals has only {signal_row_count} signal_5d rows for {date_str} "
            f"(floor: {min_required}, {int(_SANITY_MIN_SIGNAL_ROWS_FRACTION * 100)}% of "
            f"MIN_STOCKS_FOR_INFERENCE={MIN_STOCKS_FOR_INFERENCE})"
        )

    # --- Check 2: top_buys non-empty, or an explicit no-buy regime ------
    if top_buy_count == 0:
        if regime in _NO_BUY_REGIMES:
            logger.info(
                f"sanity_check: top_buys is empty for {date_str}, but regime='{regime}' "
                "is a recognized no-buy state — not a failure"
            )
        else:
            problems.append(
                f"top_buys is empty for {date_str} and regime='{regime}' is not a "
                f"recognized no-buy state ({sorted(_NO_BUY_REGIMES)}) — looks like a "
                "silent breakage, not a legitimate market condition"
            )

    # --- Check 3: no all-NaN feature columns ----------------------------
    feature_path = FEATURES_DAILY_DIR / f"{date_str}.parquet"
    if not feature_path.exists():
        problems.append(f"no feature Parquet found for {date_str} at {feature_path}")
    else:
        feature_df = pd.read_parquet(feature_path)
        if feature_df.empty:
            problems.append(f"feature Parquet for {date_str} has zero rows ({feature_path})")
        else:
            all_nan_cols = [
                c for c in feature_df.columns
                if feature_df[c].isna().all() and c not in _SANITY_KNOWN_SPARSE_COLUMNS
            ]
            if all_nan_cols:
                problems.append(
                    f"feature Parquet for {date_str} has {len(all_nan_cols)} all-NaN "
                    f"column(s): {all_nan_cols}"
                )

    if problems:
        message = f"sanity_check FAILED for {date_str}: " + "; ".join(problems)
        logger.critical(message)
        raise RuntimeError(message)

    logger.info(
        f"sanity_check: passed for {date_str} "
        f"(signal_rows={signal_row_count}, top_buys={top_buy_count}, regime={regime})"
    )


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


def step_check_ta_alerts(run_date: date_type, db_path: Optional[Path] = None) -> None:
    """
    Evaluate all 42 TA screener templates against today's feature Parquet
    (systems/technical_analysis/alerts/daily_alert_checker.py's
    evaluate(), compute-only), then persist the results (ta_signals) and
    check user-defined alerts (ta_alert_triggers) against them.

    [BUG FIX, 2026-07-02 #1] Originally called DailyAlertChecker.run() and
    alert_store.check_alerts() directly, each opening its own connection
    to SIGNALS_DUCKDB_PATH from this (scheduler) process — but the
    DataStore API process already holds a long-lived cached connection to
    that same file for its whole run (several routers default to
    persist=True), so this process lost the race for DuckDB's single-
    writer-per-file lock when run from a genuinely separate scheduler
    process (observed as a live "check_ta_alerts" Ops Monitor failure —
    IO Error: Could not set lock ... Conflicting lock is held). First fix
    routed both calls through the API via HTTP.

    [BUG FIX, 2026-07-02 #2] That HTTP-only approach broke the Ops
    Monitor's "force-run" button, which runs step_runner *inside* the API
    process itself (datastore/api/routers/ops.py's force_run_step, via
    asyncio.to_thread) — two sequential self-referential HTTP round-trips
    from a thread spawned by the API's own request handler back into
    itself reliably hung on the second call (observed: signals/write
    succeeds, check-triggers never completes). Fixed by trying the direct
    DB write first: when this function runs inside the API process (the
    force-run case), get_duckdb_connection(SIGNALS_DUCKDB_PATH) returns
    the SAME cached connection object already held by that process (see
    datastore/api/db.py's path+read_only-keyed cache) — no new OS-level
    file lock is requested at all, so it succeeds instantly with zero
    self-HTTP-call risk. Only when this function runs from a genuinely
    separate process (the real scheduler) does the direct attempt raise
    DuckDB's lock IOException; that specific exception is caught and
    triggers the HTTP fallback, which is exactly the case bug #1's fix
    was written for.

    Parameters
    ----------
    run_date : date
    db_path : Path, optional
        Unused — evaluate() reads feature Parquet directly (SPEC-DS-005).

    Returns
    -------
    None

    Spec References
    ----------------
    SPEC-TA-006, SPEC-TA-009, SPEC-DS-002

    PIT Assumptions
    ----------------
    None at this layer — evaluate() only reads run_date's own feature
    Parquet, no look-ahead.

    Raises
    ------
    Exception
        Any failure in template evaluation, or in both the direct write
        attempt and its HTTP fallback — propagated so the checkpoint
        records this step as failed.
    """
    import duckdb

    from systems.technical_analysis.alerts.daily_alert_checker import DailyAlertChecker

    date_str = run_date.isoformat()
    resolved, template_results = DailyAlertChecker().evaluate(run_date=date_str)
    if resolved is None:
        logger.warning(f"check_ta_alerts: no feature Parquet available for {date_str} — skipping")
        return

    total_matches = sum(len(v) for v in template_results.values())

    try:
        _write_ta_results_direct(resolved, template_results)
        from systems.technical_analysis.alerts import alert_store
        from datetime import date as date_type_

        newly_triggered = alert_store.check_alerts(date_type_.fromisoformat(resolved))
        logger.info(
            f"check_ta_alerts: {total_matches} template full-matches across {len(template_results)} "
            f"templates for {date_str} (direct DB write, in-process)"
        )
    except duckdb.IOException:
        # Genuinely a different process than the one holding
        # SIGNALS_DUCKDB_PATH's cached connection (the real scheduler
        # case) — fall back to writing through the API over HTTP so only
        # the API process ever opens the file (see docstring bug #1).
        logger.info("check_ta_alerts: direct DB write hit a cross-process lock — falling back to the API over HTTP")
        newly_triggered = _write_ta_results_via_api(date_str, resolved, template_results, total_matches)

    logger.info(f"check_ta_alerts: {len(newly_triggered)} user-defined alert(s) newly triggered for {date_str}")


def step_compute_momentum(run_date: date_type, db_path: Optional[Path] = None) -> None:
    """
    ML38 (2026-07-14, extended 2026-07-15): live momentum-strategy
    section. For EACH of the 5 rank-band strategies
    (features.momentum_live.STRATEGIES — Rank 1-50/51-100/100-150/
    150-200/100-200, same top 15 stocks / 6-month lookback / monthly
    rebalance / grace=2 config, different market-cap band each) computes
    run_date's momentum ranking, upserts it into momentum_rankings,
    refreshes momentum_rebalance_state's next_rebalance_date, and — on a
    rebalance day — writes fresh momentum_rebalance_suggestions by
    diffing that strategy's ranking against its own currently-open
    momentum_trades rows (applying the exact grace-period rule
    backtest.momentum_backtest.decide_grace_transitions uses, never a
    second hand-written copy). Each strategy is fully independent —
    one strategy's ranking/suggestions never affect another's.

    Deterministic given that day's own already-final EOD OHLCV — same
    backfill rationale as check_ta_alerts — and it only ever suggests
    trades for the user to manually review and record, never auto-trades,
    so a missed/late day is safe to backfill (unlike paper_trade).

    Parameters
    ----------
    run_date : date
    db_path : Path, optional
        Defaults to config.settings.DUCKDB_PATH.

    Returns
    -------
    None

    Raises
    ------
    Exception
        Any DB or compute failure — propagated so the checkpoint records
        this step as failed rather than silently skipping a day.
    """
    from config.settings import DUCKDB_PATH
    from datastore.schema.create_normalised import (
        _CREATE_MOMENTUM_RANKINGS,
        _CREATE_MOMENTUM_REBALANCE_STATE,
        _CREATE_MOMENTUM_REBALANCE_SUGGESTIONS,
        _CREATE_MOMENTUM_TRADES,
    )
    from features import momentum_live

    date_str = run_date.isoformat()
    resolved_db_path = db_path or DUCKDB_PATH

    with get_duckdb_connection(resolved_db_path, persist=False) as conn:
        # Lazily ensure every momentum table this step touches exists —
        # same idempotent CREATE TABLE IF NOT EXISTS convention as
        # datastore/api/routers/holdings.py's _ensure_table, in case this
        # runs before create_normalised.create_schema() has ever been
        # applied against this DB file.
        for ddl in (
            _CREATE_MOMENTUM_TRADES, _CREATE_MOMENTUM_RANKINGS,
            _CREATE_MOMENTUM_REBALANCE_SUGGESTIONS, _CREATE_MOMENTUM_REBALANCE_STATE,
        ):
            conn.execute(ddl)

        summary_parts = []
        for strategy in momentum_live.STRATEGIES:
            strategy_id = strategy["strategy_id"]

            ranking = momentum_live.compute_daily_ranking(conn, date_str, strategy_id=strategy_id)
            if ranking.empty:
                logger.warning(f"compute_momentum: no ranking computable for {strategy_id} on {date_str} — skipping")
                continue

            conn.execute(
                "DELETE FROM momentum_rankings WHERE date = ? AND strategy_id = ?",
                [date_str, strategy_id],
            )
            conn.executemany(
                """
                INSERT INTO momentum_rankings
                    (date, strategy_id, ticker, momentum_return, momentum_rank, in_top_n, band_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    [date_str, strategy_id, row.ticker, float(row.momentum_return),
                     int(row.momentum_rank), bool(row.in_top_n), strategy["band_id"]]
                    for row in ranking.itertuples(index=False)
                ],
            )

            next_date = momentum_live.next_rebalance_date(conn, date_str)
            conn.execute(
                """
                INSERT INTO momentum_rebalance_state (strategy_id, next_rebalance_date)
                VALUES (?, ?)
                ON CONFLICT (strategy_id) DO UPDATE SET
                    next_rebalance_date = excluded.next_rebalance_date,
                    updated_at = now()
                """,
                [strategy_id, next_date],
            )

            n_suggestions = 0
            if momentum_live.is_rebalance_day(conn, date_str, strategy_id=strategy_id):
                open_trades = conn.execute(
                    "SELECT ticker, grace_remaining FROM momentum_trades "
                    "WHERE strategy_id = ? AND sale_date IS NULL",
                    [strategy_id],
                ).fetchall()
                current_open_trades = [{"ticker": t, "grace_remaining": g} for t, g in open_trades]

                suggestions = momentum_live.compute_rebalance_suggestions(
                    conn, date_str, current_open_trades, strategy_id=strategy_id,
                )
                for s in suggestions:
                    conn.execute(
                        """
                        INSERT INTO momentum_rebalance_suggestions
                            (strategy_id, rebalance_date, ticker, action, momentum_rank, grace_remaining)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        [strategy_id, date_str, s["ticker"], s["action"],
                         s["momentum_rank"], s["grace_remaining"]],
                    )
                    # Persist the updated grace countdown onto the open trade
                    # itself (momentum_trades.grace_remaining) so the NEXT
                    # rebalance's compute_rebalance_suggestions call reads the
                    # correct current state. "add" suggestions never have an
                    # existing open trade row (compute_rebalance_suggestions
                    # only emits "add" for tickers not already held) — nothing
                    # to update there; that row is created with
                    # grace_remaining=NULL once the user records the buy.
                    if s["action"] in ("grace_hold", "exit"):
                        conn.execute(
                            "UPDATE momentum_trades SET grace_remaining = ? "
                            "WHERE strategy_id = ? AND ticker = ? AND sale_date IS NULL",
                            [s["grace_remaining"], strategy_id, s["ticker"]],
                        )
                n_suggestions = len(suggestions)
                conn.execute(
                    "UPDATE momentum_rebalance_state SET last_rebalance_date = ? WHERE strategy_id = ?",
                    [date_str, strategy_id],
                )

            summary_parts.append(f"{strategy_id}: ranked={len(ranking)} next={next_date} suggestions={n_suggestions}")

    logger.info(f"compute_momentum: {date_str} — " + "; ".join(summary_parts))


def step_publish_and_snapshot(run_date: date_type, db_path: Optional[Path] = None) -> None:
    """
    A25 (Write-Audit-Publish Architecture): takes today's incremental
    rollback snapshot of the pilot tables (fno_data, ohlcv_adjusted — the
    two tables that change daily and drive the real incremental-snapshot
    cost, per FeatureBacklog.md A25) and prunes down to
    SNAPSHOT_RETENTION_N (default 7) most recent snapshots.

    Deliberately does NOT call datastore/staging/publish.py::publish_table
    here — the daily pipeline's own download_fno/adjust_prices steps still
    write via their original direct DELETE+INSERT/upsert path (unchanged),
    not through datastore/staging/gate.py. Staged mode (--publish-mode
    staged) is opt-in today for scripts/insert_fno_files.py and
    ingestion/backfill_runner.py's manual/backfill runs only. This step
    exists so that even direct-mode daily writes get an N=7 rollback point
    — a bad day's data can be reverted with scripts/restore_snapshot.py
    regardless of which write path produced it.

    Parameters
    ----------
    run_date : date
        Unused directly — snapshots always reflect the tables' current
        (i.e. run_date's, since this runs after that day's writes) state.
    db_path : Path, optional
        Defaults to config.settings.DUCKDB_PATH.

    Raises
    ------
    None — snapshotting failure is logged, not raised, so a rollback-point
    hiccup never blocks the rest of the pipeline (this step runs last).
    """
    from config.settings import DUCKDB_PATH, SNAPSHOT_DIR, SNAPSHOT_RETENTION_N
    from datastore.staging.snapshot import prune_snapshots, take_snapshot

    resolved_db_path = db_path or DUCKDB_PATH
    tables = ["fno_data", "ohlcv_adjusted"]

    try:
        with get_duckdb_connection(resolved_db_path, persist=False) as conn:
            snapshot_path = take_snapshot(conn, tables, SNAPSHOT_DIR)
        removed = prune_snapshots(SNAPSHOT_DIR, SNAPSHOT_RETENTION_N)
        logger.info(
            "publish_and_snapshot: snapshot written to %s, %d old snapshot(s) pruned",
            snapshot_path, len(removed),
        )
    except Exception as exc:
        logger.error(f"publish_and_snapshot: snapshot failed (non-fatal): {exc}")


def _write_ta_results_direct(resolved: str, template_results: dict) -> None:
    """In-process ta_signals write — succeeds instantly (no new OS lock)
    when this process already holds SIGNALS_DUCKDB_PATH's cached
    connection (i.e. running inside the API process, e.g. Ops Monitor
    force-run); raises duckdb.IOException when a different process
    already holds it (the real scheduler case), letting the caller fall
    back to the HTTP path. See step_check_ta_alerts's docstring."""
    from config.settings import SIGNALS_DUCKDB_PATH
    from datastore.api.db import get_duckdb_connection
    from systems.technical_analysis.alerts.daily_alert_checker import DailyAlertChecker

    checker = DailyAlertChecker()
    SIGNALS_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_duckdb_connection(SIGNALS_DUCKDB_PATH, persist=False) as conn:
        checker._ensure_db_and_table(conn)
        checker._write_all_results(conn, resolved, template_results)


def _write_ta_results_via_api(date_str: str, resolved: str, template_results: dict, total_matches: int) -> list:
    """Cross-process fallback: write ta_signals + check triggers through the API over HTTP."""
    import httpx

    from config.settings import DATASTORE_API_BASE_URL
    from systems.technical_analysis.alerts.daily_alert_checker import _TEMPLATE_CATEGORY

    rows = [
        {
            "date": resolved,
            "ticker": r.ticker,
            "template_name": r.template_name,
            "category": _TEMPLATE_CATEGORY.get(r.template_name, "custom"),
            "score": r.score,
            "matched_conditions": r.matched_conditions,
            "total_conditions": r.total_conditions,
            "key_values": r.key_values or {},
        }
        for results in template_results.values()
        for r in results
    ]

    # 120s: a full day's ta_signals batch (~13k rows / ~3.4MB JSON, all 42
    # templates x full universe) took ~38s round-trip in testing — this is
    # a once-daily batch job, not an interactive call, so a generous
    # timeout is fine.
    with httpx.Client(timeout=120.0) as client:
        write_resp = client.post(f"{DATASTORE_API_BASE_URL}/api/v1/ta/signals/write", json={"rows": rows})
        write_resp.raise_for_status()
        logger.info(f"check_ta_alerts: {total_matches} template full-matches across {len(template_results)} templates for {date_str} (written={write_resp.json().get('written')})")

        check_resp = client.post(f"{DATASTORE_API_BASE_URL}/api/v1/ta/user-alerts/check-triggers", json={"date": resolved})
        check_resp.raise_for_status()
        return check_resp.json().get("newly_triggered", [])


def step_data_integrity_check(run_date: date_type, db_path: Optional[Path] = None) -> None:
    """
    A20: run the four data-integrity checks (corporate-action cross-
    check, null/NaN sweep, holiday/leakage check, random 5yr spot-check —
    datastore/integrity/checks.py) against run_date's already-published
    production data, recording every finding via
    datastore.integrity.runner.run_integrity_checks. Runs before
    compute_features/run_models (checkpoint.py's STEPS ordering) so a bad
    ingest never propagates into that day's features/signals.

    Findings are always recorded as status='pending' — never auto-applied
    (this project's "flag, don't silently write" discipline, A12/A25).
    Only a 'critical' finding fails this checkpoint step; 'warning'/'info'
    findings are recorded but don't block the pipeline.

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
    FeatureBacklog.md A20

    Raises
    ------
    RuntimeError
        If any check produced a 'critical' finding — propagated so the
        checkpoint records this step as failed and it surfaces on the Ops
        page, same as step_sanity_check's hard-floor raise.
    """
    from config.settings import DUCKDB_PATH
    from datastore.integrity.runner import run_integrity_checks

    resolved_db_path = db_path or DUCKDB_PATH
    with get_duckdb_connection(resolved_db_path, persist=False) as conn:
        result = run_integrity_checks(conn, run_date)

    logger.info(
        f"data_integrity_check: {run_date} findings_by_check={result.findings_by_check} "
        f"critical_count={result.critical_count}"
    )
    if result.critical_count > 0:
        raise RuntimeError(
            f"data_integrity_check: {result.critical_count} critical finding(s) for {run_date} — "
            f"see data_integrity_findings table (status='pending')"
        )


_STEP_DISPATCH = {
    "download_bhavcopy": step_download_bhavcopy,
    "download_fno": step_download_fno,
    "download_macro": step_download_macro,
    "download_index_ohlcv": step_download_index_ohlcv,
    "download_corporate_actions": step_download_corporate_actions,
    "download_large_deals": step_download_large_deals,
    "attribute_bulk_deals": step_attribute_bulk_deals,
    "adjust_prices": step_adjust_prices,
    "compute_features": step_compute_features,
    "run_models": step_run_models,
    "write_signals": step_write_signals,
    "sanity_check": step_sanity_check,
    "paper_trade": step_paper_trade,
    "check_ta_alerts": step_check_ta_alerts,
    "compute_momentum": step_compute_momentum,
    "publish_and_snapshot": step_publish_and_snapshot,
    "data_integrity_check": step_data_integrity_check,
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


def _wait_for_datastore_api(max_wait_seconds: int = 120, poll_interval_seconds: int = 5) -> None:
    """
    Block until the DataStore API's /health endpoint responds, or give up
    after max_wait_seconds and log a loud warning (the pipeline still runs
    afterwards — steps that need the API will fail cleanly and be retried
    on the next scheduled/catch-up run, same as any other outage under
    SPEC-PIPE-006's "mark unavailable, non-critical" philosophy).

    Raises
    ------
    None
    """
    import httpx

    from config.settings import DATASTORE_API_BASE_URL

    deadline = time.monotonic() + max_wait_seconds
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = httpx.get(f"{DATASTORE_API_BASE_URL}/health", timeout=5.0)
            if resp.status_code == 200:
                if attempt > 1:
                    logger.info("DataStore API is up (after %d attempt(s))", attempt)
                return
        except httpx.RequestError:
            pass

        if time.monotonic() >= deadline:
            logger.warning(
                "DataStore API at %s did not respond within %ds — proceeding anyway; "
                "steps requiring it will fail cleanly and be retried on the next run",
                DATASTORE_API_BASE_URL, max_wait_seconds,
            )
            return

        logger.info("Waiting for DataStore API at %s (attempt %d)...", DATASTORE_API_BASE_URL, attempt)
        time.sleep(poll_interval_seconds)


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

    from config.settings import (
        DAILY_PIPELINE_SCHEDULE_TIME,
        MORNING_CATCHUP_ENABLED,
        MORNING_CATCHUP_SCHEDULE_TIME,
    )
    from ingestion.scheduler.checkpoint import CheckpointManager
    from ingestion.scheduler.pipeline_scheduler import (
        create_scheduler,
        schedule_daily_backup,
        schedule_daily_pipeline,
        schedule_fno_late_catchup,
        schedule_forensic_scoring,
        schedule_job_health_check,
        schedule_mf_holdings_ingestion,
        schedule_morning_catchup,
        schedule_balance_sheet_backfill,
        schedule_model_training_nightly,
        schedule_multibagger_scoring,
        schedule_nse_xbrl_fundamentals,
        schedule_promoter_pledge_backfill,
        schedule_weekend_feature_backfill,
        schedule_weekend_fundamentals,
    )

    # 2026-07-07: ensure the DuckDB schema is fully provisioned before any
    # step runs. create_schema() is idempotent (CREATE TABLE IF NOT EXISTS)
    # so this is safe on every startup — it's the fix for index_ohlcv having
    # been added to _ALL_TABLES but never actually created against the live
    # DB, since nothing previously called create_schema() outside manual/
    # ad-hoc invocation.
    from datastore.schema.create_normalised import create_schema

    create_schema()

    # 2026-07-10 incident: on a laptop restart the DataStore API (uvicorn,
    # started separately) isn't guaranteed to be up yet when this process's
    # startup catch-up fires. Every path below (BackfillDataCache preload,
    # build_feature_matrix's bulk-then-per-ticker OHLCV fetch) depends on it
    # over HTTP, and a dead API turned the per-ticker fallback into an
    # unbounded loop that drove RSS to 5+ GB in under 3 minutes and paged
    # the OS's low-memory killer. Block here (bounded) instead of letting
    # that loop discover the outage the expensive way.
    _wait_for_datastore_api()

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
    if MORNING_CATCHUP_ENABLED:
        schedule_morning_catchup(
            scheduler, step_runner, checkpoint_manager, schedule_time=MORNING_CATCHUP_SCHEDULE_TIME
        )
    else:
        logger.info("morning_catchup: disabled via MORNING_CATCHUP_ENABLED=False — not scheduled.")
    # A56 follow-up (2026-07-30, user-reported): download_fno was failing
    # almost every day at 18:00 simply because NSE hadn't published that
    # day's F&O bhavcopy yet — step_download_fno now defers any live
    # (today) attempt before FNO_MIN_ATTEMPT_TIME instead of raising, and
    # this dedicated job makes the one real attempt later, by which time
    # NSE has routinely published it. See schedule_fno_late_catchup's
    # docstring for the compute_features re-trigger it also does.
    schedule_fno_late_catchup(scheduler, checkpoint_manager)
    schedule_mf_holdings_ingestion(scheduler)  # weekly (Sat 13:00 IST), primary source Groww
    # 2026-07-08: weekly scan for newly-published NSE Integrated Filing —
    # IndAS regulatory disclosures (real balance sheet + audit qualification
    # + shares_outstanding — see ingestion/scrapers/nse_xbrl_financials.py).
    # Registered FIRST among the weekend batch and fires earliest (05:00 IST
    # Saturday, ~4h before weekend_feature_backfill) per explicit operator
    # instruction: this must run ahead of forensic scoring, valuation
    # modeling, and every other model that reads `fundamentals` — a
    # full-universe scan is a real ~2-3h run, so it needs a multi-hour head
    # start over the rest of the weekend batch, not a same-morning gap.
    schedule_nse_xbrl_fundamentals(scheduler)
    # 2026-07-10 (A52, Pipeline & Monitoring Remediation): replaced the
    # single weekly Saturday model_training job with
    # schedule_model_training_nightly's Mon-Thu 23:00 IST per-group jobs
    # (_MODEL_TRAINING_GROUPS) — spreads training checks across the week
    # instead of concentrating every model's overdue-check (and any
    # resulting multi-hour retrain) into one Saturday run. Still well
    # clear of the 18:00 daily pipeline's own window. Takes effect on the
    # scheduler process's next restart, same as any other job
    # registration change here — schedule_model_training (the old
    # single-job version) is left intact and importable for any script
    # that still wants the original weekly-catch-up shape.
    schedule_model_training_nightly(scheduler)
    # Weekend jobs: feature backfill (09:00 IST Sat) + fundamentals (10:30 IST Sat).
    # weekend_fundamentals (Screener/Trendlyne) is the FALLBACK source and
    # deliberately runs after nse_xbrl_fundamentals (the primary source).
    schedule_weekend_feature_backfill(scheduler)
    schedule_weekend_fundamentals(scheduler)
    # A54 (2026-07-10): two real, live-verified backfill scripts
    # (promoter-pledge from NSE, balance-sheet from cached Screener pages)
    # existed and worked but were never scheduled — 71% of
    # shareholding.promoter_pledge rows were NULL purely because of that.
    # Fire after weekend_fundamentals (10:30) has refreshed the base rows
    # these enrich, before model_training (12:00).
    schedule_promoter_pledge_backfill(scheduler)
    schedule_balance_sheet_backfill(scheduler)
    # FutureDevelopment.md #14: multibagger/forensic scoring were operator-CLI
    # only until 2026-07-04 — schedule both weekly, Sunday morning (markets
    # closed, no contention with weekday pipeline or Saturday jobs).
    schedule_multibagger_scoring(scheduler)
    schedule_forensic_scoring(scheduler)
    # 2026-07-04: daily off-machine backup (rclone to Backblaze B2) — every
    # day, not just weekdays, since paper_trading/config can change
    # regardless of whether NSE was open. No-op (records "skipped") until
    # BACKUP_ENABLED=true + BACKBLAZE_KEY_ID/BACKBLAZE_APPLICATION_KEY/
    # BACKBLAZE_BUCKET are set — see scripts/backup_to_b2.py's module
    # docstring.
    schedule_daily_backup(scheduler)
    # A21 (Pipeline Health Checker): weekly job-completeness audit, fires
    # after the weekend batch + Sunday scoring jobs above have had a
    # chance to record their own job_run_log rows.
    schedule_job_health_check(scheduler)
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
    morning_catchup_status = (
        f"registered for {MORNING_CATCHUP_SCHEDULE_TIME} IST (mon-fri)"
        if MORNING_CATCHUP_ENABLED
        else "disabled (MORNING_CATCHUP_ENABLED=False)"
    )
    logger.info(
        f"Scheduler started: daily pipeline registered for {DAILY_PIPELINE_SCHEDULE_TIME} IST (mon-fri), "
        f"morning catch-up {morning_catchup_status}, "
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
