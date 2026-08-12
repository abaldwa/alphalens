"""
scripts/feature_backfill.py

Phase: 3 (Deep Learning — feature store preparation)
Specs: SPEC-DS-005, SPEC-PIPE-004
Owner: Platform / ML
Consumers: systems/ml_signal_engine/inference/train_deep_models.py

Backfills the daily feature parquet store (datastore/features/daily/) for a
given date range so that train_deep_models.py has sufficient training data.

PREREQUISITE — DataStore API must be running before you invoke this script:
    .venv/bin/uvicorn datastore.api.main:app --host 127.0.0.1 --port 8000

Start date rationale
--------------------
    OHLCV is available from 2006-01-02. The longest feature lookback is 252
    trading days (1-year moving average), so the first date with fully valid
    technical features is ~2007-01-03. Default start is 2007-01-03.

    Fundamentals, shareholding, F&O, and macro data are only populated for
    recent dates (scrapers run since mid-2026). For dates before those
    scrapers backfill their tables, those feature columns will be NaN — the
    deep-learning models handle this via masking, but expect the historical
    matrices to be primarily technical features.

Processing order: most-recent-first (default)
--------------------
    Processing newest dates first means you have 2023-2026 parquets ready
    (and can kick off a training run) while the backfill continues on older
    dates in the background.

Usage
-----
    # Default: 2007-01-03 → today, most-recent-first
    .venv/bin/python3 scripts/feature_backfill.py

    # Oldest-first (chronological)
    .venv/bin/python3 scripts/feature_backfill.py --chronological

    # Background run with live progress log
    nohup .venv/bin/python3 scripts/feature_backfill.py \\
        > logs/feature_backfill.log 2>&1 &

    tail -f logs/feature_backfill.log

    # Force recompute even if parquet already exists
    .venv/bin/python3 scripts/feature_backfill.py --force

--force is only actually honored ONCE per run_id
------------------------------------------------
    The first invocation of a given --run-id (explicit or auto-generated)
    that passes --force recomputes dates with an existing parquet, as
    expected, AND writes a sentinel file
    (datastore/features/daily/.<run-id>.force_applied). Any LATER
    invocation with the SAME run_id that also passes --force is treated as
    an automatic crash/OOM-kill restart of the same run, not a fresh
    request to blow away already-correct progress — it silently ignores
    --force and falls back to normal skip-if-exists behavior. This exists
    so an unattended restart supervisor (see
    scripts/run_feature_backfill_supervised.sh /
    alphalens-feature-backfill@.service) can always pass --force
    unconditionally on every attempt without ever redoing already-finished
    work after a restart — only the FIRST attempt for a run_id actually
    forces. Use a fresh --run-id if you deliberately want --force to apply
    again from scratch.

Timing
------
    Each date: ~60-300 s (2492 tickers × API fetch + vectorised feature math).
    Full backfill 2007→2026: ~4,900 dates → estimate 20-80 hours.
    Dates that already have a parquet in datastore/features/daily/ are skipped.
"""

import argparse
import logging
import os
import sys
import time
from datetime import date as date_type, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backfill daily feature parquets for deep-learning training")
    p.add_argument("--from-date", default="2007-01-03", metavar="YYYY-MM-DD",
                   help="Earliest date to compute (default: 2007-01-03, after 252-day OHLCV warmup)")
    p.add_argument("--to-date", default=None, metavar="YYYY-MM-DD",
                   help="Latest date to compute (default: today)")
    p.add_argument("--chronological", action="store_true",
                   help="Process oldest dates first (default: newest-first so training data is ready sooner)")
    p.add_argument("--force", action="store_true",
                   help="Recompute dates that already have a parquet file")
    p.add_argument("--no-hmm", action="store_true",
                   help="Skip HMM regime-feature fitting (HMM cols become NaN). "
                        "~14x faster per date — recommended for full historical backfill. "
                        "Deep-learning models handle NaN via masking.")
    p.add_argument("--skip-slow-categories", action="store_true",
                   help="[2026-07-31] Skip fundamental/governance/mf_holdings/corp_action/"
                        "fno/multibagger/real_economy_macro/deep_forensic entirely (all-NaN "
                        "columns) and skip this script's own per-ticker BackfillDataCache "
                        "pre-load that only those categories use. Use when the goal is only "
                        "repopulating technical/intraday/pnd/advanced_technical/pattern_scores "
                        "(the panel_staging-covered categories) — e.g. backfilling the columns "
                        "the TA screener templates depend on — without paying for categories "
                        "that will be backfilled separately. Not used by the live daily "
                        "pipeline; matrix_builder.py::build_feature_matrix's "
                        "skip_slow_categories param docstring has the full column list.")
    p.add_argument("--advanced-technical-used-only", action="store_true",
                   help="[2026-08-04] Compute only hurst_exp_21d of the 18 advanced_technical "
                        "features (skip the other 17's expensive per-row wavelet/entropy/"
                        "fractal/fracdiff/Lyapunov/RQA computations, leaving them NaN) — those "
                        "17 are never referenced downstream by name, only as bulk ML-model "
                        "inputs. Default False (this backfill script stays comprehensive unless "
                        "explicitly requested fast) — unlike the live daily pipeline, which "
                        "defaults this True. Use scripts/backfill_deferred_advanced_technical.py "
                        "to fill the skipped 17 back in later.")
    p.add_argument("--panel-workers", type=int, default=1,
                   help="Parallelize the technical/intraday/pnd/advanced_technical/"
                        "pattern_scores chunk loop across N worker processes (default: 1, "
                        "unchanged sequential behavior). See "
                        "features/matrix_builder.py::_compute_chunked_ticker_independent_panels "
                        "for the BLAS-thread-capping safeguard this uses. Recommended max on "
                        "this project's 16GB laptop: 8, not 12 — 12 simultaneous workers was "
                        "implicated in a sharp temporary memory dip during a real launch "
                        "(2026-07-29), on a machine with a real history of systemd-oomd kills "
                        "during long backfills.")
    p.add_argument("--run-id", default=None,
                   help="Identifier for this run's failed-dates manifest file "
                        "(default: a timestamp+PID, e.g. 20260728_101500_12345). The manifest is "
                        "written incrementally to config.settings.LOGS_DIR/"
                        "feature_backfill_failed_<run-id>.txt, one failed date per line, so a "
                        "human can grep/retry exact failures without re-scanning a potentially "
                        "huge log.")
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    from config.settings import DUCKDB_PATH, FEATURES_DAILY_DIR, LOGS_DIR
    from config.universe import get_tickers_for_feature_engineering
    from datastore.api.db import get_duckdb_connection
    from ingestion.scheduler.daily_pipeline import step_compute_features

    from_dt = date_type.fromisoformat(args.from_date)
    to_dt = date_type.fromisoformat(args.to_date) if args.to_date else date_type.today()

    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
        rows = conn.execute(
            "SELECT DISTINCT CAST(date AS VARCHAR) FROM ohlcv_adjusted "
            "WHERE date >= ? AND date <= ? ORDER BY date",
            [from_dt.isoformat(), to_dt.isoformat()],
        ).fetchall()

    all_dates = [date_type.fromisoformat(r[0]) for r in rows]
    logger.info("Found %d trading dates from %s to %s", len(all_dates), from_dt, to_dt)

    FEATURES_DAILY_DIR.mkdir(parents=True, exist_ok=True)

    # run_id is normally computed further below (it also keys the
    # failed-dates manifest), but it's needed here too — see the
    # --force-only-once sentinel handling immediately below.
    run_id = args.run_id or f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{os.getpid()}"

    # [Auto-restart support, 2026-07-29] --force means "recompute dates
    # that already have a parquet" — correct and intentional on the FIRST
    # launch of a run (e.g. stale/wrong-schema parquets need clearing),
    # but WRONG on every automatic restart after a crash/OOM-kill of the
    # SAME run: by then this run's own earlier (pre-crash) progress has
    # already written CORRECT parquets for those dates, and blindly
    # honoring --force again on every restart would mean an unattended
    # supervisor loop (e.g. systemd Restart=on-failure) never converges —
    # every restart would redo all the work its own prior attempt already
    # finished. A marker file, keyed by run_id, records that --force has
    # already been "spent" once for this run_id; subsequent invocations
    # with the same run_id silently fall back to normal skip-if-exists
    # behavior even if the restart supervisor still passes --force
    # (simplest, since the supervisor then doesn't need its own separate
    # "strip --force after the first attempt" logic).
    force_sentinel = FEATURES_DAILY_DIR / f".{run_id}.force_applied"
    effective_force = args.force
    if args.force and force_sentinel.exists():
        effective_force = False
        logger.info(
            "--force was passed but run_id=%s already applied --force once before "
            "(sentinel %s exists) — treating this as an automatic restart and falling "
            "back to normal skip-if-exists behavior instead of recomputing everything again",
            run_id, force_sentinel,
        )
    elif args.force:
        force_sentinel.write_text(datetime.now().isoformat())
        logger.info(
            "--force applied for run_id=%s (first time) — wrote sentinel %s so any "
            "automatic restart with this same run_id will NOT re-force",
            run_id, force_sentinel,
        )

    if effective_force:
        pending = all_dates
    else:
        existing = {p.stem for p in FEATURES_DAILY_DIR.glob("*.parquet")}
        pending = [d for d in all_dates if d.isoformat() not in existing]
        skipped = len(all_dates) - len(pending)
        logger.info("%d already have parquets (skipping); %d to compute", skipped, len(pending))

    # Newest-first by default so training data (recent years) is ready sooner
    if not args.chronological:
        pending = list(reversed(pending))

    if not pending:
        logger.info("Nothing to do — all dates have parquets. Use --force to recompute.")
        return

    compute_hmm = not args.no_hmm
    if not compute_hmm:
        logger.info("HMM regime features DISABLED (--no-hmm): hmm_* columns will be NaN")

    # Pre-load all per-ticker data ONCE before the date loop.  For each
    # subsequent date the cache provides in-memory PIT-filtered slices,
    # replacing ~15 M per-date API calls with ~1 500 one-time calls.
    # [2026-07-31] Skipped entirely under --skip-slow-categories: this cache
    # only feeds fundamental/governance/mf_holdings/corp_action/deep_forensic,
    # all of which build_feature_matrix skips in that mode — building it
    # would be pure wasted I/O.
    from datastore.client import DataStoreClient

    client = DataStoreClient()
    tickers_for_cache = get_tickers_for_feature_engineering()
    backfill_cache = None
    if not args.skip_slow_categories:
        from features.backfill_cache import BackfillDataCache

        backfill_cache = BackfillDataCache(client, tickers_for_cache, to_date=datetime.combine(to_dt, datetime.min.time()))

    # [BUG FIX, 2026-07-28 model-review item 6] A 20-80 hour unattended run
    # with only ERROR-level log lines and a final summary count has real
    # precedent for going badly unnoticed on this project (a June 2026
    # backfill silently failed 4,772/4,785 dates on an unrelated bug, only
    # discovered after the fact). Write each failed date incrementally to a
    # small manifest file, so a human/script can grep/retry the exact
    # failures without re-scanning a huge log.
    # run-id defaults to a second-resolution timestamp + PID so two backfill
    # launches started in the same second (e.g. a quick manual retry, or two
    # concurrent agents) don't share one manifest file. (Computed earlier,
    # above, so the --force-only-once sentinel could also key off it.)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    manifest_path = LOGS_DIR / f"feature_backfill_failed_{run_id}.txt"
    logger.info("Failed-dates manifest (if any): %s", manifest_path)

    # [2026-08-12] PANEL STAGING RETIRED (user decision).
    #
    # features/panel_staging.py used to run here: it precomputed technical/
    # intraday/pnd/advanced_technical/pattern_scores once per ticker chunk
    # across the whole `pending` range into a DuckDB table, and the per-date
    # loop below pulled each date's rows back out. Its docstring claimed the
    # per-date path was ~760x slower.
    #
    # Measured on 2026-08-12, same script/flags/worker count, real run logs:
    #
    #     staging ON,  cache off                        19.1 s/date
    #     staging off, cache off                        19.6 s/date
    #     staging off, cache ON                         15.0 s/date
    #     staging off, cache ON, staging DB shrunk       9.0 s/date
    #
    # Staging bought ~0.5 s/date for a 5-6 min fixed cost per year — and was
    # NET NEGATIVE overall, because load_staged_panel_for_date opened and
    # closed an 18.9 GB DuckDB file on EVERY date. (scripts/patch_advanced_
    # technical_pattern_scores.py:116 had already noticed the open-close-per-
    # call cost and worked around it by passing its own connection.) Removing
    # it roughly halved per-date cost and, because staging was the only shared
    # writer, also let recompute years run as independent parallel processes.
    #
    # The real win over the per-date path was never staging — it was the bulk
    # OHLCV window cache prewarmed below, which removes the repeated ~760-day
    # fetch that every date was re-issuing.
    #
    # features/panel_staging.py is left on disk (deprecated) only because the
    # one-off repair script above imports it. Nothing in the production
    # backfill path touches it any more, and staged_panel is always None.
    import pandas as pd

    # [2026-08-12] Prewarm the bulk-OHLCV window cache for the WHOLE run.
    # build_feature_matrix fetches each date's own [d - LOOKBACK_CALENDAR_DAYS,
    # d] window, so a 246-date year re-pulled ~2 years x ~2,317 tickers 246
    # times over. Staging already removed the redundant COMPUTE; this removes
    # the redundant FETCH, which was the larger remaining cost (15-20s of each
    # date's ~18s). One fetch here covers every date's window because the span
    # below is the union of them all.
    from datetime import timedelta as _timedelta

    from datastore.client import disable_bulk_ohlcv_cache, enable_bulk_ohlcv_cache
    from features.matrix_builder import LOOKBACK_CALENDAR_DAYS

    cache_from = pd.Timestamp(min(pending)) - _timedelta(days=LOOKBACK_CALENDAR_DAYS)
    cache_to = pd.Timestamp(max(pending))
    if os.environ.get("ALPHALENS_DISABLE_BULK_CACHE"):
        # Kill switch: lets a run be A/B'd against the uncached path to prove
        # the cache is behaviour-preserving, and provides an escape hatch if it
        # is ever suspected of serving wrong rows.
        logger.info("ohlcv_cache: DISABLED via ALPHALENS_DISABLE_BULK_CACHE")
        disable_bulk_ohlcv_cache()
    else:
        try:
            t_cache0 = time.monotonic()
            n_cached = enable_bulk_ohlcv_cache(cache_from, cache_to, client)
            logger.info(
                "ohlcv_cache: prewarmed %s..%s (%d rows) in %.1f min — per-date bulk fetches now served from memory",
                cache_from.date(), cache_to.date(), n_cached, (time.monotonic() - t_cache0) / 60,
            )
        except Exception as exc:  # noqa: BLE001
            # Purely an optimization: every date still fetches for itself.
            logger.error("ohlcv_cache: prewarm FAILED (%s) — falling back to per-date fetches", exc, exc_info=True)
            disable_bulk_ohlcv_cache()

    ok = err = 0
    elapsed_times: list = []

    for i, d in enumerate(pending, start=1):
        t0 = time.monotonic()
        try:
            # panel staging retired 2026-08-12 (see the note above) — the five
            # batched categories are computed per date, which measured FASTER
            # than loading them back out of the staging DB.
            staged_panel = None

            step_compute_features(
                d, compute_hmm=compute_hmm, data_cache=backfill_cache,
                panel_workers=args.panel_workers, staged_panel=staged_panel,
                skip_slow_categories=args.skip_slow_categories,
                advanced_technical_used_only=args.advanced_technical_used_only,
            )
            elapsed = time.monotonic() - t0
            elapsed_times.append(elapsed)
            remaining = len(pending) - i
            avg = sum(elapsed_times[-20:]) / len(elapsed_times[-20:])  # rolling 20-date avg
            eta_min = remaining * avg / 60
            logger.info(
                "[%d/%d] %s done in %.1fs — %d remaining, ETA ~%.0f min",
                i, len(pending), d, elapsed, remaining, eta_min,
            )
            ok += 1
        except Exception as exc:  # noqa: BLE001
            err += 1
            logger.error("[%d/%d] %s FAILED: %s", i, len(pending), d, exc)
            with open(manifest_path, "a") as f:
                f.write(f"{d.isoformat()}\n")

    total_min = sum(elapsed_times) / 60
    logger.info(
        "Backfill complete: %d succeeded, %d failed, total %.1f min",
        ok, err, total_min,
    )
    if err:
        logger.info("Failed dates written to %s — retry with those dates specifically", manifest_path)

    # (panel-staging teardown removed 2026-08-12 — nothing is staged any more.)


if __name__ == "__main__":
    main()
