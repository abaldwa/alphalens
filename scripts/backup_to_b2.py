"""
scripts/backup_to_b2.py

Phase: 3.x (Daily Off-Machine Backup)
Specs: SPEC-SYS-005 (Storage Budgets), SPEC-SEC-001 (credentials from env only)
Owner: Platform / Ops

Backs up the small, authoritative (non-re-derivable) subset of datastore/ to
Backblaze B2 via `rclone`, using an on-the-fly connection string built from
BACKBLAZE_KEY_ID/BACKBLAZE_APPLICATION_KEY/BACKBLAZE_BUCKET (config.settings)
— no `rclone config`, no saved remote, no browser/OAuth step anywhere.

[AS BUILT, 2026-07-04] Originally designed against Google Drive
(scripts/backup_to_gdrive.py), but rclone's Drive backend requires an
interactive browser OAuth consent flow (and often a Google Cloud "app
verification" step for personal accounts) that the user found impractical
to set up. Switched to Backblaze B2: auth there is two plain strings (an
Account/Key-ID pair generated once on backblaze.com's "App Keys" page),
which rclone accepts directly as connection-string parameters — the exact
same "credential in .env, no interactive step" pattern every other source
in this codebase already uses (SPEC-SEC-001). B2's always-free tier is
10GB, comfortably covering the ~3.5GB/day this backs up.

What is backed up and why (2026-07-04 architecture review decision):
  - datastore/normalised/  : OHLCV, fundamentals, corporate actions, etc.
    Years of scraped + backfilled data (FYERS 5yr pull, NSE archive-to-2006,
    fundamentals scraping) — re-deriving this from scratch is hours-to-days
    and FYERS needs an interactive login token that may not be available.
  - datastore/signals/     : ML/forensic/TA signal history — not
    re-derivable without re-running every historical model inference.
  - datastore/models/      : trained model artifacts + registry.json —
    retrainable but slow (BuildLog: hours per TFT/BiLSTM fold).
  - paper_trading/         : portfolio state + executions — the actual
    trading track record; zero way to reconstruct if lost.
  - config/                : universe CSV + settings — tiny, easy to
    include, annoying to lose.

Deliberately EXCLUDED:
  - datastore/raw/         : re-scrapable from source archives (cache, not
    authoritative) — 13GB, would dominate upload time for no safety benefit.
  - datastore/features/    : fully re-derivable from normalised/ via
    step_compute_features — 7.3GB, zero unique information.

Uses `rclone sync` (not `copy`): the remote mirrors the source exactly,
including deletions. This is safe here because every source directory is
itself a live, authoritative store (not a scratch/staging area) — anything
genuinely deleted locally (e.g. a corrected duplicate row's file) should
also disappear from the backup, not accumulate forever.

Setup (one-time, on backblaze.com's web console — no OAuth, no browser
flow through rclone itself)
--------------------------------------------------------------
    1. Install rclone:      https://rclone.org/install/
    2. Sign up at backblaze.com (free), create a bucket (private is fine).
    3. Account page -> "App Keys" -> "Add a New Application Key". Scope it
       to the bucket created above if offered. Copy the "keyID" and
       "applicationKey" shown — the applicationKey is shown only once.
    4. Set in .env: BACKBLAZE_KEY_ID, BACKBLAZE_APPLICATION_KEY,
       BACKBLAZE_BUCKET (the bucket name from step 2).
    5. Verify: run this script with --dry-run (see Usage below) and check
       the log output lists the expected local directories with no errors.
    6. Set BACKUP_ENABLED=true in .env once verified (defaults to false —
       this job is a no-op until explicitly turned on, so a fresh checkout
       never fails a scheduled run against unset credentials).

Usage
-----
    .venv/bin/python3 scripts/backup_to_b2.py
    .venv/bin/python3 scripts/backup_to_b2.py --dry-run   # rclone --dry-run, no real transfer
"""

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (
    BACKBLAZE_APPLICATION_KEY,
    BACKBLAZE_BUCKET,
    BACKBLAZE_KEY_ID,
    BACKUP_ENABLED,
    BACKUP_REMOTE_PATH,
    CONFIG_DIR,
    MODELS_DIR,
    NORMALISED_DIR,
    PROJECT_ROOT,
    SIGNALS_DIR,
)

logger = logging.getLogger(__name__)

# (local source dir, remote sub-path name) — kept as separate rclone sync
# calls per directory (rather than one call over datastore/) so that
# excluding raw/ and features/ never depends on a fragile --exclude glob.
_BACKUP_TARGETS: List[Tuple[Path, str]] = [
    (NORMALISED_DIR, "normalised"),
    (SIGNALS_DIR, "signals"),
    (MODELS_DIR, "models"),
    (PROJECT_ROOT / "paper_trading", "paper_trading"),
    (CONFIG_DIR, "config"),
]

# Per-target rclone --include patterns: an ALLOWLIST, not a blocklist.
#
# [AS BUILT, 2026-08-13] This was a blocklist (--exclude "*.bak*", "*.tmp",
# "feature_panel_staging.duckdb") for three days, and it was already
# broken when it was replaced. A blocklist fails OPEN: every new scratch
# file in a backed-up directory ships until somebody remembers to exclude
# it. On 2026-08-13 two manual safety copies —
#
#     alphalens.duckdb.pre_ca_repair_20260813_114048     (3.11GB)
#     alphalens.duckdb.pre_index_ingest_20260813_131146  (3.11GB)
#
# — matched none of the patterns ("*.bak*" does not match ".pre_ca_repair"),
# so 6.2GB of scratch was queued for upload. That is the failure mode, not
# a one-off: nobody is going to update a glob list every time they take an
# ad-hoc copy before a risky migration.
#
# An allowlist fails CLOSED. Anything not named here is simply not backed
# up, so a new scratch file is safe by default and the only maintenance
# burden is remembering to add genuinely new *authoritative* data — which
# is rare, reviewable, and loud when it goes wrong (a missing table at
# restore time) rather than silent (a 3GB surprise on the bill).
#
# Empty list == take the whole directory (paper_trading/ and config/ are
# small and wholly authoritative).
_BACKUP_INCLUDES: Dict[str, List[str]] = {
    # The canonical stores only. Not: *.bak*, *.pre_*, *.tmp, or
    # feature_panel_staging.duckdb (re-derivable via the hybrid backfill,
    # same argument as the datastore/features/ exclusion).
    "normalised": [
        "alphalens.duckdb",
        "alphalens_fno_data.duckdb",
        "fundamental_raw_cache.duckdb",
        "pipeline_log.db",
        "scheduler.db",
        "macro_real_economy.parquet",
        "mf_holdings/**",
    ],
    "signals": ["signals.duckdb", "signals_fno_data.duckdb"],
    # models/ carries trained artifacts plus registry.json at the root and
    # one subdirectory per model. _gainer_experiment/ is experiment scratch
    # (1.3GB of checkpoint parquets that do not key on their own scope) and
    # _archive_pre_a38/ is superseded, so neither is named.
    "models": [
        "registry.json",
        "*.pt",
        "*.json",
        "multibagger/**",
        "signal_5d/**",
        "signal_21d/**",
        "signal_63d/**",
        "meta_labeler/**",
        "conformal/**",
        "hmm/**",
        "pnd_detector/**",
        "training_universe/**",
    ],
    "paper_trading": [],
    "config": [],
}

# Per-directory timeout — generous for a home-connection upload of a few
# GB; DuckDB WAL files mid-write are the only thing that could make a
# source directory large/unstable, and the pipeline never writes at the
# time this job is scheduled to run (see schedule_daily_backup's docstring).
_RCLONE_TIMEOUT_SECONDS = 1800


def _b2_remote(remote_name: str) -> str:
    """
    Build an rclone on-the-fly B2 remote path — no saved remote, no config
    file, no interactive `rclone config` step.

    [AS BUILT, 2026-08-10] Credentials are NOT inlined here any more.
    They used to be passed as connection-string parameters
    (`:b2,account=...,key=...:`), which put BACKBLAZE_APPLICATION_KEY into
    the rclone process's argv — world-readable via /proc for the whole
    duration of the nightly sync, so `ps` on a shared box leaked the key
    even though nothing was ever written to a log (SPEC-SEC-001 covers
    logs, but argv is the same exposure). They now travel in the
    subprocess environment as RCLONE_B2_ACCOUNT/RCLONE_B2_KEY, which
    rclone reads for its b2 backend — see _rclone_env().
    """
    return f":b2:{BACKBLAZE_BUCKET}/{BACKUP_REMOTE_PATH}/{remote_name}"


def _backup_enabled_now() -> bool:
    """
    Re-read BACKUP_ENABLED from the environment at call time.

    [AS BUILT, 2026-08-13] config.settings evaluates BACKUP_ENABLED once,
    at module import. The scheduler is a long-lived process that imports
    settings at startup, so flipping the flag in .env had NO effect on it
    — on 2026-08-10 the flag was set to true at 18:49 and the 22:30 job
    still logged "daily_backup skipped: BACKUP_ENABLED is False", because
    the value it saw was the one loaded at 18:31. Three nights of backups
    were silently skipped against a .env that read true, and the only
    symptom was a log line nobody was watching.

    Note that re-reading os.environ alone is NOT enough: config.settings
    calls load_dotenv() at import, which copies .env into os.environ once
    and never again, so a long-lived process's os.environ is just as stale
    as the imported constant. This re-parses the .env file itself
    (override=True) before reading, so the flag takes effect on the next
    scheduled run rather than on the next scheduler restart.

    The imported constant remains the fallback for callers that set it
    programmatically rather than through .env.
    """
    try:
        from dotenv import load_dotenv

        load_dotenv(override=True)
    except Exception as exc:  # dotenv missing or .env unreadable — fall back
        logger.debug(f"backup: could not reload .env ({exc}); using import-time value")
    raw = os.environ.get("BACKUP_ENABLED")
    if raw is None:
        return bool(BACKUP_ENABLED)
    return raw.strip().lower() == "true"


def _rclone_env() -> dict:
    """
    Environment for the rclone subprocess, carrying the B2 credentials
    out of argv and into env vars rclone picks up for its b2 backend.
    """
    env = dict(os.environ)
    env["RCLONE_B2_ACCOUNT"] = BACKBLAZE_KEY_ID or ""
    env["RCLONE_B2_KEY"] = BACKBLAZE_APPLICATION_KEY or ""
    return env


def run_backup(dry_run: bool = False) -> dict:
    """
    Sync each entry in _BACKUP_TARGETS to the B2 bucket, one rclone call
    per directory.

    Parameters
    ----------
    dry_run : bool
        Passed through as rclone's own --dry-run (lists what would transfer,
        transfers nothing).

    Returns
    -------
    dict
        {"synced": [...], "failed": [...]} — directory names, not paths.

    Raises
    ------
    RuntimeError
        If BACKUP_ENABLED is False, or any BACKBLAZE_* credential is unset
        (caller should treat this as "skip", not "failed" — see
        _execute_daily_backup_job in pipeline_scheduler.py).
    """
    if not _backup_enabled_now():
        raise RuntimeError(
            "BACKUP_ENABLED is False — set BACKBLAZE_KEY_ID/BACKBLAZE_APPLICATION_KEY/"
            "BACKBLAZE_BUCKET and BACKUP_ENABLED=true in .env once verified "
            "(see this script's module docstring for the one-time setup steps)."
        )
    if not (BACKBLAZE_KEY_ID and BACKBLAZE_APPLICATION_KEY and BACKBLAZE_BUCKET):
        raise RuntimeError(
            "BACKUP_ENABLED is true but one of BACKBLAZE_KEY_ID/BACKBLAZE_APPLICATION_KEY/"
            "BACKBLAZE_BUCKET is unset in .env — see this script's module docstring."
        )

    results = {"synced": [], "failed": []}
    for local_dir, remote_name in _BACKUP_TARGETS:
        if not local_dir.exists():
            logger.warning(f"backup: {local_dir} does not exist — skipping")
            continue

        cmd = ["rclone", "sync", str(local_dir), _b2_remote(remote_name), "--fast-list"]
        # Allowlist: name what ships, then refuse everything else.
        #
        # Uses --filter rather than --include/--exclude deliberately.
        # rclone warns that mixing --include with --exclude leaves the
        # parse order "indeterminate" — which for an allowlist is the
        # difference between shipping 5GB and shipping 23GB, decided by
        # something outside our control. --filter rules are evaluated
        # strictly in the order given, so "+ each allowed pattern" then a
        # final "- **" catch-all is deterministic and fails closed.
        includes = _BACKUP_INCLUDES.get(remote_name, [])
        if includes:
            for pattern in includes:
                cmd += ["--filter", f"+ {pattern}"]
            cmd += ["--filter", "- **"]
        if dry_run:
            cmd.append("--dry-run")

        logger.info(
            f"backup: syncing {local_dir} -> b2:{BACKBLAZE_BUCKET}/{BACKUP_REMOTE_PATH}/{remote_name}"
            + (f" (allowlist: {len(includes)} pattern(s))" if includes else " (whole directory)")
        )
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=_RCLONE_TIMEOUT_SECONDS,
                env=_rclone_env(),
            )
            if result.returncode != 0:
                logger.error(f"backup: rclone failed for {remote_name}: {result.stderr[-2000:]}")
                results["failed"].append(remote_name)
            else:
                logger.info(f"backup: {remote_name} synced successfully")
                results["synced"].append(remote_name)
        except subprocess.TimeoutExpired:
            logger.error(f"backup: rclone timed out for {remote_name} after {_RCLONE_TIMEOUT_SECONDS}s")
            results["failed"].append(remote_name)

    return results


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Back up AlphaLens's authoritative stores to Backblaze B2 via rclone")
    parser.add_argument("--dry-run", action="store_true", help="rclone --dry-run: list transfers without performing them")
    args = parser.parse_args()

    results = run_backup(dry_run=args.dry_run)
    print(f"\nSynced: {results['synced']}")
    print(f"Failed: {results['failed']}")
    if results["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
