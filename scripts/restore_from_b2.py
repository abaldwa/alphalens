"""
scripts/restore_from_b2.py

Phase: 3.x (Disaster Recovery)
Specs: SPEC-SYS-005 (Storage Budgets), SPEC-SEC-001 (credentials from env only)
Owner: Platform / Ops

The read side of scripts/backup_to_b2.py: pulls the authoritative stores
back down from Backblaze B2 via `rclone`, using the same on-the-fly
connection string (no `rclone config`, no saved remote, no OAuth).

Why this exists
---------------
A backup that has never been restored is a hypothesis, not a backup. The
2026-08-09 ops review found the B2 bucket held a single sync from
2026-07-04 that nobody had ever read back — the archive could have been
truncated, mis-pathed, or storing WAL-less DuckDB files that refuse to
open, and there was no way to know.

Safety posture (deliberately the mirror image of backup_to_b2.py)
-----------------------------------------------------------------
Backup is a `sync` that runs unattended every night. Restore is the
opposite kind of operation: rare, manual, and capable of destroying the
live datastore if it points the wrong way. So the defaults invert.

  1. DRY-RUN BY DEFAULT. Writing anything requires --confirm. Running
     this script with no flags tells you what would happen and exits.
  2. RESTORES TO A STAGING DIR, NOT OVER THE LIVE STORE. The default
     target is datastore/restore_staging/<timestamp>/. Promoting a
     restored file over the live one stays a deliberate human `mv`.
     --in-place exists but additionally requires --i-know-this-overwrites.
  3. `rclone copy`, NEVER `sync`. sync in this direction would delete
     local files absent from the remote — i.e. a stale backup could wipe
     newer local data. copy only ever adds/overwrites.
  4. REFUSES TO RUN AGAINST A LOCKED DB. DuckDB is single-writer; the
     scheduler or a backtest queue holding the lock means a restore
     would either fail halfway or corrupt. Checked before any transfer.
  5. VERIFIES WHAT IT PULLED. Every restored *.duckdb is opened read-only
     and row-counted before the script reports success.

Note that BACKUP_ENABLED does NOT gate this script. That flag exists so a
fresh checkout does not fail a *scheduled* backup against unset creds;
restore is manual, and you may well need it precisely when backups have
been turned off.

Usage
-----
    # What is in the archive, and how stale is it?
    .venv/bin/python3 scripts/restore_from_b2.py --list

    # What would a restore do? (default: dry-run, writes nothing)
    .venv/bin/python3 scripts/restore_from_b2.py

    # Actually pull, into datastore/restore_staging/<timestamp>/
    .venv/bin/python3 scripts/restore_from_b2.py --confirm

    # Just one store
    .venv/bin/python3 scripts/restore_from_b2.py --only normalised --confirm

    # Verify an existing restore without re-downloading
    .venv/bin/python3 scripts/restore_from_b2.py --verify-only <dir>
"""

import argparse
import datetime as dt
import logging
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (  # noqa: E402
    BACKBLAZE_APPLICATION_KEY,
    BACKBLAZE_BUCKET,
    BACKBLAZE_KEY_ID,
    BACKUP_REMOTE_PATH,
    CONFIG_DIR,
    MODELS_DIR,
    NORMALISED_DIR,
    PROJECT_ROOT,
    SIGNALS_DIR,
)

logger = logging.getLogger(__name__)

# Mirrors _BACKUP_TARGETS in backup_to_b2.py: (remote sub-path, live local
# dir it corresponds to). Kept as an independent literal rather than
# imported so that a future edit to the backup list can never silently
# repoint an in-flight restore at the wrong directory.
_RESTORE_TARGETS: List[Tuple[str, Path]] = [
    ("normalised", NORMALISED_DIR),
    ("signals", SIGNALS_DIR),
    ("models", MODELS_DIR),
    ("paper_trading", PROJECT_ROOT / "paper_trading"),
    ("config", CONFIG_DIR),
]

_RCLONE_TIMEOUT_SECONDS = 3600  # restores run over the same home link, but cold


def _b2_remote(remote_name: str) -> str:
    """
    Build the same on-the-fly rclone B2 connection string backup_to_b2.py
    uses. Never log the result — it embeds BACKBLAZE_APPLICATION_KEY in
    plain text (SPEC-SEC-001).
    """
    return (
        f":b2,account={BACKBLAZE_KEY_ID},key={BACKBLAZE_APPLICATION_KEY}:"
        f"{BACKBLAZE_BUCKET}/{BACKUP_REMOTE_PATH}/{remote_name}"
    )


def _require_credentials() -> None:
    if not (BACKBLAZE_KEY_ID and BACKBLAZE_APPLICATION_KEY and BACKBLAZE_BUCKET):
        raise RuntimeError(
            "One of BACKBLAZE_KEY_ID/BACKBLAZE_APPLICATION_KEY/BACKBLAZE_BUCKET is "
            "unset in .env — restore cannot authenticate. See scripts/backup_to_b2.py's "
            "module docstring for the one-time setup steps."
        )


def _run_rclone(args: List[str], timeout: int = _RCLONE_TIMEOUT_SECONDS) -> subprocess.CompletedProcess:
    return subprocess.run(["rclone", *args], capture_output=True, text=True, timeout=timeout)


def describe_archive() -> Dict[str, dict]:
    """
    Report size, object count and newest-object age per remote directory,
    without transferring anything.

    The age is the number that actually matters operationally: a backup
    job that silently stopped (BACKUP_ENABLED left false, scheduler dead)
    looks completely healthy from the bucket's file listing alone.
    """
    _require_credentials()
    out: Dict[str, dict] = {}
    for remote_name, _ in _RESTORE_TARGETS:
        entry: dict = {"objects": 0, "bytes": 0, "newest": None, "error": None}
        size = _run_rclone(["size", _b2_remote(remote_name), "--json"], timeout=300)
        if size.returncode != 0:
            entry["error"] = size.stderr.strip()[-300:]
            out[remote_name] = entry
            continue
        try:
            import json

            parsed = json.loads(size.stdout)
            entry["objects"] = parsed.get("count", 0)
            entry["bytes"] = parsed.get("bytes", 0)
        except (ValueError, KeyError) as exc:
            entry["error"] = f"could not parse rclone size output: {exc}"

        listing = _run_rclone(["lsl", _b2_remote(remote_name)], timeout=300)
        if listing.returncode == 0:
            stamps = []
            for line in listing.stdout.splitlines():
                parts = line.split(maxsplit=3)
                if len(parts) >= 3:
                    try:
                        stamps.append(dt.datetime.fromisoformat(f"{parts[1]} {parts[2].split('.')[0]}"))
                    except ValueError:
                        continue
            if stamps:
                entry["newest"] = max(stamps)
        out[remote_name] = entry
    return out


def _duckdb_write_locked(db_path: Path) -> Optional[str]:
    """
    Return a human-readable reason if `db_path` cannot be opened for
    writing, else None.

    A read-only handle succeeding proves nothing: DuckDB permits many
    concurrent readers, and the 2026-08 backtest queues hold exactly that
    kind of handle. Only a write handle tells us a restore could safely
    replace the file.
    """
    if not db_path.exists():
        return None
    try:
        import duckdb

        con = duckdb.connect(str(db_path))  # read-write
        con.close()
        return None
    except Exception as exc:  # duckdb raises IOException subclasses here
        return f"{type(exc).__name__}: {str(exc)[:200]}"


def preflight(in_place: bool) -> List[str]:
    """
    Collect blocking problems before any bytes move. Returns a list of
    human-readable blockers; empty means clear to proceed.

    Only meaningful for --in-place: a staging restore writes to a fresh
    directory and cannot disturb a live reader.
    """
    blockers: List[str] = []
    if not in_place:
        return blockers

    for db in sorted(NORMALISED_DIR.glob("*.duckdb")):
        reason = _duckdb_write_locked(db)
        if reason:
            blockers.append(f"{db.name} is write-locked ({reason}) — stop the scheduler / backtest queue first")
    return blockers


def verify_restore(target_root: Path, memory_limit: str = "512MB", threads: int = 2) -> Dict[str, dict]:
    """
    Open every restored *.duckdb read-only and count its tables/rows.

    This is the step that converts "rclone exited 0" into "the archive
    actually contains a usable database" — a truncated or partially
    uploaded DuckDB file fails here, not at transfer time.

    Runs under an explicit memory_limit/threads cap: verification is
    metadata-only (duckdb_tables() reads the catalog, not the data), so
    it needs almost nothing, and an uncapped DuckDB will happily size its
    buffer pool against total RAM on a box already near its OOM ceiling.
    """
    results: Dict[str, dict] = {}
    for db in sorted(target_root.rglob("*.duckdb")):
        entry: dict = {"tables": None, "total_rows": None, "error": None}
        try:
            import duckdb

            con = duckdb.connect(
                str(db),
                read_only=True,
                config={"memory_limit": memory_limit, "threads": threads},
            )
            rows = con.execute(
                "select table_name, estimated_size from duckdb_tables()"
            ).fetchall()
            entry["tables"] = len(rows)
            entry["total_rows"] = sum(r[1] or 0 for r in rows)
            con.close()
        except Exception as exc:
            entry["error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
        results[str(db.relative_to(target_root))] = entry
    return results


def run_restore(
    target_root: Path,
    only: Optional[List[str]] = None,
    dry_run: bool = True,
    in_place: bool = False,
    transfers: int = 1,
    bwlimit: Optional[str] = None,
    buffer_size: str = "16M",
) -> dict:
    """
    Pull each selected remote directory down with `rclone copy`.

    Parameters
    ----------
    target_root : Path
        Staging root to restore into. Ignored when in_place is True, where
        each target's real live directory is used instead.
    only : list of str, optional
        Restrict to these remote names (e.g. ["normalised"]). None = all.
    dry_run : bool
        Passed through as rclone's own --dry-run. Defaults True.
    in_place : bool
        Restore over the live directories rather than into staging.
    transfers : int
        Parallel rclone transfers. Defaults to 1 (rclone's own default is
        4) so a restore competing with a live backtest queue stays polite.
    bwlimit : str, optional
        rclone --bwlimit value, e.g. "8M". None = unlimited.
    buffer_size : str
        Per-transfer in-memory buffer. Lowered from rclone's 16M default
        only matters when transfers > 1.

    Returns
    -------
    dict
        {"restored": [...], "failed": [...], "skipped": [...], "root": str}
    """
    _require_credentials()

    selected = [t for t in _RESTORE_TARGETS if only is None or t[0] in only]
    if only:
        unknown = set(only) - {t[0] for t in _RESTORE_TARGETS}
        if unknown:
            raise ValueError(f"unknown restore target(s): {sorted(unknown)}")

    results: dict = {"restored": [], "failed": [], "skipped": [], "root": str(target_root)}

    for remote_name, live_dir in selected:
        dest = live_dir if in_place else (target_root / remote_name)
        if not dry_run:
            dest.mkdir(parents=True, exist_ok=True)

        # `copy`, never `sync`: sync would delete local files missing from
        # the remote, letting a stale archive destroy newer local data.
        cmd = ["copy", _b2_remote(remote_name), str(dest), "--fast-list"]
        # Restores routinely run while backtests//the scheduler are still
        # working, on a 14-core box that systemd-oomd has killed processes
        # on before. Default to a single serialised transfer rather than
        # rclone's 4-way parallel + multi-thread streams.
        cmd += [
            "--transfers", str(transfers),
            "--checkers", str(max(1, transfers)),
            "--multi-thread-streams", "0",
            "--buffer-size", buffer_size,
            "--stats", "30s",
            "--stats-one-line",
        ]
        if bwlimit:
            cmd += ["--bwlimit", bwlimit]
        if dry_run:
            cmd.append("--dry-run")

        logger.info(
            f"restore: b2:{BACKBLAZE_BUCKET}/{BACKUP_REMOTE_PATH}/{remote_name} -> {dest}"
            f"{' [DRY RUN]' if dry_run else ''}"
        )
        try:
            result = _run_rclone(cmd)
            if result.returncode != 0:
                logger.error(f"restore: rclone failed for {remote_name}: {result.stderr[-2000:]}")
                results["failed"].append(remote_name)
            else:
                results["restored"].append(remote_name)
        except subprocess.TimeoutExpired:
            logger.error(f"restore: rclone timed out for {remote_name} after {_RCLONE_TIMEOUT_SECONDS}s")
            results["failed"].append(remote_name)

    return results


def _print_archive(info: Dict[str, dict]) -> None:
    now = dt.datetime.now()
    print(f"\nB2 archive: {BACKBLAZE_BUCKET}/{BACKUP_REMOTE_PATH}\n")
    print(f"{'store':<16}{'objects':>9}{'size':>12}   {'newest object':<21}{'age'}")
    print("-" * 76)
    for name, e in info.items():
        if e["error"]:
            print(f"{name:<16}{'ERROR':>9}   {e['error'][:40]}")
            continue
        gb = e["bytes"] / (1024 ** 3)
        newest = e["newest"]
        age = f"{(now - newest).days}d" if newest else "-"
        stamp = newest.strftime("%Y-%m-%d %H:%M") if newest else "-"
        print(f"{name:<16}{e['objects']:>9}{gb:>11.2f}G   {stamp:<21}{age}")
    stale = [n for n, e in info.items() if e["newest"] and (now - e["newest"]).days > 2]
    if stale:
        print(f"\n  WARNING: {', '.join(stale)} older than 2 days — the nightly backup is not running.")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="Restore AlphaLens's authoritative stores from Backblaze B2 via rclone"
    )
    parser.add_argument("--list", action="store_true", help="show archive contents/staleness and exit")
    parser.add_argument("--confirm", action="store_true", help="actually transfer (default is dry-run)")
    parser.add_argument("--only", nargs="+", metavar="STORE",
                        help="restrict to these stores: normalised signals models paper_trading config")
    parser.add_argument("--into", type=Path, default=None,
                        help="staging dir to restore into (default: datastore/restore_staging/<timestamp>)")
    parser.add_argument("--in-place", action="store_true",
                        help="restore over the LIVE directories instead of staging")
    parser.add_argument("--i-know-this-overwrites", action="store_true",
                        help="required companion to --in-place")
    parser.add_argument("--verify-only", type=Path, metavar="DIR",
                        help="verify DuckDB files under DIR without downloading")
    parser.add_argument("--transfers", type=int, default=1,
                        help="parallel rclone transfers (default 1, polite alongside live jobs)")
    parser.add_argument("--bwlimit", default=None, metavar="RATE",
                        help="cap bandwidth, e.g. 8M (default: unlimited)")
    parser.add_argument("--memory-limit", default="512MB",
                        help="DuckDB memory cap during verification (default 512MB)")
    args = parser.parse_args()

    if args.verify_only:
        report = verify_restore(args.verify_only, memory_limit=args.memory_limit)
        if not report:
            print(f"No *.duckdb found under {args.verify_only}")
            sys.exit(1)
        bad = False
        for name, e in report.items():
            if e["error"]:
                bad = True
                print(f"  FAIL  {name}: {e['error']}")
            else:
                print(f"  OK    {name}: {e['tables']} tables, ~{e['total_rows']:,} rows")
        sys.exit(1 if bad else 0)

    if args.list:
        _print_archive(describe_archive())
        return

    if args.in_place and not args.i_know_this_overwrites:
        print("--in-place overwrites the live datastore. Re-run with --i-know-this-overwrites "
              "to confirm you mean it, or drop --in-place to restore into staging.")
        sys.exit(2)

    blockers = preflight(in_place=args.in_place)
    if blockers:
        print("\nPreflight failed:")
        for b in blockers:
            print(f"  - {b}")
        sys.exit(3)

    target_root = args.into or (
        PROJECT_ROOT / "datastore" / "restore_staging" / dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    )

    _print_archive(describe_archive())

    results = run_restore(
        target_root=target_root,
        only=args.only,
        dry_run=not args.confirm,
        in_place=args.in_place,
        transfers=args.transfers,
        bwlimit=args.bwlimit,
    )

    print(f"\nRestored: {results['restored']}")
    print(f"Failed:   {results['failed']}")
    if not args.confirm:
        print("\nDRY RUN — nothing was written. Re-run with --confirm to transfer.")
        return

    print(f"\nVerifying DuckDB files under {target_root} ...")
    report = verify_restore(target_root if not args.in_place else NORMALISED_DIR,
                            memory_limit=args.memory_limit)
    for name, e in report.items():
        if e["error"]:
            print(f"  FAIL  {name}: {e['error']}")
        else:
            print(f"  OK    {name}: {e['tables']} tables, ~{e['total_rows']:,} rows")

    if results["failed"] or any(e["error"] for e in report.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
