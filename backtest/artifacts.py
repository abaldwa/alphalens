"""
backtest/artifacts.py

Lifecycle management for the files a backtest run produces.

WHY THIS EXISTS
---------------
A single 390-job sweep left 7.6 GB behind: 2.7 GB of reports, 1.6 GB of
orchestrator cache, 3.3 GB of restore scratch. None of it is cleaned by anything
today, and two of those directories were untracked-but-unignored until
2026-08-12, when a `git add -A` would have tried to commit 12.2 GB of DuckDB
snapshots. Disk is not the only cost: /tmp on this host is a 7.3 GB RAM-backed
tmpfs, so scratch written there consumes the same memory the workers need, and
this machine has already had systemd-oomd kill the scheduler, a 40-hour job, and
VS Code itself.

THE ONE RULE THAT MATTERS
-------------------------
Never delete an artifact a surviving report still points at.

Run reports embed `trade_log_path` as an absolute path, and the comparison
dataset and dashboard read trades back through it. Deleting a trade log whose
report survives turns a published result into a dangling pointer that fails at
read time, long after the cleanup that caused it. So every sweep here builds the
referenced set FIRST, from the reports actually on disk, and treats it as
untouchable regardless of age. `protected_by` on each candidate records which
report pinned it, so a surprising "kept" is explainable without re-running.

Deletion is opt-in (`--apply`); the default is a dry run, matching
scripts/purge_panel_staging.py and scripts/purge_technical_momentum_trades.py.
"""

from __future__ import annotations

import json
import logging
import re
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
REPORTS_DIR = REPO_ROOT / "backtest" / "reports"
CACHE_DIR = REPO_ROOT / "backtest" / "cache"

# Run reports are the durable record of a sweep; trade logs are the durable
# record of its trades. Everything else in this module is reproducible.
_RUN_REPORT_GLOB = "orchestrator_*.json"


@dataclass
class Candidate:
    """One file or directory cleanup could remove."""

    path: Path
    kind: str
    bytes: int
    reason: str
    protected_by: Optional[str] = None

    @property
    def deletable(self) -> bool:
        return self.protected_by is None


@dataclass
class Sweep:
    """The result of scanning. Nothing has been deleted yet."""

    candidates: List[Candidate] = field(default_factory=list)

    def deletable(self) -> List[Candidate]:
        return [c for c in self.candidates if c.deletable]

    def protected(self) -> List[Candidate]:
        return [c for c in self.candidates if not c.deletable]

    def bytes_reclaimable(self) -> int:
        return sum(c.bytes for c in self.deletable())

    def by_kind(self) -> Dict[str, List[Candidate]]:
        out: Dict[str, List[Candidate]] = {}
        for c in self.candidates:
            out.setdefault(c.kind, []).append(c)
        return out


def _size_of(path: Path) -> int:
    try:
        if path.is_dir():
            return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())
        return path.stat().st_size
    except OSError:
        return 0


def _age_days(path: Path) -> float:
    try:
        return (time.time() - path.stat().st_mtime) / 86400.0
    except OSError:
        return 0.0


def referenced_paths(reports_dir: Path = REPORTS_DIR) -> Dict[str, str]:
    """Absolute path -> the report filename that pins it.

    Built from the reports currently on disk, so quarantining a superseded
    report (as the tax-fix re-run did) correctly releases its trade log for
    cleanup, while a live report keeps pinning its own. Reports nested in
    subdirectories count: backtest/reports/superseded_by_taxfix_20260812/ holds
    real runs whose logs must not be collected while those reports are kept.
    """
    pinned: Dict[str, str] = {}
    if not reports_dir.exists():
        return pinned
    for report in reports_dir.rglob(_RUN_REPORT_GLOB):
        try:
            data = json.loads(report.read_text())
        except (OSError, json.JSONDecodeError):
            # An unreadable report is a reason to protect MORE, not less: we
            # cannot know what it references, so we cannot safely collect
            # anything on its behalf. Log and move on.
            logger.warning("artifacts: unreadable report %s — its references cannot be resolved", report.name)
            continue
        for key in ("trade_log_path", "trade_book_path"):
            value = data.get(key)
            if value:
                pinned[str(Path(value).resolve())] = report.name
    return pinned


def _run_id_of(path: Path) -> Optional[str]:
    m = re.search(r"(orch_[a-z]+_\d{8}_\d{6}_[0-9a-f]+)", path.name)
    return m.group(1) if m else None


def scan(
    *,
    reports_dir: Path = REPORTS_DIR,
    cache_dir: Path = CACHE_DIR,
    min_age_days: float = 0.0,
    include_trade_books: bool = True,
    include_cache: bool = True,
    include_orphan_logs: bool = True,
    extra_scratch: Iterable[Path] = (),
) -> Sweep:
    """Find reclaimable artifacts. Pure — deletes nothing."""
    pinned = referenced_paths(reports_dir)
    sweep = Sweep()

    def consider(path: Path, kind: str, reason: str) -> None:
        if not path.exists():
            return
        if _age_days(path) < min_age_days:
            return
        key = str(path.resolve())
        sweep.candidates.append(
            Candidate(
                path=path, kind=kind, bytes=_size_of(path), reason=reason,
                protected_by=pinned.get(key),
            )
        )

    # --- trade books -------------------------------------------------------
    # The orchestrator writes BOTH trade_book_*.csv and trade_log_*.csv per run.
    # Only trade_log_ is consumed: scripts/load_trade_books_to_db.py's _RUN_ID_RE
    # and _INSERT both key on it, and so does the incremental loader. Measured
    # 2026-08-12: 864 trade_book files, 1.0 GB, nothing reading them. They are
    # still protected if any report names one via trade_book_path.
    if include_trade_books:
        for path in sorted(reports_dir.glob("trade_book_*.csv")):
            consider(path, "trade_book", "duplicate of trade_log_*.csv; no reader in the codebase")

    # --- orphaned trade logs ----------------------------------------------
    # Logs whose run report is gone — e.g. runs purged from the DB, or a sweep
    # whose reports were deleted. Without a report they can never be loaded or
    # interpreted, because the report holds the run's identity and config.
    if include_orphan_logs:
        for path in sorted(reports_dir.glob("trade_log_*.csv")):
            if str(path.resolve()) not in pinned:
                run_id = _run_id_of(path) or "unknown"
                consider(path, "orphan_trade_log", f"no surviving report references run {run_id}")

    # --- orchestrator cache ------------------------------------------------
    # Fully reproducible: OHLCV snapshots, per-ticker caches, staging scratch.
    if include_cache and cache_dir.exists():
        for child in sorted(cache_dir.iterdir()):
            consider(child, "cache", "regenerable orchestrator cache")

    # --- queue progress ----------------------------------------------------
    # Only meaningful while its queue is resumable.
    for path in sorted(reports_dir.glob("queue_progress_*")):
        consider(path, "queue_progress", "queue driver finished; progress file no longer resumable")

    # --- explicit scratch --------------------------------------------------
    for path in extra_scratch:
        consider(Path(path), "scratch", "explicitly nominated scratch")

    return sweep


def apply(sweep: Sweep, *, dry_run: bool = True) -> int:
    """Delete the deletable candidates. Returns bytes actually reclaimed.

    Protected candidates are never touched, even if passed in explicitly —
    the protection decision belongs to the scan, not to the caller.
    """
    reclaimed = 0
    for candidate in sweep.deletable():
        if dry_run:
            reclaimed += candidate.bytes
            continue
        try:
            if candidate.path.is_dir():
                shutil.rmtree(candidate.path)
            else:
                candidate.path.unlink()
            reclaimed += candidate.bytes
        except OSError as exc:
            # A file we cannot delete is not a failure of the sweep; report and
            # continue so one permission problem cannot strand the rest.
            logger.warning("artifacts: could not remove %s — %s", candidate.path, exc)
    return reclaimed


class RunScratch:
    """Per-run scratch directory that removes itself.

    Use for anything a single run needs and nothing outside it should read.
    Deliberately NOT under /tmp: that path is a 7.3 GB RAM-backed tmpfs on this
    host, so scratch written there competes with the workers for the memory that
    has already caused OOM kills here. Defaults under backtest/cache/, which is
    disk-backed and gitignored.

    `keep_on_error=True` preserves the directory when the body raises, because
    the moment you most want the intermediate files is the moment something
    failed.
    """

    def __init__(self, name: str, *, root: Path = CACHE_DIR, keep_on_error: bool = True):
        self.path = Path(root) / "scratch" / name
        self._keep_on_error = keep_on_error

    def __enter__(self) -> Path:
        self.path.mkdir(parents=True, exist_ok=True)
        return self.path

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type is not None and self._keep_on_error:
            logger.warning("artifacts: keeping scratch %s for diagnosis after %s", self.path, exc_type.__name__)
            return False
        shutil.rmtree(self.path, ignore_errors=True)
        return False


def format_sweep(sweep: Sweep) -> str:
    """Human-readable summary, used by the CLI and safe to log."""
    lines: List[str] = []
    by_kind = sweep.by_kind()
    for kind in sorted(by_kind):
        items = by_kind[kind]
        deletable = [c for c in items if c.deletable]
        protected = [c for c in items if not c.deletable]
        total = sum(c.bytes for c in deletable)
        lines.append(
            f"  {kind:<20} {len(deletable):>5} reclaimable  {total / 1048576:>9.1f} MB"
            + (f"   ({len(protected)} protected)" if protected else "")
        )
        if deletable:
            lines.append(f"      reason: {deletable[0].reason}")
    lines.append(f"  {'TOTAL':<20} {len(sweep.deletable()):>5} files       "
                 f"{sweep.bytes_reclaimable() / 1048576:>9.1f} MB")
    return "\n".join(lines)
