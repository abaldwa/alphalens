"""
Git Provenance — captures the exact code version a backtest ran under.

WHY THIS EXISTS: strategy_id (metrics/nomenclature.py) names a run's
CONFIGURATION (band, top_n, lookback, ...), never its CODE. If a
strategy's ranking logic changes mid-project, two runs sharing an
identical strategy_id could have executed different code — currently
indistinguishable from each other. Explicit user requirement (2026-09-04):
"persist ... the version of the strategy, in case we change the strategy
mid-way." This module is what BacktestResult.source_commit
(backtesting/result.py) is actually populated from at run_native() time.

Dirty-tree runs are NOT rejected — a backtest run during active
development is common and legitimate — but `is_dirty=True` is always
recorded so a later comparison of two same-strategy_id runs can
immediately tell "these differ only in config" apart from "the commit
was clean-vs-dirty, treat any metric difference with more suspicion."
"""

from dataclasses import dataclass
from functools import lru_cache
import subprocess


@dataclass(frozen=True)
class GitProvenance:
    commit_hash: str        # full 40-char SHA, or "unknown" if git is unavailable
    commit_short: str       # 8-char short form for display
    is_dirty: bool          # True if the working tree had uncommitted changes at run time


@lru_cache(maxsize=1)
def get_source_commit() -> GitProvenance:
    """
    Cached per-process — a single backtest run (even one that executes
    thousands of jobs in a queue) always reports the commit the PROCESS
    started under, not a re-check per job. A long-running queue that
    outlives a `git commit` mid-run should be restarted for the new
    commit to be reflected, not silently switch identity partway through
    one execution.
    """
    try:
        commit_hash = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, timeout=5, check=True,
        ).stdout.strip()
        status = subprocess.run(
            ["git", "status", "--porcelain"], capture_output=True, text=True, timeout=5, check=True,
        ).stdout
        is_dirty = bool(status.strip())
        return GitProvenance(commit_hash=commit_hash, commit_short=commit_hash[:8], is_dirty=is_dirty)
    except Exception:
        return GitProvenance(commit_hash="unknown", commit_short="unknown", is_dirty=False)
