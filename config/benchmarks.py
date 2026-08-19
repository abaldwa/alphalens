"""
config/benchmarks.py

Owner: Platform / Benchmarks (A97, A98)
Consumers: datastore/api/routers/indices.py (GET /api/v1/indices), the
frontend benchmark selector, backtest benchmark-curve construction.

Which index a strategy should be compared against, and whether that
comparison is honest over a given window.

Two facts about index_ohlcv make this more than a list of names:

1. NOT ALL HISTORY IS EQUAL. NSE's historical PR exports back-compute an
   index before it launched, publishing a Close with no Open/High/Low. The
   boundary is exact and verifiable in the data -- the first date with a
   non-null Open is the launch:

       Nifty Microcap 250    live 2022-01-10   3,472 back-computed sessions
       Nifty Midcap 150      live 2019-01-14   2,731
       Nifty Smallcap 250    live 2019-01-14   2,731
       Nifty Smallcap 50     live 2019-01-14   2,731
       Nifty Smallcap 100    live 2011-10-03     928
       Nifty Midcap 100 / Midcap 50 / Next 50 / Nifty 50 / 100 / 500: live throughout

   A 2009-2026 backtest benchmarked against Nifty Microcap 250 is therefore
   compared against fourteen years of retrospectively computed index. That is
   NSE's own computation and defensible, but it is not the same claim as a
   comparison against a series that traded, and a report must be able to say
   which it is rather than presenting both as equivalent.

2. DAILY COVERAGE IS EARNED, NOT ASSUMED. An index only stays current if the
   daily pipeline actually captures it. Two indices were silently dropped for
   months at a time (Nifty 100 until 2026-08-09; Midcap 100 and Smallcap 100
   would have been, since NSE publishes them as "NIFTY Midcap 100" while the
   filter matched exactly). So freshness is measured from the data, not
   declared here.

The per-band defaults below answer "what should this strategy be compared
against" -- comparing a rank 150-200 momentum band to Nifty 500 flatters or
punishes it for reasons that have nothing to do with the strategy.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

# Broad-market indices: the default when a strategy's universe is not
# size-scoped.
BROAD_INDICES = ["Nifty 50", "Nifty 100", "Nifty 500"]

# Size indices, ordered small -> large.
SIZE_INDICES = [
    "Nifty Microcap 250",
    "Nifty Smallcap 250",
    "Nifty Smallcap 100",
    "Nifty Smallcap 50",
    "Nifty Midcap 150",
    "Nifty Midcap 100",
    "Nifty Midcap 50",
    "Nifty Next 50",
]

# Momentum's market-cap rank bands (features/momentum_universe.py::RANK_BANDS)
# mapped to the index that actually represents what they hold. Nifty Next 50
# is ranks ~51-100 by construction, which is why band 2 maps to it exactly.
#
# The bands were redrawn 2026-08-14 from five (stopping at rank 200) to seven
# (spanning 1-800): 1-50, 51-100, 101-150, 151-200, 200-300, 300-500, 500-800.
# Band ids 1-4 kept their historical numbers; id 5 (100-200) vanished, and ids
# 6/7/8 were added. Below maps each against the NSE size-tier that best matches
# what it holds (standard Nifty construction: ranks 101-250 midcap, 251-500
# smallcap, 501+ microcap):
#   200-300 splits the midcap/smallcap seam at 250, so it takes Smallcap 250 --
#    the next distinct tier below the Midcap 150 that bands 3/4 already claim.
RANK_BAND_BENCHMARKS: Dict[int, str] = {
    1: "Nifty 50",             # ranks 1-50
    2: "Nifty Next 50",        # ranks 51-100
    3: "Nifty Midcap 100",     # ranks 101-150
    4: "Nifty Midcap 150",     # ranks 151-200
    # 2026-08-19: renumbered from 6/7/8 to 5/6/7 with RANK_BANDS. Ranges
    # unchanged; only the ids moved.
    5: "Nifty Smallcap 250",   # ranks 201-300 (straddles the 250 midcap/smallcap seam)
    6: "Nifty Smallcap 250",   # ranks 301-500 (the smallcap tier)
    7: "Nifty Microcap 250",   # ranks 501-800 (the microcap tier)
}

# The regime index is a separate decision from the benchmark index (A98).
# Regime detection wants a broad, long-history series; a benchmark wants
# whatever the strategy actually holds. Conflating them means changing a
# report's comparison also changes which regimes the strategy traded in.
DEFAULT_REGIME_INDEX = "Nifty 500"
DEFAULT_BENCHMARK_INDEX = "Nifty 500"

# An index whose last row is older than this is not being maintained by the
# daily pipeline, whatever the intent. Generous enough to survive a long
# weekend plus a holiday without false alarms.
STALE_AFTER_DAYS = 7


@dataclass(frozen=True)
class IndexCoverage:
    """What index_ohlcv actually holds for one index."""

    index_name: str
    first_date: Optional[date]
    last_date: Optional[date]
    n_rows: int
    # First date with a real Open -- i.e. when the index went live, as
    # opposed to when NSE's back-computation starts.
    live_from: Optional[date]
    n_backcomputed: int

    @property
    def is_fresh(self) -> bool:
        """Being updated by the daily pipeline."""
        if self.last_date is None:
            return False
        return (date.today() - self.last_date).days <= STALE_AFTER_DAYS

    @property
    def has_backcomputed_history(self) -> bool:
        return self.n_backcomputed > 0

    def covers(self, start: date, end: date) -> bool:
        """Whether the stored series spans a window.

        The end is checked with STALE_AFTER_DAYS of slack. An index is
        published with a normal reporting lag -- Nifty 500's last row is
        routinely a day or two behind a strategy's last trading day -- and
        without the slack every index would report "no data covering" any
        window ending today, which is both false and unhelpful. A genuinely
        abandoned series still fails, because its lag is months not days.
        """
        if self.first_date is None or self.last_date is None:
            return False
        if self.first_date > start:
            return False
        return (end - self.last_date).days <= STALE_AFTER_DAYS

    def is_live_over(self, start: date, end: date) -> bool:
        """Whether the window is entirely inside the traded (non
        back-computed) history -- the stronger claim."""
        if not self.covers(start, end):
            return False
        return self.live_from is not None and self.live_from <= start

    def comparison_caveat(self, start: date, end: date) -> Optional[str]:
        """One sentence naming why a comparison over this window is weaker
        than it looks, or None if it is sound. Returned to the UI so the
        caveat travels with the number instead of living in a doc."""
        # Order matters: a stalled index also fails covers(), but "it stopped
        # updating" is the actionable statement, where "no data covering this
        # window" would send someone looking for missing history that is not
        # the problem.
        if not self.is_fresh:
            return (
                f"{self.index_name} was last updated {self.last_date} and is not "
                f"being refreshed by the daily pipeline."
            )
        if not self.covers(start, end):
            return (
                f"{self.index_name} has no data covering "
                f"{start.isoformat()}..{end.isoformat()} "
                f"(available: {self.first_date}..{self.last_date})."
            )
        if not self.is_live_over(start, end) and self.live_from:
            return (
                f"{self.index_name} was launched {self.live_from.isoformat()}; "
                f"values before that are NSE's retrospective back-computation, "
                f"not a series that traded."
            )
        return None


def load_coverage(conn: Any) -> Dict[str, IndexCoverage]:
    """Measure coverage from index_ohlcv. Nothing here is declared -- an
    index is fresh because rows are arriving, not because it appears in a
    list."""
    rows = conn.execute(
        """
        SELECT index_name,
               min(date) AS first_date,
               max(date) AS last_date,
               count(*) AS n_rows,
               min(CASE WHEN open IS NOT NULL THEN date END) AS live_from,
               sum(CASE WHEN open IS NULL THEN 1 ELSE 0 END) AS n_backcomputed
        FROM index_ohlcv
        GROUP BY index_name
        ORDER BY index_name
        """
    ).fetchall()
    return {
        r[0]: IndexCoverage(
            index_name=r[0],
            first_date=_as_date(r[1]),
            last_date=_as_date(r[2]),
            n_rows=int(r[3]),
            live_from=_as_date(r[4]),
            n_backcomputed=int(r[5] or 0),
        )
        for r in rows
    }


def usable_benchmarks(
    coverage: Dict[str, IndexCoverage],
    *,
    require_fresh: bool = True,
    start: Optional[date] = None,
    end: Optional[date] = None,
    require_live: bool = True,
) -> List[str]:
    """Indices fit to be offered as a benchmark, for a given window.

    require_fresh: only indices the daily pipeline actually keeps current. An
        index that stopped updating would otherwise produce a benchmark CAGR
        measured over a shorter period than the strategy, with no visible sign.

    start/end + require_live: the window-dependent rule. An index is offered
        only if it was actually TRADING across the whole window, not merely
        present in the table -- NSE back-computes a series before it launched,
        and those rows are indistinguishable from live ones apart from a null
        Open. Which indices qualify therefore depends on the window chosen:

            2020-2026  Smallcap 250 (live 2019-01-14) qualifies
            2009-2026  it does not; the first ten years never traded

        With require_live=False the back-computed span is allowed and the
        caller is expected to surface comparison_caveat() alongside the
        number. Without a window, this degrades to the freshness check only.
    """
    out = []
    for name in BROAD_INDICES + SIZE_INDICES:
        cov = coverage.get(name)
        if cov is None or cov.n_rows == 0:
            continue
        if require_fresh and not cov.is_fresh:
            continue
        if start and end:
            if not cov.covers(start, end):
                continue
            if require_live and not cov.is_live_over(start, end):
                continue
        out.append(name)
    return out


def benchmark_options(
    coverage: Dict[str, IndexCoverage],
    start: date,
    end: date,
    *,
    preferred: Optional[str] = None,
) -> Dict[str, object]:
    """Everything the UI benchmark selector needs for one window.

    Returns the indices that traded across the whole window ("live"), those
    that only reach it via back-computed history ("backcomputed", offered but
    caveated), a recommendation, and — when the size-matched index did not
    exist across the window — the reason it was not used.

    Fallback rule (A104, decided 2026-08-13): when the preferred index did not
    trade across the whole window, fall back to Nifty 500 and SAY SO. The
    substitution is deliberately visible rather than silent: Nifty 500 is a
    broad index, so a small-cap strategy compared against it is being measured
    partly on the size spread, and a reader has to know that is what they are
    looking at.

    The alternative considered and rejected was ranking on the back-computed
    series with a caveat. A recommendation is a deploy-or-not decision, and
    "beat an index that did not exist for ten of the seventeen years" is not
    evidence for one.
    """
    live = usable_benchmarks(coverage, start=start, end=end, require_live=True)
    all_covering = usable_benchmarks(coverage, start=start, end=end, require_live=False)
    backcomputed = [n for n in all_covering if n not in live]

    fallback_reason: Optional[str] = None
    # Optional because every fallback below bottoms out at `live[0] if live
    # else None` -- with no live index at all there is nothing to recommend.
    recommended: Optional[str] = None

    if preferred and preferred in live:
        recommended = preferred
    elif preferred:
        # Nifty 500 specifically, not merely "the first broad index" — it is
        # the widest series with full history, so it is the one substitution
        # that never itself introduces a coverage gap.
        recommended = (
            DEFAULT_BENCHMARK_INDEX
            if DEFAULT_BENCHMARK_INDEX in live
            else _first_live_broad(live) or (live[0] if live else None)
        )
        cov = coverage.get(preferred)
        launched = (
            f" (launched {cov.live_from.isoformat()})"
            if cov and cov.live_from
            else ""
        )
        fallback_reason = (
            f"{preferred} did not trade across "
            f"{start.isoformat()}..{end.isoformat()}{launched}, so "
            f"{recommended} is used instead. As a broad-market index it does "
            f"not match this strategy's size profile — part of any excess "
            f"return shown is the large/small-cap spread rather than the "
            f"strategy."
        )
    else:
        recommended = _first_live_broad(live) or (live[0] if live else None)

    return {
        "live": live,
        "backcomputed": backcomputed,
        "recommended": recommended,
        "preferred_available": preferred in live if preferred else None,
        "fallback_reason": fallback_reason,
    }


def _first_live_broad(live: List[str]) -> Optional[str]:
    for name in BROAD_INDICES:
        if name in live:
            return name
    return None


def default_benchmark_for(
    *,
    channel: str,
    rank_band: Optional[int] = None,
    universe_spec: Optional[str] = None,
) -> str:
    """The index a strategy should be compared against by default.

    A rank 150-200 momentum band measured against Nifty 500 is being scored
    partly on the large-cap/small-cap spread rather than on the strategy, in
    whichever direction that spread happened to run.
    """
    if channel == "momentum" and rank_band in RANK_BAND_BENCHMARKS:
        return RANK_BAND_BENCHMARKS[rank_band]
    if universe_spec and "microcap" in universe_spec.lower():
        return "Nifty Microcap 250"
    if universe_spec and "smallcap" in universe_spec.lower():
        return "Nifty Smallcap 250"
    if universe_spec and "midcap" in universe_spec.lower():
        return "Nifty Midcap 150"
    return DEFAULT_BENCHMARK_INDEX


def _as_date(v: Any) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, date):
        return v
    # DuckDB may hand back a pandas Timestamp depending on the fetch path.
    return v.date() if hasattr(v, "date") else date.fromisoformat(str(v)[:10])


def _stale_cutoff() -> date:
    return date.today() - timedelta(days=STALE_AFTER_DAYS)
