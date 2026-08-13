"""
backtest/instrumentation.py

Phase: 3.x (Technical backtest refactor — STEP 3b)
Owner: backtest
Consumers: backtest/core/engine.py, backtest/core/run_context.py,
           tests/unit/test_instrumentation.py

Per-phase wall-clock timings for a backtest run.

WHY THIS DID NOT ALREADY EXIST
It looked like it did. `execution_timing` reads like instrumentation and is
recorded on every run, but it is a FILL-TIMING POLICY — its values are
"same_day_close" and "next_day_open", it decides which bar a signal fills at,
and it has nothing to do with elapsed time. No run in the 500-run history
carries a single duration. That is why the redesign's speed target was, until
now, a guess: an 8.6-hour sweep whose internal distribution nobody could see.

WHAT IT MEASURES
The rebalance loop's real phases, which are the only candidates for the
two-pass rewrite:

    universe        config.universe_provider(as_of)
    signals         adapter.generate_signals(...)   <- expected dominant cost
    prices          per-ticker price_lookup for held + signalled names
    corp_actions    corporate-action and delisting reconciliation
    execution       sells, then buys, including PIT market-cap rank
    exit_policy     the daily exit pass
    equity          record_equity
    finalize        metrics, benchmark curve, trade-log write

DESIGN CONSTRAINTS THIS RESPECTS
1. Timing must never change results. Every timer is a context manager that
   accumulates into a dict; nothing here can alter control flow, and an
   exception inside a timed block still propagates unchanged (the elapsed time
   is recorded first, so a crashed phase still reports how long it ran before
   failing — which is exactly the case you most want timed).

2. It must be cheap. perf_counter() costs tens of nanoseconds against phases
   measured in milliseconds, but the rebalance loop runs ~4,300 times per run
   over 17 years, so timers accumulate into a preallocated dict rather than
   appending per-call records. Memory is O(number of phases), not O(calls).
   A per-call log of every phase would be ~35,000 records per run and would
   itself become a cost worth optimising, which defeats the purpose.

3. It must survive being off. `NullTimer` satisfies the same interface with
   zero overhead, so instrumentation can be disabled without the call sites
   sprouting conditionals — and, more importantly, so no code path can quietly
   behave differently depending on whether timing is on.

WHAT IT DELIBERATELY DOES NOT DO
No CPU/wall split, no per-ticker attribution, no sampling profiler. The
question this answers is "which phase should the rewrite target", and phase
totals answer it. Anything finer is a second instrument to build once there is
a specific hypothesis to test — building it now would be optimising the
measurement before measuring.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Dict, Iterator, Optional

# The phases named above. Fixed rather than free-form so a typo produces a
# missing phase at import time instead of a silently separate bucket that
# splits one phase's cost across two names in the report.
PHASES = (
    "universe",
    "signals",
    "prices",
    "corp_actions",
    "execution",
    "exit_policy",
    "equity",
    "finalize",
)


@dataclass
class PhaseTimings:
    """Accumulated seconds and call counts per phase."""

    seconds: Dict[str, float] = field(default_factory=lambda: dict.fromkeys(PHASES, 0.0))
    calls: Dict[str, int] = field(default_factory=lambda: dict.fromkeys(PHASES, 0))
    started_at: float = field(default_factory=time.time)
    total_seconds: float = 0.0

    def record(self, phase: str, elapsed: float) -> None:
        if phase not in self.seconds:
            raise KeyError(
                f"unknown phase {phase!r}; add it to instrumentation.PHASES rather than "
                "passing an ad-hoc name — an unregistered phase silently splits one cost "
                "across two buckets."
            )
        self.seconds[phase] += elapsed
        self.calls[phase] += 1

    def as_dict(self) -> Dict[str, object]:
        """Serialisable form for BacktestRunResult, including the share of
        total each phase took — the share is what makes two runs of different
        length comparable, and computing it here stops every reader
        re-deriving it (and disagreeing about the denominator)."""
        measured = sum(self.seconds.values())
        return {
            "total_seconds": round(self.total_seconds, 3),
            "measured_seconds": round(measured, 3),
            # Time inside the run that no phase claimed. A large residual means
            # the phase list has a hole, so it is reported rather than hidden
            # by normalising the shares to sum to 100%.
            "unattributed_seconds": round(max(self.total_seconds - measured, 0.0), 3),
            "phases": {
                phase: {
                    "seconds": round(self.seconds[phase], 3),
                    "calls": self.calls[phase],
                    "pct_of_measured": (
                        round(100.0 * self.seconds[phase] / measured, 2) if measured > 0 else 0.0
                    ),
                    "ms_per_call": (
                        round(1000.0 * self.seconds[phase] / self.calls[phase], 3)
                        if self.calls[phase] else 0.0
                    ),
                }
                for phase in PHASES
            },
        }

    def slowest(self) -> Optional[str]:
        measured = {p: s for p, s in self.seconds.items() if s > 0}
        return max(measured, key=measured.get) if measured else None


class RunTimer:
    """Times phases of one run.

        timer = RunTimer()
        with timer.phase("signals"):
            signals = adapter.generate_signals(...)
        ...
        timer.finish()
    """

    def __init__(self) -> None:
        self.timings = PhaseTimings()
        self._start = time.perf_counter()

    @contextmanager
    def phase(self, name: str) -> Iterator[None]:
        start = time.perf_counter()
        try:
            yield
        finally:
            # In `finally`, so a phase that raises is still timed. A failing
            # phase's duration is often the most interesting number in the run
            # — a timeout looks identical to a fast crash without it.
            self.timings.record(name, time.perf_counter() - start)

    def finish(self) -> PhaseTimings:
        self.timings.total_seconds = time.perf_counter() - self._start
        return self.timings


class NullTimer:
    """Zero-overhead stand-in. Same interface, records nothing, so call sites
    never branch on whether instrumentation is enabled."""

    timings = PhaseTimings()

    @contextmanager
    def phase(self, name: str) -> Iterator[None]:
        yield

    def finish(self) -> PhaseTimings:
        return PhaseTimings()


def format_timings(timings: PhaseTimings) -> str:
    """Human-readable summary for a run log — the thing an operator actually
    reads at the end of an 8-hour sweep."""
    d = timings.as_dict()
    lines = [
        f"run wall clock: {d['total_seconds']}s "
        f"(measured {d['measured_seconds']}s, unattributed {d['unattributed_seconds']}s)"
    ]
    ranked = sorted(
        d["phases"].items(), key=lambda kv: kv[1]["seconds"], reverse=True  # type: ignore[index]
    )
    for phase, stats in ranked:
        if stats["calls"] == 0:  # type: ignore[index]
            continue
        lines.append(
            f"  {phase:<13} {stats['seconds']:>9.3f}s  "  # type: ignore[index]
            f"{stats['pct_of_measured']:>6.2f}%  "  # type: ignore[index]
            f"{stats['calls']:>7} calls  "  # type: ignore[index]
            f"{stats['ms_per_call']:>8.3f} ms/call"  # type: ignore[index]
        )
    return "\n".join(lines)
