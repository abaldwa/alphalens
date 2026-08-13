"""
backtest/parity.py

Phase: 3.x (Technical backtest refactor — STEP 6)
Owner: backtest
Consumers: scripts/check_refactor_parity.py, tests/unit/test_parity.py

Byte-level comparison of two backtest runs, so a refactor that is supposed to
move code without changing behaviour can be shown to have done exactly that.

WHY A PARITY GATE AND NOT A TEST SUITE

The unit suite is green throughout this refactor and would stay green through
a layer split that silently changed a result. It asserts behaviours somebody
thought to assert; it cannot notice that a 17-year run's 3,886th trade now
fills a day later. Only comparing whole runs does that.

This is not hypothetical. Every defect this refactor found was invisible in
aggregate metrics — a wrong universe, unreachable exit triggers, tax that
never left the portfolio. Each produced a perfectly plausible CAGR. A pure
code move that quietly changed one is exactly as plausible.

THE RULE THIS ENFORCES

Pure moves and behaviour changes never share a commit. A move is verified by
this gate reporting IDENTICAL. A behaviour change is verified by its own
tests, and its parity report is expected to differ — but the differences must
be the ones intended, enumerated, and small enough to read. A commit that
does both at once makes each unreviewable: the diff is too large to read
line-by-line and the parity report cannot separate deliberate change from
accident.

WHAT IS COMPARED

Trades first, then the equity curve, then metrics — cheapest and most
diagnostic first. A trade-level difference names the ticker and date that
diverged, which usually identifies the cause immediately; a metrics-only
difference tells you something moved but not what.

Floating-point tolerance is deliberately tiny (1e-9 relative). A pure code
move should reproduce results EXACTLY, because it runs the same arithmetic in
the same order. A tolerance loose enough to absorb a genuine change is a
tolerance that lets the change through.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd

# Relative tolerance for float equality. A pure move recomputes identical
# arithmetic, so anything beyond accumulated float noise is a real difference.
RELATIVE_TOLERANCE = 1e-9

# How many differing rows to enumerate before truncating. Enough to see a
# pattern (one ticker? one date? every row?), few enough to read.
MAX_REPORTED_DIFFERENCES = 20


@dataclass
class ParityReport:
    identical: bool
    trade_differences: List[str] = field(default_factory=list)
    equity_differences: List[str] = field(default_factory=list)
    metric_differences: List[str] = field(default_factory=list)

    def summary(self) -> str:
        if self.identical:
            return "IDENTICAL — the refactor changed no behaviour"
        parts = []
        for label, diffs in (
            ("trades", self.trade_differences),
            ("equity curve", self.equity_differences),
            ("metrics", self.metric_differences),
        ):
            if diffs:
                parts.append(f"{len(diffs)} {label} difference(s)")
        return "DIFFERS — " + ", ".join(parts)

    def detail(self) -> str:
        lines = [self.summary()]
        for label, diffs in (
            ("TRADES", self.trade_differences),
            ("EQUITY", self.equity_differences),
            ("METRICS", self.metric_differences),
        ):
            if not diffs:
                continue
            lines.append(f"\n{label}:")
            lines.extend(f"  {d}" for d in diffs[:MAX_REPORTED_DIFFERENCES])
            if len(diffs) > MAX_REPORTED_DIFFERENCES:
                lines.append(f"  ... and {len(diffs) - MAX_REPORTED_DIFFERENCES} more")
        return "\n".join(lines)


def _close(a: Any, b: Any) -> bool:
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        if pd.isna(a) and pd.isna(b):
            # Two NaNs are equal FOR THIS PURPOSE. NaN != NaN would report
            # every absent survival curve as a difference and drown the real
            # ones — the classic way a comparison gets switched off entirely.
            return True
        if pd.isna(a) or pd.isna(b):
            return False
        scale = max(abs(a), abs(b), 1.0)
        return abs(a - b) <= RELATIVE_TOLERANCE * scale
    return a == b


def compare_trades(before: pd.DataFrame, after: pd.DataFrame) -> List[str]:
    """Trade-by-trade comparison, ordered.

    Order matters and is not normalised away. Two runs holding the same
    positions but opening them in a different sequence are NOT the same run:
    sizing depends on cash available at the moment of each buy, so a
    reordering changes quantities downstream even when the ticker set matches.
    """
    diffs: List[str] = []
    if len(before) != len(after):
        diffs.append(f"trade count: {len(before)} -> {len(after)}")

    shared_columns = [c for c in before.columns if c in after.columns]
    missing = set(before.columns) ^ set(after.columns)
    if missing:
        diffs.append(f"columns differ: {sorted(missing)}")

    for i in range(min(len(before), len(after))):
        row_b, row_a = before.iloc[i], after.iloc[i]
        for col in shared_columns:
            if not _close(row_b[col], row_a[col]):
                diffs.append(
                    f"trade[{i}] {row_b.get('ticker', '?')} {col}: "
                    f"{row_b[col]!r} -> {row_a[col]!r}"
                )
                # One field per trade is enough to identify it; listing every
                # field of a diverged trade buries the next diverged trade.
                break
    return diffs


def compare_equity(before: pd.Series, after: pd.Series) -> List[str]:
    diffs: List[str] = []
    if len(before) != len(after):
        diffs.append(f"equity curve length: {len(before)} -> {len(after)}")
    for idx in before.index.intersection(after.index):
        if not _close(before.loc[idx], after.loc[idx]):
            diffs.append(f"equity[{idx}]: {before.loc[idx]:,.4f} -> {after.loc[idx]:,.4f}")
    return diffs


def compare_metrics(before: Dict[str, Any], after: Dict[str, Any]) -> List[str]:
    diffs: List[str] = []
    for key in sorted(set(before) | set(after)):
        if key not in before:
            diffs.append(f"{key}: absent -> {after[key]!r}")
        elif key not in after:
            diffs.append(f"{key}: {before[key]!r} -> absent")
        elif not _close(before[key], after[key]):
            diffs.append(f"{key}: {before[key]!r} -> {after[key]!r}")
    return diffs


def compare_runs(
    before_trades: pd.DataFrame, after_trades: pd.DataFrame,
    before_equity: Optional[pd.Series] = None, after_equity: Optional[pd.Series] = None,
    before_metrics: Optional[Dict[str, Any]] = None,
    after_metrics: Optional[Dict[str, Any]] = None,
) -> ParityReport:
    trade_diffs = compare_trades(before_trades, after_trades)
    equity_diffs = (
        compare_equity(before_equity, after_equity)
        if before_equity is not None and after_equity is not None else []
    )
    metric_diffs = (
        compare_metrics(before_metrics, after_metrics)
        if before_metrics is not None and after_metrics is not None else []
    )
    return ParityReport(
        identical=not (trade_diffs or equity_diffs or metric_diffs),
        trade_differences=trade_diffs,
        equity_differences=equity_diffs,
        metric_differences=metric_diffs,
    )
