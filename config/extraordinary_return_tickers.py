"""
Extraordinary-return tickers — a deliberate SENSITIVITY-ANALYSIS toggle,
NOT a data-quality exclusion (contrast with backtest_exclusions.py, whose
list is tickers with unverifiable price history). These tickers' price
history is fine; the concern is different: momentum strategies captured
enormous, likely non-repeatable rallies in a small number of names, and
headline backtest returns may be dominated by them rather than reflecting
a repeatable edge.

Ranked 2026-09-06 from framework_backtest_trades (1,986,459 trades across
every strategy/band/config combination run so far): grouped by ticker,
summed pnl_inr, sorted descending. Top 30 shown here span from CUPID
(8.9% of total net P&L alone) down to BORORENEW (1.4%) — cumulative
contribution crosses ~90% of total net P&L at that point (top 10 alone =
48%), out of 1,749 distinct tickers ever traded.

This is a SNAPSHOT of one campaign's trade history ranking, not a
repeatable formula — re-derive it (same groupby query against
framework_backtest_trades) if you want it to reflect a later campaign
rather than hand-edit ratios here.

Usage: NOT applied by default. A strategy opts in per-run via the
`exclude_extraordinary_returns=True` kwarg, with the cutoff controlled by
`extraordinary_returns_top_n` (default DEFAULT_EXTRAORDINARY_RETURNS_TOP_N
= 15, configurable per user decision 2026-09-06 — was a fixed top-30 list
before this). Both flow through StrategyAdapter.extra_params ->
describe() -> orchestrator.py's universe filter, and into strategy_id as
an `exOutliersN` suffix so runs with different N (or the baseline) never
collide in the results DB. See queues/generator.py's
with_and_without_extraordinary_returns() for the queue-level pairing
helper.
"""

from __future__ import annotations

from typing import List, Set, Tuple

DEFAULT_EXTRAORDINARY_RETURNS_TOP_N = 15

#: (ticker, descriptive reason) IN RANK ORDER by total P&L contribution,
#: most extreme first — extraordinary_return_tickers(top_n) slices this
#: list, it does not re-sort it.
RANKED_EXTRAORDINARY_RETURN_TICKERS: List[Tuple[str, str]] = [
    ("CUPID", "8.9% of total net P&L across 2,654 trades (measured 2026-09-06)"),
    ("TITAGARH", "7.6% of total net P&L across 3,370 trades (measured 2026-09-06)"),
    ("BCG", "6.2% of total net P&L across 3,352 trades (measured 2026-09-06)"),
    ("MTARTECH", "5.2% of total net P&L across 786 trades (measured 2026-09-06)"),
    ("XPROINDIA", "3.7% of total net P&L across 1,094 trades (measured 2026-09-06)"),
    ("FORCEMOT", "3.6% of total net P&L across 3,234 trades (measured 2026-09-06)"),
    ("JINDALSAW", "3.5% of total net P&L across 2,340 trades (measured 2026-09-06)"),
    ("TTML", "3.2% of total net P&L across 3,482 trades (measured 2026-09-06)"),
    ("PGEL", "3.1% of total net P&L across 4,802 trades (measured 2026-09-06)"),
    ("TANLA", "3.0% of total net P&L across 4,356 trades (measured 2026-09-06) — the user's original example, max rolling-12mo return +2,039% (2021-03-26)"),
    ("OLECTRA", "3.0% of total net P&L across 5,156 trades (measured 2026-09-06)"),
    ("ADANIGREEN", "2.7% of total net P&L across 3,644 trades (measured 2026-09-06)"),
    ("SHAKTIPUMP", "2.7% of total net P&L across 4,092 trades (measured 2026-09-06)"),
    ("GVT&D", "2.5% of total net P&L across 3,692 trades (measured 2026-09-06)"),
    ("APARINDS", "2.5% of total net P&L across 2,926 trades (measured 2026-09-06)"),
    ("WEBELSOLAR", "2.5% of total net P&L across 4,778 trades (measured 2026-09-06)"),
    ("KPIGREEN", "2.4% of total net P&L across 3,019 trades (measured 2026-09-06)"),
    ("HEG", "2.3% of total net P&L across 2,776 trades (measured 2026-09-06)"),
    ("BSE", "2.3% of total net P&L across 3,604 trades (measured 2026-09-06)"),
    ("SAREGAMA", "2.2% of total net P&L across 4,806 trades (measured 2026-09-06)"),
    ("REFEX", "2.2% of total net P&L across 5,786 trades (measured 2026-09-06)"),
    ("JWL", "2.0% of total net P&L across 4,260 trades (measured 2026-09-06)"),
    ("HBLENGINE", "2.0% of total net P&L across 3,034 trades (measured 2026-09-06)"),
    ("WOCKPHARMA", "1.8% of total net P&L across 6,294 trades (measured 2026-09-06)"),
    ("MAZDOCK", "1.7% of total net P&L across 2,900 trades (measured 2026-09-06)"),
    ("COCHINSHIP", "1.6% of total net P&L across 2,052 trades (measured 2026-09-06)"),
    ("CGPOWER", "1.5% of total net P&L across 3,982 trades (measured 2026-09-06)"),
    ("GOACARBON", "1.5% of total net P&L across 1,080 trades (measured 2026-09-06)"),
    ("GENESYS", "1.4% of total net P&L across 4,624 trades (measured 2026-09-06)"),
    ("BORORENEW", "1.4% of total net P&L across 2,930 trades (measured 2026-09-06)"),
]


def extraordinary_return_tickers(top_n: int = DEFAULT_EXTRAORDINARY_RETURNS_TOP_N) -> Set[str]:
    """The top `top_n` tickers (by P&L contribution rank) to exclude.

    Raises if top_n exceeds the ranked list's length rather than silently
    returning fewer than asked for — a caller requesting more than exists
    should know its request was clamped, not get a smaller, unflagged set.
    """
    if top_n < 0:
        raise ValueError(f"top_n must be >= 0, got {top_n}")
    if top_n > len(RANKED_EXTRAORDINARY_RETURN_TICKERS):
        raise ValueError(
            f"top_n={top_n} exceeds the ranked list's length "
            f"({len(RANKED_EXTRAORDINARY_RETURN_TICKERS)}) — extend "
            "RANKED_EXTRAORDINARY_RETURN_TICKERS first"
        )
    return {ticker for ticker, _ in RANKED_EXTRAORDINARY_RETURN_TICKERS[:top_n]}
