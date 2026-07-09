"""
features/fundamental_quality_gate.py

Phase: Data Layer / Ingestion (backlog #12, AF-5)
Owner: Platform / Features
Consumers: scripts/backfill_fundamentals_trendlyne.py, scripts/load_kaggle_fundamentals.py

Why this exists
----------------
Two independent unit-scaling bugs have already been found BY HAND in the raw
`fundamentals` table, each only caught after the fact:

1. `operating_margin`/`net_margin` stored as 0-100 (percent) instead of the
   table's actual 0-1 (fraction) convention — 22,084 rows silently wrong
   until a manual `UPDATE fundamentals SET operating_margin =
   operating_margin/100 ...` (see BuildLog.md "Fundamental Dashboard
   OpMargin/NetMargin Wrong (100x too high) — 2026-07-03").
2. `roe` for financial-sector tickers reading ~4% against a real-world
   ~15-17% — flagged by inspection, not yet root-caused (likely
   ingestion/scrapers/screener.py's unvalidated `header.get("roe")`
   passthrough).

There was no automated gate that would have caught either bug before it
landed in the table. This module is that gate: a small table of plausible
ranges per ratio field, checked against every row BEFORE (or as) it is
written. Out-of-range values are FLAGGED (logged loudly + a durable
per-row flag/reason recorded), never silently written and never silently
dropped — per this project's established preference (see
`tests/quality/` no-stub-data policy) for "an honest, visible gap" over
either "silently wrong" or "silently discarded".

Low-revenue exemption
----------------------
BuildLog.md's own investigation of the operating_margin/net_margin fix
found 330 rows still outside [-1.5, 1.5] AFTER the /100 correction — every
one a genuine micro-cap/shell ticker with near-zero revenue (e.g.
revenue=₹0.06cr) producing real, extreme-but-true ratios (a ₹0.01cr PAT on
₹0.06cr revenue is a "6600% margin" and also completely real). Flagging
those as a units bug would be a false positive. `LOW_REVENUE_FLOOR_CR`
below is the ₹ crore revenue floor under which margin/profitability checks
are skipped entirely (leverage ratios that don't depend on revenue, e.g.
debt_to_equity, are still checked).

Units convention (see features/fundamental.py's docstring and
ingestion/scrapers/screener.py's module docstring): every monetary
`fundamentals` column is in rupee CRORE; every margin/ROE/ROCE field is a
FRACTION (0.15 = 15%), never a percent (15.0).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# Below this much quarterly revenue (INR crore), margin/profitability ratios
# are exempted from range checks — real micro-cap/shell tickers routinely
# produce ratios like "6600% margin" on near-zero revenue that are true,
# not a units bug. See BuildLog.md "Fundamental Dashboard OpMargin/NetMargin
# Wrong" for the ₹0.06cr example that motivated this floor.
LOW_REVENUE_FLOOR_CR = 1.0


@dataclass(frozen=True)
class RatioRange:
    lo: float
    hi: float
    # If True, this field's check is skipped when revenue < LOW_REVENUE_FLOOR_CR
    revenue_exempt: bool = False


# Plausible-range table. Fraction convention throughout (0.15 = 15%), matching
# the `fundamentals` table's actual column contract — NOT the 0-100 percent
# convention some upstream sources (e.g. Trendlyne's OPMPCT_Q/NETPCT_Q) use
# natively; callers are expected to have already normalized to fraction
# before calling this gate (both backfill scripts already do the /100
# conversion at parse time — this gate is a second, independent check on
# the post-conversion value, not a substitute for that conversion).
RATIO_RANGES: Dict[str, RatioRange] = {
    # Margins: fraction of revenue. Plausible range is wide (loss-making and
    # occasional >100% margin quarters both happen legitimately) but a raw
    # 0-100 percent value stored where a fraction is expected will blow past
    # this every time (e.g. 27.0 instead of 0.27).
    "operating_margin": RatioRange(-2.0, 2.0, revenue_exempt=True),
    "ebitda_margin": RatioRange(-2.0, 2.0, revenue_exempt=True),
    "net_margin": RatioRange(-2.0, 2.0, revenue_exempt=True),
    # Profitability against equity/capital employed: real-world range for
    # even distressed or exceptional years rarely exceeds 300%; the
    # financial-sector ROE bug (~4% vs a real ~15-17%) would NOT be caught
    # by this range (4% is plausible on its face) — that bug needs a
    # separate sector-relative check, out of scope for this range gate.
    "roe": RatioRange(-1.0, 3.0, revenue_exempt=True),
    "roce": RatioRange(-1.0, 3.0, revenue_exempt=True),
    # Leverage: Debt / Equity. Not revenue-dependent, so no low-revenue
    # exemption — a shell ticker's debt-to-equity is either real or missing,
    # not distorted by near-zero revenue the way margins are.
    "debt_to_equity": RatioRange(0.0, 20.0, revenue_exempt=False),
    # EBIT (or op profit) / Interest expense. Can be legitimately very high
    # (low-debt companies) or negative (loss-making); bounded generously.
    "interest_coverage": RatioRange(-100.0, 500.0, revenue_exempt=True),
    # Revenue / Current assets proxy (features/financial_ratios.py's
    # documented current-assets-only proxy, not a true total-asset turnover).
    "asset_turnover": RatioRange(0.0, 20.0, revenue_exempt=True),
}


@dataclass
class QualityFlag:
    field: str
    value: float
    lo: float
    hi: float
    reason: str


def check_row(row: Dict) -> List[QualityFlag]:
    """
    Validate one fundamentals row (dict of column -> value) against
    RATIO_RANGES. Returns a list of QualityFlag for every field that is
    both present and outside its plausible range; empty list means the row
    is clean.

    Low-revenue exemption: any field marked `revenue_exempt=True` in
    RATIO_RANGES is skipped when `row["revenue"]` is missing or below
    LOW_REVENUE_FLOOR_CR — see module docstring.
    """
    flags: List[QualityFlag] = []
    revenue = row.get("revenue")
    revenue_is_low = revenue is None or revenue < LOW_REVENUE_FLOOR_CR

    for field, rng in RATIO_RANGES.items():
        value = row.get(field)
        if value is None:
            continue
        if rng.revenue_exempt and revenue_is_low:
            continue
        try:
            value = float(value)
        except (TypeError, ValueError):
            continue
        if value < rng.lo or value > rng.hi:
            flags.append(QualityFlag(
                field=field,
                value=value,
                lo=rng.lo,
                hi=rng.hi,
                reason=f"{field}={value:.4g} outside plausible range [{rng.lo}, {rng.hi}]",
            ))
    return flags


def flags_to_reason_string(flags: List[QualityFlag]) -> Optional[str]:
    """Join flags into a single semicolon-separated string for storage in a
    VARCHAR column (e.g. fundamentals.quality_flag_reason); None if empty."""
    if not flags:
        return None
    return "; ".join(f.reason for f in flags)


def log_flags(ticker: str, fiscal_year: int, quarter: int, flags: List[QualityFlag]) -> None:
    """Log every flag loudly (WARNING level) — flags must be visible, not
    silently swallowed, per this project's no-silent-bad-data convention."""
    for flag in flags:
        logger.warning(
            "Fundamentals quality flag: %s FY%s Q%s — %s",
            ticker, fiscal_year, quarter, flag.reason,
        )


def validate_and_annotate(row: Dict) -> Dict:
    """
    Convenience wrapper for ingestion scripts: runs check_row() against
    `row`, logs any flags, and returns the SAME row dict with two extra
    keys added so callers can pass it straight into an INSERT alongside
    the `quality_flag`/`quality_flag_reason` columns:

        row["quality_flag"] : bool  — True if any field was out of range
        row["quality_flag_reason"] : Optional[str] — human-readable detail

    Does not mutate or reject any existing field — flagging only, never
    rejection, per this backlog item's explicit "prefer flagging over
    hard-rejecting" guidance.
    """
    flags = check_row(row)
    ticker = row.get("ticker", "?")
    fiscal_year = row.get("fiscal_year", "?")
    quarter = row.get("quarter", "?")
    if flags:
        log_flags(ticker, fiscal_year, quarter, flags)
    row["quality_flag"] = bool(flags)
    row["quality_flag_reason"] = flags_to_reason_string(flags)
    return row
