"""
features/financial_ratios.py

Phase: 2.x (Fundamentals — derived ratios)
Specs: CLAUDE.md Absolute Rule 6 (no synthetic/fabricated data, ever)
Owner: Platform / Features
Consumers: scripts/recompute_fundamental_ratios.py, features/fundamental.py

Computes financial ratios from the raw line items already stored in the
`fundamentals` table (revenue, ebitda, pat, total_debt, depreciation, etc.)
instead of trusting Trendlyne's/Screener.in's own scraped ratio fields.

Why: `ingestion/scrapers/screener.py` sources `roe`, `book_value_per_share`,
and `shares_outstanding` from a current-snapshot ratio box fetched once per
ticker, not from the historical per-quarter balance sheet — so those three
fields are only ~3-12% populated across the table even though the raw line
items needed for profitability/leverage ratios (revenue, ebitda, pat,
depreciation, total_debt, cash_and_equivalents) are 87-100% populated. See
BuildLog.md "Real data sourcing — Financial ratio derivation".

Every function here is pure and returns None (never 0, never an imputed
estimate) when a required raw input is missing or the computation is
undefined (e.g. division by zero) — per CLAUDE.md Rule 6, an honest gap is
not the same as a synthetic fallback.

Ratios that need shareholder equity (roe, roce, debt_to_equity,
asset_turnover) prefer the `total_equity` raw column when present — Equity
Capital + Reserves read directly per fiscal year from Screener.in's
#balance-sheet table (every historical FY column, not a current snapshot;
see ingestion/scrapers/screener.py's `_parse_balance_sheet_history` and
scripts/backfill_equity_from_screener.py). They fall back to the
book_value_per_share * shares_outstanding back-derivation only when
`total_equity` is absent for that row.

PIT Assumptions
----------------
None — pure arithmetic over already-PIT-correct fields the caller passes in
(the `fundamentals` row's own `announcement_date` governs PIT, untouched
here).
"""

from __future__ import annotations

from typing import Any, Optional


def _safe_div(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    """None-propagating division; returns None (not inf/nan) for a zero or missing denominator."""
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def compute_ebit(ebitda: Optional[float], depreciation: Optional[float]) -> Optional[float]:
    """EBIT = EBITDA - Depreciation & Amortization."""
    if ebitda is None or depreciation is None:
        return None
    return ebitda - depreciation


def compute_net_debt(
    total_debt: Optional[float], cash_and_equivalents: Optional[float]
) -> Optional[float]:
    """Net Debt = Total Debt - Cash & Equivalents."""
    if total_debt is None or cash_and_equivalents is None:
        return None
    return total_debt - cash_and_equivalents


def compute_debt_to_ebitda(total_debt: Optional[float], ebitda: Optional[float]) -> Optional[float]:
    """Leverage proxy: Total Debt / EBITDA. Undefined (None) if EBITDA <= 0."""
    if ebitda is not None and ebitda <= 0:
        return None
    return _safe_div(total_debt, ebitda)


def compute_shares_outstanding(pat: Optional[float], eps: Optional[float]) -> Optional[float]:
    """
    Back out share count from already-scraped PAT and EPS for the same
    period (both ~99%+ populated) rather than relying on the scraper's
    snapshot-only shares_outstanding field (~4% populated).
    """
    if eps is not None and eps == 0:
        return None
    return _safe_div(pat, eps)


def compute_equity(
    book_value_per_share: Optional[float], shares_outstanding: Optional[float]
) -> Optional[float]:
    """Shareholder equity = Book Value per Share * Shares Outstanding."""
    if book_value_per_share is None or shares_outstanding is None:
        return None
    return book_value_per_share * shares_outstanding


def compute_roe(pat: Optional[float], equity: Optional[float]) -> Optional[float]:
    """Return on Equity = PAT / Shareholder Equity. None if equity <= 0 (undefined, not 'bad')."""
    if equity is not None and equity <= 0:
        return None
    return _safe_div(pat, equity)


def compute_roce(
    ebit: Optional[float], total_debt: Optional[float], equity: Optional[float]
) -> Optional[float]:
    """ROCE = EBIT / Capital Employed, where Capital Employed = Total Debt + Equity."""
    if total_debt is None or equity is None:
        return None
    capital_employed = total_debt + equity
    if capital_employed <= 0:
        return None
    return _safe_div(ebit, capital_employed)


def compute_debt_to_equity(total_debt: Optional[float], equity: Optional[float]) -> Optional[float]:
    """Debt-to-Equity = Total Debt / Shareholder Equity. None if equity <= 0."""
    if equity is not None and equity <= 0:
        return None
    return _safe_div(total_debt, equity)


def compute_asset_turnover(
    revenue: Optional[float], current_assets: Optional[float]
) -> Optional[float]:
    """
    Revenue / Current Assets. Note: a true asset-turnover ratio uses TOTAL
    assets, which this schema does not capture (no total_assets raw field
    from either free scraper) — this is a current-assets-only proxy, kept
    honestly named via the caller's awareness rather than mislabeled.
    """
    return _safe_div(revenue, current_assets)


def compute_fcf_margin(fcf: Optional[float], revenue: Optional[float]) -> Optional[float]:
    """Free Cash Flow / Revenue."""
    return _safe_div(fcf, revenue)


def compute_capex_intensity(capex: Optional[float], revenue: Optional[float]) -> Optional[float]:
    """Capex / Revenue."""
    return _safe_div(capex, revenue)


def derive_all_ratios(row: dict[str, Any]) -> dict[str, Any]:
    """
    Compute every derivable ratio for one fundamentals row (dict of raw
    column values, e.g. a DuckDB row as a dict). Returns a dict of only the
    DERIVED columns (ebit, net_debt, debt_to_ebitda, roe, roce,
    debt_to_equity, asset_turnover, fcf_margin, capex_intensity) — callers
    decide which existing table columns to overwrite with which keys.
    """
    ebitda = row.get("ebitda")
    depreciation = row.get("depreciation")
    total_debt = row.get("total_debt")
    cash = row.get("cash_and_equivalents")
    pat = row.get("pat")
    eps = row.get("eps")
    bvps = row.get("book_value_per_share")
    revenue = row.get("revenue")
    current_assets = row.get("current_assets")
    fcf = row.get("fcf")
    capex = row.get("capex")

    ebit = compute_ebit(ebitda, depreciation)
    equity = row.get("total_equity")
    if equity is None:
        shares = row.get("shares_outstanding") or compute_shares_outstanding(pat, eps)
        equity = compute_equity(bvps, shares)

    return {
        "ebit": ebit,
        "net_debt": compute_net_debt(total_debt, cash),
        "debt_to_ebitda": compute_debt_to_ebitda(total_debt, ebitda),
        "roe": compute_roe(pat, equity),
        "roce": compute_roce(ebit, total_debt, equity),
        "debt_to_equity": compute_debt_to_equity(total_debt, equity),
        "asset_turnover": compute_asset_turnover(revenue, current_assets),
        "fcf_margin": compute_fcf_margin(fcf, revenue),
        "capex_intensity": compute_capex_intensity(capex, revenue),
    }
