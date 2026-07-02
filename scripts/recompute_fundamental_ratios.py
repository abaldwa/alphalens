"""
scripts/recompute_fundamental_ratios.py

Phase: 2.x (Fundamentals — derived ratios)
Specs: CLAUDE.md Absolute Rule 6
Owner: Platform / Ingestion
Consumers: fundamentals table (in place UPDATE)

Recomputes financial ratios for every row in `fundamentals` using
features/financial_ratios.py instead of trusting the scraped, snapshot-only
roe/debt_to_equity/asset_turnover fields from Trendlyne/Screener.in.

Overwrites: ebit, net_debt, debt_to_ebitda, fcf_margin, capex_intensity
(new, fully-derived columns) and roe, roce, debt_to_equity, asset_turnover
(existing columns — the scraped values in these are replaced with derived
ones; both are "real data", but the derived ones are consistent across
every period instead of only the latest scraped snapshot). Leaves NULL
columns (inventory_days, receivable_days, payable_days — no raw
inventory/receivable/payable fields exist in this schema) untouched.

Usage
-----
    .venv/bin/python3 scripts/recompute_fundamental_ratios.py
    .venv/bin/python3 scripts/recompute_fundamental_ratios.py --dry-run
    .venv/bin/python3 scripts/recompute_fundamental_ratios.py --limit 100
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)

_RAW_COLS = [
    "ticker", "fiscal_year", "quarter", "ebitda", "depreciation", "total_debt",
    "cash_and_equivalents", "pat", "eps", "book_value_per_share",
    "shares_outstanding", "revenue", "current_assets", "fcf", "capex",
    "total_equity",
]
_DERIVED_COLS = [
    "ebit", "net_debt", "debt_to_ebitda", "roe", "roce", "debt_to_equity",
    "asset_turnover", "fcf_margin", "capex_intensity",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Recompute fundamentals ratios from raw line items")
    parser.add_argument("--dry-run", action="store_true", help="Compute but do not write to DB")
    parser.add_argument("--limit", type=int, default=None, help="Stop after N rows (for testing)")
    args = parser.parse_args()

    from config.settings import DUCKDB_PATH
    from features.financial_ratios import derive_all_ratios

    conn = duckdb.connect(str(DUCKDB_PATH), read_only=args.dry_run)
    try:
        query = f"SELECT {', '.join(_RAW_COLS)} FROM fundamentals"
        if args.limit:
            query += f" LIMIT {args.limit}"
        rows = conn.execute(query).fetchall()
        col_idx = {c: i for i, c in enumerate(_RAW_COLS)}

        updated = 0
        populated_counts = {c: 0 for c in _DERIVED_COLS}
        for r in rows:
            row = {c: r[col_idx[c]] for c in _RAW_COLS}
            derived = derive_all_ratios(row)
            for c, v in derived.items():
                if v is not None:
                    populated_counts[c] += 1

            if not args.dry_run:
                set_clause = ", ".join(f"{c} = ?" for c in _DERIVED_COLS)
                conn.execute(
                    f"UPDATE fundamentals SET {set_clause} "
                    "WHERE ticker = ? AND fiscal_year = ? AND quarter = ?",
                    [derived[c] for c in _DERIVED_COLS] + [row["ticker"], row["fiscal_year"], row["quarter"]],
                )
            updated += 1

        total = len(rows)
        logger.info("Processed %d rows (dry_run=%s)", updated, args.dry_run)
        for c in _DERIVED_COLS:
            pct = 100.0 * populated_counts[c] / total if total else 0.0
            logger.info("  %-16s : %d/%d populated (%.1f%%)", c, populated_counts[c], total, pct)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
