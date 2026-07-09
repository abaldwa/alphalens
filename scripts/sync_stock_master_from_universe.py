"""
scripts/sync_stock_master_from_universe.py

Real gap found 2026-07-05 while verifying the Big Investor Activity
dashboard against real data: `stock_master` (datastore/schema/
create_normalised.py) has NEVER been populated anywhere in this codebase
— grepping for "INSERT INTO stock_master" across the whole project turns
up nothing. The actual, canonical universe source everywhere else is
config/nifty500_universe.csv via config/universe.py's load_universe_raw()
— stock_master is an unused Phase 0.1 skeleton table that several already
-built routers (big_investors.py, tijori.py, trendlyne.py,
groww_mf_holdings.py) join against for company_name/sector/market_cap_cr,
silently getting NULLs for every row since the table has always been
empty.

One-time (rerunnable) sync: upserts every row from
config/nifty500_universe.csv into stock_master. Real data only — no
synthetic/placeholder rows. nse_series has no CSV equivalent and is
NOT NULL in the schema; every row is real NSE-listed equity in this
universe, so it's set to the literal 'EQ' (ordinary equity series) for
all rows, not guessed per-ticker. industry and listing_date have no CSV
source and are left NULL (nullable columns).

stock_master.company_name is NOT NULL — rows with a NaN company_name in
the CSV (the same ~691-ticker screener.in-unresolved backlog tracked in
config/company_metadata_enrichment_unresolved.csv, being worked via
Trendlyne as a separate, parked task) are skipped rather than making up
a name for them; they'll sync automatically once resolved and this
script is rerun.
"""

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    from config.settings import DUCKDB_PATH
    from config.universe import load_universe_raw
    from datastore.api.db import get_duckdb_connection

    df = load_universe_raw()

    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
        upserted = 0
        skipped_no_name = 0
        for row in df.itertuples(index=False):
            if pd.isna(row.company_name):
                skipped_no_name += 1
                continue
            conn.execute(
                """
                INSERT INTO stock_master (
                    ticker, company_name, sector, industry, nse_series,
                    listing_date, market_cap_cr, adtv_cr, current_tier,
                    is_fno_eligible, is_nifty500
                ) VALUES (?, ?, ?, NULL, 'EQ', NULL, ?, ?, ?, ?, ?)
                ON CONFLICT (ticker) DO UPDATE SET
                    company_name = excluded.company_name,
                    sector = excluded.sector,
                    market_cap_cr = excluded.market_cap_cr,
                    adtv_cr = excluded.adtv_cr,
                    current_tier = excluded.current_tier,
                    is_fno_eligible = excluded.is_fno_eligible,
                    is_nifty500 = excluded.is_nifty500
                """,
                [
                    row.ticker,
                    row.company_name,
                    row.sector if pd.notna(row.sector) else None,
                    float(row.market_cap_cr) if pd.notna(row.market_cap_cr) else None,
                    float(row.adtv_cr) if pd.notna(row.adtv_cr) else None,
                    int(row.tier) if pd.notna(row.tier) else None,
                    bool(row.is_fno_eligible), bool(row.is_nifty500),
                ],
            )
            upserted += 1

    print(
        f"Synced {upserted} rows from {PROJECT_ROOT / 'config' / 'nifty500_universe.csv'} into "
        f"stock_master ({skipped_no_name} skipped for missing company_name)."
    )


if __name__ == "__main__":
    sys.exit(main() or 0)
