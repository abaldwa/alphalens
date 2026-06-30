"""
scripts/load_kaggle_fundamentals.py

Phase: 3 (Historical Fundamentals + Shareholding from Kaggle)
Specs: SPEC-PIPE-003 (CRITICAL — PIT)
Owner: Platform / Ingestion

Loads historical fundamentals and shareholding from the Kaggle dataset:
  "Detailed Financials Data Of 4456 NSE & BSE Company"

Dataset structure (per-company folders):
  <CompanyName>/<CompanyName>_Basic_Info.csv      ← NSE ticker symbol
  <CompanyName>/Quarterly_Profit_Loss.csv          ← quarterly P&L (wide format)
  <CompanyName>/Yearly_Balance_Sheet.csv           ← annual balance sheet (wide)
  <CompanyName>/Yearly_Cash_flow.csv               ← annual cash flow (wide)
  <CompanyName>/Ratios.csv                         ← annual ratios (wide)
  <CompanyName>/Quarterly_Shareholding_Pattern.csv ← quarterly shareholding (wide)

Data coverage: quarterly from ~Sep 2020, yearly from ~2012.

Usage
-----
    .venv/bin/python3 scripts/load_kaggle_fundamentals.py

    # Dry-run (parse without writing)
    .venv/bin/python3 scripts/load_kaggle_fundamentals.py --dry-run

    # Custom archive path
    .venv/bin/python3 scripts/load_kaggle_fundamentals.py \\
        --archive /home/amit/Downloads/archive
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_ARCHIVE = Path("/home/amit/Downloads/archive")
DATA_SUBDIR = (
    "Detailed-Financials-Data-Of-4456-NSE-And-BSE-Company-20231230T233228Z-001"
    "/Detailed-Financials-Data-Of-4456-NSE-_-BSE-Company"
)


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _quarter_end(col_date: str) -> Optional[str]:
    """
    Convert Kaggle column date "YYYY-MM-DD" (first of month) to the actual
    quarter-end date (last day of that month).
    """
    try:
        ts = pd.Timestamp(col_date) + pd.offsets.MonthEnd(0)
        return ts.date().isoformat()
    except Exception:
        return None


def _fiscal_year_quarter(qend: str) -> Tuple[int, int]:
    """
    Indian FY runs April-March. FY is labelled by the year it ends (March).
      Apr-Jun → Q1,  FY = year+1
      Jul-Sep → Q2,  FY = year+1
      Oct-Dec → Q3,  FY = year+1
      Jan-Mar → Q4,  FY = year
    """
    d = date.fromisoformat(qend)
    m = d.month
    y = d.year
    if m <= 3:
        return y, 4
    elif m <= 6:
        return y + 1, 1
    elif m <= 9:
        return y + 1, 2
    else:
        return y + 1, 3


def _announcement_date(qend: str) -> str:
    """
    Conservative PIT default (SPEC-PIPE-003):
      Q1/Q2/Q3 results are due within 45 days; Q4/annual within 60 days.
    """
    d = date.fromisoformat(qend)
    days = 60 if d.month == 3 else 45
    return (pd.Timestamp(qend) + pd.Timedelta(days=days)).date().isoformat()


def _num(val) -> Optional[float]:
    try:
        v = float(str(val).replace(",", "").strip())
        return None if pd.isna(v) else v
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Per-company parsers
# ---------------------------------------------------------------------------

def _get_nse_ticker(company_dir: Path) -> Optional[str]:
    """Read NSE symbol from Basic_Info CSV."""
    info_files = list(company_dir.glob("*Basic_Info.csv")) + list(company_dir.glob("*Info*.csv"))
    if not info_files:
        return None
    try:
        df = pd.read_csv(info_files[0])
        nse_col = next((c for c in df.columns if "NSE" in c.upper()), None)
        if nse_col is None:
            return None
        val = str(df[nse_col].iloc[0]).strip()
        return val if val and val.lower() not in ("nan", "", "-") else None
    except Exception:
        return None


def _parse_wide(csv_path: Path) -> Optional[pd.DataFrame]:
    """
    Read a wide-format CSV (rows=metrics, cols=dates) and return it as-is.
    Returns None on read error.
    """
    try:
        return pd.read_csv(csv_path, index_col=0)
    except Exception:
        return None


def _build_fundamentals_rows(ticker: str, qpl: pd.DataFrame,
                              bs_yearly: pd.DataFrame,
                              cf_yearly: pd.DataFrame,
                              ratios: pd.DataFrame) -> list:
    """
    Build one fundamentals dict per quarter from Quarterly_Profit_Loss.
    Yearly balance-sheet / ratio data is joined to the nearest prior year-end.
    """
    rows = []

    # Precompute yearly lookup dicts (date-string → value)
    def _yearly_lookup(df: pd.DataFrame, metric: str) -> dict:
        if df is None or metric not in df.index:
            return {}
        series = df.loc[metric]
        out = {}
        for col, val in series.items():
            qe = _quarter_end(str(col))
            if qe:
                out[qe] = _num(val)
        return out

    total_debt_by_year   = _yearly_lookup(bs_yearly, "Borrowings")
    equity_cap_by_year   = _yearly_lookup(bs_yearly, "Equity Capital")
    reserves_by_year     = _yearly_lookup(bs_yearly, "Reserves")
    op_cashflow_by_year  = _yearly_lookup(cf_yearly, "Cash from Operating Activity")
    capex_by_year        = _yearly_lookup(cf_yearly, "Cash from Investing Activity")  # negative = capex
    # Ratios: ROCE from non-bank, ROE from bank
    roce_by_year = _yearly_lookup(ratios, "ROCE %")
    roe_by_year  = _yearly_lookup(ratios, "ROE %")

    def _nearest_yearly(lookup: dict, qend: str) -> Optional[float]:
        """Return yearly value from the same FY year-end (March 31) <= qend."""
        if not lookup:
            return None
        qe_date = date.fromisoformat(qend)
        fy, _ = _fiscal_year_quarter(qend)
        # FY year-end is March 31 of FY year
        fy_end = f"{fy}-03-31"
        if fy_end in lookup:
            return lookup[fy_end]
        # Fallback: most recent yearly entry <= qend
        candidates = {k: v for k, v in lookup.items() if k <= qend}
        if not candidates:
            return None
        return candidates[max(candidates)]

    # Detect bank vs non-bank by column name presence
    is_bank = "Revenue" in qpl.index  # banks use Revenue; others use Sales

    rev_row  = "Revenue" if is_bank else "Sales"
    ebitda_row = "Financing Profit" if is_bank else "Operating Profit"
    opm_row  = "Financing Margin %" if is_bank else "OPM %"

    for col in qpl.columns:
        qend = _quarter_end(str(col))
        if qend is None:
            continue

        fy, q = _fiscal_year_quarter(qend)
        ann_date = _announcement_date(qend)

        revenue = _num(qpl.loc[rev_row, col])   if rev_row  in qpl.index else None
        ebitda  = _num(qpl.loc[ebitda_row, col]) if ebitda_row in qpl.index else None
        opm     = _num(qpl.loc[opm_row, col])    if opm_row   in qpl.index else None
        pat     = _num(qpl.loc["Net Profit", col]) if "Net Profit" in qpl.index else None
        eps     = _num(qpl.loc["EPS in Rs", col])  if "EPS in Rs" in qpl.index else None
        depr    = _num(qpl.loc["Depreciation", col]) if "Depreciation" in qpl.index else None
        interest = _num(qpl.loc["Interest", col])   if "Interest"   in qpl.index else None

        net_margin = (pat / revenue * 100) if (pat and revenue and revenue != 0) else None
        # FCF ≈ operating cash flow − capex (yearly, nearest)
        op_cf = _nearest_yearly(op_cashflow_by_year, qend)
        capex = _nearest_yearly(capex_by_year, qend)
        fcf   = (op_cf + capex) if (op_cf is not None and capex is not None) else None  # capex is negative

        # Book value per share ≈ (equity_cap + reserves) / shares
        eq_cap = _nearest_yearly(equity_cap_by_year, qend)
        res    = _nearest_yearly(reserves_by_year, qend)
        # shares_outstanding from equity capital (face value assumed ₹1 or ₹10 — skip if unknown)
        total_equity = ((eq_cap or 0) + (res or 0)) if (eq_cap or res) else None

        rows.append({
            "ticker":             ticker,
            "fiscal_year":        fy,
            "quarter":            q,
            "quarter_end_date":   qend,
            "announcement_date":  ann_date,
            "revenue":            revenue,
            "ebitda":             ebitda,
            "pat":                pat,
            "eps":                eps,
            "operating_margin":   opm,
            "net_margin":         net_margin,
            "depreciation":       depr,
            "interest_coverage":  (ebitda / interest) if (ebitda and interest and interest > 0) else None,
            "fcf":                fcf,
            "total_debt":         _nearest_yearly(total_debt_by_year, qend),
            "roe":                _nearest_yearly(roe_by_year, qend),
            "roce":               _nearest_yearly(roce_by_year, qend),
            # Fields populated from Screener or left None
            "ebitda_margin":      None,
            "debt_to_equity":     None,
            "asset_turnover":     None,
            "inventory_days":     None,
            "receivable_days":    None,
            "payable_days":       None,
            "gross_profit":       None,
            "capex":              abs(capex) if capex else None,
            "current_assets":     None,
            "current_liabilities": None,
            "cash_and_equivalents": None,
            "shares_outstanding": None,
            "book_value_per_share": None,
        })

    return rows


def _build_shareholding_rows(ticker: str, sh: pd.DataFrame) -> list:
    """Build one shareholding dict per quarter from Quarterly_Shareholding_Pattern."""
    rows = []
    for col in sh.columns:
        qend = _quarter_end(str(col))
        if qend is None:
            continue
        filing_date = _announcement_date(qend)  # conservative PIT default

        promoter = _num(sh.loc["Promoters", col]) if "Promoters" in sh.index else None
        fii      = _num(sh.loc["FIIs", col])      if "FIIs"      in sh.index else None
        dii      = _num(sh.loc["DIIs", col])      if "DIIs"      in sh.index else None
        public_  = _num(sh.loc["Public", col])    if "Public"    in sh.index else None
        govt     = _num(sh.loc["Government", col]) if "Government" in sh.index else None

        rows.append({
            "ticker":          ticker,
            "quarter_end_date": qend,
            "filing_date":     filing_date,
            "promoter_pct":    promoter,
            "fii_pct":         fii,
            "dii_pct":         dii,
            "mf_pct":          None,   # MF not separately broken out in this dataset
            "retail_pct":      public_,
            "promoter_pledge": None,
            "superstar_flag":  None,
            "superstar_change": None,
        })
    return rows


# ---------------------------------------------------------------------------
# DB write helpers
# ---------------------------------------------------------------------------

_INSERT_FUNDAMENTALS = """
    INSERT INTO fundamentals (
        ticker, fiscal_year, quarter, quarter_end_date, announcement_date,
        revenue, ebitda, pat, eps, operating_margin, ebitda_margin, net_margin,
        roe, roce, debt_to_equity, interest_coverage, fcf,
        gross_profit, capex, total_debt, cash_and_equivalents,
        shares_outstanding, book_value_per_share, depreciation
    ) VALUES (?,?,?,?,?, ?,?,?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?,?,?)
    ON CONFLICT (ticker, fiscal_year, quarter) DO NOTHING
"""

_INSERT_SHAREHOLDING = """
    INSERT INTO shareholding (
        ticker, quarter_end_date, filing_date,
        promoter_pct, promoter_pledge, fii_pct, dii_pct, mf_pct, retail_pct
    ) VALUES (?,?,?,?,?,?,?,?,?)
    ON CONFLICT (ticker, quarter_end_date) DO NOTHING
"""


def _write_batch(conn, fund_rows: list, sh_rows: list) -> Tuple[int, int]:
    f_written = s_written = 0
    for r in fund_rows:
        try:
            conn.execute(_INSERT_FUNDAMENTALS, [
                r["ticker"], r["fiscal_year"], r["quarter"],
                r["quarter_end_date"], r["announcement_date"],
                r["revenue"], r["ebitda"], r["pat"], r["eps"],
                r["operating_margin"], r["ebitda_margin"], r["net_margin"],
                r["roe"], r["roce"], r["debt_to_equity"], r["interest_coverage"],
                r["fcf"], r["gross_profit"], r["capex"], r["total_debt"],
                r["cash_and_equivalents"], r["shares_outstanding"],
                r["book_value_per_share"], r.get("depreciation"),
            ])
            f_written += 1
        except Exception as exc:
            logger.debug("Fund insert error %s FY%s Q%s: %s",
                         r["ticker"], r["fiscal_year"], r["quarter"], exc)

    for r in sh_rows:
        try:
            conn.execute(_INSERT_SHAREHOLDING, [
                r["ticker"], r["quarter_end_date"], r["filing_date"],
                r["promoter_pct"], r["promoter_pledge"],
                r["fii_pct"], r["dii_pct"], r["mf_pct"], r["retail_pct"],
            ])
            s_written += 1
        except Exception as exc:
            logger.debug("SH insert error %s %s: %s", r["ticker"], r["quarter_end_date"], exc)

    return f_written, s_written


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Load Kaggle per-company folders into DuckDB")
    parser.add_argument("--archive", default=str(DEFAULT_ARCHIVE),
                        help=f"Path to the downloaded archive folder (default: {DEFAULT_ARCHIVE})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and count rows but do not write to DB")
    parser.add_argument("--limit", type=int, default=None,
                        help="Process only N companies (for testing)")
    args = parser.parse_args()

    archive = Path(args.archive)
    data_dir = archive / DATA_SUBDIR
    if not data_dir.exists():
        # Try direct path if user pointed at the inner folder
        data_dir = archive
        if not any(data_dir.iterdir()):
            logger.error("Data directory not found: %s", data_dir)
            sys.exit(1)

    company_dirs = sorted([d for d in data_dir.iterdir() if d.is_dir()])
    if args.limit:
        company_dirs = company_dirs[:args.limit]
    logger.info("Found %d company folders in %s", len(company_dirs), data_dir)

    # Load DB ticker set for fast filtering
    from config.settings import DUCKDB_PATH
    from datastore.api.db import get_duckdb_connection

    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
        db_tickers = {r[0] for r in conn.execute(
            "SELECT DISTINCT ticker FROM ohlcv_adjusted"
        ).fetchall()}
    logger.info("DB has %d distinct tickers to match against", len(db_tickers))

    total_fund = total_sh = matched = skipped = errors = 0
    all_fund_rows, all_sh_rows = [], []

    for i, company_dir in enumerate(company_dirs, start=1):
        ticker = _get_nse_ticker(company_dir)
        if not ticker or ticker not in db_tickers:
            skipped += 1
            continue

        matched += 1
        try:
            qpl    = _parse_wide(company_dir / "Quarterly_Profit_Loss.csv")
            bs     = _parse_wide(company_dir / "Yearly_Balance_Sheet.csv")
            cf     = _parse_wide(company_dir / "Yearly_Cash_flow.csv")
            ratios = _parse_wide(company_dir / "Ratios.csv")
            sh     = _parse_wide(company_dir / "Quarterly_Shareholding_Pattern.csv")

            fund_rows = _build_fundamentals_rows(ticker, qpl, bs, cf, ratios) if qpl is not None else []
            sh_rows   = _build_shareholding_rows(ticker, sh) if sh is not None else []

            all_fund_rows.extend(fund_rows)
            all_sh_rows.extend(sh_rows)
            total_fund += len(fund_rows)
            total_sh   += len(sh_rows)

        except Exception as exc:
            logger.warning("[%d] %s (%s): parse error — %s", i, company_dir.name, ticker, exc)
            errors += 1

        if i % 500 == 0:
            logger.info("[%d/%d] matched=%d skipped=%d fund_rows=%d sh_rows=%d",
                        i, len(company_dirs), matched, skipped, total_fund, total_sh)

    logger.info("Parsing complete: %d matched, %d skipped, %d errors", matched, skipped, errors)
    logger.info("Rows to write: %d fundamentals, %d shareholding", total_fund, total_sh)

    if args.dry_run:
        logger.info("DRY RUN — no writes.")
        return

    # Write in one connection
    f_written = s_written = 0
    with get_duckdb_connection(DUCKDB_PATH, persist=False) as conn:
        f_written, s_written = _write_batch(conn, all_fund_rows, all_sh_rows)
        final_f = conn.execute("SELECT COUNT(*), COUNT(DISTINCT ticker) FROM fundamentals").fetchone()
        final_s = conn.execute("SELECT COUNT(*), COUNT(DISTINCT ticker) FROM shareholding").fetchone()

    logger.info("─" * 60)
    logger.info("Written: %d fundamentals rows, %d shareholding rows", f_written, s_written)
    logger.info("fundamentals table : %d rows, %d tickers", final_f[0], final_f[1])
    logger.info("shareholding table : %d rows, %d tickers", final_s[0], final_s[1])


if __name__ == "__main__":
    main()
