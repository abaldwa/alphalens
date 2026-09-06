"""
scripts/export_price_continuity_findings.py

Export all price-continuity findings (gaps >= 5% on corporate-action dates)
from the comprehensive scan, organized for batch processing and remediation.

This script generates multiple outputs:
1. findings_by_severity.csv — all findings ranked by gap size
2. findings_by_ticker_count.csv — tickers with highest finding count
3. remediation_priority_queue.json — tickers prioritized for empirical fix
4. summary_statistics.csv — aggregate counts and distributions

The findings inventory is essential context for understanding data quality
gaps and their impact on backtests. Most gaps (2000+ minor, 1800+ major)
require either:
a. Empirical backward-adjustment (compute factor from pre/post OHLCV)
b. Investigation of missing/misaligned corporate-action registry entries
c. Fyers re-pull (for segment migration issues)
d. Manual decision to exclude from backtests

Usage:
    python3 scripts/export_price_continuity_findings.py --output /path/to/output/dir

Output files go to --output directory (default: ./findings_export/)
"""

import argparse
import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, ".")
from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from datastore.integrity.checks import check_corporate_action_continuity


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--output", default="findings_export", help="Output directory")
    args = ap.parse_args()

    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Exporting price-continuity findings to {out_dir}...\n")

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        # Run full history scan
        as_of_date = date.today()
        print("Running full history scan (3-5 minutes)...")
        findings = check_corporate_action_continuity(conn, as_of_date, lookback_days=None)
        print(f"✓ Found {len(findings)} price gaps >= 5%\n")

        # Parse findings into structured data
        data = []
        for f in findings:
            # Extract fields from description:
            # "TICKER: gap_pct% close-to-close gap at ACTION_TYPE ex_date=YYYY-MM-DD ... (ratio=X)"
            parts = f.description.split(":")
            if len(parts) < 2:
                continue

            ticker = parts[0].strip()
            rest = parts[1].strip()

            # Extract gap_pct
            gap_str = rest.split("%")[0].strip()
            try:
                gap_pct = float(gap_str)
            except ValueError:
                continue

            # Extract action type
            action_type = rest.split("at")[-1].split("ex_date")[0].strip() if "at" in rest else "UNKNOWN"

            # Extract ex_date
            ex_date_str = rest.split("ex_date=")[-1].split()[0] if "ex_date=" in rest else None

            # Extract ratio
            ratio_str = rest.split("(ratio=")[-1].split(")")[0] if "(ratio=" in rest else None
            try:
                ratio = float(ratio_str) if ratio_str else None
            except ValueError:
                ratio = None

            data.append(
                {
                    "ticker": ticker,
                    "gap_pct": gap_pct,
                    "action_type": action_type,
                    "ex_date": ex_date_str,
                    "ratio": ratio,
                    "severity": f.severity,
                }
            )

        df_all = pd.DataFrame(data)

        # 1. Export by severity/gap size
        df_by_severity = df_all.sort_values("gap_pct", ascending=False)
        df_by_severity.to_csv(out_dir / "findings_by_severity.csv", index=False)
        print(f"✓ {out_dir / 'findings_by_severity.csv'}")
        print(
            f"  Distribution:\n"
            f"    >= 50% gaps: {len(df_by_severity[df_by_severity['gap_pct'] >= 50])}\n"
            f"    20-50% gaps: {len(df_by_severity[(df_by_severity['gap_pct'] >= 20) & (df_by_severity['gap_pct'] < 50)])}\n"
            f"    5-20% gaps: {len(df_by_severity[(df_by_severity['gap_pct'] >= 5) & (df_by_severity['gap_pct'] < 20)])}\n"
        )

        # 2. Export by ticker count
        ticker_counts = df_all.groupby("ticker").size().reset_index(name="gap_count")
        ticker_counts = ticker_counts.sort_values("gap_count", ascending=False)
        ticker_counts.to_csv(out_dir / "findings_by_ticker_count.csv", index=False)
        print(f"\n✓ {out_dir / 'findings_by_ticker_count.csv'}")
        print("  Top 10 problematic tickers:\n" + ticker_counts.head(10).to_string(index=False) + "\n")

        # 3. Compute remediation priority (high gaps + multiple events)
        df_all["gap_priority"] = df_all["gap_pct"] / 10  # >50% = score 5+
        df_all["complexity"] = df_all.groupby("ticker")["ticker"].transform("size")  # multiple events = harder

        priority_by_ticker = (
            df_all.groupby("ticker")
            .agg(
                max_gap_pct=("gap_pct", "max"),
                num_gaps=("gap_pct", "count"),
                avg_gap_pct=("gap_pct", "mean"),
                action_types=("action_type", lambda x: ",".join(x.unique())),
            )
            .reset_index()
        )
        priority_by_ticker["priority_score"] = (
            priority_by_ticker["max_gap_pct"] * 1.5 + priority_by_ticker["num_gaps"] * 2
        )
        priority_by_ticker = priority_by_ticker.sort_values("priority_score", ascending=False)

        priority_by_ticker.to_csv(out_dir / "remediation_priority_queue.csv", index=False)
        print(f"✓ {out_dir / 'remediation_priority_queue.csv'}")
        print("  Top 10 by remediation priority:\n" + priority_by_ticker.head(10).to_string(index=False) + "\n")

        # 4. Summary statistics
        summary = pd.DataFrame(
            [
                {
                    "metric": "Total gaps found",
                    "count": len(df_all),
                },
                {
                    "metric": "Unique tickers affected",
                    "count": df_all["ticker"].nunique(),
                },
                {
                    "metric": "Gaps >= 50%",
                    "count": len(df_all[df_all["gap_pct"] >= 50]),
                },
                {
                    "metric": "Gaps 20-50%",
                    "count": len(df_all[(df_all["gap_pct"] >= 20) & (df_all["gap_pct"] < 50)]),
                },
                {
                    "metric": "Gaps 5-20%",
                    "count": len(df_all[(df_all["gap_pct"] >= 5) & (df_all["gap_pct"] < 20)]),
                },
                {
                    "metric": "Avg gap size",
                    "count": df_all["gap_pct"].mean(),
                },
                {
                    "metric": "Median gap size",
                    "count": df_all["gap_pct"].median(),
                },
            ]
        )
        summary.to_csv(out_dir / "summary_statistics.csv", index=False)
        print(f"✓ {out_dir / 'summary_statistics.csv'}")
        print(summary.to_string(index=False))

        print(f"\n✓ Export complete. All findings in: {out_dir}")
        print("\nNext steps:")
        print("  1. Review remediation_priority_queue.csv for highest-impact fixes")
        print("  2. Run compute_empirical_corrections.py on top-100 tickers")
        print("  3. Use apply_empirical_corrections.py to batch-apply fixes")


if __name__ == "__main__":
    main()
