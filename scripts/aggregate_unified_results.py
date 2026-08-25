#!/usr/bin/env python3
"""
Unified Backtest Results Aggregator
Processes results from all stages: pilot validation + M13 matrix + recommendations
Outputs final strategy selection report
"""

import sys
import json
import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime

# Configuration
BASELINE_CSV = Path("backtest_all.csv")
RESULTS_DIR = Path("backtest/reports")
DB_PATH = Path.home() / ".local/share/AlphaLens/data/backtest.duckdb"

def load_baseline():
    """Load existing M1-M12 baseline results"""
    print("[LOAD] Reading baseline backtest_all.csv...")
    df = pd.read_csv(BASELINE_CSV, header=0)
    print(f"  Loaded {len(df)} baseline strategies")
    return df

def query_new_results(db, stage="all"):
    """Query newly completed backtest results from DuckDB"""
    print(f"[QUERY] Fetching backtest results for stage: {stage}...")

    if stage == "pilot":
        query = """
        SELECT
            strategy_key, cagr, sharpe, max_dd, sortino,
            volatility, trades, positive_years, consistency_3y, consistency_5y
        FROM backtest_runs
        WHERE strategy_key LIKE 'm10_%'
          AND start_date = '2019-01-01'
          AND end_date = '2025-12-31'
        ORDER BY sharpe DESC
        """
    elif stage == "m13":
        query = """
        SELECT
            strategy_key, cagr, sharpe, max_dd, sortino,
            volatility, trades, positive_years, consistency_3y, consistency_5y
        FROM backtest_runs
        WHERE strategy_key LIKE 'm13_%'
          AND start_date = '2008-04-01'
          AND end_date = '2026-06-30'
        ORDER BY sharpe DESC
        """
    else:  # all new results
        query = """
        SELECT
            strategy_key, cagr, sharpe, max_dd, sortino,
            volatility, trades, positive_years, consistency_3y, consistency_5y
        FROM backtest_runs
        WHERE start_date IN ('2019-01-01', '2008-04-01')
        ORDER BY sharpe DESC
        """

    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        results = conn.execute(query).fetch_all()
        columns = [desc[0] for desc in conn.description]
        df = pd.DataFrame(results, columns=columns)
        conn.close()
        print(f"  Fetched {len(df)} results from DuckDB")
        return df
    except Exception as e:
        print(f"  ERROR querying DuckDB: {e}")
        print("  (This is OK if backtest is still running)")
        return pd.DataFrame()

def validate_pilot(baseline_df, pilot_df):
    """Validate pilot results against baseline"""
    print("\n[VALIDATE_PILOT] Comparing M10 subset to baseline...")

    if pilot_df.empty:
        print("  ⚠️  No pilot results yet; skipping validation")
        return False

    # Baseline M10 strategies (from backtest_all.csv)
    baseline_m10 = baseline_df[baseline_df['Name'].str.contains('M10_', na=False)]

    if baseline_m10.empty:
        print("  ⚠️  No baseline M10 results found")
        return False

    baseline_sharpe = baseline_m10['Sharpe'].max()
    pilot_sharpe = pilot_df['sharpe'].max()

    print(f"  Baseline M10 max Sharpe: {baseline_sharpe:.2f}")
    print(f"  Pilot M10 max Sharpe:    {pilot_sharpe:.2f}")
    print(f"  Difference: {(pilot_sharpe - baseline_sharpe):.3f}")

    if pilot_sharpe > baseline_sharpe * 0.95:
        print("  ✓ PASS: Pilot results within 5% of baseline")
        return True
    else:
        print("  ✗ FAIL: Pilot Sharpe significantly below baseline")
        return False

def select_best_per_band_m1_m12(baseline_df):
    """Extract best M1-M12 strategy per overlapping band pair"""
    print("\n[SELECT] Identifying best M1-M12 strategies per band...")

    band_pairs = {
        "M1/M2": ("M1_", "M2_"),
        "M3/M4": ("M3_", "M4_"),
        "M5/M6": ("M5_", "M6_"),
        "M7/M8": ("M7_", "M8_"),
        "M9/M10": ("M9_", "M10_"),
        "M11/M12": ("M11_", "M12_"),
    }

    selections = {}
    for pair_name, (band1_prefix, band2_prefix) in band_pairs.items():
        band1 = baseline_df[baseline_df['Name'].str.contains(band1_prefix, na=False)]
        band2 = baseline_df[baseline_df['Name'].str.contains(band2_prefix, na=False)]

        if band1.empty and band2.empty:
            print(f"  ⚠️  {pair_name}: No results found")
            continue

        best1 = band1.nlargest(1, 'Sharpe') if not band1.empty else None
        best2 = band2.nlargest(1, 'Sharpe') if not band2.empty else None

        winner = None
        winner_name = None
        winner_sharpe = -999

        if best1 is not None and not best1.empty:
            sharpe1 = best1['Sharpe'].iloc[0]
            if sharpe1 > winner_sharpe:
                winner = best1.iloc[0]
                winner_name = f"{band1_prefix}(baseline)"
                winner_sharpe = sharpe1

        if best2 is not None and not best2.empty:
            sharpe2 = best2['Sharpe'].iloc[0]
            if sharpe2 > winner_sharpe:
                winner = best2.iloc[0]
                winner_name = f"{band2_prefix}(baseline)"
                winner_sharpe = sharpe2

        if winner is not None:
            selections[pair_name] = {
                "strategy": winner['Name'],
                "sharpe": winner['Sharpe'],
                "cagr": winner['CAGR (pre-tax)'],
                "max_dd": winner['Max drawdown'],
                "selected_from": winner_name,
            }
            print(f"  ✓ {pair_name}: {winner['Name']}")
            print(f"    → Sharpe {winner['Sharpe']:.2f}, CAGR {winner['CAGR (pre-tax)']}, MaxDD {winner['Max drawdown']}")

    return selections

def rank_m13_results(m13_df):
    """Rank M13 results by Sharpe"""
    print("\n[RANK_M13] Top M13 strategies by Sharpe...")

    if m13_df.empty:
        print("  ⚠️  No M13 results yet")
        return []

    top_10 = m13_df.nlargest(10, 'sharpe')[['strategy_key', 'sharpe', 'cagr', 'max_dd']]
    print(top_10.to_string(index=False))

    return top_10.to_dict('records')

def generate_final_report(baseline_selections, m13_top, output_path):
    """Generate final strategy recommendation report"""
    print("\n[REPORT] Generating final recommendation report...")

    report = {
        "generated_at": datetime.now().isoformat(),
        "pipeline_stage": "Unified Backtest Complete",
        "m1_m12_selections": baseline_selections,
        "m13_top_10": m13_top,
        "summary": {
            "core_strategies_deployed": len(baseline_selections),
            "m13_alternatives": len(m13_top),
            "total_candidates": len(baseline_selections) + len(m13_top),
        },
        "next_steps": [
            "1. Review core M1-M12 strategies (1 per band)",
            "2. Evaluate M13 alternatives for full-universe deployment",
            "3. If M13 outperforms M11/M12: replace with M13 Top 30/40/50",
            "4. Apply Phase 7 crash-aware overlay to selected strategies",
            "5. Gate on Phase 7 validation before paper trading",
        ]
    }

    output_file = RESULTS_DIR / output_path
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w') as f:
        json.dump(report, f, indent=2)

    print(f"  Report saved to: {output_file}")
    return report

def main():
    print("=" * 70)
    print("AlphaLens Unified Backtest Results Aggregator")
    print(f"Started: {datetime.now()}")
    print("=" * 70)
    print()

    # Load baseline
    baseline_df = load_baseline()

    # Query new results
    pilot_df = query_new_results(duckdb.connect(str(DB_PATH)), stage="pilot")
    m13_df = query_new_results(duckdb.connect(str(DB_PATH)), stage="m13")

    # Validate pilot
    pilot_valid = validate_pilot(baseline_df, pilot_df)

    if not pilot_valid:
        print("\n⚠️  PILOT VALIDATION FAILED")
        print("This could indicate:")
        print("  - Data pipeline issues")
        print("  - Backtest queue still running (check logs)")
        print("  - DuckDB lock contention")
        print("\nRetry after verifying:")
        print("  tail -f execution_logs/unified_backtest_*.log")
        return 1

    # Select best M1-M12 per band
    baseline_selections = select_best_per_band_m1_m12(baseline_df)

    # Rank M13
    m13_top = rank_m13_results(m13_df)

    # Generate final report
    report = generate_final_report(baseline_selections, m13_top, "unified_results_final.json")

    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Core M1-M12 Strategies:     {report['summary']['core_strategies_deployed']}")
    print(f"M13 Alternatives Available: {report['summary']['m13_alternatives']}")
    print(f"Total Candidates:           {report['summary']['total_candidates']}")
    print()
    print("NEXT STEPS:")
    for step in report['next_steps']:
        print(f"  {step}")
    print()
    print(f"Full report: {RESULTS_DIR / 'unified_results_final.json'}")
    print("=" * 70)

    return 0

if __name__ == "__main__":
    sys.exit(main())
