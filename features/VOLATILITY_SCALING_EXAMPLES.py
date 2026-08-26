"""
volatility_scaling.py — Usage Examples

Demonstrates how to use the four individual volatility scaling functions.
"""

import pandas as pd
import numpy as np
from features.volatility_scaling import (
    baseline,
    inverse_volatility,
    inverse_variance,
    target_volatility,
    downside_volatility,
)


# ═══════════════════════════════════════════════════════════════════════════
# Example 1: Load equity curve from backtest and compare all modes
# ═══════════════════════════════════════════════════════════════════════════

def example_compare_all_modes() -> None:
    """Load backtest result, apply all 4 modes, compare outputs."""

    # Step 1: Load portfolio equity curve from backtest
    # (This is pseudo-code; adapt to your data source)
    # equity_curve = load_from_backtest_db(run_id=12345)  # pd.Series(index=dates)
    # For now, create synthetic:
    dates = pd.date_range("2019-01-01", periods=252 * 6, freq="D")  # 6 years
    np.random.seed(42)
    daily_returns = np.random.normal(0.0005, 0.015, len(dates))
    equity_curve = pd.Series(
        np.cumprod(1 + daily_returns) * 1_000_000,
        index=dates,
        name="portfolio_value"
    )

    # Step 2: Apply each mode
    print("Computing volatility scaling multipliers for 5 modes...")
    baseline_mult = baseline(equity_curve)
    inv_vol = inverse_volatility(equity_curve, lookback_days=126)
    inv_var = inverse_variance(equity_curve, lookback_days=126)
    tgt_vol = target_volatility(equity_curve, target_vol=0.15, lookback_days=126)
    dwn_vol = downside_volatility(equity_curve, lookback_days=126)

    # Step 3: Compare tail statistics (where we have enough data)
    tail = equity_curve.iloc[-252:]  # Last year
    tail_idx = tail.index

    print("\n" + "═" * 80)
    print("VOLATILITY SCALING MULTIPLIERS (Last 252 Days)")
    print("═" * 80)

    for mode_name, multipliers in [
        ("baseline (no scaling)", baseline_mult),
        ("inverse_volatility", inv_vol),
        ("inverse_variance", inv_var),
        ("target_volatility", tgt_vol),
        ("downside_volatility", dwn_vol),
    ]:
        tail_values = multipliers.loc[tail_idx].dropna()
        if len(tail_values) > 0:
            print(
                f"\n{mode_name:25} | "
                f"Mean: {tail_values.mean():7.4f} | "
                f"Std:  {tail_values.std():7.4f} | "
                f"Min:  {tail_values.min():7.4f} | "
                f"Max:  {tail_values.max():7.4f}"
            )


# ═══════════════════════════════════════════════════════════════════════════
# Example 2: Apply mode-specific positioning
# ═══════════════════════════════════════════════════════════════════════════

def example_apply_positioning(base_position_size: float = 100_000) -> None:
    """Given base position size, apply volatility scaling to get actual positions."""

    # Load equity curve
    dates = pd.date_range("2023-01-01", periods=252, freq="D")
    np.random.seed(42)
    daily_returns = np.random.normal(0.0005, 0.015, 252)
    equity_curve = pd.Series(
        np.cumprod(1 + daily_returns) * 1_000_000,
        index=dates
    )

    # Get multipliers for each mode
    baseline_mult = baseline(equity_curve)
    inv_vol = inverse_volatility(equity_curve, lookback_days=126)
    inv_var = inverse_variance(equity_curve, lookback_days=126)
    dwn_vol = downside_volatility(equity_curve, lookback_days=126)

    # Apply to position sizing
    dates_to_show = equity_curve.iloc[-10:].index

    print("\n" + "═" * 150)
    print("POSITION SIZING BY VOLATILITY MODE")
    print("=" * 150)
    print(f"Base Position Size: ₹{base_position_size:,}")
    print("─" * 150)
    print(f"{'Date':<12} | {'baseline':>10} | {'Position':>14} | {'inverse_vol':>12} | {'Position':>14} | {'inverse_var':>12} | {'Position':>14} | {'downside_vol':>12} | {'Position':>14}")
    print("─" * 150)

    for date in dates_to_show:
        baseline_mult_val = baseline_mult.loc[date]
        inv_vol_mult = inv_vol.loc[date]
        inv_var_mult = inv_var.loc[date]
        dwn_vol_mult = dwn_vol.loc[date]

        position_baseline = base_position_size * baseline_mult_val
        position_inv_vol = base_position_size * inv_vol_mult
        position_inv_var = base_position_size * inv_var_mult
        position_dwn_vol = base_position_size * dwn_vol_mult

        print(
            f"{date.strftime('%Y-%m-%d')} | "
            f"{baseline_mult_val:>10.4f} | "
            f"₹{position_baseline:>12,.0f} | "
            f"{inv_vol_mult:>12.4f} | "
            f"₹{position_inv_vol:>12,.0f} | "
            f"{inv_var_mult:>12.4f} | "
            f"₹{position_inv_var:>12,.0f} | "
            f"{dwn_vol_mult:>12.4f} | "
            f"₹{position_dwn_vol:>12,.0f}"
        )


# ═══════════════════════════════════════════════════════════════════════════
# Example 3: Band-specific mode selection
# ═══════════════════════════════════════════════════════════════════════════

def example_band_specific_modes() -> None:
    """
    Deploy different modes for different market-cap bands.
    Recreates the Phase 2 band-sweep composite strategy.
    """

    print("\n" + "═" * 100)
    print("BAND-SPECIFIC VOLATILITY MODE SELECTION (Phase 2 Results)")
    print("═" * 100)

    bands_config = [
        {
            "band": "M7",
            "range": "161-275 (mid-cap, safe)",
            "baseline_vol": "28-35%",
            "mode": "inverse_volatility",
            "expected_cagr": "38.95%",
            "sharpe": "1.13",
            "maxdd": "-35.65%",
            "reason": "Low vol → high weights → captures full momentum alpha",
        },
        {
            "band": "M8",
            "range": "201-300 (mid-cap, growth)",
            "baseline_vol": "35-40%",
            "mode": "inverse_variance",
            "expected_cagr": "44.39%",
            "sharpe": "1.23",
            "maxdd": "-45.30%",
            "reason": "Medium vol → balanced leverage → peak returns + stability",
        },
        {
            "band": "M9",
            "range": "276-550 (small-cap, extreme)",
            "baseline_vol": "40-48%",
            "mode": "downside_volatility",
            "expected_cagr": "44.39%",
            "sharpe": "1.23",
            "maxdd": "-45.30%",
            "reason": "High vol + upside drift → asymmetric scaling → captures max alpha",
        },
    ]

    for config in bands_config:
        print(f"\n{config['band']} (Rank {config['range']})")
        print(f"  Baseline Volatility:  {config['baseline_vol']}")
        print(f"  Optimal Mode:         {config['mode']}")
        print(f"  Expected CAGR:        {config['expected_cagr']}")
        print(f"  Sharpe Ratio:         {config['sharpe']}")
        print(f"  Max Drawdown:         {config['maxdd']}")
        print(f"  Why This Mode:        {config['reason']}")

    print("\n" + "─" * 100)
    print("COMPOSITE STRATEGY (Equal-weight blend of 3 optimal bands)")
    print("─" * 100)
    print("Blended CAGR:      ~42.6% (conservative estimate)")
    print("Blended Sharpe:    ~1.20")
    print("Blended MaxDD:     ~-42% (estimated)")
    print("Benefit:           Diversification across regimes; no single-mode catastrophic failure")


# ═══════════════════════════════════════════════════════════════════════════
# Example 4: Testing each mode in isolation
# ═══════════════════════════════════════════════════════════════════════════

def example_test_individual_modes() -> None:
    """Demonstrate testing each mode with pytest."""

    print("\n" + "═" * 100)
    print("TESTING INDIVIDUAL MODES")
    print("═" * 100)

    tests = [
        {
            "test": "test_baseline_mode",
            "command": "pytest tests/unit/test_volatility_scaling_modes.py::TestBaseline -v",
            "what_it_tests": "Constant 1.0 multiplier; control/reference mode",
        },
        {
            "test": "test_inverse_volatility_mode",
            "command": "pytest tests/unit/test_volatility_scaling_modes.py::TestInverseVolatility -v",
            "what_it_tests": "Low-vol → high multipliers; high-vol → low multipliers",
        },
        {
            "test": "test_inverse_variance_mode",
            "command": "pytest tests/unit/test_volatility_scaling_modes.py::TestInverseVariance -v",
            "what_it_tests": "Stable across regimes; smoother than 1/vol",
        },
        {
            "test": "test_target_volatility_mode",
            "command": "pytest tests/unit/test_volatility_scaling_modes.py::TestTargetVolatility -v",
            "what_it_tests": "Conservative cap at 1.0; de-leverages on vol spikes",
        },
        {
            "test": "test_downside_volatility_mode",
            "command": "pytest tests/unit/test_volatility_scaling_modes.py::TestDownsideVolatility -v",
            "what_it_tests": "Penalizes downside only; higher in trending markets",
        },
        {
            "test": "test_mode_differences",
            "command": "pytest tests/unit/test_volatility_scaling_modes.py::TestModeDifferences -v",
            "what_it_tests": "Compare all 5 modes; verify they produce different outputs",
        },
    ]

    for test_spec in tests:
        print(f"\n{test_spec['test']}")
        print(f"  Command:       {test_spec['command']}")
        print(f"  Tests:         {test_spec['what_it_tests']}")

    print("\n" + "─" * 100)
    print("Run all volatility scaling tests:")
    print("  pytest tests/unit/test_volatility_scaling_modes.py -v")


# ═══════════════════════════════════════════════════════════════════════════
# Example 5: Understand the band definitions
# ═══════════════════════════════════════════════════════════════════════════

def example_band_definitions() -> None:
    """Print rank band definitions."""

    print("\n" + "═" * 100)
    print("RANK BAND DEFINITIONS (M1-M12)")
    print("═" * 100)
    print("\nBands are defined by MARKET-CAP RANK (1 = largest, 800 = smallest)")
    print("Each band contains 1-year fixed stock roster (reset Jan 1 each year)\n")

    bands = [
        ("M1", "1-50", "Largest cap, lowest momentum", "7.60%-12.06%", "Weak performers"),
        ("M2", "1-75", "Top 75 by cap (overlapping)", "17.22%", "Transition tier"),
        ("M3", "51-100", "Large-cap", "5.46%-14.71%", "Weak performers"),
        ("M4", "76-160", "Large-mid cap", "11.51%-12.21%", "Transition tier"),
        ("M5", "101-150", "Mid-cap", "12.21%-14.26%", "Transition tier"),
        ("M6", "151-200", "Mid-cap", "11.23%-14.26%", "Transition tier"),
        ("M7", "161-275", "Mid-cap SWEET SPOT", "38.20%-38.95%", "✅ ELITE (conservative)"),
        ("M8", "201-300", "Mid-cap GROWTH", "38.54%-44.39%", "✅ ELITE (peak returns)"),
        ("M9", "276-550", "Small-cap FRONTIER", "10.56%-44.39%", "✅ ELITE (extreme alpha)"),
        ("M10", "301-500", "Small-cap", "-0.94%-6.29%", "Weak (liquidity limits)"),
        ("M11", "501-800", "Micro-cap", "2.76%-4.33%", "Weak (liquidity limits)"),
        ("M12", "551-800", "Micro-cap", "10.56%-12.34%", "Weak (liquidity limits)"),
    ]

    print(f"{'Band':<5} | {'Rank Range':<15} | {'Characteristics':<30} | {'CAGR Range':<20} | {'Status':<25}")
    print("─" * 100)
    for band, rank_range, chars, cagr, status in bands:
        print(f"{band:<5} | {rank_range:<15} | {chars:<30} | {cagr:<20} | {status:<25}")


# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n" + "=" * 100)
    print("VOLATILITY SCALING MODES — COMPREHENSIVE EXAMPLES")
    print("=" * 100)

    example_compare_all_modes()
    example_apply_positioning()
    example_band_specific_modes()
    example_test_individual_modes()
    example_band_definitions()

    print("\n" + "=" * 100)
    print("DOCUMENTATION")
    print("=" * 100)
    print("\n1. Feature module:         features/volatility_scaling.py")
    print("2. Usage guide:            features/VOLATILITY_SCALING_GUIDE.md")
    print("3. Band definitions:       features/momentum_universe.py (RANK_BANDS constant)")
    print("4. Detailed band analysis: backtest/reports/r9_band_definitions.txt")
    print("5. Phase 2 results:        backtest/reports/R9_EXECUTIVE_SUMMARY.txt")
    print("\n" + "=" * 100)
