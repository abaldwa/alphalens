"""
Cross-strategy x cross-band smoke suite — every one of the 13 active
strategies, run natively against a representative sample of bands
(large-cap M2, mid-cap M9, micro-cap M12 — plus M13, the full-universe
band). Deliberately SHORT windows (this is breadth-over-depth smoke
testing, not the trade-by-trade parity check — see
scripts/parity_check.py for that): the goal here is "does every
(strategy, band) combination run to completion without error and
produce sane output," not full-history return validation.

M7 is excluded everywhere — no resolved benchmark index yet (see
common/benchmark.py), so R07/R09-with-regime-switching would fail on it;
excluded uniformly here so this file doesn't need per-strategy carve-outs.
"""


import pytest

from momentum_framework.backtesting.orchestrator import BacktestConfig, BacktestOrchestrator
from momentum_framework.strategies.r01_trailing_momentum import R01TrailingMomentum
from momentum_framework.strategies.r03_jt_skipmonth import R03JTSkipMonth
from momentum_framework.strategies.r07_crash_aware import R07CrashAware
from momentum_framework.strategies.r08_bsc_volscale import R08BSCVolScale
from momentum_framework.strategies.r09_mm_volscale import R09MMVolScale
from momentum_framework.strategies.r10_sector_momentum import R10SectorMomentum
from momentum_framework.strategies.r11_52wk_reversal import R11FiftyTwoWeekReversal
from momentum_framework.strategies.r12_reversal_1mo import R12Reversal1Mo
from momentum_framework.strategies.r13_bollinger_reversal import R13BollingerReversal
from momentum_framework.strategies.r14_inverse_volatility import R14InverseVolatility
from momentum_framework.strategies.r15_inverse_variance import R15InverseVariance
from momentum_framework.strategies.r16_target_volatility import R16TargetVolatility
from momentum_framework.strategies.r17_downside_volatility import R17DownsideVolatility

pytestmark = pytest.mark.real_data

REPRESENTATIVE_BANDS = [2, 9, 12, 13]  # large-cap, mid/small-cap, micro-cap, full-universe

# One factory per strategy, taking band_id -> a fresh instance with
# small, fast-to-run parameters. top_n and lookback_months are
# deliberately small/short for smoke-test speed, not realistic campaign values.
STRATEGY_FACTORIES: dict = {
    "R01": lambda band_id: R01TrailingMomentum(band_id=band_id, top_n=5, lookback_months=3, rebalance_cadence_days=21),
    "R03": lambda band_id: R03JTSkipMonth(band_id=band_id, top_n=5, lookback_months=3, rebalance_cadence_days=21),
    "R07": lambda band_id: R07CrashAware(band_id=band_id, top_n=5, lookback_months=3, rebalance_cadence_days=21),
    "R08": lambda band_id: R08BSCVolScale(band_id=band_id, top_n=5, lookback_months=3, rebalance_cadence_days=21),
    "R09": lambda band_id: R09MMVolScale(band_id=band_id, top_n=5, lookback_months=3, rebalance_cadence_days=21,
                                          vol_scaling_mode="inverse_volatility"),
    "R10": lambda band_id: R10SectorMomentum(band_id=band_id, top_n=5, lookback_months=3, rebalance_cadence_days=21),
    "R11": lambda band_id: R11FiftyTwoWeekReversal(band_id=band_id, top_n=5, lookback_months=12, rebalance_cadence_days=21),
    "R12": lambda band_id: R12Reversal1Mo(band_id=band_id, top_n=5, rebalance_cadence_days=21),
    "R13": lambda band_id: R13BollingerReversal(band_id=band_id, top_n=5, rebalance_cadence_days=21),
    "R14": lambda band_id: R14InverseVolatility(band_id=band_id, top_n=5, lookback_months=3, rebalance_cadence_days=21),
    "R15": lambda band_id: R15InverseVariance(band_id=band_id, top_n=5, lookback_months=3, rebalance_cadence_days=21),
    "R16": lambda band_id: R16TargetVolatility(band_id=band_id, top_n=5, lookback_months=3, rebalance_cadence_days=21),
    "R17": lambda band_id: R17DownsideVolatility(band_id=band_id, top_n=5, lookback_months=3, rebalance_cadence_days=21),
}

# (strategy_code, band_id) pairs — the full cross product, minus nothing
# (all 13 strategies are expected to work on all 4 representative bands).
ALL_COMBINATIONS = [
    (code, band_id)
    for code in STRATEGY_FACTORIES
    for band_id in REPRESENTATIVE_BANDS
]


@pytest.mark.parametrize("strategy_code,band_id", ALL_COMBINATIONS, ids=[f"{c}-M{b}" for c, b in ALL_COMBINATIONS])
def test_strategy_runs_on_band(prod_conn, strategy_code: str, band_id: int):
    """Every (strategy, band) combination must complete a short real
    backtest without error, with integrity passing and at least one
    trade (a strategy that never buys anything on a real 6-month window
    is itself a signal something's wrong)."""
    strategy = STRATEGY_FACTORIES[strategy_code](band_id)
    config = BacktestConfig(start_date="2023-01-01", end_date="2023-06-30", initial_capital=1_000_000)

    result = BacktestOrchestrator(strategy, config).run_native(prod_conn)

    assert result.integrity_passed, f"{strategy_code} on M{band_id}: integrity check failed"
    assert result.trade_count > 0, f"{strategy_code} on M{band_id}: zero trades in 6 months — investigate"
    # strategy_id is BAND-FIRST since 2026-09-04 (e.g. M02_R01_top10_...,
    # not R01_M2_...) — see metrics/nomenclature.py::build_strategy_id()'s
    # docstring. strategy_code appears as the SECOND underscore-delimited
    # token, not a prefix.
    assert result.strategy_id.split("_")[1] == strategy_code, (
        f"strategy_id={result.strategy_id} doesn't have {strategy_code} as its second token"
    )
