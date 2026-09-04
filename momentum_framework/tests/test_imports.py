"""
Import smoke test — every module in momentum_framework must import
cleanly. Catches the class of bug this session hit repeatedly during
development (a formatter silently stripping an import that a later edit
then relied on) before it reaches a real run.
"""

import importlib
from pathlib import Path

import pytest

FRAMEWORK_ROOT = Path(__file__).resolve().parent.parent


def _all_module_names():
    names = []
    for path in FRAMEWORK_ROOT.rglob("*.py"):
        if "tests" in path.parts or "__pycache__" in path.parts:
            continue
        rel = path.relative_to(FRAMEWORK_ROOT.parent)
        module = str(rel.with_suffix("")).replace("/", ".")
        names.append(module)
    return sorted(names)


@pytest.mark.parametrize("module_name", _all_module_names())
def test_module_imports_cleanly(module_name):
    importlib.import_module(module_name)


def test_all_13_strategies_expose_both_classes():
    """Every strategies/r{NN}_*.py must export a StrategyAdapter subclass
    AND a QueueGenerator subclass — the documented per-strategy contract."""
    from momentum_framework.backtesting.adapter import StrategyAdapter
    from momentum_framework.queues.generator import QueueGenerator

    strategy_modules = [
        "momentum_framework.strategies.r01_trailing_momentum",
        "momentum_framework.strategies.r03_jt_skipmonth",
        "momentum_framework.strategies.r07_crash_aware",
        "momentum_framework.strategies.r08_bsc_volscale",
        "momentum_framework.strategies.r09_mm_volscale",
        "momentum_framework.strategies.r10_sector_momentum",
        "momentum_framework.strategies.r11_52wk_reversal",
        "momentum_framework.strategies.r12_reversal_1mo",
        "momentum_framework.strategies.r13_bollinger_reversal",
        "momentum_framework.strategies.r14_inverse_volatility",
        "momentum_framework.strategies.r15_inverse_variance",
        "momentum_framework.strategies.r16_target_volatility",
        "momentum_framework.strategies.r17_downside_volatility",
    ]
    assert len(strategy_modules) == 13, "Expected exactly 13 active strategy modules"

    for module_name in strategy_modules:
        module = importlib.import_module(module_name)
        classes = [getattr(module, n) for n in dir(module) if isinstance(getattr(module, n), type)]
        adapters = [c for c in classes if issubclass(c, StrategyAdapter) and c is not StrategyAdapter]
        generators = [c for c in classes if issubclass(c, QueueGenerator) and c is not QueueGenerator]
        assert adapters, f"{module_name} has no StrategyAdapter subclass"
        assert generators, f"{module_name} has no QueueGenerator subclass"
        assert hasattr(module, "STRATEGY_CODE"), f"{module_name} missing STRATEGY_CODE constant"
        assert hasattr(module, "RANK_METHOD"), f"{module_name} missing RANK_METHOD constant"
