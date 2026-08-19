"""
tests/quality/test_sweep_optimisations_wired.py

Phase: Backtest sweep performance (2026-08-19)
Owner: Platform / Backtest
Consumers: CI

The user's stated requirement, made checkable: "We want to ensure that the
Backtest Module takes this route for Backtests and not other routes" and "I
am keen that we do not have to reinvent the wheel over and again."

The sweep optimisations measured on 2026-08-19 (31.2s -> 17.3s warm per
momentum job) are three call-site wirings. Each is one keyword argument deep
in a large module, and each is individually easy to drop in a refactor
without any test noticing -- the run still SUCCEEDS, it just silently costs
2x again. That is precisely the failure this file exists to catch: a
performance regression that no correctness test can see.

STATIC BY DESIGN. These assert the callers are WIRED, which is the property
that cannot be verified by running one job -- a single job legitimately
misses every cache. It takes a whole sweep for the wiring to pay, and a
sweep is too expensive to run in CI.

WHAT THIS DOES NOT PROVE
------------------------
It does not prove a sweep is fast, and it does not prove an operator chose
run_sweep_inprocess over the subprocess-per-job routes. Route choice is a
launch-time decision made outside the code; see the module docstring of
backtest/run_sweep_inprocess.py for why the other routes structurally cannot
share panels.
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def _keywords_at_calls(path: str, func_name: str) -> set:
    """Every keyword argument name used at any call to `func_name` in `path`."""
    tree = ast.parse((REPO_ROOT / path).read_text())
    found = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        fn = node.func
        name = fn.attr if isinstance(fn, ast.Attribute) else getattr(fn, "id", None)
        if name == func_name:
            found.update(kw.arg for kw in node.keywords if kw.arg)
    return found


def test_momentum_universe_provider_gets_the_snapshot_cache():
    """MEASURED 66.3s per call, identical for every band and strategy in a
    sweep -- the single largest per-job cost. Momentum-only: it lives in
    _momentum_rank_band_wiring, so technical never benefits from it."""
    kwargs = _keywords_at_calls(
        "backtest/run_orchestrator_backtest.py", "build_momentum_universe_provider"
    )
    assert "snapshot_cache" in kwargs, (
        "run_orchestrator_backtest no longer passes snapshot_cache= to "
        "build_momentum_universe_provider. Every job will rebuild the ranked "
        "universe from scratch (~66s each). See _momentum_rank_band_wiring."
    )


def test_pit_rank_map_gets_the_shares_cache():
    """Applies to BOTH channels -- _get_market_cap_rank_for_date has no
    channel gate. Only the per-ticker shares lookup is shared; the rank MAP
    stays per-orchestrator because it ranks within the list it is given, and
    sharing that would be a correctness bug, not a speedup."""
    kwargs = _keywords_at_calls("backtest/core/engine.py", "get_market_cap_rank_map_as_of")
    assert "shares_cache" in kwargs, (
        "engine.py no longer passes shares_cache= to get_market_cap_rank_map_as_of; "
        "PIT shares outstanding will be re-queried per date per strategy."
    )


def test_deferred_feature_log_is_always_drained():
    """Deferral leaves spill files on disk instead of loading them per job.
    That is only safe because the sweep drains them at the end -- without the
    drain, the rows are silently never loaded and the run still reports ok."""
    source = (REPO_ROOT / "backtest/run_sweep_inprocess.py").read_text()
    assert "drain_feature_log_spills" in source, (
        "run_sweep_inprocess no longer drains feature-log spills. With "
        "defer_feature_log=True the spill files are the ONLY copy."
    )
    tree = ast.parse(source)
    drains = [
        n for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and getattr(n.func, "id", getattr(n.func, "attr", None)) == "drain_feature_log_spills"
    ]
    assert drains, "drain_feature_log_spills is referenced but never called"


def test_defer_feature_log_stays_off_the_cli():
    """Deliberately kwarg-only. A CLI flag would let run_strategy_queue (which
    has no drain step) defer the feature log and orphan every spill file.
    Reachable only from run_sweep_inprocess, which always drains."""
    source = (REPO_ROOT / "backtest/run_orchestrator_backtest.py").read_text()
    assert "--defer-feature-log" not in source, (
        "defer_feature_log gained a CLI flag on run_orchestrator_backtest. Any "
        "subprocess-per-job route can now defer without draining, silently "
        "losing every feature-log row. Keep it kwarg-only."
    )


def test_sweep_forwards_defer_feature_log_to_every_job():
    """A sweep that accepts --defer-feature-log but forwards it to no job
    would drain an empty spill dir and report success."""
    from backtest.run_sweep_inprocess import run_sweep

    assert "defer_feature_log" in inspect.signature(run_sweep).parameters
    kwargs = _keywords_at_calls(
        "backtest/run_sweep_inprocess.py", "run_orchestrator_backtest"
    )
    assert "defer_feature_log" in kwargs, (
        "run_sweep_inprocess accepts defer_feature_log but does not forward it "
        "to run_orchestrator_backtest."
    )


def test_shared_panels_clear_resets_the_new_caches():
    """clear() is what keeps a test, or a live/paper caller, from reading a
    stale sweep's memo. A cache added without a matching clear() line is a
    cross-run contamination bug waiting to happen."""
    from backtest import shared_panels

    source = inspect.getsource(shared_panels.clear)
    for cache in ("_universe_snapshot_cache", "_pit_shares_cache"):
        assert cache in source, f"shared_panels.clear() does not clear {cache}"


def test_stored_config_records_the_rebalance_cadence():
    """Cadence must survive in config_json, not only as a word in strategy_id.

    Until 2026-08-19 the only record that a run rebalanced every 42 days
    rather than 21 was the substring "bimonthly" inside its strategy_id.
    Anything reading the stored config -- the report, a downstream analysis,
    a rerun -- had to parse a NAME to recover a NUMBER, and a strategy named
    by hand or renamed would simply lie. Both BacktestRun construction sites
    (_run_immediate and the deferred path) must carry the key.
    """
    tree = ast.parse((REPO_ROOT / "backtest/run_orchestrator_backtest.py").read_text())
    configs = [
        kw.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and getattr(node.func, "id", None) == "BacktestRun"
        for kw in node.keywords
        if kw.arg == "config"
    ]
    assert len(configs) == 2, (
        f"expected 2 BacktestRun(config=...) sites, found {len(configs)} -- "
        "a new one was added without this guard being widened."
    )
    for cfg in configs:
        keys = {k.value for k in cfg.keys if isinstance(k, ast.Constant)}
        assert "rebalance_cadence_days" in keys, (
            "a BacktestRun config dict omits rebalance_cadence_days; the run's "
            "cadence would again be recoverable only by parsing strategy_id."
        )
