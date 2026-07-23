"""tests/unit/test_copilot_registry.py — YAML strategy registry, real filesystem I/O."""

import systems.copilot.registry as registry_mod
from systems.copilot.strategy_spec import StrategySpec


def test_slugify_normalises_name():
    assert registry_mod.slugify("RSI Dip / Large Caps!") == "rsi-dip-large-caps"


def test_save_and_load_one_round_trip(tmp_path, monkeypatch):
    monkeypatch.setattr(registry_mod, "STRATEGIES_DIR", tmp_path)
    spec = StrategySpec(name="My Strategy", description="d", source_query="q")

    slug = registry_mod.save(spec)
    loaded = registry_mod.load_one(slug)

    assert slug == "my-strategy"
    assert loaded.name == "My Strategy"
    assert (tmp_path / "my-strategy.yaml").exists()


def test_load_all_returns_every_saved_strategy(tmp_path, monkeypatch):
    monkeypatch.setattr(registry_mod, "STRATEGIES_DIR", tmp_path)
    registry_mod.save(StrategySpec(name="Strategy One", description="", source_query=""))
    registry_mod.save(StrategySpec(name="Strategy Two", description="", source_query=""))

    specs = registry_mod.load_all()

    assert {s.name for s in specs} == {"Strategy One", "Strategy Two"}


def test_load_all_empty_when_no_directory(tmp_path, monkeypatch):
    monkeypatch.setattr(registry_mod, "STRATEGIES_DIR", tmp_path / "does-not-exist")
    assert registry_mod.load_all() == []
