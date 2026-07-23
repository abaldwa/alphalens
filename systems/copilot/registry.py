"""
systems/copilot/registry.py

File-backed registry of saved Co-Pilot strategies — one YAML file per
strategy under <repo_root>/strategies/, per user decision (no new DuckDB
table for this). Pure filesystem I/O, no synthetic entries.
"""

import re
from pathlib import Path
from typing import List

import yaml

from systems.copilot.strategy_spec import StrategySpec

REPO_ROOT = Path(__file__).resolve().parents[2]
STRATEGIES_DIR = REPO_ROOT / "strategies"

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def slugify(name: str) -> str:
    slug = _SLUG_RE.sub("-", name.strip().lower()).strip("-")
    return slug or "strategy"


def load_all() -> List[StrategySpec]:
    if not STRATEGIES_DIR.exists():
        return []
    specs = []
    for path in sorted(STRATEGIES_DIR.glob("*.yaml")):
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        if data:
            specs.append(StrategySpec.from_dict(data))
    return specs


def load_one(slug: str) -> StrategySpec:
    path = STRATEGIES_DIR / f"{slug}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"No saved strategy at {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return StrategySpec.from_dict(data)


def save(spec: StrategySpec) -> str:
    """Write spec to strategies/<slug>.yaml, returning the slug used."""
    STRATEGIES_DIR.mkdir(parents=True, exist_ok=True)
    slug = slugify(spec.name)
    path = STRATEGIES_DIR / f"{slug}.yaml"
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(spec.to_dict(), f, sort_keys=False)
    return slug
