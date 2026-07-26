"""
systems/fundamental_analysis/peers/selection.py

Fills the "peers/" location systems/copilot/strategy_spec.py and
features/fundamental_composites.py both documented as never having been
built. Re-exports the existing implementation rather than duplicating it.
"""

from features.fundamental_composites import select_peers

__all__ = ["select_peers"]
