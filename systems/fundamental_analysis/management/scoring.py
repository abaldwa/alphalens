"""
systems/fundamental_analysis/management/scoring.py

Fills the "management/" location systems/copilot/strategy_spec.py and
features/fundamental_composites.py both documented as never having been
built. The scoring logic itself already exists and is exercised by live
consumers (datastore/api/routers/fundamentals.py) — re-exported here
rather than duplicated so there is exactly one implementation.
"""

from features.fundamental_composites import management_quality_score

__all__ = ["management_quality_score"]
