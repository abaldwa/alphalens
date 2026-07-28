"""
tests/unit/test_fundamental_analysis_reexports.py

systems/fundamental_analysis/management/scoring.py and
systems/fundamental_analysis/peers/selection.py are thin re-export shims
(fill the "management/"/"peers/" locations systems/copilot/strategy_spec.py
and features/fundamental_composites.py both documented as never having
been built) — never imported by any existing test, 0% coverage. Confirms
the re-export actually resolves to the real canonical implementation, not
a stub/placeholder.
"""

import numpy as np

from features.fundamental_composites import management_quality_score, select_peers
from systems.fundamental_analysis.management.scoring import (
    management_quality_score as reexported_management_quality_score,
)
from systems.fundamental_analysis.peers.selection import select_peers as reexported_select_peers


def test_management_scoring_reexports_the_canonical_function():
    assert reexported_management_quality_score is management_quality_score
    result = reexported_management_quality_score({"promoter_pledge": 10.0})
    assert result == management_quality_score({"promoter_pledge": 10.0})
    assert result is not None


def test_management_scoring_returns_none_when_pledge_missing():
    assert reexported_management_quality_score({}) is None
    assert reexported_management_quality_score({"promoter_pledge": np.nan}) is None


def test_peers_selection_reexports_the_canonical_function():
    assert reexported_select_peers is select_peers
    result = reexported_select_peers("A", panel=__import__("pandas").DataFrame({"ticker": []}), sector_map={}, mcap_map={})
    assert result == []
