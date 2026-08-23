"""
tests/unit/test_sector_momentum.py

Phase 4 (R4): Industry/sector momentum testing.
Tests the two-stage ranking: rank sectors by average momentum, then
rank constituents within top sectors.
"""

import pandas as pd

from features.momentum_strategy import rank_sectors, rank_constituents_within_sectors


class TestRankSectors:
    """rank_sectors: compute sector-level momentum scores."""

    def test_empty_momentum_returns_empty(self):
        momentum = pd.Series(dtype=float)
        sector_lookup = {"A": "Tech", "B": "Finance"}
        result = rank_sectors(momentum, sector_lookup)
        assert result.empty

    def test_empty_sector_lookup_returns_empty(self):
        momentum = pd.Series({"A": 0.10, "B": 0.15})
        result = rank_sectors(momentum, {})
        assert result.empty

    def test_ranks_sectors_by_average_momentum(self):
        momentum = pd.Series({
            "AAPL": 0.10,
            "MSFT": 0.15,
            "TCS": 0.12,
            "INFY": 0.18,
        })
        sector_lookup = {
            "AAPL": "Tech",
            "MSFT": "Tech",
            "TCS": "IT",
            "INFY": "IT",
        }
        result = rank_sectors(momentum, sector_lookup)

        # IT average: (0.12 + 0.18) / 2 = 0.15
        # Tech average: (0.10 + 0.15) / 2 = 0.125
        # So IT should rank first (0.15 > 0.125)
        assert result.index[0] == "IT"
        assert result.index[1] == "Tech"
        assert abs(result["IT"] - 0.15) < 1e-6
        assert abs(result["Tech"] - 0.125) < 1e-6

    def test_unknown_sector_handled(self):
        momentum = pd.Series({
            "AAPL": 0.10,
            "Unknown1": 0.05,
        })
        sector_lookup = {"AAPL": "Tech"}  # Unknown1 not in lookup
        result = rank_sectors(momentum, sector_lookup)

        # Should have "Tech" and "Unknown" sectors
        assert "Tech" in result.index
        assert "Unknown" in result.index
        assert abs(result["Tech"] - 0.10) < 1e-6
        assert abs(result["Unknown"] - 0.05) < 1e-6

    def test_single_ticker_per_sector(self):
        momentum = pd.Series({"A": 0.10, "B": 0.20})
        sector_lookup = {"A": "Sector1", "B": "Sector2"}
        result = rank_sectors(momentum, sector_lookup)

        # Sector averages equal their single ticker's score
        assert abs(result["Sector2"] - 0.20) < 1e-6
        assert abs(result["Sector1"] - 0.10) < 1e-6


class TestRankConstituentsWithinSectors:
    """rank_constituents_within_sectors: filter to only tickers in top sectors."""

    def test_empty_momentum_returns_empty(self):
        momentum = pd.Series(dtype=float)
        sector_lookup = {"A": "Tech"}
        result = rank_constituents_within_sectors(momentum, sector_lookup, ["Tech"])
        assert result.empty

    def test_empty_sector_lookup_returns_momentum_unchanged(self):
        momentum = pd.Series({"A": 0.10, "B": 0.15})
        result = rank_constituents_within_sectors(momentum, {}, ["Tech"])
        # With empty sector_lookup, should return input unchanged (can't determine sectors)
        assert len(result) == 2

    def test_empty_top_sectors_returns_momentum_unchanged(self):
        momentum = pd.Series({"A": 0.10, "B": 0.15})
        sector_lookup = {"A": "Tech", "B": "Finance"}
        result = rank_constituents_within_sectors(momentum, sector_lookup, [])
        # No top sectors specified, returns input unchanged
        assert len(result) == 2

    def test_filters_to_constituents_in_top_sectors(self):
        momentum = pd.Series({
            "AAPL": 0.10,
            "MSFT": 0.15,
            "TCS": 0.12,
            "INFY": 0.18,
        })
        sector_lookup = {
            "AAPL": "Tech",
            "MSFT": "Tech",
            "TCS": "IT",
            "INFY": "IT",
        }
        result = rank_constituents_within_sectors(momentum, sector_lookup, ["IT"])

        # Should only have IT constituents
        assert len(result) == 2
        assert "TCS" in result.index
        assert "INFY" in result.index
        assert "AAPL" not in result.index
        assert "MSFT" not in result.index
        # Scores should be preserved
        assert result["TCS"] == 0.12
        assert result["INFY"] == 0.18

    def test_multiple_top_sectors(self):
        momentum = pd.Series({
            "A": 0.10,
            "B": 0.15,
            "C": 0.12,
            "D": 0.18,
        })
        sector_lookup = {
            "A": "Sector1",
            "B": "Sector1",
            "C": "Sector2",
            "D": "Sector3",
        }
        result = rank_constituents_within_sectors(momentum, sector_lookup, ["Sector1", "Sector2"])

        # Should have constituents from Sector1 and Sector2 only
        assert len(result) == 3
        assert set(result.index) == {"A", "B", "C"}

    def test_no_matching_constituents_returns_empty(self):
        momentum = pd.Series({
            "A": 0.10,
            "B": 0.15,
        })
        sector_lookup = {"A": "Sector1", "B": "Sector2"}
        result = rank_constituents_within_sectors(momentum, sector_lookup, ["Sector3"])

        # No constituents in Sector3
        assert result.empty


class TestIntegration:
    """End-to-end two-stage sector momentum."""

    def test_two_stage_ranking_pipeline(self):
        """Simulate the full two-stage ranking: rank sectors, then constituents."""
        momentum = pd.Series({
            "AAPL": 0.10,
            "MSFT": 0.15,
            "TCS": 0.20,
            "INFY": 0.08,
            "JPM": 0.12,
            "GS": 0.14,
        })
        sector_lookup = {
            "AAPL": "Tech",
            "MSFT": "Tech",
            "TCS": "IT",
            "INFY": "IT",
            "JPM": "Finance",
            "GS": "Finance",
        }

        # Stage 1: Rank sectors
        sector_scores = rank_sectors(momentum, sector_lookup)
        top_sectors_list = sector_scores.head(2).index.tolist()

        # Stage 2: Filter to constituents in top sectors
        filtered = rank_constituents_within_sectors(momentum, sector_lookup, top_sectors_list)

        # IT average: (0.20 + 0.08) / 2 = 0.14
        # Tech average: (0.10 + 0.15) / 2 = 0.125
        # Finance average: (0.12 + 0.14) / 2 = 0.13
        # Top 2: IT (0.14) and Finance (0.13)
        assert "IT" in top_sectors_list
        assert "Finance" in top_sectors_list
        assert "Tech" not in top_sectors_list

        # Filtered should have IT + Finance constituents
        assert len(filtered) == 4
        assert set(filtered.index) == {"TCS", "INFY", "JPM", "GS"}

    def test_preserves_momentum_scores_unchanged(self):
        """Filtering should not modify momentum scores."""
        momentum = pd.Series({
            "A": 0.123456,
            "B": 0.654321,
            "C": 0.111111,
        })
        sector_lookup = {"A": "S1", "B": "S2", "C": "S3"}

        filtered = rank_constituents_within_sectors(momentum, sector_lookup, ["S1", "S2"])

        # Scores should match exactly
        assert filtered["A"] == 0.123456
        assert filtered["B"] == 0.654321
        assert "C" not in filtered.index
