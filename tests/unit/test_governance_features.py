"""
tests/unit/test_governance_features.py

Phase: 2.1 (Fundamental Data Ingestion + PIT Validation)
Specs: SPEC-FEAT-002, SPEC-PIPE-003 (CRITICAL)
Owner: Platform / QA
Consumers: CI, pytest

Tests features/governance.py's QoQ deltas and the two composite flags
(promoter_pledge_spiral_flag, institutional_conviction_flag), using a
fake DataStoreClient (SPEC-SOLID-005 — no real HTTP call).
"""

from datetime import datetime
from unittest.mock import MagicMock

import numpy as np

from features.governance import GOVERNANCE_FEATURES, compute_governance_features, compute_governance_features_panel


def _filing(qed, fd, **kwargs):
    row = {
        "ticker": "TEST", "quarter_end_date": qed, "filing_date": fd,
        "promoter_pct": None, "promoter_pledge": None, "fii_pct": None,
        "dii_pct": None, "mf_pct": None, "retail_pct": None,
    }
    row.update(kwargs)
    return row


class TestGovernanceFeatureCount:
    def test_thirteen_features(self):
        # 12 original P2.1 features + institutional_ownership_pct (added
        # for Under-followed Growth Improvers / Governance-Aware Quality Growth).
        assert len(GOVERNANCE_FEATURES) == 13


class TestComputeGovernanceFeatures:
    def test_qoq_changes(self):
        client = MagicMock()
        client.get_shareholding_history.return_value = [
            _filing("2025-03-31", "2025-04-21", promoter_pct=50.0, fii_pct=20.0, dii_pct=15.0, mf_pct=5.0),
            _filing("2025-06-30", "2025-07-21", promoter_pct=51.5, fii_pct=19.0, dii_pct=16.0, mf_pct=5.5),
        ]
        client.get_ohlcv.return_value = []

        feats = compute_governance_features(client, "TEST", as_of=datetime(2025, 8, 1))

        assert feats["promoter_change_qoq"] == 1.5
        assert feats["fii_change_qoq"] == -1.0
        assert feats["dii_change_qoq"] == 1.0
        assert feats["mf_change_qoq"] == 0.5

    def test_no_history_returns_nan_and_zero_flags(self):
        client = MagicMock()
        client.get_shareholding_history.return_value = []

        feats = compute_governance_features(client, "NEWCO", as_of=datetime(2025, 8, 1))

        assert feats["promoter_pledge_spiral_flag"] == 0
        assert feats["institutional_conviction_flag"] == 0
        assert np.isnan(feats["promoter_pct"])

    def test_single_quarter_has_no_qoq_change(self):
        client = MagicMock()
        client.get_shareholding_history.return_value = [
            _filing("2025-03-31", "2025-04-21", promoter_pct=50.0),
        ]
        client.get_ohlcv.return_value = []

        feats = compute_governance_features(client, "TEST", as_of=datetime(2025, 8, 1))

        assert np.isnan(feats["promoter_change_qoq"])
        assert feats["promoter_pct"] == 50.0


class TestPromoterPledgeSpiralFlag:
    def test_high_pledge_and_falling_price_sets_flag(self):
        client = MagicMock()
        client.get_shareholding_history.return_value = [
            _filing("2025-03-31", "2025-04-21", promoter_pct=40.0, promoter_pledge=35.0),
        ]
        client.get_ohlcv.return_value = [
            {"date": "2025-04-01", "close": 100.0},
            {"date": "2025-06-01", "close": 70.0},  # falling
        ]

        feats = compute_governance_features(client, "TEST", as_of=datetime(2025, 6, 1))
        assert feats["promoter_pledge_spiral_flag"] == 1

    def test_high_pledge_but_rising_price_does_not_set_flag(self):
        client = MagicMock()
        client.get_shareholding_history.return_value = [
            _filing("2025-03-31", "2025-04-21", promoter_pct=40.0, promoter_pledge=35.0),
        ]
        client.get_ohlcv.return_value = [
            {"date": "2025-04-01", "close": 100.0},
            {"date": "2025-06-01", "close": 130.0},  # rising
        ]

        feats = compute_governance_features(client, "TEST", as_of=datetime(2025, 6, 1))
        assert feats["promoter_pledge_spiral_flag"] == 0

    def test_low_pledge_never_sets_flag_regardless_of_price(self):
        client = MagicMock()
        client.get_shareholding_history.return_value = [
            _filing("2025-03-31", "2025-04-21", promoter_pct=40.0, promoter_pledge=5.0),
        ]
        client.get_ohlcv.return_value = [
            {"date": "2025-04-01", "close": 100.0},
            {"date": "2025-06-01", "close": 50.0},
        ]

        feats = compute_governance_features(client, "TEST", as_of=datetime(2025, 6, 1))
        assert feats["promoter_pledge_spiral_flag"] == 0
        # And the OHLCV call shouldn't even be made when pledge is below threshold
        client.get_ohlcv.assert_not_called()


class TestInstitutionalConvictionFlag:
    def test_all_three_increasing_sets_flag(self):
        client = MagicMock()
        client.get_shareholding_history.return_value = [
            _filing("2025-03-31", "2025-04-21", fii_pct=18.0, dii_pct=14.0, mf_pct=5.0),
            _filing("2025-06-30", "2025-07-21", fii_pct=19.0, dii_pct=15.0, mf_pct=5.5),
        ]
        client.get_ohlcv.return_value = []

        feats = compute_governance_features(client, "TEST", as_of=datetime(2025, 8, 1))
        assert feats["institutional_conviction_flag"] == 1

    def test_one_decreasing_clears_flag(self):
        client = MagicMock()
        client.get_shareholding_history.return_value = [
            _filing("2025-03-31", "2025-04-21", fii_pct=18.0, dii_pct=14.0, mf_pct=5.0),
            _filing("2025-06-30", "2025-07-21", fii_pct=17.0, dii_pct=15.0, mf_pct=5.5),  # FII down
        ]
        client.get_ohlcv.return_value = []

        feats = compute_governance_features(client, "TEST", as_of=datetime(2025, 8, 1))
        assert feats["institutional_conviction_flag"] == 0


class TestGovernancePanel:
    def test_panel_not_sector_zscored(self):
        """Unlike fundamental.py's panel, governance features are raw — no sector z-score."""
        client = MagicMock()
        client.get_shareholding_history.return_value = [
            _filing("2025-03-31", "2025-04-21", promoter_pct=72.3),
        ]
        client.get_ohlcv.return_value = []

        panel = compute_governance_features_panel(client, ["INFY"], as_of=datetime(2025, 8, 1))
        assert panel.iloc[0]["promoter_pct"] == 72.3

    def test_one_bad_ticker_does_not_abort_panel(self):
        client = MagicMock()

        def side_effect(ticker, as_of, lookback_years=2):
            if ticker == "BADCO":
                raise ConnectionError("boom")
            return [_filing("2025-03-31", "2025-04-21", promoter_pct=60.0)]

        client.get_shareholding_history.side_effect = side_effect
        client.get_ohlcv.return_value = []

        panel = compute_governance_features_panel(client, ["GOODCO", "BADCO"], as_of=datetime(2025, 8, 1))
        assert len(panel) == 2
        bad_row = panel[panel["ticker"] == "BADCO"].iloc[0]
        assert np.isnan(bad_row["promoter_pct"])
        assert bad_row["promoter_pledge_spiral_flag"] == 0
