"""
tests/unit/test_corporate_action_features.py

Phase: 2.2 (AMFI MF Holdings + Corporate Action Features)
Specs: SPEC-FEAT-002, SPEC-PIPE-002, SPEC-PIPE-003
Owner: Platform / QA
Consumers: CI, pytest

Tests features/corporate_action_features.py using a fake DataStoreClient
(SPEC-SOLID-005 — no real HTTP call) for the 5 features computable today,
and confirms the other 5 (no corresponding ingestion yet — see module
docstring) degrade to NaN rather than raising.
"""

from datetime import datetime
from unittest.mock import MagicMock

import numpy as np
import pytest

from features.corporate_action_features import (
    CORPORATE_ACTION_FEATURES,
    compute_corporate_action_features,
    compute_corporate_action_features_panel,
)


def _action(ex_date, action_type, ratio=2.0, announcement_date=None, record_date=None):
    return {
        "ticker": "TEST", "ex_date": ex_date, "action_type": action_type, "ratio": ratio,
        "announcement_date": announcement_date, "record_date": record_date,
    }


class TestFeatureCount:
    def test_ten_features(self):
        assert len(CORPORATE_ACTION_FEATURES) == 10


class TestDaysToRecordDate:
    def test_future_record_date_returns_positive_days(self):
        client = MagicMock()
        client.get_corporate_actions.return_value = [
            _action("2025-07-01", "BONUS", announcement_date="2025-06-01", record_date="2025-07-10"),
        ]
        client.get_fundamentals_history.return_value = []

        feats = compute_corporate_action_features(client, "TEST", as_of=datetime(2025, 6, 15))

        assert feats["days_to_record_date"] == 25

    def test_no_upcoming_record_date_is_nan(self):
        client = MagicMock()
        client.get_corporate_actions.return_value = []
        client.get_fundamentals_history.return_value = []

        feats = compute_corporate_action_features(client, "TEST", as_of=datetime(2025, 6, 15))

        assert np.isnan(feats["days_to_record_date"])

    def test_unannounced_action_excluded_by_pit(self):
        """SPEC-PIPE-003: an action with no announcement_date yet (or future) must not influence features."""
        client = MagicMock()
        client.get_corporate_actions.return_value = [
            _action("2025-07-01", "BONUS", announcement_date="2025-06-20", record_date="2025-07-10"),  # future announce
        ]
        client.get_fundamentals_history.return_value = []

        feats = compute_corporate_action_features(client, "TEST", as_of=datetime(2025, 6, 15))

        assert np.isnan(feats["days_to_record_date"])


class TestCorpActionAnticipationReturn:
    def test_computes_runup_before_nearest_past_ex_date(self):
        client = MagicMock()
        client.get_corporate_actions.return_value = [
            _action("2025-06-10", "SPLIT", announcement_date="2025-06-01"),
        ]
        client.get_fundamentals_history.return_value = []
        client.get_ohlcv.return_value = [
            {"date": "2025-06-05", "close": 100.0},
            {"date": "2025-06-10", "close": 110.0},
        ]

        feats = compute_corporate_action_features(client, "TEST", as_of=datetime(2025, 6, 15))

        assert feats["corp_action_anticipation_return"] == pytest.approx(0.10)


class TestIPOFeatures:
    def test_listing_age_and_lockin_proximity(self):
        client = MagicMock()
        client.get_corporate_actions.return_value = []
        client.get_fundamentals_history.return_value = []

        listing_date = datetime(2025, 1, 1)
        as_of = datetime(2025, 4, 1)  # 90 days after listing
        feats = compute_corporate_action_features(client, "TEST", as_of, listing_date=listing_date)

        assert feats["ipo_listing_age_months"] == (90 / 30.44)
        assert feats["ipo_lockin_expiry_proximity"] == 90  # 180-day lockin - 90 elapsed = 90 remaining

    def test_no_listing_date_is_nan(self):
        client = MagicMock()
        client.get_corporate_actions.return_value = []
        client.get_fundamentals_history.return_value = []

        feats = compute_corporate_action_features(client, "TEST", as_of=datetime(2025, 4, 1), listing_date=None)

        assert np.isnan(feats["ipo_listing_age_months"])
        assert np.isnan(feats["ipo_lockin_expiry_proximity"])


class TestPostEarningsDriftSignal:
    def test_computes_drift_after_latest_announcement(self):
        client = MagicMock()
        client.get_corporate_actions.return_value = []
        client.get_fundamentals_history.return_value = [
            {"ticker": "TEST", "announcement_date": "2025-05-15", "fiscal_year": 2025, "quarter": 1},
        ]
        client.get_ohlcv.return_value = [
            {"date": "2025-05-15", "close": 100.0},
            {"date": "2025-05-20", "close": 105.0},
        ]

        feats = compute_corporate_action_features(client, "TEST", as_of=datetime(2025, 6, 1))

        assert feats["post_earnings_drift_signal"] == pytest.approx(0.05)

    def test_no_fundamentals_history_is_nan(self):
        client = MagicMock()
        client.get_corporate_actions.return_value = []
        client.get_fundamentals_history.return_value = []

        feats = compute_corporate_action_features(client, "TEST", as_of=datetime(2025, 6, 1))

        assert np.isnan(feats["post_earnings_drift_signal"])


class TestNotYetIngestedActionTypesDegradeToNaN:
    """BUYBACK/QIP/INDEX_INCLUSION/DIVIDEND ingestion doesn't exist yet (module docstring) — must not raise."""

    def test_all_five_structural_gap_features_are_nan_with_no_data(self):
        client = MagicMock()
        client.get_corporate_actions.return_value = []
        client.get_fundamentals_history.return_value = []

        feats = compute_corporate_action_features(client, "TEST", as_of=datetime(2025, 6, 1))

        for f in [
            "buyback_price_spread", "buyback_acceptance_estimated", "index_inclusion_days",
            "dividend_yield_vs_fd_rate", "qip_dilution_impact",
        ]:
            assert np.isnan(feats[f]), f"{f} should be NaN, got {feats[f]}"

    def test_buyback_price_spread_computes_when_buyback_row_present(self):
        client = MagicMock()
        client.get_corporate_actions.return_value = [
            _action("2025-06-01", "BUYBACK", ratio=150.0, announcement_date="2025-05-01"),
        ]
        client.get_fundamentals_history.return_value = []
        client.get_ohlcv.return_value = [{"date": "2025-06-01", "close": 100.0}]

        feats = compute_corporate_action_features(client, "TEST", as_of=datetime(2025, 6, 1))

        assert feats["buyback_price_spread"] == 0.5  # (150-100)/100


class TestPanel:
    def test_one_bad_ticker_does_not_abort_panel(self):
        client = MagicMock()

        def side_effect(ticker, **kwargs):
            if ticker == "BADCO":
                raise ConnectionError("boom")
            return []

        client.get_corporate_actions.side_effect = side_effect
        client.get_fundamentals_history.return_value = []
        client.get_ohlcv.return_value = []

        panel = compute_corporate_action_features_panel(client, ["GOODCO", "BADCO"], as_of=datetime(2025, 6, 1))

        assert len(panel) == 2
        bad_row = panel[panel["ticker"] == "BADCO"].iloc[0]
        assert np.isnan(bad_row["days_to_record_date"])
