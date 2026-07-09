"""
tests/unit/test_job_health_registry.py

Phase: A21 (Pipeline Health Checker)
Owner: Platform / QA

Tests datastore/health/job_registry.py's expected_dates() cadence logic.
"""

from datetime import date

import pytest

from datastore.health.job_registry import JOB_REGISTRY, expected_dates


class TestExpectedDates:
    def test_mon_fri_cadence(self):
        # 2026-06-01 is a Monday, 2026-06-07 is the following Sunday.
        dates = expected_dates("daily_pipeline", date(2026, 6, 1), date(2026, 6, 7))
        assert dates == [date(2026, 6, 1), date(2026, 6, 2), date(2026, 6, 3), date(2026, 6, 4), date(2026, 6, 5)]

    def test_saturday_cadence(self):
        dates = expected_dates("weekend_feature_backfill", date(2026, 6, 1), date(2026, 6, 7))
        assert dates == [date(2026, 6, 6)]

    def test_sunday_cadence(self):
        dates = expected_dates("multibagger_scoring", date(2026, 6, 1), date(2026, 6, 7))
        assert dates == [date(2026, 6, 7)]

    def test_daily_cadence(self):
        dates = expected_dates("daily_backup", date(2026, 6, 1), date(2026, 6, 7))
        assert len(dates) == 7

    def test_window_with_no_matching_weekday_returns_empty(self):
        dates = expected_dates("weekend_feature_backfill", date(2026, 6, 1), date(2026, 6, 2))
        assert dates == []

    def test_unknown_job_id_raises(self):
        with pytest.raises(KeyError):
            expected_dates("model_training", date(2026, 6, 1), date(2026, 6, 7))

    def test_model_training_not_registered(self):
        assert "model_training" not in JOB_REGISTRY
