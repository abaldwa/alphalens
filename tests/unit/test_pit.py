"""
tests/unit/test_pit.py

A65: pure-logic tests for `datastore/api/pit.py` (SPEC-DS-003/SPEC-PIPE-003/
SPEC-QUALITY-001), previously untested (46.51% coverage, no test file).
Real pandas DataFrames, no DB/network — this module's own docstring calls
out "100% testability" as its design goal.
"""

from datetime import datetime

import pandas as pd
import pytest

from datastore.api.pit import (
    compute_staleness_flags,
    enforce_pit_fundamentals,
    enforce_pit_mf_holdings,
    enforce_pit_shareholding,
)


class TestEnforcePitFundamentals:
    def test_rejects_non_datetime_as_of(self):
        df = pd.DataFrame({"announcement_date": [datetime(2026, 1, 1)]})
        with pytest.raises(ValueError, match="must be datetime"):
            enforce_pit_fundamentals(df, as_of="2026-01-01")

    def test_missing_column_raises(self):
        df = pd.DataFrame({"other": [1]})
        with pytest.raises(ValueError, match="not found"):
            enforce_pit_fundamentals(df, as_of=datetime(2026, 1, 1))

    def test_filters_forward_looking_rows(self):
        df = pd.DataFrame(
            {
                "ticker": ["A", "A", "A"],
                "announcement_date": [datetime(2026, 1, 1), datetime(2026, 3, 1), datetime(2026, 6, 1)],
                "eps": [1.0, 2.0, 3.0],
            }
        )
        result = enforce_pit_fundamentals(df, as_of=datetime(2026, 4, 1))
        assert len(result) == 2
        assert result["announcement_date"].max() == datetime(2026, 3, 1)

    def test_drops_null_announcement_dates(self):
        df = pd.DataFrame(
            {"announcement_date": [datetime(2026, 1, 1), None], "eps": [1.0, 2.0]}
        )
        result = enforce_pit_fundamentals(df, as_of=datetime(2026, 6, 1))
        assert len(result) == 1

    def test_sorted_ascending(self):
        df = pd.DataFrame(
            {"announcement_date": [datetime(2026, 3, 1), datetime(2026, 1, 1)], "eps": [2.0, 1.0]}
        )
        result = enforce_pit_fundamentals(df, as_of=datetime(2026, 6, 1))
        assert list(result["announcement_date"]) == [datetime(2026, 1, 1), datetime(2026, 3, 1)]


class TestEnforcePitShareholding:
    def test_rejects_non_datetime_as_of(self):
        df = pd.DataFrame({"filing_date": [datetime(2026, 1, 1)]})
        with pytest.raises(ValueError, match="must be datetime"):
            enforce_pit_shareholding(df, as_of="2026-01-01")

    def test_missing_column_raises(self):
        df = pd.DataFrame({"other": [1]})
        with pytest.raises(ValueError, match="not found"):
            enforce_pit_shareholding(df, as_of=datetime(2026, 1, 1))

    def test_filters_forward_looking_rows_and_nulls(self):
        df = pd.DataFrame(
            {
                "filing_date": [datetime(2026, 1, 1), datetime(2026, 6, 1), None],
                "promoter_pct": [50.0, 55.0, 60.0],
            }
        )
        result = enforce_pit_shareholding(df, as_of=datetime(2026, 3, 1))
        assert len(result) == 1
        assert result.iloc[0]["promoter_pct"] == 50.0


class TestEnforcePitMfHoldings:
    def test_rejects_non_datetime_as_of(self):
        df = pd.DataFrame({"month_end": [datetime(2026, 1, 31)]})
        with pytest.raises(ValueError, match="must be datetime"):
            enforce_pit_mf_holdings(df, as_of="2026-02-10")

    def test_missing_column_raises(self):
        df = pd.DataFrame({"other": [1]})
        with pytest.raises(ValueError, match="not found"):
            enforce_pit_mf_holdings(df, as_of=datetime(2026, 1, 1))

    def test_default_delay_of_5_days_applied(self):
        df = pd.DataFrame({"month_end": [datetime(2026, 1, 31)], "scheme_count": [10]})
        # as_of exactly at month_end + 4 days: not yet observable
        early = enforce_pit_mf_holdings(df, as_of=datetime(2026, 2, 4))
        assert len(early) == 0
        # as_of at month_end + 5 days: observable
        on_time = enforce_pit_mf_holdings(df, as_of=datetime(2026, 2, 5))
        assert len(on_time) == 1

    def test_custom_delay_days(self):
        df = pd.DataFrame({"month_end": [datetime(2026, 1, 31)], "scheme_count": [10]})
        result = enforce_pit_mf_holdings(df, as_of=datetime(2026, 2, 2), delay_days=2)
        assert len(result) == 1

    def test_drops_null_month_end_and_observable_date_column(self):
        df = pd.DataFrame(
            {"month_end": [datetime(2026, 1, 31), None], "scheme_count": [10, 20]}
        )
        result = enforce_pit_mf_holdings(df, as_of=datetime(2026, 3, 1))
        assert len(result) == 1
        assert "observable_date" not in result.columns


class TestComputeStalenessFlags:
    def test_rejects_non_datetime_as_of(self):
        df = pd.DataFrame({"date": [datetime(2026, 1, 1)]})
        with pytest.raises(ValueError, match="must be datetime"):
            compute_staleness_flags(df, as_of="2026-01-01")

    def test_missing_column_raises(self):
        df = pd.DataFrame({"other": [1]})
        with pytest.raises(ValueError, match="not found"):
            compute_staleness_flags(df, as_of=datetime(2026, 1, 1))

    def test_fresh_and_stale_rows_flagged_correctly(self):
        df = pd.DataFrame(
            {"ticker": ["A", "B"], "date": [datetime(2026, 6, 1), datetime(2026, 5, 1)]}
        )
        result = compute_staleness_flags(df, as_of=datetime(2026, 6, 3), lookback_days=5)
        assert result.set_index("ticker")["data_staleness_flag"].to_dict() == {"A": 0, "B": 1}
        assert "days_since_observation" not in result.columns

    def test_exactly_at_threshold_is_not_stale(self):
        df = pd.DataFrame({"date": [datetime(2026, 6, 1)]})
        result = compute_staleness_flags(df, as_of=datetime(2026, 6, 6), lookback_days=5)
        assert result.iloc[0]["data_staleness_flag"] == 0
