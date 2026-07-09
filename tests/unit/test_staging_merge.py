"""
tests/unit/test_staging_merge.py

Phase: A25 (Write-Audit-Publish Architecture) — full rollout
Owner: Platform / QA
Consumers: CI, pytest

Tests datastore/staging/merge.py's three merge policies against plain
pandas DataFrames — pure pandas logic, no DuckDB needed, no real DB
touched.
"""

import numpy as np
import pandas as pd

from datastore.staging.merge import (
    coalesce_merge,
    insert_ignore_merge,
    partition_replace_merge,
)


class TestCoalesceMergeExistingWins:
    def test_existing_non_null_value_is_kept(self):
        existing = pd.DataFrame({"ticker": ["A"], "fy": [2025], "revenue": [100.0]})
        new = pd.DataFrame({"ticker": ["A"], "fy": [2025], "revenue": [999.0]})
        merged = coalesce_merge(existing, new, ["ticker", "fy"], new_wins=False)
        assert merged.loc[0, "revenue"] == 100.0

    def test_existing_null_falls_back_to_new(self):
        existing = pd.DataFrame({"ticker": ["A"], "fy": [2025], "revenue": [np.nan]})
        new = pd.DataFrame({"ticker": ["A"], "fy": [2025], "revenue": [50.0]})
        merged = coalesce_merge(existing, new, ["ticker", "fy"], new_wins=False)
        assert merged.loc[0, "revenue"] == 50.0

    def test_force_new_wins_cols_overrides_policy(self):
        existing = pd.DataFrame({
            "ticker": ["A"], "fy": [2025], "revenue": [100.0], "quality_flag": [False],
        })
        new = pd.DataFrame({
            "ticker": ["A"], "fy": [2025], "revenue": [999.0], "quality_flag": [True],
        })
        merged = coalesce_merge(
            existing, new, ["ticker", "fy"], new_wins=False,
            force_new_wins_cols=["quality_flag"],
        )
        assert merged.loc[0, "revenue"] == 100.0  # existing wins (default policy)
        assert merged.loc[0, "quality_flag"] == True  # noqa: E712 — new wins (forced)

    def test_keys_only_in_existing_pass_through(self):
        existing = pd.DataFrame({"ticker": ["A", "B"], "fy": [2025, 2025], "revenue": [1.0, 2.0]})
        new = pd.DataFrame({"ticker": ["A"], "fy": [2025], "revenue": [999.0]})
        merged = coalesce_merge(existing, new, ["ticker", "fy"], new_wins=False)
        assert len(merged) == 2
        b_row = merged[merged["ticker"] == "B"]
        assert b_row.iloc[0]["revenue"] == 2.0

    def test_keys_only_in_new_are_added(self):
        existing = pd.DataFrame({"ticker": ["A"], "fy": [2025], "revenue": [1.0]})
        new = pd.DataFrame({"ticker": ["C"], "fy": [2025], "revenue": [3.0]})
        merged = coalesce_merge(existing, new, ["ticker", "fy"], new_wins=False)
        assert len(merged) == 2
        assert set(merged["ticker"]) == {"A", "C"}

    def test_empty_existing_returns_new(self):
        existing = pd.DataFrame(columns=["ticker", "fy", "revenue"])
        new = pd.DataFrame({"ticker": ["A"], "fy": [2025], "revenue": [1.0]})
        merged = coalesce_merge(existing, new, ["ticker", "fy"], new_wins=False)
        assert len(merged) == 1


class TestCoalesceMergeNewWins:
    def test_new_non_null_value_overwrites_existing(self):
        existing = pd.DataFrame({"ticker": ["A"], "fy": [2025], "revenue": [100.0]})
        new = pd.DataFrame({"ticker": ["A"], "fy": [2025], "revenue": [999.0]})
        merged = coalesce_merge(existing, new, ["ticker", "fy"], new_wins=True)
        assert merged.loc[0, "revenue"] == 999.0

    def test_new_null_falls_back_to_existing(self):
        existing = pd.DataFrame({"ticker": ["A"], "fy": [2025], "revenue": [100.0]})
        new = pd.DataFrame({"ticker": ["A"], "fy": [2025], "revenue": [np.nan]})
        merged = coalesce_merge(existing, new, ["ticker", "fy"], new_wins=True)
        assert merged.loc[0, "revenue"] == 100.0


class TestPartitionReplaceMerge:
    def test_target_partition_is_replaced(self):
        existing = pd.DataFrame({"month": ["2026-06-01", "2026-06-01"], "ticker": ["A", "B"]})
        new = pd.DataFrame({"month": ["2026-06-01"], "ticker": ["C"]})
        merged = partition_replace_merge(existing, new, "month", ["2026-06-01"])
        assert list(merged["ticker"]) == ["C"]

    def test_other_partitions_untouched(self):
        existing = pd.DataFrame({"month": ["2026-05-01", "2026-06-01"], "ticker": ["A", "B"]})
        new = pd.DataFrame({"month": ["2026-06-01"], "ticker": ["C"]})
        merged = partition_replace_merge(existing, new, "month", ["2026-06-01"])
        assert set(merged["ticker"]) == {"A", "C"}

    def test_empty_new_clears_the_partition(self):
        existing = pd.DataFrame({"month": ["2026-06-01"], "ticker": ["A"]})
        new = pd.DataFrame(columns=["month", "ticker"])
        merged = partition_replace_merge(existing, new, "month", ["2026-06-01"])
        assert merged.empty


class TestInsertIgnoreMerge:
    def test_existing_row_never_overwritten(self):
        existing = pd.DataFrame({"ticker": ["A"], "ex_date": ["2026-01-01"], "ratio": [1.0]})
        new = pd.DataFrame({"ticker": ["A"], "ex_date": ["2026-01-01"], "ratio": [99.0]})
        merged = insert_ignore_merge(existing, new, ["ticker", "ex_date"])
        assert merged.loc[0, "ratio"] == 1.0
        assert len(merged) == 1

    def test_genuinely_new_key_is_appended(self):
        existing = pd.DataFrame({"ticker": ["A"], "ex_date": ["2026-01-01"], "ratio": [1.0]})
        new = pd.DataFrame({"ticker": ["B"], "ex_date": ["2026-02-01"], "ratio": [2.0]})
        merged = insert_ignore_merge(existing, new, ["ticker", "ex_date"])
        assert len(merged) == 2
        assert set(merged["ticker"]) == {"A", "B"}

    def test_duplicate_new_rows_keep_first_only(self):
        existing = pd.DataFrame(columns=["ticker", "ex_date", "ratio"])
        new = pd.DataFrame({
            "ticker": ["A", "A"], "ex_date": ["2026-01-01", "2026-01-01"], "ratio": [1.0, 2.0],
        })
        merged = insert_ignore_merge(existing, new, ["ticker", "ex_date"])
        assert len(merged) == 1
        assert merged.loc[0, "ratio"] == 1.0
