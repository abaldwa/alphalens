"""
tests/unit/test_hybrid_compute.py

A65 — real-logic coverage for features/hybrid_compute.py (previously 0%).
Only the pure-computation entry points are tested here (no DuckDB/API/
network access):

- _empty_staging: all-NaN staging frame shape for a ticker with no OHLCV.
- build_benchmark_wide: pivots long-format benchmark OHLCV into the wide
  shape compute_technical_features expects.
- assemble_date: exercised with small, real (not fabricated-as-if-real)
  injected staging DataFrames covering every _STAGE1_FEATURE_COLS column
  (as NaN where not under test) — verifies the cross-ticker steps that
  are hybrid_compute.py's whole reason for existing (sector z-scoring of
  RATIO_FEATURES, mf_crowdedness_rank, calendar-feature merge), without
  needing real fundamentals/OHLCV/macro data from a live DB.
"""

import numpy as np
import pandas as pd

from features.hybrid_compute import (
    _OHLCV_PASS,
    _STAGE1_FEATURE_COLS,
    assemble_date,
    build_benchmark_wide,
)
from features.hybrid_compute import _empty_staging
from features.technical import BENCHMARK_TICKERS


class TestEmptyStaging:
    def test_all_stage1_and_ohlcv_columns_are_nan(self):
        dates = pd.to_datetime(["2026-01-01", "2026-01-02"])
        out = _empty_staging("RELIANCE", list(dates))

        assert len(out) == 2
        assert (out["ticker"] == "RELIANCE").all()
        assert list(out["date"]) == list(dates)
        for col in _STAGE1_FEATURE_COLS + _OHLCV_PASS:
            assert col in out.columns
            assert out[col].isna().all()

    def test_empty_dates_list_returns_empty_frame(self):
        out = _empty_staging("RELIANCE", [])
        assert out.empty
        assert "ticker" in out.columns


class TestBuildBenchmarkWide:
    def test_pivots_long_to_wide_with_renamed_columns(self):
        rows = []
        for name, sym in BENCHMARK_TICKERS.items():
            rows.append({"date": pd.Timestamp("2026-01-01"), "ticker": sym, "close": 100.0})
            rows.append({"date": pd.Timestamp("2026-01-02"), "ticker": sym, "close": 101.0})
        long_df = pd.DataFrame(rows)

        wide = build_benchmark_wide(long_df)

        assert wide is not None
        assert len(wide) == 2
        for name in BENCHMARK_TICKERS:
            assert f"{name}_close" in wide.columns
        first_name = next(iter(BENCHMARK_TICKERS))
        assert wide.loc[wide["date"] == pd.Timestamp("2026-01-01"), f"{first_name}_close"].iloc[0] == 100.0

    def test_empty_input_returns_none(self):
        assert build_benchmark_wide(pd.DataFrame()) is None

    def test_missing_benchmark_symbol_gets_nan_column(self):
        # Only seed data for one benchmark ticker — the others should still
        # appear as real (not fabricated) all-NaN columns, never dropped.
        one_name, one_sym = next(iter(BENCHMARK_TICKERS.items()))
        long_df = pd.DataFrame(
            [{"date": pd.Timestamp("2026-01-01"), "ticker": one_sym, "close": 250.0}]
        )
        wide = build_benchmark_wide(long_df)
        assert wide is not None
        for name in BENCHMARK_TICKERS:
            col = f"{name}_close"
            assert col in wide.columns
            if name != one_name:
                assert wide[col].isna().all()


def _staging_row(ticker: str, date: pd.Timestamp, **overrides) -> pd.DataFrame:
    """One real-shaped staging row: date/ticker + every _STAGE1_FEATURE_COLS/
    _OHLCV_PASS column present (NaN by default), overridden per test."""
    row = {"date": date, "ticker": ticker}
    for col in _STAGE1_FEATURE_COLS + _OHLCV_PASS:
        row[col] = overrides.get(col, np.nan)
    return pd.DataFrame([row])


class TestAssembleDateCrossTickerSteps:
    def test_sector_zscore_and_mf_crowdedness_rank_computed_across_tickers(self):
        date = pd.Timestamp("2026-01-05")
        tickers = ["AAA", "BBB", "CCC"]
        staging = {
            "AAA": _staging_row("AAA", date, pe_ratio=10.0, mf_scheme_count=5),
            "BBB": _staging_row("BBB", date, pe_ratio=20.0, mf_scheme_count=15),
            "CCC": _staging_row("CCC", date, pe_ratio=30.0, mf_scheme_count=25),
        }
        sector_map = {"AAA": "IT", "BBB": "IT", "CCC": "FMCG"}
        tier_map = {"AAA": "T1", "BBB": "T1", "CCC": "T1"}
        macro_all = pd.DataFrame(columns=["date"])
        benchmark_ohlcv = pd.DataFrame(columns=["date", "ticker", "close"])

        result = assemble_date(
            date=date, staging=staging, benchmark_ohlcv=benchmark_ohlcv,
            sector_map=sector_map, tier_map=tier_map, macro_all=macro_all,
            tickers=tickers,
        )

        assert not result.empty
        assert len(result) == 3
        # mf_crowdedness_rank: percentile rank within tier "T1" of all 3
        # tickers' mf_scheme_count (5 < 15 < 25) — ascending pct rank.
        ranks = result.set_index("ticker")["mf_crowdedness_rank"]
        assert ranks["AAA"] < ranks["BBB"] < ranks["CCC"]
        # Sector z-score: _sector_relative_zscore replaces RATIO_FEATURES
        # columns in place (same name, no "_z" suffix) with
        # (x - sector_mean)/(sector_std+eps), clipped to [-5, 5]. AAA/BBB
        # share sector "IT" so a real (non-NaN) z-score is computed for
        # both; AAA's lower PE => below-mean z-score vs BBB's.
        aaa_z = result.set_index("ticker").loc["AAA", "pe_ratio"]
        bbb_z = result.set_index("ticker").loc["BBB", "pe_ratio"]
        assert pd.notna(aaa_z) and pd.notna(bbb_z)
        assert aaa_z < bbb_z

    def test_calendar_features_merged_onto_every_row(self):
        date = pd.Timestamp("2026-01-05")
        staging = {"AAA": _staging_row("AAA", date)}
        result = assemble_date(
            date=date, staging=staging, benchmark_ohlcv=pd.DataFrame(columns=["date", "ticker", "close"]),
            sector_map={"AAA": "IT"}, tier_map={"AAA": "T1"}, macro_all=pd.DataFrame(columns=["date"]),
            tickers=["AAA"],
        )
        assert not result.empty
        # is_month_end / day_of_week etc are real CALENDAR_FEATURES columns —
        # merged in step 7, never left absent.
        from features.calendar import CALENDAR_FEATURES
        for col in CALENDAR_FEATURES:
            assert col in result.columns

    def test_no_staging_data_for_date_returns_empty(self):
        result = assemble_date(
            date=pd.Timestamp("2026-01-05"),
            staging={"AAA": _staging_row("AAA", pd.Timestamp("2026-01-01"))},
            benchmark_ohlcv=pd.DataFrame(columns=["date", "ticker", "close"]),
            sector_map={"AAA": "IT"}, tier_map={"AAA": "T1"}, macro_all=pd.DataFrame(columns=["date"]),
            tickers=["AAA"],
        )
        assert result.empty
