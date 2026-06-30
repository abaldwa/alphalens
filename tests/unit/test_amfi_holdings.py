"""
tests/unit/test_amfi_holdings.py

Phase: 2.2 (AMFI MF Holdings + Corporate Action Features)
Specs: SPEC-PIPE-001, SPEC-PIPE-003 (CRITICAL), SPEC-SOLID-002
Owner: Platform / QA
Consumers: CI, pytest

Tests the AMC registry architecture, PIT-default availability_date
logic, and per-AMC failure isolation — entirely offline (no real network
call), since AMC_REGISTRY ships empty by design (see module docstring on
ingestion/scrapers/amfi_holdings.py).
"""

import pandas as pd
import pytest

import ingestion.scrapers.amfi_holdings as amfi
from ingestion.scrapers.amfi_holdings import (
    availability_date_for_month,
    download_monthly_disclosure,
    register_amc,
    save_monthly_parquet,
)


@pytest.fixture(autouse=True)
def _clean_registry():
    """AMC_REGISTRY is module-level global state — reset it around every test."""
    original = dict(amfi.AMC_REGISTRY)
    amfi.AMC_REGISTRY.clear()
    yield
    amfi.AMC_REGISTRY.clear()
    amfi.AMC_REGISTRY.update(original)


class TestEmptyRegistry:
    def test_download_raises_clear_error_with_no_amcs_registered(self):
        with pytest.raises(RuntimeError, match="No AMCs registered"):
            download_monthly_disclosure(2024, 6)

    def test_requesting_an_unregistered_amc_raises(self):
        register_amc("Real AMC", lambda y, m: b"", lambda raw: pd.DataFrame())
        with pytest.raises(RuntimeError, match="not registered"):
            download_monthly_disclosure(2024, 6, amcs=["Fake AMC"])


class TestRegisterAndDownload:
    def test_registered_amc_is_fetched_and_parsed(self):
        def fetch(year, month):
            return b"raw bytes"

        def parse(raw):
            assert raw == b"raw bytes"
            return pd.DataFrame([
                {"scheme_name": "Test Fund", "isin": "INF000", "ticker": "RELIANCE",
                 "quantity": 100, "value_inr": 1000.0}
            ])

        register_amc("Test AMC", fetch, parse)
        df = download_monthly_disclosure(2024, 6)

        assert len(df) == 1
        assert df.iloc[0]["ticker"] == "RELIANCE"
        assert df.iloc[0]["month"] == "2024-06"

    def test_one_amc_failure_does_not_abort_the_batch(self):
        def good_fetch(year, month):
            return b"ok"

        def good_parse(raw):
            return pd.DataFrame(
                [{"scheme_name": "Good Fund", "isin": "INF001", "ticker": "TCS", "quantity": 50, "value_inr": 500.0}]
            )

        def bad_fetch(year, month):
            raise ConnectionError("AMC site down")

        register_amc("Good AMC", good_fetch, good_parse)
        register_amc("Bad AMC", bad_fetch, lambda raw: pd.DataFrame())

        df = download_monthly_disclosure(2024, 6)

        assert len(df) == 1
        assert df.iloc[0]["ticker"] == "TCS"

    def test_multiple_amcs_concatenate(self):
        def make_amc(scheme_name, ticker):
            return (
                lambda y, m: b"x",
                lambda raw: pd.DataFrame([
                    {"scheme_name": scheme_name, "isin": "INF999", "ticker": ticker,
                     "quantity": 10, "value_inr": 100.0}
                ]),
            )

        fetch1, parse1 = make_amc("Fund One", "RELIANCE")
        fetch2, parse2 = make_amc("Fund Two", "TCS")
        register_amc("AMC1", fetch1, parse1)
        register_amc("AMC2", fetch2, parse2)

        df = download_monthly_disclosure(2024, 6)
        assert len(df) == 2
        assert set(df["ticker"]) == {"RELIANCE", "TCS"}


class TestAvailabilityDate:
    """SPEC-PIPE-003 (CRITICAL): availability_date = 5th of month+1."""

    def test_mid_year_month(self):
        from datetime import date

        assert availability_date_for_month(2024, 6) == date(2024, 7, 5)

    def test_december_wraps_to_next_january(self):
        from datetime import date

        assert availability_date_for_month(2024, 12) == date(2025, 1, 5)


class TestSaveMonthlyParquet:
    def test_writes_availability_date_column(self, tmp_path):
        df = pd.DataFrame([
            {"scheme_name": "Test Fund", "isin": "INF000", "ticker": "RELIANCE",
             "quantity": 100, "value_inr": 1000.0, "month": "2024-06"}
        ])
        path = save_monthly_parquet(df, 2024, 6, output_dir=tmp_path)

        assert path.name == "2024-06.parquet"
        written = pd.read_parquet(path)
        assert pd.Timestamp(written.iloc[0]["availability_date"]) == pd.Timestamp("2024-07-05")

    def test_saving_a_second_amc_does_not_erase_the_first(self, tmp_path):
        """
        Regression test: download_monthly_disclosure is designed to be
        called with a SUBSET of AMCs at a time (verification, retries,
        rate-limit batching). A naive overwrite previously destroyed
        whatever other AMCs had already been saved for the same month —
        caught live saving HDFC's real data over SBI's (see BuildLog.md
        "P2.2 continued").
        """
        amc1_df = pd.DataFrame([
            {"scheme_name": "SBI Fund A", "isin": "INF001", "ticker": "RELIANCE",
             "quantity": 100, "value_inr": 1000.0, "month": "2024-06"}
        ])
        amc2_df = pd.DataFrame([
            {"scheme_name": "HDFC Fund B", "isin": None, "ticker": "TCS",
             "quantity": float("nan"), "value_inr": 2000.0, "month": "2024-06"}
        ])

        save_monthly_parquet(amc1_df, 2024, 6, output_dir=tmp_path)
        path = save_monthly_parquet(amc2_df, 2024, 6, output_dir=tmp_path)

        written = pd.read_parquet(path)
        assert set(written["scheme_name"]) == {"SBI Fund A", "HDFC Fund B"}
        assert len(written) == 2

    def test_re_saving_the_same_scheme_replaces_not_duplicates(self, tmp_path):
        original = pd.DataFrame([
            {"scheme_name": "SBI Fund A", "isin": "INF001", "ticker": "RELIANCE",
             "quantity": 100, "value_inr": 1000.0, "month": "2024-06"}
        ])
        updated = pd.DataFrame([
            {"scheme_name": "SBI Fund A", "isin": "INF001", "ticker": "RELIANCE",
             "quantity": 150, "value_inr": 1500.0, "month": "2024-06"}
        ])

        save_monthly_parquet(original, 2024, 6, output_dir=tmp_path)
        path = save_monthly_parquet(updated, 2024, 6, output_dir=tmp_path)

        written = pd.read_parquet(path)
        assert len(written) == 1
        assert written.iloc[0]["quantity"] == 150
