"""
tests/unit/test_macro_real_economy.py

Unit tests for ingestion/scrapers/macro_real_economy.py's real DPIIT ICI
(cement/power) fetch — mocked HTTP only, never hits the real network.
"""

import io

import pandas as pd
import pytest

from ingestion.scrapers import macro_real_economy as mre


def _fake_ici_workbook_bytes() -> bytes:
    """Build a minimal real .xlsx matching eaindustry.nic.in's ICI 'Growth (%)' sheet shape."""
    growth = pd.DataFrame(
        {
            "Months/Years": [
                pd.Timestamp("2026-04-01"),
                pd.Timestamp("2026-05-01"),
                "2025-26(Apr-Mar)",  # annual aggregate row — must be filtered out
            ],
            "Overall Growth rate": [1.8, 0.5, 2.7],
            "Growth of  Cement (%)": [5.5, 8.4, 9.5],
            "Growth of  Electricity  (%)": [8.2, 8.7, 8.7],
        }
    )
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        growth.to_excel(writer, sheet_name="Index", index=False)
        growth.to_excel(writer, sheet_name="Growth (%)", index=False)
    return buf.getvalue()


class _FakeResponse:
    def __init__(self, text=None, content=None, status_code=200):
        self.text = text
        self.content = content
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(f"status {self.status_code}")


class TestFindIciXlsxUrl:
    def test_extracts_current_download_link(self, monkeypatch):
        html = (
            '<a target="_blank" href="eight_core_infra/Core_Industries_2011_12_20260622.xlsx">'
            "Download Data (2011-12)</a>"
        )
        monkeypatch.setattr(mre.requests, "get", lambda url, timeout=None: _FakeResponse(text=html))
        url = mre._find_ici_xlsx_url()
        assert url == mre._EAINDUSTRY_BASE + "eight_core_infra/Core_Industries_2011_12_20260622.xlsx"

    def test_raises_when_link_not_found(self, monkeypatch):
        monkeypatch.setattr(mre.requests, "get", lambda url, timeout=None: _FakeResponse(text="<html></html>"))
        with pytest.raises(ConnectionError):
            mre._find_ici_xlsx_url()


class TestDownloadCoreIndustriesIndex:
    def test_parses_real_monthly_rows_and_drops_annual_aggregates(self, monkeypatch):
        monkeypatch.setattr(
            mre, "_find_ici_xlsx_url", lambda: "https://eaindustry.nic.in/eight_core_infra/fake.xlsx"
        )
        monkeypatch.setattr(
            mre.requests, "get", lambda url, timeout=None: _FakeResponse(content=_fake_ici_workbook_bytes())
        )
        df = mre.download_core_industries_index("2026-07-07")
        assert len(df) == 2  # the "2025-26(Apr-Mar)" aggregate row must be dropped
        assert list(df["month_end"]) == [pd.Timestamp("2026-04-01"), pd.Timestamp("2026-05-01")]
        assert df.iloc[-1]["cement_growth_pct"] == 8.4
        assert df.iloc[-1]["electricity_growth_pct"] == 8.7

    def test_raises_after_retries_on_connection_failure(self, monkeypatch):
        def _raise(*a, **k):
            raise ConnectionError("unreachable")

        monkeypatch.setattr(mre, "_find_ici_xlsx_url", _raise)
        with pytest.raises(ConnectionError):
            mre.download_core_industries_index("2026-07-07")


class TestFetchCementAndPowerGrowth:
    def test_respects_pit_release_lag(self, monkeypatch):
        idx = pd.DataFrame(
            {
                "month_end": [pd.Timestamp("2026-05-01")],
                "cement_growth_pct": [8.4],
                "electricity_growth_pct": [8.7],
            }
        )
        monkeypatch.setattr(mre, "download_core_industries_index", lambda d: idx)

        # Before either release lag has elapsed: both None.
        result_early = mre.fetch_cement_and_power_growth("2026-06-01")
        assert result_early["cement_dispatches_growth"] is None
        assert result_early["power_consumption_growth"] is None

        # After both lags: real values, correct PIT fields.
        result_later = mre.fetch_cement_and_power_growth("2026-07-07")
        assert result_later["cement_dispatches_growth"]["value"] == 8.4
        assert result_later["cement_dispatches_growth"]["reference_month_end"] == pd.Timestamp("2026-05-31")
        assert result_later["power_consumption_growth"]["value"] == 8.7


class TestUpsertMacroRealEconomyParquet:
    def test_writes_and_dedupes_on_rerun(self, monkeypatch, tmp_path):
        parquet_path = tmp_path / "macro_real_economy.parquet"
        monkeypatch.setattr(mre, "_MACRO_REAL_ECONOMY_PATH", parquet_path)
        monkeypatch.setattr(
            mre,
            "fetch_cement_and_power_growth",
            lambda d: {
                "cement_dispatches_growth": {
                    "reference_month_end": pd.Timestamp("2026-05-31"),
                    "value": 8.4,
                    "availability_date": pd.Timestamp("2026-06-15"),
                },
                "power_consumption_growth": {
                    "reference_month_end": pd.Timestamp("2026-05-31"),
                    "value": 8.7,
                    "availability_date": pd.Timestamp("2026-06-07"),
                },
            },
        )

        n1 = mre.upsert_macro_real_economy_parquet("2026-07-07")
        assert n1 == 2
        n2 = mre.upsert_macro_real_economy_parquet("2026-07-07")
        assert n2 == 2  # fetch always returns rows; upsert still dedupes on write

        final = pd.read_parquet(parquet_path)
        assert len(final) == 2  # deduped, not doubled

    def test_returns_zero_when_nothing_eligible(self, monkeypatch, tmp_path):
        parquet_path = tmp_path / "macro_real_economy.parquet"
        monkeypatch.setattr(mre, "_MACRO_REAL_ECONOMY_PATH", parquet_path)
        monkeypatch.setattr(
            mre,
            "fetch_cement_and_power_growth",
            lambda d: {"cement_dispatches_growth": None, "power_consumption_growth": None},
        )
        assert mre.upsert_macro_real_economy_parquet("2026-07-07") == 0
        assert not parquet_path.exists()
