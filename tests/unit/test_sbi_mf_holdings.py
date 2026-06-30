"""
tests/unit/test_sbi_mf_holdings.py

Phase: 2.2 (AMFI MF Holdings + Corporate Action Features)
Specs: SPEC-PIPE-001, SPEC-PIPE-003 (CRITICAL), SPEC-MFHOLD-001
Owner: Platform / QA
Consumers: CI, pytest

Tests ingestion/scrapers/sbi_mf_holdings.py's parser against a synthetic
workbook matching SBI's real, live-verified structure (BuildLog.md "P2.2
continued") — no real network call or Playwright browser launch (that
part — fetch() — is structurally simple and was verified live manually;
unit-testing a real browser launch in CI would be slow/flaky, same
reasoning this project already applies to screener.py's login()).
"""

import io
from unittest.mock import patch

import openpyxl
import pytest

from ingestion.scrapers.amfi_holdings import AMC_REGISTRY
from ingestion.scrapers.sbi_mf_holdings import parse


def _build_workbook(scheme_name, holdings):
    """
    holdings: list of (name, isin, industry, quantity, market_value_lakhs, pct_aum)
    Matches SBI's real column layout exactly (col index 2=name, 3=isin,
    4=industry, 5=quantity, 6=market_value_lakhs, 7=%aum), verified live.
    """
    wb = openpyxl.Workbook()
    index_sheet = wb.active
    index_sheet.title = "Index"
    index_sheet.append(["Scheme Code", "Scheme Short code", "Scheme Name"])
    index_sheet.append(["017", "TESTSCHEME", scheme_name])

    ws = wb.create_sheet("TESTSCHEME")
    ws.append([None, None, None])  # row 1
    ws.append([None, None, "SBI Mutual Fund", "017"])  # row 2
    ws.append([None, None, "SCHEME NAME :", scheme_name])  # row 3
    ws.append([None, None, "PORTFOLIO STATEMENT AS ON :", "2026-05-31"])  # row 4
    ws.append([None])  # row 5
    ws.append(
        [None, None, "Name of the Instrument / Issuer", "ISIN", "Rating / Industry^",
         "Quantity", "Market value\n(Rs. in Lakhs)", "% to AUM"]
    )  # row 6
    ws.append([None])  # row 7
    ws.append([None, None, "EQUITY & EQUITY RELATED"])  # row 8, section header, no ISIN
    for name, isin, industry, qty, mv_lakhs, pct in holdings:
        ws.append([None, "100001", name, isin, industry, qty, mv_lakhs, pct])

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


class TestSBIParser:
    def test_parses_real_holding_rows_resolving_known_isin(self):
        raw = _build_workbook(
            "SBI Large and Midcap Fund",
            [("HDFC Bank Ltd.", "INE040A01034", "Banks", 38000000, 282929, 7.18)],
        )
        with patch(
            "ingestion.scrapers.sbi_mf_holdings.get_isin_to_ticker_map",
            return_value={"INE040A01034": "HDFCBANK"},
        ):
            df = parse(raw)

        assert len(df) == 1
        row = df.iloc[0]
        assert row["scheme_name"] == "SBI Large and Midcap Fund"
        assert row["ticker"] == "HDFCBANK"
        assert row["isin"] == "INE040A01034"
        assert row["quantity"] == 38000000
        assert row["value_inr"] == pytest.approx(282929 * 100_000)

    def test_section_header_rows_without_isin_are_skipped(self):
        """'EQUITY & EQUITY RELATED' etc. have no ISIN — must not appear as a holding."""
        raw = _build_workbook(
            "SBI Large and Midcap Fund",
            [("HDFC Bank Ltd.", "INE040A01034", "Banks", 38000000, 282929, 7.18)],
        )
        with patch(
            "ingestion.scrapers.sbi_mf_holdings.get_isin_to_ticker_map",
            return_value={"INE040A01034": "HDFCBANK"},
        ):
            df = parse(raw)

        assert "EQUITY & EQUITY RELATED" not in df["scheme_name"].values
        assert all(df["isin"].str.startswith("IN"))

    def test_isin_not_in_universe_is_skipped(self):
        """A holding whose ISIN doesn't resolve to a known ticker (e.g. a bond) must not appear."""
        raw = _build_workbook(
            "SBI Test Fund",
            [
                ("HDFC Bank Ltd.", "INE040A01034", "Banks", 100, 1000, 1.0),
                ("Some Corp Bond", "INE999Z99999", "Debt", 50, 500, 0.5),
            ],
        )
        with patch(
            "ingestion.scrapers.sbi_mf_holdings.get_isin_to_ticker_map",
            return_value={"INE040A01034": "HDFCBANK"},
        ):
            df = parse(raw)

        assert len(df) == 1
        assert df.iloc[0]["isin"] == "INE040A01034"

    def test_index_sheet_itself_is_never_parsed_as_holdings(self):
        raw = _build_workbook(
            "SBI Test Fund",
            [("HDFC Bank Ltd.", "INE040A01034", "Banks", 100, 1000, 1.0)],
        )
        with patch(
            "ingestion.scrapers.sbi_mf_holdings.get_isin_to_ticker_map",
            return_value={"INE040A01034": "HDFCBANK"},
        ):
            df = parse(raw)

        assert set(df["scheme_name"].unique()) == {"SBI Test Fund"}

    def test_no_recognizable_holdings_returns_empty_dataframe_not_exception(self):
        raw = _build_workbook("SBI Empty Fund", [])
        with patch("ingestion.scrapers.sbi_mf_holdings.get_isin_to_ticker_map", return_value={}):
            df = parse(raw)

        assert df.empty
        assert list(df.columns) == ["scheme_name", "isin", "ticker", "quantity", "value_inr"]


class TestSBIRegistration:
    def test_sbi_mutual_fund_is_registered_on_import(self):
        import ingestion.scrapers.sbi_mf_holdings  # noqa: F401 (import for its registration side effect)

        assert "SBI Mutual Fund (Direct, ISIN-exact)" in AMC_REGISTRY
