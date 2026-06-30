"""
tests/unit/test_groww_mf_holdings.py

Phase: 2.2 (AMFI MF Holdings + Corporate Action Features)
Specs: SPEC-PIPE-001, SPEC-PIPE-003 (CRITICAL), SPEC-MFHOLD-001
Owner: Platform / QA
Consumers: CI, pytest

Tests ingestion/scrapers/groww_mf_holdings.py (the primary MF-holdings
source — see SPEC-MFHOLD-001) against synthetic data matching the real,
live-verified Groww response shapes — no real network call in CI (every
`requests.get` is mocked).
"""

import json
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from ingestion.scrapers.groww_mf_holdings import (
    _normalize_company_name,
    discover_amc_directory,
    make_amc_fetcher,
    parse_amc,
)


def _scheme_detail(scheme_name, aum, holdings, portfolio_date="2026-05-30T18:30:00.000Z"):
    return {
        "scheme_name": scheme_name,
        "aum": aum,
        "isin": "INF200K01RA0",
        "holdings": [
            {
                "scheme_code": "119835",
                "portfolio_date": portfolio_date,
                "company_name": name,
                "nature_name": nature,
                "sector_name": "Financial",
                "instrument_name": nature.title(),
                "corpus_per": pct,
                "stock_search_id": None,
            }
            for name, nature, pct in holdings
        ],
    }


class TestNormalizeCompanyName:
    def test_strips_ltd_and_punctuation(self):
        assert _normalize_company_name("HDFC Bank Ltd") == _normalize_company_name("Hdfc Bank Limited")

    def test_empty_or_none_returns_empty_string(self):
        assert _normalize_company_name(None) == ""
        assert _normalize_company_name("") == ""


class TestGrowwParseAMC:
    def test_resolves_known_company_name_to_ticker(self):
        raw = json.dumps(
            [_scheme_detail("Test Fund Direct Growth", aum=1000.0, holdings=[("HDFC Bank Ltd", "EQUITY", 10.0)])]
        ).encode()

        with patch(
            "ingestion.scrapers.groww_mf_holdings._build_company_name_to_ticker_isin_map",
            return_value={"hdfcbank": ("HDFCBANK", "INE040A01034")},
        ):
            df = parse_amc(raw)

        assert len(df) == 1
        row = df.iloc[0]
        assert row["ticker"] == "HDFCBANK"
        assert row["isin"] == "INE040A01034"
        assert np.isnan(row["quantity"])
        assert row["value_inr"] == pytest.approx((10.0 / 100.0) * 1000.0 * 1e7)

    def test_non_equity_nature_is_excluded(self):
        raw = json.dumps(
            [_scheme_detail("Test Fund", aum=1000.0, holdings=[("Some Bond", "DEBT", 5.0)])]
        ).encode()

        with patch("ingestion.scrapers.groww_mf_holdings._build_company_name_to_ticker_isin_map", return_value={}):
            df = parse_amc(raw)

        assert df.empty

    def test_futures_and_options_are_excluded_even_though_tagged_equity(self):
        """A futures contract isn't share ownership — must not be counted as a holding."""
        raw = json.dumps(
            [
                _scheme_detail(
                    "Test Fund",
                    aum=1000.0,
                    holdings=[
                        ("HDFC Bank Ltd", "EQUITY", 10.0),
                        ("Jindal Steel & Power Ltd Futures", "EQUITY", 3.0),
                        ("Nifty Options", "EQUITY", 1.0),
                    ],
                )
            ]
        ).encode()

        with patch(
            "ingestion.scrapers.groww_mf_holdings._build_company_name_to_ticker_isin_map",
            return_value={
                "hdfcbank": ("HDFCBANK", "INE040A01034"),
                "jindalsteelpower": ("JINDALSTEL", "INE749A01030"),
            },
        ):
            df = parse_amc(raw)

        assert len(df) == 1
        assert df.iloc[0]["ticker"] == "HDFCBANK"

    def test_unresolvable_company_name_is_skipped_not_an_exception(self):
        raw = json.dumps(
            [_scheme_detail("Test Fund", aum=1000.0, holdings=[("Some Obscure Microcap Ltd", "EQUITY", 1.0)])]
        ).encode()

        with patch("ingestion.scrapers.groww_mf_holdings._build_company_name_to_ticker_isin_map", return_value={}):
            df = parse_amc(raw)

        assert df.empty
        assert list(df.columns) == ["scheme_name", "isin", "ticker", "quantity", "value_inr"]

    def test_multiple_schemes_aggregate(self):
        raw = json.dumps(
            [
                _scheme_detail("Fund A", aum=1000.0, holdings=[("HDFC Bank Ltd", "EQUITY", 10.0)]),
                _scheme_detail("Fund B", aum=2000.0, holdings=[("HDFC Bank Ltd", "EQUITY", 5.0)]),
            ]
        ).encode()

        with patch(
            "ingestion.scrapers.groww_mf_holdings._build_company_name_to_ticker_isin_map",
            return_value={"hdfcbank": ("HDFCBANK", "INE040A01034")},
        ):
            df = parse_amc(raw)

        assert len(df) == 2
        assert set(df["scheme_name"]) == {"Fund A", "Fund B"}


class TestGrowwFetcherPITValidation:
    """SPEC-PIPE-003 spirit: Groww only exposes the current live snapshot — never backfill a stale month silently."""

    def test_snapshot_matching_requested_month_succeeds(self):
        detail = _scheme_detail(
            "Test Fund", aum=1000.0, holdings=[("HDFC Bank Ltd", "EQUITY", 10.0)],
            portfolio_date="2026-05-30T18:30:00.000Z",
        )
        import pathlib

        with patch("ingestion.scrapers.groww_mf_holdings._list_scheme_ids", return_value=["test-fund"]), \
             patch("ingestion.scrapers.groww_mf_holdings._fetch_scheme_detail", return_value=detail), \
             patch("ingestion.scrapers.groww_mf_holdings.time.sleep"), \
             patch("ingestion.scrapers.groww_mf_holdings.AMFI_RAW_DIR", new=pathlib.Path("/tmp/test_groww_raw")):
            fetch_fn = make_amc_fetcher("Test AMC")
            raw = fetch_fn(2026, 5)

        schemes = json.loads(raw)
        assert len(schemes) == 1

    def test_snapshot_not_matching_requested_month_raises(self):
        """Requesting March data when Groww's live snapshot is May — must fail loud, not mislabel."""
        detail = _scheme_detail(
            "Test Fund", aum=1000.0, holdings=[("HDFC Bank Ltd", "EQUITY", 10.0)],
            portfolio_date="2026-05-30T18:30:00.000Z",
        )
        with patch("ingestion.scrapers.groww_mf_holdings._list_scheme_ids", return_value=["test-fund"]), \
             patch("ingestion.scrapers.groww_mf_holdings._fetch_scheme_detail", return_value=detail), \
             patch("ingestion.scrapers.groww_mf_holdings.time.sleep"):
            fetch_fn = make_amc_fetcher("Test AMC")
            with pytest.raises(ConnectionError, match="no historical archive"):
                fetch_fn(2026, 3)

    def test_no_schemes_found_raises_clear_error(self):
        with patch("ingestion.scrapers.groww_mf_holdings._list_scheme_ids", return_value=[]):
            fetch_fn = make_amc_fetcher("Empty AMC")
            with pytest.raises(ConnectionError, match="no schemes found"):
                fetch_fn(2026, 5)

    def test_one_scheme_fetch_failure_does_not_abort_the_amc(self):
        good_detail = _scheme_detail("Fund A", aum=1000.0, holdings=[("HDFC Bank Ltd", "EQUITY", 10.0)])

        def fake_fetch(scheme_id):
            return None if scheme_id == "bad-fund" else good_detail

        with patch("ingestion.scrapers.groww_mf_holdings._list_scheme_ids", return_value=["good-fund", "bad-fund"]), \
                patch("ingestion.scrapers.groww_mf_holdings._fetch_scheme_detail", side_effect=fake_fetch), \
                patch("ingestion.scrapers.groww_mf_holdings.time.sleep"):
            fetch_fn = make_amc_fetcher("Test AMC")
            raw = fetch_fn(2026, 5)

        schemes = json.loads(raw)
        assert len(schemes) == 1


class TestDiscoverGrowwAMCDirectory:
    def test_parses_amc_list_from_next_data(self):
        next_data = {
            "props": {
                "pageProps": {
                    "amcMainPageData": [
                        {"amcs": [{"name": "SBI Mutual Fund", "search_id": "sbi-mutual-funds"}]}
                    ]
                }
            }
        }
        html = f'<html><script id="__NEXT_DATA__">{json.dumps(next_data)}</script></html>'
        mock_response = MagicMock(status_code=200, text=html)
        mock_response.raise_for_status = MagicMock()

        with patch("ingestion.scrapers.groww_mf_holdings.requests.get", return_value=mock_response):
            amcs = discover_amc_directory()

        assert amcs == [{"name": "SBI Mutual Fund", "search_id": "sbi-mutual-funds"}]

    def test_missing_next_data_raises_clear_error(self):
        mock_response = MagicMock(status_code=200, text="<html>no data here</html>")
        mock_response.raise_for_status = MagicMock()

        with patch("ingestion.scrapers.groww_mf_holdings.requests.get", return_value=mock_response):
            with pytest.raises(ValueError, match="__NEXT_DATA__ not found"):
                discover_amc_directory()
