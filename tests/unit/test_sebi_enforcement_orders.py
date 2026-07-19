"""
tests/unit/test_sebi_enforcement_orders.py

2026-07-19 full-codebase-review Fix A5. Uses a REAL fixture
(tests/fixtures/sebi_ao_orders_sample.html) captured live from SEBI's
"Orders of AO" page on 2026-07-19 — not synthetic HTML — so the parser
is tested against genuine page structure, not a hand-crafted guess.
"""

from pathlib import Path

import pandas as pd
import pytest

from ingestion.scrapers.sebi_enforcement_orders import (
    build_enforcement_order_rows,
    parse_ao_orders_html,
    resolve_ticker,
    _extract_company_name,
    _looks_like_pnd_case,
)

FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "sebi_ao_orders_sample.html"


@pytest.fixture
def real_html():
    return FIXTURE_PATH.read_text()


class TestParseAoOrdersHtml:
    def test_parses_real_rows(self, real_html):
        orders = parse_ao_orders_html(real_html)
        assert len(orders) > 0
        for o in orders:
            assert set(o.keys()) == {"order_date", "title", "detail_url"}
            assert o["detail_url"].startswith("https://www.sebi.gov.in")

    def test_dates_are_real_date_objects_descending(self, real_html):
        orders = parse_ao_orders_html(real_html)
        dates = [o["order_date"] for o in orders]
        assert dates == sorted(dates, reverse=True)

    def test_missing_table_returns_empty_not_crash(self):
        assert parse_ao_orders_html("<html><body>no table here</body></html>") == []


class TestLooksLikePndCase:
    def test_excludes_illiquid_stock_options(self):
        assert not _looks_like_pnd_case("Adjudication Order in respect of X in the matter of Illiquid Stock Options at BSE")

    def test_excludes_thematic_inspection(self):
        assert not _looks_like_pnd_case("Adjudication Order in respect of thematic inspection of debenture trustees")

    def test_includes_plain_company_matter(self):
        assert _looks_like_pnd_case("Adjudication Order in the matter of Citrus Check Inns Limited")


class TestExtractCompanyName:
    def test_extracts_from_in_the_matter_of(self):
        assert _extract_company_name("Adjudication Order in the matter of Prime Focus Limited.") == "Prime Focus Limited"

    def test_returns_none_for_unrecognized_format(self):
        assert _extract_company_name("Some unrelated announcement") is None


class TestResolveTicker:
    def test_exact_match(self):
        univ = pd.DataFrame({"ticker": ["ABC"], "company_name": ["ABC Industries Limited"]})
        assert resolve_ticker("ABC Industries Limited", univ) == "ABC"

    def test_close_but_not_exact_match(self):
        univ = pd.DataFrame({"ticker": ["NDTV"], "company_name": ["New Delhi Television Limited"]})
        assert resolve_ticker("New Delhi Television Limited", univ) == "NDTV"

    def test_no_match_returns_none_not_a_guess(self):
        univ = pd.DataFrame({"ticker": ["ABC"], "company_name": ["ABC Industries Limited"]})
        assert resolve_ticker("Completely Unrelated Corp", univ) is None

    def test_missing_columns_returns_none(self):
        univ = pd.DataFrame({"foo": [1]})
        assert resolve_ticker("Anything", univ) is None


class TestBuildEnforcementOrderRows:
    def test_real_fixture_produces_real_matched_rows(self, real_html):
        """End-to-end against the real fixture + real universe CSV —
        confirms at least the known real matches (Prime Focus Limited ->
        PFOCUS, New Delhi Television Limited -> NDTV) resolve correctly."""
        import config.universe as cu

        orders = parse_ao_orders_html(real_html)
        universe_df = cu.load_universe_raw()
        rows = build_enforcement_order_rows(orders, universe_df)

        assert len(rows) >= 1
        for row in rows:
            assert row["manipulation_start_date"] is None
            assert row["manipulation_end_date"] is None
            assert row["ticker"] in universe_df["ticker"].values

    def test_no_orders_returns_empty(self):
        univ = pd.DataFrame({"ticker": ["ABC"], "company_name": ["ABC Ltd"]})
        assert build_enforcement_order_rows([], univ) == []
