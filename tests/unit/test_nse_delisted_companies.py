"""
tests/unit/test_nse_delisted_companies.py

2026-07-19 full-codebase-review Fix A4. NSE's real delisted-companies
endpoint structure is UNVERIFIED in this environment (see
ingestion/scrapers/nse_delisted_companies.py's module docstring — NSE
returns HTTP 403 to every request from this environment). These tests
exercise the parser's robustness (multiple candidate key names, missing
fields, malformed shapes) against hand-constructed records shaped like
NSE's documented corporate-database JSON convention — explicitly NOT a
claim that this is the real, verified response shape.
"""


from ingestion.scrapers.nse_delisted_companies import (
    parse_delisted_companies,
    _parse_date,
)


class TestParseDelistedCompanies:
    def test_parses_record_with_primary_key_names(self):
        records = [{
            "symbol": "OLDCO", "companyName": "Old Company Ltd",
            "delistingDate": "15-06-2020", "delistingType": "Compulsory",
        }]
        rows = parse_delisted_companies(records)
        assert len(rows) == 1
        assert rows[0]["ticker"] == "OLDCO"
        assert rows[0]["company_name"] == "Old Company Ltd"
        assert rows[0]["delisting_type"] == "Compulsory"

    def test_falls_back_to_alternate_key_names(self):
        records = [{"sm_symbol": "ALTCO", "sm_name": "Alt Company", "delistDt": "2020-06-15"}]
        rows = parse_delisted_companies(records)
        assert rows[0]["ticker"] == "ALTCO"

    def test_record_with_no_symbol_key_is_skipped_not_fabricated(self):
        records = [{"companyName": "Nameless Co", "delistingDate": "15-06-2020"}]
        rows = parse_delisted_companies(records)
        assert rows == []

    def test_ticker_is_uppercased_and_stripped(self):
        records = [{"symbol": " lowerco "}]
        rows = parse_delisted_companies(records)
        assert rows[0]["ticker"] == "LOWERCO"

    def test_unparseable_date_returns_none_not_a_guess(self):
        records = [{"symbol": "BADDATE", "delistingDate": "not-a-date"}]
        rows = parse_delisted_companies(records)
        assert rows[0]["delisting_date"] is None

    def test_empty_records_returns_empty(self):
        assert parse_delisted_companies([]) == []


class TestParseDate:
    def test_dd_mm_yyyy_dash(self):
        assert str(_parse_date("15-06-2020")) == "2020-06-15"

    def test_iso_format(self):
        assert str(_parse_date("2020-06-15")) == "2020-06-15"

    def test_none_input_returns_none(self):
        assert _parse_date(None) is None
