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


import duckdb

from datastore.schema.create_normalised import _CREATE_DELISTED_COMPANIES
from ingestion.scrapers.nse_delisted_companies import (
    KNOWN_MAJOR_DELISTINGS,
    parse_delisted_companies,
    seed_known_major_delistings,
    write_delisted_companies,
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


class TestKnownMajorDelistingsSeed:
    """2026-07-21 full-codebase-review REV13: real, documented major NSE
    delistings/mergers/suspensions, used as a stopgap when the live NSE
    scraper can't reach its endpoint (confirmed network-blocked from this
    environment)."""

    def test_seed_writes_every_known_entry(self):
        conn = duckdb.connect(":memory:")
        conn.execute(_CREATE_DELISTED_COMPANIES)
        n = seed_known_major_delistings(conn)
        assert n == len(KNOWN_MAJOR_DELISTINGS)
        count = conn.execute("SELECT COUNT(*) FROM delisted_companies").fetchone()[0]
        assert count == len(KNOWN_MAJOR_DELISTINGS)

    def test_every_known_entry_has_a_ticker_and_real_delisting_date(self):
        for row in KNOWN_MAJOR_DELISTINGS:
            assert row["ticker"]
            assert row["delisting_date"] is not None
            assert row["delisting_type"] in ("merger", "suspension")

    def test_seed_is_idempotent_and_does_not_duplicate(self):
        conn = duckdb.connect(":memory:")
        conn.execute(_CREATE_DELISTED_COMPANIES)
        seed_known_major_delistings(conn)
        seed_known_major_delistings(conn)
        count = conn.execute("SELECT COUNT(*) FROM delisted_companies").fetchone()[0]
        assert count == len(KNOWN_MAJOR_DELISTINGS)

    def test_live_scraper_row_overwrites_seed_row_for_same_ticker(self):
        """A later, genuine NSE-sourced row must win over the stopgap
        entry for the same ticker (ON CONFLICT upsert), not be blocked or
        duplicated."""
        conn = duckdb.connect(":memory:")
        conn.execute(_CREATE_DELISTED_COMPANIES)
        seed_known_major_delistings(conn)
        write_delisted_companies(conn, [{
            "ticker": "SATYAMCOMP", "company_name": "Satyam Computer Services Ltd (NSE-verified)",
            "delisting_date": None, "delisting_type": "merger (NSE-confirmed)",
            "source_url": "https://www.nseindia.com/real-endpoint",
        }])
        row = conn.execute(
            "SELECT company_name, delisting_type FROM delisted_companies WHERE ticker = 'SATYAMCOMP'"
        ).fetchone()
        assert row[0] == "Satyam Computer Services Ltd (NSE-verified)"
        assert row[1] == "merger (NSE-confirmed)"
