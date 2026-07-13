"""
tests/unit/test_large_deals.py

Phase: A65 (test coverage improvement)
Specs: SPEC-PIPE-008 (large deals ingestion, NSE archive fallback)
Owner: Platform / Ingestion
Consumers: pytest

Real-logic unit tests for ingestion/scrapers/large_deals.py's pure
parsing/normalisation functions and its DuckDB persistence path. No
network calls, no mocked/synthetic market data — only deterministic,
hand-built dicts exercising the real field-mapping/date-parsing logic
against realistic NSE/BSE payload shapes documented in the module's own
docstring, and a real in-memory DuckDB connection for persist_large_deals.
"""

import duckdb
import pandas as pd
import pytest

from datastore.schema.create_normalised import _CREATE_LARGE_DEALS
from ingestion.scrapers.large_deals import (
    _normalise_transaction_type,
    _parse_bse_date,
    _parse_bse_records,
    _parse_nse_date,
    _parse_nse_records,
    _REQUIRED_COLUMNS,
    persist_large_deals,
)


class TestParseNseDate:
    def test_dd_mmm_yyyy(self):
        assert _parse_nse_date("25-JUN-2024") == "2024-06-25"

    def test_dd_mm_yyyy(self):
        assert _parse_nse_date("25-06-2024") == "2024-06-25"

    def test_iso_format(self):
        assert _parse_nse_date("2024-06-25") == "2024-06-25"

    def test_dash_placeholder_returns_none(self):
        assert _parse_nse_date("-") is None

    def test_empty_string_returns_none(self):
        assert _parse_nse_date("") is None

    def test_unparseable_returns_none(self):
        assert _parse_nse_date("not-a-date") is None


class TestParseBseDate:
    def test_yyyymmdd(self):
        assert _parse_bse_date("20240625") == "2024-06-25"

    def test_dd_mm_yyyy(self):
        assert _parse_bse_date("25-06-2024") == "2024-06-25"

    def test_dd_slash_mm_slash_yyyy(self):
        assert _parse_bse_date("25/06/2024") == "2024-06-25"

    def test_dd_mmm_yyyy(self):
        assert _parse_bse_date("25-Jun-2024") == "2024-06-25"

    def test_placeholder_returns_none(self):
        assert _parse_bse_date("-") is None

    def test_none_input_returns_none(self):
        assert _parse_bse_date(None) is None


class TestNormaliseTransactionType:
    @pytest.mark.parametrize("raw,expected", [
        ("B", "B"), ("BUY", "B"), ("PURCHASE", "B"), ("buy", "B"),
        ("S", "S"), ("SELL", "S"), ("SALE", "S"), ("sell", "S"),
    ])
    def test_known_values(self, raw, expected):
        assert _normalise_transaction_type(raw) == expected

    def test_unknown_value_falls_back_to_first_char(self):
        assert _normalise_transaction_type("XYZ") == "X"

    def test_empty_string_returns_empty(self):
        assert _normalise_transaction_type("") == ""


class TestParseNseRecords:
    def test_snapshot_style_fields(self):
        records = [{
            "name": "RELIANCE INDUSTRIES", "no": "RELIANCE", "dt": "25-JUN-2024",
            "pd": "SOME CLIENT", "bs": "BUY", "qt": "1,234,567", "vl": "2850.50",
            "remarks": "",
        }]
        df = _parse_nse_records(records, "2024-06-25", "BULK")
        assert list(df.columns) == _REQUIRED_COLUMNS
        assert len(df) == 1
        row = df.iloc[0]
        assert row["ticker"] == "RELIANCE"
        assert row["trade_date"] == "2024-06-25"
        assert row["exchange"] == "NSE"
        assert row["deal_type"] == "BULK"
        assert row["client_name"] == "SOME CLIENT"
        assert row["transaction_type"] == "B"
        assert row["quantity"] == 1234567
        assert row["price"] == 2850.50

    def test_historical_style_field_names(self):
        records = [{
            "SCRIP_CD": "tcs", "TRADE_DT": "01-01-2024", "CLIENT_NAME": "HNI CLIENT",
            "BUY_SELL": "SELL", "QTY_TRD": "500000", "TRADE_PRICE": "3900.25",
            "REMARKS": "block window",
        }]
        df = _parse_nse_records(records, "2024-01-01", "BLOCK")
        row = df.iloc[0]
        assert row["ticker"] == "TCS"
        assert row["trade_date"] == "2024-01-01"
        assert row["transaction_type"] == "S"
        assert row["quantity"] == 500000
        assert row["remarks"] == "block window"

    def test_missing_ticker_skipped(self):
        records = [{"dt": "25-JUN-2024", "bs": "BUY", "qt": 100, "vl": 10.0}]
        df = _parse_nse_records(records, "2024-06-25", "BULK")
        assert df.empty
        assert list(df.columns) == _REQUIRED_COLUMNS

    def test_missing_date_falls_back_to_target_date(self):
        records = [{"no": "INFY", "bs": "BUY", "qt": 100, "vl": 10.0}]
        df = _parse_nse_records(records, "2024-06-25", "BULK")
        assert df.iloc[0]["trade_date"] == "2024-06-25"

    def test_empty_records_returns_empty_df_with_columns(self):
        df = _parse_nse_records([], "2024-06-25", "BULK")
        assert df.empty
        assert list(df.columns) == _REQUIRED_COLUMNS


class TestParseBseRecords:
    def test_standard_fields(self):
        records = [{
            "DT_DATE": "20240625", "SC_CODE": "500325", "SC_NAME": "RELIANCE",
            "SCRIP_ID": "RELIANCE", "CLNT_NAME": "CLIENT X", "BUY_SELL": "B",
            "DEAL_QTY": "1000000", "DEAL_PRICE": "2850.50",
        }]
        df = _parse_bse_records(records, "2024-06-25", "BULK")
        row = df.iloc[0]
        assert row["ticker"] == "RELIANCE"
        assert row["exchange"] == "BSE"
        assert row["transaction_type"] == "B"
        assert row["quantity"] == 1000000
        assert row["price"] == 2850.50

    def test_missing_scrip_id_falls_back_to_sc_name(self):
        records = [{
            "DT_DATE": "20240625", "SC_NAME": "SOME COMPANY LTD",
            "CLNT_NAME": "CLIENT", "BUY_SELL": "S",
            "DEAL_QTY": 1000, "DEAL_PRICE": 100.0,
        }]
        df = _parse_bse_records(records, "2024-06-25", "BLOCK")
        assert df.iloc[0]["ticker"] == "SOME COMPANY LTD"

    def test_missing_ticker_skipped(self):
        records = [{"DT_DATE": "20240625", "BUY_SELL": "B", "DEAL_QTY": 1, "DEAL_PRICE": 1.0}]
        df = _parse_bse_records(records, "2024-06-25", "BULK")
        assert df.empty


class TestPersistLargeDeals:
    def _conn(self):
        conn = duckdb.connect(":memory:")
        conn.execute(_CREATE_LARGE_DEALS)
        return conn

    def test_inserts_rows(self):
        conn = self._conn()
        df = pd.DataFrame([{
            "trade_date": "2024-06-25", "exchange": "NSE", "deal_type": "BULK",
            "ticker": "RELIANCE", "client_name": "X", "transaction_type": "B",
            "quantity": 1000, "price": 100.5, "remarks": None,
        }])
        inserted = persist_large_deals(conn, df, "2024-06-25")
        assert inserted == 1
        result = conn.execute("SELECT ticker, quantity, price FROM large_deals").fetchall()
        assert result == [("RELIANCE", 1000, 100.5)]

    def test_empty_dataframe_inserts_nothing(self):
        conn = self._conn()
        df = pd.DataFrame(columns=_REQUIRED_COLUMNS)
        inserted = persist_large_deals(conn, df, "2024-06-25")
        assert inserted == 0
        assert conn.execute("SELECT COUNT(*) FROM large_deals").fetchone()[0] == 0

    def test_replaces_existing_rows_for_same_date(self):
        conn = self._conn()
        df1 = pd.DataFrame([{
            "trade_date": "2024-06-25", "exchange": "NSE", "deal_type": "BULK",
            "ticker": "OLD", "client_name": None, "transaction_type": "B",
            "quantity": 1, "price": 1.0, "remarks": None,
        }])
        persist_large_deals(conn, df1, "2024-06-25")

        df2 = pd.DataFrame([{
            "trade_date": "2024-06-25", "exchange": "NSE", "deal_type": "BULK",
            "ticker": "NEW", "client_name": None, "transaction_type": "S",
            "quantity": 2, "price": 2.0, "remarks": None,
        }])
        persist_large_deals(conn, df2, "2024-06-25")

        tickers = conn.execute("SELECT ticker FROM large_deals").fetchall()
        assert tickers == [("NEW",)]
