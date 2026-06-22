"""
tests/unit/test_macro.py

Phase: 0.4 (Data Ingestion Scrapers)
Specs: SPEC-PIPE-006
Owner: Platform / Ingestion
Consumers: CI, pytest

Unit tests for ingestion/scrapers/macro.py. All HTTP calls are mocked —
these tests never make real network calls.
"""

from datetime import date

import pytest
import requests

from datastore.api.db import get_duckdb_connection
from datastore.schema import create_normalised
from ingestion.scrapers import macro


class _StubResponse:
    def __init__(self, json_data, status_ok=True):
        self._json_data = json_data
        self._status_ok = status_ok

    def raise_for_status(self):
        if not self._status_ok:
            raise requests.HTTPError("simulated HTTP error")

    def json(self):
        return self._json_data


class _StubSession:
    """Records every .get() call and returns canned responses in order."""

    def __init__(self, responses):
        self._responses = iter(responses)
        self.calls = []
        self.headers = {}  # download_fx's _fetch() calls session.headers.update(...)

    def get(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return next(self._responses)


def _seed_macro_indicator(indicator, rows):
    """rows: list of (date_str, value)."""
    create_normalised.create_schema(in_memory=True)
    with get_duckdb_connection(None) as conn:
        conn.executemany(
            "INSERT INTO macro_indicators (date, indicator, value) VALUES (?, ?, ?)",
            [(d, indicator, v) for d, v in rows],
        )


# ===== download_vix =====


def test_download_vix_happy_path(monkeypatch):
    stub = _StubSession([_StubResponse({"data": [{"EOD_CLOSE_INDEX_VAL": "14.25"}]})])
    monkeypatch.setattr(macro, "_session", lambda: stub)

    result = macro.download_vix("2026-06-19")

    assert result == 14.25


def test_download_vix_empty_data_falls_back_to_previous_value(monkeypatch):
    """The empty-data guard added this session must route through the documented fallback, not raise."""
    _seed_macro_indicator("INDIA_VIX", [("2026-06-18", 13.5)])
    monkeypatch.setattr(macro, "RETRY_DELAY_SECONDS", 0)
    stub = _StubSession([_StubResponse({"data": []})] * macro.MAX_RETRIES)
    monkeypatch.setattr(macro, "_session", lambda: stub)

    result = macro.download_vix("2026-06-19", in_memory=True)

    assert result == 13.5
    assert len(stub.calls) == macro.MAX_RETRIES


def test_download_vix_raises_connection_error_when_no_fallback(monkeypatch):
    monkeypatch.setattr(macro, "RETRY_DELAY_SECONDS", 0)
    stub = _StubSession([_StubResponse({"data": []})] * macro.MAX_RETRIES)
    monkeypatch.setattr(macro, "_session", lambda: stub)

    with pytest.raises(ConnectionError):
        macro.download_vix("2026-06-19", in_memory=True)


def test_download_vix_malformed_json_also_falls_back(monkeypatch):
    """A missing 'data' key (not just an empty list) must also be treated as empty, not raise KeyError."""
    _seed_macro_indicator("INDIA_VIX", [("2026-06-18", 13.5)])
    monkeypatch.setattr(macro, "RETRY_DELAY_SECONDS", 0)
    stub = _StubSession([_StubResponse({})] * macro.MAX_RETRIES)
    monkeypatch.setattr(macro, "_session", lambda: stub)

    result = macro.download_vix("2026-06-19", in_memory=True)

    assert result == 13.5


# ===== download_fiidii =====


def test_download_fiidii_happy_path(monkeypatch):
    payload = [
        {"category": "FII/FPI", "buyValue": "1000.5", "sellValue": "800.25"},
        {"category": "DII", "buyValue": "500.0", "sellValue": "600.0"},
    ]
    stub = _StubSession([_StubResponse(payload)])
    monkeypatch.setattr(macro, "_session", lambda: stub)

    result = macro.download_fiidii("2026-06-19")

    assert result["fii_buy_cr"] == 1000.5
    assert result["fii_net_cr"] == pytest.approx(200.25)
    assert result["dii_net_cr"] == pytest.approx(-100.0)
    assert result["is_stale"] is False


def test_download_fiidii_falls_back_with_is_stale_flag(monkeypatch):
    """SPEC-PIPE-006: FII/DII fallback must mark is_stale=True, unlike VIX/FX's plain value fallback."""
    _seed_macro_indicator("FII_DII_NET", [("2026-06-18", 250.0)])
    monkeypatch.setattr(macro, "RETRY_DELAY_SECONDS", 0)
    stub = _StubSession([_StubResponse(None, status_ok=False)] * macro.MAX_RETRIES)
    monkeypatch.setattr(macro, "_session", lambda: stub)

    result = macro.download_fiidii("2026-06-19", in_memory=True)

    assert result["is_stale"] is True
    assert result["fii_net_cr"] == 250.0
    assert result["fii_buy_cr"] is None


# ===== download_fx =====


def test_download_fx_happy_path(monkeypatch):
    payload = {"chart": {"result": [{"meta": {"regularMarketPrice": 83.45}}]}}
    stub = _StubSession([_StubResponse(payload)])
    monkeypatch.setattr(macro.requests, "Session", lambda: stub)

    result = macro.download_fx("2026-06-19")

    assert result == {"usd_inr": 83.45}


def test_download_fx_empty_result_falls_back_to_previous_value(monkeypatch):
    """The empty-result guard added this session must route through the documented fallback, not raise IndexError."""
    _seed_macro_indicator("USD_INR", [("2026-06-18", 83.0)])
    monkeypatch.setattr(macro, "RETRY_DELAY_SECONDS", 0)
    payload = {"chart": {"result": []}}
    stub = _StubSession([_StubResponse(payload)] * macro.MAX_RETRIES)
    monkeypatch.setattr(macro.requests, "Session", lambda: stub)

    result = macro.download_fx("2026-06-19", in_memory=True)

    assert result == {"usd_inr": 83.0}


def test_download_fx_raises_connection_error_when_no_fallback(monkeypatch):
    monkeypatch.setattr(macro, "RETRY_DELAY_SECONDS", 0)
    payload = {"chart": {"result": []}}
    stub = _StubSession([_StubResponse(payload)] * macro.MAX_RETRIES)
    monkeypatch.setattr(macro.requests, "Session", lambda: stub)

    with pytest.raises(ConnectionError):
        macro.download_fx("2026-06-19", in_memory=True)


# ===== _get_previous_value =====


def test_get_previous_value_returns_none_when_no_history():
    create_normalised.create_schema(in_memory=True)
    result = macro._get_previous_value("INDIA_VIX", date(2026, 6, 19), in_memory=True)
    assert result is None


def test_get_previous_value_only_looks_strictly_before_date():
    """PIT: must never return a value dated on or after before_date."""
    _seed_macro_indicator("INDIA_VIX", [("2026-06-17", 12.0), ("2026-06-18", 13.0), ("2026-06-19", 14.0)])

    result = macro._get_previous_value("INDIA_VIX", date(2026, 6, 19), in_memory=True)

    assert result == 13.0  # the most recent value strictly before 2026-06-19, not the same-day 14.0


# ===== download_crude_oil / download_gold (P1.2) =====


def test_download_crude_oil_happy_path(monkeypatch):
    payload = {"chart": {"result": [{"meta": {"regularMarketPrice": 78.12}}]}}
    stub = _StubSession([_StubResponse(payload)])
    monkeypatch.setattr(macro.requests, "Session", lambda: stub)

    result = macro.download_crude_oil("2026-06-19")

    assert result == {"crude_oil_price": 78.12}


def test_download_crude_oil_empty_result_falls_back_to_previous_value(monkeypatch):
    _seed_macro_indicator("CRUDE_OIL", [("2026-06-18", 77.0)])
    monkeypatch.setattr(macro, "RETRY_DELAY_SECONDS", 0)
    payload = {"chart": {"result": []}}
    stub = _StubSession([_StubResponse(payload)] * macro.MAX_RETRIES)
    monkeypatch.setattr(macro.requests, "Session", lambda: stub)

    result = macro.download_crude_oil("2026-06-19", in_memory=True)

    assert result == {"crude_oil_price": 77.0}


def test_download_gold_happy_path(monkeypatch):
    payload = {"chart": {"result": [{"meta": {"regularMarketPrice": 4150.0}}]}}
    stub = _StubSession([_StubResponse(payload)])
    monkeypatch.setattr(macro.requests, "Session", lambda: stub)

    result = macro.download_gold("2026-06-19")

    assert result == {"gold_price": 4150.0}


def test_download_gold_raises_connection_error_when_no_fallback(monkeypatch):
    monkeypatch.setattr(macro, "RETRY_DELAY_SECONDS", 0)
    payload = {"chart": {"result": []}}
    stub = _StubSession([_StubResponse(payload)] * macro.MAX_RETRIES)
    monkeypatch.setattr(macro.requests, "Session", lambda: stub)

    with pytest.raises(ConnectionError):
        macro.download_gold("2026-06-19", in_memory=True)


# ===== download_bond_yields (P1.2) =====

_FRED_CSV_10YR = "observation_date,INDIRLTLT01STM\n2026-04-01,7.05\n2026-05-01,7.02\n"
_FRED_CSV_3M = "observation_date,INDIR3TIB01STM\n2026-04-01,5.50\n2026-05-01,5.39\n"


class _StubGetResponse:
    def __init__(self, text, status_ok=True):
        self.text = text
        self._status_ok = status_ok

    def raise_for_status(self):
        if not self._status_ok:
            raise requests.HTTPError("simulated HTTP error")


def test_download_bond_yields_happy_path(monkeypatch):
    """Picks the most recent FRED observation <= the requested date (monthly series, forward-fill)."""
    responses = iter([_StubGetResponse(_FRED_CSV_10YR), _StubGetResponse(_FRED_CSV_3M)])
    monkeypatch.setattr(macro.requests, "get", lambda *a, **k: next(responses))

    result = macro.download_bond_yields("2026-06-19")

    assert result == {"yield_10yr": 7.02, "yield_3m": 5.39}


def test_download_bond_yields_never_reads_observation_after_as_of(monkeypatch):
    """PIT: a request for 2026-04-15 must not see the 2026-05-01 observation."""
    responses = iter([_StubGetResponse(_FRED_CSV_10YR), _StubGetResponse(_FRED_CSV_3M)])
    monkeypatch.setattr(macro.requests, "get", lambda *a, **k: next(responses))

    result = macro.download_bond_yields("2026-04-15")

    assert result == {"yield_10yr": 7.05, "yield_3m": 5.50}


def test_download_bond_yields_falls_back_to_previous_value_on_fetch_failure(monkeypatch):
    _seed_macro_indicator("YIELD_10YR", [("2026-06-01", 6.9)])
    _seed_macro_indicator("YIELD_3M", [("2026-06-01", 5.2)])
    monkeypatch.setattr(macro, "RETRY_DELAY_SECONDS", 0)
    monkeypatch.setattr(macro.requests, "get", lambda *a, **k: _StubGetResponse("", status_ok=False))

    result = macro.download_bond_yields("2026-06-19", in_memory=True)

    assert result == {"yield_10yr": 6.9, "yield_3m": 5.2}


def test_download_bond_yields_raises_connection_error_when_no_fallback(monkeypatch):
    monkeypatch.setattr(macro, "RETRY_DELAY_SECONDS", 0)
    monkeypatch.setattr(macro.requests, "get", lambda *a, **k: _StubGetResponse("", status_ok=False))

    with pytest.raises(ConnectionError):
        macro.download_bond_yields("2026-06-19", in_memory=True)
