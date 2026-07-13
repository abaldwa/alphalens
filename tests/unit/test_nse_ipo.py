"""
tests/unit/test_nse_ipo.py

Unit tests for ingestion/scrapers/nse_ipo.py — mocked HTTP transport only
(same pattern as test_nse_pledge.py); the real parsing/dedup logic in
download_past_issues() runs unmocked against real-shaped NSE JSON.
"""

from datetime import date

import pytest
import requests

from ingestion.scrapers import nse_ipo


class _FakeResponse:
    def __init__(self, json_data, status_code=200):
        self._json = json_data
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status {self.status_code}")

    def json(self):
        return self._json


class _FakeSession:
    def __init__(self, json_data):
        self._json_data = json_data
        self.homepage_hit = False

    def get(self, url, timeout=None):
        if url == nse_ipo.NSE_HOMEPAGE_URL:
            self.homepage_hit = True
            return _FakeResponse({})
        return _FakeResponse(self._json_data)


class TestDownloadPastIssues:
    def test_parses_real_shaped_rows_and_skips_not_listed(self, monkeypatch):
        payload = [
            {"symbol": "FAKECO", "listingDate": "10-Jan-2024"},
            {"symbol": "NOTLISTEDYET", "listingDate": "-"},
            {"symbol": None, "listingDate": "10-Jan-2024"},
            {"symbol": "BADDATE", "listingDate": "not-a-date"},
        ]
        monkeypatch.setattr(nse_ipo, "_nse_session", lambda: _FakeSession(payload))
        result = nse_ipo.download_past_issues()
        assert result == {"FAKECO": date(2024, 1, 10)}

    def test_duplicate_ticker_keeps_earliest_listing(self, monkeypatch):
        payload = [
            {"symbol": "RELISTED", "listingDate": "10-Jan-2024"},
            {"symbol": "RELISTED", "listingDate": "05-Jan-2020"},
        ]
        monkeypatch.setattr(nse_ipo, "_nse_session", lambda: _FakeSession(payload))
        result = nse_ipo.download_past_issues()
        assert result == {"RELISTED": date(2020, 1, 5)}

    def test_empty_response_returns_empty_dict(self, monkeypatch):
        monkeypatch.setattr(nse_ipo, "_nse_session", lambda: _FakeSession([]))
        assert nse_ipo.download_past_issues() == {}

    def test_retries_then_raises_connection_error_on_persistent_failure(self, monkeypatch):
        calls = {"n": 0}

        def _failing_session():
            calls["n"] += 1
            raise requests.ConnectionError("boom")

        monkeypatch.setattr(nse_ipo, "_nse_session", _failing_session)
        monkeypatch.setattr(nse_ipo, "_TIMEOUT_S", 0.01)
        with pytest.raises(ConnectionError):
            nse_ipo.download_past_issues()
        assert calls["n"] == nse_ipo._MAX_RETRIES

    def test_recovers_after_transient_failure(self, monkeypatch):
        calls = {"n": 0}
        payload = [{"symbol": "OK", "listingDate": "01-Feb-2022"}]

        def _flaky_session():
            calls["n"] += 1
            if calls["n"] < 2:
                raise requests.ConnectionError("transient")
            return _FakeSession(payload)

        monkeypatch.setattr(nse_ipo, "_nse_session", _flaky_session)
        result = nse_ipo.download_past_issues()
        assert result == {"OK": date(2022, 2, 1)}
        assert calls["n"] == 2
