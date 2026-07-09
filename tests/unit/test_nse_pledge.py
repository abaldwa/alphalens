"""
tests/unit/test_nse_pledge.py

Unit tests for ingestion/scrapers/nse_pledge.py — mocked HTTP only.
"""

import pytest

from ingestion.scrapers import nse_pledge


class _FakeResponse:
    def __init__(self, json_data, status_code=200):
        self._json = json_data
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(f"status {self.status_code}")

    def json(self):
        return self._json


class _FakeSession:
    def __init__(self, json_data):
        self._json_data = json_data

    def get(self, url, params=None, timeout=None):
        if "www.nseindia.com" == url.rstrip("/").replace("https://", ""):
            return _FakeResponse({})
        return _FakeResponse(self._json_data)


class TestDownloadPledgeData:
    def test_parses_real_shaped_response(self, monkeypatch):
        payload = {
            "promoterNameList": ["Some Promoter"],
            "data": [
                {
                    "broadcastdate": "08-Jul-2025 11:53:03",
                    "postEventHoldingPerc": "    25.77",
                    "attachment": "https://nsearchives.nseindia.com/corporate/FAKE.zip",
                },
                {
                    "broadcastdate": "not-a-date",
                    "postEventHoldingPerc": "0",
                    "attachment": None,
                },
            ],
        }
        monkeypatch.setattr(nse_pledge, "_nse_session", lambda: _FakeSession(payload))
        df = nse_pledge.download_pledge_data("FAKE", "2020-01-01", "2026-07-07")
        assert len(df) == 1  # the unparseable-date row is dropped
        assert df.iloc[0]["post_event_holding_pct"] == 25.77
        assert df.iloc[0]["ticker"] == "FAKE"

    def test_empty_data_returns_empty_frame(self, monkeypatch):
        monkeypatch.setattr(nse_pledge, "_nse_session", lambda: _FakeSession({"data": []}))
        df = nse_pledge.download_pledge_data("NOPLEDGE", "2020-01-01", "2026-07-07")
        assert df.empty

    def test_raises_after_retries_on_connection_failure(self, monkeypatch):
        def _raise_session():
            raise nse_pledge.requests.RequestException("unreachable")

        monkeypatch.setattr(nse_pledge, "_nse_session", _raise_session)
        with pytest.raises(ConnectionError):
            nse_pledge.download_pledge_data("FAKE", "2020-01-01", "2026-07-07")


class TestParsePct:
    def test_handles_whitespace_padded_and_zero(self):
        assert nse_pledge._parse_pct("    25.77") == 25.77
        assert nse_pledge._parse_pct("0") == 0.0
        assert nse_pledge._parse_pct(None) is None
        assert nse_pledge._parse_pct("") is None
