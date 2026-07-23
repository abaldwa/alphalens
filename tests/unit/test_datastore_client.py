from datetime import datetime
import json

import pandas as pd

from datastore.client import DataStoreClient


class _DummyResponse:
    def __init__(self, payload, status_code: int = 200):
        self._payload = payload
        self.text = json.dumps(payload)
        self.status_code = status_code

    def raise_for_status(self) -> None:
        return None


class _DummyClient:
    def __init__(self, response):
        self._response = response

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, url, params=None):
        return self._response


def test_get_ohlcv_bulk_parses_data_payload(monkeypatch):
    payload = {
        "data": [
            {
                "date": "2024-01-01",
                "ticker": "RELIANCE",
                "open": 2500.0,
                "high": 2520.0,
                "low": 2480.0,
                "close": 2510.0,
                "volume": 1000,
                "delivery_pct": 0.12,
                "adj_factor": 1.0,
            }
        ]
    }

    monkeypatch.setattr(
        "datastore.client.httpx.Client",
        lambda timeout=30.0: _DummyClient(_DummyResponse(payload)),
    )

    client = DataStoreClient(base_url="http://example.test")
    df = client.get_ohlcv_bulk(datetime(2024, 1, 1), datetime(2024, 1, 2))

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["ticker"] == "RELIANCE"
    assert pd.api.types.is_datetime64_any_dtype(df["date"])


def test_get_index_ohlcv_parses_flat_array_payload(monkeypatch):
    # ML17a: GET /api/v1/ohlcv/index/{index_name} returns a flat JSON array
    # (pandas .to_json(orient="records")), not a {"data": [...]} envelope —
    # unlike get_ohlcv_bulk's payload shape above.
    payload = [
        {
            "date": "2026-07-01", "index_name": "Nifty 500", "open": 22000.0,
            "high": 22100.0, "low": 21950.0, "close": 22050.0, "volume": None,
        }
    ]

    captured_urls = []

    class _CapturingDummyClient(_DummyClient):
        def get(self, url, params=None):
            captured_urls.append(url)
            return self._response

    monkeypatch.setattr(
        "datastore.client.httpx.Client",
        lambda timeout=60.0: _CapturingDummyClient(_DummyResponse(payload)),
    )

    client = DataStoreClient(base_url="http://example.test")
    df = client.get_index_ohlcv("Nifty 500", datetime(2026, 1, 1), datetime(2026, 7, 1))

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["index_name"] == "Nifty 500"
    assert df.iloc[0]["close"] == 22050.0
    assert pd.api.types.is_datetime64_any_dtype(df["date"])
    # Space in "Nifty 500" must be percent-encoded in the request path.
    assert "Nifty%20500" in captured_urls[0]


def test_get_index_ohlcv_empty_payload_returns_empty_df_with_columns(monkeypatch):
    monkeypatch.setattr(
        "datastore.client.httpx.Client",
        lambda timeout=60.0: _DummyClient(_DummyResponse([])),
    )
    client = DataStoreClient(base_url="http://example.test")
    df = client.get_index_ohlcv("Nifty 500", datetime(2026, 1, 1), datetime(2026, 7, 1))
    assert df.empty
    assert list(df.columns) == ["date", "index_name", "open", "high", "low", "close", "volume"]


class _SequenceDummyClient:
    """Returns a different queued response on each successive .get() call —
    for testing _get_with_retry's retry-then-succeed / retry-then-exhaust paths."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, url, params=None):
        response = self._responses[min(self.calls, len(self._responses) - 1)]
        self.calls += 1
        return response


def test_get_with_retry_succeeds_after_transient_503(monkeypatch):
    from datastore.client import _get_with_retry

    monkeypatch.setattr("datastore.client.time.sleep", lambda s: None)
    responses = [_DummyResponse({}, status_code=503), _DummyResponse({"ok": True}, status_code=200)]
    client = _SequenceDummyClient(responses)
    response = _get_with_retry(client, "http://example.test/x")
    assert response.status_code == 200
    assert client.calls == 2


def test_get_with_retry_gives_up_after_exhausting_attempts(monkeypatch):
    from datastore.client import _RETRY_ATTEMPTS, _get_with_retry

    monkeypatch.setattr("datastore.client.time.sleep", lambda s: None)
    client = _SequenceDummyClient([_DummyResponse({}, status_code=503)])
    response = _get_with_retry(client, "http://example.test/x")
    assert response.status_code == 503
    assert client.calls == _RETRY_ATTEMPTS


def test_get_with_retry_does_not_retry_non_503_errors(monkeypatch):
    from datastore.client import _get_with_retry

    monkeypatch.setattr("datastore.client.time.sleep", lambda s: None)
    client = _SequenceDummyClient([_DummyResponse({}, status_code=404)])
    response = _get_with_retry(client, "http://example.test/x")
    assert response.status_code == 404
    assert client.calls == 1
