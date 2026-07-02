from datetime import datetime
import json

import pandas as pd

from datastore.client import DataStoreClient


class _DummyResponse:
    def __init__(self, payload):
        self._payload = payload
        self.text = json.dumps(payload)

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
