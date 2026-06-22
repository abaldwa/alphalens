"""
tests/unit/test_bhavcopy.py

Phase: 0.4 (Data Ingestion Scrapers)
Specs: SPEC-PIPE-001, SPEC-PIPE-005
Owner: Platform / Ingestion
Consumers: CI, pytest

Unit tests for ingestion/scrapers/bhavcopy.py. The NSE HTTP fetch is
mocked at `_fetch_bhavcopy_csv` — these tests exercise the parsing and
validation logic only, never real network calls.
"""

import pandas as pd
import pytest

from ingestion.scrapers import bhavcopy
from config.settings import MIN_STOCKS_FOR_INFERENCE


def _make_raw_bhavcopy(n_eq: int = 460, delivery_pct: float = 50.0) -> pd.DataFrame:
    """Build a synthetic raw NSE sec_bhavdata_full DataFrame with n_eq EQ rows."""
    traded_qty = 100_000
    delivery_qty = int(traded_qty * delivery_pct / 100)

    rows = []
    for i in range(n_eq):
        rows.append(
            {
                "SYMBOL": f"TICKER{i:04d}",
                " SERIES": "EQ",
                "OPEN_PRICE": 100.0 + i,
                "HIGH_PRICE": 105.0 + i,
                "LOW_PRICE": 95.0 + i,
                "CLOSE_PRICE": 102.0 + i,
                "TTL_TRD_QNTY": traded_qty,
                "DELIV_QTY": delivery_qty,
                "DELIV_PER": delivery_pct,
            }
        )
    # A couple of non-EQ rows that must be filtered out entirely.
    rows.append(
        {
            "SYMBOL": "SOMEBOND", " SERIES": "BE", "OPEN_PRICE": 50, "HIGH_PRICE": 51,
            "LOW_PRICE": 49, "CLOSE_PRICE": 50, "TTL_TRD_QNTY": 1000, "DELIV_QTY": 500,
            "DELIV_PER": 50.0,
        }
    )
    return pd.DataFrame(rows)


def test_download_returns_dataframe_with_required_columns(monkeypatch):
    """SPEC-PIPE-001: download_bhavcopy must return exactly REQUIRED_COLUMNS, EQ-only."""
    raw = _make_raw_bhavcopy(n_eq=460)
    monkeypatch.setattr(bhavcopy, "_fetch_bhavcopy_csv", lambda trade_date: raw)
    monkeypatch.setattr(bhavcopy, "_save_raw", lambda trade_date, raw_df: None)

    df = bhavcopy.download_bhavcopy("2026-01-15")

    assert list(df.columns) == bhavcopy.REQUIRED_COLUMNS
    assert len(df) == 460  # the BE row must have been filtered out
    assert set(df["series"]) == {"EQ"}


def test_raises_value_error_when_fewer_than_450_stocks(monkeypatch):
    """SPEC-PIPE-005: completeness gate — fewer than 450 EQ stocks must raise ValueError."""
    raw = _make_raw_bhavcopy(n_eq=MIN_STOCKS_FOR_INFERENCE - 1)
    monkeypatch.setattr(bhavcopy, "_fetch_bhavcopy_csv", lambda trade_date: raw)
    monkeypatch.setattr(bhavcopy, "_save_raw", lambda trade_date, raw_df: None)

    with pytest.raises(ValueError, match="minimum required"):
        bhavcopy.download_bhavcopy("2026-01-15")


def test_delivery_pct_validation_catches_out_of_range_values(monkeypatch):
    """SPEC-PIPE-005: delivery_pct must be in [0, 100]; a >100% row must raise ValueError."""
    raw = _make_raw_bhavcopy(n_eq=460)
    # Corrupt one row's delivery_qty so delivery_pct = 100_000 / 1_000 * 100 = way over 100%.
    raw.loc[0, "TTL_TRD_QNTY"] = 1_000
    raw.loc[0, "DELIV_QTY"] = 100_000

    monkeypatch.setattr(bhavcopy, "_fetch_bhavcopy_csv", lambda trade_date: raw)
    monkeypatch.setattr(bhavcopy, "_save_raw", lambda trade_date, raw_df: None)

    with pytest.raises(ValueError, match="delivery_pct"):
        bhavcopy.download_bhavcopy("2026-01-15")


def test_connection_error_after_three_retries(monkeypatch):
    """SPEC-PIPE-001: a session that always fails must raise ConnectionError after exactly MAX_RETRIES attempts."""
    import requests

    attempts = {"count": 0}

    class _AlwaysFailingSession:
        def get(self, *args, **kwargs):
            attempts["count"] += 1
            raise requests.RequestException("simulated network failure")

    monkeypatch.setattr(bhavcopy, "RETRY_DELAY_SECONDS", 0)
    monkeypatch.setattr(bhavcopy, "_nse_session", lambda: _AlwaysFailingSession())

    with pytest.raises(ConnectionError, match="after 3 attempts"):
        bhavcopy.download_bhavcopy("2026-01-15")

    assert attempts["count"] == bhavcopy.MAX_RETRIES == 3


def test_duplicate_tickers_raise_value_error(monkeypatch):
    """SPEC-PIPE-001: a ticker appearing twice in EQ series must raise ValueError."""
    raw = _make_raw_bhavcopy(n_eq=460)
    raw.loc[1, "SYMBOL"] = raw.loc[0, "SYMBOL"]  # introduce a duplicate

    monkeypatch.setattr(bhavcopy, "_fetch_bhavcopy_csv", lambda trade_date: raw)
    monkeypatch.setattr(bhavcopy, "_save_raw", lambda trade_date, raw_df: None)

    with pytest.raises(ValueError, match="Duplicate tickers"):
        bhavcopy.download_bhavcopy("2026-01-15")


def test_validate_bhavcopy_flags_missing_tickers():
    """validate_bhavcopy must report tickers present in the universe but absent from the bhavcopy."""
    df = pd.DataFrame(
        {
            "ticker": ["AAA", "BBB"],
            "open": [100.0, 200.0],
            "high": [101.0, 201.0],
            "low": [99.0, 199.0],
            "close": [100.5, 200.5],
            "volume": [1000, 2000],
            "traded_qty": [1000, 2000],
            "delivery_qty": [500, 1000],
            "series": ["EQ", "EQ"],
        }
    )

    result = bhavcopy.validate_bhavcopy(df, expected_tickers=["AAA", "BBB", "CCC"])

    assert result["ok"] is False
    assert result["missing"] == ["CCC"]
    assert result["anomalies"] == []
