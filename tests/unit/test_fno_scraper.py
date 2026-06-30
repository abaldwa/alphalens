"""
tests/unit/test_fno_scraper.py

Phase: 2.3 (F&O Features + Signal63D + Full Phase 2 Feature Matrix)
Specs: SPEC-PIPE-001
Owner: Platform / QA
Consumers: CI, pytest

Unit tests for ingestion/scrapers/fno.py's UDiFF column-mapping logic
(the real bug fix this phase made — the pre-existing
archives.nseindia.com/content/historical/DERIVATIVES/ URL 404s against
NSE's current archive; see module docstring and BuildLog.md "P2.3"). The
NSE HTTP fetch is mocked at `_fetch_fno_bhavcopy_csv` — these tests
exercise the column-mapping/parsing logic only, never a real network call.
"""

import pandas as pd
import pytest

from ingestion.scrapers import fno


def _make_raw_udiff(rows):
    """Build a synthetic raw UDiFF DataFrame with the exact real column
    names verified live against NSE's BhavCopy_NSE_FO_*.csv (2026-06-22)."""
    return pd.DataFrame(rows)


def _stf_row(ticker, expiry, settle, underlying, oi=1000, oi_change=50, volume=200, close=None):
    return {
        "TckrSymb": ticker, "FinInstrmTp": "STF", "XpryDt": expiry,
        "StrkPric": None, "OptnTp": None, "OpnIntrst": oi, "ChngInOpnIntrst": oi_change,
        "TtlTradgVol": volume, "SttlmPric": settle, "ClsPric": close if close is not None else settle,
        "UndrlygPric": underlying,
    }


def _sto_row(ticker, expiry, strike, option_type, settle, underlying, oi=500, oi_change=10, volume=50):
    return {
        "TckrSymb": ticker, "FinInstrmTp": "STO", "XpryDt": expiry,
        "StrkPric": strike, "OptnTp": option_type, "OpnIntrst": oi, "ChngInOpnIntrst": oi_change,
        "TtlTradgVol": volume, "SttlmPric": settle, "ClsPric": settle, "UndrlygPric": underlying,
    }


class TestDownloadFnoBhavcopy:
    def test_parses_futures_row_with_null_strike_and_option_type(self, monkeypatch):
        raw = _make_raw_udiff([_stf_row("RELIANCE", "2026-06-30", 1303.0, 1300.0)])
        monkeypatch.setattr(fno, "_fetch_fno_bhavcopy_csv", lambda trade_date: raw)
        monkeypatch.setattr(fno, "_save_raw", lambda trade_date, raw_df: None)

        df = fno.download_fno_bhavcopy("2026-06-22")

        assert len(df) == 1
        row = df.iloc[0]
        assert row["ticker"] == "RELIANCE"
        assert row["instrument"] == "STF"
        assert pd.isna(row["strike"])
        assert pd.isna(row["option_type"])
        assert row["settle_price"] == 1303.0
        assert row["underlying_price"] == 1300.0

    def test_parses_option_row_with_real_strike_and_type(self, monkeypatch):
        raw = _make_raw_udiff([_sto_row("RELIANCE", "2026-06-30", 1300.0, "CE", 25.5, 1300.0)])
        monkeypatch.setattr(fno, "_fetch_fno_bhavcopy_csv", lambda trade_date: raw)
        monkeypatch.setattr(fno, "_save_raw", lambda trade_date, raw_df: None)

        df = fno.download_fno_bhavcopy("2026-06-22")

        row = df.iloc[0]
        assert row["strike"] == 1300.0
        assert row["option_type"] == "CE"
        assert row["settle_price"] == 25.5

    def test_oi_change_and_underlying_price_columns_present(self, monkeypatch):
        """The two real columns this phase's fix newly captures (not in the old, broken schema)."""
        raw = _make_raw_udiff([_stf_row("TCS", "2026-06-30", 3500.0, 3490.0, oi_change=-120)])
        monkeypatch.setattr(fno, "_fetch_fno_bhavcopy_csv", lambda trade_date: raw)
        monkeypatch.setattr(fno, "_save_raw", lambda trade_date, raw_df: None)

        df = fno.download_fno_bhavcopy("2026-06-22")

        assert df.iloc[0]["oi_change"] == -120
        assert df.iloc[0]["underlying_price"] == 3490.0

    def test_multiple_instrument_types_all_parsed(self, monkeypatch):
        raw = _make_raw_udiff(
            [
                _stf_row("INFY", "2026-06-30", 1500.0, 1498.0),
                _sto_row("INFY", "2026-06-30", 1500.0, "CE", 30.0, 1498.0),
                _sto_row("INFY", "2026-06-30", 1500.0, "PE", 28.0, 1498.0),
            ]
        )
        monkeypatch.setattr(fno, "_fetch_fno_bhavcopy_csv", lambda trade_date: raw)
        monkeypatch.setattr(fno, "_save_raw", lambda trade_date, raw_df: None)

        df = fno.download_fno_bhavcopy("2026-06-22")

        assert len(df) == 3
        assert set(df["instrument"]) == {"STF", "STO"}
        assert set(df.columns) == set(fno.REQUIRED_COLUMNS)

    def test_fetch_failure_propagates_not_swallowed(self, monkeypatch):
        def always_fail(trade_date):
            raise ConnectionError("simulated persistent NSE archive failure")

        monkeypatch.setattr(fno, "_fetch_fno_bhavcopy_csv", always_fail)

        with pytest.raises(ConnectionError):
            fno.download_fno_bhavcopy("2026-06-22")
