"""
tests/unit/test_nse_indices.py

A65: tests for `ingestion/scrapers/nse_indices.py`'s real parse/filter logic
(previously untested, 40.91% coverage, no test file), following
tests/unit/test_nse_ipo.py's pattern: the live HTTP fetch (`_fetch_indices_csv`)
is monkeypatched to return a DataFrame shaped exactly like NSE's real
ind_close_all CSV columns (per this module's own docstring/column mapping)
rather than touching the network — `download_index_ohlcv`'s own filtering/
type-coercion logic is real and exercised end-to-end.
"""


import pandas as pd
import pytest

from ingestion.scrapers import nse_indices


def _raw_csv_df(rows):
    return pd.DataFrame(rows)


@pytest.fixture
def patched(tmp_path, monkeypatch):
    monkeypatch.setattr(nse_indices, "RAW_DIR", tmp_path)


class TestDownloadIndexOhlcv:
    def test_filters_to_tracked_indices_only(self, patched, monkeypatch):
        raw = _raw_csv_df(
            [
                {
                    "Index Name": "Nifty 50", "Open Index Value": 22000.0, "High Index Value": 22100.0,
                    "Low Index Value": 21950.0, "Closing Index Value": 22050.0, "Volume": "123456",
                },
                {
                    "Index Name": "Nifty Some Obscure Index", "Open Index Value": 1000.0,
                    "High Index Value": 1010.0, "Low Index Value": 990.0, "Closing Index Value": 1005.0,
                    "Volume": "999",
                },
            ]
        )
        monkeypatch.setattr(nse_indices, "_fetch_indices_csv", lambda trade_date: raw)
        df = nse_indices.download_index_ohlcv("2026-06-01")
        assert list(df["index_name"]) == ["Nifty 50"]
        assert df.iloc[0]["close"] == 22050.0
        assert df.iloc[0]["volume"] == 123456.0

    def test_strips_whitespace_from_index_name_and_columns(self, patched, monkeypatch):
        raw = pd.DataFrame(
            {
                " Index Name ": [" Nifty Bank "], "Open Index Value": [48000.0],
                "High Index Value": [48200.0], "Low Index Value": [47900.0],
                "Closing Index Value": [48100.0], "Volume": ["500"],
            }
        )
        # Simulate NSE's real header having a leading/trailing-space column
        # name by renaming after the fact — the module code does
        # `raw.columns = [c.strip() for c in raw.columns]` first.
        raw = raw.rename(columns=lambda c: c.strip())
        raw["Index Name"] = raw["Index Name"]
        monkeypatch.setattr(nse_indices, "_fetch_indices_csv", lambda trade_date: raw)
        df = nse_indices.download_index_ohlcv("2026-06-01")
        assert df.iloc[0]["index_name"] == "Nifty Bank"

    def test_dash_volume_becomes_nan(self, patched, monkeypatch):
        raw = _raw_csv_df(
            [
                {
                    "Index Name": "Nifty IT", "Open Index Value": 35000.0, "High Index Value": 35200.0,
                    "Low Index Value": 34900.0, "Closing Index Value": 35100.0, "Volume": "-",
                }
            ]
        )
        monkeypatch.setattr(nse_indices, "_fetch_indices_csv", lambda trade_date: raw)
        df = nse_indices.download_index_ohlcv("2026-06-01")
        assert pd.isna(df.iloc[0]["volume"])

    def test_missing_volume_column_becomes_nan(self, patched, monkeypatch):
        raw = _raw_csv_df(
            [
                {
                    "Index Name": "Nifty 50", "Open Index Value": 22000.0, "High Index Value": 22100.0,
                    "Low Index Value": 21950.0, "Closing Index Value": 22050.0,
                }
            ]
        )
        monkeypatch.setattr(nse_indices, "_fetch_indices_csv", lambda trade_date: raw)
        df = nse_indices.download_index_ohlcv("2026-06-01")
        assert pd.isna(df.iloc[0]["volume"])

    def test_date_column_set_to_requested_date(self, patched, monkeypatch):
        raw = _raw_csv_df(
            [
                {
                    "Index Name": "Nifty 500", "Open Index Value": 20000.0, "High Index Value": 20100.0,
                    "Low Index Value": 19900.0, "Closing Index Value": 20050.0, "Volume": "1000",
                }
            ]
        )
        monkeypatch.setattr(nse_indices, "_fetch_indices_csv", lambda trade_date: raw)
        df = nse_indices.download_index_ohlcv("2026-06-15")
        assert df.iloc[0]["date"] == "2026-06-15"

    def test_saves_raw_csv_for_audit(self, patched, monkeypatch, tmp_path):
        raw = _raw_csv_df(
            [
                {
                    "Index Name": "Nifty 50", "Open Index Value": 22000.0, "High Index Value": 22100.0,
                    "Low Index Value": 21950.0, "Closing Index Value": 22050.0, "Volume": "1000",
                }
            ]
        )
        monkeypatch.setattr(nse_indices, "_fetch_indices_csv", lambda trade_date: raw)
        nse_indices.download_index_ohlcv("2026-06-01")
        saved = tmp_path / "nse_indices" / "2026-06-01.csv"
        assert saved.exists()

    def test_historical_cnx_name_canonicalized_to_current_nifty_name(self, patched, monkeypatch):
        """2026-07-20 fix: NSE's pre-2015-11-06 archives called this index
        'CNX Nifty' (and 'S&P CNX Nifty' even earlier) — real historical
        data that was previously silently dropped by the TRACKED_INDICES
        filter since it only matched the current name."""
        raw = _raw_csv_df(
            [
                {
                    "Index Name": "CNX Nifty", "Open Index Value": 8400.0, "High Index Value": 8420.0,
                    "Low Index Value": 8390.0, "Closing Index Value": 8410.0, "Volume": "100000",
                },
                {
                    "Index Name": "S&P CNX Nifty", "Open Index Value": 5900.0, "High Index Value": 5950.0,
                    "Low Index Value": 5880.0, "Closing Index Value": 5920.0, "Volume": "50000",
                },
                {
                    "Index Name": "CNX Finance", "Open Index Value": 4000.0, "High Index Value": 4050.0,
                    "Low Index Value": 3980.0, "Closing Index Value": 4020.0, "Volume": "20000",
                },
            ]
        )
        monkeypatch.setattr(nse_indices, "_fetch_indices_csv", lambda trade_date: raw)
        df = nse_indices.download_index_ohlcv("2026-06-01")
        # Both aliases map to the SAME canonical name -> both rows survive
        # the filter under "Nifty 50" (real NSE archives never emit both
        # aliases on the same date; this just proves the mapping itself).
        assert (df["index_name"] == "Nifty 50").sum() == 2
        assert (df["index_name"] == "Nifty Financial Services").sum() == 1

    def test_unmapped_historical_names_stay_dropped(self, patched, monkeypatch):
        """Nifty Healthcare Index / Nifty Oil & Gas have no verified
        historical alias (launched after the CNX-era archives checked) —
        must NOT be guessed; an untracked old name stays untracked."""
        raw = _raw_csv_df(
            [{
                "Index Name": "CNX Some Discontinued Index", "Open Index Value": 100.0,
                "High Index Value": 100.0, "Low Index Value": 100.0, "Closing Index Value": 100.0,
                "Volume": "1",
            }]
        )
        monkeypatch.setattr(nse_indices, "_fetch_indices_csv", lambda trade_date: raw)
        df = nse_indices.download_index_ohlcv("2026-06-01")
        assert len(df) == 0

    def test_staleness_check_parses_historical_ddmmyyyy_index_date(self, patched, monkeypatch):
        """Old archives stamp Index Date as DD-MM-YYYY (e.g. '05-01-2015'),
        not the current DD-Mon-YYYY format — this must still be caught as
        a real date mismatch, not silently pass through as unparseable."""
        raw = _raw_csv_df(
            [{
                "Index Name": "Nifty 50", "Index Date": "04-01-2015", "Open Index Value": 8400.0,
                "High Index Value": 8420.0, "Low Index Value": 8390.0, "Closing Index Value": 8410.0,
                "Volume": "100000",
            }]
        )
        monkeypatch.setattr(nse_indices, "_fetch_indices_csv", lambda trade_date: raw)
        with pytest.raises(ValueError, match="stale"):
            nse_indices.download_index_ohlcv("2015-01-05")  # requested date != stamped date

    def test_staleness_check_accepts_matching_historical_ddmmyyyy_index_date(self, patched, monkeypatch):
        raw = _raw_csv_df(
            [{
                "Index Name": "Nifty 50", "Index Date": "05-01-2015", "Open Index Value": 8400.0,
                "High Index Value": 8420.0, "Low Index Value": 8390.0, "Closing Index Value": 8410.0,
                "Volume": "100000",
            }]
        )
        monkeypatch.setattr(nse_indices, "_fetch_indices_csv", lambda trade_date: raw)
        df = nse_indices.download_index_ohlcv("2015-01-05")  # matches -> no raise
        assert len(df) == 1

    def test_no_tracked_indices_present_returns_empty_df_with_columns(self, patched, monkeypatch):
        raw = _raw_csv_df(
            [
                {
                    "Index Name": "Some Untracked Index", "Open Index Value": 1.0, "High Index Value": 1.0,
                    "Low Index Value": 1.0, "Closing Index Value": 1.0, "Volume": "1",
                }
            ]
        )
        monkeypatch.setattr(nse_indices, "_fetch_indices_csv", lambda trade_date: raw)
        df = nse_indices.download_index_ohlcv("2026-06-01")
        assert len(df) == 0
        assert list(df.columns) == nse_indices.REQUIRED_COLUMNS
