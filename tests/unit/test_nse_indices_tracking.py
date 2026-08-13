"""
Regression tests for daily index capture (A97).

The bug these exist to prevent: TRACKED_INDICES is matched EXACTLY against
NSE's "Index Name" column, and NSE is not internally consistent about
capitalisation -- the same ind_close_all file publishes "NIFTY Midcap 100"
next to "Nifty Midcap 150". An exact match therefore drops an index silently,
every day, while the pipeline reports success. That is not hypothetical: it is
what left Nifty 100 with zero rows until 2026-08-09, which in turn left
rs_vs_nifty100_21d falling back to an ETF proxy that only lists from 2015.

These tests run against a real archived ind_close_all file when one is
present, so they check the actual published names rather than names this
project imagined.
"""

import pandas as pd
import pytest

from config.settings import RAW_DIR
from ingestion.scrapers.nse_indices import (
    TRACKED_INDICES,
    _normalise_index_name,
    download_index_ohlcv,
)

SIZE_INDICES = [
    "Nifty Next 50",
    "Nifty Midcap 50",
    "Nifty Midcap 100",
    "Nifty Midcap 150",
    "Nifty Smallcap 50",
    "Nifty Smallcap 100",
    "Nifty Smallcap 250",
    "Nifty Microcap 250",
]


def _patch(monkeypatch, raw):
    """Serve the archived file instead of hitting NSE, and skip the raw-save."""
    import ingestion.scrapers.nse_indices as ni

    monkeypatch.setattr(ni, "_fetch_indices_csv", lambda *_: raw.copy())
    monkeypatch.setattr(ni, "_save_raw", lambda *_: None)


def _latest_archive():
    raw_dir = RAW_DIR / "nse_indices"
    if not raw_dir.exists():
        return None
    files = sorted(raw_dir.glob("*.csv"))
    return files[-1] if files else None


class TestNormalisation:
    def test_casing_difference_normalises_to_the_same_key(self):
        assert _normalise_index_name("NIFTY Midcap 100") == _normalise_index_name(
            "Nifty Midcap 100"
        )

    def test_extra_whitespace_normalises(self):
        assert _normalise_index_name("Nifty  Midcap   150") == _normalise_index_name(
            "Nifty Midcap 150"
        )

    def test_distinct_indices_do_not_collide(self):
        """Normalisation must not merge genuinely different indices."""
        keys = {_normalise_index_name(n) for n in TRACKED_INDICES}
        assert len(keys) == len(TRACKED_INDICES)


class TestTrackedList:
    def test_all_size_indices_tracked(self):
        missing = set(SIZE_INDICES) - set(TRACKED_INDICES)
        assert not missing, f"not captured daily: {sorted(missing)}"

    def test_no_duplicates(self):
        assert len(TRACKED_INDICES) == len(set(TRACKED_INDICES))


class TestAgainstRealArchive:
    """Against a real ind_close_all file, not a fixture of our own naming."""

    @pytest.fixture
    def archive(self):
        path = _latest_archive()
        if path is None:
            pytest.skip("no archived ind_close_all file available")
        # The filename is the trading date. Do NOT re-derive it from the
        # "Index Date" column: NSE stamps it "10-Aug-2026", and a naive parse
        # reads that as October 8th, which then trips the scraper's own
        # stale-file guard.
        return pd.read_csv(path), path.stem

    def test_every_tracked_index_is_captured(self, archive, monkeypatch):
        """The end-to-end assertion: nothing in TRACKED_INDICES is silently
        dropped by the name filter."""
        raw, date_str = archive
        _patch(monkeypatch, raw)
        df = download_index_ohlcv(date_str)

        captured = set(df["index_name"])
        missing = set(TRACKED_INDICES) - captured
        assert not missing, f"dropped by the name filter: {sorted(missing)}"

    def test_mixed_case_indices_survive_the_filter(self, archive, monkeypatch):
        """NIFTY Midcap 100 / NIFTY Smallcap 100 are the two NSE publishes
        with different casing from their siblings."""
        raw, date_str = archive
        _patch(monkeypatch, raw)
        captured = set(download_index_ohlcv(date_str)["index_name"])
        assert {"Nifty Midcap 100", "Nifty Smallcap 100"} <= captured

    def test_stored_under_the_canonical_name(self, archive, monkeypatch):
        """Capturing the row under NSE's spelling rather than the canonical
        one would be the same outage wearing a different hat -- the row lands,
        but under a name no consumer queries."""
        raw, date_str = archive
        _patch(monkeypatch, raw)
        captured = set(download_index_ohlcv(date_str)["index_name"])
        assert captured <= set(TRACKED_INDICES)
        assert "NIFTY Midcap 100" not in captured

    def test_untracked_indices_still_excluded(self, archive, monkeypatch):
        """The filter must still filter -- NSE publishes ~100 indices."""
        raw, date_str = archive
        _patch(monkeypatch, raw)
        df = download_index_ohlcv(date_str)
        assert len(df) == len(TRACKED_INDICES)
        assert len(df) < len(raw)
