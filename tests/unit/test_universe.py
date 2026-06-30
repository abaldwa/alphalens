"""
tests/unit/test_universe.py

Phase: 0
Specs: SPEC-SYS-001, SPEC-SYS-011, SPEC-DS-001
Owner: Platform / DataStore
Consumers: CI, pytest

Unit tests for config/universe.py.
"""

import pandas as pd
import pytest

import config.universe as universe_mod


def _write_csv(path, rows):
    """rows: list of dicts with universe_mod.REQUIRED_COLUMNS keys."""
    pd.DataFrame(rows).to_csv(path, index=False)


def _row(ticker, tier=1, market_cap_cr=1000, adtv_cr=10.0, is_nifty500=True):
    return {
        "ticker": ticker,
        "company_name": f"{ticker} Ltd.",
        "sector": "Test",
        "tier": tier,
        "market_cap_cr": market_cap_cr,
        "adtv_cr": adtv_cr,
        "is_fno_eligible": False,
        "is_nifty500": is_nifty500,
        "isin": f"INE{hash(ticker) % 1000000000:09d}",
    }


@pytest.fixture(autouse=True)
def _isolated_filters(monkeypatch):
    """Pin the filter thresholds so tests don't depend on the active UNIVERSE_PROFILE."""
    monkeypatch.setattr(universe_mod, "TIER_THRESHOLD", 2)
    monkeypatch.setattr(universe_mod, "MIN_ADTV_CR", 5.0)
    monkeypatch.setattr(universe_mod, "MIN_MCAP_CR", 500)


# ===== load_universe_raw =====


def test_load_universe_raw_raises_file_not_found_when_csv_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(universe_mod, "UNIVERSE_CSV_PATH", tmp_path / "does-not-exist.csv")

    with pytest.raises(FileNotFoundError, match="Universe CSV not found"):
        universe_mod.load_universe_raw()


def test_load_universe_raw_raises_value_error_on_missing_columns(monkeypatch, tmp_path):
    csv_path = tmp_path / "universe.csv"
    pd.DataFrame([{"ticker": "AAA"}]).to_csv(csv_path, index=False)
    monkeypatch.setattr(universe_mod, "UNIVERSE_CSV_PATH", csv_path)

    with pytest.raises(ValueError, match="missing required columns"):
        universe_mod.load_universe_raw()


def test_load_universe_raw_returns_all_rows_unfiltered(monkeypatch, tmp_path):
    csv_path = tmp_path / "universe.csv"
    _write_csv(csv_path, [_row("AAA", tier=1), _row("BBB", tier=5)])
    monkeypatch.setattr(universe_mod, "UNIVERSE_CSV_PATH", csv_path)

    df = universe_mod.load_universe_raw()

    assert len(df) == 2
    assert set(df["ticker"]) == {"AAA", "BBB"}


# ===== load_universe: tier filter =====


def test_load_universe_filters_by_tier_threshold(monkeypatch, tmp_path):
    csv_path = tmp_path / "universe.csv"
    _write_csv(csv_path, [_row("AAA", tier=1), _row("BBB", tier=2), _row("CCC", tier=3)])
    monkeypatch.setattr(universe_mod, "UNIVERSE_CSV_PATH", csv_path)

    df = universe_mod.load_universe()

    assert set(df["ticker"]) == {"AAA", "BBB"}  # tier<=2 only


# ===== load_universe: adtv/mcap known-vs-unsourced relaxation =====


def test_load_universe_excludes_known_insufficient_adtv(monkeypatch, tmp_path):
    csv_path = tmp_path / "universe.csv"
    _write_csv(csv_path, [_row("AAA", adtv_cr=10.0), _row("BBB", adtv_cr=1.0)])  # BBB < MIN_ADTV_CR=5.0
    monkeypatch.setattr(universe_mod, "UNIVERSE_CSV_PATH", csv_path)

    df = universe_mod.load_universe()

    assert set(df["ticker"]) == {"AAA"}


def test_load_universe_excludes_known_insufficient_mcap(monkeypatch, tmp_path):
    csv_path = tmp_path / "universe.csv"
    _write_csv(csv_path, [_row("AAA", market_cap_cr=1000), _row("BBB", market_cap_cr=100)])  # BBB < 500
    monkeypatch.setattr(universe_mod, "UNIVERSE_CSV_PATH", csv_path)

    df = universe_mod.load_universe()

    assert set(df["ticker"]) == {"AAA"}


def test_load_universe_treats_zero_adtv_as_unsourced_not_excluded(monkeypatch, tmp_path):
    """adtv_cr == 0 means 'not yet backfilled', not 'below threshold' -- must pass, not be excluded."""
    csv_path = tmp_path / "universe.csv"
    _write_csv(csv_path, [_row("AAA", adtv_cr=0.0)])
    monkeypatch.setattr(universe_mod, "UNIVERSE_CSV_PATH", csv_path)

    df = universe_mod.load_universe()

    assert set(df["ticker"]) == {"AAA"}


def test_load_universe_treats_zero_mcap_as_unsourced_not_excluded(monkeypatch, tmp_path):
    """market_cap_cr == 0 means 'not yet sourced', not 'below threshold' -- must pass, not be excluded."""
    csv_path = tmp_path / "universe.csv"
    _write_csv(csv_path, [_row("AAA", market_cap_cr=0)])
    monkeypatch.setattr(universe_mod, "UNIVERSE_CSV_PATH", csv_path)

    df = universe_mod.load_universe()

    assert set(df["ticker"]) == {"AAA"}


def test_load_universe_combines_all_three_filters(monkeypatch, tmp_path):
    csv_path = tmp_path / "universe.csv"
    _write_csv(
        csv_path,
        [
            _row("PASS", tier=2, market_cap_cr=1000, adtv_cr=10.0),
            _row("BAD_TIER", tier=3, market_cap_cr=1000, adtv_cr=10.0),
            _row("BAD_MCAP", tier=2, market_cap_cr=100, adtv_cr=10.0),
            _row("BAD_ADTV", tier=2, market_cap_cr=1000, adtv_cr=1.0),
            _row("UNSOURCED", tier=2, market_cap_cr=0, adtv_cr=0.0),
        ],
    )
    monkeypatch.setattr(universe_mod, "UNIVERSE_CSV_PATH", csv_path)

    df = universe_mod.load_universe()

    assert set(df["ticker"]) == {"PASS", "UNSOURCED"}


# ===== get_tickers =====


def test_get_tickers_returns_flat_list(monkeypatch, tmp_path):
    csv_path = tmp_path / "universe.csv"
    _write_csv(csv_path, [_row("AAA", tier=1), _row("BBB", tier=2), _row("CCC", tier=5)])
    monkeypatch.setattr(universe_mod, "UNIVERSE_CSV_PATH", csv_path)

    tickers = universe_mod.get_tickers()

    assert tickers == ["AAA", "BBB"]


def test_get_tickers_empty_when_nothing_passes(monkeypatch, tmp_path):
    csv_path = tmp_path / "universe.csv"
    _write_csv(csv_path, [_row("AAA", tier=5)])
    monkeypatch.setattr(universe_mod, "UNIVERSE_CSV_PATH", csv_path)

    assert universe_mod.get_tickers() == []
