"""
Signal correctness tests — TrailingMomentumSignal, PctOf52WeekHighSignal,
BollingerBandSignal, IndustryMomentumSignal, against hand-crafted
in-memory data where the correct answer is known exactly.
"""

import pandas as pd
import pytest

from momentum_framework.common.signals import (
    IndustryMomentumSignal,
    PctOf52WeekHighSignal,
    TrailingMomentumSignal,
)
from momentum_framework.common.bollinger_signal import BollingerBandSignal
from momentum_framework.tests.conftest import seed_ohlcv


def _trading_days(n, start="2024-01-01"):
    return [str(d.date()) for d in pd.bdate_range(start, periods=n)]


def test_trailing_momentum_known_return(memory_conn):
    dates = _trading_days(64)  # comfortably more than 63 (3mo) trading days
    prices = [100.0] * len(dates)
    prices[-1] = 120.0  # ticker A: +20% over the window
    seed_ohlcv(memory_conn, "A", list(zip(dates, prices)))

    flat_prices = [100.0] * len(dates)
    seed_ohlcv(memory_conn, "B", list(zip(dates, flat_prices)))

    signal = TrailingMomentumSignal(lookback_months=3)
    scores = signal.compute(memory_conn, ["A", "B"], dates[-1], signal.lookback_days)

    assert scores["A"] == pytest.approx(0.20, abs=1e-6)
    assert scores["B"] == pytest.approx(0.0, abs=1e-6)


def test_trailing_momentum_insufficient_history_excluded(memory_conn):
    """A ticker with fewer than lookback_days+1 closes must be dropped,
    not given a fabricated partial-window return."""
    dates = _trading_days(10)  # far short of a 3-month (63d) window
    seed_ohlcv(memory_conn, "SHORT", list(zip(dates, [100.0] * 10)))

    signal = TrailingMomentumSignal(lookback_months=3)
    scores = signal.compute(memory_conn, ["SHORT"], dates[-1], signal.lookback_days)
    assert "SHORT" not in scores.index


def test_pct_of_52wk_high_at_the_high(memory_conn):
    dates = _trading_days(260)
    prices = [100.0 + i * 0.1 for i in range(260)]  # steadily rising, today IS the high
    seed_ohlcv(memory_conn, "HIGH", list(zip(dates, prices)))

    signal = PctOf52WeekHighSignal()
    scores = signal.compute(memory_conn, ["HIGH"], dates[-1], 252)
    assert scores["HIGH"] == pytest.approx(1.0, abs=1e-6)


def test_pct_of_52wk_high_well_off_the_high(memory_conn):
    dates = _trading_days(260)
    prices = [200.0] * 100 + [100.0] * 160  # peaked at 200, now at 100 (half the high)
    seed_ohlcv(memory_conn, "DROPPED", list(zip(dates, prices)))

    signal = PctOf52WeekHighSignal()
    scores = signal.compute(memory_conn, ["DROPPED"], dates[-1], 252)
    assert scores["DROPPED"] == pytest.approx(0.5, abs=1e-6)


def test_bollinger_oversold_near_lower_band(memory_conn):
    dates = _trading_days(25)
    prices = [100.0] * 20 + [95, 90, 85, 80, 75]  # sharp recent drop
    seed_ohlcv(memory_conn, "OVERSOLD", list(zip(dates, prices)))

    signal = BollingerBandSignal(window=20)
    scores = signal.compute(memory_conn, ["OVERSOLD"], dates[-1])
    assert scores["OVERSOLD"] < 0.3, "A sharp recent drop should read as oversold (%B near 0)"


def test_bollinger_flat_price_is_neutral(memory_conn):
    dates = _trading_days(25)
    prices = [100.0] * 25  # perfectly flat -> zero band width
    seed_ohlcv(memory_conn, "FLAT", list(zip(dates, prices)))

    signal = BollingerBandSignal(window=20)
    scores = signal.compute(memory_conn, ["FLAT"], dates[-1])
    # band_range is clamped to a small epsilon, not zero -- must not raise/NaN
    assert 0.0 <= scores["FLAT"] <= 1.0


def test_industry_momentum_restricts_to_top_sector(memory_conn):
    dates = _trading_days(64)
    # Tech: strong momentum. Energy: weak momentum.
    for ticker, end_price in [("TECH1", 130), ("TECH2", 125), ("ENERGY1", 102), ("ENERGY2", 101)]:
        prices = [100.0] * (len(dates) - 1) + [float(end_price)]
        seed_ohlcv(memory_conn, ticker, list(zip(dates, prices)))

    memory_conn.executemany(
        "INSERT INTO stock_master (ticker, company_name, sector, industry) VALUES (?, ?, ?, NULL)",
        [("TECH1", "T1", "Tech"), ("TECH2", "T2", "Tech"),
         ("ENERGY1", "E1", "Energy"), ("ENERGY2", "E2", "Energy")],
    )

    signal = IndustryMomentumSignal(lookback_months=3, top_sectors=1)
    scores = signal.compute(memory_conn, ["TECH1", "TECH2", "ENERGY1", "ENERGY2"], dates[-1])

    assert set(scores.index) == {"TECH1", "TECH2"}, "Only the top (Tech) sector's constituents should survive"
