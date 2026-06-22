"""
tests/unit/test_validator.py

Phase: 0.6 (Data Quality & Observability)
Specs: SPEC-PIPE-005, SPEC-SYS-003
Owner: Platform / Ingestion
Consumers: CI, pytest

Unit tests for ingestion/quality/validator.py (completeness gate, anomaly
detection) and ingestion/quality/drift_monitor.py (PSI calculation).
"""

import numpy as np
import pandas as pd
import pytest

from config.settings import MIN_STOCKS_FOR_INFERENCE, PSI_MODERATE_THRESHOLD, PSI_SEVERE_THRESHOLD
from ingestion.quality import validator
from ingestion.quality.drift_monitor import PSI_EPSILON, PSIMonitor


def _make_bhavcopy(n_stocks: int, anomaly_pct_change: float = 0.0) -> pd.DataFrame:
    """n_stocks rows of valid OHLC data; the first row carries anomaly_pct_change% close-vs-open move."""
    rows = []
    for i in range(n_stocks):
        open_price = 100.0
        close_price = open_price * (1 + anomaly_pct_change / 100) if i == 0 else open_price + 1.0
        rows.append(
            {
                "ticker": f"TICKER{i:04d}",
                "open": open_price,
                "high": max(open_price, close_price) + 1,
                "low": min(open_price, close_price) - 1,
                "close": close_price,
                "volume": 1000,
                "traded_qty": 1000,
                "delivery_qty": 500,
                "series": "EQ",
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# SPEC-SYS-003: completeness gate
# ---------------------------------------------------------------------------


def test_completeness_gate_blocks_at_449_stocks():
    """SPEC-SYS-003: stock_count < 450 must set ok=False, even with no missing/anomalies."""
    assert MIN_STOCKS_FOR_INFERENCE == 450
    n = MIN_STOCKS_FOR_INFERENCE - 1  # 449
    df = _make_bhavcopy(n)
    expected_tickers = df["ticker"].tolist()  # nothing "missing" by this measure

    result = validator.validate_bhavcopy(df, expected_tickers=expected_tickers)

    assert result["stock_count"] == 449
    assert result["missing"] == []
    assert result["anomalies"] == []
    assert result["ok"] is False


def test_completeness_gate_passes_at_450_stocks():
    """SPEC-SYS-003: exactly 450 stocks, no missing/anomalies, must pass."""
    n = MIN_STOCKS_FOR_INFERENCE  # 450
    df = _make_bhavcopy(n)
    expected_tickers = df["ticker"].tolist()

    result = validator.validate_bhavcopy(df, expected_tickers=expected_tickers)

    assert result["stock_count"] == 450
    assert result["ok"] is True


# ---------------------------------------------------------------------------
# SPEC-PIPE-005: anomaly detection
# ---------------------------------------------------------------------------


def test_anomaly_detection_flags_35_pct_price_change():
    """SPEC-PIPE-005: a 35% single-day close-vs-open move must be flagged as an anomaly."""
    n = MIN_STOCKS_FOR_INFERENCE
    df = _make_bhavcopy(n, anomaly_pct_change=35.0)
    expected_tickers = df["ticker"].tolist()

    result = validator.validate_bhavcopy(df, expected_tickers=expected_tickers)

    assert result["anomalies"] == ["TICKER0000"]
    assert result["ok"] is False


def test_no_anomaly_below_threshold():
    """SPEC-PIPE-005: a move at exactly the threshold or below must not be flagged."""
    n = MIN_STOCKS_FOR_INFERENCE
    df = _make_bhavcopy(n, anomaly_pct_change=29.0)
    expected_tickers = df["ticker"].tolist()

    result = validator.validate_bhavcopy(df, expected_tickers=expected_tickers)

    assert result["anomalies"] == []


# ---------------------------------------------------------------------------
# SPEC-PIPE-005: PSI calculation
# ---------------------------------------------------------------------------


def test_psi_known_distribution_shift_returns_expected_value():
    """
    SPEC-PIPE-005: a known, hand-computable distribution shift must produce
    the analytically expected PSI value.

    Setup: 4 explicit bins, each holding 25% of the baseline. current_values
    is entirely concentrated in the top bin. Expected PSI is computed
    independently here via the textbook formula
    PSI = sum((actual% - expected%) * ln(actual% / expected%)), using the
    same epsilon floor the implementation uses for zero-proportion bins,
    and compared against PSIMonitor.compute_psi()'s output.
    """
    bin_edges = np.array([-np.inf, 0.5, 1.5, 2.5, np.inf])
    baseline_values = np.array([0] * 5 + [1] * 5 + [2] * 5 + [3] * 5, dtype=float)  # 25% per bin
    current_values = np.array([3] * 20, dtype=float)  # 100% in the top bin

    expected_baseline_pct = np.array([0.25, 0.25, 0.25, 0.25])
    expected_current_pct = np.clip(np.array([0.0, 0.0, 0.0, 1.0]), PSI_EPSILON, None)
    expected_psi = float(
        np.sum(
            (expected_current_pct - expected_baseline_pct)
            * np.log(expected_current_pct / expected_baseline_pct)
        )
    )

    monitor = PSIMonitor()
    psi = monitor.compute_psi(
        "test_feature", current_values, baseline_values, bin_edges=bin_edges
    )

    assert psi == pytest.approx(expected_psi, rel=1e-9)
    assert psi > PSI_SEVERE_THRESHOLD  # a complete distribution shift must classify as 'halt'
    assert monitor.classify(psi) == "halt"


def test_psi_identical_distributions_is_near_zero():
    """SPEC-PIPE-005: current == baseline must yield PSI ~= 0 (no drift)."""
    rng = np.random.default_rng(42)
    values = rng.normal(loc=0.0, scale=1.0, size=2000)

    monitor = PSIMonitor()
    psi = monitor.compute_psi("test_feature", values, values)

    assert psi < PSI_MODERATE_THRESHOLD
    assert monitor.classify(psi) == "ok"


def test_psi_moderate_shift_classified_as_warning():
    """SPEC-ALERT-001: a moderate shift (0.10 < PSI <= 0.25) must classify as 'warning'."""
    monitor = PSIMonitor()
    assert monitor.classify(0.15) == "warning"
    assert monitor.classify(0.10) == "ok"  # boundary is exclusive (> not >=)
    assert monitor.classify(0.25) == "warning"  # boundary is exclusive on the halt side too
    assert monitor.classify(0.26) == "halt"


def test_psi_returns_zero_for_empty_current_array():
    """An empty current_values array (after dropping NaNs) must return 0.0, not raise."""
    monitor = PSIMonitor()
    psi = monitor.compute_psi("test_feature", current_values=[np.nan, np.nan], baseline_values=[1, 2, 3])
    assert psi == 0.0


def test_psi_returns_zero_for_empty_baseline_array():
    """An empty baseline_values array (after dropping NaNs) must return 0.0, not raise."""
    monitor = PSIMonitor()
    psi = monitor.compute_psi("test_feature", current_values=[1, 2, 3], baseline_values=[np.nan, np.nan])
    assert psi == 0.0
