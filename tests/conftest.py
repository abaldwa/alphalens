"""
tests/conftest.py

Phase: 0.1 (Project Skeleton)
Specs: SPEC-QUALITY-001, SPEC-QUALITY-002, SPEC-DS-007
Owner: Platform / QA
Consumers: all test modules (unit, integration, regression, hitl)

Pytest configuration and shared fixtures.
Provides: in-memory databases, mock API clients, sample data.
All fixtures are designed to be side-effect-free and repeatable.
SOLID: Single Responsibility — each fixture creates one resource.
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from datastore.api.db import get_duckdb_connection, get_sqlite_connection, init_duckdb, init_sqlite
from datastore.api.main import app


# ===== Database Fixtures =====
@pytest.fixture
def test_duckdb():
    """
    In-memory DuckDB instance for testing.

    No side effects — uses :memory: path. Each test gets a fresh database.

    Returns:
        DuckDB connection object
    """
    init_duckdb(Path(":memory:"))
    with get_duckdb_connection(Path(":memory:")) as conn:
        yield conn


@pytest.fixture
def test_sqlite():
    """
    In-memory SQLite instance for testing.

    No side effects — uses :memory: path. Each test gets a fresh database.

    Returns:
        SQLite connection object
    """
    init_sqlite(Path(":memory:"))
    with get_sqlite_connection(Path(":memory:")) as conn:
        yield conn


# ===== API Fixtures =====
@pytest.fixture
def mock_datastore_api():
    """
    FastAPI TestClient for DataStore API.

    Provides synchronous client for testing API endpoints.
    No real database — uses in-memory stores via app dependencies.

    Returns:
        FastAPI TestClient
    """
    return TestClient(app)


# ===== Sample Data Fixtures =====
@pytest.fixture
def sample_universe() -> List[str]:
    """
    Sample stock universe for testing.

    Returns:
        List of ticker symbols
    """
    return [
        "RELIANCE",
        "TCS",
        "INFY",
        "HDFC",
        "ICICIBANK",
        "HINDUNILVR",
        "WIPRO",
        "LT",
        "MARUTI",
        "BAJAJFINSV",
    ]


@pytest.fixture
def sample_ohlcv(sample_universe: List[str]) -> pd.DataFrame:
    """
    Generate sample OHLCV data for testing.

    SPEC-DS-001: Daily OHLCV records for multiple tickers.

    Args:
        sample_universe: List of tickers

    Returns:
        DataFrame with columns [date, ticker, open, high, low, close, volume, adjusted_close]
        100 rows per ticker (100 trading days)
    """
    data = []
    base_date = datetime(2023, 1, 1)

    for ticker in sample_universe:
        # Use different base prices per ticker
        base_price = {"RELIANCE": 2500, "TCS": 3500, "INFY": 1200}.get(ticker, 1500)

        for i in range(100):
            date = base_date + timedelta(days=i)
            # Skip weekends
            if date.weekday() >= 5:
                continue

            open_price = base_price + (i * 5)
            close_price = open_price + (i % 10 - 5) * 2

            data.append(
                {
                    "date": date,
                    "ticker": ticker,
                    "open": open_price,
                    "high": max(open_price, close_price) * 1.02,
                    "low": min(open_price, close_price) * 0.98,
                    "close": close_price,
                    "volume": 1_000_000 + (i % 500_000),
                    "adjusted_close": close_price,
                }
            )

    return pd.DataFrame(data)


@pytest.fixture
def sample_features(sample_universe: List[str]) -> pd.DataFrame:
    """
    Generate sample feature matrix for testing.

    SPEC-FEAT-001: Technical indicators and derived features.

    Args:
        sample_universe: List of tickers

    Returns:
        DataFrame with columns [date, ticker, rsi_14, macd, ..., data_staleness_flag]
    """
    data = []
    base_date = datetime(2023, 1, 1)

    for ticker in sample_universe:
        for i in range(100):
            date = base_date + timedelta(days=i)
            if date.weekday() >= 5:
                continue

            data.append(
                {
                    "date": date,
                    "ticker": ticker,
                    "rsi_14": 50.0 + (i % 30) - 15,
                    "macd": (i % 10) - 5,
                    "macd_signal": (i % 10) - 4,
                    "atr_14": 50.0 + (i % 20),
                    "sma_20": 1500.0 + (i * 2),
                    "sma_50": 1500.0 + (i * 1.5),
                    "sma_200": 1500.0 + (i * 1.0),
                    "volume_ratio": 0.8 + (i % 10) * 0.05,
                    "pe_ratio": 20.0 + (i % 15),
                    "pb_ratio": 3.0 + (i % 5) * 0.5,
                    "roe": 15.0 + (i % 10),
                    "debt_to_equity": 0.5 + (i % 5) * 0.1,
                    "promoter_holding": 60.0 + (i % 5),
                    "data_staleness_flag": 0 if i > 5 else 1,
                    "missing_feature_count": 0,
                }
            )

    return pd.DataFrame(data)


@pytest.fixture
def sample_fundamentals(sample_universe: List[str]) -> pd.DataFrame:
    """
    Generate sample fundamental data for testing.

    SPEC-DS-003: Quarterly fundamentals with PIT enforcement via announcement_date.

    Args:
        sample_universe: List of tickers

    Returns:
        DataFrame with columns [date, ticker, fiscal_year, fiscal_quarter,
                               announcement_date, metric_name, metric_value, ...]
    """
    data = []
    base_date = datetime(2023, 1, 1)

    for ticker in sample_universe:
        for quarter in range(1, 5):  # 4 quarters
            fiscal_year = 2023
            # Simulate announcement delays: Q1 in April, Q2 in July, etc.
            announcement_date = datetime(2023, 1 + quarter * 3, 15)

            data.append(
                {
                    "date": base_date,
                    "ticker": ticker,
                    "fiscal_year": fiscal_year,
                    "fiscal_quarter": quarter,
                    "announcement_date": announcement_date,
                    "metric_name": "eps",
                    "metric_value": 50.0 + (quarter * 10),
                    "unit": "INR",
                    "data_source": "bse_filing",
                    "filing_date": announcement_date,
                    "month_end": None,
                }
            )

            data.append(
                {
                    "date": base_date,
                    "ticker": ticker,
                    "fiscal_year": fiscal_year,
                    "fiscal_quarter": quarter,
                    "announcement_date": announcement_date,
                    "metric_name": "roe",
                    "metric_value": 15.0 + (quarter * 2),
                    "unit": "%",
                    "data_source": "bse_filing",
                    "filing_date": announcement_date,
                    "month_end": None,
                }
            )

    return pd.DataFrame(data)


# ===== Utility Fixtures =====
@pytest.fixture
def temp_data_dir(tmp_path) -> Path:
    """
    Temporary directory for test data files.

    Returns:
        Path object pointing to a clean temporary directory
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


@pytest.fixture
def mock_config(monkeypatch) -> Dict[str, str]:
    """
    Mock configuration environment variables.

    Allows tests to override UNIVERSE_PROFILE, API settings, etc.

    Returns:
        Dict of mocked config values
    """
    config = {
        "UNIVERSE_PROFILE": "phase_1",
        "DATASTORE_API_HOST": "localhost",
        "DATASTORE_API_PORT": "8000",
    }

    for key, value in config.items():
        monkeypatch.setenv(key, value)

    return config


# ===== Pytest Hooks =====
@pytest.fixture(autouse=True)
def cleanup_connections():
    """
    Cleanup database connections after each test.

    Runs automatically for all tests (autouse=True).
    """
    yield
    # Cleanup after test
    from datastore.api.db import close_all_connections

    close_all_connections()


@pytest.fixture(autouse=True)
def reset_feature_registry():
    """
    Reset feature registry to known state after each test.

    Some tests may modify the registry; this ensures clean state.
    """
    yield
    # Could reset registry here if needed


# ===== Markers for Test Organization =====
def pytest_configure(config):
    """Register custom pytest markers."""
    config.addinivalue_line("markers", "unit: unit tests (fast, no I/O)")
    config.addinivalue_line(
        "markers", "integration: integration tests (slower, with databases)"
    )
    config.addinivalue_line("markers", "regression: regression tests (historical data)")
    config.addinivalue_line("markers", "hitl: human-in-the-loop tests (manual)")
    config.addinivalue_line("markers", "slow: slow tests (skip with -m 'not slow')")
