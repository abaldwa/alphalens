"""
Shared fixtures for the momentum_framework smoke test suite.

Two DB fixtures, matching this project's own convention (see
tests/unit/test_momentum_signal.py): a real seeded in-memory DuckDB for
tests that need controlled, hand-crafted data (never write synthetic
rows into the production DB — see feedback_no_synthetic_db_writes
memory), and a READ-ONLY connection to the real production DB for tests
that verify against actual market data (regime detection through COVID,
real sector mappings, real ADTV — the standard this whole framework was
held to during porting).
"""

from pathlib import Path
from typing import List, Tuple

import duckdb
import pytest

PROD_DB_PATH = Path("/home/amit/projects/AlphaLens/datastore/normalised/alphalens.duckdb")


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "real_data: test reads the live production DB (prod_conn) rather than synthetic data"
    )


@pytest.fixture(scope="session")
def prod_conn():
    """Read-only connection to the real production DB. Session-scoped —
    opened once, reused across every test that needs real market data."""
    if not PROD_DB_PATH.exists():
        pytest.skip(f"Production DB not found at {PROD_DB_PATH} — skipping real-data tests")
    conn = duckdb.connect(str(PROD_DB_PATH), read_only=True)
    yield conn
    conn.close()


@pytest.fixture
def memory_conn():
    """
    Fresh in-memory DuckDB per test, with an ohlcv_adjusted table shaped
    like the real schema. Tests seed their own rows via seed_ohlcv() below
    — never write synthetic rows into the production DB.
    """
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE ohlcv_adjusted (
            date DATE, ticker VARCHAR, open DOUBLE, high DOUBLE, low DOUBLE,
            close DOUBLE, volume BIGINT
        )
    """)
    conn.execute("""
        CREATE TABLE stock_master (
            ticker VARCHAR PRIMARY KEY, company_name VARCHAR, sector VARCHAR, industry VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE index_ohlcv (
            index_name VARCHAR, date DATE, open DOUBLE, high DOUBLE, low DOUBLE,
            close DOUBLE, volume BIGINT
        )
    """)
    yield conn
    conn.close()


def seed_ohlcv(conn, ticker: str, prices: List[Tuple[str, float]]) -> None:
    """Insert (date, close) rows for `ticker`; open=high=low=close, volume=1_000_000."""
    rows = [(d, ticker, p, p, p, p, 1_000_000) for d, p in prices]
    conn.executemany(
        "INSERT INTO ohlcv_adjusted (date, ticker, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )


def seed_index_ohlcv(conn, index_name: str, prices: List[Tuple[str, float]]) -> None:
    rows = [(index_name, d, p, p, p, p, 1_000_000) for d, p in prices]
    conn.executemany(
        "INSERT INTO index_ohlcv (index_name, date, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
