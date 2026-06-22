"""
datastore/api/db.py

Phase: 0.1 (Project Skeleton)
Specs: SPEC-DS-007, SPEC-QUALITY-002
Owner: Platform / DataStore
Consumers: datastore/api, ingestion/*, systems/*, backtest

Database connection management and initialization.
Abstracts DuckDB (analytical queries) and SQLite (transactional/scheduling) setup.
SOLID: Dependency Injection — clients receive connections via context managers.
"""

import logging
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional

try:
    import duckdb
except ImportError:
    duckdb = None

logger = logging.getLogger(__name__)


def init_duckdb(path: Path) -> None:
    """
    Initialize DuckDB database file and schema.

    Creates the file if it doesn't exist. Idempotent — safe to call multiple times.

    Args:
        path: Path to .duckdb file (parent directory must exist)

    Raises:
        ImportError: If duckdb is not installed
        IOError: If parent directory does not exist or write fails
    """
    if duckdb is None:
        raise ImportError(
            "duckdb is not installed. Install via: pip install duckdb"
        )

    if not path.parent.exists():
        raise IOError(f"Parent directory does not exist: {path.parent}")

    # Create/connect to database
    conn = duckdb.connect(str(path))

    # Create standard tables (idempotent with IF NOT EXISTS)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ohlcv (
            date DATE NOT NULL,
            ticker VARCHAR NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            adjusted_close DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, ticker)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS fundamentals (
            date DATE NOT NULL,
            ticker VARCHAR NOT NULL,
            fiscal_year INTEGER,
            fiscal_quarter INTEGER,
            announcement_date TIMESTAMP,
            metric_name VARCHAR,
            metric_value DOUBLE,
            unit VARCHAR,
            data_source VARCHAR,
            filing_date TIMESTAMP,
            month_end DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, ticker, metric_name)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS features (
            date DATE NOT NULL,
            ticker VARCHAR NOT NULL,
            feature_name VARCHAR NOT NULL,
            feature_value DOUBLE,
            data_staleness_flag INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, ticker, feature_name)
        )
    """)

    conn.close()

    logger.info(f"Initialized DuckDB at {path}")


def init_sqlite(path: Path) -> None:
    """
    Initialize SQLite database file and schema.

    Creates the file if it doesn't exist. Used for transactional logs and scheduling.
    Idempotent — safe to call multiple times.

    Args:
        path: Path to .db file (parent directory must exist)

    Raises:
        IOError: If parent directory does not exist or write fails
    """
    if not path.parent.exists():
        raise IOError(f"Parent directory does not exist: {path.parent}")

    conn = sqlite3.connect(str(path))
    cursor = conn.cursor()

    # Create standard tables (idempotent)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE NOT NULL,
            status TEXT NOT NULL,
            stage TEXT NOT NULL,
            records_processed INTEGER DEFAULT 0,
            records_skipped INTEGER DEFAULT 0,
            records_failed INTEGER DEFAULT 0,
            data_completeness_pct REAL DEFAULT 0.0,
            started_at TIMESTAMP NOT NULL,
            completed_at TIMESTAMP,
            duration_seconds REAL,
            error_summary TEXT,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, stage)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS model_registry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            version TEXT NOT NULL,
            model_type TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            features_used TEXT,
            accuracy_on_validation REAL,
            metadata TEXT,
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(name, version)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS scheduler_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL,
            scheduled_time TIMESTAMP NOT NULL,
            started_at TIMESTAMP NOT NULL,
            completed_at TIMESTAMP,
            status TEXT NOT NULL,
            retry_count INTEGER DEFAULT 0,
            error_msg TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(job_name, scheduled_time)
        )
    """)

    conn.commit()
    conn.close()

    logger.info(f"Initialized SQLite at {path}")


# Global connection pools (simple implementation; upgrade to proper pooling if needed)
_duckdb_connections: dict = {}
_sqlite_connections: dict = {}


@contextmanager
def get_duckdb_connection(
    db_path: Optional[Path] = None,
    read_only: bool = False,
) -> Iterator:
    """
    Context manager for DuckDB connections.

    Yields a DuckDB connection object. Handles cleanup automatically.

    Args:
        db_path: Path to .duckdb file. If None, uses in-memory database (for testing).
        read_only: Open in read-only mode. DuckDB allows only one read-write
            connection to a given file at a time, but any number of
            concurrent read_only connections — pass True for any caller
            that never writes (e.g. the DataStore API's GET endpoints,
            features/macro_features.py's direct reads per SPEC-DS-002) so
            a long-lived process holding the file open doesn't lock out
            other readers (caught wiring features/matrix_builder.py, P1.1
            — see BuildLog.md).

    Yields:
        DuckDB connection object

    Raises:
        ImportError: If duckdb not installed
        IOError: If db_path is invalid

    Example:
        with get_duckdb_connection(db_path) as conn:
            result = conn.execute("SELECT * FROM ohlcv LIMIT 10").fetchall()
    """
    if duckdb is None:
        raise ImportError("duckdb is not installed")

    # Default to in-memory for testing
    path_key = str(db_path) if db_path else ":memory:"
    cache_key = f"{path_key}|read_only={read_only}"

    if cache_key not in _duckdb_connections:
        _duckdb_connections[cache_key] = duckdb.connect(path_key, read_only=read_only)

    conn = _duckdb_connections[cache_key]
    try:
        yield conn
    finally:
        # Keep connection open in pool; close only on explicit cleanup
        pass


@contextmanager
def get_sqlite_connection(
    db_path: Optional[Path] = None,
) -> Iterator[sqlite3.Connection]:
    """
    Context manager for SQLite connections.

    Yields a SQLite connection object. Handles cleanup automatically.

    Args:
        db_path: Path to .db file. If None, uses in-memory database (for testing).

    Yields:
        SQLite connection object

    Example:
        with get_sqlite_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM pipeline_runs")
    """
    # Default to in-memory for testing
    path_key = str(db_path) if db_path else ":memory:"

    if path_key not in _sqlite_connections:
        _sqlite_connections[path_key] = sqlite3.connect(path_key)

    conn = _sqlite_connections[path_key]
    try:
        yield conn
    finally:
        # Keep connection open in pool
        pass


def close_all_connections() -> None:
    """
    Close all pooled database connections.

    Call during application shutdown or test cleanup.
    """
    for conn in _duckdb_connections.values():
        try:
            conn.close()
        except Exception as e:
            logger.warning(f"Error closing DuckDB connection: {e}")

    for conn in _sqlite_connections.values():
        try:
            conn.close()
        except Exception as e:
            logger.warning(f"Error closing SQLite connection: {e}")

    _duckdb_connections.clear()
    _sqlite_connections.clear()

    logger.info("Closed all database connections")
