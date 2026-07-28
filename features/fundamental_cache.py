"""
features/fundamental_cache.py

Phase: Fundamental feature backfill performance (2026-07-28)
Owner: Platform / Features
Consumers: features/fundamental.py

Persistent (ticker, fiscal_year, quarter) -> raw-feature-dict cache.

Measured motivation: profiling the daily feature build showed
compute_fundamental_features_panel costing ~42s/day scaled to the full
~2,317-ticker universe, almost entirely in per-ticker history-walk work
(quarter matching, 5-year rolling stats, YoY deltas) that is IDENTICAL
every day a ticker's announcement_date hasn't advanced — i.e. on >98% of
trading days, since real companies report ~4x/year. Only 7 of
FUNDAMENTAL_FEATURES' 51 ratios (PRICE_DEPENDENT_FEATURES in
features/fundamental.py) actually need today's price and must be
recomputed daily regardless.

Backed by DuckDB rather than an in-memory dict alone so the cache
survives a crashed/rebooted backfill process (see BuildLog.md — a laptop
reboot mid-backfill on 2026-07-27 lost an in-memory-only day's worth of
progress; this cache is deliberately disk-persistent for the same reason
the feature Parquet store itself is written incrementally per date).

Not wired into governance/mf_holdings/corporate_action/deep_forensic yet
(2026-07-28 scoping decision: fundamental.py first, expand once this is
validated) — those have their own PIT cache keys (filing_date, ex_date)
and would need their own tables, not this one.
"""

import json
import logging
from typing import Any, Dict, Tuple

from config.settings import FUNDAMENTAL_RAW_CACHE_DB_PATH
from datastore.api.db import get_duckdb_connection

logger = logging.getLogger(__name__)

CacheKey = Tuple[str, int, int]  # (ticker, fiscal_year, quarter)

_TABLE_NAME = "fundamental_raw_cache"


def _ensure_table(conn) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_TABLE_NAME} (
            ticker VARCHAR NOT NULL,
            fiscal_year INTEGER NOT NULL,
            quarter INTEGER NOT NULL,
            raw_json VARCHAR NOT NULL,
            PRIMARY KEY (ticker, fiscal_year, quarter)
        )
        """
    )


def load_fundamental_raw_cache(db_path=None) -> Dict[CacheKey, Dict[str, Any]]:
    """
    Bulk-read the entire cache table once (called once per backfill process
    start, not per ticker/date) — a full scan of a few hundred thousand
    small rows is a one-off cost, not something to repeat per date.

    Returns
    -------
    dict
        {(ticker, fiscal_year, quarter): raw_feature_dict}. Empty dict if
        the table doesn't exist yet (first run) or can't be read.
    """
    path = db_path or FUNDAMENTAL_RAW_CACHE_DB_PATH
    if not path.exists():
        return {}
    try:
        with get_duckdb_connection(path, read_only=False, persist=False) as conn:
            _ensure_table(conn)
            rows = conn.execute(f"SELECT ticker, fiscal_year, quarter, raw_json FROM {_TABLE_NAME}").fetchall()
    except Exception as exc:
        logger.warning("Could not load fundamental_raw_cache (%s) — starting cold", exc)
        return {}
    cache: Dict[CacheKey, Dict[str, Any]] = {}
    for ticker, fy, q, raw_json in rows:
        try:
            cache[(ticker, int(fy), int(q))] = json.loads(raw_json)
        except (ValueError, TypeError) as exc:
            logger.warning("Skipping corrupt fundamental_raw_cache row (%s, %s, %s): %s", ticker, fy, q, exc)
    logger.info("fundamental_raw_cache: loaded %d entries from %s", len(cache), path)
    return cache


def save_fundamental_raw_cache_entries(entries: Dict[CacheKey, Dict[str, Any]], db_path=None) -> None:
    """
    Bulk upsert newly-computed (cache-miss) entries — called once per
    backfill date with only that date's new entries (typically a small
    fraction of the universe once the cache is warm), not the whole cache.
    """
    if not entries:
        return
    path = db_path or FUNDAMENTAL_RAW_CACHE_DB_PATH
    try:
        with get_duckdb_connection(path, read_only=False, persist=False) as conn:
            _ensure_table(conn)
            rows = [(ticker, fy, q, json.dumps(raw)) for (ticker, fy, q), raw in entries.items()]
            conn.executemany(
                f"""
                INSERT INTO {_TABLE_NAME} (ticker, fiscal_year, quarter, raw_json)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (ticker, fiscal_year, quarter) DO UPDATE SET raw_json = excluded.raw_json
                """,
                rows,
            )
    except Exception as exc:
        # A cache-write failure must never break the feature build itself —
        # worst case, tomorrow's run recomputes these entries again.
        logger.warning("Could not persist %d fundamental_raw_cache entries (%s)", len(entries), exc)
