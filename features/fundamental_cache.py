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

Restatement invalidation [BUG FIX, 2026-07-28 model-review]: the cache
key here is only (ticker, fiscal_year, quarter) — on its own, a
corrected/restated quarterly filing for the same (fy, quarter) would be
served stale forever. The one restatement-sensitive marker this data
source exposes per row is announcement_date (SPEC-PIPE-003's PIT key),
which IS stored in every cache entry (see `raw` dict's
"announcement_date" field below). features/fundamental.py's cache-read
path (compute_fundamental_features_panel) compares the freshly-fetched
row's announcement_date against the cached one on every lookup and
invalidates the entry on a mismatch — restatement invalidation is
handled there, not in this module, since this module only knows about
opaque JSON blobs, not the PIT semantics of what's inside them.

KNOWN LIMITATION [2026-07-28 second model-review, item 4]: the
restatement check above only compares announcement_date on the single
LATEST (ticker, fiscal_year, quarter) row. The 5-year rolling features
(avg_roce_5y, margin_stability_5y, earnings_volatility_5y, sales_cagr_5y,
delta_roce_3y — features/fundamental.py ~line 375-495) are computed from
up to ~20 quarters of history, not just the latest one. If a HISTORICAL
quarter feeding those rolling windows gets restated (its announcement_date
changes without the LATEST quarter's announcement_date also changing),
this cache key is unaffected and the stale rolling-window value is served
stale — but NOT indefinitely: the cache key is (ticker, fiscal_year,
quarter) for the ticker's LATEST quarter, so the very next time that
ticker's latest quarter itself advances (its next real quarterly filing),
a brand-new cache key is created and the rolling-window features are
recomputed fresh from the full historical quarter set, including the
restated value. The real worst case is bounded to roughly one filing
cycle (~1 quarter, ~3 months) of staleness for a historical restatement,
not indefinite exposure. Extending the check to
hash/compare announcement_date across the full set of historical quarters
actually used by each rolling window would require this cache key
structure (or the read path's cache-hit/-miss decision) to depend on that
full quarter set, which is more invasive than this session's scope — not
implemented, and this cache continues to only guard against latest-quarter
restatements pending a follow-up.

KNOWN LIMITATION [2026-07-28 second model-review, item 13]: this table has
no TTL/pruning for orphaned rows. If a ticker's "latest quarter"
resolution changes fiscal_year/quarter attribution (e.g. a reclassification
or backdated filing correction), old (ticker, fiscal_year, quarter) keys
that no longer correspond to any quarter this ticker will ever resolve to
again are never cleaned up — they just sit in the table indefinitely,
unused but harmless (never read, since the read path always keys off the
CURRENT latest-quarter resolution). Not addressed here; a periodic
pruning job (delete rows whose key hasn't been touched in N runs) is a
reasonable follow-up, not implemented in this session.
"""

import json
import logging
from typing import Any, Dict, Tuple

import numpy as np

from config.settings import FUNDAMENTAL_RAW_CACHE_DB_PATH
from datastore.api.db import get_duckdb_connection

logger = logging.getLogger(__name__)

CacheKey = Tuple[str, int, int]  # (ticker, fiscal_year, quarter)

_TABLE_NAME = "fundamental_raw_cache"


def _json_default(obj: Any) -> Any:
    """
    [BUG FIX, 2026-07-28 model-review] json.dumps has no idea how to
    serialize numpy scalar types (int64/float64/bool_ etc.) — raw
    priced-input dicts built from pandas/duckdb query results routinely
    carry these instead of native Python int/float. Before this fix, that
    TypeError was caught by save_fundamental_raw_cache_entries's blanket
    except-and-warn and the WHOLE bulk upsert call silently failed (one
    bad value poisons the executemany batch), meaning affected tickers
    never got cached, indefinitely, with no visible symptom beyond a
    once-per-run WARNING log easy to miss. Cast any numpy scalar to its
    native Python equivalent via .item(); anything else genuinely
    unserializable still raises (unchanged behavior for real bugs).
    """
    if isinstance(obj, np.generic):
        return obj.item()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


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
    # [BUG FIX, 2026-07-28 model-review item 4] Encode each entry inside its
    # own try/except rather than an eager list comprehension over the whole
    # batch. json.dumps(raw, default=_json_default) only special-cases
    # np.generic — any other JSON-unencodable value (datetime.date,
    # pd.Timestamp, Decimal, bytes, ...) anywhere in the batch used to raise
    # while building `rows`, which the outer except caught and logged as a
    # single WARNING — silently dropping EVERY entry in that date's batch,
    # not just the one bad one. Skipping just the poisoned entry lets its
    # siblings still upsert.
    rows = []
    for (ticker, fy, q), raw in entries.items():
        try:
            rows.append((ticker, fy, q, json.dumps(raw, default=_json_default)))
        except (TypeError, ValueError) as exc:
            logger.warning(
                "Skipping unencodable fundamental_raw_cache entry (%s, %s, %s): %s", ticker, fy, q, exc
            )
    if not rows:
        return
    try:
        with get_duckdb_connection(path, read_only=False, persist=False) as conn:
            _ensure_table(conn)
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
        logger.warning("Could not persist %d fundamental_raw_cache entries (%s)", len(rows), exc)
