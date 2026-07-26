"""
backtest/core/screener_cache.py

Phase: 3.x (Backtest Queue performance — Technical channel signal reuse)
Owner: Platform / Backtest
Consumers: backtest/adapters/technical_adapter.py

Persistent, cross-process cache of ScreenerEngine.screen()'s raw output,
keyed by (template_name, as_of_date, ticker) — the technical_screener_cache
DuckDB table (datastore/schema/create_backtest.py).

Why this exists: the live strategy queue runs each of the 42 Technical
screener templates once per exit-policy variant (up to 9 jobs per template
— 5 base variants + regime_conditional at 4 regime_method thresholds, see
FeatureBacklog.md). Entry-signal generation (which tickers a template
matches on a given date, and their scores) is exit-policy-agnostic — it
depends only on (template_name, date), never on which exit policy the job
uses or what any specific run currently holds — so every one of those
~9 jobs per template was independently re-reading/re-scoring the same
full-universe daily feature Parquet for the same dates, a confirmed 2026-
07-25 finding (root-caused live via py-spy on a stalled regime_conditional
job). This cache turns that into: first job for a given (template, date)
computes and persists; every subsequent job (any exit variant, including
future ones) reads instead of recomputing.

Design, reviewed by ml-rigor-reviewer + backtest-reviewer before
implementation (2026-07-25, FeatureBacklog.md):
  - Cache key is (template_name, as_of_date, ticker) — NOT scoped by
    top_n/universe/exit_variant, matching ScreenerEngine.screen()'s actual
    dependency (a pure function of template + date, confirmed via review:
    no caching/mutation inside ScreenerEngine itself, deterministic
    md5(ticker) tiebreak not random, no portfolio/exit-policy reference
    anywhere in engine.py/templates.py).
  - Only ever caches FULL matches (score == 1.0), matching screen()'s own
    "Return only full matches" behavior — the cache-population read uses
    an unbounded limit (_CACHE_POPULATE_LIMIT), never any one job's
    top_n*5, so a job configured with a different top_n than whichever job
    first populated the cache still gets every real match, not a truncated
    subset (a hardcoded-limit cache would have silently under-supplied
    candidates for a future differently-configured job — an identified,
    avoided failure mode, not a hypothetical).
  - key_values (the per-ticker technical indicator snapshot) is preserved
    verbatim in the cache — TechnicalAdapter.feature_vector() depends on
    it for backtest_feature_log; dropping it would silently degrade
    downstream ML feature-vector consumers with no error.
  - No separate offline "precompute" script with its own idea of which
    dates to cover: population happens lazily, from the same live call
    path every job already uses (get_or_compute below), driven by
    whichever job's actual day-by-day walk asks for a date first. This
    sidesteps the biggest reviewed risk of a standalone precompute pass —
    its date range silently drifting out of sync with what a real job's
    walk visits — by construction: there is no second implementation of
    "which dates matter" to drift.
  - A cache miss NEVER silently resolves to "no match" (which would be
    indistinguishable from a real, legitimate empty screener result and
    could silently liquidate positions) — a miss always falls through to
    a live ScreenerEngine.screen() call, which is what populates the
    cache for the next caller.
"""

import json
import logging
from typing import List, Optional

from systems.technical_analysis.screener.engine import ScreenerEngine, ScreenerResult

logger = logging.getLogger(__name__)

# Larger than any real universe (~2,317 tickers) and, per screen()'s own
# "full matches only" filtering, the true candidate count on any given day
# is bounded by how many tickers meet EVERY condition — always a small
# fraction of the universe. This limit exists only so a future template
# with an unusually permissive condition set can't produce an unbounded
# result; it is not a tuning knob and must never be set from any job's
# own --top-n.
_CACHE_POPULATE_LIMIT = 10_000


def _row_to_result(row: tuple, template_name: str, as_of_date: str) -> ScreenerResult:
    ticker, matched_conditions, total_conditions, score, key_values_json = row
    return ScreenerResult(
        ticker=ticker, date=as_of_date, template_name=template_name,
        matched_conditions=matched_conditions, total_conditions=total_conditions,
        score=score, key_values=json.loads(key_values_json),
    )


def _read_cached(conn, template_name: str, as_of_date: str) -> Optional[List[ScreenerResult]]:
    """None means "not cached yet" (a real cache miss); [] means "cached,
    and there were genuinely zero full matches that day" — the two are
    deliberately distinguishable so a miss is never mistaken for a
    legitimate empty result. _write_cache() always writes a sentinel
    presence row (ticker=_PRESENCE_TICKER) even on a zero-match day, so a
    single SELECT can tell the two apart: rows == [] means never cached
    (not even the sentinel), rows == [sentinel] means cached-and-empty."""
    rows = conn.execute(
        """
        SELECT ticker, matched_conditions, total_conditions, score, key_values_json
        FROM technical_screener_cache
        WHERE template_name = ? AND as_of_date = ?
        """,
        [template_name, as_of_date],
    ).fetchall()
    if not rows:
        return None
    return [_row_to_result(r, template_name, as_of_date) for r in rows if r[0] != _PRESENCE_TICKER]


_PRESENCE_TICKER = "__CACHE_POPULATED__"


def _write_cache(conn, template_name: str, as_of_date: str, results: List[ScreenerResult]) -> None:
    rows = [
        (template_name, as_of_date, r.ticker, r.matched_conditions, r.total_conditions, r.score,
         json.dumps(r.key_values, default=str))
        for r in results
    ]
    # A sentinel presence row, even for a genuinely-zero-match day, so a
    # later read can tell "cached empty" apart from "never cached" without
    # a second table/column — see _read_cached()'s docstring.
    rows.append((template_name, as_of_date, _PRESENCE_TICKER, 0, 0, 0.0, "{}"))
    conn.executemany(
        """
        INSERT INTO technical_screener_cache
            (template_name, as_of_date, ticker, matched_conditions, total_conditions, score, key_values_json)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (template_name, as_of_date, ticker) DO UPDATE SET
            matched_conditions = excluded.matched_conditions,
            total_conditions = excluded.total_conditions,
            score = excluded.score,
            key_values_json = excluded.key_values_json
        """,
        rows,
    )


def get_or_compute(
    conn, engine: ScreenerEngine, template_name: str, as_of_date: str,
) -> List[ScreenerResult]:
    """The one entry point TechnicalAdapter uses instead of calling
    engine.screen() directly. Same return shape/ordering contract as
    screen() itself (full matches, score desc) — callers that already
    apply their own limit/top_n slicing downstream (as TechnicalAdapter
    does) are unaffected by switching to this function; it only ever
    returns MORE candidates than a limited screen() call would, never
    fewer.

    conn: an open, read-write DuckDB connection to BACKTEST_DUCKDB_PATH
    (the caller's existing connection — this function does not manage
    connection lifecycle, matching every other backtest/core/*.py writer).
    """
    cached = _read_cached(conn, template_name, as_of_date)
    if cached is not None:
        return cached
    results = engine.screen(template_name, date=as_of_date, limit=_CACHE_POPULATE_LIMIT)
    _write_cache(conn, template_name, as_of_date, results)
    logger.debug(f"technical_screener_cache: populated {template_name}/{as_of_date} ({len(results)} matches)")
    return results
