"""
datastore/api/routers/indices.py

Owner: Platform / Benchmarks (A97)
Consumers: the frontend benchmark selector on /backtest-report/*.

GET /api/v1/indices -- which indices exist in index_ohlcv, what each one
actually covers, and whether comparing a strategy against it over a given
window is honest.

The selector is driven by this endpoint rather than a hardcoded list, so an
index newly captured by the daily pipeline appears with no frontend change,
and one that stops updating disappears from the options instead of silently
producing a benchmark CAGR measured over a shorter period than the strategy.
"""

from datetime import date
from typing import List, Optional, cast

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from config.benchmarks import (
    BROAD_INDICES,
    SIZE_INDICES,
    load_coverage,
    usable_benchmarks,
)
from config.settings import DUCKDB_PATH
from datastore.api.db import get_duckdb_connection

router = APIRouter(prefix="/api/v1/indices", tags=["Indices"])


class IndexInfo(BaseModel):
    index_name: str
    kind: str = Field(description="broad | size | sector")
    first_date: Optional[date]
    last_date: Optional[date]
    n_rows: int
    live_from: Optional[date] = Field(
        default=None,
        description=(
            "First date with a real Open, i.e. when the index actually "
            "launched. Rows before this are NSE's retrospective "
            "back-computation, published with Close only."
        ),
    )
    n_backcomputed: int = Field(
        default=0, description="Sessions before live_from."
    )
    is_fresh: bool = Field(description="Being updated by the daily pipeline.")
    usable_as_benchmark: bool
    caveat: Optional[str] = Field(
        default=None,
        description=(
            "Why a comparison over the requested window is weaker than it "
            "looks, or null if sound. Only set when start/end are supplied."
        ),
    )


class IndexListResponse(BaseModel):
    indices: List[IndexInfo]
    default_benchmark: str
    regime_index: str = Field(
        description=(
            "The index used for regime detection, which is deliberately a "
            "separate choice from the benchmark (A98)."
        )
    )
    live_over_window: List[str] = Field(
        default_factory=list,
        description=(
            "Indices that were actually trading across the whole requested "
            "window. Only set when start_date and end_date are supplied."
        ),
    )
    backcomputed_over_window: List[str] = Field(
        default_factory=list,
        description=(
            "Indices that reach the window only via NSE's retrospective "
            "back-computation. Selectable, but every figure derived from them "
            "carries a caveat."
        ),
    )
    recommended_benchmark: Optional[str] = Field(
        default=None,
        description="Best benchmark for this window, preferring a live series.",
    )
    fallback_reason: Optional[str] = Field(
        default=None,
        description=(
            "Set when the size-matched index did not trade across the window "
            "and Nifty 500 was substituted. Must be shown to the user "
            "alongside any excess-return figure: a broad index does not match "
            "a size-scoped strategy, so part of the excess is the size spread."
        ),
    )


def _kind(name: str) -> str:
    if name in BROAD_INDICES:
        return "broad"
    if name in SIZE_INDICES:
        return "size"
    return "sector"


@router.get("", response_model=IndexListResponse)
def list_indices(
    start_date: Optional[date] = Query(
        None, description="Window start; enables per-index caveats."
    ),
    end_date: Optional[date] = Query(None, description="Window end."),
    include_stale: bool = Query(
        False,
        description=(
            "Include indices the daily pipeline is no longer updating. Off by "
            "default so they are not offered for returns comparison."
        ),
    ),
    preferred: Optional[str] = Query(
        None,
        description=(
            "The size-matched index this strategy would ideally use. If it did "
            "not trade across the window, Nifty 500 is recommended instead and "
            "fallback_reason explains why (A104)."
        ),
    ),
) -> IndexListResponse:
    from config.benchmarks import (
        DEFAULT_BENCHMARK_INDEX,
        DEFAULT_REGIME_INDEX,
        benchmark_options,
    )

    # persist=False is required of every router call site: a cached connection
    # holds the DuckDB file locked open for the life of the API process, which
    # has caused two prior incidents. read_only=True because this endpoint only
    # measures coverage.
    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        coverage = load_coverage(conn)

    live: List[str] = []
    backcomputed: List[str] = []
    recommended: Optional[str] = None
    fallback_reason: Optional[str] = None
    if start_date and end_date:
        # benchmark_options returns a heterogeneous dict, so each value is
        # narrowed at the boundary rather than assigned straight into a typed
        # local -- mypy cannot see through the dict and the alternative is an
        # untyped `opts` leaking through the rest of the function.
        opts = benchmark_options(coverage, start_date, end_date, preferred=preferred)
        live = cast(List[str], opts["live"])
        backcomputed = cast(List[str], opts["backcomputed"])
        recommended = cast(Optional[str], opts["recommended"])
        fallback_reason = cast(Optional[str], opts["fallback_reason"])
        # Over a window, "usable" means the index actually traded then.
        # Anything reachable only through back-computation stays selectable
        # but is reported separately, so the UI marks it rather than hides it.
        usable = set(live)
    else:
        usable = set(usable_benchmarks(coverage, require_fresh=not include_stale))

    out: List[IndexInfo] = []
    for name, cov in sorted(coverage.items()):
        if not cov.is_fresh and not include_stale:
            continue
        caveat = (
            cov.comparison_caveat(start_date, end_date)
            if start_date and end_date
            else None
        )
        out.append(
            IndexInfo(
                index_name=name,
                kind=_kind(name),
                first_date=cov.first_date,
                last_date=cov.last_date,
                n_rows=cov.n_rows,
                live_from=cov.live_from,
                n_backcomputed=cov.n_backcomputed,
                is_fresh=cov.is_fresh,
                usable_as_benchmark=name in usable,
                caveat=caveat,
            )
        )

    return IndexListResponse(
        indices=out,
        default_benchmark=DEFAULT_BENCHMARK_INDEX,
        regime_index=DEFAULT_REGIME_INDEX,
        live_over_window=live,
        backcomputed_over_window=backcomputed,
        recommended_benchmark=recommended,
        fallback_reason=fallback_reason,
    )


class IndexReturn(BaseModel):
    """Buy-and-hold performance of one index over one window."""

    index_name: str
    start_date: Optional[date]
    end_date: Optional[date]
    start_close: Optional[float]
    end_close: Optional[float]
    cagr: Optional[float] = Field(
        default=None, description="Annualised, calendar/365.25 basis, as a fraction."
    )
    n_rows: int
    status: str = Field(
        description="ok | no_data | insufficient_history — never a synthetic figure."
    )
    caveat: Optional[str] = None


class IndexReturnsResponse(BaseModel):
    returns: List[IndexReturn]


@router.get("/returns", response_model=IndexReturnsResponse)
def index_returns(
    start_date: date = Query(description="Window start (inclusive)."),
    end_date: date = Query(description="Window end (inclusive)."),
    index_name: Optional[List[str]] = Query(
        None,
        description=(
            "Indices to measure. Repeat the parameter for several. Omitted "
            "means every index the daily pipeline is still updating."
        ),
    ),
) -> IndexReturnsResponse:
    """Buy-and-hold CAGR per index over an explicit window (A98/A104).

    WHY THIS EXISTS: a backtest run stores the benchmark it was measured
    against at run time. The report's benchmark selector could therefore
    change which index it *said* it was comparing to while every number on
    screen stayed pinned to the run's original choice — a strategy shown
    against "Nifty 100" was still being scored against Nifty 500. This
    endpoint gives the report a real figure for the selected index over the
    same window, so switching benchmark changes the comparison instead of
    only the label.

    The measurement is deliberately the same one backtest/core/engine.py's
    benchmark curve makes: first real close in the window to last real close
    in the window, annualised on the calendar 365.25 basis. Nothing is
    interpolated and no missing series is filled — an index with fewer than
    two real bars in the window reports `status` and a null CAGR rather than
    a number that would silently be measured over a shorter period than the
    strategy it is about to be subtracted from.
    """
    from config.benchmarks import DEFAULT_BENCHMARK_INDEX  # noqa: F401

    with get_duckdb_connection(DUCKDB_PATH, read_only=True, persist=False) as conn:
        coverage = load_coverage(conn)
        wanted = (
            [n for n in index_name if n in coverage]
            if index_name
            else sorted(n for n, c in coverage.items() if c.is_fresh)
        )
        out: List[IndexReturn] = []
        for name in wanted:
            rows = conn.execute(
                """
                SELECT date, close FROM index_ohlcv
                WHERE index_name = ? AND date BETWEEN ? AND ? AND close > 0
                ORDER BY date
                """,
                [name, start_date, end_date],
            ).fetchall()
            cov = coverage[name]
            caveat = cov.comparison_caveat(start_date, end_date)
            if len(rows) < 2:
                out.append(
                    IndexReturn(
                        index_name=name,
                        start_date=None,
                        end_date=None,
                        start_close=None,
                        end_close=None,
                        cagr=None,
                        n_rows=len(rows),
                        status="no_data" if not rows else "insufficient_history",
                        caveat=caveat,
                    )
                )
                continue
            first_d, first_c = rows[0]
            last_d, last_c = rows[-1]
            years = (last_d - first_d).days / 365.25
            cagr = (
                (float(last_c) / float(first_c)) ** (1.0 / years) - 1.0
                if years > 0 and float(first_c) > 0 and float(last_c) > 0
                else None
            )
            out.append(
                IndexReturn(
                    index_name=name,
                    start_date=first_d,
                    end_date=last_d,
                    start_close=float(first_c),
                    end_close=float(last_c),
                    cagr=cagr,
                    n_rows=len(rows),
                    status="ok" if cagr is not None else "insufficient_history",
                    caveat=caveat,
                )
            )
    return IndexReturnsResponse(returns=out)
