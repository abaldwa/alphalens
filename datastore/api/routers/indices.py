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
from typing import List, Optional

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
        opts = benchmark_options(coverage, start_date, end_date, preferred=preferred)
        live = opts["live"]
        backcomputed = opts["backcomputed"]
        recommended = opts["recommended"]
        fallback_reason = opts["fallback_reason"]
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
