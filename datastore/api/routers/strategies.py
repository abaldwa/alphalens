"""
datastore/api/routers/strategies.py

Owner: Platform / Strategy registry (A95)
Consumers: the /backtest-report frontend, the deploy page, and anything that
needs to know what a strategy IS rather than how one run of it performed.

GET /api/v1/strategies      -- every declared strategy, from strategy_registry
GET /api/v1/strategies/{key}
GET /api/v1/filters         -- every declared filter, from filter_registry

This is the endpoint that makes AGENTS.md invariant 5 real: backtest, API and
frontend read the same rows. Before it, the frontend derived strategy identity
and labels client-side from variant-id string parsing, and the backtest
imported TEMPLATES/build_category_presets/STRATEGY_CATALOG directly -- three
copies of the same facts, free to drift.

Point-in-time is preserved: `version` and `as_of` select a historical
definition, because a run executed against version 3 must not be explained
using version 5's rules.
"""

from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from config.settings import BACKTEST_DUCKDB_PATH
from strategies.registry import (
    get_filter,
    get_strategy,
    list_filters,
    list_strategies,
    resolve_filters,
)

router = APIRouter(prefix="/api/v1", tags=["Strategy registry"])


class StrategyOut(BaseModel):
    strategy_key: str
    version: int
    channel: str
    name: str
    display_label: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    definition: Dict[str, Any] = Field(default_factory=dict)
    # Entry is an ORDERED LIST of predicates ({feature, op, value|feature2}),
    # uniform across all four channels; exit is a single policy object. The
    # asymmetry is real, not an oversight: entry conditions compose and their
    # order is meaningful, an exit policy is one choice with parameters.
    entry_criterion: List[Dict[str, Any]] = Field(default_factory=list)
    exit_criterion: Dict[str, Any] = Field(default_factory=dict)
    filter_ids: List[str] = Field(default_factory=list)
    status: str
    valid_from: Optional[date] = None
    valid_to: Optional[date] = Field(
        default=None,
        description="Null for the current version. A non-null value means this row was superseded.",
    )
    source_ref: Optional[str] = None


class StrategyListResponse(BaseModel):
    strategies: List[StrategyOut]
    total: int
    channels: Dict[str, int] = Field(
        default_factory=dict, description="Count per channel, for the UI's filter chips."
    )


class FilterOut(BaseModel):
    filter_id: str
    version: int
    name: str
    description: Optional[str] = None
    filter_type: str
    params_schema: Dict[str, Any] = Field(default_factory=dict)
    default_params: Dict[str, Any] = Field(default_factory=dict)
    applies_to_channels: List[str] = Field(default_factory=list)
    implementation_ref: Optional[str] = None
    status: str


class FilterListResponse(BaseModel):
    filters: List[FilterOut]
    total: int


class ResolvedFilter(BaseModel):
    filter_id: str
    params: Dict[str, Any]
    implementation_ref: Optional[str] = None


def _db_path() -> Path:
    return BACKTEST_DUCKDB_PATH


@router.get("/strategies", response_model=StrategyListResponse)
def list_strategies_endpoint(
    channel: Optional[str] = Query(
        None, description="momentum | technical | fundamental | ml"
    ),
    status: Optional[str] = Query(
        "active",
        description=(
            "active | draft | retired. Pass an empty string for every status, "
            "which is what an audit view wants."
        ),
    ),
) -> StrategyListResponse:
    rows = list_strategies(
        channel=channel,
        status=status or None,
        db_path=_db_path(),
    )
    counts: Dict[str, int] = {}
    for r in rows:
        counts[r["channel"]] = counts.get(r["channel"], 0) + 1
    return StrategyListResponse(
        strategies=[StrategyOut(**r) for r in rows],
        total=len(rows),
        channels=counts,
    )


@router.get(
    "/strategies/{strategy_key:path}/filters", response_model=List[ResolvedFilter]
)
def resolve_strategy_filters(strategy_key: str) -> List[ResolvedFilter]:
    """A strategy's filters with their parameters already resolved -- declared
    defaults with the strategy's own overrides applied on top. This is what a
    runner should execute, and what the UI should display, so neither has to
    re-implement the override precedence."""
    row = get_strategy(strategy_key, db_path=_db_path())
    if row is None:
        raise HTTPException(status_code=404, detail=f"No strategy {strategy_key!r}")
    resolved = resolve_filters(
        row.get("filter_ids") or [],
        (row.get("definition") or {}).get("filter_overrides"),
        db_path=_db_path(),
    )
    return [ResolvedFilter(**r) for r in resolved]


# Registered BEFORE the greedy /strategies/{key:path} route below: `:path`
# matches slashes, so it would otherwise swallow "/filters" and this endpoint
# would be unreachable (it 404s with the suffix treated as part of the key).
# FastAPI matches in registration order, so order here is behaviour.
@router.get("/strategies/{strategy_key:path}", response_model=StrategyOut)
def get_strategy_endpoint(
    strategy_key: str,
    version: Optional[int] = Query(
        None, description="A specific version. Defaults to the current one."
    ),
    as_of: Optional[date] = Query(
        None,
        description=(
            "The definition in force on this date. Use this rather than the "
            "current row when explaining a historical run: a run executed "
            "against version 3 must not be described using version 5's rules."
        ),
    ),
) -> StrategyOut:
    row = get_strategy(
        strategy_key, version=version, as_of=as_of, db_path=_db_path()
    )
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No strategy {strategy_key!r}"
                + (f" at version {version}" if version else "")
                + (f" as of {as_of}" if as_of else "")
            ),
        )
    return StrategyOut(**row)


@router.get("/filters", response_model=FilterListResponse)
def list_filters_endpoint(
    channel: Optional[str] = Query(
        None, description="Only filters that apply to this channel."
    ),
) -> FilterListResponse:
    rows = list_filters(channel=channel, db_path=_db_path())
    return FilterListResponse(
        filters=[FilterOut(**r) for r in rows], total=len(rows)
    )


@router.get("/filters/{filter_id}", response_model=FilterOut)
def get_filter_endpoint(filter_id: str) -> FilterOut:
    row = get_filter(filter_id, db_path=_db_path())
    if row is None:
        raise HTTPException(status_code=404, detail=f"No filter {filter_id!r}")
    return FilterOut(**row)
