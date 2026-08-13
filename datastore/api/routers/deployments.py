"""
datastore/api/routers/deployments.py

Owner: Platform / Deployment (A91)
Consumers: the Deploy control on /backtest-report, and the deploy page.

Channel-agnostic deployment of a registry strategy. `/api/v1/momentum/configs`
is momentum-shaped down to its column names, so Technical, Fundamental and ML
strategies could not be deployed at all -- the Deploy checkbox had to render
disabled for three channels out of four, and the report's whole purpose is a
deploy decision.

A deployment REFERENCES a strategy_registry row; it never copies the rules.
That is what makes AGENTS.md invariant 6 true -- the backtested definition and
the deployed definition are the same row -- and it is why there is no field
here for entry or exit criteria. A deployment that needs different rules needs
a new registry version, not a config field, or the deployed strategy quietly
stops being the one that was tested.
"""

from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from config.settings import BACKTEST_DUCKDB_PATH, DUCKDB_PATH
from datastore.api.db import get_duckdb_connection
from strategies.registry import get_strategy

router = APIRouter(prefix="/api/v1/deployments", tags=["Deployments"])

#: "no portfolio". A sentinel rather than NULL because NULLs compare distinct,
#: which would let the duplicate-deployment check below pass on rows it should
#: have caught.
NO_PORTFOLIO = 0


class DeploymentCreate(BaseModel):
    strategy_key: str
    strategy_version: Optional[int] = Field(
        default=None,
        description=(
            "Version to deploy. Defaults to the strategy's current version, "
            "resolved and PINNED at creation: a live position must not start "
            "running rules that were revised after it opened."
        ),
    )
    initial_capital: float = Field(..., ge=0)
    start_date: date
    sip_amount: float = Field(default=0.0, ge=0)
    portfolio_id: int = NO_PORTFOLIO
    rebalance_frequency: Optional[str] = None
    rebalance_day_of_month: Optional[int] = None
    filter_overrides: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    benchmark_index_name: Optional[str] = None
    capital_mode: str = "lump"
    source_run_id: Optional[str] = Field(
        default=None,
        description="The backtest run this was approved from, so a live position traces to its evidence.",
    )
    created_by: Optional[str] = None


class DeploymentOut(BaseModel):
    deployment_id: int
    strategy_key: str
    strategy_version: int
    channel: str
    initial_capital: float
    sip_amount: float
    start_date: date
    portfolio_id: int
    rebalance_frequency: Optional[str] = None
    rebalance_day_of_month: Optional[int] = None
    benchmark_index_name: Optional[str] = None
    capital_mode: str
    source_run_id: Optional[str] = None
    is_active: bool


class DeploymentListResponse(BaseModel):
    deployments: List[DeploymentOut]
    total: int


def _db() -> Path:
    return DUCKDB_PATH


_SELECT = """
    SELECT deployment_id, strategy_key, strategy_version, channel,
           initial_capital, sip_amount, start_date, portfolio_id,
           rebalance_frequency, rebalance_day_of_month, benchmark_index_name,
           capital_mode, source_run_id, is_active
    FROM strategy_deployments
"""


@router.get("", response_model=DeploymentListResponse)
def list_deployments(
    channel: Optional[str] = Query(None),
    active_only: bool = Query(True),
) -> DeploymentListResponse:
    sql = _SELECT
    where, params = [], []
    if channel:
        where.append("channel = ?")
        params.append(channel)
    if active_only:
        where.append("is_active")
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at DESC"

    with get_duckdb_connection(_db(), read_only=True, persist=False) as conn:
        rows = conn.execute(sql, params).fetchdf().to_dict("records")
    out = [DeploymentOut(**r) for r in rows]
    return DeploymentListResponse(deployments=out, total=len(out))


@router.post("", response_model=DeploymentOut, status_code=201)
def create_deployment(body: DeploymentCreate) -> DeploymentOut:
    # The strategy must exist in the registry. Deploying an unknown key would
    # create a live config that no runner can resolve -- it would fail at the
    # first rebalance, in production, rather than here.
    strategy = get_strategy(
        body.strategy_key, version=body.strategy_version, db_path=BACKTEST_DUCKDB_PATH
    )
    if strategy is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No strategy {body.strategy_key!r}"
                + (f" at version {body.strategy_version}" if body.strategy_version else "")
                + ". A deployment must reference a registry row (A92)."
            ),
        )
    if strategy.get("status") == "retired":
        raise HTTPException(
            status_code=409,
            detail=(
                f"{body.strategy_key} is retired. Deploying a retired strategy "
                "is almost always an accident; revive it in the registry first "
                "if it is deliberate."
            ),
        )

    version = int(strategy["version"])
    channel = strategy["channel"]

    with get_duckdb_connection(_db(), persist=False) as conn:
        # The rule the schema cannot express: DuckDB has no partial unique
        # index, and putting is_active in a plain UNIQUE would forbid a second
        # RETIRED deployment and destroy the history. Two active deployments
        # of the same strategy to the same portfolio would both trade it and
        # double the position, so the check happens here.
        existing = conn.execute(
            """
            SELECT deployment_id FROM strategy_deployments
            WHERE strategy_key = ? AND strategy_version = ?
              AND portfolio_id = ? AND is_active
            """,
            [body.strategy_key, version, body.portfolio_id],
        ).fetchone()
        if existing:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"{body.strategy_key} v{version} is already deployed to "
                    f"portfolio {body.portfolio_id} (deployment {existing[0]}). "
                    "Two active deployments would both trade it and double the "
                    "position. Deactivate the existing one first."
                ),
            )

        import json as _json

        conn.execute(
            """
            INSERT INTO strategy_deployments (
                strategy_key, strategy_version, channel, initial_capital,
                sip_amount, start_date, portfolio_id, rebalance_frequency,
                rebalance_day_of_month, filter_overrides_json,
                benchmark_index_name, capital_mode, source_run_id, created_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                body.strategy_key, version, channel, body.initial_capital,
                body.sip_amount, body.start_date, body.portfolio_id,
                body.rebalance_frequency, body.rebalance_day_of_month,
                _json.dumps(body.filter_overrides), body.benchmark_index_name,
                body.capital_mode, body.source_run_id, body.created_by,
            ],
        )
        row = conn.execute(
            _SELECT + " WHERE deployment_id = (SELECT max(deployment_id) FROM strategy_deployments)"
        ).fetchdf().to_dict("records")[0]
    return DeploymentOut(**row)


@router.post("/{deployment_id}/deactivate", response_model=DeploymentOut)
def deactivate_deployment(deployment_id: int) -> DeploymentOut:
    """Retire a deployment. The row is kept, not deleted: a live position's
    history is the audit trail for every trade it produced."""
    with get_duckdb_connection(_db(), persist=False) as conn:
        found = conn.execute(
            "SELECT deployment_id FROM strategy_deployments WHERE deployment_id = ?",
            [deployment_id],
        ).fetchone()
        if not found:
            raise HTTPException(status_code=404, detail=f"No deployment {deployment_id}")
        conn.execute(
            "UPDATE strategy_deployments SET is_active = FALSE, "
            "updated_at = current_timestamp WHERE deployment_id = ?",
            [deployment_id],
        )
        row = conn.execute(
            _SELECT + " WHERE deployment_id = ?", [deployment_id]
        ).fetchdf().to_dict("records")[0]
    return DeploymentOut(**row)
