"""
datastore/api/routers/copilot.py

Phase: Co-Pilot v1
Specs: SPEC-COPILOT-001
Owner: Co-Pilot
Consumers: frontend/ CopilotPanel component

Co-Pilot REST endpoints: natural-language query -> structured strategy
spec -> dedup check -> backtest -> save to the strategies/ registry.
No arbitrary code execution (the LLM only ever produces a StrategySpec,
interpreted by the existing backtest engines) and no synthetic data in
any response — LLM failures, missing features, and uncomputable metrics
are all surfaced as explicit errors/nulls, never fabricated.

Endpoints
---------
POST /api/v1/copilot/query               — NL text -> StrategySpec
POST /api/v1/copilot/dedup                — StrategySpec -> MatchResult or null
POST /api/v1/copilot/backtest             — StrategySpec -> backtest result dict
POST /api/v1/copilot/save                 — StrategySpec + name -> saved slug
GET  /api/v1/copilot/strategies           — list saved strategies
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from systems.copilot import backtest_bridge, dedup, registry, spec_builder
from systems.copilot.llm_client import LLMCallError, LLMConfigError
from systems.copilot.strategy_spec import StrategySpec

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/copilot", tags=["Copilot"])


class QueryRequest(BaseModel):
    text: str


class ConditionModel(BaseModel):
    feature: Optional[str] = None
    op: Optional[str] = None
    value: Optional[Any] = None
    feature2: Optional[str] = None


class UniverseModel(BaseModel):
    rank_start: Optional[int] = None
    rank_end: Optional[int] = None
    mcap_min: Optional[float] = None
    mcap_max: Optional[float] = None


class RulesModel(BaseModel):
    lookback_days: Optional[int] = None
    rebalance_every_n_trading_days: Optional[int] = None
    top_n: Optional[int] = None
    # [2026-08-18] grace_cycles/min_momentum deprecated -- see
    # systems.copilot.strategy_spec.RebalanceRules.


class SpecModel(BaseModel):
    name: str
    description: str = ""
    source_query: str = ""
    universe: UniverseModel = UniverseModel()
    technical: List[Dict[str, Any]] = []
    fundamental: List[Dict[str, Any]] = []
    valuation: List[Dict[str, Any]] = []
    rules: RulesModel = RulesModel()
    unresolved: List[str] = []
    created_at: str = ""
    created_by: str = "copilot"


class SaveRequest(BaseModel):
    spec: SpecModel


def _to_spec(model: SpecModel) -> StrategySpec:
    return StrategySpec.from_dict(model.model_dump())


@router.post("/query")
async def query(request: QueryRequest) -> Dict[str, Any]:
    """NL text -> StrategySpec. Real OpenRouter call — raises a clear 502
    if OPENROUTER_API_KEY is unset or the call fails, never a fake spec."""
    try:
        spec = spec_builder.build_spec(request.text)
    except LLMConfigError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except LLMCallError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return spec.to_dict()


@router.post("/dedup")
async def check_dedup(spec_model: SpecModel) -> Dict[str, Any]:
    spec = _to_spec(spec_model)
    match = dedup.find_similar(spec)
    if match is None:
        return {"matched": False}
    return {
        "matched": True,
        "matched_name": match.matched_name,
        "matched_source": match.matched_source,
        "similarity": match.similarity,
    }


@router.post("/backtest")
async def run_backtest(spec_model: SpecModel) -> Dict[str, Any]:
    spec = _to_spec(spec_model)
    return backtest_bridge.run_backtest(spec)


@router.post("/save")
async def save_strategy(request: SaveRequest) -> Dict[str, Any]:
    spec = _to_spec(request.spec)
    slug = registry.save(spec)
    return {"slug": slug, "name": spec.name}


@router.get("/strategies")
async def list_strategies() -> Dict[str, Any]:
    specs = registry.load_all()
    return {"strategies": [s.to_dict() for s in specs]}
