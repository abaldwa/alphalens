"""
datastore/api/routers/macro.py

A27 (2026-07-10): manual-entry endpoints for the real-economy macro series
that have no free automated source (see features/real_economy_macro.py's
module docstring for the per-series live-verification findings —
pmi_manufacturing/pmi_services are commercially licensed; gst_collection_growth/
auto_monthly_sales_growth/rail_freight_growth/upi_transaction_growth/
bank_credit_growth/iip_growth have no free structured feed). Writes into the
SAME macro_real_economy.parquet file and long-format schema
(feature_name, reference_month_end, value, availability_date) that
ingestion/scrapers/macro_real_economy.py's automated scrapers already use
for cement_dispatches_growth/power_consumption_growth — a manual entry is
indistinguishable to features/real_economy_macro.py's PIT-filtered reader
from an automated one, by design.

Monthly cadence: one value per (feature_name, reference_month_end) —
matches this file's existing granularity, not daily.
"""

import logging
from datetime import date as date_type
from typing import List, Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from features.real_economy_macro import REAL_ECONOMY_MACRO_FEATURES, _MACRO_REAL_ECONOMY_PATH

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/macro", tags=["Macro"])

# Only the genuinely-blocked series may be manually entered — cement/power
# already have a real automated source (ingestion/scrapers/macro_real_economy.py)
# and should not be silently overridden by a manual entry that could go stale.
MANUAL_ENTRY_FEATURES = [
    f for f in REAL_ECONOMY_MACRO_FEATURES
    if f not in ("cement_dispatches_growth", "power_consumption_growth")
]


class MacroIndicatorEntry(BaseModel):
    feature_name: str
    reference_month_end: date_type
    value: float = Field(description="The reported value for this indicator/month")


class MacroIndicatorRow(BaseModel):
    feature_name: str
    reference_month_end: str
    value: float
    availability_date: str


class MacroIndicatorsResponse(BaseModel):
    rows: List[MacroIndicatorRow]


class MacroWriteResult(BaseModel):
    feature_name: str
    reference_month_end: str
    written: bool


def _month_end(d: date_type) -> pd.Timestamp:
    return pd.Timestamp(d) + pd.offsets.MonthEnd(0)


@router.get("/indicators", response_model=MacroIndicatorsResponse)
async def get_macro_indicators(
    feature_name: Optional[str] = Query(None, description="Filter to one indicator, e.g. pmi_manufacturing"),
    limit_months: int = Query(12, description="Most recent N months per indicator to return"),
) -> MacroIndicatorsResponse:
    """List manually/automatically entered macro_real_economy.parquet rows, most recent first."""
    if not _MACRO_REAL_ECONOMY_PATH.exists():
        return MacroIndicatorsResponse(rows=[])

    df = pd.read_parquet(_MACRO_REAL_ECONOMY_PATH)
    if feature_name:
        df = df[df["feature_name"] == feature_name]
    df = df.sort_values(["feature_name", "reference_month_end"], ascending=[True, False])
    df = df.groupby("feature_name", group_keys=False).head(limit_months)

    rows = [
        MacroIndicatorRow(
            feature_name=r["feature_name"],
            reference_month_end=str(pd.Timestamp(r["reference_month_end"]).date()),
            value=float(r["value"]),
            availability_date=str(pd.Timestamp(r["availability_date"]).date()),
        )
        for _, r in df.iterrows()
    ]
    return MacroIndicatorsResponse(rows=rows)


@router.post("/indicators", response_model=MacroWriteResult)
async def write_macro_indicator(entry: MacroIndicatorEntry) -> MacroWriteResult:
    """
    Manual upsert of one (feature_name, month) macro reading.

    Only MANUAL_ENTRY_FEATURES may be written here — cement_dispatches_growth/
    power_consumption_growth have a real automated source and must not be
    overridden by a manual entry.

    availability_date is set to today (SPEC-PIPE-003: a value a human just
    typed in is "available" as of right now, same as any other same-day
    disclosure this codebase treats as immediately PIT-eligible).
    """
    if entry.feature_name not in MANUAL_ENTRY_FEATURES:
        raise HTTPException(
            status_code=400,
            detail=f"{entry.feature_name!r} is not manually enterable — expected one of {MANUAL_ENTRY_FEATURES}"
            if entry.feature_name in REAL_ECONOMY_MACRO_FEATURES
            else f"{entry.feature_name!r} is not a real_economy_macro feature — expected one of {REAL_ECONOMY_MACRO_FEATURES}",
        )

    reference_month_end = _month_end(entry.reference_month_end)
    new_row = pd.DataFrame([{
        "feature_name": entry.feature_name,
        "reference_month_end": reference_month_end,
        "value": entry.value,
        "availability_date": pd.Timestamp.now().normalize(),
    }])

    _MACRO_REAL_ECONOMY_PATH.parent.mkdir(parents=True, exist_ok=True)
    if _MACRO_REAL_ECONOMY_PATH.exists():
        existing = pd.read_parquet(_MACRO_REAL_ECONOMY_PATH)
        combined = pd.concat([existing, new_row], ignore_index=True)
        combined = combined.drop_duplicates(subset=["feature_name", "reference_month_end"], keep="last")
    else:
        combined = new_row
    combined.to_parquet(_MACRO_REAL_ECONOMY_PATH, index=False)

    logger.info(f"macro.write_indicator: {entry.feature_name} {reference_month_end.date()} = {entry.value}")
    return MacroWriteResult(
        feature_name=entry.feature_name,
        reference_month_end=str(reference_month_end.date()),
        written=True,
    )
