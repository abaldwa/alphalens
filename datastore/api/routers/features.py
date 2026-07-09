"""
datastore/api/routers/features.py

Phase: 3.x (Backlog item #6 refactor + #10/AF-3 perf fix)
Specs: SPEC-FEAT-001, SPEC-DS-006
Owner: Platform / DataStore
Consumers: dashboard, systems/ml_signal_engine

GET /api/v1/features/{ticker} — precomputed feature matrix for a ticker
across a date range.

[AS BUILT, item #6] Moved out of datastore/api/main.py (previously the
last inline route left over from before P1.7's router-file reorganization
— see main.py's module docstring) into its own router file, same path,
same tags, wired into main.py the same way as every other router.

[AS BUILT, item #10 / AF-3] The original inline implementation answered a
date-range query by looping `pd.date_range(start, end)` and opening one
Parquet file per calendar day under FEATURES_DAILY_DIR (4,792+ files since
the ~2006 backfill) — a linear file-open cost per query. Now delegates to
datastore.api.utils.feature_store.read_feature_range(), which registers
the `daily/*.parquet` glob as a single DuckDB `read_parquet()` query
(Option A from the backlog note) and lets DuckDB's own per-file min/max
Parquet metadata prune files outside [start_date, end_date] — no
writer-side change to features/matrix_builder.py.
"""

import logging
from datetime import datetime
from typing import List, Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from datastore.api import schemas
from datastore.api.utils.feature_store import read_feature_range

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/features", tags=["Features"])


@router.get("/{ticker}", response_model=schemas.FeatureMatrixResponse)
async def get_features(
    ticker: str,
    start_date: datetime = Query(..., description="Inclusive start date (YYYY-MM-DD)"),
    end_date: datetime = Query(..., description="Inclusive end date (YYYY-MM-DD)"),
    feature_names: Optional[List[str]] = Query(
        None, description="Subset of features (default: all)"
    ),
) -> schemas.FeatureMatrixResponse:
    """
    Query precomputed feature matrix for a ticker.

    SPEC-FEAT-001, SPEC-DS-006: Returns all technical indicators, fundamental ratios,
    and derived features. Includes data_staleness_flag (SPEC-SYS-003) for filtering
    stale observations.

    Args:
        ticker: Stock ticker
        start_date: Inclusive start date
        end_date: Inclusive end date
        feature_names: If provided, return only these features (else return all)

    Returns:
        FeatureMatrixResponse with rows sorted by date ascending
        Each row includes all features + staleness flag

    Raises:
        HTTPException 404: If ticker not found
        HTTPException 400: If date range or feature names invalid
    """
    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker cannot be empty")
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="end_date must be >= start_date")

    df = read_feature_range(ticker, start_date, end_date)

    if df.empty:
        raise HTTPException(
            status_code=404, detail=f"No feature data for {ticker} in [{start_date}, {end_date}]"
        )

    all_feature_cols = [c for c in df.columns if c not in ("date", "ticker")]
    cols = feature_names if feature_names else all_feature_cols
    missing_cols = [c for c in cols if c not in df.columns]
    if missing_cols:
        raise HTTPException(status_code=400, detail=f"Unknown feature name(s): {missing_cols}")

    rows: List[schemas.FeatureMatrixRow] = []
    for _, row in df.iterrows():
        feature_values = {c: (None if pd.isna(row[c]) else float(row[c])) for c in cols}
        rows.append(
            schemas.FeatureMatrixRow(
                date=row["date"],
                ticker=ticker,
                feature_values=feature_values,
                missing_feature_count=sum(1 for v in feature_values.values() if v is None),
            )
        )

    return schemas.FeatureMatrixResponse(
        ticker=ticker,
        start_date=start_date,
        end_date=end_date,
        feature_names=feature_names or [],
        data=rows,
        record_count=len(rows),
    )
