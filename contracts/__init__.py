"""
contracts package.

Phase: 0.1 (Project Skeleton)
Specs: SPEC-PIPE-001, SPEC-PIPE-002, SPEC-MODEL-001, SPEC-MODEL-002, SPEC-MODEL-003,
       SPEC-DS-001, SPEC-DS-003, SPEC-DS-004, SPEC-DS-005, SPEC-SOLID-004, SPEC-SOLID-005
Owner: Platform / Architecture
Consumers: systems/ml_signal_engine, systems/technical_analysis,
           backtest, ingestion/quality, features/registry, datastore/api

Abstract interfaces for all pluggable components (models, data access, explainability).
SOLID: Interface Segregation Principle — each contract is lean and focused.
"""

from .interfaces import (
    IClassificationModel,
    IDataStoreReader,
    IDataStoreWriter,
    IExplainableModel,
    IModel,
    IRegimeModel,
    ISurvivalModel,
)

__all__ = [
    "IModel",
    "IClassificationModel",
    "IExplainableModel",
    "IRegimeModel",
    "ISurvivalModel",
    "IDataStoreReader",
    "IDataStoreWriter",
]
