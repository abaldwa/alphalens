"""
contracts/interfaces.py

Phase: 0.1 (Project Skeleton)
Specs: SPEC-PIPE-001, SPEC-PIPE-002, SPEC-MODEL-001, SPEC-MODEL-002, SPEC-MODEL-003,
       SPEC-DS-001, SPEC-DS-003, SPEC-DS-004, SPEC-DS-005
Owner: Platform / Architecture
Consumers: systems/ml_signal_engine, systems/fundamental_analysis, systems/technical_analysis,
           backtest, ingestion/quality, features/registry

Abstract base interfaces for all pluggable components in AlphaLens.
Defines contracts for models, data access, and explainability layers.
SOLID: Interface Segregation Principle — lean, focused contracts only.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


class IModel(ABC):
    """
    SPEC-MODEL-001: Core Model Interface
    All trained models must implement IModel to enable pluggable retraining and inference.
    Supports serialization/deserialization for reproducibility (SPEC-SEC-002).
    """

    @abstractmethod
    def train(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        sample_weight: Optional[pd.Series] = None,
    ) -> None:
        """
        Train the model.

        Args:
            X: Feature matrix (n_samples, n_features)
            y: Target vector (n_samples,)
            sample_weight: Optional weights per sample

        Raises:
            ValueError: If X, y shapes are incompatible or sample_weight invalid
        """

    @abstractmethod
    def predict(self, X: pd.DataFrame) -> pd.Series:
        """
        Generate predictions.

        Args:
            X: Feature matrix (n_samples, n_features)

        Returns:
            Predictions matching input row order; type depends on model
        """

    @abstractmethod
    def save(self, path: str) -> None:
        """
        Serialize model to disk (SPEC-SEC-002: reproducibility).

        Args:
            path: Destination file path (parent directory must exist)

        Raises:
            IOError: If write fails
        """

    @abstractmethod
    def load(self, path: str) -> None:
        """
        Load model from disk.

        Args:
            path: Source file path

        Raises:
            FileNotFoundError: If file does not exist
            ValueError: If file format is invalid
        """

    @abstractmethod
    def metadata(self) -> Dict[str, Any]:
        """
        Return model metadata for logging and versioning.

        Returns:
            Dict with keys: 'name', 'version', 'created_at', 'features_count',
            'hyperparams' (optional), 'training_samples' (optional)
        """


class IClassificationModel(IModel):
    """
    SPEC-MODEL-001: Classification Model Interface
    For models predicting discrete labels (e.g., BUY/HOLD/SELL).
    """

    @abstractmethod
    def predict_proba(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Generate probability predictions for each class.

        Args:
            X: Feature matrix (n_samples, n_features)

        Returns:
            DataFrame with shape (n_samples, n_classes), each column is a class probability
            Column names must match class labels from training
        """


class IExplainableModel(IModel):
    """
    SPEC-PIPE-002: Model Explainability (SHAP values)
    For models generating per-sample explanations (SPEC-MODEL-002).
    """

    @abstractmethod
    def get_shap_values(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Compute SHAP values for each feature (feature attribution).

        Args:
            X: Feature matrix (n_samples, n_features)

        Returns:
            DataFrame with shape (n_samples, n_features), same column order as X
            Each cell is the SHAP value (contribution) for that feature-sample pair
        """


class IRegimeModel(ABC):
    """
    SPEC-MODEL-003: Market Regime Classification
    Identifies market conditions (bull/bear/consolidation) for position sizing adjustments.
    """

    @abstractmethod
    def fit(self, X: pd.DataFrame) -> None:
        """
        Fit regime detector to historical data.

        Args:
            X: Feature matrix (typically market returns, vol, breadth indicators)
        """

    @abstractmethod
    def predict_regime(self, X: pd.DataFrame) -> Tuple[pd.Series, Optional[pd.DataFrame]]:
        """
        Predict current market regime.

        Args:
            X: Recent market data (typically last 20-60 observations)

        Returns:
            Tuple of (regimes, probabilities) where:
            - regimes: Series of regime labels (e.g., 'BULL', 'BEAR', 'NEUTRAL')
            - probabilities: Optional DataFrame of regime probabilities per sample (n_samples, n_regimes)
        """


class ISurvivalModel(IModel):
    """
    SPEC-MODEL-003: Survival Analysis (time-to-event)
    For predicting how long a position remains profitable / when to exit.
    """

    @abstractmethod
    def predict_survival(self, X: pd.DataFrame, time_horizon_days: int = 20) -> pd.DataFrame:
        """
        Predict survival probability (continuation of profitability) over time horizon.

        Args:
            X: Feature matrix
            time_horizon_days: Number of days to forecast

        Returns:
            DataFrame with shape (n_samples, time_horizon_days)
            Each column t is the survival probability at day t
        """


class IDataStoreReader(ABC):
    """
    SPEC-DS-001, SPEC-DS-003: Data Store Read Interface
    Abstracts querying across multiple data stores (raw, normalised, features, signals).
    Enforces PIT logic and data staleness checks.
    """

    @abstractmethod
    def query_ohlcv(
        self,
        ticker: str,
        start_date: datetime,
        end_date: datetime,
        as_of: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """
        Query OHLCV (open, high, low, close, volume) for a ticker.

        SPEC-DS-003: Point-in-time correctness — returns only data observable as_of.

        Args:
            ticker: Stock ticker (e.g., 'RELIANCE')
            start_date: Inclusive start date
            end_date: Inclusive end date
            as_of: Reference date for PIT logic (None = use end_date)

        Returns:
            DataFrame columns: [date, open, high, low, close, volume, adjusted_close]
            Index: date (DatetimeIndex, sorted ascending)
        """

    @abstractmethod
    def query_fundamentals(
        self,
        ticker: str,
        start_date: datetime,
        end_date: datetime,
        as_of: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """
        Query fundamental data (earnings, margins, shareholding, etc).

        SPEC-DS-003: PIT logic — returns only fundamentals publicly known as_of date
        (e.g., Q1 results known after announcement_date, not fiscal year end).

        Args:
            ticker: Stock ticker
            start_date: Inclusive start date (for results period, not announcement date)
            end_date: Inclusive end date
            as_of: Reference date for PIT filtering

        Returns:
            DataFrame columns: [fiscal_year_quarter, announcement_date, eps, pe_ratio, roe,
                                debt_to_equity, promoter_holding_pct, ...]
            May have multiple rows per ticker if many fundamentals are tracked.
        """

    @abstractmethod
    def query_features(
        self,
        ticker: str,
        start_date: datetime,
        end_date: datetime,
        feature_names: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Query precomputed features for ML models.

        Args:
            ticker: Stock ticker
            start_date: Inclusive start date
            end_date: Inclusive end date
            feature_names: If provided, return only these columns (else return all)

        Returns:
            DataFrame columns: [date, feature_1, feature_2, ..., feature_n]
            Index: date (DatetimeIndex)
            Includes data_staleness_flag column (SPEC-SYS-003)
        """


class IDataStoreWriter(ABC):
    """
    SPEC-DS-001, SPEC-DS-004, SPEC-DS-005: Data Store Write Interface
    Abstracts writing signals, models, and outputs across all stores.
    """

    @abstractmethod
    def write_signals(
        self,
        ticker: str,
        date: datetime,
        signal_name: str,
        signal_value: float,
        model_version: str,
        probability: Optional[float] = None,
        shap_values: Optional[Dict[str, float]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Write ML signal (BUY/SELL/HOLD prediction) for a ticker-date pair.

        Args:
            ticker: Stock ticker
            date: Date of signal
            signal_name: Signal model identifier (e.g., 'ml_classifier_v2')
            signal_value: Signal magnitude (e.g., 0.85 for 85% buy probability)
            model_version: Semantic version of model (e.g., '2.1.0')
            probability: Optional confidence/probability of prediction
            shap_values: Optional per-feature importance dict
            metadata: Optional extra info (e.g., training_date, latency_ms)

        Raises:
            IOError: If write fails
        """

    @abstractmethod
    def write_model(
        self,
        model_name: str,
        model_version: str,
        model_type: str,
        created_at: datetime,
        features_used: List[str],
        accuracy_on_validation: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Register trained model in model registry.

        Args:
            model_name: Human-readable name (e.g., 'ClassificationModel')
            model_version: Semantic version (e.g., '2.1.0')
            model_type: Type identifier (e.g., 'xgboost_classifier', 'survival_model')
            created_at: Training completion timestamp
            features_used: List of feature names used in model
            accuracy_on_validation: Performance metric (if applicable)
            metadata: Extra info (hyperparams, training_time, training_samples, etc)

        Raises:
            IOError: If write fails
        """

    @abstractmethod
    def write_output(
        self,
        output_type: str,
        date: datetime,
        content: Dict[str, Any],
    ) -> None:
        """
        Write analysis output (e.g., portfolio, backtest results, reports).

        Args:
            output_type: Type of output (e.g., 'portfolio_state', 'backtest_summary')
            date: Date of output
            content: Serializable dict with output data

        Raises:
            IOError: If write fails
        """
