"""
features/registry.py

Phase: 0.1 (Project Skeleton)
Specs: SPEC-PIPE-004, SPEC-FEAT-001, SPEC-DS-006, SPEC-QUALITY-003
Owner: Platform / Features
Consumers: systems/ml_signal_engine, features/*, datastore/api, backtest

Feature discovery and documentation registry.
Single source of truth for all ~76 technical indicators and their metadata.
Enforces PIT awareness and staleness tracking for each feature (SPEC-DS-003).
SOLID: Single Responsibility — registry only; no computation here.
"""

import json
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


class FeatureCategory(str, Enum):
    """Feature classification for discovery and filtering."""

    PRICE_ACTION = "price_action"
    MOMENTUM = "momentum"
    VOLATILITY = "volatility"
    VOLUME_BASED = "volume_based"
    BREADTH = "breadth"
    SENTIMENT = "sentiment"
    MICROSTRUCTURE = "microstructure"
    FUNDAMENTAL = "fundamental"
    REGIME = "regime"
    TECHNICAL_COMPOSITE = "technical_composite"


class UpdateFrequency(str, Enum):
    """How often the feature is refreshed."""

    TICK = "tick"  # Real-time
    MINUTE = "minute"
    DAILY = "daily"
    WEEKLY = "weekly"
    QUARTERLY = "quarterly"
    ANNUAL = "annual"


class DataSource(str, Enum):
    """Where feature data comes from (for dependency tracking)."""

    OHLCV = "ohlcv"  # Price/volume from bhavcopy
    FUNDAMENTALS = "fundamentals"  # Quarterly results, shareholding
    DERIVED = "derived"  # Computed from other features
    EXTERNAL = "external"  # Third-party data


class PITRule(str, Enum):
    """Point-in-time enforcement strategy (SPEC-DS-003)."""

    NONE = "none"  # No PIT constraint (e.g., price is always knowable)
    ANNOUNCEMENT_DATE = "announcement_date"  # Known only after announcement
    FISCAL_QUARTER_END = "fiscal_quarter_end"  # Known only after quarter ends + delay
    FILING_DATE = "filing_date"  # Known only after regulatory filing
    MONTH_END_PLUS_DELAY = "month_end_plus_delay"  # Known end-of-month + N days


@dataclass
class FeatureDefinition:
    """
    SPEC-FEAT-001: Feature metadata.
    Defines a single feature's schema, update cadence, and consumers.
    """

    name: str  # Unique identifier (e.g., 'rsi_14', 'pe_trailing')
    category: FeatureCategory
    phase: int  # Which phase this feature is active (0-5)
    update_frequency: UpdateFrequency
    source_store: DataSource
    pit_rule: PITRule
    description: str = ""  # Human-readable explanation
    range: Tuple[float, float] = (0.0, 100.0)  # Expected min/max for sanity checks
    consumers: List[str] = field(default_factory=list)  # Systems that use this feature
    lookback_days: Optional[int] = None  # Historical depth needed
    computation_depends_on: List[str] = field(default_factory=list)  # Feature dependencies
    unit: str = ""  # Unit of measurement (%, ratio, INR, etc)
    staleness_flag_column: Optional[str] = None  # Column name if staleness tracked


# No unimplemented placeholder entries (CLAUDE.md Absolute Rule 6) — every
# entry below was a real, named feature at the time it was written. NOTE:
# this catalog predates most of features/matrix_builder.py's current
# ALL_FEATURE_COLUMNS naming (e.g. this registry's "rsi_14" matches, but
# "close_price"/"sma_20"/"macd" etc. don't — matrix_builder.py uses
# different current names/has no equivalent). Nothing in production
# imports features.registry, so this staleness isn't corrupting any live
# output, but the catalog itself is unreliable until reconciled with
# ALL_FEATURE_COLUMNS — don't trust it as a feature-discovery source yet.
FEATURE_REGISTRY: Dict[str, FeatureDefinition] = {
    # ===== PHASE 1 (Price Action) =====
    "close_price": FeatureDefinition(
        name="close_price",
        category=FeatureCategory.PRICE_ACTION,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.OHLCV,
        pit_rule=PITRule.NONE,
        description="Daily closing price (adjusted for splits/dividends)",
        range=(1.0, 100000.0),
        consumers=["ml_signal_engine"],
        lookback_days=252,
        unit="INR",
    ),
    "high_price": FeatureDefinition(
        name="high_price",
        category=FeatureCategory.PRICE_ACTION,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.OHLCV,
        pit_rule=PITRule.NONE,
        description="Daily high price",
        range=(1.0, 100000.0),
        consumers=["ml_signal_engine"],
        lookback_days=252,
        unit="INR",
    ),
    "low_price": FeatureDefinition(
        name="low_price",
        category=FeatureCategory.PRICE_ACTION,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.OHLCV,
        pit_rule=PITRule.NONE,
        description="Daily low price",
        range=(1.0, 100000.0),
        consumers=["ml_signal_engine"],
        lookback_days=252,
        unit="INR",
    ),
    "volume": FeatureDefinition(
        name="volume",
        category=FeatureCategory.VOLUME_BASED,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.OHLCV,
        pit_rule=PITRule.NONE,
        description="Daily trading volume",
        range=(0.0, 1000000000.0),
        consumers=["ml_signal_engine"],
        lookback_days=252,
        unit="shares",
    ),
    # ===== PHASE 1 (Momentum) =====
    "rsi_14": FeatureDefinition(
        name="rsi_14",
        category=FeatureCategory.MOMENTUM,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="Relative Strength Index (14-day)",
        range=(0.0, 100.0),
        consumers=["ml_signal_engine"],
        lookback_days=20,
        unit="%",
    ),
    "macd": FeatureDefinition(
        name="macd",
        category=FeatureCategory.MOMENTUM,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="MACD line (12/26 EMA)",
        range=(-1000.0, 1000.0),
        consumers=["ml_signal_engine"],
        lookback_days=30,
        computation_depends_on=["close_price"],
    ),
    "macd_signal": FeatureDefinition(
        name="macd_signal",
        category=FeatureCategory.MOMENTUM,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="MACD signal line (9 EMA of MACD)",
        range=(-1000.0, 1000.0),
        consumers=["ml_signal_engine"],
        lookback_days=30,
        computation_depends_on=["macd"],
    ),
    "macd_histogram": FeatureDefinition(
        name="macd_histogram",
        category=FeatureCategory.MOMENTUM,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="MACD histogram (MACD - Signal)",
        range=(-1000.0, 1000.0),
        consumers=["ml_signal_engine"],
        lookback_days=30,
        computation_depends_on=["macd", "macd_signal"],
    ),
    # ===== PHASE 1 (Volatility) =====
    "atr_14": FeatureDefinition(
        name="atr_14",
        category=FeatureCategory.VOLATILITY,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="Average True Range (14-day)",
        range=(0.0, 10000.0),
        consumers=["ml_signal_engine"],
        lookback_days=20,
        unit="INR",
    ),
    "bollinger_upper": FeatureDefinition(
        name="bollinger_upper",
        category=FeatureCategory.VOLATILITY,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="Bollinger Band upper (20 SMA + 2 SD)",
        range=(1.0, 100000.0),
        consumers=["ml_signal_engine"],
        lookback_days=25,
        computation_depends_on=["close_price"],
        unit="INR",
    ),
    "bollinger_lower": FeatureDefinition(
        name="bollinger_lower",
        category=FeatureCategory.VOLATILITY,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="Bollinger Band lower (20 SMA - 2 SD)",
        range=(1.0, 100000.0),
        consumers=["ml_signal_engine"],
        lookback_days=25,
        computation_depends_on=["close_price"],
        unit="INR",
    ),
    "bollinger_mid": FeatureDefinition(
        name="bollinger_mid",
        category=FeatureCategory.VOLATILITY,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="Bollinger Band middle (20 SMA)",
        range=(1.0, 100000.0),
        consumers=["ml_signal_engine"],
        lookback_days=25,
        computation_depends_on=["close_price"],
        unit="INR",
    ),
    "kc_upper": FeatureDefinition(
        name="kc_upper",
        category=FeatureCategory.VOLATILITY,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="Keltner Channel upper (20 EMA + 2 ATR)",
        range=(1.0, 100000.0),
        consumers=["ml_signal_engine"],
        lookback_days=25,
        computation_depends_on=["close_price", "atr_14"],
        unit="INR",
    ),
    "kc_lower": FeatureDefinition(
        name="kc_lower",
        category=FeatureCategory.VOLATILITY,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="Keltner Channel lower (20 EMA - 2 ATR)",
        range=(1.0, 100000.0),
        consumers=["ml_signal_engine"],
        lookback_days=25,
        computation_depends_on=["close_price", "atr_14"],
        unit="INR",
    ),
    "std_dev_20": FeatureDefinition(
        name="std_dev_20",
        category=FeatureCategory.VOLATILITY,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="Standard deviation of returns (20-day)",
        range=(0.0, 100.0),
        consumers=["ml_signal_engine"],
        lookback_days=25,
        computation_depends_on=["close_price"],
        unit="%",
    ),
    # ===== PHASE 1 (Returns & Ratios) =====
    "return_1d": FeatureDefinition(
        name="return_1d",
        category=FeatureCategory.PRICE_ACTION,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="1-day log return",
        range=(-1.0, 1.0),
        consumers=["ml_signal_engine"],
        lookback_days=5,
        computation_depends_on=["close_price"],
        unit="%",
    ),
    "return_5d": FeatureDefinition(
        name="return_5d",
        category=FeatureCategory.PRICE_ACTION,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="5-day log return",
        range=(-1.0, 1.0),
        consumers=["ml_signal_engine"],
        lookback_days=10,
        computation_depends_on=["close_price"],
        unit="%",
    ),
    "return_20d": FeatureDefinition(
        name="return_20d",
        category=FeatureCategory.PRICE_ACTION,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="20-day log return",
        range=(-1.0, 1.0),
        consumers=["ml_signal_engine"],
        lookback_days=30,
        computation_depends_on=["close_price"],
        unit="%",
    ),
    "return_60d": FeatureDefinition(
        name="return_60d",
        category=FeatureCategory.MOMENTUM,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="60-day log return",
        range=(-1.0, 1.0),
        consumers=["ml_signal_engine"],
        lookback_days=70,
        computation_depends_on=["close_price"],
        unit="%",
    ),
    # ===== PHASE 1 (Volume-Based) =====
    "obv": FeatureDefinition(
        name="obv",
        category=FeatureCategory.VOLUME_BASED,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="On-Balance Volume",
        range=(-1000000000.0, 1000000000.0),
        consumers=["ml_signal_engine"],
        lookback_days=30,
        computation_depends_on=["close_price", "volume"],
    ),
    "obv_ema": FeatureDefinition(
        name="obv_ema",
        category=FeatureCategory.VOLUME_BASED,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="OBV EMA (20-day)",
        range=(-1000000000.0, 1000000000.0),
        consumers=["ml_signal_engine"],
        lookback_days=30,
        computation_depends_on=["obv"],
    ),
    "volume_sma_20": FeatureDefinition(
        name="volume_sma_20",
        category=FeatureCategory.VOLUME_BASED,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="Average daily volume (20-day SMA)",
        range=(0.0, 1000000000.0),
        consumers=["ml_signal_engine"],
        lookback_days=25,
        computation_depends_on=["volume"],
        unit="shares",
    ),
    "volume_ratio": FeatureDefinition(
        name="volume_ratio",
        category=FeatureCategory.VOLUME_BASED,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="Current volume / 20-day average volume",
        range=(0.0, 10.0),
        consumers=["ml_signal_engine"],
        lookback_days=25,
        computation_depends_on=["volume", "volume_sma_20"],
        unit="ratio",
    ),
    # ===== PHASE 1 (Oscillators) =====
    "stochastic_k": FeatureDefinition(
        name="stochastic_k",
        category=FeatureCategory.MOMENTUM,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="Stochastic %K (14/3)",
        range=(0.0, 100.0),
        consumers=["ml_signal_engine"],
        lookback_days=20,
        computation_depends_on=["high_price", "low_price", "close_price"],
        unit="%",
    ),
    "stochastic_d": FeatureDefinition(
        name="stochastic_d",
        category=FeatureCategory.MOMENTUM,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="Stochastic %D (3 SMA of %K)",
        range=(0.0, 100.0),
        consumers=["ml_signal_engine"],
        lookback_days=20,
        computation_depends_on=["stochastic_k"],
        unit="%",
    ),
    "williams_r": FeatureDefinition(
        name="williams_r",
        category=FeatureCategory.MOMENTUM,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="Williams %R (14-day)",
        range=(-100.0, 0.0),
        consumers=["ml_signal_engine"],
        lookback_days=20,
        computation_depends_on=["high_price", "low_price", "close_price"],
        unit="%",
    ),
    # ===== PHASE 1 (Moving Averages) =====
    "sma_20": FeatureDefinition(
        name="sma_20",
        category=FeatureCategory.PRICE_ACTION,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="Simple Moving Average (20-day)",
        range=(1.0, 100000.0),
        consumers=["ml_signal_engine"],
        lookback_days=25,
        computation_depends_on=["close_price"],
        unit="INR",
    ),
    "sma_50": FeatureDefinition(
        name="sma_50",
        category=FeatureCategory.PRICE_ACTION,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="Simple Moving Average (50-day)",
        range=(1.0, 100000.0),
        consumers=["ml_signal_engine"],
        lookback_days=60,
        computation_depends_on=["close_price"],
        unit="INR",
    ),
    "sma_200": FeatureDefinition(
        name="sma_200",
        category=FeatureCategory.PRICE_ACTION,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="Simple Moving Average (200-day)",
        range=(1.0, 100000.0),
        consumers=["ml_signal_engine"],
        lookback_days=210,
        computation_depends_on=["close_price"],
        unit="INR",
    ),
    "ema_12": FeatureDefinition(
        name="ema_12",
        category=FeatureCategory.PRICE_ACTION,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="Exponential Moving Average (12-day)",
        range=(1.0, 100000.0),
        consumers=["ml_signal_engine"],
        lookback_days=20,
        computation_depends_on=["close_price"],
        unit="INR",
    ),
    "ema_26": FeatureDefinition(
        name="ema_26",
        category=FeatureCategory.PRICE_ACTION,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="Exponential Moving Average (26-day)",
        range=(1.0, 100000.0),
        consumers=["ml_signal_engine"],
        lookback_days=35,
        computation_depends_on=["close_price"],
        unit="INR",
    ),
    # ===== PHASE 1 (Trend & Support/Resistance) =====
    "sma_20_distance": FeatureDefinition(
        name="sma_20_distance",
        category=FeatureCategory.PRICE_ACTION,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="Distance to 20-day SMA (% deviation)",
        range=(-100.0, 100.0),
        consumers=["ml_signal_engine"],
        lookback_days=25,
        computation_depends_on=["close_price", "sma_20"],
        unit="%",
    ),
    "sma_200_distance": FeatureDefinition(
        name="sma_200_distance",
        category=FeatureCategory.PRICE_ACTION,
        phase=1,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="Distance to 200-day SMA (% deviation)",
        range=(-100.0, 100.0),
        consumers=["ml_signal_engine"],
        lookback_days=210,
        computation_depends_on=["close_price", "sma_200"],
        unit="%",
    ),
    # ===== PHASE 2 (Additional Momentum/Oscillators) =====
    "cci_20": FeatureDefinition(
        name="cci_20",
        category=FeatureCategory.MOMENTUM,
        phase=2,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="Commodity Channel Index (20-day)",
        range=(-500.0, 500.0),
        consumers=["ml_signal_engine"],
        lookback_days=25,
        computation_depends_on=["high_price", "low_price", "close_price"],
    ),
    "adx": FeatureDefinition(
        name="adx",
        category=FeatureCategory.MOMENTUM,
        phase=2,
        update_frequency=UpdateFrequency.DAILY,
        source_store=DataSource.DERIVED,
        pit_rule=PITRule.NONE,
        description="Average Directional Index (14-day)",
        range=(0.0, 100.0),
        consumers=["ml_signal_engine"],
        lookback_days=20,
        computation_depends_on=["high_price", "low_price", "close_price"],
        unit="points",
    ),
    # ===== PHASE 2 (Fundamental Ratios) =====
    "pe_ratio": FeatureDefinition(
        name="pe_ratio",
        category=FeatureCategory.FUNDAMENTAL,
        phase=2,
        update_frequency=UpdateFrequency.QUARTERLY,
        source_store=DataSource.FUNDAMENTALS,
        pit_rule=PITRule.ANNOUNCEMENT_DATE,
        description="Price-to-Earnings ratio (trailing 12 months)",
        range=(0.0, 1000.0),
        consumers=["ml_signal_engine"],
        unit="ratio",
    ),
    "pb_ratio": FeatureDefinition(
        name="pb_ratio",
        category=FeatureCategory.FUNDAMENTAL,
        phase=2,
        update_frequency=UpdateFrequency.QUARTERLY,
        source_store=DataSource.FUNDAMENTALS,
        pit_rule=PITRule.ANNOUNCEMENT_DATE,
        description="Price-to-Book ratio",
        range=(0.0, 100.0),
        consumers=["ml_signal_engine"],
        unit="ratio",
    ),
    "roe": FeatureDefinition(
        name="roe",
        category=FeatureCategory.FUNDAMENTAL,
        phase=2,
        update_frequency=UpdateFrequency.QUARTERLY,
        source_store=DataSource.FUNDAMENTALS,
        pit_rule=PITRule.ANNOUNCEMENT_DATE,
        description="Return on Equity",
        range=(-100.0, 100.0),
        consumers=["ml_signal_engine"],
        unit="%",
    ),
    "debt_to_equity": FeatureDefinition(
        name="debt_to_equity",
        category=FeatureCategory.FUNDAMENTAL,
        phase=2,
        update_frequency=UpdateFrequency.QUARTERLY,
        source_store=DataSource.FUNDAMENTALS,
        pit_rule=PITRule.ANNOUNCEMENT_DATE,
        description="Debt-to-Equity ratio",
        range=(0.0, 100.0),
        consumers=["ml_signal_engine"],
        unit="ratio",
    ),
    "promoter_holding": FeatureDefinition(
        name="promoter_holding",
        category=FeatureCategory.FUNDAMENTAL,
        phase=2,
        update_frequency=UpdateFrequency.QUARTERLY,
        source_store=DataSource.FUNDAMENTALS,
        pit_rule=PITRule.FILING_DATE,
        description="Promoter shareholding %",
        range=(0.0, 100.0),
        consumers=["ml_signal_engine"],
        unit="%",
    ),
}
# No breadth/sentiment/options-flow features (nifty_breadth, vix_proxy,
# put_call_ratio, FII/DII flow, option OI, etc.) are registered: none of
# them have a real computation wired into features/matrix_builder.py or a
# real external data source ingested yet (CLAUDE.md Absolute Rule 6 — a
# registry entry with no backing computation is itself a stub). Add the
# entry here only once an ingestion path + matrix_builder.py column exists
# for it.


def validate_feature_registry() -> List[str]:
    """
    Validate registry consistency (SPEC-QUALITY-003).

    Returns:
        List of validation errors (empty if all valid)
    """
    errors = []

    for name, defn in FEATURE_REGISTRY.items():
        # Check name consistency
        if defn.name != name:
            errors.append(f"Feature '{name}': registry key != definition.name")

        # Check phase is valid (0-5)
        if not 0 <= defn.phase <= 5:
            errors.append(f"Feature '{name}': invalid phase {defn.phase}")

        # Check dependencies exist
        for dep in defn.computation_depends_on:
            if dep not in FEATURE_REGISTRY:
                errors.append(f"Feature '{name}': dependency '{dep}' not in registry")

        # Warn if no consumers
        if not defn.consumers:
            errors.append(f"Feature '{name}': no consumers registered")

    return errors


def export_feature_catalog(output_path: Optional[Path] = None) -> Dict[str, Any]:
    """
    Export registry to JSON for external discovery (SPEC-FEAT-001).

    SPEC-DS-006: Feature catalog is queryable by consumers (dashboards, docs).

    Args:
        output_path: If provided, write JSON to this path

    Returns:
        Dict suitable for JSON serialization
    """
    catalog: Dict[str, Any] = {
        "version": "0.1",
        "generated_at": None,  # Will be filled by ingestion layer
        "total_features": len(FEATURE_REGISTRY),
        "features": {},
    }

    for name, defn in FEATURE_REGISTRY.items():
        catalog["features"][name] = asdict(defn)

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(catalog, f, indent=2, default=str)

    return catalog
