"""
backtest/core/validation_classifier.py

Classifies backtest runs into validation categories based on test period,
data quality, and configuration.

Categories:
- 'valid': Standard 2009-04-01 to 2025+ period (touches recent years)
- 'alternative_period': Other substantial periods (>1 year) not touching 2025+
- 'flagged': Valid but with significant data gaps (>50)
- 'invalid': Leverage, very short period, or missing metrics
"""

from datetime import datetime
from typing import Any, Optional, Tuple


def classify_run(
    start_date: str,
    end_date: str,
    data_gaps_count: int,
    config: dict[str, Any],
) -> Tuple[bool, str, Optional[str]]:
    """Classify a backtest run into a validation category.

    Returns:
        Tuple of (is_valid, validation_status, marked_invalid_reason)
        where is_valid=True unless marked_invalid_reason is set.
    """

    # Parse dates
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return False, "invalid", f"Invalid date format: {start_date} to {end_date}"

    # Check for leverage (config contains leverage settings)
    if config.get("use_leverage") or config.get("margin") or config.get("leverage_ratio"):
        return False, "invalid", "Run uses leverage (not paper-trading safe)"

    # Check for very short periods (<180 days = ~6 months)
    period_days = (end - start).days
    if period_days < 180:
        return False, "invalid", f"Period too short ({period_days} days, minimum 180)"

    # Substantial period is >1 year (365 days)
    is_substantial = period_days >= 365

    # Check if touches recent years (2025 or 2026)
    touches_recent = end.year >= 2025

    # Check if standard period (2009-04-01 to 2025+)
    STANDARD_START = datetime.strptime("2009-04-01", "%Y-%m-%d").date()
    is_standard_start = start <= STANDARD_START
    is_standard_period = is_standard_start and touches_recent

    # Determine validation status
    if not is_substantial:
        # Very short period that doesn't meet minimum
        return False, "invalid", f"Period too short for analysis ({period_days} days)"

    if is_standard_period:
        # Standard 2009-2025+ period
        if data_gaps_count > 50:
            # Standard period but with data gaps
            return True, "flagged", None
        else:
            # Standard period, clean data
            return True, "valid", None
    else:
        # Non-standard period
        if not touches_recent:
            # Substantial period but doesn't touch recent years (alternative period)
            return True, "alternative_period", None
        else:
            # Touches recent but not from standard start
            if data_gaps_count > 50:
                return True, "flagged", None
            else:
                return True, "alternative_period", None
