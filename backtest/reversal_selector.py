"""
backtest/reversal_selector.py

Utility functions for reversal / mean-reversion strategy selection.
Handles the key difference: reversal strategies buy LOSERS (lowest returns)
not WINNERS (highest returns).

Usage:
  from backtest.reversal_selector import select_losers_for_reversal
  target = select_losers_for_reversal(momentum_series, top_n=15)
"""

import logging
from typing import Optional, Set

import pandas as pd

logger = logging.getLogger(__name__)


def select_losers_for_reversal(
    momentum_or_returns: pd.Series,
    top_n: int,
    strategy_name: str = "reversal",
) -> Set[str]:
    """
    Select LOSERS (lowest recent returns) for mean-reversion strategies.

    Args:
        momentum_or_returns: pd.Series indexed by ticker with 1-month trailing returns
        top_n: Number of losers to select
        strategy_name: Name for logging (e.g., "R11", "R12_reversal")

    Returns:
        Set of tickers representing the top N losers (lowest returns)

    Example:
        >>> returns = pd.Series({'STOCK_A': -0.05, 'STOCK_B': 0.10, 'STOCK_C': -0.02})
        >>> losers = select_losers_for_reversal(returns, top_n=2)
        >>> losers
        {'STOCK_A', 'STOCK_C'}  # Two worst performers
    """
    if momentum_or_returns.empty:
        return set()

    # *** KEY: Sort ASCENDING to get LOWEST returns (losers) ***
    losers = set(momentum_or_returns.sort_values(ascending=True).head(top_n).index)
    logger.debug(f"{strategy_name}: selected {len(losers)} losers (lowest {top_n} returns)")
    return losers


def select_winners_for_momentum(
    momentum_or_returns: pd.Series,
    top_n: int,
    strategy_name: str = "momentum",
) -> Set[str]:
    """
    Select WINNERS (highest recent returns) for momentum strategies.

    Args:
        momentum_or_returns: pd.Series indexed by ticker with 1-month trailing returns
        top_n: Number of winners to select
        strategy_name: Name for logging (e.g., "R1", "R3")

    Returns:
        Set of tickers representing the top N winners (highest returns)

    Example:
        >>> returns = pd.Series({'STOCK_A': -0.05, 'STOCK_B': 0.10, 'STOCK_C': 0.02})
        >>> winners = select_winners_for_momentum(returns, top_n=2)
        >>> winners
        {'STOCK_B', 'STOCK_C'}  # Two best performers
    """
    if momentum_or_returns.empty:
        return set()

    # Sort DESCENDING to get HIGHEST returns (winners)
    winners = set(momentum_or_returns.sort_values(ascending=False).head(top_n).index)
    logger.debug(f"{strategy_name}: selected {len(winners)} winners (highest {top_n} returns)")
    return winners


def get_sort_order_for_rank_method(rank_method: str) -> bool:
    """
    Determine sort order (ascending) based on rank method.

    Args:
        rank_method: e.g., "trailing_reversal_1mo", "trailing_return", "pct_of_52wk_high"

    Returns:
        True = ascending (for reversal, select losers)
        False = descending (for momentum, select winners)

    Convention:
        - Reversal methods: ascending=True (LOW is better)
        - Momentum methods: ascending=False (HIGH is better)
    """
    reversal_methods = {
        "trailing_reversal_1mo",
        "pct_of_52wk_high",  # B-029: R11 mean-reversion: select stocks FAR from 52wk highs (lowest scores)
    }

    is_reversal = rank_method in reversal_methods
    logger.debug(f"Sort order for {rank_method}: ascending={is_reversal} (reversal={is_reversal})")
    return is_reversal


def select_by_rank_method(
    momentum_or_returns: pd.Series,
    top_n: int,
    rank_method: str,
) -> Set[str]:
    """
    Unified selector: automatically chooses loser or winner selection based on rank_method.

    Args:
        momentum_or_returns: pd.Series indexed by ticker with returns
        top_n: Number to select
        rank_method: e.g., "trailing_reversal_1mo" or "trailing_return"

    Returns:
        Set of tickers (losers for reversal, winners for momentum)
    """
    ascending = get_sort_order_for_rank_method(rank_method)

    if momentum_or_returns.empty:
        return set()

    selected = set(momentum_or_returns.sort_values(ascending=ascending).head(top_n).index)
    strategy_type = "reversal" if ascending else "momentum"
    logger.info(f"{rank_method} ({strategy_type}): selected {len(selected)} tickers (ascending={ascending})")
    return selected


def validate_reversal_strategy(
    rank_method: str,
    lookback_months: Optional[int] = None,
) -> None:
    """
    Validate reversal strategy configuration. Logs warnings if misconfigured.

    Args:
        rank_method: Should be "trailing_reversal_1mo"
        lookback_months: Should be 1 (for 1-month reversal)
    """
    if rank_method == "trailing_reversal_1mo":
        if lookback_months and lookback_months != 1:
            logger.warning(
                f"Reversal strategy {rank_method} typically uses 1-month lookback, "
                f"but got lookback_months={lookback_months}"
            )
        logger.info(f"✓ Reversal strategy validated: {rank_method}")
    else:
        logger.debug(f"Not a reversal strategy: {rank_method}")


# Optional type import (top of file didn't include)
