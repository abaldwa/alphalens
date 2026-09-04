"""
Momentum Signal Computation

Provides base class for momentum signal calculations.
Source: features/momentum_signal.py (adapted)
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import pandas as pd


class MomentumSignal(ABC):
    """Base class for momentum signal computation."""

    def __init__(self, name: str):
        self.name = name
        # Set by BacktestOrchestrator.run_native() to the backtest's
        # start_date, so native matches legacy's REAL behavior: legacy's
        # OHLCV fetch (backtest/core/ohlcv_prewarm.py::get_or_fetch_ohlcv_bulk)
        # is scoped strictly to [start_date, end_date] with no reach-back,
        # even though ohlcv_adjusted has data back to 2005 — every
        # published legacy campaign (e.g. queues/r1_full_2009_2026.json,
        # start_date="2009-01-01") has therefore always sat idle until its
        # longest lookback warms up INSIDE the window, never before it.
        # Confirmed 2026-09-04 (see project_parity_check_first_result_
        # divergence memory) — explicit user decision: native must
        # reproduce this, not "fix" it, so it's comparable to the real
        # 2,220-run legacy baseline rather than a hypothetical corrected
        # version of it. None (default) means unbounded — used by
        # anything not wired to a floor (tests, ad hoc scripts).
        self.floor_date: Optional[str] = None

    @abstractmethod
    def compute(
        self,
        normalised_conn: Any,
        tickers: List[str],
        as_of_date: str,
        lookback_days: int
    ) -> pd.Series:
        """
        Compute momentum signal for given tickers.

        Args:
            normalised_conn: DuckDB connection to ohlcv_adjusted table
            tickers: List of ticker symbols
            as_of_date: Reference date (YYYY-MM-DD)
            lookback_days: Number of trading days to look back

        Returns:
            pd.Series indexed by ticker with momentum scores (e.g., returns %)
        """
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"


class TrailingMomentumSignal(MomentumSignal):
    """
    Trailing price momentum: percentage return over lookback period.

    Formula: (close_t / close_t-N) - 1, where N = lookback_days

    THIS IS THE SHARED RANKING for rank_method="trailing_return" —
    verified 2026-09-04 against every legacy queue generator that sets
    that rank_method: R01, R03 (skip-month offset applied on top, see
    strategies/r03_jt_skipmonth.py), R07 (crash overlay), R08 (BSC
    vol-target), R09 (Moreira-Muir vol-scaling), and R14-R17 (weighting
    schemes) all rank their band's universe with this exact same signal.
    They differ ONLY in: skip_months (R03), an exposure/crash overlay
    applied after ranking (R07), a position-sizing leverage multiplier
    applied after ranking (R08/R09), or a weighting scheme applied after
    ranking (R14-R17). A strategy file must never reimplement trailing-
    return computation — instantiate this class instead, exactly as
    strategies/r01_trailing_momentum.py and strategies/base.py's
    WeightedMomentumStrategy already do.

    ALSO the shared ranking for rank_method="trailing_reversal_1mo" (R12)
    — confirmed 2026-09-04 against strategies/migrations/
    r12_momentum_reversal_liquidity.py's own docstring: "ranks the band's
    universe by 1-month reversal (low returns = strong reversal signal)
    and buys the top N." That is this SAME class instantiated with
    lookback_months=1, with the caller selecting the LOWEST scores
    (ascending sort / buy the losers) instead of TrailingMomentumSignal's
    default highest-wins convention — not a different signal, only a
    different selection direction on identical output. R12's strategy
    file (not yet ported) should reuse this class, not
    backtest.reversal_selector.select_losers_for_reversal()'s legacy
    duplicate of the same math.

    R10 (industry_momentum), R11 (pct_of_52wk_high), and R13
    (bollinger_mean_reversion) use DIFFERENT signals entirely — see their
    own MomentumSignal subclasses below / to be added.
    """

    TRADING_DAYS_PER_MONTH = 21

    def __init__(self, lookback_months: int):
        self.lookback_months = lookback_months
        self.lookback_days = lookback_months * self.TRADING_DAYS_PER_MONTH
        super().__init__(f"trailing_momentum_{lookback_months}mo")

    def compute(
        self,
        normalised_conn: Any,
        tickers: List[str],
        as_of_date: str,
        lookback_days: Optional[int] = None
    ) -> pd.Series:
        """Compute trailing momentum for tickers."""
        days = lookback_days or self.lookback_days

        if not tickers:
            return pd.Series(dtype=float)

        placeholders = ",".join("?" for _ in tickers)
        floor_clause = " AND date >= ?" if self.floor_date else ""
        floor_params = [self.floor_date] if self.floor_date else []
        df = normalised_conn.execute(
            f"""
            SELECT ticker, date, close
            FROM (
                SELECT ticker, date, close,
                       ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
                FROM ohlcv_adjusted
                WHERE ticker IN ({placeholders}) AND date <= ?{floor_clause}
            )
            WHERE rn <= ?
            ORDER BY ticker, date
            """,
            list(tickers) + [as_of_date] + floor_params + [days + 1],
        ).fetch_df()

        if df.empty:
            return pd.Series(dtype=float)

        def _return(group: pd.DataFrame) -> Optional[float]:
            if len(group) < days + 1:
                return None
            start_close = group.iloc[0]["close"]
            end_close = group.iloc[-1]["close"]
            if start_close is None or start_close == 0:
                return None
            return float((end_close / start_close) - 1.0)

        returns = df.groupby("ticker").apply(_return, include_groups=False)
        # groupby().apply() degrades to an empty DataFrame (not a Series)
        # when EVERY group's _return() returns None — e.g. right after
        # floor_date, when no ticker yet has `days` of history inside the
        # window. Coerce explicitly rather than let that shape leak to
        # callers expecting a Series (.sort_values() etc. would break).
        if isinstance(returns, pd.DataFrame):
            return pd.Series(dtype=float)
        returns = returns.dropna()
        return returns.astype(float)


class PctOf52WeekHighSignal(MomentumSignal):
    """
    52-week high proximity signal: % of 52-week high.

    Formula: close_t / max(close_t-252), values in [0, 1]
    High values = near highs (winners), Low values = far from highs (losers/reversal targets)
    """

    def __init__(self) -> None:
        super().__init__("pct_of_52wk_high")

    def compute(
        self,
        normalised_conn: Any,
        tickers: List[str],
        as_of_date: str,
        lookback_days: int = 252
    ) -> pd.Series:
        """Compute 52-week high proximity for tickers."""
        if not tickers:
            return pd.Series(dtype=float)

        placeholders = ",".join("?" for _ in tickers)
        floor_clause = " AND date >= ?" if self.floor_date else ""
        floor_params = [self.floor_date] if self.floor_date else []
        df = normalised_conn.execute(
            f"""
            SELECT ticker, date, close,
                   MAX(close) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN {lookback_days} PRECEDING AND CURRENT ROW) as high_52w
            FROM ohlcv_adjusted
            WHERE ticker IN ({placeholders}) AND date <= ?{floor_clause}
            ORDER BY ticker, date DESC
            LIMIT {len(tickers)}
            """,
            list(tickers) + [as_of_date] + floor_params,
        ).fetch_df()

        if df.empty:
            return pd.Series(dtype=float)

        def _pct_of_high(group: pd.DataFrame) -> Optional[float]:
            if group.empty or group.iloc[0]["high_52w"] is None:
                return None
            return float(group.iloc[0]["close"] / group.iloc[0]["high_52w"])

        pcts = df.groupby("ticker").apply(_pct_of_high, include_groups=False)
        if isinstance(pcts, pd.DataFrame):  # see TrailingMomentumSignal.compute()'s comment on this degradation
            return pd.Series(dtype=float)
        pcts = pcts.dropna()
        return pcts.astype(float)


class IndustryMomentumSignal(MomentumSignal):
    """
    Sector/Industry-level momentum (R10, Nigam-Pandey style): two-stage
    ranking on top of the SAME TrailingMomentumSignal every other
    trailing_return strategy uses (see this module's TrailingMomentumSignal
    docstring) — (1) rank every ticker by trailing return, (2) average by
    sector to rank sectors, (3) keep only tickers in the top_sectors
    sectors. NOT a different per-ticker formula from R01/R03/etc — a
    sector filter layered on the identical computation.

    `sector_lookup` (ticker -> sector name): if not supplied at
    construction, auto-resolved from `stock_master.sector` (see
    common/sector_data.py — 100% coverage verified 2026-09-04) on first
    `compute()` call, scoped to whatever `tickers` is passed then, and
    cached for the life of this instance. Pass it explicitly only to
    override (e.g. a point-in-time mapping, when one exists).
    """

    def __init__(self, lookback_months: int, sector_lookup: Optional[Dict[str, str]] = None, top_sectors: int = 5):
        super().__init__("industry_momentum")
        self.sector_lookup = sector_lookup  # None => auto-resolve lazily in compute()
        self.top_sectors = top_sectors
        self._trailing = TrailingMomentumSignal(lookback_months=lookback_months)

    def compute(
        self,
        normalised_conn: Any,
        tickers: List[str],
        as_of_date: str,
        lookback_days: Optional[int] = None,
    ) -> pd.Series:
        """
        Two-stage: (1) trailing_return for every ticker, (2) filter to
        constituents of the top_sectors sectors by average score. Returns
        the FILTERED per-ticker scores (not sector-level scores) — the
        caller still does top_n selection on what this returns, same as
        every other rank_method.
        """
        from momentum_framework.common.sector_ranking import (
            rank_constituents_within_sectors,
            rank_sectors,
        )

        if self.sector_lookup is None:
            # Loads the FULL stock_master table (not scoped to this call's
            # `tickers`) — the band's universe can drift over a 17-year
            # backtest (companies entering/leaving by market-cap rank), and
            # scoping to only the first rebalance's tickers would silently
            # miss sectors for names that enter the band later. stock_master
            # is ~1,626 rows; loading it once per instance is cheap.
            from momentum_framework.common.sector_data import load_sector_lookup
            self.sector_lookup = load_sector_lookup(normalised_conn)

        self._trailing.floor_date = self.floor_date  # propagate — see MomentumSignal.floor_date docstring
        momentum = self._trailing.compute(normalised_conn, tickers, as_of_date, lookback_days)
        if momentum.empty or not self.sector_lookup:
            return momentum

        sector_scores = rank_sectors(momentum, self.sector_lookup, self.top_sectors)
        if sector_scores.empty:
            return pd.Series(dtype=float)

        top_sectors_list = sector_scores.head(self.top_sectors).index.tolist()
        return rank_constituents_within_sectors(momentum, self.sector_lookup, top_sectors_list)
