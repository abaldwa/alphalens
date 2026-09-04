"""
Bollinger Band %B Signal — R13's ranking, a signal genuinely distinct
from the shared TrailingMomentumSignal family (R01/R03/R07-R10/R12/R14-R17)
and from PctOf52WeekHighSignal (R11).

Ported from features/momentum_signal.py::bollinger_mean_reversion() —
same formula (SMA ± 2·STD over a 20-day window, %B position within the
band), reimplemented with plain pandas rolling functions instead of
TA-Lib, to avoid adding a talib build dependency to the framework. Values
are numerically identical: talib.BBANDS's matype=0 IS a simple moving
average, and its stddev is the standard (ddof=0-adjusted) population
formula — both reproduced exactly by pandas' .rolling().mean()/.std().
"""

from typing import Any, List, Optional

import pandas as pd

from momentum_framework.common.signals import MomentumSignal


class BollingerBandSignal(MomentumSignal):
    """
    %B position within a Bollinger Band: (close - lower_band) / (upper - lower).
    0 = at the lower band (oversold), 1 = at the upper band (overbought).
    Ascending sort selects the most oversold names — R13 buys the lowest.
    """

    def __init__(self, window: int = 20, num_std: float = 2.0):
        super().__init__("bollinger_mean_reversion")
        self.window = window
        self.num_std = num_std

    def compute(
        self,
        normalised_conn: Any,
        tickers: List[str],
        as_of_date: str,
        lookback_days: Optional[int] = None,
    ) -> pd.Series:
        """
        %B for each ticker as of as_of_date. A ticker with insufficient
        history (fewer than `window` closes) gets 0.5 (neutral) — matches
        the legacy convention (never exclude on missing, and an unratable
        ticker is not artificially oversold/overbought).
        """
        days = lookback_days or self.window
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
            list(tickers) + [as_of_date] + floor_params + [days],
        ).fetch_df()

        if df.empty:
            return pd.Series(0.5, index=tickers, dtype=float)

        def _pct_b(group: pd.DataFrame) -> Optional[float]:
            if len(group) < days:
                return 0.5
            closes = group["close"]
            sma = closes.mean()
            std = closes.std(ddof=0)
            upper = sma + self.num_std * std
            lower = sma - self.num_std * std
            band_range = max(upper - lower, 1e-6)
            position = (closes.iloc[-1] - lower) / band_range
            return float(min(max(position, 0.0), 1.0))

        result = df.groupby("ticker").apply(_pct_b, include_groups=False)
        # Tickers with no rows at all (no history) still get neutral 0.5,
        # not dropped — matches legacy's "never exclude on missing" rule.
        result = result.reindex(tickers).fillna(0.5)
        return result.astype(float)
