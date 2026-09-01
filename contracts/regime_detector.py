#!/usr/bin/env python3
"""
contracts/regime_detector.py

Regime detection for market-cap-specific strategies.

Supports multiple detection algorithms and market caps (Nifty 50, Next 50, 150, Smallcap 250, Microcap).
Outputs to temp files for walk-forward validation before DB commitment.

Algorithms:
1. EMA Crossover (current, basic)
2. RSI-based (momentum extremes)
3. Volatility-based (GARCH-like)
4. Combined (multi-factor ensemble)
"""

import logging
from datetime import date as date_type, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Any
import sys

import pandas as pd
import numpy as np
import duckdb

from contracts.interfaces import IRegimeDetector

project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

logger = logging.getLogger(__name__)

# Market cap band definitions
MARKET_CAP_BANDS = {
    'nifty_50': {'index': 'Nifty 50', 'ranks': (1, 50)},
    'nifty_next_50': {'index': 'Nifty Next 50', 'ranks': (51, 100)},
    'nifty_150': {'index': 'Nifty 150', 'ranks': (1, 150)},
    'nifty_smallcap_250': {'index': 'Nifty Smallcap 250', 'ranks': (151, 400)},
    'nifty_microcap': {'index': 'Nifty Microcap 250', 'ranks': (401, 650)},
}

# Regime classes
REGIMES = ['Bull', 'Bear', 'Choppy']


class RegimeDetector(IRegimeDetector):
    """Base regime detector class."""

    def __init__(self, market_cap_band: str) -> None:
        self.market_cap_band = market_cap_band
        band_config = MARKET_CAP_BANDS.get(market_cap_band)
        if not band_config:
            raise ValueError(f"Unknown market cap band: {market_cap_band}")
        self.band_config = band_config

    def load_index_ohlcv(
        self, start_date: date_type, end_date: date_type
    ) -> pd.DataFrame:
        """Load index OHLCV data from DuckDB."""
        db_path = Path(__file__).parent.parent / 'datastore/normalised/alphalens.duckdb'
        db = duckdb.connect(str(db_path), read_only=True)

        index_name = self.band_config['index']
        df = db.execute(
            """
            SELECT date, open, high, low, close, volume
            FROM index_ohlcv
            WHERE index_name = ?
            AND date >= ?
            AND date <= ?
            ORDER BY date
        """,
            [index_name, start_date, end_date],
        ).df()

        db.close()

        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        return df

    def detect(self, ohlcv_df: pd.DataFrame) -> pd.DataFrame:
        """Detect regime for each date. Override in subclasses."""
        raise NotImplementedError("Subclasses must implement detect()")

    def validate_walk_forward(
        self, start_date: Any, end_date: Any, train_window_days: int = 252, test_window_days: int = 63
    ) -> Dict[str, Any]:
        """Walk-forward validation: train on window, predict next window, measure accuracy."""
        raise NotImplementedError("Subclasses must implement validate_walk_forward()")


class EMARegimeDetector(RegimeDetector):
    """EMA-based regime detection (current implementation)."""

    def __init__(
        self,
        market_cap_band: str,
        short_span: int = 5,
        long_span: int = 10,
    ):
        super().__init__(market_cap_band)
        self.short_span = short_span
        self.long_span = long_span

    def detect(self, ohlcv_df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect regime using EMA crossover.
        Bull: close > ema_short AND ema_short > ema_long
        Bear: ema_short <= ema_long
        Choppy: else
        """
        df = ohlcv_df.copy()

        df['ema_short'] = df['close'].ewm(span=self.short_span, adjust=False).mean()
        df['ema_long'] = df['close'].ewm(span=self.long_span, adjust=False).mean()

        def get_regime(row: pd.Series) -> str:
            close: float = row['close']
            ema_short: float = row['ema_short']
            ema_long: float = row['ema_long']

            if close > ema_short and ema_short > ema_long:
                return 'Bull'
            elif ema_short <= ema_long:
                return 'Bear'
            else:
                return 'Choppy'

        df['regime'] = df.apply(get_regime, axis=1)
        return df[['regime']]


class RSIRegimeDetector(RegimeDetector):
    """RSI-based regime detection (momentum extremes)."""

    def __init__(
        self,
        market_cap_band: str,
        rsi_period: int = 14,
        overbought: float = 70,
        oversold: float = 30,
    ):
        super().__init__(market_cap_band)
        self.rsi_period = rsi_period
        self.overbought = overbought
        self.oversold = oversold

    def _calculate_rsi(self, prices: pd.Series) -> pd.Series:
        """Calculate RSI."""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=self.rsi_period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=self.rsi_period).mean()

        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    def detect(self, ohlcv_df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect regime using RSI.
        Bull: RSI > overbought (market strong, momentum-driven)
        Bear: RSI < oversold (market weak, momentum-driven)
        Choppy: else (in middle range)
        """
        df = ohlcv_df.copy()

        df['rsi'] = self._calculate_rsi(df['close'])

        def get_regime(row: pd.Series) -> str:
            rsi: float = row['rsi']
            if pd.isna(rsi):
                return 'Choppy'
            if rsi > self.overbought:
                return 'Bull'
            elif rsi < self.oversold:
                return 'Bear'
            else:
                return 'Choppy'

        df['regime'] = df.apply(get_regime, axis=1)
        return df[['regime']]


class VolatilityRegimeDetector(RegimeDetector):
    """Volatility-based regime detection (market stress)."""

    def __init__(
        self,
        market_cap_band: str,
        vol_window: int = 21,
        vol_high_percentile: float = 75,
        vol_low_percentile: float = 25,
    ):
        super().__init__(market_cap_band)
        self.vol_window = vol_window
        self.vol_high_percentile = vol_high_percentile
        self.vol_low_percentile = vol_low_percentile

    def detect(self, ohlcv_df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect regime using realized volatility.
        Bull: low vol (market calm, growth-oriented)
        Bear: high vol (market stressed, risk-off)
        Choppy: mid vol (transition)
        """
        df = ohlcv_df.copy()

        # Calculate returns
        df['returns'] = df['close'].pct_change()

        # Calculate rolling volatility
        df['volatility'] = df['returns'].rolling(window=self.vol_window).std() * np.sqrt(252)

        # Calculate percentiles dynamically
        df['vol_high_thresh'] = df['volatility'].rolling(window=self.vol_window * 2).quantile(
            self.vol_high_percentile / 100
        )
        df['vol_low_thresh'] = df['volatility'].rolling(window=self.vol_window * 2).quantile(
            self.vol_low_percentile / 100
        )

        def get_regime(row: pd.Series) -> str:
            vol: float = row['volatility']
            vol_high: float = row['vol_high_thresh']
            vol_low: float = row['vol_low_thresh']

            if pd.isna(vol) or pd.isna(vol_high) or pd.isna(vol_low):
                return 'Choppy'

            if vol > vol_high:
                return 'Bear'
            elif vol < vol_low:
                return 'Bull'
            else:
                return 'Choppy'

        df['regime'] = df.apply(get_regime, axis=1)
        return df[['regime']]


class EnsembleRegimeDetector(RegimeDetector):
    """Ensemble regime detection combining multiple algorithms."""

    def __init__(
        self,
        market_cap_band: str,
        detectors: Optional[List[RegimeDetector]] = None,
    ):
        super().__init__(market_cap_band)
        if detectors is None:
            detectors = [
                EMARegimeDetector(market_cap_band),
                RSIRegimeDetector(market_cap_band),
                VolatilityRegimeDetector(market_cap_band),
            ]
        self.detectors = detectors

    def detect(self, ohlcv_df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect regime using majority voting across multiple detectors.
        """
        df = ohlcv_df.copy()

        results = {}
        for detector in self.detectors:
            regime_df = detector.detect(ohlcv_df)
            results[detector.__class__.__name__] = regime_df['regime']

        # Majority voting
        regime_df = pd.DataFrame(results)

        def get_ensemble_regime(row: pd.Series) -> str:
            regimes = row.dropna()
            if len(regimes) == 0:
                return 'Choppy'

            counts = regimes.value_counts()
            if counts.iloc[0] > len(regimes) / 2:
                # Majority consensus
                return str(counts.index[0])
            else:
                # No clear majority, default to Choppy (conservative)
                return 'Choppy'

        df['regime'] = regime_df.apply(get_ensemble_regime, axis=1)
        return df[['regime']]


def run_regime_analysis(
    start_date: date_type,
    end_date: date_type,
    output_dir: Path = Path('/tmp/alphalens_regime_analysis'),
) -> Dict[tuple[str, str], Path]:
    """
    Run regime detection for all market caps using all algorithms.
    Write results to temp CSV files for review.

    Returns: dict mapping (market_cap_band, algorithm) -> csv_file_path
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    algorithms: Dict[str, type[RegimeDetector]] = {
        'ema': EMARegimeDetector,
        'rsi': RSIRegimeDetector,
        'volatility': VolatilityRegimeDetector,
        'ensemble': EnsembleRegimeDetector,
    }

    results_map: Dict[tuple[str, str], Path] = {}

    for market_cap_band in MARKET_CAP_BANDS.keys():
        logger.info(f"Processing market cap band: {market_cap_band}")

        for algo_name, algo_factory in algorithms.items():
            try:
                detector = algo_factory(market_cap_band)
                ohlcv_df = detector.load_index_ohlcv(start_date, end_date)

                if ohlcv_df.empty:
                    logger.warning(f"No data for {market_cap_band} ({algo_name})")
                    continue

                regime_df = detector.detect(ohlcv_df)

                # Combine OHLCV with regime for output
                output_df = ohlcv_df[['close']].copy()
                output_df['regime'] = regime_df['regime']

                output_file = output_dir / f'{market_cap_band}_{algo_name}.csv'
                output_df.to_csv(output_file)

                results_map[(market_cap_band, algo_name)] = output_file
                logger.info(f"  ✓ {market_cap_band} / {algo_name} -> {output_file}")

            except Exception as e:
                logger.error(f"Error processing {market_cap_band} ({algo_name}): {e}")
                continue

    return results_map


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    # Run analysis for last 3 years of data
    end_date = date_type.today()
    start_date = end_date - timedelta(days=3 * 365)

    logger.info(f"Regime detection analysis: {start_date} to {end_date}")
    results = run_regime_analysis(start_date, end_date)

    logger.info(f"\nGenerated {len(results)} regime analysis files:")
    for (band, algo), path in sorted(results.items()):
        logger.info(f"  {band:25} + {algo:12} -> {path}")

    logger.info("\nReview the CSV files to validate regime detection before deployment.")
