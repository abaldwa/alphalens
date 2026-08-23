"""
tests/unit/test_nifty_regime.py

Unit tests for features/nifty_regime.py regime detector.

Tests:
  - 6-branch regime logic (BULL, BULL_WEAK, CHOPPY, CHOPPY_BEARISH, BEAR, UNDEFINED)
  - EMA crossover detection
  - Boundary conditions (RSI at exact thresholds)
  - Missing data handling (NaN fallback)
  - Output validation
"""

import pandas as pd

from features.nifty_regime import (
    compute_nifty_regime,
    validate_regime,
    _assign_regime,
)


class TestAssignRegime:
    """Test individual regime assignment logic."""

    def test_bull_regime(self):
        """Close > EMA(5) AND EMA(5) > EMA(10) AND RSI(14) > 55 → BULL"""
        row = pd.Series({
            'close': 100,
            'ema_5': 99,
            'ema_10': 97,
            'rsi_14': 60,
            'ema_crossover': False,
        })
        assert _assign_regime(row) == 'BULL'

    def test_bull_weak_regime(self):
        """Close > EMA(5) AND EMA(5) > EMA(10) AND 50 <= RSI(14) <= 55 → BULL_WEAK"""
        row = pd.Series({
            'close': 100,
            'ema_5': 99,
            'ema_10': 97,
            'rsi_14': 52,
            'ema_crossover': False,
        })
        assert _assign_regime(row) == 'BULL_WEAK'

    def test_bull_weak_at_lower_bound(self):
        """RSI = 50 (at lower bound) → BULL_WEAK"""
        row = pd.Series({
            'close': 100,
            'ema_5': 99,
            'ema_10': 97,
            'rsi_14': 50.0,
            'ema_crossover': False,
        })
        assert _assign_regime(row) == 'BULL_WEAK'

    def test_bull_weak_at_upper_bound(self):
        """RSI = 55 (at upper bound) → BULL_WEAK"""
        row = pd.Series({
            'close': 100,
            'ema_5': 99,
            'ema_10': 97,
            'rsi_14': 55.0,
            'ema_crossover': False,
        })
        assert _assign_regime(row) == 'BULL_WEAK'

    def test_choppy_regime_with_good_rsi(self):
        """Close < EMA(5) AND EMA(5) >= EMA(10) AND RSI(14) >= 45 → CHOPPY"""
        row = pd.Series({
            'close': 95,
            'ema_5': 99,
            'ema_10': 97,
            'rsi_14': 48,
            'ema_crossover': False,
        })
        assert _assign_regime(row) == 'CHOPPY'

    def test_choppy_bearish_regime(self):
        """Close < EMA(5) AND EMA(5) >= EMA(10) AND RSI(14) < 45 → CHOPPY_BEARISH"""
        row = pd.Series({
            'close': 95,
            'ema_5': 99,
            'ema_10': 97,
            'rsi_14': 40,
            'ema_crossover': False,
        })
        assert _assign_regime(row) == 'CHOPPY_BEARISH'

    def test_choppy_bearish_at_rsi_threshold(self):
        """RSI = 45 (at threshold) should be CHOPPY, not CHOPPY_BEARISH"""
        row = pd.Series({
            'close': 95,
            'ema_5': 99,
            'ema_10': 97,
            'rsi_14': 45.0,
            'ema_crossover': False,
        })
        assert _assign_regime(row) == 'CHOPPY'

    def test_bear_regime_via_crossover(self):
        """EMA(5) crosses below EMA(10) → BEAR (regardless of other factors)"""
        row = pd.Series({
            'close': 100,
            'ema_5': 95,
            'ema_10': 97,
            'rsi_14': 60,
            'ema_crossover': True,  # Crossover detected
        })
        assert _assign_regime(row) == 'BEAR'

    def test_bear_regime_via_ema_structure(self):
        """EMA(5) < EMA(10) without crossover flag (edge case) → CHOPPY or CHOPPY_BEARISH"""
        # Note: This tests the fallback when crossover detection fails for some reason
        # The actual regime depends on RSI
        row = pd.Series({
            'close': 95,
            'ema_5': 95,
            'ema_10': 98,
            'rsi_14': 40,
            'ema_crossover': False,
        })
        # EMA(5) < EMA(10) with low RSI → CHOPPY_BEARISH
        assert _assign_regime(row) == 'CHOPPY_BEARISH'

    def test_undefined_with_nan_close(self):
        """Missing close → UNDEFINED"""
        row = pd.Series({
            'close': pd.NA,
            'ema_5': 99,
            'ema_10': 97,
            'rsi_14': 60,
            'ema_crossover': False,
        })
        assert _assign_regime(row) == 'UNDEFINED'

    def test_undefined_with_nan_rsi(self):
        """Missing RSI → UNDEFINED"""
        row = pd.Series({
            'close': 100,
            'ema_5': 99,
            'ema_10': 97,
            'rsi_14': pd.NA,
            'ema_crossover': False,
        })
        assert _assign_regime(row) == 'UNDEFINED'

    def test_undefined_with_nan_ema5(self):
        """Missing EMA(5) → UNDEFINED"""
        row = pd.Series({
            'close': 100,
            'ema_5': pd.NA,
            'ema_10': 97,
            'rsi_14': 60,
            'ema_crossover': False,
        })
        assert _assign_regime(row) == 'UNDEFINED'


class TestComputeNiftyRegime:
    """Test regime computation on synthetic OHLCV data."""

    def test_empty_input(self):
        """Empty DataFrame → empty regime DataFrame"""
        df = pd.DataFrame()
        result = compute_nifty_regime(df)
        assert result.empty

    def test_insufficient_data_for_ema(self):
        """Very few rows (< EMA period) → UNDEFINED regimes"""
        df = pd.DataFrame({
            'date': pd.date_range('2026-01-01', periods=3),
            'close': [100.0, 101.0, 102.0],
            'high': [101.0, 102.0, 103.0],
            'low': [99.0, 100.0, 101.0],
            'volume': [1000000] * 3,
        })
        result = compute_nifty_regime(df)
        # First few rows should have NaN in ema/rsi, leading to UNDEFINED
        assert (result['regime'] == 'UNDEFINED').all()  # EMA(10) needs 10+ rows

    def test_synthetic_bull_market(self):
        """Synthetic uptrend: price > EMA(5) > EMA(10), high RSI → BULL"""
        dates = pd.date_range('2026-01-01', periods=50)
        # Steady uptrend
        prices = [100.0 + 2*i for i in range(50)]
        df = pd.DataFrame({
            'date': dates,
            'close': prices,
            'high': [p + 1 for p in prices],
            'low': [p - 1 for p in prices],
            'volume': [1000000] * 50,
        })
        result = compute_nifty_regime(df)

        # After EMA stabilizes (row 14+), should see BULL regime
        # (price > EMA(5) > EMA(10), RSI high in uptrend)
        bull_regimes = result.iloc[14:]['regime']
        # Exact regime depends on RSI calculation, but should mostly be BULL/BULL_WEAK
        assert bull_regimes.isin(['BULL', 'BULL_WEAK']).sum() > 10

    def test_synthetic_bearish_crossover(self):
        """Synthetic downtrend with EMA crossover → BEAR"""
        dates = pd.date_range('2026-01-01', periods=50)
        # Uptrend then sharp downtrend
        prices = [100.0 + 2*i for i in range(25)] + [150.0 - 3*i for i in range(25)]
        df = pd.DataFrame({
            'date': dates,
            'close': prices,
            'high': [p + 1 for p in prices],
            'low': [p - 1 for p in prices],
            'volume': [1000000] * 50,
        })
        result = compute_nifty_regime(df)

        # Around row 30-35, downtrend should cause EMA(5) < EMA(10)
        # and should register ema_crossover = True → BEAR
        bear_rows = result[result['regime'] == 'BEAR']
        assert not bear_rows.empty, "Expected BEAR regime in downtrend"

    def test_output_columns(self):
        """Output has all required columns"""
        df = pd.DataFrame({
            'date': pd.date_range('2026-01-01', periods=20),
            'close': [100.0 + i for i in range(20)],
            'high': [101.0 + i for i in range(20)],
            'low': [99.0 + i for i in range(20)],
            'volume': [1000000] * 20,
        })
        result = compute_nifty_regime(df)

        expected_cols = ['date', 'close', 'ema_5', 'ema_10', 'rsi_14',
                         'regime', 'exposure', 'ema_crossover']
        assert list(result.columns) == expected_cols

    def test_regime_exposure_mapping(self):
        """Each regime maps to correct exposure"""
        df = pd.DataFrame({
            'date': pd.date_range('2026-01-01', periods=50),
            'close': [100.0 + 0.5*i for i in range(50)],  # Slow uptrend
            'high': [101.0 + 0.5*i for i in range(50)],
            'low': [99.0 + 0.5*i for i in range(50)],
            'volume': [1000000] * 50,
        })
        result = compute_nifty_regime(df)

        # Check that exposure values match regime
        for _, row in result.iterrows():
            if row['regime'] == 'BULL':
                assert row['exposure'] == 1.0
            elif row['regime'] == 'BULL_WEAK':
                assert row['exposure'] == 0.75
            elif row['regime'] == 'CHOPPY':
                assert row['exposure'] == 0.50
            elif row['regime'] == 'CHOPPY_BEARISH':
                assert row['exposure'] == 0.25
            elif row['regime'] == 'BEAR':
                assert row['exposure'] == 0.0
            elif row['regime'] == 'UNDEFINED':
                assert row['exposure'] == 0.50


class TestValidateRegime:
    """Test regime output validation."""

    def test_valid_regime_dataframe(self):
        """Valid regime DF passes validation"""
        df = pd.DataFrame({
            'date': pd.date_range('2026-01-01', periods=10),
            'close': [100.0] * 10,
            'ema_5': [99.0] * 10,
            'ema_10': [98.0] * 10,
            'rsi_14': [55.0] * 10,
            'regime': ['BULL'] * 10,
            'exposure': [1.0] * 10,
            'ema_crossover': [False] * 10,
        })
        is_valid, msg = validate_regime(df)
        assert is_valid
        assert 'Valid' in msg

    def test_empty_regime_dataframe(self):
        """Empty DataFrame is valid (acceptable)"""
        df = pd.DataFrame()
        is_valid, msg = validate_regime(df)
        assert is_valid
        assert 'Empty' in msg

    def test_missing_column(self):
        """Missing required column → invalid"""
        df = pd.DataFrame({
            'date': pd.date_range('2026-01-01', periods=10),
            'close': [100.0] * 10,
            'ema_5': [99.0] * 10,
            # Missing 'ema_10'
            'regime': ['BULL'] * 10,
            'exposure': [1.0] * 10,
        })
        is_valid, msg = validate_regime(df)
        assert not is_valid
        assert 'Missing' in msg

    def test_nan_in_regime(self):
        """NaN in regime column → invalid"""
        df = pd.DataFrame({
            'date': pd.date_range('2026-01-01', periods=10),
            'close': [100.0] * 10,
            'ema_5': [99.0] * 10,
            'ema_10': [98.0] * 10,
            'rsi_14': [55.0] * 10,
            'regime': ['BULL'] * 9 + [pd.NA],
            'exposure': [1.0] * 10,
            'ema_crossover': [False] * 10,
        })
        is_valid, msg = validate_regime(df)
        assert not is_valid
        assert 'regime' in msg.lower()

    def test_invalid_regime_value(self):
        """Invalid regime name → invalid"""
        df = pd.DataFrame({
            'date': pd.date_range('2026-01-01', periods=10),
            'close': [100.0] * 10,
            'ema_5': [99.0] * 10,
            'ema_10': [98.0] * 10,
            'rsi_14': [55.0] * 10,
            'regime': ['BULL'] * 9 + ['INVALID'],
            'exposure': [1.0] * 10,
            'ema_crossover': [False] * 10,
        })
        is_valid, msg = validate_regime(df)
        assert not is_valid
        assert 'Invalid regime' in msg

    def test_invalid_exposure_value(self):
        """Invalid exposure value → invalid"""
        df = pd.DataFrame({
            'date': pd.date_range('2026-01-01', periods=10),
            'close': [100.0] * 10,
            'ema_5': [99.0] * 10,
            'ema_10': [98.0] * 10,
            'rsi_14': [55.0] * 10,
            'regime': ['BULL'] * 10,
            'exposure': [1.0] * 9 + [0.99],  # Invalid: not in {0.0, 0.25, 0.5, 0.75, 1.0}
            'ema_crossover': [False] * 10,
        })
        is_valid, msg = validate_regime(df)
        assert not is_valid
        assert 'exposure' in msg.lower()

    def test_non_monotonic_dates(self):
        """Dates not monotonically increasing → invalid"""
        df = pd.DataFrame({
            'date': pd.to_datetime(['2026-01-01', '2026-01-03', '2026-01-02']),
            'close': [100.0] * 3,
            'ema_5': [99.0] * 3,
            'ema_10': [98.0] * 3,
            'rsi_14': [55.0] * 3,
            'regime': ['BULL'] * 3,
            'exposure': [1.0] * 3,
            'ema_crossover': [False] * 3,
        })
        is_valid, msg = validate_regime(df)
        assert not is_valid
        assert 'monotonic' in msg.lower()

    def test_duplicate_dates(self):
        """Duplicate dates → invalid"""
        df = pd.DataFrame({
            'date': pd.to_datetime(['2026-01-01', '2026-01-01', '2026-01-02']),
            'close': [100.0] * 3,
            'ema_5': [99.0] * 3,
            'ema_10': [98.0] * 3,
            'rsi_14': [55.0] * 3,
            'regime': ['BULL'] * 3,
            'exposure': [1.0] * 3,
            'ema_crossover': [False] * 3,
        })
        is_valid, msg = validate_regime(df)
        assert not is_valid
        assert 'Duplicate' in msg
