"""
tests/unit/test_engine_regime_integration.py

Unit tests for backtest/core/engine.py regime integration (Phase 2).
Tests regime loading, signal enrichment, and trade logging.
"""

from datetime import date as date_type

import pandas as pd
import pytest

from backtest.core.engine import BacktestOrchestrator, Signal


class MockRegimeData:
    """Mock regime data for testing."""

    @staticmethod
    def create_test_regime_df() -> pd.DataFrame:
        """Create a small test regime DataFrame."""
        dates = pd.date_range('2026-01-01', periods=20)
        regimes = ['BULL'] * 10 + ['CHOPPY'] * 5 + ['BEAR'] * 5
        exposures = [1.0] * 10 + [0.5] * 5 + [0.0] * 5

        return pd.DataFrame({
            'date': dates,
            'regime': regimes,
            'exposure': exposures,
            'ema_5': [100.0 + i for i in range(20)],
            'ema_10': [99.0 + i for i in range(20)],
            'rsi_14': [60.0 - i for i in range(20)],
        })


class TestBacktestOrchestratorRegimeLoading:
    """Test regime data loading."""

    def test_regime_type_none(self):
        """With regime_type=None, regime data is never loaded."""
        orchestrator = BacktestOrchestrator(regime_type=None)
        assert orchestrator._regime_type is None
        assert orchestrator._regime_data is None

    def test_regime_type_set(self):
        """With regime_type set, orchestrator stores it."""
        orchestrator = BacktestOrchestrator(regime_type='ema_rsi_v1')
        assert orchestrator._regime_type == 'ema_rsi_v1'
        assert orchestrator._regime_data is None  # Not loaded yet

    def test_load_regime_data_with_mock(self):
        """Test regime data loading with mock data."""
        orchestrator = BacktestOrchestrator(regime_type='ema_rsi_v1')
        # Manually set regime data (simulating successful load)
        orchestrator._regime_data = MockRegimeData.create_test_regime_df()

        regime = orchestrator.get_regime_for_date(date_type(2026, 1, 5))
        assert regime is not None
        assert regime.regime == 'BULL'
        assert regime.exposure == 1.0
        assert regime.ema_5 == 104.0
        assert regime.rsi_14 == 56.0

    def test_get_regime_for_date_no_regime_type(self):
        """With no regime_type, get_regime_for_date returns None."""
        orchestrator = BacktestOrchestrator(regime_type=None)
        regime = orchestrator.get_regime_for_date(date_type(2026, 1, 1))
        assert regime is None

    def test_get_regime_for_date_date_not_found(self):
        """get_regime_for_date returns None for date not in data."""
        orchestrator = BacktestOrchestrator(regime_type='ema_rsi_v1')
        orchestrator._regime_data = MockRegimeData.create_test_regime_df()

        regime = orchestrator.get_regime_for_date(date_type(1900, 1, 1))
        assert regime is None


class TestApplyRegimeToSignals:
    """Test signal enrichment with regime data."""

    def test_apply_regime_to_signals_no_regime(self):
        """With no regime_type, signals are returned unchanged."""
        orchestrator = BacktestOrchestrator(regime_type=None)
        signal = Signal(ticker='SBIN', action='buy', conviction=0.8)
        signals = [signal]

        result = orchestrator.apply_regime_to_signals(signals, date_type(2026, 1, 1))

        assert len(result) == 1
        assert result[0].ticker == 'SBIN'
        assert result[0].regime_exposure is None
        assert result[0].regime is None

    def test_apply_regime_to_signals_with_regime(self):
        """With regime_type, signals are enriched with regime data."""
        orchestrator = BacktestOrchestrator(regime_type='ema_rsi_v1')
        orchestrator._regime_data = MockRegimeData.create_test_regime_df()

        signal = Signal(ticker='SBIN', action='buy', conviction=0.8)
        signals = [signal]

        result = orchestrator.apply_regime_to_signals(signals, date_type(2026, 1, 5))

        assert len(result) == 1
        assert result[0].ticker == 'SBIN'
        assert result[0].action == 'buy'
        assert result[0].conviction == 0.8
        assert result[0].regime_exposure == 1.0
        assert result[0].regime == 'BULL'
        assert result[0].nifty_rsi_14 == 56.0
        assert result[0].nifty_ema_5 == 104.0
        assert result[0].nifty_ema_10 == 103.0

    def test_apply_regime_to_multiple_signals(self):
        """Regime is applied to all signals uniformly."""
        orchestrator = BacktestOrchestrator(regime_type='ema_rsi_v1')
        orchestrator._regime_data = MockRegimeData.create_test_regime_df()

        signals = [
            Signal(ticker='SBIN', action='buy', conviction=0.8),
            Signal(ticker='INFY', action='buy', conviction=0.7),
            Signal(ticker='TCS', action='sell', conviction=0.9),
        ]

        result = orchestrator.apply_regime_to_signals(signals, date_type(2026, 1, 15))

        assert len(result) == 3
        # All signals should have same regime (same date)
        for sig in result:
            assert sig.regime == 'CHOPPY'
            assert sig.regime_exposure == 0.5
            assert sig.nifty_rsi_14 == 46.0  # 60.0 - 14 = 46.0

    def test_apply_regime_preserves_signal_fields(self):
        """Regime enrichment preserves original signal fields."""
        orchestrator = BacktestOrchestrator(regime_type='ema_rsi_v1')
        orchestrator._regime_data = MockRegimeData.create_test_regime_df()

        signal = Signal(
            ticker='SBIN',
            action='buy',
            sector='Financial',
            conviction=0.85,
            adtv_cr=100.5,
            template='technical_a1',
            size_multiplier=1.2,
        )

        result = orchestrator.apply_regime_to_signals([signal], date_type(2026, 1, 5))

        assert result[0].sector == 'Financial'
        assert result[0].conviction == 0.85
        assert result[0].adtv_cr == 100.5
        assert result[0].template == 'technical_a1'
        assert result[0].size_multiplier == 1.2

    def test_apply_regime_with_bear_regime(self):
        """Regime enrichment works for BEAR regime."""
        orchestrator = BacktestOrchestrator(regime_type='ema_rsi_v1')
        orchestrator._regime_data = MockRegimeData.create_test_regime_df()

        signal = Signal(ticker='SBIN', action='buy', conviction=0.8)

        result = orchestrator.apply_regime_to_signals([signal], date_type(2026, 1, 17))

        assert result[0].regime == 'BEAR'
        assert result[0].regime_exposure == 0.0


class TestRegimeSignalIntegration:
    """Integration tests for regime application in signal flow."""

    def test_signal_is_frozen(self):
        """Verify Signal dataclass is frozen."""
        signal = Signal(ticker='SBIN', action='buy')
        # Frozen dataclass should raise FrozenInstanceError on mutation
        with pytest.raises(Exception):  # FrozenInstanceError
            signal.regime = 'BULL'  # type: ignore

    def test_signal_with_all_regime_fields(self):
        """Signal can be created with all regime fields."""
        signal = Signal(
            ticker='SBIN',
            action='buy',
            regime='BULL',
            regime_exposure=1.0,
            nifty_rsi_14=60.0,
            nifty_ema_5=100.0,
            nifty_ema_10=99.0,
        )

        assert signal.regime == 'BULL'
        assert signal.regime_exposure == 1.0
        assert signal.nifty_rsi_14 == 60.0
        assert signal.nifty_ema_5 == 100.0
        assert signal.nifty_ema_10 == 99.0


class TestRegimeCachePopulation:
    """Test regime cache population for trade logging."""

    def test_populate_regime_cache_with_data(self):
        """_populate_regime_cache should populate cache from _regime_data."""
        orchestrator = BacktestOrchestrator(regime_type='ema_rsi_v1')
        orchestrator._regime_data = MockRegimeData.create_test_regime_df()

        orchestrator._populate_regime_cache()

        # Cache should have 20 entries (one per row in test data)
        assert len(orchestrator._regime_cache) == 20

        # Verify cache has correct data for sample dates
        first_date = date_type(2026, 1, 1)
        assert first_date in orchestrator._regime_cache
        regime = orchestrator._regime_cache[first_date]
        assert regime.regime == 'BULL'
        assert regime.exposure == 1.0
        assert regime.ema_5 == 100.0

    def test_populate_regime_cache_empty_data(self):
        """With empty _regime_data, cache should remain empty."""
        orchestrator = BacktestOrchestrator(regime_type='ema_rsi_v1')
        orchestrator._regime_data = pd.DataFrame(columns=['date', 'regime', 'exposure', 'ema_5', 'ema_10', 'rsi_14'])

        orchestrator._populate_regime_cache()

        assert len(orchestrator._regime_cache) == 0

    def test_populate_regime_cache_no_regime_data(self):
        """With no _regime_data, cache should remain empty."""
        orchestrator = BacktestOrchestrator(regime_type='ema_rsi_v1')

        orchestrator._populate_regime_cache()

        assert len(orchestrator._regime_cache) == 0

    def test_regime_cache_lookup_by_date(self):
        """Regime cache should support O(1) lookups by date."""
        orchestrator = BacktestOrchestrator(regime_type='ema_rsi_v1')
        orchestrator._regime_data = MockRegimeData.create_test_regime_df()
        orchestrator._populate_regime_cache()

        # Lookup various dates
        date_1_5 = date_type(2026, 1, 5)
        date_1_15 = date_type(2026, 1, 15)
        date_1_18 = date_type(2026, 1, 18)

        assert orchestrator._regime_cache[date_1_5].regime == 'BULL'
        assert orchestrator._regime_cache[date_1_15].regime == 'CHOPPY'
        assert orchestrator._regime_cache[date_1_18].regime == 'BEAR'
