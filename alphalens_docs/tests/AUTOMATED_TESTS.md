# AlphaLens — Automated Test Suite Specification
## pytest · Coverage Target ≥ 80% · Run Before Every Commit

Run all tests: `pytest tests/ -v --cov=pipeline --cov=models --cov-report=html`

---

## Test Fixtures (conftest.py)

```python
# tests/conftest.py
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

@pytest.fixture
def sample_ohlcv():
    """252 days of synthetic OHLCV for 5 tickers."""
    dates = pd.date_range('2024-01-01', periods=252, freq='B')
    tickers = ['RELIANCE', 'TCS', 'HDFCBANK', 'INFY', 'WIPRO']
    rows = []
    for ticker in tickers:
        np.random.seed(hash(ticker) % 1000)
        price = 1000.0
        for date in dates:
            ret = np.random.normal(0.0005, 0.015)
            price *= (1 + ret)
            rows.append({
                'date': date.strftime('%Y-%m-%d'),
                'ticker': ticker,
                'open': price * 0.998,
                'high': price * 1.012,
                'low': price * 0.988,
                'close': price,
                'volume': np.random.randint(100000, 5000000),
                'delivery_qty': np.random.randint(50000, 2500000),
            })
    df = pd.DataFrame(rows)
    df['delivery_pct'] = df['delivery_qty'] / df['volume'] * 100
    return df

@pytest.fixture
def sample_fundamentals():
    """2 years of quarterly fundamentals for 5 tickers."""
    rows = []
    for ticker in ['RELIANCE','TCS','HDFCBANK','INFY','WIPRO']:
        for yr in [2023, 2024]:
            for q in [1, 2, 3, 4]:
                qe = pd.Timestamp(yr, [3,6,9,12][q-1], 30)
                ann = qe + timedelta(days=50)
                rows.append({
                    'ticker': ticker, 'fiscal_year': yr, 'quarter': q,
                    'quarter_end_date': qe.strftime('%Y-%m-%d'),
                    'announcement_date': ann.strftime('%Y-%m-%d'),
                    'revenue': np.random.uniform(1e9, 1e10),
                    'ebitda': np.random.uniform(2e8, 3e9),
                    'pat': np.random.uniform(1e8, 2e9),
                    'eps': np.random.uniform(10, 100),
                    'roe': np.random.uniform(10, 30),
                    'roce': np.random.uniform(12, 35),
                    'debt_to_equity': np.random.uniform(0.1, 1.5),
                    'operating_margin': np.random.uniform(15, 35),
                })
    return pd.DataFrame(rows)

@pytest.fixture
def split_event():
    return {'ticker': 'RELIANCE', 'ex_date': '2024-06-15',
            'action_type': 'SPLIT', 'ratio': 0.5}  # 1:2 split

@pytest.fixture
def pnd_stock_features():
    """Features of a known P&D stock."""
    return pd.DataFrame([{
        'ticker': 'SCAMCO', 'volume_spike_magnitude': 8.5,
        'volume_spike_persistence': 7, 'delivery_pct_collapse': 0.25,
        'parabolic_curve_score': 0.92, 'price_velocity_5d': 4.2,
        'consecutive_upper_circuits': 4, 'asm_flag': 1,
        **{f'feature_{i}': 0.5 for i in range(15)}
    }])

@pytest.fixture
def normal_stock_features():
    """Features of a normal stock."""
    return pd.DataFrame([{
        'ticker': 'RELIABLE', 'volume_spike_magnitude': 1.2,
        'volume_spike_persistence': 0, 'delivery_pct_collapse': 0.95,
        'parabolic_curve_score': 0.1, 'price_velocity_5d': 0.3,
        'consecutive_upper_circuits': 0, 'asm_flag': 0,
        **{f'feature_{i}': 0.5 for i in range(15)}
    }])
```

---

## T-01: Data Ingestion Tests

```python
# tests/test_ingestion.py

class TestBhavcopyDownload:
    def test_download_returns_500_plus_tickers(self, mock_nse):
        df = download_bhavcopy('2024-12-30')
        assert len(df) >= 450, "Expected ≥ 450 stocks in bhavcopy"

    def test_required_columns_present(self, mock_nse):
        df = download_bhavcopy('2024-12-30')
        required = ['ticker','open','high','low','close','volume','delivery_qty']
        assert all(col in df.columns for col in required)

    def test_retry_on_connection_error(self, mock_nse_failure):
        """Should retry 3 times then raise ConnectionError."""
        with pytest.raises(ConnectionError):
            download_bhavcopy('2024-12-30')

    def test_anomaly_detection_flags_large_moves(self, sample_ohlcv):
        """Stock with > 30% move without corp action should be flagged."""
        sample_ohlcv.loc[0, 'close'] = sample_ohlcv.loc[0, 'close'] * 1.5
        result = validate_bhavcopy(sample_ohlcv, sample_ohlcv['ticker'].unique())
        assert 'anomalies' in result
        assert len(result['anomalies']) > 0
```

---

## T-02: Corporate Action Adjustment Tests (CRITICAL)

```python
# tests/test_corp_actions.py

class TestCorporateActionAdjustment:
    def test_1_to_2_split_halves_pre_ex_prices(self, sample_ohlcv, split_event):
        """1:2 split on 2024-06-15 should halve all prices before that date."""
        conn = create_test_db(sample_ohlcv)
        insert_corp_action(conn, split_event)

        pre_split_close = get_close(conn, 'RELIANCE', '2024-06-14')
        apply_corporate_actions(conn, 'RELIANCE')
        adjusted_close = get_close(conn, 'RELIANCE', '2024-06-14')

        assert abs(adjusted_close - pre_split_close * 0.5) < 0.01, \
            "Pre-split price should be halved after 1:2 split"

    def test_1_to_1_bonus_halves_pre_bonus_prices(self, sample_ohlcv):
        """1:1 bonus doubles shares → prices should halve before record date."""
        conn = create_test_db(sample_ohlcv)
        insert_corp_action(conn, {'ticker':'RELIANCE','ex_date':'2024-06-15',
                                    'action_type':'BONUS','ratio':1.0})
        pre = get_close(conn, 'RELIANCE', '2024-06-14')
        apply_corporate_actions(conn, 'RELIANCE')
        post = get_close(conn, 'RELIANCE', '2024-06-14')
        assert abs(post - pre * 0.5) < 0.01

    def test_post_action_prices_unchanged(self, sample_ohlcv, split_event):
        """Prices ON and AFTER ex_date should not be adjusted."""
        conn = create_test_db(sample_ohlcv)
        insert_corp_action(conn, split_event)
        pre = get_close(conn, 'RELIANCE', '2024-06-15')  # ex_date itself
        apply_corporate_actions(conn, 'RELIANCE')
        post = get_close(conn, 'RELIANCE', '2024-06-15')
        assert abs(post - pre) < 0.01, "Ex-date price should not be adjusted"

    def test_adjustment_is_idempotent(self, sample_ohlcv, split_event):
        """Running adjust twice should give same result as running once."""
        conn = create_test_db(sample_ohlcv)
        insert_corp_action(conn, split_event)
        apply_corporate_actions(conn, 'RELIANCE')
        result_once = get_close(conn, 'RELIANCE', '2024-06-14')
        apply_corporate_actions(conn, 'RELIANCE')  # Run again
        result_twice = get_close(conn, 'RELIANCE', '2024-06-14')
        assert abs(result_once - result_twice) < 0.001, "Adjustment must be idempotent"

    def test_volume_increases_after_split(self, sample_ohlcv, split_event):
        """After 1:2 split, pre-split volume should double."""
        conn = create_test_db(sample_ohlcv)
        insert_corp_action(conn, split_event)
        pre_vol = get_volume(conn, 'RELIANCE', '2024-06-14')
        apply_corporate_actions(conn, 'RELIANCE')
        post_vol = get_volume(conn, 'RELIANCE', '2024-06-14')
        assert abs(post_vol - pre_vol * 2) < pre_vol * 0.01
```

---

## T-03: Point-in-Time Alignment Tests (CRITICAL)

```python
# tests/test_pit_alignment.py

class TestPointInTime:
    def test_fundamentals_not_available_before_announcement(self,
                                                               sample_fundamentals):
        """Q1 results announced 2024-05-15 must NOT appear in features on 2024-04-01."""
        fund = sample_fundamentals[
            (sample_fundamentals['ticker']=='TCS') &
            (sample_fundamentals['announcement_date']=='2024-05-15')
        ]
        result = get_latest_as_of(None, 'fundamentals', 'TCS',
                                   '2024-04-01', 'announcement_date')
        assert result is None or result['announcement_date'] < '2024-05-15', \
            "LOOKAHEAD BIAS: future fundamentals appearing in past features"

    def test_shareholding_uses_filing_date_not_quarter_end(self, mock_db):
        """Quarter ending 2024-03-31 filed on 2024-04-21. Not available on 2024-04-10."""
        result = get_latest_as_of(mock_db, 'shareholding', 'RELIANCE',
                                   '2024-04-10', 'filing_date')
        # Filing date is 2024-04-21, so should get previous quarter's data
        assert result is None or result['filing_date'] <= '2024-04-10'

    def test_mf_holdings_5th_of_month_rule(self, mock_db):
        """March holdings published ~5th April. Not available on 4th April."""
        result_before = get_mf_holdings_as_of(mock_db, 'TCS', '2024-04-04')
        result_after  = get_mf_holdings_as_of(mock_db, 'TCS', '2024-04-06')
        assert result_before['month_end_date'] == '2024-02-29'  # Feb data
        assert result_after['month_end_date']  == '2024-03-31'  # Mar data

    def test_staleness_features_computed_correctly(self, sample_fundamentals):
        """days_since_results should be 30 for announcement 30 days ago."""
        staleness = compute_staleness('2024-04-01', '2024-05-01')
        assert staleness['days_since_results'] == 30
        assert staleness['results_pending_flag'] == 0  # 30 < 70
        assert abs(staleness['quarter_age_pct'] - 30/63) < 0.01
```

---

## T-04: Feature Computation Tests

```python
# tests/test_features.py

class TestTechnicalFeatures:
    def test_76_features_produced(self, sample_ohlcv):
        """Exactly 76 technical features should be in output."""
        result = compute_technical_features(sample_ohlcv)
        core_features = [c for c in result.columns
                          if c not in ['date','ticker']]
        assert len(core_features) == 76, \
            f"Expected 76 features, got {len(core_features)}"

    def test_rsi_range(self, sample_ohlcv):
        """RSI must always be in [0, 100]."""
        result = compute_technical_features(sample_ohlcv)
        assert result['rsi_14'].dropna().between(0, 100).all(), \
            "RSI out of valid range [0, 100]"

    def test_pct_rank_range(self, sample_ohlcv):
        """Percentile rank features must be in [0, 1]."""
        result = compute_technical_features(sample_ohlcv)
        for col in ['pct_rank_5d','pct_rank_21d','pct_rank_63d']:
            assert result[col].dropna().between(0, 1).all(), \
                f"{col} out of valid range [0, 1]"

    def test_no_forward_looking_features(self, sample_ohlcv):
        """Features for date T must not use prices after date T."""
        # Verify by checking that same-day feature values don't change
        # when future prices are removed from input
        df_full = sample_ohlcv.copy()
        df_partial = sample_ohlcv[sample_ohlcv['date'] <= '2024-06-30'].copy()
        full_result = compute_technical_features(df_full)
        partial_result = compute_technical_features(df_partial)
        # Features for 2024-06-30 should be identical in both
        full_row = full_result[full_result['date']=='2024-06-30']
        part_row = partial_result[partial_result['date']=='2024-06-30']
        pd.testing.assert_frame_equal(full_row, part_row,
            check_exact=False, rtol=1e-6)

    def test_features_vectorized_no_loops(self):
        """Feature computation time should scale linearly with stocks."""
        import time
        t1 = time.time(); compute_for_n_stocks(10); d1 = time.time() - t1
        t2 = time.time(); compute_for_n_stocks(100); d2 = time.time() - t2
        # Should scale roughly linearly (not exponentially)
        assert d2 < d1 * 15, "Feature computation not vectorized — scaling too slow"

    def test_sector_zscore_mean_zero(self, sample_ohlcv, sample_fundamentals):
        """Sector z-scores should have mean ≈ 0 within each sector."""
        features = compute_fundamental_features(sample_fundamentals)
        for sector in features['sector'].unique():
            sector_roe = features[features['sector']==sector]['roe_sector_zscore']
            assert abs(sector_roe.mean()) < 0.1, \
                f"Sector z-score mean not near zero for {sector}"
```

---

## T-05: Model Tests

```python
# tests/test_models.py

class TestPnDDetector:
    def test_pnd_stock_scores_above_60(self, trained_pnd_model, pnd_stock_features):
        """Known P&D pattern should score > 60."""
        result = trained_pnd_model.predict_full(pnd_stock_features)
        assert result['pnd_score'].iloc[0] > 60, \
            "P&D stock should be blocked (score > 60)"
        assert result['pnd_block'].iloc[0] == True

    def test_normal_stock_scores_below_40(self, trained_pnd_model, normal_stock_features):
        """Normal stock should not be flagged."""
        result = trained_pnd_model.predict_full(normal_stock_features)
        assert result['pnd_score'].iloc[0] < 40

    def test_pnd_runs_before_signal_model(self):
        """Verify pipeline order: P&D gate before signal model."""
        execution_order = []
        with mock_pipeline(record_order=execution_order):
            run_pipeline(date='2024-12-30')
        pnd_idx = execution_order.index('pnd_detector')
        sig_idx = execution_order.index('signal_5d')
        assert pnd_idx < sig_idx, "P&D detector must run before signal model"

    def test_blocked_stock_not_in_buy_signals(self, trained_pnd_model,
                                                signal_model, features_df):
        """Stock with P&D score > 60 must not appear in buy recommendations."""
        pnd_results = trained_pnd_model.predict_full(features_df)
        blocked = set(pnd_results[pnd_results['pnd_block']]['ticker'])
        signal_results = signal_model.predict_signals(features_df)
        buys = set(signal_results[signal_results['signal_buy_prob'] > 0.65]['ticker'])
        overlap = blocked & buys
        assert len(overlap) == 0, \
            f"Blocked stocks appearing in buy signals: {overlap}"

class TestHMMRegime:
    def test_4_states_produced(self, trained_hmm, sample_ohlcv):
        """HMM must produce exactly 4 regime states."""
        result = trained_hmm.predict(sample_ohlcv)
        assert result['hmm_regime'].nunique() == 4

    def test_probabilities_sum_to_one(self, trained_hmm, sample_ohlcv):
        """State probabilities must sum to 1."""
        probs = trained_hmm.predict_proba(sample_ohlcv)
        row_sums = probs.sum(axis=1)
        assert np.allclose(row_sums, 1.0, atol=1e-6)

    def test_bullish_state_has_positive_mean_return(self, trained_hmm, sample_ohlcv):
        """State labeled 'bullish' must have the highest mean return."""
        result = trained_hmm.predict(sample_ohlcv)
        sample_ohlcv['regime'] = result['hmm_regime'].values
        sample_ohlcv['return'] = sample_ohlcv.groupby('ticker')['close'].pct_change()
        state_returns = sample_ohlcv.groupby('regime')['return'].mean()
        bullish_state = 3  # Convention: highest mean return = state 3
        assert state_returns[bullish_state] == state_returns.max()

class TestConformalPrediction:
    def test_coverage_at_least_90_percent(self, trained_signal, conformal_wrapper,
                                             X_cal, y_cal, X_test, y_test):
        """90% conformal intervals must contain true value ≥ 90% of the time."""
        _, y_pis = conformal_wrapper.predict(X_test, alpha=0.10)
        lower = y_pis[:, 0, 0]
        upper = y_pis[:, 0, 1]
        coverage = ((y_test >= lower) & (y_test <= upper)).mean()
        assert coverage >= 0.88, \
            f"Conformal coverage {coverage:.1%} below 88% (target 90%)"

class TestMetaLabeler:
    def test_labels_based_on_profitability_not_direction(self):
        """Meta-labeler label=1 only if profitable AFTER transaction costs."""
        # A 0.3% gain with 0.5% round-trip cost = label 0 (unprofitable)
        label = create_meta_label(signal_return=0.003, transaction_cost=0.005)
        assert label == 0

        # A 1.2% gain with 0.5% round-trip cost = label 1 (profitable)
        label = create_meta_label(signal_return=0.012, transaction_cost=0.005)
        assert label == 1
```

---

## T-06: Backtesting Tests

```python
# tests/test_backtest.py

class TestWalkForwardBacktest:
    def test_test_data_never_in_training(self, feature_store):
        """Test fold data must never appear in training data."""
        bt = WalkForwardBacktester(feature_store, MockTrainer())
        folds = bt.generate_folds()
        for fold in folds:
            train_dates = set(fold['train']['date'])
            test_dates = set(fold['test']['date'])
            overlap = train_dates & test_dates
            assert len(overlap) == 0, f"Leakage: {len(overlap)} dates in both train and test"

    def test_includes_delisted_stocks(self, feature_store_with_delisted):
        """Backtester must include delisted stocks for survivorship bias prevention."""
        bt = WalkForwardBacktester(feature_store_with_delisted, MockTrainer())
        universe = bt.load_universe()
        delisted = universe[universe['is_delisted'] == True]
        assert len(delisted) > 0, "Delisted stocks missing from backtest universe"

    def test_transaction_costs_applied(self, backtest_result):
        """Net returns must be less than gross returns (costs applied)."""
        assert backtest_result['net_cagr'] < backtest_result['gross_cagr']
        expected_cost_drag = 0.003  # ~0.3% per year minimum
        assert (backtest_result['gross_cagr'] -
                backtest_result['net_cagr']) >= expected_cost_drag

    def test_liquidity_constraint_enforced(self, backtest_result):
        """No trades should occur in stocks with ADTV < ₹10L."""
        for trade in backtest_result['trade_log']:
            assert trade['adtv'] >= 1_000_000, \
                f"Trade in illiquid stock: {trade['ticker']} ADTV={trade['adtv']}"

    def test_position_size_cap(self, backtest_result):
        """No single position should exceed 10% of portfolio."""
        for day in backtest_result['daily_portfolio']:
            for pos in day['positions']:
                pct = pos['value'] / day['portfolio_value']
                assert pct <= 0.11, \
                    f"Position {pos['ticker']} = {pct:.1%} exceeds 10% limit"

class TestMetrics:
    def test_sharpe_calculation(self):
        """Verify Sharpe ratio formula."""
        returns = np.array([0.01, 0.02, -0.005, 0.015, 0.008])
        sharpe = compute_sharpe(returns)
        expected = returns.mean() / returns.std() * np.sqrt(252)
        assert abs(sharpe - expected) < 0.001

    def test_fold_stability_reported(self, multi_fold_result):
        """std(Sharpe across folds) must be computed and reported."""
        assert 'sharpe_std' in multi_fold_result
        assert multi_fold_result['sharpe_std'] >= 0
```

---

## T-07: Pipeline Integration Tests

```python
# tests/test_pipeline_integration.py

class TestDailyPipelineIntegration:
    def test_full_pipeline_completes_within_90_min(self, mock_data_sources):
        """Full pipeline 4:00 PM → 5:30 PM should complete within 90 minutes."""
        start = time.time()
        run_pipeline('2024-12-30')
        elapsed_minutes = (time.time() - start) / 60
        assert elapsed_minutes < 90, \
            f"Pipeline took {elapsed_minutes:.1f} min (limit: 90 min)"

    def test_parquet_written_with_correct_shape(self, mock_data_sources, tmp_path):
        """Output parquet should have 500 rows × ≥98 columns."""
        run_pipeline('2024-12-30', output_path=tmp_path)
        result = pd.read_parquet(tmp_path / '2024-12-30.parquet')
        assert len(result) >= 450, "Less than 450 stocks in output"
        assert len(result.columns) >= 98, "Less than 98 features in Phase 1 output"

    def test_psi_alert_triggers_on_severe_drift(self, drifted_features, baseline_stats):
        """PSI > 0.25 should trigger halt_new_positions flag."""
        report = quality_check(drifted_features, baseline_stats)
        if report['psi_severe']:
            assert report['pipeline_ok'] == False
            assert report.get('halt_new_positions') == True
```

---

## T-08: Drift Monitoring Tests

```python
# tests/test_drift_monitor.py

class TestPSIDrift:
    def test_psi_zero_for_same_distribution(self):
        """PSI should be ~0 when distributions are identical."""
        data = np.random.normal(0, 1, 1000)
        psi = compute_psi(pd.Series(data[:500]), pd.Series(data[500:]))
        assert psi < 0.05

    def test_psi_high_for_shifted_distribution(self):
        """PSI should be > 0.25 when distribution shifts significantly."""
        base = pd.Series(np.random.normal(0, 1, 1000))
        shifted = pd.Series(np.random.normal(3, 1, 1000))  # 3σ shift
        psi = compute_psi(base, shifted)
        assert psi > 0.25, f"PSI {psi:.3f} should be > 0.25 for 3σ shift"

    def test_retrain_triggered_on_severe_psi(self, mock_model_registry):
        """Severe PSI breach should create retrain task."""
        monitor = DriftMonitor(mock_model_registry)
        features_with_drift = create_drifted_features(psi=0.30)
        monitor.check(features_with_drift)
        assert mock_model_registry.retrain_requested == True
```
