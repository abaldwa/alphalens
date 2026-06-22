# AlphaLens — Feature Specification
## 330 Features Across 3 Phases

All features are computed per-stock per-day and stored in the daily Parquet feature matrix.
Mixed-frequency data (quarterly, monthly) is forward-filled from latest available date.
**Point-in-time rule:** never use data not yet publicly available on the feature date.

---

## Phase 1 Features — 98 Total (OHLCV + free data only)

### Category 1: Price Position & Range (8)
| Feature | Formula | Lookback |
|---------|---------|----------|
| `pct_rank_5d` | (close - rolling_low) / (rolling_high - rolling_low) | 5d |
| `pct_rank_21d` | Same | 21d |
| `pct_rank_63d` | Same | 63d |
| `pct_rank_252d` | Same | 252d |
| `pct_rank_504d` | Same | 504d |
| `pct_rank_all_time` | Expanding window | All |
| `dist_from_52w_high` | (close - max_252d) / max_252d | 252d |
| `dist_from_52w_low` | (close - min_252d) / min_252d | 252d |

### Category 2: SMA Ratios (8)
| Feature | Formula |
|---------|---------|
| `close_sma20_ratio` | close / SMA(20) |
| `close_sma50_ratio` | close / SMA(50) |
| `close_sma100_ratio` | close / SMA(100) |
| `close_sma200_ratio` | close / SMA(200) |
| `sma20_sma50_ratio` | SMA(20) / SMA(50) |
| `sma50_sma100_ratio` | SMA(50) / SMA(100) |
| `sma50_sma200_ratio` | SMA(50) / SMA(200) |
| `close_sma200_weekly` | close / SMA(weekly_close, 50) |

### Category 3: EMA Ratios (4)
| Feature | Formula |
|---------|---------|
| `close_ema21_ratio` | close / EMA(21) |
| `close_ema55_ratio` | close / EMA(55) |
| `ema21_ema55_ratio` | EMA(21) / EMA(55) |
| `sma_ema_spread_50` | SMA(50) / EMA(55) - 1 |

### Category 4: Momentum Oscillators (9)
| Feature | Formula |
|---------|---------|
| `rsi_14` | RSI(14) |
| `rsi_14_delta_5d` | RSI(14)[today] - RSI(14)[5d ago] |
| `rsi_weekly` | RSI(14) on weekly closes, forward-filled |
| `mfi_14` | Money Flow Index(14) |
| `cci_20` | Commodity Channel Index(20) |
| `stoch_k` | Stochastic %K(14,3) |
| `stoch_k_minus_d` | %K - %D |
| `williams_r` | Williams %R(14) |
| `roc_10` | Rate of Change(10) |

### Category 5: Trend Strength (8)
| Feature | Formula |
|---------|---------|
| `macd_histogram` | MACD line - Signal line |
| `macd_line` | EMA(12) - EMA(26) |
| `macd_histogram_delta` | histogram[today] - histogram[yesterday] |
| `adx_14` | ADX(14) Wilder's method |
| `plus_di_minus_di` | +DI(14) - (-DI(14)) |
| `supertrend_dist` | (close - supertrend) / close |
| `supertrend_weekly_dist` | Weekly supertrend distance |
| `psar_dist` | (close - PSAR) / close |

### Category 6: Volatility (5)
| Feature | Formula |
|---------|---------|
| `atr_14` | ATR(14) / close × 100 |
| `bb_position` | (close - BB_lower) / (BB_upper - BB_lower) |
| `bb_width` | (BB_upper - BB_lower) / BB_middle |
| `hist_vol_21d` | Annualized std(log returns, 21d) |
| `vol_ratio_21d_63d` | hist_vol_21d / hist_vol_63d |

### Category 7: Relative Strength (5)
| Feature | Formula |
|---------|---------|
| `rs_vs_benchmark_21d` | stock_return_21d - nifty_return_21d |
| `rs_vs_benchmark_63d` | stock_return_63d - nifty_return_63d |
| `rs_vs_benchmark_252d` | stock_return_252d - nifty_return_252d |
| `rs_vs_sector_21d` | stock_return_21d - sector_return_21d |
| `rs_vs_sector_63d` | stock_return_63d - sector_return_63d |

### Category 8: Momentum Scores (5)
| Feature | Formula |
|---------|---------|
| `momentum_3m` | close / close[63d ago] - 1 |
| `momentum_6m` | close / close[126d ago] - 1 |
| `momentum_12m` | close / close[252d ago] - 1 |
| `momentum_12m_skip_1m` | close[21d ago] / close[252d ago] - 1 |
| `momentum_quality` | momentum_12m / hist_vol_252d |

### Category 9: Volume & Delivery (5)
| Feature | Formula |
|---------|---------|
| `delivery_pct` | delivery_qty / traded_qty × 100 |
| `delivery_pct_zscore` | z-score of delivery_pct (63d window) |
| `delivery_qty_ratio` | delivery_qty / SMA(delivery_qty, 20) |
| `volume_ratio` | volume / SMA(volume, 20) |
| `weekly_delivery_pct` | 5-day avg delivery% |

### Category 10: Ichimoku Cloud (5)
| Feature | Formula |
|---------|---------|
| `close_tenkan_ratio` | close / Tenkan-sen (9-period) |
| `close_kijun_ratio` | close / Kijun-sen (26-period) |
| `senkou_span_a_dist` | (close - Senkou A) / close |
| `senkou_span_b_dist` | (close - Senkou B) / close |
| `cloud_thickness` | (Senkou A - Senkou B) / close |

### Category 11: Derived / Engineered (8)
| Feature | Formula |
|---------|---------|
| `trend_alignment_score` | Count of: price > SMA20, SMA50, SMA200 + RSI > 50 + MACD positive (0–5) |
| `mean_reversion_signal` | (RSI - 50) / 50 × -1 |
| `vol_price_divergence` | sign(volume ratio - 1) × -sign(return_21d) |
| `multi_tf_momentum` | 0.5×momentum_3m + 0.3×momentum_6m + 0.2×momentum_12m |
| `return_5d` | close / close[5d ago] - 1 |
| `return_21d` | close / close[21d ago] - 1 |
| `return_63d` | close / close[63d ago] - 1 |
| `volatility_regime` | Percentile rank of hist_vol_21d vs rolling distribution |

### Category 12: Intraday Patterns from OHLC (8)
| Feature | Formula |
|---------|---------|
| `gap_up_pct` | (open - prev_close) / prev_close × 100 (positive) |
| `gap_down_pct` | Same (negative direction) |
| `intraday_reversal_score` | (close - open) / (high - low) → +1=bullish, -1=bearish |
| `upper_shadow_pct` | (high - max(open,close)) / (high - low) |
| `lower_shadow_pct` | (min(open,close) - low) / (high - low) |
| `body_to_range_ratio` | abs(close - open) / (high - low) |
| `close_position_in_range` | (close - low) / (high - low) |
| `opening_drive_strength` | Proxy from OHLC (see features/intraday.py) |

### Category 13: Calendar & Seasonal (7)
| Feature | Notes |
|---------|-------|
| `month_of_year` | 1–12, encode as sin/cos pair |
| `day_of_week` | 1–5, encode as sin/cos pair |
| `days_to_expiry` | Trading days to monthly F&O expiry |
| `budget_proximity` | Days to/from Feb 1 Union Budget |
| `quarter_end_proximity` | Days to quarter end |
| `election_proximity` | Days to nearest known election date |
| `earnings_season_flag` | 1 if within earnings announcement window |

### Category 14: Macro & Market Context (14)
| Feature | Source | Update |
|---------|--------|--------|
| `nifty_pct_rank_252d` | Nifty 50 price position in 52-week range | Daily |
| `nifty_hmm_regime` | HMM regime label for Nifty 50 | Daily |
| `india_vix` | India VIX level | Daily |
| `india_vix_pctile` | VIX percentile rank (252d) | Daily |
| `fii_net_flow_21d` | 21d cumulative FII cash market flow | Daily |
| `dii_net_flow_21d` | 21d cumulative DII cash market flow | Daily |
| `advance_decline_ratio_5d` | 5d avg advance/decline ratio | Daily |
| `sector_rotation_score` | Relative performance of stock's sector | Daily |
| `us_market_overnight` | S&P 500 overnight return | Daily |
| `crude_oil_change_21d` | 21d change in Brent crude | Daily |
| `usd_inr_change_21d` | 21d change in USD/INR | Daily |
| `bond_yield_10yr` | India 10-year government bond yield | Daily |
| `yield_spread_change` | Change in 10yr-2yr yield spread | Daily |
| `market_breadth_52w` | % of stocks above 52-week high in universe | Daily |

### P&D Detection Features (22) — Phase 1
These are computed daily and used as both a pre-filter and as features for the signal models.

| Feature | Formula |
|---------|---------|
| `volume_spike_magnitude` | volume / SMA(volume, 20) |
| `volume_spike_persistence` | days in last 10 with volume > 3x avg |
| `volume_acceleration` | rate of change of volume over 10d |
| `volume_price_asymmetry` | volume spike without proportional price move |
| `consecutive_upper_circuits` | count of UC hits in last 10 days |
| `price_velocity_5d` | annualized 5d return magnitude |
| `price_velocity_21d` | annualized 21d return magnitude |
| `parabolic_curve_score` | R² of exponential fit to last 30d price |
| `gap_up_clustering` | count of gap-ups > 3% in last 10 days |
| `price_above_all_mas` | binary: above all of SMA20/50/100/200 |
| `intraday_volatility_spike` | (high-low)/open vs 20d average |
| `delivery_pct_collapse` | current delivery% / 20d avg delivery% |
| `delivery_volume_divergence` | volume up but delivery down |
| `traded_qty_delivery_gap` | trend of (traded - delivery) |
| `avg_daily_turnover_pre_move` | ADT before volume spike began |
| `free_float_mcap` | free float market cap (manipulation risk proxy) |
| `mcap_velocity` | rate of change of market cap |
| `illiquidity_amihud` | Amihud illiquidity ratio |
| `asm_flag` | NSE Additional Surveillance Measure (binary) |
| `gsm_flag` | NSE Graded Surveillance Measure (binary) |
| `circuit_limit_proximity` | price / upper circuit limit |
| `small_trade_ratio` | proxy from turnover/volume |

---

## Phase 2 Features — +170 (Total: 268)

### Core Fundamental Features (28) — Screener.in
All use `announcement_date` for point-in-time alignment. Forward-filled daily.

| Feature | Update |
|---------|--------|
| `revenue_growth_yoy` | Quarterly |
| `revenue_growth_qoq` | Quarterly |
| `ebitda_growth_yoy` | Quarterly |
| `eps_growth_yoy` | Quarterly |
| `operating_profit_growth_yoy` | Quarterly |
| `revenue_growth_3yr_cagr` | Quarterly |
| `operating_margin` | Quarterly |
| `operating_margin_change_yoy` | Quarterly |
| `ebitda_margin` | Quarterly |
| `net_profit_margin` | Quarterly |
| `roe` | Quarterly |
| `roce` | Quarterly |
| `fcf_conversion` | Quarterly |
| `debt_to_equity` | Quarterly |
| `debt_to_equity_change` | Quarterly |
| `interest_coverage` | Quarterly |
| `cash_flow_to_debt` | Quarterly |
| `asset_turnover` | Quarterly |
| `inventory_days` | Quarterly |
| `receivable_days` | Quarterly |
| `payable_days` | Quarterly |
| `cash_conversion_cycle` | Quarterly |
| `working_capital_change` | Quarterly |
| `pe_ratio` | Daily (uses today's price + latest EPS) |
| `price_to_book` | Daily |
| `peg_ratio` | Quarterly |
| `ev_to_ebitda` | Daily |
| `mcap_to_sales` | Daily |

### Staleness Features (3) — Derived from announcement dates
| Feature | Formula |
|---------|---------|
| `days_since_results` | trading days since announcement_date |
| `quarter_age_pct` | days_since_results / 63 (normalized 0–1) |
| `results_pending_flag` | 1 if days_since_results > 70 |

### Governance Features (12) — BSE Shareholding
All use `filing_date` (available ~21 days after quarter end).

| Feature |
|---------|
| `promoter_holding` |
| `promoter_holding_change_1q` |
| `promoter_holding_change_4q` |
| `promoter_pledge` |
| `promoter_pledge_change_1q` |
| `fii_holding` |
| `fii_holding_change_1q` |
| `dii_holding` |
| `dii_holding_change_1q` |
| `mf_holding` |
| `mf_holding_change_1q` |
| `institutional_total_change` |

### MF Portfolio Holdings (12) — AMFI Monthly
Available from ~5th of following month. Use this date for PIT alignment.

| Feature | Signal |
|---------|--------|
| `mf_scheme_count` | Institutional breadth |
| `mf_scheme_count_change_1m` | Discovery phase if rising |
| `mf_total_holding_change_1m` | Net MF buying/selling |
| `mf_smallcap_fund_holding` | Institutional quality stamp |
| `mf_new_entry_count` | Fresh institutional discovery |
| `mf_exit_count` | Eroding confidence |
| `mf_concentration_top5` | Fragility of institutional position |
| `mf_avg_holding_period` | Sticky vs speculative money |
| `mf_sip_inflow_proxy` | Systematic buying pressure |
| `superstar_investor_flag` | Binary: ace investor holds this |
| `superstar_investor_change` | +1 increased, -1 decreased |
| `mf_crowdedness_rank` | Percentile within market-cap tier |

### Corporate Action Features (10)
| Feature |
|---------|
| `days_to_record_date` |
| `corp_action_anticipation_return` |
| `buyback_price_spread` |
| `buyback_acceptance_estimated` |
| `index_inclusion_days` |
| `ipo_lockin_expiry_proximity` |
| `ipo_listing_age_months` |
| `post_earnings_drift_signal` |
| `dividend_yield_vs_fd_rate` |
| `qip_dilution_impact` |

### F&O Derivative Features (16) — NSE F&O + Option Chain (F&O stocks only)
| Feature |
|---------|
| `pcr_oi`, `pcr_oi_change_5d` |
| `max_pain_distance` |
| `futures_oi_change_pct` |
| `futures_basis`, `futures_basis_change_5d` |
| `iv_atm`, `iv_skew`, `iv_term_structure` |
| `oi_concentration_call`, `oi_concentration_put` |
| `long_buildup_flag`, `short_buildup_flag` |
| `short_covering_flag`, `long_unwinding_flag` |
| `rollover_pct` |

### Market Microstructure (8)
| Feature |
|---------|
| `bulk_deal_flag`, `block_deal_flag` |
| `bulk_deal_net_buy_pct` |
| `insider_trade_net_30d` |
| `asm_gsm_flag` |
| `circuit_limit_proximity` |
| `turnover_ratio` |
| `impact_cost` |

### Multibagger-Specific Features (33)
Base formation (6): `base_length_days`, `base_tightness`, `price_compression`,
`higher_lows_count`, `lower_highs_count`, `consolidation_breakout_ratio`

Recovery (2): `dist_from_ath_pct`, `recovery_from_low_pct`

Volume accumulation (7): `obv_slope_63d`, `obv_price_divergence`, `accumulation_days_ratio`,
`distribution_days_ratio`, `volume_trend_ratio`, `delivery_accumulation`, `quiet_volume_score`

Relative strength (5): `rs_benchmark_126d`, `rs_benchmark_252d`, `rs_improving_6m`,
`sector_rs_rank`, `universe_rs_rank`

Multi-timeframe (2): `weekly_macd_histogram`, `monthly_rsi`

Trend quality (5): `sma50_slope`, `sma200_slope`, `trend_duration_up`, `golden_cross_age`,
`sma50_above_sma200_days`

Volatility compression (4): `vol_contraction_ratio`, `atr_contraction`, `bb_width_contraction`,
`range_contraction_weeks`

Price behavior (3): `gap_up_frequency`, `gap_down_frequency`, `post_gap_follow_through`

### Seasonality Features (8)
`diwali_muhurat_proximity`, `budget_anticipation`, `rbi_policy_proximity`,
`tax_loss_harvesting_window`, `advance_tax_date_proximity`, `fund_manager_window_dressing`,
`election_state_proximity`, `monsoon_phase`

### Classical Forensic Scores (30) — Formula-based, no ML
Beneish components (8 + composite): DSRI, GMI, AQI, SGI, DEPI, SGAI, LVGI, TATA + M-Score
Benford's Law (5): `benford_revenue_chi2`, `benford_expense_chi2`, `benford_receivables_chi2`,
`benford_overall_deviation`, `benford_mad`
Altman Z-Score (5 components + composite)
Piotroski F-Score (9 binary components → 0–9 score)
Cash flow quality (4): `cfo_to_net_income`, `accrual_ratio`, `capex_to_cfo_ratio`,
`cash_flow_quality_score`
Dechow F-Score (composite)

---

## Phase 3 Features — +62 (Total: 330)

### Deep Forensic ML Features (54) — Groups B–H
Group B: Cash flow quality deep (9 additional)
Group C: Revenue quality deep (7 additional)
Group D: Balance sheet quality (12): goodwill/assets, intangibles, contingent liabilities
Group E: India governance & promoter risk (15): promoter remuneration%, RPT growth,
board independence, auditor flags, director churn, CFO tenure, pledge spiral risk
Group H: Cross-validation & consistency (10): tax rate consistency, employee cost,
statutory dues, segment consistency

### Real Economy Macro (10)
`gst_collection_growth`, `pmi_manufacturing`, `pmi_services`, `iip_growth`,
`auto_monthly_sales`, `cement_dispatches`, `power_consumption_growth`,
`rail_freight_growth`, `upi_transaction_growth`, `bank_credit_growth`

### Advanced Technical (20)
Wavelet decomposition (4): `wavelet_trend`, `wavelet_cycle`, `wavelet_noise`, `wavelet_energy_ratio`
Fractional differentiation (3): `close_fracdiff`, `volume_fracdiff`, `delivery_qty_fracdiff`
Complexity & entropy (7): `hurst_exponent_63d`, `hurst_exponent_252d`, `approx_entropy_21d`,
`sample_entropy_63d`, `fractal_dimension_63d`, `predictability_score`, `lyapunov_exponent_proxy`
Pattern recognition (6): `head_shoulders_score`, `double_bottom_score`, `cup_handle_score`,
`support_strength`, `resistance_strength`, `trendline_slope`

---

## Feature Engineering Implementation Notes

### Vectorized computation (required)
```python
# CORRECT — vectorized across all stocks
features_df = ohlcv_df.groupby('ticker').apply(compute_features)

# WRONG — never loop over stocks
for ticker in tickers:
    features[ticker] = compute_features(ohlcv_df[ohlcv_df['ticker'] == ticker])
```

### Minimum history requirement
Features requiring N days of lookback will return NaN if insufficient history.
Use `min_history=252` as the minimum before any stock is included in model input.

### Cyclical encoding for calendar features
```python
import numpy as np
df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
```

### Sector-relative z-score normalization (required for fundamental features)
```python
df['roe_sector_zscore'] = df.groupby('sector')['roe'].transform(
    lambda x: (x - x.mean()) / (x.std() + 1e-8)
)
```
