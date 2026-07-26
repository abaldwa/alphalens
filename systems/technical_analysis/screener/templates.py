"""
systems/technical_analysis/screener/templates.py

Phase: 3.x (Technical Analysis Screener)
Specs: SPEC-TA-005
Owner: Technical Analysis / Screener
Consumers: systems/technical_analysis/screener/engine.py,
           systems/technical_analysis/alerts/daily_alert_checker.py

42 named screener templates mapping to features already computed and stored
in the daily feature Parquet (config.settings.FEATURES_DAILY_DIR).

All feature names used here are ACTUAL column names present in the Parquet:
  - Core technical (70 cols): sma_200_ratio, volume_ratio_21d, macd_hist,
    adx_14, rsi_14, roc_10, bb_width_pct, ema_ribbon_alignment, etc.
    (from features/technical.py::CORE_TECHNICAL_FEATURES)
  - Advanced (18 cols): hurst_exp_21d, wavelet_trend, etc.
    (from features/advanced_technical.py::ADVANCED_TECHNICAL_FEATURES)
  - Pattern scores (6 cols): base_breakout_score, double_bottom_score,
    cup_handle_score, flag_pattern_score, wedge_score, head_shoulders_score
    (from features/pattern_scores.py::PATTERN_FEATURES)

Column reference rationale (key mappings from strategy concept → stored column):
  - "close > SMA200"  → sma_200_ratio > 1.0   (close/sma200; ratio >1 = above)
  - "close > SMA50"   → sma_50_ratio > 1.0    (close/sma50)
  - "volume_ratio_20d"→ volume_ratio_21d       (computed on 21-day SMA, nearest proxy)
  - "macd_histogram"  → macd_hist              (TA-Lib output name)
  - "hurst_exp"       → hurst_exp_21d          (no adv_ prefix in actual features)
  - "breakout_prob"   → base_breakout_score    (close vs prior 21d high breakout)
  - "reversal_prob"   → double_bottom_score    (double-bottom reversal pattern)
  - "trend_strength"  → flag_pattern_score     (flag/continuation pattern)
  - "close/52w_high"  → base_breakout_ratio    (close/prior_21d_high; nearest proxy)
  - "EMA ribbon"      → ema_ribbon_alignment   (composite: +1 = all EMAs in order)
  - "supertrend bull" → supertrend_dir > 0     (direction; signal is only on-flip)
  - "ichimoku bull"   → ichimoku_cloud_position > 0 (above cloud)

Supported ops in condition dicts:
  - "lt", "gt", "lte", "gte", "eq": column vs scalar
  - "between": column in [lo, hi] (value = [lo, hi])
  - "top_pct": column >= quantile(1-value) across the universe (cross-sectional)
  - "bottom_pct": column <= quantile(value) across the universe (cross-sectional)

No stdlib, pandas, or project imports — pure data structures so this module
loads instantly without any external dependencies.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ScreenerTemplate:
    """One named screener strategy with its conditions and display metadata.

    Parameters
    ----------
    name : str
        Unique identifier, e.g. "A1", "E2", "S004".
    category : str
        One-letter category code (A-F, S).
    description : str
        Human-readable strategy name.
    conditions : list of dict
        Each dict has at minimum {"feature": str, "op": str} and either
        {"value": scalar/list} or {"feature2": str} for col-vs-col ops.
    key_display_features : list of str
        Ordered list of feature column names to include in ScreenerResult.key_values.

    Spec References
    ---------------
    SPEC-TA-005: Custom Technical Screener with 42 Pre-Built Templates
    """

    name: str
    category: str
    description: str
    conditions: List[Dict[str, Any]] = field(default_factory=list)
    key_display_features: List[str] = field(default_factory=list)
    # Per-template exit params for PerTemplateExitPolicy (systems/
    # ml_signal_engine/models/exit/per_template_exit_policy.py). None by
    # default (untagged/legacy construction); populated in bulk below,
    # by TEMPLATE_STYLE group, once TEMPLATE_STYLE exists — see
    # STYLE_EXIT_PARAMS and the assignment loop at the bottom of this
    # module. Kept as plain Optional fields (not required at
    # construction) so this dataclass stays a pure data structure with
    # no import-order dependency on the style table below.
    exit_stop_pct: Optional[float] = None
    exit_target_pct: Optional[float] = None
    exit_max_hold_days: Optional[int] = None


# ---------------------------------------------------------------------------
# Category A — Technical Momentum (4 templates)
# ---------------------------------------------------------------------------

_A1 = ScreenerTemplate(
    name="A1",
    category="A",
    description="BB Squeeze Breakout",
    conditions=[
        # BB Width in bottom 25% of universe (volatility squeeze)
        {"feature": "bb_width_pct", "op": "bottom_pct", "value": 0.25},
        # Close above 200-day SMA (sma_200_ratio = close/sma200; >1 = above)
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
        # Volume surge confirming breakout
        {"feature": "volume_ratio_21d", "op": "gt", "value": 1.8},
    ],
    key_display_features=["bb_width_pct", "sma_200_ratio", "volume_ratio_21d", "rsi_14"],
)

_A2 = ScreenerTemplate(
    name="A2",
    category="A",
    description="MACD Histogram Divergence",
    conditions=[
        # MACD histogram positive (macd_line > macd_signal)
        {"feature": "macd_hist", "op": "gt", "value": 0},
        # RSI not oversold — momentum building
        {"feature": "rsi_14", "op": "gte", "value": 40},
    ],
    key_display_features=["macd_hist", "rsi_14", "volume_ratio_21d", "sma_200_ratio"],
)

_A3 = ScreenerTemplate(
    name="A3",
    category="A",
    description="Williams %R Mean Reversion",
    conditions=[
        # Williams %R in oversold zone (<= -85)
        {"feature": "williams_r", "op": "lt", "value": -85},
        # But price is still in an uptrend (above SMA200)
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
    ],
    key_display_features=["williams_r", "sma_200_ratio", "rsi_14", "volume_ratio_21d"],
)

_A4 = ScreenerTemplate(
    name="A4",
    category="A",
    description="RSI Oversold + Trend",
    conditions=[
        # RSI in oversold territory
        {"feature": "rsi_14", "op": "lt", "value": 30},
        # Price above SMA200 (long-term trend is still up)
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
    ],
    key_display_features=["rsi_14", "sma_200_ratio", "volume_ratio_21d", "adx_14"],
)

# ---------------------------------------------------------------------------
# Category B — Price Action (5 templates)
# ---------------------------------------------------------------------------

_B1 = ScreenerTemplate(
    name="B1",
    category="B",
    description="Weinstein Stage 2",
    conditions=[
        # Close above 200-day SMA (Stage 2 characteristic)
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
        # Volume expansion (RS new high proxy via volume)
        {"feature": "volume_ratio_21d", "op": "gt", "value": 1.5},
        # ADX confirms trend is strong
        {"feature": "adx_14", "op": "gt", "value": 20},
    ],
    key_display_features=["sma_200_ratio", "volume_ratio_21d", "adx_14", "rsi_14"],
)

_B2 = ScreenerTemplate(
    name="B2",
    category="B",
    description="IBD Base Breakout",
    conditions=[
        # Close near recent high (base_breakout_ratio = close/prior_21d_high)
        {"feature": "base_breakout_ratio", "op": "gt", "value": 0.97},
        # Volume confirmation (≥40% above avg)
        {"feature": "volume_ratio_21d", "op": "gt", "value": 1.4},
    ],
    key_display_features=["base_breakout_ratio", "volume_ratio_21d", "rsi_14", "adx_14"],
)

_B3 = ScreenerTemplate(
    name="B3",
    category="B",
    description="Darvas Box",
    conditions=[
        # Price near box-high (recent 21d high proxy)
        {"feature": "base_breakout_ratio", "op": "gt", "value": 0.98},
        # ADX confirms trending environment
        {"feature": "adx_14", "op": "gt", "value": 20},
        # Volume confirms breakout
        {"feature": "volume_ratio_21d", "op": "gt", "value": 1.5},
    ],
    key_display_features=["base_breakout_ratio", "adx_14", "volume_ratio_21d", "rsi_14"],
)

_B4 = ScreenerTemplate(
    name="B4",
    category="B",
    description="AVWAP Support",
    conditions=[
        # RSI in neutral zone (VWAP pullback without trend exhaustion)
        {"feature": "rsi_14", "op": "between", "value": [40, 60]},
        # Double-bottom / reversal pattern forming
        {"feature": "double_bottom_score", "op": "gt", "value": 0.4},
    ],
    key_display_features=["rsi_14", "double_bottom_score", "volume_ratio_21d", "sma_200_ratio"],
)

_B5 = ScreenerTemplate(
    name="B5",
    category="B",
    description="Livermore Pivot",
    conditions=[
        # Strong breakout probability score
        {"feature": "base_breakout_score", "op": "gt", "value": 0.5},
        # Volume surge confirming the pivot
        {"feature": "volume_ratio_21d", "op": "gt", "value": 2.0},
    ],
    key_display_features=["base_breakout_score", "volume_ratio_21d", "rsi_14", "adx_14"],
)

# ---------------------------------------------------------------------------
# Category C — Momentum (7 templates)
# ---------------------------------------------------------------------------

_C1 = ScreenerTemplate(
    name="C1",
    category="C",
    description="Time Series Momentum",
    conditions=[
        # Close above SMA200 (absolute momentum — uptrend)
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
        # Positive 10-day return (time-series momentum)
        {"feature": "roc_10", "op": "gt", "value": 0},
    ],
    key_display_features=["sma_200_ratio", "roc_10", "composite_momentum_63d", "adx_14"],
)

_C2 = ScreenerTemplate(
    name="C2",
    category="C",
    description="Cross-Sectional Momentum",
    conditions=[
        # Top 20% by 10-day return (cross-sectional momentum filter)
        {"feature": "roc_10", "op": "top_pct", "value": 0.2},
    ],
    key_display_features=["roc_10", "composite_momentum_21d", "sma_200_ratio", "volume_ratio_21d"],
)

_C3 = ScreenerTemplate(
    name="C3",
    category="C",
    description="Dual Momentum",
    conditions=[
        # Absolute momentum: positive 10-day return
        {"feature": "roc_10", "op": "gt", "value": 0},
        # Trend filter: above SMA200
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
    ],
    key_display_features=["roc_10", "sma_200_ratio", "composite_momentum_63d", "rs_vs_nifty500_21d"],
)

_C4 = ScreenerTemplate(
    name="C4",
    category="C",
    description="CAN SLIM proxy",
    conditions=[
        # Above SMA200 (L — market direction)
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
        # Volume expansion (I — institutional buying)
        {"feature": "volume_ratio_21d", "op": "gt", "value": 1.5},
        # Trending (N — new highs)
        {"feature": "adx_14", "op": "gt", "value": 20},
        # RSI above 50 (momentum)
        {"feature": "rsi_14", "op": "gt", "value": 50},
    ],
    key_display_features=["sma_200_ratio", "volume_ratio_21d", "adx_14", "rsi_14"],
)

_C5 = ScreenerTemplate(
    name="C5",
    category="C",
    description="52-Week High Proximity",
    conditions=[
        # Near recent high (base_breakout_ratio = close/prior_21d_high; 52w proxy)
        {"feature": "base_breakout_ratio", "op": "gt", "value": 0.99},
        # Volume surge (institutional accumulation at highs)
        {"feature": "volume_ratio_21d", "op": "gt", "value": 2.0},
        # ADX confirms trending
        {"feature": "adx_14", "op": "gte", "value": 20},
    ],
    key_display_features=["base_breakout_ratio", "volume_ratio_21d", "adx_14", "rsi_14"],
)

_C6 = ScreenerTemplate(
    name="C6",
    category="C",
    description="EMA Ribbon Alignment",
    conditions=[
        # All 3 EMA pairs in bullish order (ema8 > ema21 > ema55 > ema89)
        # ema_ribbon_alignment = (sign(ema8-ema21) + sign(ema21-ema55) + sign(ema55-ema89)) / 3
        # Value of 1.0 means fully bullish aligned; use >= 1.0
        {"feature": "ema_ribbon_alignment", "op": "gte", "value": 1.0},
        # Close above EMA8 (ema_8_ratio = close/ema8; > 1 = above)
        {"feature": "ema_8_ratio", "op": "gt", "value": 1.0},
    ],
    key_display_features=["ema_ribbon_alignment", "ema_8_ratio", "rsi_14", "adx_14"],
)

_C7 = ScreenerTemplate(
    name="C7",
    category="C",
    description="Post-Earnings Drift",
    conditions=[
        # Volume surge (earnings announcement proxy)
        {"feature": "volume_ratio_21d", "op": "gt", "value": 2.0},
        # Positive price reaction (>2% in 10 days)
        {"feature": "roc_10", "op": "gt", "value": 0.02},
        # Breakout probability confirms
        {"feature": "base_breakout_score", "op": "gt", "value": 0.4},
    ],
    key_display_features=["volume_ratio_21d", "roc_10", "base_breakout_score", "rsi_14"],
)

# ---------------------------------------------------------------------------
# Category D — Reversal (4 templates)
# ---------------------------------------------------------------------------

_D1 = ScreenerTemplate(
    name="D1",
    category="D",
    description="RSI-2 Mean Reversion",
    conditions=[
        # Connors RSI(2) oversold: use rsi_14 < 10 as proxy (rsi_2 not stored separately)
        {"feature": "rsi_14", "op": "lt", "value": 10},
        # Long-term uptrend (above SMA200)
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
    ],
    key_display_features=["rsi_14", "sma_200_ratio", "macd_hist", "volume_ratio_21d"],
)

_D2 = ScreenerTemplate(
    name="D2",
    category="D",
    description="Long-Horizon Contrarian",
    conditions=[
        # Oversold RSI
        {"feature": "rsi_14", "op": "lt", "value": 35},
        # Well below recent high (contrarian — opposite of breakout)
        {"feature": "base_breakout_ratio", "op": "lt", "value": 0.7},
    ],
    key_display_features=["rsi_14", "base_breakout_ratio", "sma_200_ratio", "volume_ratio_21d"],
)

_D3 = ScreenerTemplate(
    name="D3",
    category="D",
    description="MACD + RSI Divergence",
    conditions=[
        # MACD histogram turning positive while RSI still low → dual divergence
        {"feature": "macd_hist", "op": "gt", "value": 0},
        # RSI still not overbought (divergence window)
        {"feature": "rsi_14", "op": "lt", "value": 45},
        # Reversal pattern forming
        {"feature": "double_bottom_score", "op": "gt", "value": 0.4},
    ],
    key_display_features=["macd_hist", "rsi_14", "double_bottom_score", "volume_ratio_21d"],
)

_D4 = ScreenerTemplate(
    name="D4",
    category="D",
    description="IBD Follow-Through Day",
    conditions=[
        # Market bouncing (≥1.7% in 10 days — IBD follow-through threshold)
        {"feature": "roc_10", "op": "gt", "value": 0.017},
        # Volume surge confirming accumulation
        {"feature": "volume_ratio_21d", "op": "gt", "value": 1.5},
        # Breakout pattern forming
        {"feature": "base_breakout_score", "op": "gt", "value": 0.4},
    ],
    key_display_features=["roc_10", "volume_ratio_21d", "base_breakout_score", "adx_14"],
)

# ---------------------------------------------------------------------------
# Category E — Trend Following & Systematic (7 templates; E8 = alias of C6, excluded)
# ---------------------------------------------------------------------------

_E1 = ScreenerTemplate(
    name="E1",
    category="E",
    description="Turtle Donchian",
    conditions=[
        # Near recent high (Donchian 20-day channel proxy via base_breakout_ratio)
        {"feature": "base_breakout_ratio", "op": "gt", "value": 0.99},
        # ADX confirms trend is established
        {"feature": "adx_14", "op": "gte", "value": 15},
    ],
    key_display_features=["base_breakout_ratio", "adx_14", "atr_14_pct", "volume_ratio_21d"],
)

_E2 = ScreenerTemplate(
    name="E2",
    category="E",
    description="Minervini SEPA",
    conditions=[
        # Above SMA50 (SEPA criterion: close > 50-day SMA)
        {"feature": "sma_50_ratio", "op": "gt", "value": 1.0},
        # Above SMA200 (SEPA criterion: close > 200-day SMA)
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
        # ADX > 20 (trending, not ranging)
        {"feature": "adx_14", "op": "gt", "value": 20},
        # Above-average volume (institutional sponsorship)
        {"feature": "volume_ratio_21d", "op": "gt", "value": 1.0},
        # RSI > 50 (bullish momentum)
        {"feature": "rsi_14", "op": "gt", "value": 50},
    ],
    key_display_features=["sma_50_ratio", "sma_200_ratio", "adx_14", "rsi_14"],
)

_E3 = ScreenerTemplate(
    name="E3",
    category="E",
    description="Piotroski F proxy",
    conditions=[
        # Flag pattern (continuation) as fundamental quality proxy
        # Real Piotroski F requires fundamentals; flag pattern scores proxy trend quality
        {"feature": "flag_pattern_score", "op": "gt", "value": 0.6},
        # Hurst exponent > 0.5 indicates trending (persistent) behaviour → quality proxy
        {"feature": "hurst_exp_21d", "op": "gt", "value": 0.5},
    ],
    key_display_features=["flag_pattern_score", "hurst_exp_21d", "sma_200_ratio", "adx_14"],
)

_E4 = ScreenerTemplate(
    name="E4",
    category="E",
    description="Sector Rotation",
    conditions=[
        # Trending (flag pattern indicates sector rotation momentum)
        {"feature": "flag_pattern_score", "op": "gt", "value": 0.5},
        # Above SMA50 (recent strength in rotation)
        {"feature": "sma_50_ratio", "op": "gt", "value": 1.0},
    ],
    key_display_features=["flag_pattern_score", "sma_50_ratio", "rs_vs_nifty500_21d", "adx_14"],
)

_E5 = ScreenerTemplate(
    name="E5",
    category="E",
    description="Earnings Acceleration",
    conditions=[
        # Volume surge (earnings acceleration proxy)
        {"feature": "volume_ratio_21d", "op": "gt", "value": 1.5},
        # Strong 10-day return (acceleration in price)
        {"feature": "roc_10", "op": "gt", "value": 0.05},
        # ADX confirms new trend
        {"feature": "adx_14", "op": "gt", "value": 20},
    ],
    key_display_features=["volume_ratio_21d", "roc_10", "adx_14", "rsi_14"],
)

_E6 = ScreenerTemplate(
    name="E6",
    category="E",
    description="GARP Momentum",
    conditions=[
        # Above SMA200 (quality + uptrend filter)
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
        # RSI in bullish but not overbought range (50-70 = GARP sweet spot)
        {"feature": "rsi_14", "op": "between", "value": [50, 70]},
    ],
    key_display_features=["sma_200_ratio", "rsi_14", "composite_momentum_21d", "adx_14"],
)

_E7 = ScreenerTemplate(
    name="E7",
    category="E",
    description="Greenblatt Magic Formula proxy",
    conditions=[
        # High trend quality (flag pattern proxy for earnings yield + ROIC rank)
        {"feature": "flag_pattern_score", "op": "gt", "value": 0.6},
        # Above SMA200 (quality filter)
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
    ],
    key_display_features=["flag_pattern_score", "sma_200_ratio", "rsi_14", "hurst_exp_21d"],
)

# ---------------------------------------------------------------------------
# Category F — Fundamental proxies (8 templates)
# ---------------------------------------------------------------------------

_F1 = ScreenerTemplate(
    name="F1",
    category="F",
    description="Low RSI Quality",
    conditions=[
        # RSI slightly oversold but not crashed (value stock range)
        {"feature": "rsi_14", "op": "lt", "value": 45},
        # Above SMA200 (fundamental quality filter — still in uptrend)
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
        # Hurst exponent > 0.5 (persistent/trending — quality proxy)
        {"feature": "hurst_exp_21d", "op": "gt", "value": 0.5},
    ],
    key_display_features=["rsi_14", "sma_200_ratio", "hurst_exp_21d", "adx_14"],
)

_F2 = ScreenerTemplate(
    name="F2",
    category="F",
    description="Momentum + Volume",
    conditions=[
        # Above SMA200
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
        # Volume expansion (High ROE attracts institutional buying)
        {"feature": "volume_ratio_21d", "op": "gt", "value": 1.5},
        # ADX confirms trending
        {"feature": "adx_14", "op": "gt", "value": 20},
    ],
    key_display_features=["sma_200_ratio", "volume_ratio_21d", "adx_14", "rsi_14"],
)

_F3 = ScreenerTemplate(
    name="F3",
    category="F",
    description="Dividend/Consistent Growth proxy",
    conditions=[
        # Flag pattern (consistent price trend proxy for dividend consistency)
        {"feature": "flag_pattern_score", "op": "gt", "value": 0.5},
        # Hurst > 0.5 (persistence in returns — consistent compounder proxy)
        {"feature": "hurst_exp_21d", "op": "gt", "value": 0.5},
    ],
    key_display_features=["flag_pattern_score", "hurst_exp_21d", "sma_200_ratio", "rsi_14"],
)

_F4 = ScreenerTemplate(
    name="F4",
    category="F",
    description="Compounder proxy",
    conditions=[
        # Above SMA200 (compounders stay above long-term MA)
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
        # High Hurst exponent (strong persistence/trend in returns)
        {"feature": "hurst_exp_21d", "op": "gt", "value": 0.55},
        # ADX > 15 (not just coasting — actively trending)
        {"feature": "adx_14", "op": "gt", "value": 15},
    ],
    key_display_features=["sma_200_ratio", "hurst_exp_21d", "adx_14", "rsi_14"],
)

_F5 = ScreenerTemplate(
    name="F5",
    category="F",
    description="Cash Flow King proxy",
    conditions=[
        # Flag pattern (strong underlying business proxy)
        {"feature": "flag_pattern_score", "op": "gt", "value": 0.6},
        # Low volume ratio (not in a speculative frenzy — steady accumulation)
        {"feature": "volume_ratio_21d", "op": "lt", "value": 0.8},
    ],
    key_display_features=["flag_pattern_score", "volume_ratio_21d", "sma_200_ratio", "hurst_exp_21d"],
)

_F6 = ScreenerTemplate(
    name="F6",
    category="F",
    description="Turnaround proxy",
    conditions=[
        # RSI recovering (30-50 = turnaround zone)
        {"feature": "rsi_14", "op": "between", "value": [30, 50]},
        # MACD histogram turning positive (momentum shift)
        {"feature": "macd_hist", "op": "gt", "value": 0},
    ],
    key_display_features=["rsi_14", "macd_hist", "sma_200_ratio", "volume_ratio_21d"],
)

_F7 = ScreenerTemplate(
    name="F7",
    category="F",
    description="Promoter Confidence proxy",
    conditions=[
        # Flag pattern (promoters buying = steady trend formation proxy)
        {"feature": "flag_pattern_score", "op": "gt", "value": 0.5},
        # Hurst > 0.5 (persistent trend = insider confidence proxy)
        {"feature": "hurst_exp_21d", "op": "gt", "value": 0.5},
    ],
    key_display_features=["flag_pattern_score", "hurst_exp_21d", "sma_200_ratio", "adx_14"],
)

_F8 = ScreenerTemplate(
    name="F8",
    category="F",
    description="PEG proxy",
    conditions=[
        # Positive 10-day momentum (growth proxy)
        {"feature": "roc_10", "op": "gt", "value": 0.02},
        # RSI not overbought (value element of PEG — not fully priced in)
        {"feature": "rsi_14", "op": "lt", "value": 60},
        # ADX > 15 (growth is real — not just noise)
        {"feature": "adx_14", "op": "gt", "value": 15},
    ],
    key_display_features=["roc_10", "rsi_14", "adx_14", "sma_200_ratio"],
)

# ---------------------------------------------------------------------------
# Category S — Core Technical Library (7 templates; S007/S009/S010/S011/S012 excluded)
# S007 = E1 (Turtle), S009 = A1 (BB Squeeze), S012 = E4 (Sector Rotation) are
# excluded as duplicates. S010 and S011 are excluded to reach exactly 42 total.
# ---------------------------------------------------------------------------

_S001 = ScreenerTemplate(
    name="S001",
    category="S",
    description="EMA Crossover",
    conditions=[
        # EMA8 above EMA21 (golden cross on fast EMAs)
        {"feature": "ema_ribbon_alignment", "op": "gt", "value": 0},
        # Close above SMA200 (trend filter)
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
    ],
    key_display_features=["ema_ribbon_alignment", "sma_200_ratio", "rsi_14", "volume_ratio_21d"],
)

_S002 = ScreenerTemplate(
    name="S002",
    category="S",
    description="Supertrend Breakout",
    conditions=[
        # Supertrend direction = bullish (dir = +1 means in uptrend)
        {"feature": "supertrend_dir", "op": "gt", "value": 0},
        # Volume confirms supertrend
        {"feature": "volume_ratio_21d", "op": "gt", "value": 1.5},
    ],
    key_display_features=["supertrend_dir", "volume_ratio_21d", "adx_14", "rsi_14"],
)

_S003 = ScreenerTemplate(
    name="S003",
    category="S",
    description="RSI Mean Reversion",
    conditions=[
        # RSI oversold in uptrend (mean reversion setup)
        {"feature": "rsi_14", "op": "lt", "value": 35},
        # Above SMA200 (structural uptrend maintained)
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
    ],
    key_display_features=["rsi_14", "sma_200_ratio", "macd_hist", "volume_ratio_21d"],
)

_S004 = ScreenerTemplate(
    name="S004",
    category="S",
    description="52-Week High Breakout",
    conditions=[
        # At or just above recent 21-day high (proxy for 52w high breakout)
        {"feature": "base_breakout_ratio", "op": "gt", "value": 0.995},
        # High volume confirming the breakout
        {"feature": "volume_ratio_21d", "op": "gt", "value": 2.0},
    ],
    key_display_features=["base_breakout_ratio", "volume_ratio_21d", "adx_14", "rsi_14"],
)

_S005 = ScreenerTemplate(
    name="S005",
    category="S",
    description="VWAP Reversal",
    conditions=[
        # Close above SMA20 (VWAP proxy — close above 20-day avg = above intraday VWAP)
        {"feature": "sma_20_ratio", "op": "gt", "value": 1.0},
        # Volume confirms reversal
        {"feature": "volume_ratio_21d", "op": "gt", "value": 1.3},
    ],
    key_display_features=["sma_20_ratio", "volume_ratio_21d", "rsi_14", "macd_hist"],
)

_S006 = ScreenerTemplate(
    name="S006",
    category="S",
    description="Ichimoku Cloud Breakout",
    conditions=[
        # Price above cloud (ichimoku_cloud_position > 0 = above cloud midpoint)
        {"feature": "ichimoku_cloud_position", "op": "gt", "value": 0},
        # ADX confirms trend
        {"feature": "adx_14", "op": "gt", "value": 20},
        # Hurst > 0.5 (persistent trend — Ichimoku works best in trending markets)
        {"feature": "hurst_exp_21d", "op": "gt", "value": 0.5},
    ],
    key_display_features=["ichimoku_cloud_position", "adx_14", "hurst_exp_21d", "rsi_14"],
)

_S008 = ScreenerTemplate(
    name="S008",
    category="S",
    description="MACD Histogram",
    conditions=[
        # MACD histogram positive
        {"feature": "macd_hist", "op": "gt", "value": 0},
        # MACD histogram in top half of universe (recently turned positive)
        {"feature": "macd_hist", "op": "top_pct", "value": 0.4},
    ],
    key_display_features=["macd_hist", "rsi_14", "sma_200_ratio", "volume_ratio_21d"],
)

# ---------------------------------------------------------------------------
# Master registry
# ---------------------------------------------------------------------------

TEMPLATES: List[ScreenerTemplate] = [
    # Category A (4)
    _A1, _A2, _A3, _A4,
    # Category B (5)
    _B1, _B2, _B3, _B4, _B5,
    # Category C (7)
    _C1, _C2, _C3, _C4, _C5, _C6, _C7,
    # Category D (4)
    _D1, _D2, _D3, _D4,
    # Category E (7 — E8 excluded as duplicate of C6)
    _E1, _E2, _E3, _E4, _E5, _E6, _E7,
    # Category F (8)
    _F1, _F2, _F3, _F4, _F5, _F6, _F7, _F8,
    # Category S (7 — S007/S009/S010/S011/S012 excluded; see module docstring)
    _S001, _S002, _S003, _S004, _S005, _S006, _S008,
]

# Fast name → template lookup used by ScreenerEngine
TEMPLATE_MAP: Dict[str, ScreenerTemplate] = {t.name: t for t in TEMPLATES}

assert len(TEMPLATES) == 42, (
    f"Expected 42 templates, got {len(TEMPLATES)}. "
    "If you added or removed templates, update this assertion."
)

# ---------------------------------------------------------------------------
# Strategy-style classification (Momentum / Trend Following / Mean Reversion /
# Volatility). No such bucketing existed before this — it's a first-pass
# manual classification by each template's actual condition logic (e.g. an
# oversold-RSI-bounce condition -> Mean Reversion, a breakout-with-ADX-trend
# condition -> Trend Following), not derived from the pre-existing A-F/S
# letter categories, which are purely descriptive groupings and don't map
# cleanly to these 4 styles. Intended as a reasonable starting point for
# win-rate-by-style reporting; refine here if a template is judged
# misclassified.
# ---------------------------------------------------------------------------
TEMPLATE_STYLE: Dict[str, str] = {
    "A1": "Trend Following",   # BB squeeze + hard sma_200_ratio>1.0 trend gate, not pure volatility
    "A2": "Momentum",          # MACD histogram turning positive
    "A3": "Mean Reversion",    # Williams %R oversold bounce
    "A4": "Mean Reversion",    # RSI oversold in uptrend
    "B1": "Trend Following",   # Weinstein Stage 2 (trend + volume + ADX)
    "B2": "Trend Following",   # IBD base breakout
    "B3": "Trend Following",   # Darvas Box breakout
    "B4": "Mean Reversion",    # AVWAP support bounce / double-bottom
    "B5": "Momentum",          # Livermore pivot breakout with volume surge
    "C1": "Momentum",          # Time series momentum
    "C2": "Momentum",          # Cross-sectional momentum
    "C3": "Momentum",          # Dual momentum
    "C4": "Momentum",          # CAN SLIM proxy (institutional momentum)
    "C5": "Trend Following",   # 52-week high proximity
    "C6": "Trend Following",   # EMA ribbon alignment
    "C7": "Momentum",          # Post-earnings drift
    "D1": "Mean Reversion",    # RSI-2 mean reversion
    "D2": "Mean Reversion",    # Long-horizon contrarian
    "D3": "Mean Reversion",    # MACD + RSI divergence reversal
    "D4": "Momentum",          # IBD follow-through day
    "E1": "Trend Following",   # Turtle Donchian breakout
    "E2": "Trend Following",   # Minervini SEPA
    "E3": "Trend Following",   # Piotroski F proxy (trend persistence quality)
    "E4": "Trend Following",   # Sector rotation
    "E5": "Momentum",          # Earnings acceleration
    "E6": "Momentum",          # GARP momentum
    "E7": "Trend Following",   # Greenblatt Magic Formula proxy
    "F1": "Mean Reversion",    # Low RSI quality
    "F2": "Momentum",          # Momentum + volume
    "F3": "Trend Following",   # Dividend/consistent growth proxy
    "F4": "Trend Following",   # Compounder proxy
    "F5": "Volatility",        # Cash flow king proxy (low-volume-ratio accumulation)
    "F6": "Mean Reversion",    # Turnaround proxy
    "F7": "Trend Following",   # Promoter confidence proxy
    "F8": "Momentum",          # PEG proxy (growth momentum)
    "S001": "Trend Following",  # EMA crossover
    "S002": "Trend Following",  # Supertrend breakout
    "S003": "Mean Reversion",   # RSI mean reversion
    "S004": "Trend Following",  # 52-week high breakout
    "S005": "Mean Reversion",   # VWAP reversal
    "S006": "Trend Following",  # Ichimoku cloud breakout
    "S008": "Momentum",        # MACD histogram
}
assert set(TEMPLATE_STYLE) == set(TEMPLATE_MAP), "TEMPLATE_STYLE must classify every template, no more, no less."
STRATEGY_STYLES: List[str] = ["Momentum", "Trend Following", "Mean Reversion", "Volatility"]

# ---------------------------------------------------------------------------
# Per-template exit params (PerTemplateExitPolicy), one set of stop/target/
# max_hold per TEMPLATE_STYLE group rather than 42 individually-reasoned
# figures — the style groupings above already say what kind of trade each
# template is, and exit discipline should follow the trade's *style*, not
# its specific condition logic:
#   - Momentum: signal decays fast once the move is already underway, so
#     exits are tight and short-horizon (4% stop / 10% target / 15d).
#   - Trend Following: designed to ride a real trend once confirmed, so
#     gets the most room to run (5% stop / 12% target / 25d).
#   - Mean Reversion: the whole thesis is a fast snap-back; if it hasn't
#     reverted quickly the thesis is wrong, so exits are the tightest and
#     shortest of all four (3% stop / 6% target / 10d).
#   - Volatility (breakout/squeeze): a confirmed breakout needs slightly
#     more room than a pure reversion trade but should still resolve
#     faster than a trend-following thesis (4.5% stop / 9% target / 20d).
# ---------------------------------------------------------------------------
STYLE_EXIT_PARAMS: Dict[str, Dict[str, float]] = {
    "Momentum": {"stop_pct": 0.04, "target_pct": 0.10, "max_hold_days": 15},
    "Trend Following": {"stop_pct": 0.05, "target_pct": 0.12, "max_hold_days": 25},
    "Mean Reversion": {"stop_pct": 0.03, "target_pct": 0.06, "max_hold_days": 10},
    "Volatility": {"stop_pct": 0.045, "target_pct": 0.09, "max_hold_days": 20},
}

for _tname, _style in TEMPLATE_STYLE.items():
    _params = STYLE_EXIT_PARAMS[_style]
    _t = TEMPLATE_MAP[_tname]
    _t.exit_stop_pct = _params["stop_pct"]
    _t.exit_target_pct = _params["target_pct"]
    _t.exit_max_hold_days = int(_params["max_hold_days"])
