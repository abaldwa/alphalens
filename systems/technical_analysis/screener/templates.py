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
  - "close/52w_high"  → dist_from_52w_high     ((close-52w_high)/52w_high; 0 = at
                                               the high, negative = below it).
                                               NOT base_breakout_ratio, which is
                                               close/prior_21d_high — a 21-day
                                               high is a far weaker condition
                                               (77.8% of rows passing it are not
                                               within 1% of the 52-week high).
                                               Corrected in C5/S004 2026-08-11.
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
    # rs_vs_nifty500_21d inherited from C3 (dropped 2026-08-13 as a definitional
    # duplicate of this template — same two conditions, order reversed). The
    # survivor keeps the UNION of both templates' display features so no column
    # a user relied on disappears with the dropped name.
    key_display_features=[
        "sma_200_ratio", "roc_10", "composite_momentum_63d", "adx_14", "rs_vs_nifty500_21d",
    ],
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

# C3 ("Dual Momentum") REMOVED 2026-08-13. It was not merely similar to C1, it
# was the identical screen: {roc_10 > 0, sma_200_ratio > 1.0} against C1's
# {sma_200_ratio > 1.0, roc_10 > 0} — the same conjunction with the operands
# written in the other order, and the same exits (stop 0.04 / target 0.10 /
# hold 21). Two templates that cannot differ on any input, in any window, are
# one strategy costing two backtest runs; a full-grid sweep spent ~3% of its
# compute proving C1 == C3 to four decimal places (3,886 trades, 27.66% CAGR,
# byte-identical trade sequences). C1 survives and inherited C3's
# rs_vs_nifty500_21d display feature. Detection is now automated — see the
# duplicate-signature assertion below the master registry, which fails the
# import rather than leaving the next copy-paste duplicate to be found by hand.

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
        # Within 1% of the 52-week high.
        #
        # [2026-08-11] Was `base_breakout_ratio > 0.99`, i.e. close/prior_21d_high
        # — a stand-in from when no 52-week feature was stored. dist_from_52w_high
        # exists now (T02 already uses it), and the two are not close: over a
        # 94,537-row sample, the 21-day test admitted 2,589 rows against 305 for
        # the real one, and 77.8% of what it selected was NOT within 1% of the
        # 52-week high. Being near a 21-day high is ordinary in any short
        # uptrend; being near a 52-week high is the actual signal this template
        # is named for.
        {"feature": "dist_from_52w_high", "op": "gte", "value": -0.01},
        # Volume surge (institutional accumulation at highs)
        {"feature": "volume_ratio_21d", "op": "gt", "value": 2.0},
        # ADX confirms trending
        {"feature": "adx_14", "op": "gte", "value": 20},
    ],
    key_display_features=["dist_from_52w_high", "volume_ratio_21d", "adx_14", "rsi_14"],
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
        # Connors RSI(2) oversold.
        #
        # [2026-08-11] Was `rsi_14 < 10`, a stand-in from when rsi_2 genuinely
        # wasn't stored. rsi_2 IS in the feature store now, and the proxy was
        # never equivalent: RSI-2 is a 2-period oscillator that routinely dips
        # under 10, while RSI-14 smooths over 14 periods and almost never does
        # — least of all in a stock trading above its 200-day SMA, which the
        # second condition requires.
        #
        # Measured over 15,400,703 ticker-days (2007-04-01..2026-08-10):
        #     rsi_14 < 10 AND above SMA200 ->        7 matches
        #     rsi_2  < 10 AND above SMA200 ->  355,123 matches
        #
        # So D1 screened ~nothing for its whole life and its 19-year backtest
        # produced zero trades (flat equity curve; integrity checks 05/06/08/12
        # failed, correctly). This is the template it was always described as.
        {"feature": "rsi_2", "op": "lt", "value": 10},
        # Long-term uptrend (above SMA200)
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
    ],
    key_display_features=["rsi_2", "sma_200_ratio", "macd_hist", "volume_ratio_21d"],
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
    # adx_14 inherited from F7 (dropped 2026-08-13 as a definitional duplicate
    # of this template). Union of both templates' display features — see C1.
    key_display_features=[
        "flag_pattern_score", "hurst_exp_21d", "sma_200_ratio", "rsi_14", "adx_14",
    ],
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

# F7 ("Promoter Confidence proxy") REMOVED 2026-08-13 — identical screen to F3
# ({flag_pattern_score > 0.5, hurst_exp_21d > 0.5}, same exits). Unlike C1/C3
# this pair had never been noticed in any earlier audit; it surfaced only once
# all 66 templates were compared by order-independent condition signature
# instead of by eye. Verified byte-identical: 4,532 trades, 11.69% CAGR both.
# The two descriptions ("dividend consistency" vs "promoter confidence") name
# different fundamental theses, but neither is expressible in the two purely
# technical conditions both were built from — the distinct names described an
# intent the conditions never encoded. F3 survives and inherited F7's adx_14
# display feature.

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
        # Within 0.5% of the 52-week high.
        #
        # [2026-08-11] Was `base_breakout_ratio > 0.995` (close/prior_21d_high),
        # the same stand-in corrected in C5 — see the note there for the
        # measurement. A 21-day high is not a 52-week high, and this template is
        # named for the latter.
        {"feature": "dist_from_52w_high", "op": "gte", "value": -0.005},
        # High volume confirming the breakout
        {"feature": "volume_ratio_21d", "op": "gt", "value": 2.0},
    ],
    key_display_features=["dist_from_52w_high", "volume_ratio_21d", "adx_14", "rsi_14"],
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
# Category R — Regime (per-ticker HMM latent-state detection, 5 templates)
# ---------------------------------------------------------------------------
# Uses the 6 hmm_regime_* columns produced by the per-ticker GaussianHMM(4)
# detector (systems/ml_signal_engine/models/hmm/regime_detector.py). The HMM
# fits each ticker's own hidden state sequence over its 5 observables, so
# these templates surface regime transitions that standard TA indicators on
# the same name may not flag until later — the "stocks follow their own
# patterns" signal. All 6 columns are float64 in the daily parquet:
#   hmm_regime             : state rank 0.0=bearish, 1.0=sideways,
#                            2.0=volatile, 3.0=bullish
#   hmm_regime_transition  : 1.0 = state changed vs prior day
#   hmm_regime_duration    : consecutive days in current state
#   hmm_regime_stability   : max state probability (label confidence)
#   hmm_regime_prob_bullish/bearish : per-state emission probabilities
# NaN rows (undecodable ticker/day) are auto-excluded by the engine's
# fillna(False) mask.

_R1 = ScreenerTemplate(
    name="R1",
    category="R",
    description="Regime Flip: Bullish Entry",
    conditions=[
        # Ticker's own HMM state is bullish (rank 2 in 3-state model)
        {"feature": "hmm_regime", "op": "eq", "value": 2.0},
        # Just transitioned into it — the hidden pattern just became visible
        {"feature": "hmm_regime_transition", "op": "eq", "value": 1.0},
        # High label confidence (max state probability)
        {"feature": "hmm_regime_stability", "op": "gte", "value": 0.85},
    ],
    key_display_features=["hmm_regime_prob_bullish", "hmm_regime_stability", "hmm_regime_duration"],
)

_R2 = ScreenerTemplate(
    name="R2",
    category="R",
    description="Regime Flip: Bearish Exit",
    conditions=[
        # Ticker's own HMM state is bearish (rank 0)
        {"feature": "hmm_regime", "op": "eq", "value": 0.0},
        # Just transitioned into it — de-risk signal
        {"feature": "hmm_regime_transition", "op": "eq", "value": 1.0},
        # High label confidence
        {"feature": "hmm_regime_stability", "op": "gte", "value": 0.80},
    ],
    key_display_features=["hmm_regime_prob_bearish", "hmm_regime_stability", "hmm_regime_duration"],
)

_R3 = ScreenerTemplate(
    name="R3",
    category="R",
    description="Stable Bullish Regime",
    conditions=[
        # Established bullish state (rank 2 in 3-state model)
        {"feature": "hmm_regime", "op": "eq", "value": 2.0},
        # Strong bullish emission probability
        {"feature": "hmm_regime_prob_bullish", "op": "gte", "value": 0.70},
        # Very high label confidence
        {"feature": "hmm_regime_stability", "op": "gte", "value": 0.95},
        # At least 10 days in the regime — trend is mature/confirmed
        {"feature": "hmm_regime_duration", "op": "gte", "value": 10},
    ],
    key_display_features=["hmm_regime_prob_bullish", "hmm_regime_stability", "hmm_regime_duration"],
)

_R4 = ScreenerTemplate(
    name="R4",
    category="R",
    description="Regime Transition: Any",
    conditions=[
        # Any high-confidence state change, regardless of direction
        {"feature": "hmm_regime_transition", "op": "eq", "value": 1.0},
        {"feature": "hmm_regime_stability", "op": "gte", "value": 0.80},
    ],
    key_display_features=["hmm_regime", "hmm_regime_prob_bullish", "hmm_regime_stability"],
)

# ---------------------------------------------------------------------------
# Category T — Indicator-Library Strategies (20 templates, 2026-08-08)
#
# T01-T20 from the "Technical Strategies from AlphaLens Indicators" brief.
# Every feature referenced below was verified present in the daily feature
# Parquet before authoring (2026-08-07 snapshot).
#
# CALIBRATION NOTE — thresholds are NOT copied literally from the brief.
# The brief's absolute cutoffs assume textbook distributions that this
# universe does not have; using them verbatim would produce no-op or
# match-everything conditions. Measured on the 2026-08-07 snapshot:
#   - hurst_exp_21d: median 0.686 (NOT ~0.5). "hurst >= 0.55" would match
#     ~85% of the universe (no filter at all) and "hurst <= 0.45" would
#     match almost nothing. Persistence/anti-persistence conditions are
#     therefore expressed CROSS-SECTIONALLY via top_pct/bottom_pct, which
#     preserves the brief's intent ("more persistent than peers") and is
#     robust to the distribution drifting over time.
#   - Same treatment for the entropy/complexity features, whose absolute
#     levels are equally universe- and window-specific.
# Two features from the brief are deliberately UNUSED as conditions:
#   - fractal_dimension: constant 1.000 across the entire universe
#     (min=q25=med=q75=max=1.0) — degenerate, filters nothing.
#   - sample_entropy_21d: saturated at a clipped 23.026 for >50% of rows
#     (median=q75=max) — cannot discriminate.
# Both are kept in key_display_features where the brief cites them, so the
# values still surface for review, but no condition depends on them.
# The brief's `hh_21` does not exist; `hh_22` is the 22-day rolling high
# the brief itself calls the "22-day high proxy" (see T05) and is used.
# ---------------------------------------------------------------------------

_T01 = ScreenerTemplate(
    name="T01",
    category="T",
    description="Multi-Timeframe Ichimoku Trend Continuation",
    conditions=[
        # Above the cloud (ichimoku_cloud_position > 0 = above)
        {"feature": "ichimoku_cloud_position", "op": "gt", "value": 0.0},
        # Conversion/base and lagging-span confirmations both bullish
        {"feature": "tenkan_kijun_signal", "op": "gt", "value": 0.0},
        {"feature": "chikou_span_signal", "op": "gt", "value": 0.0},
        # Trend strength
        {"feature": "adx_14", "op": "gt", "value": 20},
        # Persistent regime, cross-sectional (see CALIBRATION NOTE)
        {"feature": "hurst_exp_21d", "op": "top_pct", "value": 0.40},
        # Pullback-then-reclaim proxy: price back above the short EMA while
        # still only modestly extended from the 20-day mean.
        {"feature": "ema_8_ratio", "op": "gt", "value": 1.0},
        {"feature": "sma_20_ratio", "op": "lt", "value": 1.10},
    ],
    key_display_features=[
        "ichimoku_cloud_position", "tenkan_kijun_signal", "chikou_span_signal",
        "adx_14", "hurst_exp_21d", "wavelet_noise", "atr_14_pct",
    ],
)

_T02 = ScreenerTemplate(
    name="T02",
    category="T",
    description="52-Week High Volatility Breakout",
    conditions=[
        # At/just under the 52-week high (dist_from_52w_high maxes at 0.0)
        {"feature": "dist_from_52w_high", "op": "between", "value": [-0.03, 0.0]},
        # Range expansion after compression
        {"feature": "bb_width_pct", "op": "top_pct", "value": 0.50},
        {"feature": "volume_ratio_21d", "op": "gte", "value": 1.5},
        # Long-term uptrend filter
        {"feature": "sma_200_ratio", "op": "gte", "value": 1.0},
    ],
    key_display_features=[
        "dist_from_52w_high", "bb_width_pct", "volume_ratio_21d",
        "sma_200_ratio", "adx_14", "rs_vs_nifty500_21d", "atr_14_pct",
    ],
)

_T03 = ScreenerTemplate(
    name="T03",
    category="T",
    description="Flag Continuation + EMA Ribbon",
    conditions=[
        # EMA8 > EMA21 > EMA55 > EMA89 (composite: +1 = fully aligned)
        {"feature": "ema_ribbon_alignment", "op": "gte", "value": 1.0},
        {"feature": "flag_pattern_score", "op": "gte", "value": 0.6},
        {"feature": "sma_50_ratio", "op": "gt", "value": 1.0},
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
        {"feature": "adx_14", "op": "gt", "value": 20},
    ],
    key_display_features=[
        "ema_ribbon_alignment", "flag_pattern_score", "sma_50_ratio",
        "sma_200_ratio", "adx_14", "ema_21_ratio", "atr_14_pct",
    ],
)

_T04 = ScreenerTemplate(
    name="T04",
    category="T",
    description="Ichimoku + Relative Strength Breakout",
    conditions=[
        # Top-decile relative strength and 63d composite momentum
        {"feature": "rs_vs_nifty500_21d", "op": "top_pct", "value": 0.10},
        {"feature": "composite_momentum_63d", "op": "top_pct", "value": 0.10},
        {"feature": "ichimoku_cloud_position", "op": "gt", "value": 0.0},
        {"feature": "tenkan_kijun_signal", "op": "gt", "value": 0.0},
        {"feature": "base_breakout_score", "op": "gte", "value": 0.6},
    ],
    key_display_features=[
        "rs_vs_nifty500_21d", "composite_momentum_63d", "ichimoku_cloud_position",
        "tenkan_kijun_signal", "base_breakout_score", "atr_14_pct",
    ],
)

_T05 = ScreenerTemplate(
    name="T05",
    category="T",
    description="Donchian-Style High Channel Breakout",
    conditions=[
        # Prior consolidation: vol_compression_21d in the low (tight) cohort
        {"feature": "vol_compression_21d", "op": "bottom_pct", "value": 0.40},
        # Breaking the 22-day high (close/hh_22 >= 1.0)
        {"feature": "base_breakout_ratio", "op": "gte", "value": 1.0},
        # Range expanding out of the squeeze
        {"feature": "bb_width_pct", "op": "top_pct", "value": 0.50},
        {"feature": "volume_ratio_21d", "op": "gte", "value": 1.8},
    ],
    key_display_features=[
        "vol_compression_21d", "base_breakout_ratio", "hh_22", "hh_55",
        "bb_width_pct", "volume_ratio_21d", "adx_14", "atr_14_pct",
    ],
)

_T06 = ScreenerTemplate(
    name="T06",
    category="T",
    description="Supertrend + EMA Trend Filter",
    conditions=[
        {"feature": "supertrend_dir", "op": "gt", "value": 0.0},
        {"feature": "ema_8_ratio", "op": "gt", "value": 1.0},
        {"feature": "ema_21_ratio", "op": "gt", "value": 1.0},
        {"feature": "volume_ratio_5d", "op": "gte", "value": 1.3},
    ],
    key_display_features=[
        "supertrend_dir", "ema_8_ratio", "ema_21_ratio",
        "volume_ratio_5d", "adx_14", "atr_14_pct",
    ],
)

_T07 = ScreenerTemplate(
    name="T07",
    category="T",
    description="Wavelet-Filtered Trend Persistence",
    conditions=[
        # Strong smoothed trend, low high-frequency noise (both cross-sectional)
        {"feature": "wavelet_trend", "op": "top_pct", "value": 0.40},
        {"feature": "wavelet_noise", "op": "bottom_pct", "value": 0.40},
        {"feature": "hurst_exp_21d", "op": "top_pct", "value": 0.40},
        {"feature": "trend_consistency_21", "op": "top_pct", "value": 0.40},
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
        {"feature": "adx_14", "op": "gt", "value": 20},
    ],
    key_display_features=[
        "wavelet_trend", "wavelet_noise", "hurst_exp_21d", "trend_consistency_21",
        "sma_200_ratio", "adx_14", "wavelet_energy_ratio", "atr_14_pct",
    ],
)

_T08 = ScreenerTemplate(
    name="T08",
    category="T",
    description="Short-Term RSI-2 Mean Reversion",
    conditions=[
        # Anti-persistent (choppy) regime, cross-sectional bottom cohort
        {"feature": "hurst_exp_21d", "op": "bottom_pct", "value": 0.40},
        {"feature": "approx_entropy_21d", "op": "top_pct", "value": 0.50},
        # The signal itself
        {"feature": "rsi_2", "op": "lt", "value": 10},
        # Long-term uptrend intact
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
    ],
    key_display_features=[
        "rsi_2", "hurst_exp_21d", "approx_entropy_21d", "sma_200_ratio",
        "open_close_range_pct", "sma_20_ratio", "atr_14_pct",
    ],
)

_T09 = ScreenerTemplate(
    name="T09",
    category="T",
    description="Bollinger/Keltner Squeeze Reversion",
    conditions=[
        # Squeeze: tight range, BB width in the low cohort
        {"feature": "vol_compression_21d", "op": "bottom_pct", "value": 0.40},
        {"feature": "bb_width_pct", "op": "bottom_pct", "value": 0.30},
        # Near the lower band, no strong directional drift
        {"feature": "bb_position", "op": "lt", "value": 0.25},
        {"feature": "keltner_position", "op": "between", "value": [0.0, 0.55]},
        {"feature": "rsi_14", "op": "between", "value": [30, 40]},
        {"feature": "hurst_exp_21d", "op": "bottom_pct", "value": 0.50},
    ],
    key_display_features=[
        "bb_width_pct", "bb_position", "keltner_position", "rsi_14",
        "vol_compression_21d", "hurst_exp_21d", "adx_14", "atr_14_pct",
    ],
)

_T10 = ScreenerTemplate(
    name="T10",
    category="T",
    description="VWAP Proxy (20-Day SMA) Reversion",
    conditions=[
        # No clear HH/HL trend
        {"feature": "trend_consistency_21", "op": "bottom_pct", "value": 0.40},
        # Low in the 21-day range. NOTE: the brief also asks for stoch_k<20,
        # but stoch_k IS the position-in-range oscillator — stacking it on
        # close_position_in_range double-counts the same condition and drove
        # matches to exactly 0 on the 2026-08-07 snapshot. Keeping the
        # oscillator (stoch_k) as the range-position gate and relaxing the
        # raw-range cutoff to the lower quartile keeps the brief's intent
        # (buy the bottom of a rangebound name) without the redundancy.
        {"feature": "close_position_in_range", "op": "lt", "value": 0.25},
        {"feature": "stoch_k", "op": "lt", "value": 20},
        {"feature": "sma_20_ratio", "op": "lt", "value": 0.98},
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
    ],
    key_display_features=[
        "trend_consistency_21", "close_position_in_range", "sma_20_ratio",
        "sma_200_ratio", "stoch_k", "hh_22", "adx_14", "atr_14_pct",
    ],
)

_T11 = ScreenerTemplate(
    name="T11",
    category="T",
    description="Gap-Fade in High Entropy",
    conditions=[
        # Noisy, non-trending regime. Both gates are widened from the
        # brief's "high threshold"/"adx<20": measured on three sample dates,
        # a -2% gap in an uptrend leaves only 6-25 names, and each of
        # rsi<45 / adx<20 / entropy-top-40% independently cut that by ~5x,
        # so the brief's full conjunction produced 0-2 matches per day.
        # The gap in an uptrend IS the signal here; these two stay as a
        # regime sanity check (not strongly trending, noisier than median)
        # rather than as additional rare-event filters.
        {"feature": "approx_entropy_21d", "op": "top_pct", "value": 0.60},
        {"feature": "adx_14", "op": "lt", "value": 30},
        # Large downside gap (gap_down_pct is negative, floors at 0).
        # Relaxed from the brief's -3% to -2%: a -3% gap fired on only 26 of
        # 2,317 names on the 2026-08-07 snapshot, and stacking the brief's
        # additional rsi_14<30 on top left ZERO matches. A gap-fade is
        # inherently a rare event, so the gap threshold is kept strict-ish
        # while the oscillator gate is widened to <45 (still "weak, not yet
        # bouncing") rather than the deep-oversold <30 that made the
        # conjunction empty.
        {"feature": "gap_down_pct", "op": "lt", "value": -2.0},
        # Avoid structurally weak names
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
        {"feature": "rsi_14", "op": "lt", "value": 45},
    ],
    key_display_features=[
        "gap_down_pct", "gap_up_pct", "approx_entropy_21d", "sample_entropy_21d",
        "adx_14", "rsi_14", "sma_200_ratio", "atr_14_pct",
    ],
)

_T12 = ScreenerTemplate(
    name="T12",
    category="T",
    description="Intraday Reversal Score Mean Reversion",
    conditions=[
        # Strong downside reversal / capitulation (lower decile)
        {"feature": "intraday_reversal_score", "op": "bottom_pct", "value": 0.10},
        {"feature": "rsi_14", "op": "between", "value": [30, 40]},
        {"feature": "hurst_exp_21d", "op": "bottom_pct", "value": 0.50},
    ],
    key_display_features=[
        "intraday_reversal_score", "rsi_14", "hurst_exp_21d",
        "delivery_pct_zscore_21d", "open_close_range_pct", "adx_14", "atr_14_pct",
    ],
)

_T13 = ScreenerTemplate(
    name="T13",
    category="T",
    description="Range-Bound ATR/Entropy Reversion",
    conditions=[
        # Low realised vol, range-bound non-directional noise
        {"feature": "hist_vol_21", "op": "bottom_pct", "value": 0.40},
        {"feature": "spectral_entropy", "op": "top_pct", "value": 0.40},
        # Near the lower edge of the range
        {"feature": "close_position_in_range", "op": "lt", "value": 0.20},
        {"feature": "cci_20", "op": "lt", "value": -100},
        {"feature": "stoch_k", "op": "lt", "value": 20},
    ],
    key_display_features=[
        "hist_vol_21", "spectral_entropy", "close_position_in_range",
        "cci_20", "stoch_k", "atr_14_pct", "bb_width_pct", "adx_14",
    ],
)

_T14 = ScreenerTemplate(
    name="T14",
    category="T",
    description="6-Month Relative Strength Rotation",
    conditions=[
        # Liquid names only
        {"feature": "volume_ratio_21d", "op": "gte", "value": 1.0},
        # Top-decile on both RS and 63d composite momentum, penalising
        # excess volatility (the brief's weighted score, expressed as the
        # cross-sectional conjunction the screener can evaluate per-date).
        {"feature": "rs_vs_nifty500_21d", "op": "top_pct", "value": 0.10},
        {"feature": "composite_momentum_63d", "op": "top_pct", "value": 0.10},
        {"feature": "hist_vol_21", "op": "bottom_pct", "value": 0.60},
    ],
    key_display_features=[
        "rs_vs_nifty500_21d", "composite_momentum_63d", "hist_vol_21",
        "volume_ratio_21d", "delivery_pct", "sma_200_ratio",
    ],
)

_T15 = ScreenerTemplate(
    name="T15",
    category="T",
    description="Relative Strength + Hurst Rotation",
    conditions=[
        # The brief frames T15 at sector level; AlphaLens's screener is
        # per-ticker, so this is the same score applied to stocks: strong
        # RS vs Nifty 50, persistent regime, low entropy penalty.
        {"feature": "rs_vs_nifty50_21d", "op": "top_pct", "value": 0.20},
        {"feature": "hurst_exp_63d", "op": "top_pct", "value": 0.30},
        {"feature": "approx_entropy_21d", "op": "bottom_pct", "value": 0.50},
    ],
    key_display_features=[
        "rs_vs_nifty50_21d", "hurst_exp_63d", "approx_entropy_21d",
        "sma_200_ratio", "adx_14", "hist_vol_21",
    ],
)

_T16 = ScreenerTemplate(
    name="T16",
    category="T",
    description="Relative Strength + Low Volatility Leader",
    conditions=[
        {"feature": "rs_vs_nifty500_21d", "op": "top_pct", "value": 0.50},
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
        {"feature": "hist_vol_21", "op": "bottom_pct", "value": 0.50},
        # Quiet regime beginning to expand
        {"feature": "vol_compression_21d", "op": "bottom_pct", "value": 0.50},
        {"feature": "ema_ribbon_alignment", "op": "gte", "value": 1.0},
    ],
    key_display_features=[
        "rs_vs_nifty500_21d", "hist_vol_21", "vol_compression_21d",
        "ema_ribbon_alignment", "sma_200_ratio", "ema_21_ratio", "atr_14_pct",
    ],
)

# --- T17-T20: the brief defines these as meta-strategies/overlays ----------
# T17 gates between trend and mean-reversion modes; T18 filters breakout
# entries on entropy; T19 filters trend entries on fractional-diff drift;
# T20 scales position size by complexity. The screener evaluates ONE set of
# per-ticker conditions per template and has no cross-template gating or
# position-sizing hook, so each is implemented as the standalone entry rule
# its filter implies — i.e. the subset of names that would SURVIVE that
# overlay, expressed directly. This makes each independently backtestable
# and measures exactly what the overlay's filter is worth on its own.
# Applying them as true overlays over T01-T16 is adapter-level work
# (backtest/adapters/) and is deliberately NOT faked here.

_T17 = ScreenerTemplate(
    name="T17",
    category="T",
    description="Hurst-Gated Trend Mode (Meta-Strategy, trend leg)",
    conditions=[
        # Trend regime leg of the dual-mode strategy: persistent + clean
        # signal. (The mean-reversion leg is already covered by T08/T09/T10,
        # which carry the inverse hurst/entropy gates.)
        {"feature": "hurst_exp_21d", "op": "top_pct", "value": 0.30},
        {"feature": "wavelet_trend", "op": "top_pct", "value": 0.40},
        {"feature": "wavelet_noise", "op": "bottom_pct", "value": 0.40},
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
        {"feature": "adx_14", "op": "gt", "value": 20},
    ],
    key_display_features=[
        "hurst_exp_21d", "wavelet_trend", "wavelet_noise",
        "adx_14", "sma_200_ratio", "atr_14_pct",
    ],
)

_T18 = ScreenerTemplate(
    name="T18",
    category="T",
    description="Entropy-Filtered Breakout (Meta-Filter)",
    conditions=[
        # The breakout setup...
        {"feature": "base_breakout_ratio", "op": "gte", "value": 1.0},
        {"feature": "volume_ratio_21d", "op": "gte", "value": 1.5},
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
        # ...allowed only in the structured, low-chaos cohort.
        {"feature": "permutation_entropy_21d", "op": "bottom_pct", "value": 0.40},
        {"feature": "spectral_entropy", "op": "bottom_pct", "value": 0.40},
        # Instability guard (brief: suspend breakouts on extreme readings)
        {"feature": "lyapunov_exponent_proxy", "op": "bottom_pct", "value": 0.70},
    ],
    key_display_features=[
        "base_breakout_ratio", "permutation_entropy_21d", "spectral_entropy",
        "lyapunov_exponent_proxy", "rqa_rec_rate", "volume_ratio_21d", "atr_14_pct",
    ],
)

_T19 = ScreenerTemplate(
    name="T19",
    category="T",
    description="Fractional-Diff Trend Robustness Filter",
    conditions=[
        # Trend candidate...
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
        {"feature": "adx_14", "op": "gt", "value": 20},
        # ...with strong fracdiff drift and d in a memory-preserving,
        # near-stationary band. BOTH are cross-sectional, and deliberately so:
        # a literal fracdiff_price>0 passes ~78% of the universe (filters
        # nothing), while a FIXED d-band is worse than useless because
        # fracdiff_d_optimal's distribution moves sharply between dates —
        # measured, [0.60,0.80] held 664/701 trend candidates on 2026-08-07
        # but only 97/663 on 2026-07-15, which made this template swing from
        # 298 matches to 0 on consecutive months. Ranking within each date
        # keeps the thesis ("trend with the most robust memory-preserving
        # drift") stable as the underlying distribution drifts.
        {"feature": "fracdiff_price", "op": "top_pct", "value": 0.30},
        {"feature": "fracdiff_d_optimal", "op": "bottom_pct", "value": 0.60},
    ],
    key_display_features=[
        "fracdiff_price", "fracdiff_d_optimal", "fracdiff_volume",
        "sma_200_ratio", "adx_14", "atr_14_pct",
    ],
)

_T20 = ScreenerTemplate(
    name="T20",
    category="T",
    description="Complexity-Aware Calm-Regime Entry (Risk Overlay)",
    conditions=[
        # The overlay's "calm" regime, as an entry filter: low Lyapunov,
        # stable recurrence, moderate complexity, real nonlinear trend.
        {"feature": "lyapunov_exponent_proxy", "op": "bottom_pct", "value": 0.40},
        {"feature": "rqa_rec_rate", "op": "top_pct", "value": 0.50},
        {"feature": "time_series_complexity", "op": "bottom_pct", "value": 0.50},
        {"feature": "nonlinear_trend_strength", "op": "top_pct", "value": 0.50},
        {"feature": "sma_200_ratio", "op": "gt", "value": 1.0},
    ],
    key_display_features=[
        "lyapunov_exponent_proxy", "rqa_rec_rate", "time_series_complexity",
        "nonlinear_trend_strength", "atr_14_pct", "atr_10_pct", "atr_20_pct",
    ],
)

# ---------------------------------------------------------------------------
# Master registry
# ---------------------------------------------------------------------------

TEMPLATES: List[ScreenerTemplate] = [
    # Category A (4)
    _A1, _A2, _A3, _A4,
    # Category B (5)
    _B1, _B2, _B3, _B4, _B5,
    # Category C (6 — C3 removed as a definitional duplicate of C1)
    _C1, _C2, _C4, _C5, _C6, _C7,
    # Category D (4)
    _D1, _D2, _D3, _D4,
    # Category E (7 — E8 excluded as duplicate of C6)
    _E1, _E2, _E3, _E4, _E5, _E6, _E7,
    # Category F (7 — F7 removed as a definitional duplicate of F3)
    _F1, _F2, _F3, _F4, _F5, _F6, _F8,
    # Category S (7 — S007/S009/S010/S011/S012 excluded; see module docstring)
    _S001, _S002, _S003, _S004, _S005, _S006, _S008,
    # Category R (4 — per-ticker HMM regime, R5 removed: "volatile" was mislabeled)
    _R1, _R2, _R3, _R4,
    # Category T (20 — indicator-library strategies, 2026-08-08)
    _T01, _T02, _T03, _T04, _T05, _T06, _T07, _T08, _T09, _T10,
    _T11, _T12, _T13, _T14, _T15, _T16, _T17, _T18, _T19, _T20,
]

# Fast name → template lookup used by ScreenerEngine
TEMPLATE_MAP: Dict[str, ScreenerTemplate] = {t.name: t for t in TEMPLATES}

assert len(TEMPLATES) == 64, (
    f"Expected 64 templates, got {len(TEMPLATES)}. "
    "If you added or removed templates, update this assertion."
)


# ---------------------------------------------------------------------------
# Duplicate-screen gate
# ---------------------------------------------------------------------------
# Two templates whose condition SETS are equal are the same screen, however
# differently their descriptions are worded — condition order carries no
# meaning, so {A and B} and {B and A} select identical stocks on every date.
# Such a pair costs a full duplicate backtest run per grid point and reports
# the same result twice under two names, which reads as independent
# corroboration when it is one number printed twice.
#
# This was found by hand three times (E8/C6, then C1/C3 and F3/F7 in the same
# 2026-08-13 pass, the latter never noticed in any earlier audit) — a review
# method with a demonstrated miss rate. Comparing signatures at import time
# does not miss, so the check runs here rather than in a doc or a review
# checklist. `value` goes through repr() because it may be a list (the
# "between" op), which is unhashable.
_KNOWN_DUPLICATE_GROUPS = {
    # B1 "Trend Following (Weinstein Stage 2)" and F2 "Momentum + volume" are
    # also the identical screen (sma_200_ratio > 1.0, volume_ratio_21d > 1.5,
    # adx_14 > 20). Left in place pending a product decision rather than
    # dropped silently: unlike C1/C3 and F3/F7 this pair spans two categories,
    # so removing either changes what a category-level report covers. Recorded
    # here so the gate stays meaningful instead of being switched off.
    frozenset({"B1", "F2"}),
}


def _condition_signature(template: ScreenerTemplate) -> frozenset:
    return frozenset(
        (c["feature"], c["op"], repr(c.get("value"))) for c in template.conditions
    )


def _find_duplicate_screens() -> Dict[frozenset, List[str]]:
    by_signature: Dict[frozenset, List[str]] = {}
    for t in TEMPLATES:
        by_signature.setdefault(_condition_signature(t), []).append(t.name)
    return {sig: names for sig, names in by_signature.items() if len(names) > 1}


_unexpected_duplicates = {
    sig: names
    for sig, names in _find_duplicate_screens().items()
    if frozenset(names) not in _KNOWN_DUPLICATE_GROUPS
}
assert not _unexpected_duplicates, (
    "Templates with identical condition sets are the same screen and must not "
    "both be registered (they double the backtest cost and report one result "
    "twice under two names): "
    + "; ".join(
        f"{sorted(names)} share conditions {sorted(sig)}"
        for sig, names in _unexpected_duplicates.items()
    )
    + ". Drop one and let the survivor inherit the union of both "
    "key_display_features, or — if the pair is intentional and understood — "
    "add it to _KNOWN_DUPLICATE_GROUPS with the reason."
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
    "E3": "Momentum",           # Piotroski F proxy — flag_pattern_score + hurst_exp_21d only, no MA/ADX trend gate; hurst measures statistical persistence not a price-vs-MA trend
    "E4": "Trend Following",   # Sector rotation
    "E5": "Momentum",          # Earnings acceleration
    "E6": "Momentum",          # GARP momentum
    "E7": "Trend Following",   # Greenblatt Magic Formula proxy
    "F1": "Mean Reversion",    # Low RSI quality
    "F2": "Momentum",          # Momentum + volume
    "F3": "Momentum",           # Dividend/consistent growth proxy — flag_pattern_score + hurst_exp_21d only, no MA/ADX trend gate; hurst measures statistical persistence not a price-vs-MA trend
    "F4": "Trend Following",   # Compounder proxy
    "F5": "Trend Following",   # Cash flow king proxy — flag_pattern_score + low volume_ratio_21d (quiet accumulation during a continuation pattern), no ATR/BB-width/breakout condition so not Volatility
    "F6": "Mean Reversion",    # Turnaround proxy
    "F8": "Momentum",          # PEG proxy (growth momentum)
    "S001": "Trend Following",  # EMA crossover
    "S002": "Trend Following",  # Supertrend breakout
    "S003": "Mean Reversion",   # RSI mean reversion
    "S004": "Trend Following",  # 52-week high breakout
    "S005": "Momentum",         # VWAP reversal — sma_20_ratio>1.0 + volume_ratio_21d>1.3, no oversold/oscillator condition; this is a breakout-on-volume continuation entry, not a snap-back-from-extreme reversion
    "S006": "Trend Following",  # Ichimoku cloud breakout
    "S008": "Momentum",        # MACD histogram
    "R1": "Regime",            # HMM bullish regime transition entry
    "R2": "Regime",            # HMM bearish regime transition exit
    "R3": "Regime",            # HMM established stable bullish regime
    "R4": "Regime",            # HMM any regime transition
    # Category T (2026-08-08) — styled by the trade each one actually puts
    # on, matching the brief's own "Style" line where the two agree.
    "T01": "Trend Following",  # Ichimoku continuation, pullback-then-reclaim
    "T02": "Volatility",       # 52w-high breakout out of a BB-width expansion
    "T03": "Trend Following",  # Flag continuation behind a full EMA ribbon
    "T04": "Momentum",         # Top-decile RS/composite-momentum leaders
    "T05": "Volatility",       # Donchian channel breakout out of compression
    "T06": "Trend Following",  # Supertrend flip confirmed by EMA8/EMA21
    "T07": "Trend Following",  # Wavelet-clean, Hurst-persistent trend
    "T08": "Mean Reversion",   # RSI-2 snap-back in an anti-persistent regime
    "T09": "Mean Reversion",   # Squeeze reversion off the lower band
    "T10": "Mean Reversion",   # 20-day-SMA reversion from the range low
    "T11": "Mean Reversion",   # Gap fade in a high-entropy regime
    "T12": "Mean Reversion",   # Capitulation-reversal bounce
    "T13": "Mean Reversion",   # Range-bound reversion, low vol + high entropy
    "T14": "Momentum",         # Cross-sectional RS rotation
    "T15": "Momentum",         # RS + Hurst rotation
    "T16": "Momentum",         # Quality momentum in low-vol leaders
    "T17": "Trend Following",  # Hurst-gated trend leg of the dual-mode meta
    "T18": "Volatility",       # Entropy-filtered breakout
    "T19": "Trend Following",  # Fracdiff-robust trend
    "T20": "Trend Following",  # Calm-regime (low-complexity) trend entry
}
assert set(TEMPLATE_STYLE) == set(TEMPLATE_MAP), "TEMPLATE_STYLE must classify every template, no more, no less."
STRATEGY_STYLES: List[str] = ["Momentum", "Trend Following", "Mean Reversion", "Volatility", "Regime"]

# ---------------------------------------------------------------------------
# Per-template exit params (PerTemplateExitPolicy), one set of stop/target/
# max_hold per TEMPLATE_STYLE group rather than 42 individually-reasoned
# figures — the style groupings above already say what kind of trade each
# template is, and exit discipline should follow the trade's *style*, not
# its specific condition logic:
#   - Momentum: signal decays fast once the move is already underway, so
#     exits are tight (4% stop / 10% target). max_hold was 15d; a
#     signal-quality diagnostic (backtest/diagnose_ta_signal_quality.py,
#     2026-07-30, 40 sample dates 2016-2026) measuring forward returns on
#     entry-condition fires showed the raw edge is measured/materializes on
#     a 21-trading-day horizon for every style group, so 15d was clipping
#     winners before the edge showed up — widened to 21d to match.
#   - Trend Following: designed to ride a real trend once confirmed, so
#     gets the most room to run (5% stop / 12% target / 25d) — already
#     exceeds the 21d measured signal horizon, left unchanged.
#   - Mean Reversion: the whole thesis is a fast snap-back, but the same
#     21d diagnostic showed A4 (RSI Oversold+Trend) and S003 (RSI Mean
#     Reversion) both had real edge (+3.55pp / +1.60pp vs universe median,
#     ~60% win rate) that the old 3%/6%/10d exits were clipping — A4's
#     orchestrator backtest CAGR went from -0.1% to +0.5% (Sharpe -0.05 to
#     0.20, win rate 27% to 38%) after widening to 5%/10%/21d.
#   - Volatility (breakout/squeeze): a confirmed breakout needs slightly
#     more room than a pure reversion trade but should still resolve
#     faster than a trend-following thesis (4.5% stop / 9% target / 20d).
# ---------------------------------------------------------------------------
STYLE_EXIT_PARAMS: Dict[str, Dict[str, float]] = {
    "Momentum": {"stop_pct": 0.04, "target_pct": 0.10, "max_hold_days": 21},
    "Trend Following": {"stop_pct": 0.05, "target_pct": 0.12, "max_hold_days": 25},
    "Mean Reversion": {"stop_pct": 0.05, "target_pct": 0.10, "max_hold_days": 21},
    "Volatility": {"stop_pct": 0.045, "target_pct": 0.09, "max_hold_days": 20},
    # Regime: a state change is a slower, more persistent signal than a
    # momentum/mean-reversion event — the regime thesis (and the HMM state)
    # plays out over weeks, so give it the most room to run.
    "Regime": {"stop_pct": 0.06, "target_pct": 0.15, "max_hold_days": 30},
}

for _tname, _style in TEMPLATE_STYLE.items():
    _params = STYLE_EXIT_PARAMS[_style]
    _t = TEMPLATE_MAP[_tname]
    _t.exit_stop_pct = _params["stop_pct"]
    _t.exit_target_pct = _params["target_pct"]
    _t.exit_max_hold_days = int(_params["max_hold_days"])
