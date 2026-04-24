"""
alphalens/core/strategy/library.py

Seed library of 12 trading strategies with full parameter definitions.

Each strategy is stored in the DuckDB strategies table with:
  - entry_rules:    JSON dict defining exact entry conditions
  - exit_rules:     JSON dict defining exit conditions
  - stoploss_rules: JSON dict defining stop-loss calculation
  - parameters:     JSON dict of all tunable parameters

The rule dicts are interpreted by the backtester and signal generator.
This design allows the genetic algorithm to mutate parameters while
keeping the structural logic intact.

Usage:
    from alphalens.core.strategy.library import seed_strategy_library
    seed_strategy_library()   # Called once during --init
"""

import json
import uuid
from datetime import datetime

from loguru import logger

from alphalens.core.database import get_duck


def seed_strategy_library():
    """Insert all 12 seed strategies into DuckDB strategies table."""
    con = get_duck()
    count = 0

    for strategy in STRATEGY_DEFINITIONS:
        existing = con.execute(
            "SELECT strategy_id FROM strategies WHERE name = ?",
            [strategy["name"]]
        ).fetchone()

        if existing:
            logger.debug(f"Strategy already exists: {strategy['name']}")
            continue

        strategy_id = strategy.get("strategy_id", str(uuid.uuid4())[:8])
        con.execute("""
            INSERT INTO strategies (
                strategy_id, name, type, description,
                timeframes, best_cycles,
                entry_rules, exit_rules, stoploss_rules, parameters,
                discovered_by, is_active, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'seeded', true, ?)
        """, [
            strategy_id,
            strategy["name"],
            strategy["type"],
            strategy["description"],
            json.dumps(strategy["timeframes"]),
            json.dumps(strategy["best_cycles"]),
            json.dumps(strategy["entry_rules"]),
            json.dumps(strategy["exit_rules"]),
            json.dumps(strategy["stoploss_rules"]),
            json.dumps(strategy["parameters"]),
            datetime.now(),
        ])
        count += 1

    logger.info(f"Strategy library seeded: {count} new strategies added")
    return count


def get_all_strategies(active_only: bool = True) -> list:
    """Return all strategies as list of dicts."""
    con = get_duck()
    where = "WHERE is_active = true" if active_only else ""
    rows  = con.execute(f"SELECT * FROM strategies {where} ORDER BY name").fetchdf()
    return rows.to_dict("records")


def get_strategy(strategy_id: str) -> dict | None:
    """Return a single strategy by ID."""
    con = get_duck()
    row = con.execute(
        "SELECT * FROM strategies WHERE strategy_id = ?", [strategy_id]
    ).fetchdf()
    if row.empty:
        return None
    r = row.iloc[0].to_dict()
    for key in ("entry_rules", "exit_rules", "stoploss_rules", "parameters",
                "timeframes", "best_cycles"):
        if isinstance(r.get(key), str):
            try:
                r[key] = json.loads(r[key])
            except Exception:
                pass
    return r


# ── Strategy Definitions ──────────────────────────────────────────────────
# Rule schema for entry_rules:
#   indicators: list of (indicator_name, operator, value_or_indicator)
#   logic: "AND" | "OR"
#   confirmation: optional additional conditions
#
# Operators: >, <, >=, <=, ==, crosses_above, crosses_below, is_true
# Values can be: float, "indicator:NAME" (reference another indicator)

STRATEGY_DEFINITIONS = [

    # ── 01: EMA Crossover Momentum ────────────────────────────────────────
    {
        "strategy_id": "S001",
        "name":        "EMA Crossover Momentum",
        "type":        "trend_following",
        "description": (
            "Classic EMA crossover — short EMA crosses above long EMA with "
            "ADX confirming trend strength and RSI in bullish zone. "
            "Works best in trending Bull markets. Avoids choppy Neutral markets."
        ),
        "timeframes":   ["swing", "medium"],
        "best_cycles":  ["bull"],
        "entry_rules": {
            "logic": "AND",
            "conditions": [
                {"indicator": "ema_9",  "op": "crosses_above", "value": "indicator:ema_21"},
                {"indicator": "adx_14", "op": ">=",            "value": 25},
                {"indicator": "rsi_14", "op": ">=",            "value": 50},
                {"indicator": "rsi_14", "op": "<=",            "value": 70},
                {"indicator": "close",  "op": ">",             "value": "indicator:ema_50"},
            ],
            "confirmation": {"volume_ratio": {"op": ">=", "value": 1.2}},
        },
        "exit_rules": {
            "logic": "OR",
            "conditions": [
                {"indicator": "ema_9", "op": "crosses_below", "value": "indicator:ema_21"},
                {"indicator": "rsi_14", "op": ">=", "value": 75},
            ],
        },
        "stoploss_rules": {
            "type":       "atr_based",
            "atr_period": 14,
            "multiplier": 2.0,
            "trailing":   True,
        },
        "parameters": {
            "ema_fast":        9,
            "ema_slow":        21,
            "adx_threshold":   25,
            "rsi_min":         50,
            "rsi_max":         70,
            "rsi_exit":        75,
            "volume_min_ratio": 1.2,
            "atr_sl_mult":     2.0,
            "atr_target_mult": 3.0,
        },
    },

    # ── 02: Supertrend Breakout ───────────────────────────────────────────
    {
        "strategy_id": "S002",
        "name":        "Supertrend Breakout",
        "type":        "trend_following",
        "description": (
            "Price flips from below to above the Supertrend line, confirmed by "
            "a volume surge. One of the cleanest trend-entry signals on Indian "
            "stocks. Very reliable in Bull and early Neutral markets."
        ),
        "timeframes":  ["swing", "medium"],
        "best_cycles": ["bull", "neutral"],
        "entry_rules": {
            "logic": "AND",
            "conditions": [
                {"indicator": "supertrend_dir", "op": "crosses_above", "value": 0},
                {"indicator": "volume_ratio",   "op": ">=",            "value": 1.5},
                {"indicator": "close",          "op": ">",             "value": "indicator:ema_20"},
            ],
        },
        "exit_rules": {
            "logic": "OR",
            "conditions": [
                {"indicator": "supertrend_dir", "op": "crosses_below", "value": 0},
            ],
        },
        "stoploss_rules": {
            "type":       "supertrend",
            "use_supertrend_as_sl": True,
            "atr_period": 10,
            "multiplier": 3.0,
        },
        "parameters": {
            "atr_period":    10,
            "multiplier":    3.0,
            "volume_min_ratio": 1.5,
            "ema_filter":    20,
            "atr_target_mult": 2.5,
        },
    },

    # ── 03: RSI Mean Reversion ────────────────────────────────────────────
    {
        "strategy_id": "S003",
        "name":        "RSI Mean Reversion",
        "type":        "mean_reversion",
        "description": (
            "Buy extreme oversold conditions (RSI < 35) when price is at or "
            "near the lower Bollinger Band with a bullish RSI divergence. "
            "Works well in Neutral and early Bull recoveries. "
            "Avoid in sustained Bear markets."
        ),
        "timeframes":  ["swing"],
        "best_cycles": ["neutral", "bull"],
        "entry_rules": {
            "logic": "AND",
            "conditions": [
                {"indicator": "rsi_14",  "op": "<=",  "value": 35},
                {"indicator": "bb_pct_b","op": "<=",  "value": 0.1},
                {"indicator": "close",   "op": ">",   "value": "indicator:bb_lower"},
                {"indicator": "volume_ratio", "op": ">=", "value": 1.0},
            ],
        },
        "exit_rules": {
            "logic": "OR",
            "conditions": [
                {"indicator": "rsi_14",  "op": ">=", "value": 55},
                {"indicator": "bb_pct_b","op": ">=", "value": 0.5},
            ],
        },
        "stoploss_rules": {
            "type":        "atr_based",
            "atr_period":  14,
            "multiplier":  1.5,
            "trailing":    False,
        },
        "parameters": {
            "rsi_entry":     35,
            "rsi_exit":      55,
            "bb_pct_entry":  0.1,
            "bb_pct_exit":   0.5,
            "atr_sl_mult":   1.5,
            "atr_target_mult": 2.5,
        },
    },

    # ── 04: 52-Week High Breakout ─────────────────────────────────────────
    {
        "strategy_id": "S004",
        "name":        "52-Week High Breakout",
        "type":        "breakout",
        "description": (
            "Stocks breaking out to new 52-week highs on high volume often "
            "continue to make new highs (momentum persistence). Used by "
            "CANSLIM and IBD strategies. Requires sector and market alignment."
        ),
        "timeframes":  ["medium", "long_term"],
        "best_cycles": ["bull"],
        "entry_rules": {
            "logic": "AND",
            "conditions": [
                {"indicator": "pct_from_52w_high", "op": ">=", "value": -1.0},
                {"indicator": "volume_ratio",       "op": ">=", "value": 2.0},
                {"indicator": "close",              "op": ">",  "value": "indicator:ema_50"},
                {"indicator": "adx_14",             "op": ">=", "value": 20},
            ],
        },
        "exit_rules": {
            "logic": "OR",
            "conditions": [
                {"indicator": "pct_from_52w_high", "op": "<=", "value": -10.0},
                {"indicator": "close",             "op": "<",  "value": "indicator:ema_50"},
            ],
        },
        "stoploss_rules": {
            "type":       "trailing_pct",
            "trail_pct":  8.0,
        },
        "parameters": {
            "near_high_threshold": -1.0,
            "volume_min_ratio":     2.0,
            "trail_stop_pct":       8.0,
            "ema_filter":          50,
            "adx_min":             20,
            "atr_target_mult":     4.0,
        },
    },

    # ── 05: VWAP Intraday Reversal ────────────────────────────────────────
    {
        "strategy_id": "S005",
        "name":        "VWAP Intraday Reversal",
        "type":        "mean_reversion",
        "description": (
            "Intraday: stock dips 1.5%+ below VWAP in a strong market, "
            "showing oversold RSI and high volume — institutional buyers "
            "often step in at VWAP. All intraday positions must be closed "
            "by 3:15 PM regardless of outcome."
        ),
        "timeframes":  ["intraday"],
        "best_cycles": ["bull", "neutral"],
        "entry_rules": {
            "logic": "AND",
            "conditions": [
                {"indicator": "pct_from_vwap", "op": "<=",  "value": -1.5},
                {"indicator": "rsi_9",          "op": "<=",  "value": 35},
                {"indicator": "volume_ratio",   "op": ">=",  "value": 1.5},
            ],
        },
        "exit_rules": {
            "logic": "OR",
            "conditions": [
                {"indicator": "close", "op": ">=", "value": "indicator:vwap"},
                {"indicator": "time",  "op": ">=", "value": "15:15"},
            ],
        },
        "stoploss_rules": {
            "type":          "fixed_pct",
            "pct":           1.0,
            "eod_force_exit": True,
        },
        "parameters": {
            "vwap_dip_pct":   -1.5,
            "rsi_entry":       35,
            "volume_min_ratio": 1.5,
            "sl_pct":          1.0,
            "target_pct":      1.5,
        },
    },

    # ── 06: Ichimoku Cloud Breakout ───────────────────────────────────────
    {
        "strategy_id": "S006",
        "name":        "Ichimoku Cloud Breakout",
        "type":        "trend_following",
        "description": (
            "Price emerges above the Ichimoku cloud with Tenkan > Kijun "
            "and Chikou span above the price — the full Ichimoku buy signal. "
            "High-conviction entries. Suited for medium and long-term holds."
        ),
        "timeframes":  ["medium", "long_term"],
        "best_cycles": ["bull"],
        "entry_rules": {
            "logic": "AND",
            "conditions": [
                {"indicator": "close",            "op": ">",             "value": "indicator:ichimoku_senkou_a"},
                {"indicator": "close",            "op": ">",             "value": "indicator:ichimoku_senkou_b"},
                {"indicator": "ichimoku_tenkan",  "op": ">",             "value": "indicator:ichimoku_kijun"},
                {"indicator": "ichimoku_chikou",  "op": ">",             "value": "indicator:close_26d_ago"},
            ],
        },
        "exit_rules": {
            "logic": "OR",
            "conditions": [
                {"indicator": "close",           "op": "<", "value": "indicator:ichimoku_senkou_b"},
                {"indicator": "ichimoku_tenkan", "op": "<", "value": "indicator:ichimoku_kijun"},
            ],
        },
        "stoploss_rules": {
            "type":       "ichimoku",
            "use_cloud_bottom_as_sl": True,
        },
        "parameters": {
            "tenkan_period":  9,
            "kijun_period":  26,
            "senkou_period": 52,
            "atr_target_mult": 3.0,
        },
    },

    # ── 07: Turtle Trading (20/10) ────────────────────────────────────────
    {
        "strategy_id": "S007",
        "name":        "Turtle Trading 20-10",
        "type":        "breakout",
        "description": (
            "The original Turtle Trading System by Richard Dennis: buy when "
            "price makes a new 20-day high, sell when price makes a new 10-day "
            "low. Position sized by ATR. Simple, robust, effective in trending markets."
        ),
        "timeframes":  ["medium", "long_term"],
        "best_cycles": ["bull"],
        "entry_rules": {
            "logic": "AND",
            "conditions": [
                {"indicator": "close", "op": ">=", "value": "indicator:high_20d"},
                {"indicator": "adx_14","op": ">=", "value": 15},
            ],
        },
        "exit_rules": {
            "logic": "OR",
            "conditions": [
                {"indicator": "close", "op": "<=", "value": "indicator:low_10d"},
            ],
        },
        "stoploss_rules": {
            "type":       "atr_based",
            "atr_period": 20,
            "multiplier": 2.0,
            "trailing":   True,
        },
        "parameters": {
            "entry_period":    20,
            "exit_period":     10,
            "atr_period":      20,
            "atr_sl_mult":     2.0,
            "adx_min":         15,
        },
    },

    # ── 08: MACD Histogram Divergence ─────────────────────────────────────
    {
        "strategy_id": "S008",
        "name":        "MACD Histogram Divergence",
        "type":        "momentum",
        "description": (
            "Bullish divergence: price makes a lower low but MACD histogram "
            "makes a higher low — early momentum reversal signal. "
            "Most effective in Neutral markets transitioning to Bull."
        ),
        "timeframes":  ["swing", "medium"],
        "best_cycles": ["neutral", "bull"],
        "entry_rules": {
            "logic": "AND",
            "conditions": [
                {"indicator": "macd_hist",          "op": "bullish_divergence", "lookback": 10},
                {"indicator": "rsi_14",             "op": ">=",                 "value": 40},
                {"indicator": "macd_hist",          "op": ">",                  "value": "indicator:macd_hist_prev"},
                {"indicator": "volume_ratio",       "op": ">=",                 "value": 1.0},
            ],
        },
        "exit_rules": {
            "logic": "OR",
            "conditions": [
                {"indicator": "macd_hist", "op": "<",  "value": 0},
                {"indicator": "rsi_14",    "op": ">=", "value": 70},
            ],
        },
        "stoploss_rules": {
            "type":       "atr_based",
            "atr_period": 14,
            "multiplier": 1.8,
            "trailing":   False,
        },
        "parameters": {
            "divergence_lookback": 10,
            "rsi_min":             40,
            "rsi_exit":            70,
            "atr_sl_mult":         1.8,
            "atr_target_mult":     2.5,
        },
    },

    # ── 09: Bollinger Band Squeeze Breakout ───────────────────────────────
    {
        "strategy_id": "S009",
        "name":        "Bollinger Band Squeeze Breakout",
        "type":        "volatility_breakout",
        "description": (
            "BB width contracts to a 6-month low (squeeze = coiled spring), "
            "then price breaks above the upper band on volume. "
            "Captures explosive moves after prolonged consolidation. "
            "Works in both Bull and Bear directions — direction filter important."
        ),
        "timeframes":  ["swing", "medium"],
        "best_cycles": ["bull", "neutral"],
        "entry_rules": {
            "logic": "AND",
            "conditions": [
                {"indicator": "bb_width",   "op": "squeeze_breakout", "squeeze_lookback": 126},
                {"indicator": "close",      "op": ">",                "value": "indicator:bb_upper"},
                {"indicator": "volume_ratio","op": ">=",              "value": 1.8},
                {"indicator": "close",      "op": ">",                "value": "indicator:ema_20"},
            ],
        },
        "exit_rules": {
            "logic": "OR",
            "conditions": [
                {"indicator": "close", "op": "<",  "value": "indicator:bb_mid"},
                {"indicator": "rsi_14","op": ">=", "value": 78},
            ],
        },
        "stoploss_rules": {
            "type":       "atr_based",
            "atr_period": 14,
            "multiplier": 2.0,
            "trailing":   True,
        },
        "parameters": {
            "squeeze_lookback": 126,
            "volume_min_ratio": 1.8,
            "rsi_exit":         78,
            "atr_sl_mult":      2.0,
            "atr_target_mult":  3.0,
        },
    },

    # ── 10: Fundamental Value + Momentum ─────────────────────────────────
    {
        "strategy_id": "S010",
        "name":        "Fundamental Value + Momentum",
        "type":        "value_momentum",
        "description": (
            "Quality stocks at value prices showing early price momentum — "
            "P/E below sector average, ROE > 15%, revenue growing, and "
            "stock beginning to outperform Nifty200. Long-term hold strategy. "
            "Best deployed in Bear/Neutral when quality stocks are cheap."
        ),
        "timeframes":  ["long_term"],
        "best_cycles": ["bear", "neutral"],
        "entry_rules": {
            "logic": "AND",
            "conditions": [
                {"indicator": "pe_vs_sector",  "op": "<=", "value": 0.85},
                {"indicator": "roe",           "op": ">=", "value": 15},
                {"indicator": "revenue_growth","op": ">=", "value": 10},
                {"indicator": "close",         "op": ">",  "value": "indicator:ema_200"},
                {"indicator": "rs_nifty200",   "op": ">",  "value": 1.0},
            ],
        },
        "exit_rules": {
            "logic": "OR",
            "conditions": [
                {"indicator": "pe_vs_sector",  "op": ">=", "value": 1.3},
                {"indicator": "roe",           "op": "<=", "value": 10},
                {"indicator": "close",         "op": "<",  "value": "indicator:ema_200"},
                {"indicator": "time_held_months","op": ">=","value": 18},
            ],
        },
        "stoploss_rules": {
            "type":    "fixed_pct",
            "pct":     15.0,
            "trailing": False,
        },
        "parameters": {
            "pe_discount":     0.85,
            "roe_min":         15.0,
            "revenue_growth_min": 10.0,
            "sl_pct":          15.0,
            "max_hold_months": 18,
        },
    },

    # ── 11: Gap & Go ──────────────────────────────────────────────────────
    {
        "strategy_id": "S011",
        "name":        "Gap and Go",
        "type":        "momentum",
        "description": (
            "Intraday: stock gaps up >1.5% at open and holds above the "
            "previous day's close by 9:25 AM — institutional buying confirmation. "
            "Trade is taken at 9:25 AM entry. Tight SL at previous close. "
            "Target: gap size × 2. All positions closed by 3:15 PM."
        ),
        "timeframes":  ["intraday"],
        "best_cycles": ["bull"],
        "entry_rules": {
            "logic": "AND",
            "conditions": [
                {"indicator": "gap_pct",   "op": ">=", "value": 1.5},
                {"indicator": "close",     "op": ">=", "value": "indicator:prev_close"},
                {"indicator": "time",      "op": ">=", "value": "09:25"},
                {"indicator": "volume_ratio","op": ">=","value": 2.0},
            ],
        },
        "exit_rules": {
            "logic": "OR",
            "conditions": [
                {"indicator": "return_pct","op": ">=", "value": "indicator:gap_pct_x2"},
                {"indicator": "time",      "op": ">=", "value": "15:15"},
                {"indicator": "close",     "op": "<",  "value": "indicator:prev_close"},
            ],
        },
        "stoploss_rules": {
            "type":          "prev_close",
            "eod_force_exit": True,
        },
        "parameters": {
            "min_gap_pct":    1.5,
            "volume_min_ratio": 2.0,
            "target_mult":    2.0,
        },
    },

    # ── 12: Sector Rotation ───────────────────────────────────────────────
    {
        "strategy_id": "S012",
        "name":        "Sector Rotation Momentum",
        "type":        "macro_rotation",
        "description": (
            "Buy top-ranked stocks in sectors that are transitioning from "
            "neutral to bull cycle. Uses relative strength (RS) ranking "
            "within sector — buy stocks in the top 30% RS rank when their "
            "sector just turned bullish. Exit when sector weakens."
        ),
        "timeframes":  ["medium", "long_term"],
        "best_cycles": ["neutral", "bull"],
        "entry_rules": {
            "logic": "AND",
            "conditions": [
                {"indicator": "sector_cycle",    "op": "==",  "value": "bull"},
                {"indicator": "rs_percentile",   "op": ">=",  "value": 70},
                {"indicator": "close",           "op": ">",   "value": "indicator:ema_50"},
                {"indicator": "rsi_14",          "op": ">=",  "value": 50},
                {"indicator": "adx_14",          "op": ">=",  "value": 20},
            ],
        },
        "exit_rules": {
            "logic": "OR",
            "conditions": [
                {"indicator": "sector_cycle",  "op": "==",  "value": "bear"},
                {"indicator": "rs_percentile", "op": "<=",  "value": 40},
                {"indicator": "close",         "op": "<",   "value": "indicator:ema_50"},
            ],
        },
        "stoploss_rules": {
            "type":       "atr_based",
            "atr_period": 14,
            "multiplier": 2.5,
            "trailing":   True,
        },
        "parameters": {
            "rs_min_percentile":  70,
            "rs_exit_percentile": 40,
            "adx_min":            20,
            "ema_filter":         50,
            "atr_sl_mult":        2.5,
            "atr_target_mult":    3.5,
        },
    },
]
