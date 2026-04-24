"""
alphalens/ml/features/pipeline.py

Feature extraction pipeline for ML signal models.
Converts raw indicator rows into ordered feature vectors
matching what each model was trained on.
"""

import numpy as np
import pandas as pd
from typing import Optional


SIGNAL_FEATURES = {
    "intraday": [
        "rsi_9", "rsi_14", "stoch_k", "stoch_d", "williams_r",
        "macd", "macd_hist", "bb_pct_b", "volume_ratio", "atr_14",
        "pct_from_vwap", "gap_pct", "adx_14",
        "market_cycle_bull", "market_cycle_bear",
        "india_vix",
    ],
    "swing": [
        "rsi_9", "rsi_14", "rsi_21", "stoch_k", "stoch_d",
        "macd", "macd_signal", "macd_hist", "adx_14", "plus_di", "minus_di",
        "bb_pct_b", "bb_width", "atr_14", "supertrend_dir",
        "volume_ratio", "cmf_20", "mfi_14", "roc_10",
        "pct_from_52w_high", "pct_from_ema200",
        "above_50dma", "above_200dma", "ema50_vs_ema200",
        "rs_nifty200",
        "market_cycle_bull", "market_cycle_bear",
        "sector_cycle_bull", "sector_cycle_bear",
        "india_vix",
    ],
    "medium": [
        "rsi_14", "rsi_21", "adx_14", "macd_hist",
        "bb_pct_b", "atr_14", "supertrend_dir",
        "volume_ratio", "obv", "cmf_20",
        "pct_from_52w_high", "pct_from_52w_low", "pct_from_ema200",
        "above_50dma", "above_200dma", "ema50_vs_ema200",
        "ichimoku_above_cloud", "rs_nifty200",
        "hist_vol_21", "hist_vol_63",
        "market_cycle_bull", "market_cycle_bear", "market_cycle_neutral",
        "sector_cycle_bull", "sector_cycle_bear",
        "stock_cycle_bull", "stock_cycle_bear",
        "india_vix",
    ],
    "long_term": [
        "rsi_14", "adx_14", "macd_hist",
        "pct_from_52w_high", "pct_from_ema200",
        "above_200dma", "ema50_vs_ema200",
        "rs_nifty200", "hist_vol_63",
        "market_cycle_bull", "market_cycle_bear",
        "stock_cycle_bull", "stock_cycle_bear",
        "india_vix",
        # Fundamentals
        "pe_ratio", "pb_ratio", "roe", "roce",
        "debt_equity", "promoter_holding",
        "eps_growth_yoy", "revenue_growth",
    ],
}


class FeaturePipeline:

    def build_signal_features(self, row: pd.Series,
                                timeframe: str, ctx) -> list:
        """
        Build ordered feature vector for ML signal model inference.
        All missing values filled with 0.
        """
        feature_names = SIGNAL_FEATURES.get(timeframe, SIGNAL_FEATURES["swing"])
        cycle_enc     = self._encode_cycles(ctx, row)
        merged        = {**row.to_dict(), **cycle_enc}

        vector = []
        for name in feature_names:
            val = merged.get(name, 0.0)
            if val is None or (isinstance(val, float) and np.isnan(val)):
                val = 0.0
            vector.append(float(val))

        return vector

    def get_feature_names(self, timeframe: str) -> list:
        return SIGNAL_FEATURES.get(timeframe, SIGNAL_FEATURES["swing"])

    def _encode_cycles(self, ctx, row: pd.Series) -> dict:
        """Encode cycle labels as binary features."""
        mc = ctx.market_cycle if ctx else row.get("market_cycle", "neutral")
        sc = ctx.get_sector_cycle(row.get("sector", "")).get("cycle", "neutral") if ctx else "neutral"

        close  = float(row.get("close", 0) or 0)
        sma50  = float(row.get("sma_50",  0) or 0)
        sma200 = float(row.get("sma_200", 0) or 0)
        ema50  = float(row.get("ema_50",  0) or 0)
        ema200 = float(row.get("ema_200", 0) or 0)
        sa     = float(row.get("ichimoku_senkou_a", 0) or 0)
        sb     = float(row.get("ichimoku_senkou_b", 0) or 0)

        return {
            "market_cycle_bull":    1 if mc == "bull"    else 0,
            "market_cycle_bear":    1 if mc == "bear"    else 0,
            "market_cycle_neutral": 1 if mc == "neutral" else 0,
            "sector_cycle_bull":    1 if sc == "bull"    else 0,
            "sector_cycle_bear":    1 if sc == "bear"    else 0,
            "stock_cycle_bull":     0,   # filled from cycle context if available
            "stock_cycle_bear":     0,
            "above_50dma":          1 if close > sma50  else 0,
            "above_200dma":         1 if close > sma200 else 0,
            "ema50_vs_ema200":      (ema50 - ema200) / ema200 * 100 if ema200 else 0,
            "ichimoku_above_cloud": 1 if close > max(sa, sb) else 0,
        }
