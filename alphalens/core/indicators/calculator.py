"""
alphalens/core/indicators/calculator.py

Calculates all 40+ technical indicators for a given symbol using pandas-ta.
Reads OHLCV from DuckDB and writes calculated indicators back to DuckDB.

Handles:
  - Full recalculation (initial backfill)
  - Incremental update (last N bars)
  - Batch calculation across all symbols

Usage:
    calc = IndicatorCalculator()
    calc.calculate_all()                    # All 200 stocks
    calc.calculate_symbol("RELIANCE")       # Single stock
    calc.calculate_incremental()            # Last 30 days for all
"""

from datetime import date, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import pandas_ta as ta
from loguru import logger

from alphalens.core.database import get_duck
from alphalens.core.ingestion.universe import get_all_symbols

MIN_BARS_REQUIRED = 250   # Need 250 bars for 200-DMA to be meaningful


class IndicatorCalculator:

    def __init__(self):
        self.con = get_duck()

    # ── Public Methods ─────────────────────────────────────────────────────

    def calculate_all(self, symbols: Optional[list] = None) -> dict:
        """Calculate indicators for all symbols (or subset). Returns stats."""
        if symbols is None:
            symbols = get_all_symbols()

        logger.info(f"Calculating indicators for {len(symbols)} symbols")
        ok, failed = 0, []

        for symbol in symbols:
            try:
                n = self.calculate_symbol(symbol)
                if n > 0:
                    ok += 1
            except Exception as e:
                logger.warning(f"Indicator calc failed for {symbol}: {e}")
                failed.append(symbol)

        logger.info(f"Indicator calc complete: {ok} ok, {len(failed)} failed")
        return {"ok": ok, "failed": failed}

    def calculate_incremental(self, days: int = 30, symbols: Optional[list] = None) -> int:
        """Recalculate indicators for last N days for all symbols."""
        if symbols is None:
            symbols = get_all_symbols()

        total = 0
        for symbol in symbols:
            try:
                n = self.calculate_symbol(symbol, last_n_days=days)
                total += n
            except Exception as e:
                logger.debug(f"Incremental indicator calc failed for {symbol}: {e}")

        logger.info(f"Incremental indicator calc: {total} rows updated across {len(symbols)} symbols")
        return total

    def calculate_symbol(self, symbol: str, last_n_days: Optional[int] = None) -> int:
        """
        Calculate all indicators for one symbol.
        Returns number of rows written to technical_indicators table.
        """
        df = self._load_prices(symbol, last_n_days)
        if df is None or len(df) < MIN_BARS_REQUIRED:
            logger.debug(f"{symbol}: insufficient bars ({len(df) if df is not None else 0})")
            return 0

        indicators = self._compute_indicators(df)
        if indicators.empty:
            return 0

        count = self._store_indicators(symbol, indicators)
        return count

    def get_latest_indicators(self, symbol: str) -> Optional[dict]:
        """Return the most recent indicator row for a symbol as a dict."""
        result = self.con.execute("""
            SELECT * FROM technical_indicators
            WHERE symbol = ?
            ORDER BY date DESC LIMIT 1
        """, [symbol]).fetchdf()

        if result.empty:
            return None
        return result.iloc[0].to_dict()

    # ── Core Computation ──────────────────────────────────────────────────

    def _compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute all technical indicators on an OHLCV DataFrame."""
        # pandas-ta strategy — define all at once for efficiency
        custom_strategy = ta.Strategy(
            name="alphalens_full",
            ta=[
                # Trend
                {"kind": "ema",  "length": 9},
                {"kind": "ema",  "length": 20},
                {"kind": "ema",  "length": 50},
                {"kind": "ema",  "length": 100},
                {"kind": "ema",  "length": 200},
                {"kind": "sma",  "length": 20},
                {"kind": "sma",  "length": 50},
                {"kind": "sma",  "length": 200},
                {"kind": "macd", "fast": 12, "slow": 26, "signal": 9},
                {"kind": "adx",  "length": 14},
                {"kind": "psar"},
                {"kind": "ichimoku"},
                # Momentum
                {"kind": "rsi",   "length": 9},
                {"kind": "rsi",   "length": 14},
                {"kind": "rsi",   "length": 21},
                {"kind": "stoch", "k": 14, "d": 3, "smooth_k": 3},
                {"kind": "stochrsi"},
                {"kind": "willr", "length": 14},
                {"kind": "cci",   "length": 20},
                {"kind": "roc",   "length": 10},
                {"kind": "roc",   "length": 20},
                {"kind": "mfi",   "length": 14},
                # Volatility
                {"kind": "bbands", "length": 20, "std": 2},
                {"kind": "atr",    "length": 14},
                {"kind": "atr",    "length": 21},
                {"kind": "kc"},
                {"kind": "natr",   "length": 14},
                # Volume
                {"kind": "obv"},
                {"kind": "cmf",  "length": 20},
                {"kind": "pvol"},
            ]
        )

        df.ta.strategy(custom_strategy)

        out = pd.DataFrame(index=df.index)
        out["date"] = df.index

        # ── Trend ──────────────────────────────────────────────────────────
        out["ema_9"]   = df.get("EMA_9")
        out["ema_20"]  = df.get("EMA_20")
        out["ema_50"]  = df.get("EMA_50")
        out["ema_100"] = df.get("EMA_100")
        out["ema_200"] = df.get("EMA_200")
        out["sma_20"]  = df.get("SMA_20")
        out["sma_50"]  = df.get("SMA_50")
        out["sma_200"] = df.get("SMA_200")

        out["macd"]        = df.get("MACD_12_26_9")
        out["macd_signal"] = df.get("MACDs_12_26_9")
        out["macd_hist"]   = df.get("MACDh_12_26_9")

        out["adx_14"]  = df.get("ADX_14")
        out["plus_di"] = df.get("DMP_14")
        out["minus_di"]= df.get("DMN_14")

        out["psar"] = df.get("PSARl_0.02_0.2") if "PSARl_0.02_0.2" in df.columns else df.get("PSAR_0.02_0.2_0.0")

        # Ichimoku
        out["ichimoku_tenkan"]   = df.get("ITS_9")
        out["ichimoku_kijun"]    = df.get("IKS_26")
        out["ichimoku_senkou_a"] = df.get("ISA_9")
        out["ichimoku_senkou_b"] = df.get("ISB_26")
        out["ichimoku_chikou"]   = df.get("ICS_26")

        # Supertrend (compute manually — pandas-ta supertrend needs explicit call)
        st_result = self._supertrend(df["high"], df["low"], df["close"], period=10, multiplier=3.0)
        out["supertrend"]     = st_result["supertrend"]
        out["supertrend_dir"] = st_result["direction"]

        # ── Momentum ───────────────────────────────────────────────────────
        out["rsi_9"]   = df.get("RSI_9")
        out["rsi_14"]  = df.get("RSI_14")
        out["rsi_21"]  = df.get("RSI_21")

        out["stoch_k"]   = df.get("STOCHk_14_3_3")
        out["stoch_d"]   = df.get("STOCHd_14_3_3")
        out["stoch_rsi"] = df.get("STOCHRSId_14_14_3_3")

        out["williams_r"] = df.get("WILLR_14")
        out["cci_20"]     = df.get("CCI_20_0.015")
        out["roc_10"]     = df.get("ROC_10")
        out["roc_20"]     = df.get("ROC_20")
        out["mfi_14"]     = df.get("MFI_14")

        # ── Volatility ─────────────────────────────────────────────────────
        out["bb_upper"]  = df.get("BBU_20_2.0")
        out["bb_mid"]    = df.get("BBM_20_2.0")
        out["bb_lower"]  = df.get("BBL_20_2.0")
        out["bb_pct_b"]  = df.get("BBP_20_2.0")
        out["bb_width"]  = df.get("BBB_20_2.0")

        out["atr_14"] = df.get("ATRr_14")
        out["atr_21"] = df.get("ATRr_21")

        out["kc_upper"] = df.get("KCUe_20_2")
        out["kc_lower"] = df.get("KCLe_20_2")

        # Historical volatility (annualised std of log returns)
        log_ret = np.log(df["close"] / df["close"].shift(1))
        out["hist_vol_21"] = log_ret.rolling(21).std() * np.sqrt(252) * 100
        out["hist_vol_63"] = log_ret.rolling(63).std() * np.sqrt(252) * 100

        # ── Volume ─────────────────────────────────────────────────────────
        out["obv"]           = df.get("OBV")
        out["cmf_20"]        = df.get("CMF_20")
        out["volume_sma20"]  = df["volume"].rolling(20).mean()
        out["volume_ratio"]  = df["volume"] / out["volume_sma20"]

        # ── Price Structure ────────────────────────────────────────────────
        rolling_52w          = df["close"].rolling(252, min_periods=50)
        out["pct_from_52w_high"] = (df["close"] / rolling_52w.max() - 1) * 100
        out["pct_from_52w_low"]  = (df["close"] / rolling_52w.min() - 1) * 100
        out["pct_from_ema200"]   = np.where(
            out["ema_200"].notna(),
            (df["close"] - out["ema_200"]) / out["ema_200"] * 100,
            np.nan
        )

        # rs_nifty200 — requires Nifty200 close; filled later by cycle module
        out["rs_nifty200"] = np.nan

        out = out.dropna(subset=["date"])
        out["date"] = pd.to_datetime(out["date"]).dt.date
        return out

    # ── Supertrend ─────────────────────────────────────────────────────────

    @staticmethod
    def _supertrend(high: pd.Series, low: pd.Series, close: pd.Series,
                    period: int = 10, multiplier: float = 3.0) -> pd.DataFrame:
        """Calculate Supertrend indicator. Returns DataFrame with supertrend and direction."""
        hl2    = (high + low) / 2
        # ATR via Wilder's smoothing
        tr     = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low  - close.shift()).abs()
        ], axis=1).max(axis=1)
        atr    = tr.ewm(alpha=1/period, adjust=False).mean()

        upper  = hl2 + multiplier * atr
        lower  = hl2 - multiplier * atr

        supertrend = pd.Series(np.nan, index=close.index)
        direction  = pd.Series(0,     index=close.index, dtype=int)

        for i in range(1, len(close)):
            prev_upper = upper.iloc[i-1] if not pd.isna(upper.iloc[i-1]) else upper.iloc[i]
            prev_lower = lower.iloc[i-1] if not pd.isna(lower.iloc[i-1]) else lower.iloc[i]

            # Adjust bands
            lower.iloc[i] = lower.iloc[i] if lower.iloc[i] > prev_lower or close.iloc[i-1] < prev_lower else prev_lower
            upper.iloc[i] = upper.iloc[i] if upper.iloc[i] < prev_upper or close.iloc[i-1] > prev_upper else prev_upper

            prev_st = supertrend.iloc[i-1]
            if pd.isna(prev_st):
                direction.iloc[i] = 1   # default bull
                supertrend.iloc[i] = lower.iloc[i]
            elif prev_st == prev_upper:
                direction.iloc[i] = -1 if close.iloc[i] > upper.iloc[i] else 1
                supertrend.iloc[i] = lower.iloc[i] if direction.iloc[i] == -1 else upper.iloc[i]
            else:
                direction.iloc[i] = 1 if close.iloc[i] < lower.iloc[i] else -1
                supertrend.iloc[i] = upper.iloc[i] if direction.iloc[i] == 1 else lower.iloc[i]

        return pd.DataFrame({"supertrend": supertrend, "direction": direction})

    # ── DB I/O ─────────────────────────────────────────────────────────────

    def _load_prices(self, symbol: str, last_n_days: Optional[int] = None) -> Optional[pd.DataFrame]:
        """Load OHLCV from DuckDB into a pandas DataFrame with Date index."""
        if last_n_days:
            # Load extra history for indicator warmup
            lookback = last_n_days + 300
            df = self.con.execute("""
                SELECT date, open, high, low, close, volume
                FROM daily_prices
                WHERE symbol = ?
                ORDER BY date DESC
                LIMIT ?
            """, [symbol, lookback]).fetchdf()
        else:
            df = self.con.execute("""
                SELECT date, open, high, low, close, volume
                FROM daily_prices
                WHERE symbol = ?
                ORDER BY date ASC
            """, [symbol]).fetchdf()

        if df.empty:
            return None

        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").set_index("date")
        return df

    def _store_indicators(self, symbol: str, indicators: pd.DataFrame) -> int:
        """Write indicator rows to DuckDB technical_indicators table."""
        if indicators.empty:
            return 0

        indicators = indicators.copy()
        indicators["symbol"] = symbol

        # Get all columns that exist in our target table
        table_cols = self.con.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'technical_indicators'
        """).fetchall()
        table_col_set = {c[0] for c in table_cols}

        cols_to_write = [c for c in indicators.columns if c in table_col_set]
        sub = indicators[cols_to_write]

        # Replace NaN with None for SQL NULL
        sub = sub.where(pd.notna(sub), None)

        col_str      = ", ".join(cols_to_write)
        placeholders = ", ".join(["?"] * len(cols_to_write))

        self.con.executemany(
            f"INSERT OR REPLACE INTO technical_indicators ({col_str}) VALUES ({placeholders})",
            sub.values.tolist()
        )
        return len(sub)
