"""
alphalens/core/cycle/labeller.py

Generates historical Bull / Bear / Neutral labels for:
  - Market level  (Nifty200)
  - Sector level  (12 sector indices)
  - Stock level   (individual Nifty200 stocks)

These labels become the training targets for the cycle classifiers.

Labelling methodology:
  1. Drawdown-based regime detection
     - Bear: rolling drawdown from peak > threshold AND price < DMA
     - Bull: price near highs AND price > DMA AND RSI confirming
     - Neutral: everything else
  2. Duration filter: regimes < min_days collapse to neutral
  3. Smooth transitions: no flip-flop — regime must hold N days

Usage:
    labeller = CycleLabeller()
    df = labeller.label_market(from_date="2009-01-01")
    df = labeller.label_sector("IT", from_date="2009-01-01")
    df = labeller.label_stock("RELIANCE", from_date="2009-01-01")
    labeller.label_all_and_store()   # labels all → writes to market_cycles table
"""

from datetime import date, timedelta
from typing import Optional

import numpy as np
import pandas as pd
from loguru import logger

from alphalens.core.database import get_duck


# ── Labelling parameters per scope ────────────────────────────────────────

MARKET_PARAMS = {
    "bear_drawdown":     -0.15,   # >15% peak-to-trough = bear candidate
    "bull_recovery":      0.15,   # >15% from trough = bull candidate
    "neutral_band":       0.07,   # within 7% of recent high = neutral/bull boundary
    "dma_period":         200,    # DMA to use for trend gate
    "rsi_period":         14,
    "bear_rsi_max":       45,     # RSI must be < 45 to confirm bear
    "bull_rsi_min":       55,     # RSI must be > 55 to confirm bull
    "min_duration_days":  20,     # minimum regime length
}

SECTOR_PARAMS = {
    "bear_drawdown":     -0.12,
    "bull_recovery":      0.12,
    "neutral_band":       0.06,
    "dma_period":         50,    # sector cycles shorter, use 50 DMA
    "rsi_period":         14,
    "bear_rsi_max":       45,
    "bull_rsi_min":       55,
    "min_duration_days":  15,
}

STOCK_PARAMS = {
    "bear_drawdown":     -0.20,   # stocks move more — wider threshold
    "bull_recovery":      0.20,
    "neutral_band":       0.10,
    "dma_period":         50,
    "rsi_period":         14,
    "bear_rsi_max":       45,
    "bull_rsi_min":       55,
    "min_duration_days":  10,
}

SCOPE_SYMBOL_MAP = {
    "nifty200":   "nifty200",
    "IT":         "sector_it_close",
    "Financials": "sector_bank_close",
    "Auto":       "sector_auto_close",
    "FMCG":       "sector_fmcg_close",
    "Pharma":     "sector_pharma_close",
    "Metal":      "sector_metal_close",
    "Realty":     "sector_realty_close",
    "Energy":     "sector_energy_close",
    "Infra":      "sector_infra_close",
    "PSUBank":    "sector_psubank_close",
    "Finance":    "sector_fin_close",
    "Media":      "sector_media_close",
}


class CycleLabeller:

    def __init__(self):
        self.con = get_duck()

    # ── Public API ─────────────────────────────────────────────────────────

    def label_market(self, from_date: str = "2009-01-01") -> pd.DataFrame:
        """Generate market-level (Nifty200) cycle labels."""
        prices = self._load_market_prices(from_date)
        if prices.empty:
            logger.warning("No market price data found")
            return pd.DataFrame()
        return self._apply_labels(prices, MARKET_PARAMS, "market")

    def label_sector(self, sector: str, from_date: str = "2009-01-01") -> pd.DataFrame:
        """Generate cycle labels for one sector."""
        col = SCOPE_SYMBOL_MAP.get(sector)
        if col is None:
            logger.warning(f"Unknown sector: {sector}")
            return pd.DataFrame()

        prices = self._load_sector_prices(col, from_date)
        if prices.empty:
            return pd.DataFrame()
        return self._apply_labels(prices, SECTOR_PARAMS, "sector", scope_id=sector)

    def label_stock(self, symbol: str, from_date: str = "2009-01-01") -> pd.DataFrame:
        """Generate cycle labels for one stock."""
        prices = self._load_stock_prices(symbol, from_date)
        if prices.empty:
            logger.warning(f"No price data for {symbol}")
            return pd.DataFrame()
        return self._apply_labels(prices, STOCK_PARAMS, "stock", scope_id=symbol)

    def label_all_and_store(self, from_date: str = "2009-01-01") -> dict:
        """
        Generate and store cycle labels for market + all sectors + all stocks.
        Writes to market_cycles table in DuckDB.
        Returns stats dict.
        """
        from alphalens.core.ingestion.universe import get_all_symbols, get_sectors

        stats = {"market": 0, "sectors": 0, "stocks": 0, "errors": []}

        # Market level
        logger.info("Labelling market cycle (Nifty200)...")
        df = self.label_market(from_date)
        if not df.empty:
            self._store_labels(df)
            stats["market"] = len(df)

        # Sector level
        sectors = list(SCOPE_SYMBOL_MAP.keys())
        sectors = [s for s in sectors if s != "nifty200"]
        for sector in sectors:
            try:
                df = self.label_sector(sector, from_date)
                if not df.empty:
                    self._store_labels(df)
                    stats["sectors"] += len(df)
            except Exception as e:
                logger.warning(f"Sector labelling failed for {sector}: {e}")
                stats["errors"].append(f"sector:{sector}")

        # Stock level
        symbols = get_all_symbols()
        logger.info(f"Labelling {len(symbols)} stocks...")
        for symbol in symbols:
            try:
                df = self.label_stock(symbol, from_date)
                if not df.empty:
                    self._store_labels(df)
                    stats["stocks"] += 1
            except Exception as e:
                logger.debug(f"Stock labelling failed for {symbol}: {e}")
                stats["errors"].append(f"stock:{symbol}")

        logger.info(
            f"Labelling complete: market={stats['market']} rows, "
            f"sectors={stats['sectors']} rows, stocks={stats['stocks']}, "
            f"errors={len(stats['errors'])}"
        )
        return stats

    # ── Core Labelling Algorithm ───────────────────────────────────────────

    def _apply_labels(self, prices: pd.Series, params: dict,
                      scope: str, scope_id: Optional[str] = None) -> pd.DataFrame:
        """
        Apply Bull/Bear/Neutral labels to a price series.
        Returns DataFrame with columns: date, scope, scope_id, cycle, cycle_int
        """
        df = pd.DataFrame({"close": prices})
        df = df.sort_index()

        # ── Compute features for labelling ──────────────────────────────
        dma    = params["dma_period"]
        rsi_p  = params["rsi_period"]

        df["dma"]       = df["close"].rolling(dma).mean()
        df["above_dma"] = (df["close"] > df["dma"]).astype(int)

        # RSI
        delta   = df["close"].diff()
        gain    = delta.clip(lower=0).rolling(rsi_p).mean()
        loss    = (-delta.clip(upper=0)).rolling(rsi_p).mean()
        rs      = gain / loss.replace(0, np.nan)
        df["rsi"] = 100 - (100 / (1 + rs))

        # Rolling max and drawdown from peak
        df["rolling_max"] = df["close"].cummax()
        df["drawdown"]    = (df["close"] - df["rolling_max"]) / df["rolling_max"]

        # 20-day forward return (for validation)
        df["fwd_20d"] = df["close"].shift(-20) / df["close"] - 1

        # ── Bear labelling ──────────────────────────────────────────────
        # Bear: deep drawdown + below DMA + weak RSI
        bear_mask = (
            (df["drawdown"]    <= params["bear_drawdown"]) &
            (df["above_dma"]   == 0) &
            (df["rsi"].fillna(50) < params["bear_rsi_max"])
        )

        # ── Bull labelling ──────────────────────────────────────────────
        # Bull: near highs + above DMA + strong RSI
        bull_mask = (
            (df["drawdown"]    >= -params["neutral_band"]) &
            (df["above_dma"]   == 1) &
            (df["rsi"].fillna(50) > params["bull_rsi_min"])
        )

        df["cycle_int"] = 0  # 0 = neutral
        df.loc[bear_mask, "cycle_int"] = -1
        df.loc[bull_mask, "cycle_int"] =  1

        # ── Duration filter ─────────────────────────────────────────────
        df["cycle_int"] = self._apply_duration_filter(
            df["cycle_int"], params["min_duration_days"]
        )

        # ── Map to string labels ────────────────────────────────────────
        df["cycle"] = df["cycle_int"].map({1: "bull", -1: "bear", 0: "neutral"})
        df["scope"]    = scope
        df["scope_id"] = scope_id

        result = df[["scope", "scope_id", "cycle", "cycle_int", "drawdown", "rsi", "above_dma", "fwd_20d"]].copy()
        result.index.name = "date"
        result = result.reset_index()
        result["date"] = pd.to_datetime(result["date"]).dt.date
        result = result.dropna(subset=["cycle"])

        return result

    @staticmethod
    def _apply_duration_filter(labels: pd.Series, min_duration: int) -> pd.Series:
        """
        Collapse any regime run shorter than min_duration days to neutral (0).
        """
        result = labels.copy()
        n = len(labels)
        i = 0

        while i < n:
            val = labels.iloc[i]
            if val == 0:
                i += 1
                continue
            # Find run end
            j = i
            while j < n and labels.iloc[j] == val:
                j += 1
            if (j - i) < min_duration:
                result.iloc[i:j] = 0
            i = j

        return result

    # ── Data Loading ───────────────────────────────────────────────────────

    def _load_market_prices(self, from_date: str) -> pd.Series:
        df = self.con.execute("""
            SELECT date, nifty200_close AS close
            FROM market_context
            WHERE date >= ? AND nifty200_close IS NOT NULL
            ORDER BY date ASC
        """, [from_date]).fetchdf()

        if df.empty:
            return pd.Series(dtype=float)

        df["date"] = pd.to_datetime(df["date"])
        return df.set_index("date")["close"]

    def _load_sector_prices(self, col: str, from_date: str) -> pd.Series:
        df = self.con.execute(f"""
            SELECT date, {col} AS close
            FROM market_context
            WHERE date >= ? AND {col} IS NOT NULL
            ORDER BY date ASC
        """, [from_date]).fetchdf()

        if df.empty:
            return pd.Series(dtype=float)

        df["date"] = pd.to_datetime(df["date"])
        return df.set_index("date")["close"]

    def _load_stock_prices(self, symbol: str, from_date: str) -> pd.Series:
        df = self.con.execute("""
            SELECT date, close
            FROM daily_prices
            WHERE symbol = ? AND date >= ?
            ORDER BY date ASC
        """, [symbol, from_date]).fetchdf()

        if df.empty:
            return pd.Series(dtype=float)

        df["date"] = pd.to_datetime(df["date"])
        return df.set_index("date")["close"]

    # ── Storage ────────────────────────────────────────────────────────────

    def _store_labels(self, df: pd.DataFrame):
        """Upsert cycle labels into market_cycles DuckDB table."""
        if df.empty:
            return

        records = [
            (
                row["date"],
                row["scope"],
                row.get("scope_id"),
                row["cycle"],
                row.get("cycle_int", 0),
                "v1_labeller",
                None
            )
            for _, row in df.iterrows()
        ]

        self.con.executemany("""
            INSERT OR REPLACE INTO market_cycles
            (date, scope, scope_id, cycle, confidence, model_version, features_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, records)
