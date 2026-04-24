"""
alphalens/core/cycle/classifier.py

Trains and runs Random Forest cycle classifiers at 3 levels:
  - Market level  (Nifty200 + 24-indicator feature set)
  - Sector level  (one model shared across 12 sectors)
  - Stock level   (one model shared across all 200 stocks)

Feature set: 24 indicators across 5 families (trend, volatility, global
macro, institutional flows, momentum) — mirrors the blueprint.

Models are saved as joblib files in alphalens/models/.

Usage:
    clf = CycleClassifier()
    clf.train_all()                           # Train all 3 models
    result = clf.classify_market_today()      # -> {"cycle": "bull", "confidence": 0.82}
    result = clf.classify_sector_today("IT")  # -> {"cycle": "neutral", "confidence": 0.71}
    result = clf.classify_stock_today("RELIANCE") # -> {"cycle": "bull", "confidence": 0.77}
    clf.classify_all_and_store()              # Classify everything → market_cycles table
"""

import json
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import joblib
import numpy as np
import pandas as pd
from loguru import logger
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import classification_report, f1_score
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder

from alphalens.core.database import get_duck
from config.settings import settings

MODELS_DIR = Path(settings.models_dir)

# ── Feature Definitions ────────────────────────────────────────────────────

MARKET_FEATURES = [
    # Trend
    "nifty200_above_50dma", "nifty200_above_200dma",
    "nifty200_dma50_pct", "nifty200_dma200_pct",
    "nifty200_1d_ret", "nifty200_5d_ret", "nifty200_20d_ret",
    # Volatility / Fear
    "india_vix", "india_vix_1d_chg", "india_vix_pct252",
    # Global Macro
    "dxy", "brent_crude", "nasdaq_5d_ret", "dow_5d_ret",
    "us_10yr_yield", "usd_inr",
    # Institutional Flows
    "fii_10d_sum", "dii_net_buy_sell",
    # Breadth & Sentiment
    "advance_decline_ratio", "pcr_nifty",
    "pct_above_50dma", "pct_above_200dma",
    # Sector relative
    "sector_it_5d", "sector_bank_5d",
]

SECTOR_FEATURES = [
    "index_close", "ret_5d", "ret_20d", "ret_60d",
    "above_50dma", "above_200dma", "rs_vs_nifty200_20d",
    "volume_ratio", "rsi_14", "macd_hist",
    "india_vix", "nifty200_20d_ret",
    "market_cycle_enc",  # encoded market-level cycle
]

STOCK_FEATURES = [
    "pct_from_ema200", "pct_from_52w_high", "pct_from_52w_low",
    "above_50dma", "above_200dma", "ema50_vs_ema200",
    "rsi_14", "rsi_9", "adx_14", "macd_hist",
    "volume_ratio", "rs_nifty200",
    "india_vix", "market_cycle_enc", "sector_cycle_enc",
    "hist_vol_21",
]


class CycleClassifier:

    def __init__(self):
        self.con     = get_duck()
        self.models  = {}   # key → {pipeline, label_encoder, feature_cols, meta}
        MODELS_DIR.mkdir(parents=True, exist_ok=True)
        self._load_all_models()

    # ── Training ───────────────────────────────────────────────────────────

    def train_all(self):
        """Train market, sector, and stock cycle classifiers."""
        logger.info("Training cycle classifiers...")
        results = {}
        results["market"] = self.train_market_classifier()
        results["sector"] = self.train_sector_classifier()
        results["stock"]  = self.train_stock_classifier()
        logger.info(f"All cycle classifiers trained: {results}")
        return results

    def train_market_classifier(self) -> dict:
        """Train Random Forest on market-level features."""
        logger.info("Building market cycle training dataset...")
        X, y, le = self._build_market_dataset()
        if X is None:
            return {"error": "no_data"}

        result = self._train_rf("market", X, y, le, MARKET_FEATURES)
        self._save_model("cycle_market", result)
        return {"mean_f1": result["mean_f1"], "n_samples": len(X)}

    def train_sector_classifier(self) -> dict:
        """Train shared Random Forest across all sector data."""
        logger.info("Building sector cycle training dataset...")
        X, y, le = self._build_sector_dataset()
        if X is None:
            return {"error": "no_data"}

        result = self._train_rf("sector", X, y, le, SECTOR_FEATURES)
        self._save_model("cycle_sector", result)
        return {"mean_f1": result["mean_f1"], "n_samples": len(X)}

    def train_stock_classifier(self) -> dict:
        """Train shared Random Forest across all stock data (sampled)."""
        logger.info("Building stock cycle training dataset...")
        X, y, le = self._build_stock_dataset()
        if X is None:
            return {"error": "no_data"}

        result = self._train_rf("stock", X, y, le, STOCK_FEATURES)
        self._save_model("cycle_stock", result)
        return {"mean_f1": result["mean_f1"], "n_samples": len(X)}

    # ── Live Classification ────────────────────────────────────────────────

    def classify_market_today(self) -> dict:
        """Classify current market cycle from latest context data."""
        features = self._get_latest_market_features()
        if features is None:
            return {"cycle": "neutral", "confidence": 0.0, "error": "no_features"}
        return self._predict("cycle_market", features, MARKET_FEATURES)

    def classify_sector_today(self, sector: str) -> dict:
        """Classify current cycle for one sector."""
        features = self._get_latest_sector_features(sector)
        if features is None:
            return {"cycle": "neutral", "confidence": 0.0, "error": "no_features"}
        return self._predict("cycle_sector", features, SECTOR_FEATURES)

    def classify_stock_today(self, symbol: str,
                              market_cycle: str = "neutral",
                              sector_cycle: str = "neutral") -> dict:
        """Classify current cycle for one stock."""
        features = self._get_latest_stock_features(symbol, market_cycle, sector_cycle)
        if features is None:
            return {"cycle": "neutral", "confidence": 0.0, "error": "no_features"}
        return self._predict("cycle_stock", features, STOCK_FEATURES)

    def classify_all_and_store(self) -> dict:
        """
        Full classification run: market → sectors → stocks.
        Stores today's labels in market_cycles table.
        Called by EOD scheduler.
        """
        from alphalens.core.ingestion.universe import get_all_symbols, get_sectors

        today  = date.today()
        stats  = {"market": None, "sectors": {}, "stocks": {}}
        records = []

        # ── Market ─────────────────────────────────────────────────────
        m_result = self.classify_market_today()
        stats["market"] = m_result
        records.append((today, "market", None, m_result["cycle"],
                         m_result["confidence"], "v1_rf", None))
        logger.info(f"Market cycle: {m_result['cycle']} (conf={m_result['confidence']:.2f})")

        # ── Sectors ────────────────────────────────────────────────────
        sector_cycles = {}
        for sector in get_sectors():
            s_result = self.classify_sector_today(sector)
            stats["sectors"][sector] = s_result
            sector_cycles[sector] = s_result["cycle"]
            records.append((today, "sector", sector, s_result["cycle"],
                             s_result["confidence"], "v1_rf", None))

        # ── Stocks ─────────────────────────────────────────────────────
        symbols = get_all_symbols()
        logger.info(f"Classifying {len(symbols)} stocks...")

        # Load sector for each symbol
        sector_map = self._load_symbol_sectors()

        for symbol in symbols:
            try:
                sector       = sector_map.get(symbol, "Unknown")
                sec_cycle    = sector_cycles.get(sector, "neutral")
                st_result    = self.classify_stock_today(
                    symbol,
                    market_cycle=m_result["cycle"],
                    sector_cycle=sec_cycle
                )
                stats["stocks"][symbol] = st_result
                records.append((today, "stock", symbol, st_result["cycle"],
                                 st_result["confidence"], "v1_rf", None))
            except Exception as e:
                logger.debug(f"Stock classification failed for {symbol}: {e}")

        # Batch upsert
        self.con.executemany("""
            INSERT OR REPLACE INTO market_cycles
            (date, scope, scope_id, cycle, confidence, model_version, features_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, records)

        logger.info(
            f"Classification stored: market={stats['market']['cycle']}, "
            f"sectors={len(stats['sectors'])}, stocks={len(stats['stocks'])}"
        )
        return stats

    def get_current_cycle(self, scope: str = "market",
                           scope_id: Optional[str] = None) -> dict:
        """Get the most recent cycle classification from DB."""
        result = self.con.execute("""
            SELECT cycle, confidence, date
            FROM market_cycles
            WHERE scope = ? AND (scope_id = ? OR (scope_id IS NULL AND ? IS NULL))
            ORDER BY date DESC LIMIT 1
        """, [scope, scope_id, scope_id]).fetchone()

        if result is None:
            return {"cycle": "neutral", "confidence": 0.0, "date": None}
        return {"cycle": result[0], "confidence": result[1] or 0.0, "date": result[2]}

    def get_cycle_history(self, scope: str, scope_id: Optional[str],
                           days: int = 365) -> pd.DataFrame:
        """Return cycle history for charting."""
        from_date = date.today() - pd.Timedelta(days=days)
        df = self.con.execute("""
            SELECT date, cycle, confidence
            FROM market_cycles
            WHERE scope = ? AND scope_id = ? AND date >= ?
            ORDER BY date ASC
        """, [scope, scope_id, from_date]).fetchdf()
        return df

    # ── Training Helpers ──────────────────────────────────────────────────

    def _train_rf(self, name: str, X: pd.DataFrame, y: np.ndarray,
                  le: LabelEncoder, feature_cols: list) -> dict:
        """Walk-forward Random Forest training with 5-fold time-series CV."""
        logger.info(f"Training RF [{name}]: {len(X)} samples, {len(feature_cols)} features")
        logger.info(f"Class distribution: {dict(zip(*np.unique(y, return_counts=True)))}")

        tscv   = TimeSeriesSplit(n_splits=5)
        cv_f1s = []

        pipeline = Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("model",   RandomForestClassifier(
                n_estimators  = 300,
                max_depth     = 8,
                min_samples_leaf = 10,
                class_weight  = "balanced",
                n_jobs        = -1,
                random_state  = 42
            ))
        ])

        for fold, (tr_idx, te_idx) in enumerate(tscv.split(X)):
            pipeline.fit(X.iloc[tr_idx], y[tr_idx])
            preds   = pipeline.predict(X.iloc[te_idx])
            fold_f1 = f1_score(y[te_idx], preds, average="weighted")
            cv_f1s.append(fold_f1)
            logger.debug(f"  Fold {fold+1}: F1={fold_f1:.4f}")

        # Final fit on all data
        pipeline.fit(X, y)
        mean_f1 = float(np.mean(cv_f1s))

        logger.info(f"  Mean CV F1: {mean_f1:.4f} ± {np.std(cv_f1s):.4f}")
        if mean_f1 < 0.55:
            logger.warning(f"  Low F1 for {name} cycle classifier — consider more training data")

        # Feature importance
        rf = pipeline.named_steps["model"]
        importances = dict(zip(feature_cols, rf.feature_importances_))
        top = sorted(importances.items(), key=lambda x: x[1], reverse=True)[:8]
        logger.info(f"  Top features: {[(k, round(v,4)) for k,v in top]}")

        return {
            "pipeline":     pipeline,
            "label_encoder": le,
            "feature_cols": feature_cols,
            "classes":      list(le.classes_),
            "cv_f1_scores": [float(f) for f in cv_f1s],
            "mean_f1":      mean_f1,
            "trained_at":   datetime.now().isoformat(),
            "feature_importances": {k: round(v, 6) for k, v in importances.items()},
        }

    def _predict(self, model_key: str, features: dict, feature_cols: list) -> dict:
        """Run inference using a loaded model."""
        if model_key not in self.models:
            return {"cycle": "neutral", "confidence": 0.0, "error": "model_not_loaded"}

        bundle = self.models[model_key]
        pipeline = bundle["pipeline"]
        le       = bundle["label_encoder"]

        X = np.array([[features.get(c, 0.0) or 0.0 for c in feature_cols]])
        proba  = pipeline.predict_proba(X)[0]
        pred   = pipeline.predict(X)[0]
        label  = le.inverse_transform([pred])[0]
        conf   = float(max(proba))
        probs  = dict(zip(le.classes_, [float(p) for p in proba]))

        return {"cycle": label, "confidence": conf, "probabilities": probs}

    # ── Dataset Builders ──────────────────────────────────────────────────

    def _build_market_dataset(self):
        """Build market-level training dataset from DuckDB."""
        # Join market_context with market_cycles labels
        df = self.con.execute("""
            SELECT
                mc.*,
                ml.cycle,
                -- Computed features
                CASE WHEN mc.nifty200_close > mc.nifty200_close * 1.0 THEN 1 ELSE 0 END AS above_check,
                (mc.sector_it_close / LAG(mc.sector_it_close, 5) OVER (ORDER BY mc.date) - 1) * 100 AS sector_it_5d,
                (mc.sector_bank_close / LAG(mc.sector_bank_close, 5) OVER (ORDER BY mc.date) - 1) * 100 AS sector_bank_5d
            FROM market_context mc
            JOIN market_cycles ml
                ON mc.date = ml.date AND ml.scope = 'market' AND ml.scope_id IS NULL
            WHERE mc.nifty200_close IS NOT NULL
            ORDER BY mc.date ASC
        """).fetchdf()

        if df.empty or "cycle" not in df.columns:
            logger.warning("No market cycle labels found — run CycleLabeller first")
            return None, None, None

        # Add computed features not in market_context
        df["nifty200_dma50_pct"]  = df.get("nifty200_dma50_pct",  pd.Series(0.0, index=df.index))
        df["nifty200_dma200_pct"] = df.get("nifty200_dma200_pct", pd.Series(0.0, index=df.index))

        # Compute above_50dma, above_200dma from close + rolling mean
        if "nifty200_close" in df.columns:
            df["nifty200_above_50dma"]  = (df["nifty200_close"] > df["nifty200_close"].rolling(50).mean()).astype(int)
            df["nifty200_above_200dma"] = (df["nifty200_close"] > df["nifty200_close"].rolling(200).mean()).astype(int)
            df["nifty200_dma50_pct"]    = (df["nifty200_close"] / df["nifty200_close"].rolling(50).mean() - 1) * 100
            df["nifty200_dma200_pct"]   = (df["nifty200_close"] / df["nifty200_close"].rolling(200).mean() - 1) * 100

        available_features = [f for f in MARKET_FEATURES if f in df.columns]
        X = df[available_features].fillna(0)

        le = LabelEncoder()
        y  = le.fit_transform(df["cycle"])

        return X, y, le

    def _build_sector_dataset(self):
        """Build sector-level training dataset (all sectors combined)."""
        sector_cols = {
            "IT":         "sector_it_close",
            "Financials": "sector_bank_close",
            "Auto":       "sector_auto_close",
            "FMCG":       "sector_fmcg_close",
            "Pharma":     "sector_pharma_close",
            "Metal":      "sector_metal_close",
        }

        all_dfs = []
        for sector, col in sector_cols.items():
            df = self.con.execute(f"""
                SELECT
                    mc.date,
                    mc.{col} AS index_close,
                    ml.cycle,
                    mc.india_vix,
                    mc.nifty200_20d_ret
                FROM market_context mc
                JOIN market_cycles ml
                    ON mc.date = ml.date AND ml.scope = 'sector' AND ml.scope_id = ?
                WHERE mc.{col} IS NOT NULL
                ORDER BY mc.date ASC
            """, [sector]).fetchdf()

            if df.empty:
                continue

            # Compute sector-specific features
            df["ret_5d"]  = df["index_close"].pct_change(5)  * 100
            df["ret_20d"] = df["index_close"].pct_change(20) * 100
            df["ret_60d"] = df["index_close"].pct_change(60) * 100

            sma50  = df["index_close"].rolling(50).mean()
            sma200 = df["index_close"].rolling(200).mean()
            df["above_50dma"]       = (df["index_close"] > sma50).astype(int)
            df["above_200dma"]      = (df["index_close"] > sma200).astype(int)
            df["rs_vs_nifty200_20d"] = 0.0  # simplified
            df["volume_ratio"]       = 1.0

            # RSI for sector
            delta = df["index_close"].diff()
            gain  = delta.clip(lower=0).rolling(14).mean()
            loss  = (-delta.clip(upper=0)).rolling(14).mean()
            rs    = gain / loss.replace(0, np.nan)
            df["rsi_14"] = 100 - (100 / (1 + rs))
            df["macd_hist"] = 0.0
            df["market_cycle_enc"] = 0

            all_dfs.append(df)

        if not all_dfs:
            return None, None, None

        combined = pd.concat(all_dfs, ignore_index=True).sort_values("date")
        available = [f for f in SECTOR_FEATURES if f in combined.columns]
        X  = combined[available].fillna(0)
        le = LabelEncoder()
        y  = le.fit_transform(combined["cycle"])
        return X, y, le

    def _build_stock_dataset(self):
        """Build stock-level training dataset (sample of 30 stocks for speed)."""
        from alphalens.core.ingestion.universe import get_all_symbols
        import random
        random.seed(42)

        all_symbols = get_all_symbols()
        # Sample 30 diverse stocks for training — enough signal without OOM
        sample = random.sample(all_symbols, min(30, len(all_symbols)))

        all_dfs = []
        for symbol in sample:
            df = self.con.execute("""
                SELECT
                    ti.date, ti.symbol,
                    ti.rsi_14, ti.rsi_9, ti.adx_14, ti.macd_hist,
                    ti.volume_ratio, ti.rs_nifty200,
                    ti.pct_from_ema200, ti.pct_from_52w_high, ti.pct_from_52w_low,
                    ti.hist_vol_21,
                    (CASE WHEN dp.close > ti.sma_50  THEN 1 ELSE 0 END) AS above_50dma,
                    (CASE WHEN dp.close > ti.sma_200 THEN 1 ELSE 0 END) AS above_200dma,
                    ((ti.ema_50 - ti.ema_200) / NULLIF(ti.ema_200,0) * 100) AS ema50_vs_ema200,
                    mc_ind.india_vix,
                    ml.cycle
                FROM technical_indicators ti
                JOIN daily_prices dp ON ti.date = dp.date AND ti.symbol = dp.symbol
                JOIN market_context mc_ind ON ti.date = mc_ind.date
                JOIN market_cycles ml
                    ON ti.date = ml.date AND ml.scope = 'stock' AND ml.scope_id = ti.symbol
                WHERE ti.symbol = ? AND ti.rsi_14 IS NOT NULL
                ORDER BY ti.date ASC
            """, [symbol]).fetchdf()

            if df.empty:
                continue

            df["market_cycle_enc"] = 0  # simplified — enriched during live inference
            df["sector_cycle_enc"] = 0
            all_dfs.append(df)

        if not all_dfs:
            return None, None, None

        combined = pd.concat(all_dfs, ignore_index=True)
        available = [f for f in STOCK_FEATURES if f in combined.columns]
        X  = combined[available].fillna(0)
        le = LabelEncoder()
        y  = le.fit_transform(combined["cycle"])
        return X, y, le

    # ── Feature Extraction (live) ─────────────────────────────────────────

    def _get_latest_market_features(self) -> Optional[dict]:
        row = self.con.execute("""
            SELECT * FROM market_context
            ORDER BY date DESC LIMIT 1
        """).fetchdf()

        if row.empty:
            return None

        r = row.iloc[0].to_dict()

        # Compute derived fields
        if r.get("nifty200_close"):
            # Approximate DMA position from recent context
            dma_df = self.con.execute("""
                SELECT nifty200_close FROM market_context
                ORDER BY date DESC LIMIT 250
            """).fetchdf()
            closes = dma_df["nifty200_close"].dropna()
            r["nifty200_above_50dma"]  = int(r["nifty200_close"] > closes.iloc[:50].mean()) if len(closes) >= 50 else 0
            r["nifty200_above_200dma"] = int(r["nifty200_close"] > closes.mean())           if len(closes) >= 200 else 0
            r["nifty200_dma50_pct"]    = (r["nifty200_close"] / closes.iloc[:50].mean()  - 1) * 100 if len(closes) >= 50 else 0
            r["nifty200_dma200_pct"]   = (r["nifty200_close"] / closes.mean() - 1) * 100             if len(closes) >= 200 else 0

        # Sector 5-day returns
        for sector_col, feature_key in [
            ("sector_it_close",   "sector_it_5d"),
            ("sector_bank_close", "sector_bank_5d"),
        ]:
            hist = self.con.execute(f"""
                SELECT {sector_col} FROM market_context
                ORDER BY date DESC LIMIT 6
            """).fetchdf()
            vals = hist[sector_col].dropna()
            r[feature_key] = (vals.iloc[0] / vals.iloc[-1] - 1) * 100 if len(vals) >= 2 else 0.0

        return r

    def _get_latest_sector_features(self, sector: str) -> Optional[dict]:
        from alphalens.core.cycle.labeller import SCOPE_SYMBOL_MAP
        col = SCOPE_SYMBOL_MAP.get(sector)
        if not col:
            return None

        hist = self.con.execute(f"""
            SELECT date, {col} AS close, india_vix, nifty200_20d_ret
            FROM market_context
            ORDER BY date DESC LIMIT 70
        """).fetchdf()

        if hist.empty or hist["close"].dropna().empty:
            return None

        hist = hist.sort_values("date")
        closes = hist["close"].dropna()
        latest_close = closes.iloc[-1]

        features = {
            "index_close":    float(latest_close),
            "ret_5d":         float((closes.iloc[-1] / closes.iloc[-6] - 1) * 100) if len(closes) >= 6 else 0.0,
            "ret_20d":        float((closes.iloc[-1] / closes.iloc[-21] - 1) * 100) if len(closes) >= 21 else 0.0,
            "ret_60d":        float((closes.iloc[-1] / closes.iloc[-61] - 1) * 100) if len(closes) >= 61 else 0.0,
            "above_50dma":    int(latest_close > closes.iloc[-50:].mean()) if len(closes) >= 50 else 0,
            "above_200dma":   0,
            "rs_vs_nifty200_20d": 0.0,
            "volume_ratio":   1.0,
            "india_vix":      float(hist["india_vix"].dropna().iloc[-1]) if not hist["india_vix"].dropna().empty else 15.0,
            "nifty200_20d_ret": float(hist["nifty200_20d_ret"].dropna().iloc[-1]) if not hist["nifty200_20d_ret"].dropna().empty else 0.0,
            "market_cycle_enc": 0,
        }

        # RSI
        delta = closes.diff()
        gain  = delta.clip(lower=0).rolling(14).mean()
        loss  = (-delta.clip(upper=0)).rolling(14).mean()
        rs    = gain / loss.replace(0, np.nan)
        rsi   = (100 - (100 / (1 + rs))).iloc[-1]
        features["rsi_14"]   = float(rsi) if not np.isnan(rsi) else 50.0
        features["macd_hist"] = 0.0

        return features

    def _get_latest_stock_features(self, symbol: str,
                                     market_cycle: str = "neutral",
                                     sector_cycle: str = "neutral") -> Optional[dict]:
        row = self.con.execute("""
            SELECT ti.*, mc.india_vix
            FROM technical_indicators ti
            LEFT JOIN market_context mc ON ti.date = mc.date
            WHERE ti.symbol = ?
            ORDER BY ti.date DESC LIMIT 1
        """, [symbol]).fetchdf()

        if row.empty:
            return None

        r = row.iloc[0].to_dict()

        # Encode cycle labels
        cycle_enc = {"bull": 1, "neutral": 0, "bear": -1}
        r["market_cycle_enc"] = cycle_enc.get(market_cycle, 0)
        r["sector_cycle_enc"] = cycle_enc.get(sector_cycle, 0)

        # above_50dma / above_200dma from price vs SMA
        price_row = self.con.execute("""
            SELECT close FROM daily_prices WHERE symbol = ? ORDER BY date DESC LIMIT 1
        """, [symbol]).fetchone()

        if price_row:
            close = price_row[0]
            r["above_50dma"]  = int(r.get("sma_50",  0) and close > r.get("sma_50",  0))
            r["above_200dma"] = int(r.get("sma_200", 0) and close > r.get("sma_200", 0))
            r["ema50_vs_ema200"] = (
                (r.get("ema_50", 0) - r.get("ema_200", 0)) / r.get("ema_200", 1) * 100
                if r.get("ema_200") else 0.0
            )

        return r

    # ── Model Persistence ─────────────────────────────────────────────────

    def _save_model(self, key: str, bundle: dict):
        path = MODELS_DIR / f"{key}_v1.pkl"
        meta_path = MODELS_DIR / f"{key}_v1_meta.json"

        joblib.dump({
            "pipeline":      bundle["pipeline"],
            "label_encoder": bundle["label_encoder"],
            "feature_cols":  bundle["feature_cols"],
            "classes":       bundle["classes"],
        }, path)

        meta = {k: v for k, v in bundle.items() if k not in ("pipeline", "label_encoder")}
        import json
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2, default=str)

        self.models[key] = bundle
        logger.info(f"Model saved: {path} (F1={bundle['mean_f1']:.4f})")

    def _load_all_models(self):
        """Load all saved cycle models from disk on startup."""
        for key in ["cycle_market", "cycle_sector", "cycle_stock"]:
            path = MODELS_DIR / f"{key}_v1.pkl"
            if path.exists():
                try:
                    bundle = joblib.load(path)
                    self.models[key] = bundle
                    logger.debug(f"Loaded cycle model: {key}")
                except Exception as e:
                    logger.warning(f"Could not load model {key}: {e}")

    def _load_symbol_sectors(self) -> dict:
        """Load symbol → sector mapping from DuckDB."""
        rows = self.con.execute(
            "SELECT symbol, sector FROM nifty200_stocks"
        ).fetchall()
        return {r[0]: r[1] for r in rows}
