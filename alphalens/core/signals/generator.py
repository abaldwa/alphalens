"""
alphalens/core/signals/generator.py

ML ensemble signal generator.

For each active stock and timeframe:
  1. Build feature vector from DuckDB (indicators + fundamentals + cycle)
  2. Run ML model inference (LightGBM / XGBoost / LSTM / RF)
  3. Apply cycle-conditioned confidence threshold
  4. Calculate entry price, target (ATR-based), stop-loss (ATR-based)
  5. Validate R:R >= 1.5
  6. Check strategy rule confirmation
  7. Write to watchlist (SQLite) and signals_log
  8. If portfolio slot available → include in portfolio signals

Usage:
    gen = SignalGenerator()
    gen.generate_all()                   # All stocks, all timeframes
    gen.generate_timeframe("swing")      # One timeframe
    gen.generate_stock("RELIANCE")       # One stock, all timeframes
"""

import json
from datetime import date, datetime
from typing import Optional

import numpy as np
import pandas as pd
from loguru import logger

from alphalens.core.database import get_duck, get_sqlite, get_config, Watchlist, SignalLog
from alphalens.core.cycle.context import get_cycle_context
from alphalens.core.portfolio.manager import PortfolioManager
from alphalens.core.strategy.backtester import Backtester
from alphalens.core.strategy.library import get_all_strategies
from alphalens.core.ingestion.universe import get_all_symbols
from config.settings import settings


class SignalGenerator:

    def __init__(self):
        self.con        = get_duck()
        self.pm         = PortfolioManager()
        self.backtester = Backtester()
        self._models    = {}   # lazy-loaded ML models

    # ── Public API ─────────────────────────────────────────────────────────

    def generate_all(self) -> dict:
        """Generate signals for all symbols and all timeframes."""
        symbols    = get_all_symbols()
        strategies = get_all_strategies(active_only=True)
        ctx        = get_cycle_context()

        logger.info(
            f"Signal generation: {len(symbols)} symbols, "
            f"{len(strategies)} strategies, market={ctx.market_cycle}"
        )

        stats = {tf: {"signals": 0, "watchlist": 0} for tf in ("intraday", "swing", "medium", "long_term")}

        for symbol in symbols:
            try:
                result = self._generate_for_symbol(symbol, strategies, ctx)
                for tf, s in result.items():
                    stats[tf]["signals"]   += s.get("signals", 0)
                    stats[tf]["watchlist"] += s.get("watchlist", 0)
            except Exception as e:
                logger.debug(f"Signal gen failed for {symbol}: {e}")

        total_signals = sum(v["signals"] for v in stats.values())
        logger.info(f"Signal generation complete: {total_signals} signals generated")
        return stats

    def generate_timeframe(self, timeframe: str) -> dict:
        """Generate signals for all stocks for one timeframe."""
        symbols    = get_all_symbols()
        strategies = [
            s for s in get_all_strategies(active_only=True)
            if timeframe in (s.get("timeframes") or [])
        ]
        ctx = get_cycle_context()

        count = 0
        for symbol in symbols:
            try:
                sigs = self._generate_for_symbol_timeframe(symbol, timeframe, strategies, ctx)
                count += len(sigs)
            except Exception:
                pass
        logger.info(f"Timeframe {timeframe}: {count} signals generated")
        return {"timeframe": timeframe, "signals": count}

    def generate_stock(self, symbol: str) -> dict:
        """Generate signals for one stock across all timeframes."""
        strategies = get_all_strategies(active_only=True)
        ctx        = get_cycle_context()
        return self._generate_for_symbol(symbol, strategies, ctx)

    # ── Core Logic ────────────────────────────────────────────────────────

    def _generate_for_symbol(self, symbol: str, strategies: list,
                               ctx) -> dict:
        """Generate signals for one symbol across all timeframes."""
        inds  = self._load_latest_indicators(symbol)
        if inds is None:
            return {}

        result = {}
        for timeframe in ("intraday", "swing", "medium", "long_term"):
            tf_strategies = [s for s in strategies if timeframe in (s.get("timeframes") or [])]
            sigs = self._generate_for_symbol_timeframe(symbol, timeframe, tf_strategies, ctx, inds)
            result[timeframe] = {"signals": len(sigs), "watchlist": len(sigs)}
        return result

    def _generate_for_symbol_timeframe(self, symbol: str, timeframe: str,
                                         strategies: list, ctx,
                                         inds: dict = None) -> list:
        """Generate signals for one symbol + timeframe combination."""
        if inds is None:
            inds = self._load_latest_indicators(symbol)
        if not inds:
            return []

        prices = self._load_recent_prices(symbol, 60)
        if prices is None or len(prices) < 10:
            return []

        row       = self._build_signal_row(inds, prices, symbol, ctx)
        prev_rows = prices

        signals_generated = []

        for strategy in strategies:
            try:
                signal = self._evaluate_strategy_signal(
                    symbol, timeframe, strategy, row, prev_rows, ctx
                )
                if signal:
                    self._write_signal(signal)
                    signals_generated.append(signal)
            except Exception as e:
                logger.debug(f"Strategy eval failed {strategy.get('strategy_id')}/{symbol}: {e}")

        return signals_generated

    def _evaluate_strategy_signal(self, symbol: str, timeframe: str,
                                    strategy: dict, row: pd.Series,
                                    prev_rows: pd.DataFrame, ctx) -> Optional[dict]:
        """
        Evaluate one strategy for one stock.
        Returns signal dict if conditions met, else None.
        """
        # ── Parse strategy rules ───────────────────────────────────────
        entry_rules = strategy.get("entry_rules", {})
        if isinstance(entry_rules, str):
            entry_rules = json.loads(entry_rules)

        # ── Check entry conditions ─────────────────────────────────────
        if not self.backtester.check_entry_signal(strategy, row, prev_rows):
            return None

        # ── Compute entry / target / SL ────────────────────────────────
        close     = float(row.get("close", 0))
        if close <= 0:
            return None

        stop_loss = self.backtester.compute_stop_loss(strategy, row, close)
        target    = self.backtester.compute_target(strategy, row, close, stop_loss)
        risk      = close - stop_loss

        if risk <= 0:
            return None

        rr = (target - close) / risk

        # ── Enforce minimum R:R ────────────────────────────────────────
        min_rr = get_config("min_risk_reward", 1.5)
        if rr < min_rr:
            return None

        # ── ML confidence score ────────────────────────────────────────
        confidence = self._ml_confidence(symbol, timeframe, row, ctx)

        # ── Apply cycle-conditioned threshold ──────────────────────────
        threshold = self._get_threshold(ctx.market_cycle)
        if confidence < threshold:
            return None

        # ── Build signal ───────────────────────────────────────────────
        stock_cycle   = ctx.get_stock_cycle(symbol).get("cycle", "neutral")
        sector        = self._get_symbol_sector(symbol)
        sector_cycle  = ctx.get_sector_cycle(sector).get("cycle", "neutral")
        pattern_state = self._get_pattern_state(symbol)

        signal = {
            "symbol":        symbol,
            "timeframe":     timeframe,
            "signal_type":   "buy",
            "strategy_id":   strategy.get("strategy_id"),
            "strategy_name": strategy.get("name"),
            "entry_price":   round(close, 2),
            "target_price":  round(target, 2),
            "stop_loss":     round(stop_loss, 2),
            "risk_reward":   round(rr, 2),
            "confidence":    round(confidence, 4),
            "cycle_context": ctx.market_cycle,
            "stock_cycle":   stock_cycle,
            "sector_cycle":  sector_cycle,
            "pattern_state": pattern_state,
            "sector":        sector,
            "reasoning":     self._build_reasoning(strategy, row, ctx),
            "generated_at":  datetime.now(),
        }
        return signal

    # ── ML Confidence ─────────────────────────────────────────────────────

    def _ml_confidence(self, symbol: str, timeframe: str,
                         row: pd.Series, ctx) -> float:
        """
        Get ML model confidence for a BUY signal.
        Falls back to rule-based score if model not loaded.
        """
        try:
            model_key = f"signal_{timeframe}"
            model     = self._load_model(model_key)
            if model is None:
                return self._rule_based_confidence(row, ctx)

            features  = self._build_feature_vector(row, timeframe, ctx)
            pipeline  = model["pipeline"]
            le        = model["label_encoder"]
            proba     = pipeline.predict_proba([features])[0]
            classes   = list(le.classes_)

            buy_idx   = classes.index("buy") if "buy" in classes else 0
            return float(proba[buy_idx])

        except Exception:
            return self._rule_based_confidence(row, ctx)

    def _rule_based_confidence(self, row: pd.Series, ctx) -> float:
        """
        Heuristic confidence score when ML model is not yet trained.
        Based on indicator alignment (used during initial deployment).
        """
        score = 0.5

        # Trend alignment
        rsi = float(row.get("rsi_14", 50) or 50)
        adx = float(row.get("adx_14", 20) or 20)
        st  = int(row.get("supertrend_dir", 0) or 0)
        macd_h = float(row.get("macd_hist", 0) or 0)
        vol_r  = float(row.get("volume_ratio", 1) or 1)

        if 50 < rsi < 70:    score += 0.05
        if adx > 25:         score += 0.05
        if st == -1:         score += 0.05   # ST dir -1 = uptrend in our convention
        if macd_h > 0:       score += 0.05
        if vol_r > 1.2:      score += 0.03

        # Cycle bonus
        if ctx.market_cycle == "bull":    score += 0.07
        elif ctx.market_cycle == "bear":  score -= 0.10

        return round(min(0.95, max(0.30, score)), 4)

    def _build_feature_vector(self, row: pd.Series, timeframe: str, ctx) -> list:
        """Build ordered feature vector for ML model inference."""
        from alphalens.ml.features.pipeline import FeaturePipeline
        fp = FeaturePipeline()
        return fp.build_signal_features(row, timeframe, ctx)

    # ── DB I/O ────────────────────────────────────────────────────────────

    def _write_signal(self, signal: dict):
        """Write signal to watchlist and signals_log tables."""
        with get_sqlite() as session:
            # Deactivate any previous signal for same symbol+timeframe
            session.query(Watchlist).filter(
                Watchlist.symbol    == signal["symbol"],
                Watchlist.timeframe == signal["timeframe"],
                Watchlist.is_active == True,
            ).update({"is_active": False})

            # Add to watchlist
            watch = Watchlist(
                symbol          = signal["symbol"],
                timeframe       = signal["timeframe"],
                strategy_id     = signal["strategy_id"],
                signal_type     = signal["signal_type"],
                suggested_entry = signal["entry_price"],
                target_price    = signal["target_price"],
                stop_loss       = signal["stop_loss"],
                risk_reward     = signal["risk_reward"],
                confidence      = signal["confidence"],
                cycle_context   = signal["cycle_context"],
                sector          = signal["sector"],
                pattern_state   = signal.get("pattern_state"),
                reasoning       = signal.get("reasoning"),
                valid_till      = date.today(),
                is_active       = True,
                created_at      = signal["generated_at"],
                updated_at      = signal["generated_at"],
            )
            session.add(watch)

            # Add to signals_log (permanent audit trail)
            log = SignalLog(
                generated_at    = signal["generated_at"],
                symbol          = signal["symbol"],
                timeframe       = signal["timeframe"],
                signal_type     = signal["signal_type"],
                strategy_id     = signal["strategy_id"],
                entry_price     = signal["entry_price"],
                target_price    = signal["target_price"],
                stop_loss       = signal["stop_loss"],
                risk_reward     = signal["risk_reward"],
                confidence      = signal["confidence"],
                cycle_context   = signal["cycle_context"],
                pattern_state   = signal.get("pattern_state"),
                reasoning       = signal.get("reasoning"),
                is_active       = True,
            )
            session.add(log)

    def _load_latest_indicators(self, symbol: str) -> Optional[dict]:
        row = self.con.execute("""
            SELECT * FROM technical_indicators
            WHERE symbol = ? ORDER BY date DESC LIMIT 1
        """, [symbol]).fetchdf()
        if row.empty:
            return None
        return row.iloc[0].to_dict()

    def _load_recent_prices(self, symbol: str, n: int) -> Optional[pd.DataFrame]:
        df = self.con.execute("""
            SELECT date, open, high, low, close, volume
            FROM daily_prices
            WHERE symbol = ?
            ORDER BY date DESC LIMIT ?
        """, [symbol, n]).fetchdf()
        if df.empty:
            return None
        df["date"] = pd.to_datetime(df["date"]).dt.date
        return df.sort_values("date").reset_index(drop=True)

    def _build_signal_row(self, inds: dict, prices: pd.DataFrame,
                            symbol: str, ctx) -> pd.Series:
        """Merge latest indicators with price data into a single Series."""
        latest_price = prices.iloc[-1].to_dict() if not prices.empty else {}

        row_data = {**inds, **latest_price}
        row_data["prev_close"]     = float(prices.iloc[-2]["close"]) if len(prices) > 1 else row_data.get("close", 0)
        row_data["market_cycle"]   = ctx.market_cycle
        row_data["sector_cycle"]   = ctx.get_sector_cycle(self._get_symbol_sector(symbol)).get("cycle", "neutral")
        row_data["rs_percentile"]  = float(inds.get("rs_nifty200", 50) or 50)
        row_data["gap_pct"]        = (
            (row_data.get("open", 0) - row_data["prev_close"]) / row_data["prev_close"] * 100
            if row_data["prev_close"] else 0
        )
        return pd.Series(row_data)

    # ── Helpers ────────────────────────────────────────────────────────────

    def _get_threshold(self, market_cycle: str) -> float:
        thresholds = settings.signal_thresholds
        return thresholds.get(market_cycle, thresholds["neutral"])

    def _get_symbol_sector(self, symbol: str) -> str:
        row = self.con.execute(
            "SELECT sector FROM nifty200_stocks WHERE symbol = ?", [symbol]
        ).fetchone()
        return row[0] if row else "Unknown"

    def _get_pattern_state(self, symbol: str) -> Optional[str]:
        row = self.con.execute(
            "SELECT current_state, state_labels FROM stock_patterns WHERE symbol = ?",
            [symbol]
        ).fetchone()
        if not row or row[0] is None:
            return None
        try:
            labels = json.loads(row[1]) if row[1] else {}
            return labels.get(str(row[0]), f"State {row[0]}")
        except Exception:
            return None

    def _build_reasoning(self, strategy: dict, row: pd.Series, ctx) -> str:
        """Build a human-readable reasoning string for the signal."""
        name   = strategy.get("name", "Unknown")
        rsi    = float(row.get("rsi_14", 0) or 0)
        adx    = float(row.get("adx_14", 0) or 0)
        vol_r  = float(row.get("volume_ratio", 1) or 1)
        market = ctx.market_cycle.upper()
        return (
            f"{name} | Market: {market} | "
            f"RSI: {rsi:.1f} | ADX: {adx:.1f} | Vol Ratio: {vol_r:.1f}x"
        )

    def _load_model(self, model_key: str):
        """Lazy-load ML model from disk."""
        if model_key in self._models:
            return self._models[model_key]
        import joblib
        from pathlib import Path
        path = Path(settings.models_dir) / f"{model_key}_v1.pkl"
        if not path.exists():
            return None
        try:
            bundle = joblib.load(path)
            self._models[model_key] = bundle
            return bundle
        except Exception as e:
            logger.debug(f"Could not load model {model_key}: {e}")
            return None
