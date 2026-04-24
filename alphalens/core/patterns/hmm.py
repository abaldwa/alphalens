"""
alphalens/core/patterns/hmm.py

Hidden Markov Model-based stock pattern detector.

Hypothesis: Each stock exhibits recurring behavioural regimes
(e.g. Accumulation → Trending → Distribution) that can be modelled
as hidden states in a Gaussian HMM.

States are automatically labelled by their return characteristics:
  - High positive mean return + low volatility → "Trending Up"
  - Negative mean return + high volatility     → "Distribution/Decline"
  - Near-zero return + moderate volatility     → "Accumulation/Consolidation"

Usage:
    detector = StockPatternDetector()
    detector.fit("RELIANCE")             # Train HMM for one stock
    detector.fit_all()                   # Train all 200 stocks
    state = detector.current_state("RELIANCE")
    history = detector.state_history("RELIANCE")
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from loguru import logger

from alphalens.core.database import get_duck
from config.settings import settings

MODELS_DIR = Path(settings.models_dir)
N_STATES   = 3
N_ITER     = 100


class StockPatternDetector:

    def __init__(self):
        self.con = get_duck()

    def fit(self, symbol: str, n_states: int = N_STATES) -> dict:
        """
        Fit a Gaussian HMM for one stock.
        Returns state metadata dict.
        """
        try:
            from hmmlearn.hmm import GaussianHMM
        except ImportError:
            logger.error("hmmlearn not installed: pip install hmmlearn")
            return {}

        prices = self._load_prices(symbol)
        if prices is None or len(prices) < 200:
            logger.warning(f"HMM: insufficient data for {symbol}")
            return {}

        # Feature matrix: daily return + log volume ratio
        returns  = prices["close"].pct_change().fillna(0)
        vol_ratio = np.log(prices["volume"] / prices["volume"].rolling(20).mean()).fillna(0)
        X = np.column_stack([returns.values, vol_ratio.values])

        # Fit Gaussian HMM
        model = GaussianHMM(
            n_components = n_states,
            covariance_type = "diag",
            n_iter = N_ITER,
            random_state = 42,
            verbose = False,
        )
        model.fit(X)
        state_sequence = model.predict(X)
        score          = model.score(X)

        # Auto-label states by mean return
        state_labels = self._auto_label_states(model, state_sequence, returns)

        # State history
        history = list(zip(prices.index.astype(str), state_sequence.tolist()))

        # Current state
        current_state = int(state_sequence[-1])

        # Transition matrix
        transmat = model.transmat_.tolist()

        # Store model to disk
        model_path = MODELS_DIR / f"hmm_{symbol}_v1.pkl"
        MODELS_DIR.mkdir(parents=True, exist_ok=True)
        import joblib
        joblib.dump(model, model_path)

        # Store metadata to DB
        self.con.execute("""
            INSERT OR REPLACE INTO stock_patterns
            (symbol, n_states, state_labels, transition_matrix,
             current_state, state_history, model_path, fitted_at, score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            symbol, n_states,
            json.dumps(state_labels),
            json.dumps(transmat),
            current_state,
            json.dumps(history[-500:]),  # keep last 500 for storage
            str(model_path),
            datetime.now(),
            float(score),
        ])

        logger.debug(
            f"HMM fitted for {symbol}: states={n_states}, "
            f"current={state_labels.get(str(current_state), current_state)}, "
            f"score={score:.2f}"
        )

        return {
            "symbol":        symbol,
            "n_states":      n_states,
            "current_state": current_state,
            "state_labels":  state_labels,
            "score":         score,
        }

    def fit_all(self, symbols: Optional[list] = None) -> dict:
        """Fit HMM for all Nifty200 stocks."""
        if symbols is None:
            from alphalens.core.ingestion.universe import get_all_symbols
            symbols = get_all_symbols()

        ok, failed = 0, []
        for symbol in symbols:
            try:
                result = self.fit(symbol)
                if result:
                    ok += 1
            except Exception as e:
                logger.debug(f"HMM failed for {symbol}: {e}")
                failed.append(symbol)

        logger.info(f"HMM fit complete: {ok} ok, {len(failed)} failed")
        return {"ok": ok, "failed": failed}

    def current_state(self, symbol: str) -> Optional[dict]:
        """Get the current regime state for a stock."""
        row = self.con.execute(
            "SELECT current_state, state_labels, n_states FROM stock_patterns WHERE symbol = ?",
            [symbol]
        ).fetchone()

        if not row:
            return None

        labels = json.loads(row[1]) if row[1] else {}
        state  = row[0]
        return {
            "state":       state,
            "label":       labels.get(str(state), f"State {state}"),
            "n_states":    row[2],
        }

    def state_history(self, symbol: str) -> list:
        """Get state history for a stock."""
        row = self.con.execute(
            "SELECT state_history FROM stock_patterns WHERE symbol = ?",
            [symbol]
        ).fetchone()
        if not row or not row[0]:
            return []
        try:
            return json.loads(row[0])
        except Exception:
            return []

    def regime_statistics(self, symbol: str) -> dict:
        """Compute return statistics per regime state."""
        history = self.state_history(symbol)
        if not history:
            return {}

        row = self.con.execute(
            "SELECT state_labels, n_states FROM stock_patterns WHERE symbol = ?",
            [symbol]
        ).fetchone()
        if not row:
            return {}

        labels   = json.loads(row[0]) if row[0] else {}
        n_states = row[1]

        prices = self._load_prices(symbol)
        if prices is None:
            return {}

        hist_df = pd.DataFrame(history, columns=["date", "state"])
        hist_df["date"] = pd.to_datetime(hist_df["date"])
        hist_df = hist_df.set_index("date")

        returns = prices["close"].pct_change()
        merged  = returns.to_frame("return").join(hist_df)

        stats = {}
        for s in range(n_states):
            state_returns = merged[merged["state"] == s]["return"].dropna()
            if len(state_returns) == 0:
                continue
            stats[str(s)] = {
                "label":      labels.get(str(s), f"State {s}"),
                "count":      len(state_returns),
                "mean_ret":   float(state_returns.mean() * 100),
                "vol":        float(state_returns.std() * 100),
                "win_rate":   float((state_returns > 0).mean()),
                "sharpe":     float(state_returns.mean() / state_returns.std() * np.sqrt(252))
                              if state_returns.std() > 0 else 0,
            }

        return stats

    # ── Private ────────────────────────────────────────────────────────────

    def _load_prices(self, symbol: str) -> Optional[pd.DataFrame]:
        df = self.con.execute("""
            SELECT date, close, volume
            FROM daily_prices
            WHERE symbol = ?
            ORDER BY date ASC
        """, [symbol]).fetchdf()
        if df.empty:
            return None
        df["date"] = pd.to_datetime(df["date"])
        return df.set_index("date")

    def _auto_label_states(self, model, state_sequence: np.ndarray,
                            returns: pd.Series) -> dict:
        """
        Auto-label states based on mean return characteristics.
        Highest mean return → "Trending Up"
        Most negative mean return → "Distribution/Decline"
        Middle → "Accumulation/Consolidation"
        """
        n = model.n_components
        state_means = {}

        for s in range(n):
            mask = state_sequence == s
            if mask.sum() > 0:
                state_means[s] = float(returns.values[mask].mean())

        sorted_states = sorted(state_means.items(), key=lambda x: x[1])
        labels = {}

        if len(sorted_states) == 3:
            labels[str(sorted_states[2][0])] = "Trending Up"
            labels[str(sorted_states[0][0])] = "Declining"
            labels[str(sorted_states[1][0])] = "Consolidating"
        else:
            for i, (s, mean_ret) in enumerate(sorted_states):
                if mean_ret > 0.001:
                    labels[str(s)] = "Trending Up"
                elif mean_ret < -0.001:
                    labels[str(s)] = "Declining"
                else:
                    labels[str(s)] = "Consolidating"

        return labels
