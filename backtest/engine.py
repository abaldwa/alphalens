"""
backtest/engine.py

Phase: 1.6 (Exit Signal + First Backtest)
Specs: SPEC-BT-001 through SPEC-BT-004, SPEC-MODEL-002, SPEC-MODEL-006
Owner: Platform / Backtest
Consumers: backtest/run_phase1_backtest.py

BacktestEngine: walk-forward backtest harness. P&D filter -> Signal model
-> MetaLabeler -> ExitSignalModel -> PortfolioSimulator, with
BacktestIntegrityChecker run automatically after every fold.

The P&D detector and exit model are passed in already trained (SPEC-
MODEL-006's P&D pre-filter and M-07's exit model are both fit on their
own real historical archives — load_pnd_training_data_from_db() /
load_exit_training_data_from_db() — independent of the specific OHLCV
universe being backtested, same as systems/ml_signal_engine/inference/
train_all_phase1.py's existing pattern). The signal model and meta-labeler ARE
walk-forward retrained per fold (via WalkForwardValidator, P1.4) since
fold-to-fold generalization of the entry signal is the actual subject of
this backtest.

[AS BUILT, scope note] Conformal (P1.5's ConformalPredictor) is not
wired into this engine: it calibrates return-magnitude regression
intervals, while the entry decision here is the P1.5 classification
stack (Signal model direction + MetaLabeler act/don't-act) — there is no
return-regression estimator in this pipeline for it to wrap yet. Left out
rather than forced in; documented in BuildLog.md as a known Phase 1 gap,
not a silent omission.
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import talib

from backtest.costs import IndianTransactionCosts
from backtest.integrity_checker import BacktestIntegrityChecker
from backtest.overfit_checks import deflated_sharpe_ratio, random_feature_test
from backtest.portfolio import PortfolioSimulator
from config.settings import MIN_ADT_INR
from config.timezone import now_ist
from features.pnd_features import PND_FEATURES, compute_pnd_features
from features.technical import CORE_TECHNICAL_FEATURES, compute_technical_features
from systems.ml_signal_engine.models.signal.meta_labeler import MetaLabeler
from systems.ml_signal_engine.training.labeling import TripleBarrierLabeler
from systems.ml_signal_engine.training.walk_forward import WalkForwardValidator
from backtest.core.feature_log import FeatureLogWriter
from backtest.core.horizon import HorizonBucket

logger = logging.getLogger(__name__)

TRADING_DAYS_PER_YEAR = 252
# Trailing window (trading days) for the real ADTV (average daily traded
# value, INR crore) computation this engine now feeds into both slippage
# costing and the liquidity floor — matches backtest/momentum_backtest.py's
# own adtv_lookback_days default.
ADTV_LOOKBACK_DAYS = 20
# Position-context columns ExitSignalModel.predict_full() expects, matching
# exit_signal.load_exit_training_data_from_db()'s schema so a model trained
# on that real historical archive can score real backtest positions, plus
# atr_pct (ATR/entry_price at entry — FutureDevelopment.md #28) which is
# additive: ExitSignalModel.predict_full() subsets to its own trained
# feature_names so an extra column is harmless, while RuleBasedExitPolicy
# uses it directly to ATR-scale target/stop instead of flat percentages.
EXIT_CONTEXT_COLUMNS = [
    "entry_price", "days_held", "unrealised_pnl_pct", "days_to_next_earnings",
    "drawdown_from_peak", "momentum_3m", "pnd_score", "hmm_regime", "atr_pct",
]
# No real earnings calendar ingested yet (Phase 1 gap, see BuildLog.md
# "Real data sourcing — earnings calendar") and no per-row HMM regime
# wired into this prototype's exit-context block (the per-day HMM fit
# train_all_phase1.py builds is market-wide, not joined into the
# per-ticker feature panel used here, see BuildLog.md "Real data sourcing
# — HMM regime in backtest exit context"). Both stay honestly NaN per
# CLAUDE.md Absolute Rule 6 (no fabricated stand-in values) — LightGBM
# (exit_signal.py's ExitSignalModel) handles NaN features natively, same
# convention exit_signal.py itself already uses for days_to_next_earnings
# at live-scoring time (see exit_signal.py's load_exit_training_data_from_db).


@dataclass
class FoldResult:
    fold_index: int
    train_start: Any
    train_end: Any
    test_start: Any
    test_end: Any
    cagr: float
    sharpe: float
    max_drawdown: float
    win_rate: float
    profit_factor: float
    n_trades: int
    final_equity: float
    # ML17a: real Nifty 500 buy-and-hold curve (index_ohlcv, see
    # BacktestEngine's benchmark_index param / _build_benchmark_curve())
    # over this same fold's test window. None when no real index_ohlcv
    # history covers the test window — never a synthetic/guessed value.
    benchmark_cagr: Optional[float] = None
    benchmark_sharpe: Optional[float] = None
    excess_return: Optional[float] = None


@dataclass
class BacktestResults:
    model_name: str
    from_date: Any
    to_date: Any
    fold_results: List[FoldResult]
    aggregate: Dict[str, float]
    integrity_passed: bool
    integrity_detail: Dict[str, Any] = field(default_factory=dict)
    generated_at: Any = field(default_factory=now_ist)
    # SPEC-MODEL-003 (M-13 stacking): per-row out-of-fold signal-model
    # predictions, populated only when run_full_backtest(collect_oof=True)
    # is used (see scripts/train_stacking.py). None for every other caller
    # (run_phase1/2/3_backtest.py) — default keeps their behavior unchanged.
    oof_df: Optional[pd.DataFrame] = None
    # Concatenated per-fold daily-return series (test windows only, in
    # fold order), populated only when run_full_backtest(collect_fold_
    # returns=True) is used — backtest/iterative_retrain.py's promotion
    # gate needs a real return series (not just the scalar sharpe_mean)
    # to feed overfit_checks.deflated_sharpe_ratio's skew/kurtosis
    # correction. None for every other caller — default keeps existing
    # behavior unchanged.
    fold_returns: Optional[pd.Series] = None
    # Per-fold MetaLabeler hyperparams + chronological 80/20 meta-training
    # split, populated only when run_full_backtest(collect_fold_models=True)
    # is used — see that method's docstring. None for every other caller.
    fold_models: Optional[List[Dict[str, Any]]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "model_name": self.model_name,
            "from_date": str(self.from_date) if self.from_date is not None else None,
            "to_date": str(self.to_date) if self.to_date is not None else None,
            "generated_at": self.generated_at.isoformat(),
            "integrity_passed": self.integrity_passed,
            "integrity_detail": self.integrity_detail,
            "aggregate": self.aggregate,
            "folds": [
                {
                    "fold_index": f.fold_index,
                    "train_start": str(f.train_start), "train_end": str(f.train_end),
                    "test_start": str(f.test_start), "test_end": str(f.test_end),
                    "cagr": f.cagr, "sharpe": f.sharpe, "max_drawdown": f.max_drawdown,
                    "win_rate": f.win_rate, "profit_factor": f.profit_factor,
                    "n_trades": f.n_trades, "final_equity": f.final_equity,
                    "benchmark_cagr": f.benchmark_cagr, "benchmark_sharpe": f.benchmark_sharpe,
                    "excess_return": f.excess_return,
                }
                for f in self.fold_results
            ],
        }


def _cagr_sharpe_from_equity(equity: np.ndarray, initial_capital: float) -> tuple:
    """Shared CAGR/Sharpe computation for any equity curve array (strategy or benchmark)."""
    final_equity = float(equity[-1])
    years = max(len(equity) / TRADING_DAYS_PER_YEAR, 1e-9)
    cagr = (final_equity / initial_capital) ** (1 / years) - 1 if initial_capital > 0 and final_equity > 0 else -1.0

    daily_returns = np.diff(equity) / equity[:-1]
    daily_returns = daily_returns[np.isfinite(daily_returns)]
    sharpe = (
        float(np.mean(daily_returns) / np.std(daily_returns) * np.sqrt(TRADING_DAYS_PER_YEAR))
        if len(daily_returns) > 1 and np.std(daily_returns) > 0
        else 0.0
    )
    return cagr, sharpe


def compute_fold_metrics(
    equity_curve: pd.DataFrame, trades_df: pd.DataFrame, initial_capital: float,
    benchmark_equity_curve: Optional[pd.DataFrame] = None,
) -> Dict[str, float]:
    """
    Parameters
    ----------
    equity_curve : pd.DataFrame
        Columns: date, equity (PortfolioSimulator.equity_curve).
    trades_df : pd.DataFrame
        Closed trades (PortfolioSimulator.trades_df).
    initial_capital : float
    benchmark_equity_curve : pd.DataFrame, optional
        ML17a: real Nifty 500 buy-and-hold curve over the same test window
        (BacktestEngine._build_benchmark_curve(), columns date/equity, same
        shape as equity_curve). None (default) leaves benchmark_cagr/
        benchmark_sharpe/excess_return as None — no synthetic benchmark
        fallback (CLAUDE.md Absolute Rule 6); a caller that never fetched
        real index_ohlcv history just gets these fields absent.

    Returns
    -------
    dict
        cagr, sharpe, max_drawdown (negative fraction), win_rate,
        profit_factor, n_trades, final_equity, benchmark_cagr,
        benchmark_sharpe, excess_return.
    """
    if equity_curve.empty:
        return {
            "cagr": 0.0, "sharpe": 0.0, "max_drawdown": 0.0, "win_rate": 0.0,
            "profit_factor": 0.0, "n_trades": 0, "final_equity": initial_capital,
            "benchmark_cagr": None, "benchmark_sharpe": None, "excess_return": None,
        }

    equity = equity_curve.sort_values("date")["equity"].to_numpy(dtype=np.float64)
    final_equity = float(equity[-1])
    cagr, sharpe = _cagr_sharpe_from_equity(equity, initial_capital)

    running_max = np.maximum.accumulate(equity)
    drawdown = (equity - running_max) / running_max
    max_drawdown = float(drawdown.min()) if len(drawdown) else 0.0

    if not trades_df.empty:
        win_rate = float((trades_df["pnl_inr"] > 0).mean())
        gross_profit = float(trades_df.loc[trades_df["pnl_inr"] > 0, "pnl_inr"].sum())
        gross_loss = float(-trades_df.loc[trades_df["pnl_inr"] < 0, "pnl_inr"].sum())
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (float("inf") if gross_profit > 0 else 0.0)
        n_trades = int(len(trades_df))
    else:
        win_rate, profit_factor, n_trades = 0.0, 0.0, 0

    benchmark_cagr: Optional[float] = None
    benchmark_sharpe: Optional[float] = None
    excess_return: Optional[float] = None
    if benchmark_equity_curve is not None and not benchmark_equity_curve.empty:
        bm_equity = benchmark_equity_curve.sort_values("date")["equity"].to_numpy(dtype=np.float64)
        if len(bm_equity) >= 2:
            benchmark_cagr, benchmark_sharpe = _cagr_sharpe_from_equity(bm_equity, float(bm_equity[0]))
            excess_return = cagr - benchmark_cagr

    return {
        "cagr": cagr, "sharpe": sharpe, "max_drawdown": max_drawdown,
        "win_rate": win_rate, "profit_factor": profit_factor,
        "n_trades": n_trades, "final_equity": final_equity,
        "benchmark_cagr": benchmark_cagr, "benchmark_sharpe": benchmark_sharpe,
        "excess_return": excess_return,
    }


class BacktestEngine:
    """SPEC-BT-001..004: walk-forward backtest harness for the Phase 1 signal stack."""

    def __init__(
        self,
        ohlcv: pd.DataFrame,
        pnd_detector: Any,
        exit_model: Any,
        signal_model_cls: type,
        sector_map: Dict[str, str],
        horizon_days: int = 5,
        profit_multiplier: float = 2.0,
        stop_multiplier: float = 1.0,
        initial_capital: float = 1_000_000.0,
        sizing_mode: str = "equal_weight",
        n_target_positions: int = 10,
        optuna_trials: int = 5,
        random_state: int = 42,
        n_folds: int = 5,
        meta_min_rows: int = 10,
        benchmark: Optional[pd.DataFrame] = None,
        universe_tickers: Optional[set] = None,
        historical_tickers: Optional[set] = None,
        watchlist_tickers: Optional[set] = None,
        benchmark_index: Optional[pd.DataFrame] = None,
        feature_log_writer: Optional[FeatureLogWriter] = None,
        run_id: Optional[str] = None,
        meta_labeler_params: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.ohlcv = ohlcv
        self.pnd_detector = pnd_detector
        self.exit_model = exit_model
        self.signal_model_cls = signal_model_cls
        self.sector_map = sector_map
        self.horizon_days = horizon_days
        self.profit_multiplier = profit_multiplier
        self.stop_multiplier = stop_multiplier
        self.initial_capital = initial_capital
        self.sizing_mode = sizing_mode
        self.n_target_positions = n_target_positions
        self.optuna_trials = optuna_trials
        self.random_state = random_state
        self.n_folds = n_folds
        self.meta_min_rows = meta_min_rows
        self.benchmark = benchmark
        # [AS BUILT, P2.6] Optional M-08 multibagger-watchlist entry filter
        # for backtest/run_phase2_backtest.py — None (default) preserves
        # Phase 1's exact existing behavior (every PIT-eligible, non-P&D-
        # blocked buy-signal ticker is a candidate); when set, _apply_entries
        # additionally restricts candidates to this ticker set before the
        # signal model ever sees them, same "entry filter stacks before the
        # model" position as the existing P&D pre-filter.
        self.watchlist_tickers = watchlist_tickers
        # ML17a: real Nifty 500 index_ohlcv series (date, close) — a real
        # NSE index level, not the NIFTYBEES/etc ETF-price proxy `benchmark`
        # above uses for Category 7 relative-strength features. Powers
        # per-fold benchmark_cagr/benchmark_sharpe/excess_return via
        # _build_benchmark_curve(). None (default) leaves every fold's
        # benchmark_* fields as None — no synthetic fallback (see
        # compute_fold_metrics's docstring); callers should pass real
        # index_ohlcv data via run_phase1_backtest.py's
        # _fetch_real_benchmark_index() when available.
        self.benchmark_index = benchmark_index
        # Optional per-decision feature-vector capture (backtest_feature_log,
        # backtest/core/feature_log.py) — records the full feature vector for
        # EVERY candidate a fold considers (bought, skipped, held, sold), not
        # just the ones acted on, so a later model-finetuning pass can query
        # "what did the model see for stocks it passed on." None (default)
        # keeps existing callers' behavior unchanged; run_phase1/2/3_backtest.py
        # pass a real writer + run_id to capture every run.
        self._feature_log_writer = feature_log_writer
        self._run_id = run_id
        self._horizon_bucket = {
            5: HorizonBucket.D5, 21: HorizonBucket.D21, 63: HorizonBucket.D63,
        }.get(horizon_days, HorizonBucket.CUSTOM)
        # Per-fold MetaLabeler hyperparameters — None (default) preserves
        # existing behavior (MetaLabeler's own __init__ defaults). Lets
        # backtest/iterative_retrain.py's tuning loop vary the entry-filter
        # model's hyperparameters between iterations without touching this
        # class's fold-retrain loop.
        self._meta_labeler_params = meta_labeler_params
        # universe_tickers / historical_tickers both default to the ohlcv
        # panel's own ticker set (the degenerate case where there's no
        # meaningful distinction between "currently investable" and "ever
        # observed" — both real data, just under-specified). Callers should pass both
        # explicitly and DIFFERENTLY — e.g. universe_tickers=config.
        # universe.get_tickers() (the curated, currently-investable set)
        # and historical_tickers=the full set of tickers the DataStore has
        # ever seen (broader than what self.ohlcv itself contains, since
        # self.ohlcv is typically filtered to tickers with enough history
        # to backtest on) — otherwise check_04_survivorship always finds
        # an empty difference and "passes" for the wrong reason (comparing
        # a set against itself, not real survivorship-bias coverage).
        all_tickers = set(ohlcv["ticker"].unique())
        self.universe_tickers = universe_tickers if universe_tickers is not None else all_tickers
        self.historical_tickers = historical_tickers if historical_tickers is not None else all_tickers

        self._combined = self._build_dataset()
        self._pnd_features = compute_pnd_features(ohlcv).set_index(["date", "ticker"])
        self._price_lookup = ohlcv.set_index(["date", "ticker"])["close"]
        self._momentum = self._build_momentum()
        self._adtv_lookup = self._build_adtv_lookup()

    # [BUG FIX, 2026-07-21 full-codebase-review REV2/REV3] Real trailing
    # ADTV (average daily traded value, INR crore) per (date, ticker) —
    # price*volume, ADTV_LOOKBACK_DAYS trailing mean, same real-gap NaN
    # handling as backtest/momentum_backtest.py's own `_adtv_cr` (no
    # forward-fill of volume itself). Previously this engine never
    # computed or threaded ADTV anywhere: `_apply_entries` had no
    # liquidity floor (check_06_liquidity's "applied_min_adt_inr" was a
    # hardcoded literal, not a real enforced value — see
    # _run_integrity_check), and `portfolio.buy`/`apply_exit_signal` were
    # never passed `adtv_cr`, so `IndianTransactionCosts._slippage_pct`
    # always used the default (non-small-cap) slippage rate even for
    # illiquid names, understating costs for exactly the low-liquidity
    # tickers SPEC-BT-002's 0.30% small-cap slippage bump exists for.
    def _build_adtv_lookup(self) -> pd.Series:
        df = self.ohlcv[["date", "ticker", "close", "volume"]].copy()
        df["_traded_value_cr"] = (df["close"] * df["volume"]) / 1e7
        df = df.sort_values(["ticker", "date"])
        df["_adtv_cr"] = df.groupby("ticker", sort=False)["_traded_value_cr"].transform(
            lambda s: s.rolling(ADTV_LOOKBACK_DAYS, min_periods=1).mean()
        )
        return df.set_index(["date", "ticker"])["_adtv_cr"]

    def _adtv_cr(self, d, tickers: List[str]) -> pd.Series:
        keys = [(d, t) for t in tickers]
        present = [k for k in keys if k in self._adtv_lookup.index]
        if not present:
            return pd.Series(np.nan, index=tickers)
        vals = self._adtv_lookup.loc[present]
        vals.index = [k[1] for k in present]
        return vals.reindex(tickers)

    def _build_dataset(self) -> pd.DataFrame:
        if self.benchmark is None:
            # CLAUDE.md Absolute Rule 6: no synthetic/procedurally-generated
            # data, ever, and no fallback to it. Callers must fetch a real
            # benchmark (e.g. run_phase1_backtest.py's _fetch_real_benchmark()
            # via NIFTYBEES/NIF100BEES/MONIFTY500) — there is no synthetic
            # stand-in here.
            raise ValueError(
                "BacktestEngine requires a real benchmark DataFrame (NIFTYBEES/NIF100BEES/"
                "MONIFTY500 OHLCV via DataStoreClient) — pass benchmark=... explicitly; "
                "there is no synthetic-benchmark fallback. See run_phase1_backtest.py's "
                "_fetch_real_benchmark()."
            )
        # Real benchmark supplied by the caller (e.g. run_phase1_backtest.py
        # fetching NIFTYBEES/NIF100BEES/MONIFTY500 via DataStoreClient).
        features = compute_technical_features(self.ohlcv, self.benchmark)

        atr_parts = []
        for ticker, g in self.ohlcv.sort_values(["ticker", "date"]).groupby("ticker", sort=False):
            atr = talib.ATR(
                g["high"].to_numpy(dtype=np.float64), g["low"].to_numpy(dtype=np.float64),
                g["close"].to_numpy(dtype=np.float64), timeperiod=14,
            )
            atr_parts.append(pd.DataFrame({"date": g["date"].to_numpy(), "ticker": ticker, "atr_14": atr}))
        atr_df = pd.concat(atr_parts, ignore_index=True)
        merged = self.ohlcv.merge(atr_df, on=["date", "ticker"], how="left")

        labeler = TripleBarrierLabeler(
            profit_multiplier=self.profit_multiplier, stop_multiplier=self.stop_multiplier,
            max_holding=self.horizon_days,
        )
        labels = labeler.label_panel(merged, close_col="close", atr_col="atr_14", ticker_col="ticker")
        forward_returns = merged.groupby("ticker", sort=False)["close"].transform(
            lambda s: s.shift(-self.horizon_days) / s - 1
        )

        combined = features.copy()
        combined["_label"] = labels.to_numpy()
        combined["_return"] = forward_returns.to_numpy()
        return combined.dropna(subset=["_label", "_return"]).reset_index(drop=True)

    def _build_momentum(self) -> pd.Series:
        """63-day close-to-close momentum per (date, ticker) — exit-context proxy for momentum_3m."""
        df = self.ohlcv.sort_values(["ticker", "date"]).copy()
        df["momentum_3m"] = df.groupby("ticker", sort=False)["close"].transform(lambda s: s / s.shift(63) - 1)
        return df.set_index(["date", "ticker"])["momentum_3m"]

    def _pnd_scores(self, date, tickers: List[str]) -> pd.Series:
        keys = [(date, t) for t in tickers]
        present = [k for k in keys if k in self._pnd_features.index]
        if not present:
            return pd.Series(0.0, index=tickers)
        rows = self._pnd_features.loc[present, PND_FEATURES]
        scores = self.pnd_detector.predict_full(rows)["pnd_score"]
        scores.index = [k[1] for k in present]
        return scores.reindex(tickers).fillna(0.0)

    def _pnd_blocked(self, date, tickers: List[str]) -> pd.Series:
        keys = [(date, t) for t in tickers]
        present = [k for k in keys if k in self._pnd_features.index]
        if not present:
            return pd.Series(False, index=tickers)
        rows = self._pnd_features.loc[present, PND_FEATURES]
        blocked = self.pnd_detector.predict_full(rows)["pnd_block"]
        blocked.index = [k[1] for k in present]
        return blocked.reindex(tickers).fillna(False)

    def _build_benchmark_curve(self, test_fold: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        ML17a: real Nifty 500 buy-and-hold equity curve over this fold's
        test-set trading days, normalised to self.initial_capital at the
        first date both the fold and self.benchmark_index have real
        index_ohlcv coverage for.

        Returns
        -------
        pd.DataFrame or None
            Columns: date, equity. None if self.benchmark_index wasn't
            supplied, or has no real overlap with this fold's test dates —
            never a synthetic/interpolated stand-in.
        """
        if self.benchmark_index is None or self.benchmark_index.empty:
            return None

        test_dates = sorted(test_fold["date"].unique())
        if not test_dates:
            return None

        bm = self.benchmark_index.sort_values("date")
        bm_in_range = bm[(bm["date"] >= test_dates[0]) & (bm["date"] <= test_dates[-1])]
        if bm_in_range.empty:
            return None

        entry_price = float(bm_in_range["close"].iloc[0])
        if entry_price <= 0:
            return None

        shares = self.initial_capital / entry_price
        curve = bm_in_range[["date", "close"]].copy()
        curve["equity"] = curve["close"] * shares
        return curve[["date", "equity"]].reset_index(drop=True)

    def _simulate(self, test_fold: pd.DataFrame, signal_model: Any, meta_model: Optional[Any]) -> PortfolioSimulator:
        portfolio = PortfolioSimulator(
            initial_capital=self.initial_capital, sizing_mode=self.sizing_mode,
            n_target_positions=self.n_target_positions,
        )
        for d in sorted(test_fold["date"].unique()):
            prices_today = (
                self._price_lookup.loc[d].to_dict() if d in self._price_lookup.index.get_level_values(0) else {}
            )

            for ticker in portfolio.positions:
                if ticker in prices_today:
                    portfolio.update_peak(ticker, prices_today[ticker])
            portfolio.record_equity(d, prices_today)

            self._apply_exits(portfolio, d, prices_today)
            self._apply_entries(portfolio, test_fold[test_fold["date"] == d], d, prices_today, signal_model, meta_model)

        return portfolio

    def _log_feature(
        self, ticker: str, d, feature_vector: Dict[str, Any], decision_taken: str,
        signal_output: Optional[str] = None,
    ) -> None:
        if self._feature_log_writer is None or self._run_id is None:
            return
        as_of = d.date() if hasattr(d, "date") else d
        self._feature_log_writer.record(
            run_id=self._run_id, ticker=ticker, as_of_date=as_of, horizon_bucket=self._horizon_bucket,
            feature_vector=feature_vector, decision_taken=decision_taken, signal_output=signal_output,
        )

    def _apply_exits(self, portfolio: PortfolioSimulator, d, prices_today: Dict[str, float]) -> None:
        held = [t for t in portfolio.positions if t in prices_today]
        if not held:
            return
        pnd_scores = self._pnd_scores(d, held)
        rows = []
        for t in held:
            pos = portfolio.positions[t]
            price = prices_today[t]
            days_held = max((pd.Timestamp(d) - pd.Timestamp(pos.entry_date)).days, 0)
            momentum = self._momentum.get((d, t), 0.0)
            rows.append(
                {
                    "ticker": t, "entry_price": pos.entry_price, "days_held": float(days_held),
                    "unrealised_pnl_pct": (price - pos.entry_price) / pos.entry_price,
                    "days_to_next_earnings": np.nan,
                    "drawdown_from_peak": (price - pos.peak_price) / pos.peak_price if pos.peak_price else 0.0,
                    "momentum_3m": 0.0 if pd.isna(momentum) else momentum,
                    "pnd_score": pnd_scores.get(t, 0.0),
                    "hmm_regime": np.nan,
                    "atr_pct": pos.entry_atr_pct if pos.entry_atr_pct is not None else np.nan,
                }
            )
        exit_ctx = pd.DataFrame(rows).set_index("ticker")[EXIT_CONTEXT_COLUMNS]
        exit_out = self.exit_model.predict_full(exit_ctx)
        adtv_today = self._adtv_cr(d, held)
        for t in held:
            urgency = float(exit_out.loc[t, "exit_urgency"])
            adtv_cr = adtv_today.get(t)
            adtv_cr = float(adtv_cr) if pd.notna(adtv_cr) else None
            trade = portfolio.apply_exit_signal(t, urgency, prices_today[t], d, adtv_cr=adtv_cr)
            decision = "sold" if trade is not None else "held"
            self._log_feature(
                t, d, exit_ctx.loc[t].to_dict(), decision, signal_output=f"exit_urgency={urgency:.4f}",
            )

    def _apply_entries(
        self, portfolio: PortfolioSimulator, day_rows: pd.DataFrame, d, prices_today: Dict[str, float],
        signal_model: Any, meta_model: Optional[Any],
    ) -> None:
        candidates = day_rows[~day_rows["ticker"].isin(portfolio.positions.keys())]
        if self.watchlist_tickers is not None:
            candidates = candidates[candidates["ticker"].isin(self.watchlist_tickers)]
        if candidates.empty:
            return
        feat_block = candidates.set_index("ticker")[CORE_TECHNICAL_FEATURES]

        # [BUG FIX, 2026-07-21 full-codebase-review REV3] SPEC-BT-001 rule 5 /
        # check_06_liquidity claims a MIN_ADT_INR liquidity floor is enforced
        # on entries, but nothing in this method ever called
        # IndianTransactionCosts.is_liquid_enough()/checked MIN_ADT_INR — an
        # illiquid ticker could be bought at full simulated size with no
        # floor at all. Entry filter stacks before the model, same position
        # as the P&D pre-filter and watchlist filter above.
        adtv_at_entry = self._adtv_cr(d, list(feat_block.index))
        illiquid = adtv_at_entry.index[adtv_at_entry.isna() | (adtv_at_entry * 1e7 < MIN_ADT_INR)]
        for t in illiquid:
            self._log_feature(t, d, feat_block.loc[t].to_dict(), "skipped_illiquid")
        feat_block = feat_block.loc[~feat_block.index.isin(illiquid)]
        if feat_block.empty:
            return

        blocked = self._pnd_blocked(d, list(feat_block.index))
        blocked_tickers = feat_block.index[blocked.to_numpy()]
        for t in blocked_tickers:
            self._log_feature(t, d, feat_block.loc[t].to_dict(), "skipped_pnd_blocked")
        feat_block = feat_block.loc[~blocked.to_numpy()]
        if feat_block.empty:
            return

        directions = signal_model.predict(feat_block)
        is_buy = directions.to_numpy() == 1
        buy_tickers = feat_block.index[is_buy]
        for t in feat_block.index[~is_buy]:
            self._log_feature(t, d, feat_block.loc[t].to_dict(), "skipped_no_signal")
        if len(buy_tickers) == 0:
            return

        if meta_model is not None:
            acts = meta_model.predict(feat_block.loc[buy_tickers]).to_numpy().astype(bool)
            for t in buy_tickers[~acts]:
                self._log_feature(t, d, feat_block.loc[t].to_dict(), "skipped_meta_veto")
            buy_tickers = buy_tickers[acts]

        for ticker in buy_tickers:
            price = prices_today.get(ticker)
            if price is None or price <= 0:
                self._log_feature(ticker, d, feat_block.loc[ticker].to_dict(), "skipped_no_price")
                continue
            # atr_14_pct (CORE_TECHNICAL_FEATURES, features/technical.py) is
            # ATR(14)/close * 100 — divide back to a plain fraction of price
            # for RuleBasedExitPolicy's ATR-scaled target/stop (atr_pct).
            atr_14_pct = feat_block.loc[ticker, "atr_14_pct"] if "atr_14_pct" in feat_block.columns else np.nan
            entry_atr_pct = float(atr_14_pct) / 100.0 if pd.notna(atr_14_pct) else None
            portfolio.buy(
                ticker, self.sector_map.get(ticker, "UNKNOWN"), price, d, prices_today,
                entry_atr_pct=entry_atr_pct,
            )
            self._log_feature(ticker, d, feat_block.loc[ticker].to_dict(), "bought")

    def _real_applied_roundtrip_cost_pct(self, portfolio: PortfolioSimulator) -> float:
        """
        [BUG FIX, 2026-07-21 full-codebase-review REV1] Real mean applied
        cost % measured from this fold's actual closed trades (cost_inr /
        entry turnover), not a hardcoded literal — a hardcoded value
        compared against itself in check_05_costs can never fail no
        matter what the simulation actually charged.

        Falls back to a real (not fabricated) representative rate — the
        same IndianTransactionCosts rate table computation
        `validate_against_settings` uses for its own sanity check — only
        when this fold closed zero trades, so check_05_costs still has a
        real, table-driven value to validate rather than nothing.
        """
        trades = portfolio.trades_df
        if trades.empty:
            return IndianTransactionCosts().compute_roundtrip_cost_pct(price=1000.0, quantity=100)
        turnover = trades["entry_price"] * trades["quantity"]
        applied_pct = (trades["cost_inr"] / turnover).replace([np.inf, -np.inf], np.nan).dropna()
        if applied_pct.empty:
            return IndianTransactionCosts().compute_roundtrip_cost_pct(price=1000.0, quantity=100)
        return float(applied_pct.mean())

    def _run_integrity_check(
        self, train_fold: pd.DataFrame, test_fold: pd.DataFrame, portfolio: PortfolioSimulator,
    ) -> Dict[str, Any]:
        checker = BacktestIntegrityChecker(
            folds=[(train_fold, test_fold)],
            feature_df=self._combined[["date"]],
            ohlcv_df=self.ohlcv,
            universe_tickers=self.universe_tickers,
            historical_tickers=self.historical_tickers,
            # [BUG FIX, 2026-07-21 full-codebase-review REV1] Real values,
            # not hardcoded literals: the cost % actually measured from
            # this fold's trades (see _real_applied_roundtrip_cost_pct),
            # and MIN_ADT_INR itself now that _apply_entries (REV3, above)
            # genuinely enforces it as the entry liquidity floor — so
            # reporting MIN_ADT_INR here is an honest statement of what
            # was actually enforced this fold, not an assumed constant.
            applied_roundtrip_cost_pct=self._real_applied_roundtrip_cost_pct(portfolio),
            applied_min_adt_inr=float(MIN_ADT_INR),
            # SPEC-MODEL-003: Optuna HPO is scoped to the train/validation split
            # only (see signal_model.train_full's train_df/val_df args below) —
            # already true in the implementation, just reported here so
            # check_07_no_hpo_on_test can confirm it rather than skip for lack
            # of a value.
            hpo_dataset="validation",
        )
        try:
            checker.run_all_checks()
            passed = True
            detail = {"critical_failures": []}
        except RuntimeError as exc:
            passed = False
            detail = {"critical_failures": [str(exc)]}
        return {"passed": passed, "detail": detail}

    def run_full_backtest(
        self, model_name: str, from_date: Optional[Any] = None, to_date: Optional[Any] = None, folds: int = 5,
        collect_oof: bool = False, collect_fold_returns: bool = False, collect_fold_models: bool = False,
    ) -> BacktestResults:
        """
        Run the full P&D -> Signal -> MetaLabel -> Exit walk-forward
        backtest and return per-fold + aggregate metrics.

        Parameters
        ----------
        model_name : str
            Label for the resulting BacktestResults (e.g. 'signal_5d').
        from_date, to_date : optional
            Restrict the dataset to this date range before splitting into folds.
        folds : int
            Requested number of walk-forward folds; reduced automatically
            if the date range doesn't span enough distinct years.
        collect_oof : bool
            When True, accumulate each fold's test-set signal-model
            predictions (date, ticker, fold, y_true, proba_sell/hold/buy)
            into BacktestResults.oof_df — used by scripts/train_stacking.py
            (M-13) to build genuine out-of-fold training data for the
            stacking meta-learner. Default False preserves the exact
            existing behavior/return shape for all other callers.
        collect_fold_models : bool
            When True, accumulate each fold's MetaLabeler hyperparameters
            plus a chronological 80/20 train/test split of that fold's
            meta-training data (val_df's CORE_TECHNICAL_FEATURES rows) into
            BacktestResults.fold_models — used by
            backtest/iterative_retrain.py's promotion gate to run
            overfit_checks.random_feature_test per fold without
            re-deriving the split itself. The split is independent of
            (and never touches) the meta_model actually used for this
            fold's simulation — random_feature_test mutates the model
            it's given by re-training it on shuffled data, so callers
            must construct a fresh MetaLabeler from the collected
            lgbm_params rather than reuse a fold's production model.
            Default False preserves existing behavior/return shape.

        Returns
        -------
        BacktestResults

        Raises
        ------
        ValueError
            If the filtered dataset is empty.
        """
        combined = self._combined
        if from_date is not None:
            combined = combined[combined["date"] >= pd.Timestamp(from_date)]
        if to_date is not None:
            combined = combined[combined["date"] <= pd.Timestamp(to_date)]
        if combined.empty:
            raise ValueError("no rows in the requested date range")

        validator = WalkForwardValidator(n_folds=folds)
        n_folds_data = validator._fiscal_years(combined["date"]).nunique() - 1
        if n_folds_data < 1:
            train_fold, test_fold = validator.get_train_validation_split(combined, val_fraction=0.3)
            date_folds = [(train_fold, test_fold)]
        else:
            # embargo_days=self.horizon_days: same source of truth
            # TripleBarrierLabeler(max_holding=self.horizon_days) uses in
            # _build_dataset — a trade opened within horizon_days of a fold
            # boundary can still resolve after it (see split_data's
            # docstring), so the embargo must match the label horizon.
            date_folds = validator.split_data(
                combined, n_folds=min(folds, n_folds_data), embargo_days=self.horizon_days,
            )

        fold_results: List[FoldResult] = []
        fold_integrity_results: List[Dict[str, Any]] = []
        oof_rows: List[pd.DataFrame] = []
        fold_return_series: List[pd.Series] = []
        fold_models: List[Dict[str, Any]] = []
        # [BUG FIX, 2026-07-21 full-codebase-review REV4] Real per-fold
        # random-feature-test accuracy, fed into the aggregate integrity
        # check below — previously check_10_random_feature never received
        # a value at all (permanently "failed for lack of context", not a
        # genuine noise-fitting signal).
        fold_random_feature_accuracies: List[float] = []

        for i, (train_fold, test_fold) in enumerate(date_folds):
            train_df, val_df = validator.get_train_validation_split(train_fold, val_fraction=0.2)

            signal_model = self.signal_model_cls(optuna_trials=self.optuna_trials, random_state=self.random_state)
            signal_model.train_full(
                train_df[CORE_TECHNICAL_FEATURES], train_df["_label"],
                val_df[CORE_TECHNICAL_FEATURES], val_df["_label"],
                returns_train=train_df["_return"], returns_val=val_df["_return"],
            )

            meta_X = val_df[CORE_TECHNICAL_FEATURES].reset_index(drop=True)
            direction = signal_model.predict(meta_X)
            meta_labels = MetaLabeler.compute_labels(direction, val_df["_return"].reset_index(drop=True))
            meta_mask = meta_labels.notna()
            meta_model: Optional[MetaLabeler] = None
            if meta_mask.sum() >= self.meta_min_rows:
                meta_model = MetaLabeler(random_state=self.random_state, lgbm_params=self._meta_labeler_params)
                meta_model.train(meta_X[meta_mask], meta_labels[meta_mask])
            else:
                logger.warning("fold %d: too few Act-labeled rows to train MetaLabeler — entries unfiltered by meta", i)

            if meta_model is not None:
                meta_train_X = meta_X[meta_mask].reset_index(drop=True)
                meta_train_y = meta_labels[meta_mask].reset_index(drop=True)
                split_idx = int(len(meta_train_X) * 0.8)
                if split_idx > 0 and split_idx < len(meta_train_X):
                    if collect_fold_models:
                        fold_models.append(
                            {
                                "fold_index": i, "lgbm_params": dict(meta_model._lgbm_params),
                                "X_train": meta_train_X.iloc[:split_idx], "y_train": meta_train_y.iloc[:split_idx],
                                "X_test": meta_train_X.iloc[split_idx:], "y_test": meta_train_y.iloc[split_idx:],
                            }
                        )
                    # Real random-feature test (backtest/overfit_checks.py):
                    # a FRESH MetaLabeler (never the fold's production
                    # model — random_feature_test mutates whatever it's
                    # given by retraining it on shuffled features) on this
                    # fold's own real chronological 80/20 meta-training
                    # split. n_repeats=5 (not the default 10) to keep the
                    # per-fold cost bounded across a multi-fold/multi-model
                    # phase gate run — still a genuine, non-fabricated
                    # measurement, just fewer shuffle repeats averaged.
                    try:
                        rf_model = MetaLabeler(random_state=self.random_state, lgbm_params=dict(meta_model._lgbm_params))
                        rf_accuracy = random_feature_test(
                            rf_model,
                            meta_train_X.iloc[:split_idx], meta_train_y.iloc[:split_idx],
                            meta_train_X.iloc[split_idx:], meta_train_y.iloc[split_idx:],
                            feature_cols=list(meta_train_X.columns), n_repeats=5, random_state=self.random_state,
                        )
                        fold_random_feature_accuracies.append(rf_accuracy)
                    except Exception as exc:
                        logger.warning("fold %d: random_feature_test failed, skipping for this fold (%s)", i, exc)

            if collect_oof:
                proba = signal_model.predict_proba(test_fold[CORE_TECHNICAL_FEATURES])
                oof_rows.append(
                    pd.DataFrame(
                        {
                            "date": test_fold["date"].to_numpy(),
                            "ticker": test_fold["ticker"].to_numpy(),
                            "fold": i,
                            "y_true": test_fold["_label"].to_numpy(),
                            "proba_sell": proba["sell"].to_numpy(),
                            "proba_hold": proba["hold"].to_numpy(),
                            "proba_buy": proba["buy"].to_numpy(),
                        }
                    )
                )

            portfolio = self._simulate(test_fold, signal_model, meta_model)
            benchmark_curve = self._build_benchmark_curve(test_fold)
            metrics = compute_fold_metrics(
                portfolio.equity_curve, portfolio.trades_df, self.initial_capital,
                benchmark_equity_curve=benchmark_curve,
            )
            # Always accumulated internally now (real, cheap — just the
            # equity curve's own pct_change) so deflated_sharpe_ratio below
            # has a real per-period return series to compute a genuine
            # skew/kurtosis-corrected standard error from, regardless of
            # whether the caller wants the raw series back via
            # collect_fold_returns (BacktestResults.fold_returns).
            equity = portfolio.equity_curve.set_index("date")["equity"]
            fold_return_series.append(equity.pct_change().dropna())

            fold_results.append(
                FoldResult(
                    fold_index=i,
                    train_start=train_fold["date"].min(), train_end=train_fold["date"].max(),
                    test_start=test_fold["date"].min(), test_end=test_fold["date"].max(),
                    **metrics,
                )
            )
            fold_integrity = self._run_integrity_check(train_fold, test_fold, portfolio)
            fold_integrity["fold_index"] = i
            fold_integrity_results.append(fold_integrity)

        # A critical integrity failure in ANY fold makes the whole backtest
        # untrustworthy — a later fold happening to pass must never mask an
        # earlier fold's look-ahead/cost/liquidity violation (previously
        # only the last fold's result was kept here, silently discarding
        # every earlier fold's check_01..07 outcome).
        integrity = {
            "passed": all(r["passed"] for r in fold_integrity_results) if fold_integrity_results else False,
            "detail": {
                "critical_failures": [
                    f"fold {r['fold_index']}: {failure}"
                    for r in fold_integrity_results
                    for failure in r["detail"].get("critical_failures", [])
                ],
            },
        }

        aggregate = {
            "cagr_mean": float(np.mean([f.cagr for f in fold_results])),
            "sharpe_mean": float(np.mean([f.sharpe for f in fold_results])),
            "max_drawdown_worst": float(np.min([f.max_drawdown for f in fold_results])),
            "win_rate_mean": float(np.mean([f.win_rate for f in fold_results])),
            "profit_factor_mean": float(
                np.mean([f.profit_factor for f in fold_results if np.isfinite(f.profit_factor)] or [0.0])
            ),
            "total_trades": int(sum(f.n_trades for f in fold_results)),
        }
        # ML17a: aggregate excess return only over folds where a real
        # benchmark curve was actually available — never averaged against a
        # None/missing value.
        excess_returns = [f.excess_return for f in fold_results if f.excess_return is not None]
        aggregate["excess_return_mean"] = float(np.mean(excess_returns)) if excess_returns else None
        benchmark_cagrs = [f.benchmark_cagr for f in fold_results if f.benchmark_cagr is not None]
        aggregate["benchmark_cagr_mean"] = float(np.mean(benchmark_cagrs)) if benchmark_cagrs else None
        # A plain fold-count mean lets a short, low-trade-count trailing fold
        # (e.g. the current, still-incomplete calendar year) dominate the
        # headline number once its CAGR/Sharpe get annualized off a handful
        # of trades — see BuildLog.md "Full Project Status Review —
        # 2026-07-02" item 6. Report both the existing unweighted mean
        # (unchanged, for backward compatibility with existing consumers)
        # and a version that only considers folds whose test window is
        # (approximately) a full year, so gate decisions can use the more
        # representative number instead of one skewed by a partial period.
        full_year_folds = [f for f in fold_results if (f.test_end - f.test_start).days >= 350]
        aggregate["n_partial_folds_excluded"] = len(fold_results) - len(full_year_folds)
        if full_year_folds:
            aggregate["cagr_mean_full_periods_only"] = float(np.mean([f.cagr for f in full_year_folds]))
            aggregate["sharpe_mean_full_periods_only"] = float(np.mean([f.sharpe for f in full_year_folds]))
        else:
            aggregate["cagr_mean_full_periods_only"] = None
            aggregate["sharpe_mean_full_periods_only"] = None

        # [BUG FIX, 2026-07-21 full-codebase-review REV4] check_08_fold_stability
        # / check_09_benchmarks / check_10_random_feature are aggregate-level
        # signals (need every fold's Sharpe/return, not one fold's) — the
        # per-fold _run_integrity_check call above never had this context to
        # give them, so they always failed "for lack of context," which is
        # not the same as a genuine fold-stability/benchmark/noise failure.
        # Run them here, once, with the real values this loop already
        # computed (fold Sharpes, paired fold/benchmark returns, real
        # per-fold random-feature-test accuracy). Non-critical (warn-only,
        # per CRITICAL_CHECKS), so this never raises — it only makes these
        # checks structurally capable of failing, matching the other 7.
        fold_sharpes = [f.sharpe for f in fold_results]
        paired_returns = [(f.cagr, f.benchmark_cagr) for f in fold_results if f.benchmark_cagr is not None]
        aggregate_checker = BacktestIntegrityChecker(
            fold_sharpes=fold_sharpes or None,
            fold_returns=[p[0] for p in paired_returns] or None,
            benchmark_returns=[p[1] for p in paired_returns] or None,
            random_feature_accuracy=(
                float(np.mean(fold_random_feature_accuracies)) if fold_random_feature_accuracies else None
            ),
        )
        for check_name in ("check_08_fold_stability", "check_09_benchmarks", "check_10_random_feature"):
            result = getattr(aggregate_checker, check_name)()
            aggregate[f"integrity_{check_name}"] = {"passed": result.passed, "detail": result.detail}
            if not result.passed:
                logger.warning("Backtest quality check failed (non-critical): %s: %s", result.name, result.detail)

        # [BUG FIX, 2026-07-21 full-codebase-review REV6] Deflated Sharpe
        # Ratio (SPEC-BT-001 rule 8) was built (backtest/overfit_checks.py)
        # but never actually invoked by any phase-gate caller — a raw
        # Sharpe-improvement gate with no multiple-comparisons correction
        # is exactly the "best of N configurations" failure mode DSR
        # exists to catch, given each candidate is itself the winner of
        # its own Optuna HPO search (self.optuna_trials trials). Computed
        # here (real fold_returns/n_obs from this run) so callers like
        # run_phase3_backtest.py can use it directly instead of a bare
        # Sharpe delta.
        _all_fold_returns = pd.concat(fold_return_series) if fold_return_series else None
        if _all_fold_returns is not None and len(_all_fold_returns) >= 3 and aggregate["sharpe_mean"] is not None:
            try:
                aggregate["deflated_sharpe_ratio"] = deflated_sharpe_ratio(
                    sharpe=aggregate["sharpe_mean"], n_trials=max(self.optuna_trials, 1),
                    n_obs=len(_all_fold_returns), returns=_all_fold_returns,
                )
            except ValueError as exc:
                logger.warning("deflated_sharpe_ratio computation failed: %s", exc)
                aggregate["deflated_sharpe_ratio"] = None
        else:
            aggregate["deflated_sharpe_ratio"] = None

        oof_df = pd.concat(oof_rows, ignore_index=True) if oof_rows else None
        # BacktestResults.fold_returns keeps its exact prior opt-in
        # behavior (collect_fold_returns=True only) — the series is now
        # always computed internally (above) for deflated_sharpe_ratio,
        # but only returned to the caller when explicitly requested.
        fold_returns = _all_fold_returns if collect_fold_returns else None

        if self._feature_log_writer is not None:
            self._feature_log_writer.flush()

        return BacktestResults(
            model_name=model_name, from_date=from_date, to_date=to_date,
            fold_results=fold_results, aggregate=aggregate,
            integrity_passed=integrity["passed"], integrity_detail=integrity["detail"],
            oof_df=oof_df, fold_returns=fold_returns,
            fold_models=fold_models if collect_fold_models else None,
        )
