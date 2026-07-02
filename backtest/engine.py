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

from backtest.integrity_checker import BacktestIntegrityChecker
from backtest.portfolio import PortfolioSimulator
from config.timezone import now_ist
from features.pnd_features import PND_FEATURES, compute_pnd_features
from features.technical import CORE_TECHNICAL_FEATURES, compute_technical_features
from systems.ml_signal_engine.models.signal.meta_labeler import MetaLabeler
from systems.ml_signal_engine.training.labeling import TripleBarrierLabeler
from systems.ml_signal_engine.training.walk_forward import WalkForwardValidator

logger = logging.getLogger(__name__)

TRADING_DAYS_PER_YEAR = 252
# Position-context columns ExitSignalModel.predict_full() expects, matching
# exit_signal.load_exit_training_data_from_db()'s schema exactly so a model
# trained on that real historical archive can score real backtest positions.
EXIT_CONTEXT_COLUMNS = [
    "entry_price", "days_held", "unrealised_pnl_pct", "days_to_next_earnings",
    "drawdown_from_peak", "momentum_3m", "pnd_score", "hmm_regime",
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
                }
                for f in self.fold_results
            ],
        }


def compute_fold_metrics(
    equity_curve: pd.DataFrame, trades_df: pd.DataFrame, initial_capital: float
) -> Dict[str, float]:
    """
    Parameters
    ----------
    equity_curve : pd.DataFrame
        Columns: date, equity (PortfolioSimulator.equity_curve).
    trades_df : pd.DataFrame
        Closed trades (PortfolioSimulator.trades_df).
    initial_capital : float

    Returns
    -------
    dict
        cagr, sharpe, max_drawdown (negative fraction), win_rate,
        profit_factor, n_trades, final_equity.
    """
    if equity_curve.empty:
        return {
            "cagr": 0.0, "sharpe": 0.0, "max_drawdown": 0.0, "win_rate": 0.0,
            "profit_factor": 0.0, "n_trades": 0, "final_equity": initial_capital,
        }

    equity = equity_curve.sort_values("date")["equity"].to_numpy(dtype=np.float64)
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

    return {
        "cagr": cagr, "sharpe": sharpe, "max_drawdown": max_drawdown,
        "win_rate": win_rate, "profit_factor": profit_factor,
        "n_trades": n_trades, "final_equity": final_equity,
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
                }
            )
        exit_ctx = pd.DataFrame(rows).set_index("ticker")[EXIT_CONTEXT_COLUMNS]
        exit_out = self.exit_model.predict_full(exit_ctx)
        for t in held:
            portfolio.apply_exit_signal(t, float(exit_out.loc[t, "exit_urgency"]), prices_today[t], d)

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

        blocked = self._pnd_blocked(d, list(feat_block.index))
        feat_block = feat_block.loc[~blocked.to_numpy()]
        if feat_block.empty:
            return

        directions = signal_model.predict(feat_block)
        buy_tickers = feat_block.index[directions.to_numpy() == 1]
        if len(buy_tickers) == 0:
            return

        if meta_model is not None:
            acts = meta_model.predict(feat_block.loc[buy_tickers])
            buy_tickers = buy_tickers[acts.to_numpy().astype(bool)]

        for ticker in buy_tickers:
            price = prices_today.get(ticker)
            if price is None or price <= 0:
                continue
            portfolio.buy(ticker, self.sector_map.get(ticker, "UNKNOWN"), price, d, prices_today)

    def _run_integrity_check(self, train_fold: pd.DataFrame, test_fold: pd.DataFrame) -> Dict[str, Any]:
        checker = BacktestIntegrityChecker(
            folds=[(train_fold, test_fold)],
            feature_df=self._combined[["date"]],
            ohlcv_df=self.ohlcv,
            universe_tickers=self.universe_tickers,
            historical_tickers=self.historical_tickers,
            applied_roundtrip_cost_pct=0.4,
            applied_min_adt_inr=1_000_000,
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
        collect_oof: bool = False,
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
        n_folds_data = combined["date"].dt.year.nunique() - 1
        if n_folds_data < 1:
            train_fold, test_fold = validator.get_train_validation_split(combined, val_fraction=0.3)
            date_folds = [(train_fold, test_fold)]
        else:
            date_folds = validator.split_data(combined, n_folds=min(folds, n_folds_data))

        fold_results: List[FoldResult] = []
        integrity: Dict[str, Any] = {"passed": False, "detail": {}}
        oof_rows: List[pd.DataFrame] = []

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
                meta_model = MetaLabeler(random_state=self.random_state)
                meta_model.train(meta_X[meta_mask], meta_labels[meta_mask])
            else:
                logger.warning("fold %d: too few Act-labeled rows to train MetaLabeler — entries unfiltered by meta", i)

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
            metrics = compute_fold_metrics(portfolio.equity_curve, portfolio.trades_df, self.initial_capital)

            fold_results.append(
                FoldResult(
                    fold_index=i,
                    train_start=train_fold["date"].min(), train_end=train_fold["date"].max(),
                    test_start=test_fold["date"].min(), test_end=test_fold["date"].max(),
                    **metrics,
                )
            )
            integrity = self._run_integrity_check(train_fold, test_fold)

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

        oof_df = pd.concat(oof_rows, ignore_index=True) if oof_rows else None

        return BacktestResults(
            model_name=model_name, from_date=from_date, to_date=to_date,
            fold_results=fold_results, aggregate=aggregate,
            integrity_passed=integrity["passed"], integrity_detail=integrity["detail"],
            oof_df=oof_df,
        )
