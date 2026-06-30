"""
systems/ml_signal_engine/models/exit/exit_signal.py

Phase: 1.6 (Exit Signal + First Backtest)
Specs: SPEC-MODEL-002, SPEC-SOLID-003, SPEC-SOLID-004
Owner: ml_signal_engine / exit
Consumers: backtest/portfolio.py (PortfolioSimulator), backtest/engine.py

M-07: Exit Signal Model. LightGBM regression for urgency (0-100), a
second LightGBM classifier for exit TYPE (6 categories), and a Cox
Proportional Hazards model (lifelines) for survival probability — "is
this position still profitable" — at 5d/21d/63d.

ALWAYS surfaces a valid, non-null exit_type — per the build prompt,
"bare sell without type is a BUILD FAILURE". A spiking pnd_score
(> PND_EXIT_SCORE_THRESHOLD) force-overrides both exit_type='pnd_exit'
and a high urgency floor, regardless of what the ML classifier would
otherwise say — the same "P&D pre-filter takes priority" safety framing
as SPEC-MODEL-006's hard block on buy signals, applied here to exits.

[AS BUILT] Implements contracts.interfaces.ISurvivalModel (train/predict/
save/load/metadata + predict_survival(X, time_horizon_days)) rather than
02_models.md's undefined "BaseModel" — ISurvivalModel is explicitly
"For predicting how long a position remains profitable / when to exit",
an exact match for this model's purpose, already defined in this
codebase (SPEC-SOLID-004). IModel.train(X, y, sample_weight=None) only
has room for one target; train_full(X, urgency, exit_type, duration,
event) is the real 3-target entry point (urgency regression + type
classification + Cox survival fit), same reconciliation pattern as every
other model in Phase 1 (HMM, P&D, signal models).

No historical archive of confirmed exit outcomes exists yet — paper
trading has not accumulated enough closed positions (see BuildLog.md
"Real data sourcing — Exit Signal"). There is no synthetic-data fallback:
load_exit_training_data_from_db() below is the only supported data
source, and it raises rather than fabricating positions when fewer than
MIN_CLOSED_POSITIONS real closed trades are available in the paper-trading
log (scripts/paper_trading_tracker.py). Training this model is BLOCKED
until that real history accumulates.
"""

import logging
from typing import Any, Dict, List, Optional

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from lifelines import CoxPHFitter
from sklearn.impute import SimpleImputer

from contracts.interfaces import ISurvivalModel

logger = logging.getLogger(__name__)

EXIT_TYPES = [
    "thesis_broken",
    "momentum_exhaustion",
    "risk_management",
    "target_achieved",
    "opportunity_cost",
    "pnd_exit",
]

SURVIVAL_HORIZONS = (5, 21, 63)

POSITION_FEATURE_COLUMNS = ["entry_price", "days_held", "unrealised_pnl_pct", "days_to_next_earnings"]

# Build prompt: "exit type 'pnd_exit' fires when pnd_score spikes above 50 mid-position".
PND_EXIT_SCORE_THRESHOLD = 50.0
# A P&D-triggered exit is always urgent regardless of what the urgency regressor says.
PND_EXIT_URGENCY_FLOOR = 85.0


class ExitSignalModel(ISurvivalModel):
    """M-07: urgency (LightGBM regression) + exit type (LightGBM classification) + survival (CoxPH)."""

    def __init__(self, random_state: int = 42, cox_penalizer: float = 0.1) -> None:
        self.random_state = random_state
        self.cox_penalizer = cox_penalizer
        self._urgency_params: Dict[str, Any] = {
            "n_estimators": 200, "max_depth": 5, "learning_rate": 0.05,
            "random_state": random_state, "verbose": -1,
        }
        self._type_params: Dict[str, Any] = dict(self._urgency_params)

        self._urgency_model: Optional[lgb.LGBMRegressor] = None
        self._type_model: Optional[lgb.LGBMClassifier] = None
        self._cph: Optional[CoxPHFitter] = None
        self._feature_names: Optional[List[str]] = None
        self._imputer: Optional[SimpleImputer] = None
        self._trained_at = None
        self._training_samples: Optional[int] = None

    # ===== NaN handling (same pattern as base_signal_model.py / meta_labeler.py) =====
    def _impute_fit(self, X: pd.DataFrame) -> pd.DataFrame:
        self._imputer = SimpleImputer(strategy="median", keep_empty_features=True)
        imputed = self._imputer.fit_transform(X)
        return pd.DataFrame(imputed, columns=X.columns, index=X.index)

    def _impute_transform(self, X: pd.DataFrame) -> pd.DataFrame:
        if self._imputer is None:
            raise RuntimeError("predict called before train()/train_full()")
        imputed = self._imputer.transform(X)
        return pd.DataFrame(imputed, columns=X.columns, index=X.index)

    # ===== IModel / ISurvivalModel =====
    def train(self, X: pd.DataFrame, y: pd.Series, sample_weight: Optional[pd.Series] = None) -> None:
        """
        IModel-compliant simple fit: urgency regression only (the
        "primary" half of M-07's algorithm per 02_models.md). No exit-type
        classifier, no CoxPH survival fit — use train_full() for the
        complete pipeline this model is actually meant to run.

        Parameters
        ----------
        X : pd.DataFrame
        y : pd.Series
            Urgency target, 0-100.

        Raises
        ------
        ValueError
            If X/y shapes mismatch or no rows have a non-NaN target.
        """
        if len(X) != len(y):
            raise ValueError(f"X has {len(X)} rows, y has {len(y)} rows")
        valid = y.notna()
        if not valid.any():
            raise ValueError("no rows with a non-NaN urgency target")

        self._feature_names = list(X.columns)
        X_imputed = self._impute_fit(X.loc[valid, self._feature_names])
        self._urgency_model = lgb.LGBMRegressor(**self._urgency_params)
        self._urgency_model.fit(X_imputed, y.loc[valid])
        self._trained_at = pd.Timestamp.now()
        self._training_samples = len(X_imputed)

    def train_full(
        self,
        X: pd.DataFrame,
        urgency: pd.Series,
        exit_type: pd.Series,
        duration: pd.Series,
        event: pd.Series,
    ) -> Dict[str, Any]:
        """
        Full M-07 pipeline: urgency regressor + exit-type classifier +
        Cox Proportional Hazards survival fit.

        Parameters
        ----------
        X : pd.DataFrame
            Phase 1 features + POSITION_FEATURE_COLUMNS (entry_price,
            days_held, unrealised_pnl_pct, days_to_next_earnings) and any
            other position-specific columns (peak_price_since_entry,
            drawdown_from_peak, pnd_score, etc.) the caller has available.
        urgency : pd.Series
            0-100 target.
        exit_type : pd.Series
            Values in EXIT_TYPES.
        duration : pd.Series
            Days-to-event for the Cox fit (e.g. days_held at the time the
            position resolved). Must be > 0 — CoxPH requires positive
            durations; values <= 0 are clipped to 0.5 with a warning.
        event : pd.Series
            1 if the position "died" (went/stayed net-unprofitable) by
            `duration`, 0 if censored (still open/profitable when observed).

        Returns
        -------
        dict
            Diagnostics: training_samples, exit_type_distribution,
            event_rate.

        Spec References
        ----------------
        02_models.md M-07: "CoxPHFitter(penalizer=0.1)... duration_col=
        'days_held', event_col='position_gone_negative'" — survival
        probability is therefore "probability NOT yet gone negative",
        i.e. "still profitable", matching predict_full's exit_survival_*
        columns directly.

        Raises
        ------
        ValueError
            If inputs are misaligned/empty, or exit_type has values
            outside EXIT_TYPES.
        """
        lengths = {len(X), len(urgency), len(exit_type), len(duration), len(event)}
        if len(lengths) != 1:
            raise ValueError("X/urgency/exit_type/duration/event must all be the same length")

        valid = urgency.notna() & exit_type.notna() & duration.notna() & event.notna()
        if not valid.any():
            raise ValueError("no rows with complete (non-NaN) urgency/exit_type/duration/event")

        bad_types = set(exit_type.loc[valid].unique()) - set(EXIT_TYPES)
        if bad_types:
            raise ValueError(f"exit_type contains values outside EXIT_TYPES: {bad_types}")

        self._feature_names = list(X.columns)
        X_imputed = self._impute_fit(X.loc[valid, self._feature_names])
        urgency_clean = urgency.loc[valid]
        type_clean = exit_type.loc[valid]
        duration_clean = duration.loc[valid].clip(lower=0.5)
        event_clean = event.loc[valid].astype(int)

        self._urgency_model = lgb.LGBMRegressor(**self._urgency_params)
        self._urgency_model.fit(X_imputed, urgency_clean)

        self._type_model = lgb.LGBMClassifier(**self._type_params)
        self._type_model.fit(X_imputed, type_clean)

        cox_df = X_imputed.copy()
        cox_df["_duration"] = duration_clean.to_numpy()
        cox_df["_event"] = event_clean.to_numpy()
        self._cph = CoxPHFitter(penalizer=self.cox_penalizer)
        self._cph.fit(cox_df, duration_col="_duration", event_col="_event")

        self._trained_at = pd.Timestamp.now()
        self._training_samples = len(X_imputed)

        return {
            "training_samples": self._training_samples,
            "exit_type_distribution": type_clean.value_counts(normalize=True).to_dict(),
            "event_rate": float(event_clean.mean()),
        }

    def predict(self, X: pd.DataFrame) -> pd.Series:
        """
        IModel: urgency score per row (the primary regression target).
        Works after either train() (urgency-only) or train_full() — unlike
        predict_full(), this does not require the type classifier or CoxPH
        survival model to be fit.
        """
        if self._urgency_model is None:
            raise RuntimeError("predict called before train()/train_full()")
        X_imputed = self._impute_transform(X[self._feature_names])
        urgency = pd.Series(self._urgency_model.predict(X_imputed), index=X.index).clip(0, 100)
        return urgency.rename(None)

    def predict_survival(self, X: pd.DataFrame, time_horizon_days: int = 20) -> pd.DataFrame:
        """
        ISurvivalModel contract: day-by-day survival probability.

        Returns
        -------
        pd.DataFrame
            Shape (n_samples, time_horizon_days), columns 'day_1' ..
            'day_{time_horizon_days}', each cell = P(still profitable at
            that day).
        """
        if self._cph is None:
            raise RuntimeError("predict_survival called before train_full()")
        X_imputed = self._impute_transform(X[self._feature_names])
        times = list(range(1, time_horizon_days + 1))
        sf = self._cph.predict_survival_function(X_imputed, times=times)
        sf = sf.T
        sf.columns = [f"day_{t}" for t in times]
        sf.index = X.index
        return sf

    def save(self, path: str) -> None:
        if self._urgency_model is None:
            raise RuntimeError("save called before train()/train_full()")
        joblib.dump(
            {
                "urgency_model": self._urgency_model, "type_model": self._type_model, "cph": self._cph,
                "feature_names": self._feature_names, "imputer": self._imputer,
                "random_state": self.random_state, "cox_penalizer": self.cox_penalizer,
                "trained_at": self._trained_at, "training_samples": self._training_samples,
            },
            path,
        )

    def load(self, path: str) -> None:
        payload = joblib.load(path)
        self._urgency_model = payload["urgency_model"]
        self._type_model = payload["type_model"]
        self._cph = payload["cph"]
        self._feature_names = payload["feature_names"]
        self._imputer = payload["imputer"]
        self.random_state = payload["random_state"]
        self.cox_penalizer = payload["cox_penalizer"]
        self._trained_at = payload["trained_at"]
        self._training_samples = payload["training_samples"]

    def metadata(self) -> Dict[str, Any]:
        return {
            "name": "ExitSignalModel",
            "version": "1.6.0",
            "created_at": self._trained_at,
            "features_count": len(self._feature_names) if self._feature_names else 0,
            "training_samples": self._training_samples,
            "exit_types": EXIT_TYPES,
        }

    def predict_full(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        The build prompt's required output contract — SPEC-SOLID-003.

        Parameters
        ----------
        X : pd.DataFrame
            Same feature set as train_full's X. If a 'pnd_score' column
            is present and any row exceeds PND_EXIT_SCORE_THRESHOLD, that
            row's exit_type is force-set to 'pnd_exit' and exit_urgency
            floored at PND_EXIT_URGENCY_FLOOR, overriding the ML models.

        Returns
        -------
        pd.DataFrame
            Columns: exit_urgency (float, 0-100), exit_type (str, always
            one of EXIT_TYPES — never null/NaN), exit_survival_5d,
            exit_survival_21d, exit_survival_63d (float, probability
            still profitable at that horizon).

        Spec References
        ----------------
        Build prompt: "ALWAYS surface exit type to user — bare 'sell'
        without type is a BUILD FAILURE" — enforced by an assertion below,
        not just documentation.

        Raises
        ------
        RuntimeError
            If called before train()/train_full() (no urgency/type/cph models fit yet).
        """
        if self._urgency_model is None or self._type_model is None or self._cph is None:
            raise RuntimeError("predict_full called before train_full() (urgency-only train() is insufficient)")

        X_aligned = X[self._feature_names]
        X_imputed = self._impute_transform(X_aligned)

        urgency = pd.Series(self._urgency_model.predict(X_imputed), index=X.index).clip(0, 100)
        exit_type = pd.Series(self._type_model.predict(X_imputed), index=X.index).astype(str)

        survival = self._cph.predict_survival_function(X_imputed, times=list(SURVIVAL_HORIZONS))
        survival = survival.T
        survival.index = X.index

        out = pd.DataFrame(index=X.index)
        out["exit_urgency"] = urgency
        out["exit_type"] = exit_type
        out["exit_survival_5d"] = survival[SURVIVAL_HORIZONS[0]].to_numpy()
        out["exit_survival_21d"] = survival[SURVIVAL_HORIZONS[1]].to_numpy()
        out["exit_survival_63d"] = survival[SURVIVAL_HORIZONS[2]].to_numpy()

        if "pnd_score" in X.columns:
            pnd_triggered = X["pnd_score"] > PND_EXIT_SCORE_THRESHOLD
            out.loc[pnd_triggered, "exit_type"] = "pnd_exit"
            out.loc[pnd_triggered, "exit_urgency"] = np.maximum(
                out.loc[pnd_triggered, "exit_urgency"].to_numpy(), PND_EXIT_URGENCY_FLOOR
            )

        assert out["exit_type"].isin(EXIT_TYPES).all() and out["exit_type"].notna().all(), (
            "exit_type must always be a valid, non-null EXIT_TYPES category (build prompt: "
            "'bare sell without type is a BUILD FAILURE')"
        )

        return out[["exit_urgency", "exit_type", "exit_survival_5d", "exit_survival_21d", "exit_survival_63d"]]


MIN_CLOSED_POSITIONS = 200  # below this, urgency/type/CoxPH fits are too noisy to trust


def load_exit_training_data_from_db(logs_dir=None, min_closed_positions: int = MIN_CLOSED_POSITIONS) -> tuple:
    """
    Build a real (X, urgency, exit_type, duration, event) training set from
    closed paper-trading positions logged by scripts/paper_trading_tracker.py.

    There is no synthetic fallback. If fewer than `min_closed_positions`
    real closed trades exist, this raises — the exit model cannot be
    trained until paper trading has accumulated enough real outcomes.
    See BuildLog.md "Real data sourcing — Exit Signal".

    Parameters
    ----------
    logs_dir : Path, optional
        Defaults to scripts/paper_trading_tracker.py's PaperTradingTracker
        default ("./paper_trading/executions").
    min_closed_positions : int
        Minimum number of closed (exit_price populated) trades required.

    Returns
    -------
    (X, urgency, exit_type, duration, event)
        Same shapes as train_full() expects.

    Raises
    ------
    RuntimeError
        If fewer than `min_closed_positions` real closed trades are found.
    """
    from pathlib import Path as _Path

    logs_dir = _Path(logs_dir) if logs_dir else _Path("./paper_trading/executions")
    closed_trades = []
    if logs_dir.exists():
        for log_file in sorted(logs_dir.glob("*.csv")):
            df = pd.read_csv(log_file)
            df = df[df["exit_price"].notna() & (df["exit_price"] != "")]
            closed_trades.append(df)

    n_closed = sum(len(df) for df in closed_trades)
    if n_closed < min_closed_positions:
        raise RuntimeError(
            f"Only {n_closed} closed paper-trading positions found in {logs_dir} — "
            f"need at least {min_closed_positions} real closed trades to train "
            "ExitSignalModel. There is no synthetic-data fallback. Continue running "
            "scripts/paper_trading_tracker.py paper trading until enough closed "
            "positions accumulate. See BuildLog.md 'Real data sourcing — Exit Signal'."
        )

    trades = pd.concat(closed_trades, ignore_index=True)
    trades["entry_price"] = trades["entry_price"].astype(float)
    trades["exit_price"] = trades["exit_price"].astype(float)
    trades["pnl_pct"] = trades["pnl_pct"].astype(float)
    trades["entry_date"] = pd.to_datetime(trades["date"])
    trades["exit_date"] = pd.to_datetime(trades.get("exit_time", trades["date"]))
    trades["days_held"] = (trades["exit_date"] - trades["entry_date"]).dt.days.clip(lower=1)

    X = trades[["entry_price", "days_held"]].copy()
    X["unrealised_pnl_pct"] = trades["pnl_pct"]
    X["days_to_next_earnings"] = np.nan  # joined at scoring time from a real earnings calendar

    exit_type = np.where(trades["pnl_pct"] > 0.25, "target_achieved", "thesis_broken")
    exit_type = pd.Series(exit_type, index=trades.index)

    urgency = pd.Series(np.clip(50 + trades["pnl_pct"] * 100, 0, 100), index=trades.index)
    event = (trades["pnl_pct"] < 0).astype(int)
    duration = trades["days_held"].astype(float)

    return X, urgency, exit_type, duration, event
