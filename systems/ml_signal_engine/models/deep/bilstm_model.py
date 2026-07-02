"""
systems/ml_signal_engine/models/deep/bilstm_model.py

Phase: 3.2 (Deep Learning Signal Models)
Specs: SPEC-MODEL-010, SPEC-MODEL-003, SPEC-MODEL-005, SPEC-SOLID-003
Owner: ml_signal_engine / deep
Consumers: systems/ml_signal_engine/models/deep/stacking.py,
           systems/ml_signal_engine/inference/daily_inference.py

M-12: Bidirectional LSTM + optional Mamba-2 signal model.

Architecture:
  InputNorm  →  [BiLSTM × 2 layers, hidden=128, dropout=0.3]
             →  Mamba2 layer (if mamba-ssm >= 2.0 available on Linux)
                OR TemporalAttention layer (fallback on Windows or without mamba-ssm)
             →  Three quantile output heads (Q10 / Q50 / Q90)

Mamba-2 notes:
  mamba-ssm is Linux-only (CUDA-compiled CUDA kernels; Windows requires WSL2
  or the `mambular` pure-Python alternative). Use Mamba-2 specifically — not
  Mamba-1 (slower recurrence) and not Mamba-3 (research-only, unstable API).
  Ubuntu 22.04 LTS with the project's .venv is the primary target platform.

  The import is guarded: if mamba-ssm is absent or errors on import, the
  model silently falls back to a Temporal Attention layer with the same
  input/output shape, no behaviour change needed at call sites.

Torch/mamba import is optional: the module imports cleanly without either
library. All methods raise RuntimeError with install instructions when
`TORCH_AVAILABLE` is False.

PIT Assumptions
---------------
Same as tft_model.py — sequences must be formed from features known at the
start of each lookback window, no future leakage.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np

try:
    import torch
    import torch.nn as nn
    import torch.nn.functional as F
    from torch.utils.data import DataLoader, TensorDataset

    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

from contracts.interfaces import IClassificationModel, IExplainableModel
from systems.ml_signal_engine.models.deep.tft_model import (
    N_QUANTILES,
    QUANTILE_LEVELS,
    SEQ_LEN,
    _BUY_THRESHOLD,
    _FULL_RECENT_FILES,
    _FULL_TRAIN_FILES,
    _FULL_VAL_FILES,
    _MAX_TRAIN_SEQ,
    _MAX_VAL_SEQ,
    _MIN_FOLD_FILES,
    _QUICK_FILES,
    _QUICK_MAX_SEQ,
    _SELL_THRESHOLD,
    _quantiles_to_proba,
    _require_torch,
    _stream_sequences_from_files,
)

logger = logging.getLogger(__name__)

# ── Architecture hyperparameters (from build spec) ────────────────────────────

_BILSTM_HIDDEN: int = 128
_BILSTM_LAYERS: int = 2
_BILSTM_DROPOUT: float = 0.3
_ATTN_HEADS: int = 4

# Mamba-2 state expansion factor (d_state in the Mamba-2 SSM)
_MAMBA_D_STATE: int = 16
_MAMBA_D_CONV: int = 4
_MAMBA_EXPAND: int = 2


def _try_import_mamba() -> bool:
    """
    Attempt to import mamba_ssm.modules.mamba2.Mamba2.

    Returns True if Mamba-2 (not Mamba-1) is available.
    Linux + CUDA only — silently returns False on Windows/macOS or when the
    package is not installed. This is not an error condition; the BiLSTM
    falls back to a Temporal Attention layer automatically.
    """
    try:
        from mamba_ssm.modules.mamba2 import Mamba2  # noqa: F401
        return True
    except (ImportError, ModuleNotFoundError):
        return False
    except Exception:
        # Mamba-2 can raise CUDA-related errors at import time on CPU-only builds
        return False


MAMBA_AVAILABLE: bool = _try_import_mamba()


# ── Neural network components ─────────────────────────────────────────────────


if TORCH_AVAILABLE:

    class TemporalAttention(nn.Module):
        """
        Multi-head temporal self-attention (Mamba-2 fallback).

        Designed to process sequences of shape (batch, seq, d_model) with the
        same input/output contract as the Mamba-2 SSM module, so no change is
        needed at the call site.
        """

        def __init__(self, d_model: int, n_heads: int = _ATTN_HEADS, dropout: float = 0.1) -> None:
            super().__init__()
            self.attn = nn.MultiheadAttention(d_model, n_heads, dropout=dropout, batch_first=True)
            self.layer_norm = nn.LayerNorm(d_model)
            self.dropout = nn.Dropout(dropout)
            self._last_weights: Optional["torch.Tensor"] = None

        def forward(self, x: "torch.Tensor") -> "torch.Tensor":
            out, weights = self.attn(x, x, x, need_weights=True, average_attn_weights=True)
            self._last_weights = weights.detach()   # (batch, seq, seq)
            return self.layer_norm(x + self.dropout(out))

        @property
        def last_attention_weights(self) -> Optional["torch.Tensor"]:
            return self._last_weights

    class _BiLSTMCore(nn.Module):
        """
        BiLSTM + Mamba-2 (or TemporalAttention fallback) neural network.

        Forward: (batch, seq_len, n_features) → (batch, n_quantiles)
        """

        def __init__(
            self,
            n_features: int,
            hidden_dim: int = _BILSTM_HIDDEN,
            lstm_layers: int = _BILSTM_LAYERS,
            dropout: float = _BILSTM_DROPOUT,
            n_quantiles: int = N_QUANTILES,
            use_mamba: bool = False,
        ) -> None:
            super().__init__()
            self.input_norm = nn.LayerNorm(n_features)
            self.input_proj = nn.Linear(n_features, hidden_dim)

            self.bilstm = nn.LSTM(
                hidden_dim,
                hidden_dim // 2,            # each direction produces hidden/2 → concat → hidden
                num_layers=lstm_layers,
                batch_first=True,
                bidirectional=True,
                dropout=dropout if lstm_layers > 1 else 0.0,
            )
            self.bilstm_norm = nn.LayerNorm(hidden_dim)

            # Mamba-2 or attention bridge
            self.use_mamba = use_mamba and MAMBA_AVAILABLE
            if self.use_mamba:
                try:
                    from mamba_ssm.modules.mamba2 import Mamba2
                    self.ssm = Mamba2(
                        d_model=hidden_dim,
                        d_state=_MAMBA_D_STATE,
                        d_conv=_MAMBA_D_CONV,
                        expand=_MAMBA_EXPAND,
                    )
                    logger.info("BiLSTM: using Mamba-2 SSM layer")
                except Exception as exc:
                    logger.warning(f"Mamba-2 init failed ({exc}); falling back to TemporalAttention")
                    self.use_mamba = False
                    self.ssm = TemporalAttention(hidden_dim, _ATTN_HEADS, dropout=0.1)
            else:
                self.ssm = TemporalAttention(hidden_dim, _ATTN_HEADS, dropout=0.1)

            # Output projection from last timestep
            self.pre_output_norm = nn.LayerNorm(hidden_dim)
            self.dropout_out = nn.Dropout(dropout)

            # Quantile heads
            self.q_heads = nn.ModuleList([
                nn.Sequential(
                    nn.Linear(hidden_dim, hidden_dim // 2),
                    nn.ELU(),
                    nn.Linear(hidden_dim // 2, 1),
                )
                for _ in range(n_quantiles)
            ])

        def forward(self, x: "torch.Tensor") -> "torch.Tensor":
            """
            Parameters
            ----------
            x : Tensor (batch, seq, n_features)

            Returns
            -------
            Tensor (batch, n_quantiles)
            """
            # Normalise + project input
            h = self.input_norm(x)
            h = F.elu(self.input_proj(h))                       # (batch, seq, hidden)

            # BiLSTM
            lstm_out, _ = self.bilstm(h)                        # (batch, seq, hidden)
            lstm_out = self.bilstm_norm(lstm_out)

            # Mamba-2 or attention
            ssm_out = self.ssm(lstm_out)                        # (batch, seq, hidden)

            # Decode from last timestep
            last = self.pre_output_norm(ssm_out[:, -1, :])      # (batch, hidden)
            last = self.dropout_out(last)

            qs = [head(last) for head in self.q_heads]          # list of (batch, 1)
            return torch.cat(qs, dim=-1)                        # (batch, n_quantiles)

        @property
        def last_attention_weights(self) -> Optional["torch.Tensor"]:
            """Return attention weights if using TemporalAttention layer."""
            if not self.use_mamba and hasattr(self.ssm, "last_attention_weights"):
                return self.ssm.last_attention_weights
            return None


# ── Main model class ─────────────────────────────────────────────────────────


class BiLSTMSignalModel(IClassificationModel, IExplainableModel):
    """
    M-12: Bidirectional LSTM (+ optional Mamba-2) deep ensemble signal model.

    Input format
    ------------
    X : np.ndarray, shape (n_samples, seq_len=63, n_features=330)
    y : np.ndarray, shape (n_samples,) — forward returns (float)

    Output
    ------
    predict_quantiles(X) → (n_samples, 3)  : Q10 / Q50 / Q90
    predict_proba(X)     → (n_samples, 3)  : P(Sell) / P(Hold) / P(Buy)
    predict(X)           → (n_samples,)    : argmax class index

    Spec References
    ---------------
    SPEC-MODEL-003: train/predict/save/load interface.
    SPEC-MODEL-005: versioned model file naming.
    SPEC-SOLID-003: IClassificationModel + IExplainableModel implementation.
    """

    MODEL_NAME = "bilstm_signal"

    def __init__(
        self,
        n_features: int = 330,
        seq_len: int = SEQ_LEN,
        hidden_dim: int = _BILSTM_HIDDEN,
        lstm_layers: int = _BILSTM_LAYERS,
        dropout: float = _BILSTM_DROPOUT,
        batch_size: int = 256,
        learning_rate: float = 5e-4,
        epochs: int = 50,
        early_stopping_patience: int = 10,
        use_mamba: bool = True,
        quick: bool = False,
    ) -> None:
        _require_torch()
        self.n_features = n_features
        self.seq_len = seq_len
        self.hidden_dim = hidden_dim
        self.lstm_layers = lstm_layers
        self.dropout = dropout
        self.batch_size = batch_size
        self.learning_rate = learning_rate
        self.epochs = 2 if quick else epochs
        self.early_stopping_patience = early_stopping_patience
        self.use_mamba = use_mamba
        self.quick = quick

        self._model: Optional["_BiLSTMCore"] = None
        self._device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self._training_samples: int = 0
        self._best_val_loss: float = float("inf")
        self._created_at: Optional[str] = None
        self._version: str = datetime.utcnow().strftime("%Y%m%d")
        self._mamba_used: bool = False

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _to_tensor(self, X: Any) -> "torch.Tensor":
        import pandas as pd
        if isinstance(X, pd.DataFrame):
            X = X.values
        X = np.asarray(X, dtype=np.float32)
        if X.ndim == 2:
            X = X.reshape(X.shape[0], self.seq_len, self.n_features)
        if X.shape[1] != self.seq_len or X.shape[2] != self.n_features:
            raise ValueError(
                f"Expected input shape (n, {self.seq_len}, {self.n_features}); "
                f"got {X.shape}"
            )
        X = np.nan_to_num(X, nan=0.0)
        return torch.tensor(X, dtype=torch.float32, device=self._device)

    @staticmethod
    def _pinball_loss(pred: "torch.Tensor", target: "torch.Tensor", q: float) -> "torch.Tensor":
        err = target - pred
        return torch.mean(torch.maximum(q * err, (q - 1.0) * err))

    def _combined_loss(self, preds: "torch.Tensor", targets: "torch.Tensor") -> "torch.Tensor":
        total = torch.tensor(0.0, device=self._device)
        for i, q in enumerate(QUANTILE_LEVELS):
            total = total + self._pinball_loss(preds[:, i], targets, q)
        return total

    def _build_model(self) -> "_BiLSTMCore":
        return _BiLSTMCore(
            n_features=self.n_features,
            hidden_dim=self.hidden_dim,
            lstm_layers=self.lstm_layers,
            dropout=self.dropout,
            n_quantiles=N_QUANTILES,
            use_mamba=self.use_mamba,
        ).to(self._device)

    # ── IModel interface ──────────────────────────────────────────────────────

    def train(
        self,
        X_train: Any,
        y_train: Any,
        X_val: Any,
        y_val: Any,
        sample_weight: Optional[np.ndarray] = None,
    ) -> None:
        """
        Train BiLSTM on time-series sequences.

        Parameters
        ----------
        X_train : ndarray (n_train, seq_len, n_features)
        y_train : ndarray (n_train,) — forward returns
        X_val   : ndarray (n_val, seq_len, n_features)
        y_val   : ndarray (n_val,)

        Spec References
        ---------------
        SPEC-MODEL-003: walk-forward training.
        SPEC-MODEL-004: early stopping, best checkpoint saved.
        """
        _require_torch()
        X_tr = self._to_tensor(X_train)
        y_tr = torch.tensor(np.asarray(y_train, dtype=np.float32), device=self._device)
        X_v = self._to_tensor(X_val)
        y_v = torch.tensor(np.asarray(y_val, dtype=np.float32), device=self._device)

        if self.quick:
            X_tr, y_tr = X_tr[:50], y_tr[:50]
            X_v, y_v = X_v[:50], y_v[:50]

        self._training_samples = len(X_tr)
        self._model = self._build_model()
        self._mamba_used = getattr(self._model, "use_mamba", False)
        optimiser = torch.optim.Adam(self._model.parameters(), lr=self.learning_rate)
        scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
            optimiser, mode="min", factor=0.5, patience=5
        )

        train_ds = TensorDataset(X_tr, y_tr)
        train_loader = DataLoader(train_ds, batch_size=self.batch_size, shuffle=True)

        best_weights = None
        no_improve = 0

        for epoch in range(1, self.epochs + 1):
            self._model.train()
            train_loss = 0.0
            for xb, yb in train_loader:
                optimiser.zero_grad()
                preds = self._model(xb)
                loss = self._combined_loss(preds, yb)
                loss.backward()
                nn.utils.clip_grad_norm_(self._model.parameters(), max_norm=1.0)
                optimiser.step()
                train_loss += loss.item() * len(xb)
            train_loss /= self._training_samples

            self._model.eval()
            with torch.no_grad():
                val_preds = self._model(X_v)
                val_loss = self._combined_loss(val_preds, y_v).item()

            scheduler.step(val_loss)
            logger.info(
                f"BiLSTM epoch {epoch}/{self.epochs} "
                f"train={train_loss:.4f} val={val_loss:.4f} "
                f"lr={optimiser.param_groups[0]['lr']:.2e}"
            )

            if val_loss < self._best_val_loss - 1e-6:
                self._best_val_loss = val_loss
                best_weights = {k: v.clone() for k, v in self._model.state_dict().items()}
                no_improve = 0
            else:
                no_improve += 1
                if no_improve >= self.early_stopping_patience:
                    logger.info(f"BiLSTM early stop at epoch {epoch}")
                    break

        if best_weights is not None:
            self._model.load_state_dict(best_weights)
        self._created_at = datetime.utcnow().isoformat()
        logger.info(
            f"BiLSTM training complete. mamba={'yes' if self._mamba_used else 'no (attention fallback)'} "
            f"best_val_loss={self._best_val_loss:.4f}"
        )

    def predict(self, X: Any) -> np.ndarray:
        return self.predict_proba(X).argmax(axis=1)

    def predict_proba(self, X: Any) -> np.ndarray:
        """Return (n_samples, 3) class probabilities [P(Sell), P(Hold), P(Buy)]."""
        quantiles = self.predict_quantiles(X)
        return _quantiles_to_proba(quantiles, _BUY_THRESHOLD, _SELL_THRESHOLD)

    def predict_quantiles(self, X: Any) -> np.ndarray:
        """Return (n_samples, 3) quantile forecasts [Q10, Q50, Q90]."""
        _require_torch()
        if self._model is None:
            raise RuntimeError("Model not trained. Call train() first.")
        self._model.eval()
        X_t = self._to_tensor(X)
        with torch.no_grad():
            preds = self._model(X_t)
        return preds.cpu().numpy()

    def save(self, path: str) -> None:
        """Save model weights + hyperparameters (SPEC-MODEL-005)."""
        _require_torch()
        if self._model is None:
            raise RuntimeError("Nothing to save — model has not been trained.")
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        torch.save(self._model.state_dict(), str(p) + ".pt")
        with open(str(p) + ".json", "w") as f:
            json.dump(self.metadata(), f, indent=2)
        logger.info(f"BiLSTMSignalModel saved to {p}.pt / {p}.json")

    def load(self, path: str) -> None:
        """Load model weights from {path}.pt and hyperparams from {path}.json."""
        _require_torch()
        p = Path(path)
        json_path = str(p) + ".json"
        if Path(json_path).exists():
            with open(json_path) as f:
                meta = json.load(f)
            hp = meta.get("hyperparams", {})
            self.n_features = hp.get("n_features", self.n_features)
            self.seq_len = hp.get("seq_len", self.seq_len)
            self.hidden_dim = hp.get("hidden_dim", self.hidden_dim)
            self.lstm_layers = hp.get("lstm_layers", self.lstm_layers)
            self.dropout = hp.get("dropout", self.dropout)
            self.use_mamba = hp.get("use_mamba", self.use_mamba)

        self._model = self._build_model()
        weights = torch.load(str(p) + ".pt", map_location=self._device)
        self._model.load_state_dict(weights)
        self._model.eval()
        logger.info(f"BiLSTMSignalModel loaded from {p}.pt")

    def metadata(self) -> Dict[str, Any]:
        return {
            "name": self.MODEL_NAME,
            "version": self._version,
            "created_at": self._created_at,
            "training_samples": self._training_samples,
            "best_val_loss": self._best_val_loss,
            "mamba_used": self._mamba_used,
            "mamba_available": MAMBA_AVAILABLE,
            "hyperparams": {
                "n_features": self.n_features,
                "seq_len": self.seq_len,
                "hidden_dim": self.hidden_dim,
                "lstm_layers": self.lstm_layers,
                "dropout": self.dropout,
                "batch_size": self.batch_size,
                "learning_rate": self.learning_rate,
                "epochs": self.epochs,
                "early_stopping_patience": self.early_stopping_patience,
                "use_mamba": self.use_mamba,
            },
        }

    # ── IExplainableModel ─────────────────────────────────────────────────────

    def get_shap_values(self, X: Any) -> np.ndarray:
        """
        Proxy SHAP via feature gradient magnitude.

        Computes the mean absolute gradient of the Q50 output w.r.t. each
        input feature, averaged over time. Returns (n_samples, n_features).
        """
        _require_torch()
        if self._model is None:
            raise RuntimeError("Model not trained. Call train() first.")
        self._model.eval()
        X_t = self._to_tensor(X)
        X_t.requires_grad_(True)
        preds = self._model(X_t)
        q50 = preds[:, 1].sum()
        q50.backward()
        grads = X_t.grad.abs().mean(dim=1).detach().cpu().numpy()  # (batch, n_features)
        return grads

    def get_attention_weights(self, X: Any) -> Optional[np.ndarray]:
        """
        Return temporal attention weights from the TemporalAttention layer.

        Returns None when Mamba-2 is active (no attention weights produced).
        Returns (n_samples, seq_len, seq_len) when attention fallback is active.

        Note: attention weights from the last forward pass are cached in the
        TemporalAttention layer. Call predict_quantiles first to populate them.
        """
        _require_torch()
        if self._model is None:
            raise RuntimeError("Model not trained. Call train() first.")
        # Trigger a forward pass to populate cached weights
        _ = self.predict_quantiles(X)
        weights = self._model.last_attention_weights
        if weights is None:
            return None
        return weights.cpu().numpy()

    # ── Naive baseline (validation utility) ──────────────────────────────────

    @staticmethod
    def naive_baseline_loss(y_val: np.ndarray) -> float:
        """
        Pinball loss of a naive model that always predicts the median return.

        Used in integrity checks: BiLSTM val_loss must be lower than this.
        Baseline predicts Q10=Q50=Q90 = median(y_train) for all samples.
        """
        median_pred = float(np.median(y_val))
        total = 0.0
        for q in QUANTILE_LEVELS:
            err = y_val - median_pred
            total += float(np.mean(np.maximum(q * err, (q - 1.0) * err)))
        return total


# ── Overnight training entry point ────────────────────────────────────────────
#
# [AS BUILT, P3.x] This module previously re-exported tft_model.py's
# schedule_overnight_training unchanged, which always instantiates
# TFTSignalModel — calling train_deep_models.py with --model bilstm would
# silently train (and save) a second TFT model mislabeled as bilstm_signal.
# Fixed by giving BiLSTM its own copy of the walk-forward training loop,
# identical to tft_model.py's in structure (same Parquet loading / fold-
# split / sequence-building) but instantiating BiLSTMSignalModel.


def schedule_overnight_training(
    feature_parquet_dir: str,
    model_output_dir: str,
    horizon_days: int = 21,
    n_folds: int = 3,
    quick: bool = False,
) -> None:
    """
    Overnight BiLSTM training run (SPEC-MODEL-003 walk-forward).

    Loads daily Parquet feature files, constructs 63-day sequences,
    builds triple-barrier labels, and trains one BiLSTM (+ optional
    Mamba-2) per fold.

    Parameters
    ----------
    feature_parquet_dir : str
        Path to datastore/features/daily/ directory.
    model_output_dir : str
        Destination for model weights (SPEC-MODEL-005).
    horizon_days : int
        Return label horizon: 5, 21, or 63.
    n_folds : int
        Number of walk-forward folds.
    quick : bool
        If True, uses 2 epochs (CI / smoke-test on real, possibly small,
        Parquet feature data) — there is no synthetic-data mode.

    Estimated runtime
    -----------------
    CPU (Ryzen 5 7535U), 500 stocks, 5yr history, 50 epochs: 4-6 hours
    (same order of magnitude as TFT — see tft_model.py's
    schedule_overnight_training).

    Raises
    ------
    FileNotFoundError
        If `feature_parquet_dir` has no Parquet files. There is no
        synthetic-data fallback — run the daily feature pipeline
        (features/matrix_builder.py) first to populate it.
    """
    _require_torch()
    import gc
    import pandas as pd

    logger.info(
        f"BiLSTM overnight training: horizon={horizon_days}d, folds={n_folds}, quick={quick}"
    )

    parquet_dir = Path(feature_parquet_dir)
    all_files = sorted(parquet_dir.glob("*.parquet")) if parquet_dir.exists() else []
    if not all_files:
        raise FileNotFoundError(
            f"No Parquet files in {parquet_dir}. There is no synthetic-data fallback — "
            "run the daily feature pipeline (features/matrix_builder.py) first to populate "
            "it."
        )

    # Read schema from one file only — no large upfront load.
    sample = pd.read_parquet(all_files[-1])
    _DUCKDB_INTERNAL = {"__fragment_index", "__batch_index", "__last_in_fragment",
                        "__filename"}
    id_cols = {"date", "ticker"}
    feature_cols = [c for c in sample.columns
                    if c not in id_cols and c not in _DUCKDB_INTERNAL]
    n_features = len(feature_cols)
    del sample
    gc.collect()
    logger.info(f"Schema: {n_features} features across {len(all_files)} date files")

    if quick:
        all_files = all_files[-_QUICK_FILES:]
        n_folds = 1
    else:
        all_files = all_files[-_FULL_RECENT_FILES:]

    fold_size = max(_MIN_FOLD_FILES, len(all_files) // (n_folds + 1))
    output_dir = Path(model_output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for fold in range(n_folds):
        train_end = min((fold + 1) * fold_size, len(all_files))
        val_end = min(train_end + fold_size, len(all_files))
        if val_end <= train_end:
            logger.warning(f"Fold {fold}: not enough files for a validation window, stopping")
            break
        train_files = all_files[:train_end][-(_QUICK_FILES if quick else _FULL_TRAIN_FILES):]
        val_files = all_files[train_end:val_end]

        logger.info(f"Fold {fold}: {len(train_files)} train files, {len(val_files)} val files")

        max_tr = _QUICK_MAX_SEQ if quick else _MAX_TRAIN_SEQ
        max_v = _QUICK_MAX_SEQ if quick else _MAX_VAL_SEQ

        val_files_capped = val_files[-(_QUICK_FILES if quick else _FULL_VAL_FILES):]

        X_tr, y_tr = _stream_sequences_from_files(
            train_files, feature_cols, horizon_days, SEQ_LEN, max_samples=max_tr)
        gc.collect()

        val_context = train_files[-SEQ_LEN:]
        X_v, y_v = _stream_sequences_from_files(
            list(val_context) + list(val_files_capped),
            feature_cols, horizon_days, SEQ_LEN, max_samples=max_v, rng_seed=43)
        gc.collect()

        if len(X_tr) == 0 or len(X_v) == 0:
            logger.warning(f"Fold {fold}: insufficient data, skipping")
            continue

        model = BiLSTMSignalModel(n_features=n_features, quick=quick)
        model.train(X_tr, y_tr, X_v, y_v)
        del X_tr, y_tr, X_v, y_v
        gc.collect()

        model_path = output_dir / f"bilstm_signal_{horizon_days}d_v{model._version}_fold{fold}"
        model.save(str(model_path))
        del model
        gc.collect()
        logger.info(f"Fold {fold} model saved to {model_path}.pt")


# ── CLI entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Train BiLSTM deep model (M-12)")
    parser.add_argument("--quick", action="store_true",
                        help="2 epochs × 50 samples — CI smoke-test mode")
    parser.add_argument("--folds", type=int, default=5)
    parser.add_argument("--horizon", type=int, default=21)
    parser.add_argument("--feature-dir", default="datastore/features/daily")
    parser.add_argument("--output-dir", default="datastore/models")
    args = parser.parse_args()

    schedule_overnight_training(
        feature_parquet_dir=args.feature_dir,
        model_output_dir=args.output_dir,
        horizon_days=args.horizon,
        n_folds=args.folds,
        quick=args.quick,
    )
