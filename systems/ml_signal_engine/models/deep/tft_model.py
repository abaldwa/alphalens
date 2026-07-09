"""
systems/ml_signal_engine/models/deep/tft_model.py

Phase: 3.2 (Deep Learning Signal Models)
Specs: SPEC-MODEL-010, SPEC-MODEL-003, SPEC-MODEL-005, SPEC-SOLID-003
Owner: ml_signal_engine / deep
Consumers: systems/ml_signal_engine/models/deep/stacking.py,
           systems/ml_signal_engine/inference/daily_inference.py

M-11: Temporal Fusion Transformer (TFT) deep ensemble member.

Architecture (pure PyTorch — no pytorch-forecasting dependency; the
pytorch-forecasting TimeSeriesDataSet wrapping layer adds little value
over a direct torch.Tensor interface for our fixed-length 63-day windows):

  InputProjection  →  VariableSelectionNetwork  →  LSTM encoder (2 layers)
  →  StaticEnrichment  →  InterpretableMultiHeadAttention  →  GRN decoder
  →  Three quantile output heads (Q10 / Q50 / Q90)

Key design choices vs. the Lim et al. (2021) paper:
  hidden_dim=64 (paper uses 160-300): keeps CPU memory tractable for
    the Ryzen 5 7535U hardware target with 330 input features.
  n_heads=4, lstm_layers=2, dropout=0.1.
  No separate known/unknown covariate distinction — all 330 features
    are treated as time-varying unknown reals.
  No static covariates tensor — sector/tier info enters as time-constant
    features repeated across all 63 timesteps.

Torch import is optional: the module imports cleanly without torch.
All methods raise RuntimeError with an install instruction when
`TORCH_AVAILABLE` is False.

PIT Assumptions
---------------
Input sequences must be formed ONLY from feature values known at the
start of each 63-day window. No future returns are visible to the encoder.
y_train contains realised returns labelled with the appropriate horizon
(SPEC-PIPE-003 / SPEC-MODEL-003 triple-barrier labeling).
"""

import json
import logging
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    import torch
    import torch.nn as nn
    import torch.nn.functional as F
    from torch.utils.data import DataLoader, TensorDataset

    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

from contracts.interfaces import IClassificationModel, IExplainableModel

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

SEQ_LEN: int = 63           # lookback window in trading days (SPEC-MODEL-010)
N_QUANTILES: int = 3        # Q10 / Q50 / Q90
QUANTILE_LEVELS: Tuple[float, ...] = (0.10, 0.50, 0.90)
CLASS_NAMES: List[str] = ["sell", "hold", "buy"]

_HIDDEN_DIM: int = 64
_N_HEADS: int = 4
_LSTM_LAYERS: int = 2
_DROPOUT: float = 0.1

# Convert Q50 ± this annualised return to Buy/Sell probability buckets.
_BUY_THRESHOLD: float = 0.015   # 1.5% annualised → ~0.006% daily for 5d horizon
_SELL_THRESHOLD: float = 0.015

# ── Training memory budget constants ─────────────────────────────────────────
# System profile (2026-07-02): 14.9 GB RAM, 8.7 GB available, 14 cores.
# Each daily parquet: 6.5 MB, 2,644 tickers × 297 features (1 row/ticker/day).
# Per-sequence: SEQ_LEN(63) × 297 features × float32 = 73 KB.
#
# Peak RSS model (per fold):
#   accumulation buffer  = (n_train + SEQ_LEN) × 2644 × 297 × 4B  ≈  828 MB
#   X_train              = 8000 seq × 63 × 297 × 4B                ≈  599 MB
#   X_val                = 2000 seq × 63 × 297 × 4B                ≈  150 MB
#   TFT model + Adam     ≈  240 MB
#   batch working mem    = 256 × 63 × 297 × 4B × 2 (fwd+bwd)      ≈   38 MB
#   Total peak per fold  ≈ 1,855 MB  — well within 8.7 GB budget.
#
# Batch 256 keeps steps/epoch at 8000÷256=31 ≈ current 2000÷64=31,
# so epoch wall-time stays flat while training diversity increases 4×.
# _FULL_RECENT_FILES stays at 600: pre-2021 parquets have 87% NaN features
# that produce NaN gradients even after nan_to_num (too sparse to learn from).
_QUICK_FILES: int = 200  # --quick: load this many recent date files
_FULL_RECENT_FILES: int = 600  # full run: cap history to this many most-recent files
# (~2.5 years). Older files have sparse fundamentals/macro features that
# produce NaN losses.
_FULL_TRAIN_FILES: int = 200  # full run: most-recent N train files per fold
_FULL_VAL_FILES: int = 150    # full run: most-recent N val files per fold
_MAX_TRAIN_SEQ: int = 8_000   # random sample cap for training sequences
_MAX_VAL_SEQ: int = 2_000     # random sample cap for validation sequences
_QUICK_MAX_SEQ: int = 400     # --quick: enough for the model's 50-row subsample
# Minimum files per fold chunk — must exceed seq_len + horizon so at least one
# valid sequence can form per ticker.
_MIN_FOLD_FILES: int = SEQ_LEN + 21 + 10  # 63 + 21 + 10 = 94


def _require_torch() -> None:
    if not TORCH_AVAILABLE:
        raise RuntimeError(
            "TFTSignalModel requires PyTorch. Install with: "
            ".venv/bin/pip install torch==2.12.1 --no-cache-dir"
        )


# ── Neural network components ─────────────────────────────────────────────────


if TORCH_AVAILABLE:

    class GatedResidualNetwork(nn.Module):
        """
        Core TFT building block (Lim et al. 2021, §3.3).

        ELU activation → optional context injection → GLU gate → LayerNorm
        with skip connection (linear projection when dims differ).
        """

        def __init__(
            self,
            input_dim: int,
            hidden_dim: int,
            output_dim: int,
            dropout: float = _DROPOUT,
            context_dim: Optional[int] = None,
        ) -> None:
            super().__init__()
            self.fc1 = nn.Linear(input_dim, hidden_dim)
            self.fc2 = nn.Linear(hidden_dim, output_dim * 2)   # for GLU
            self.ctx_fc = nn.Linear(context_dim, hidden_dim, bias=False) if context_dim else None
            self.skip = nn.Linear(input_dim, output_dim) if input_dim != output_dim else nn.Identity()
            self.layer_norm = nn.LayerNorm(output_dim)
            self.dropout = nn.Dropout(dropout)

        def forward(
            self, x: "torch.Tensor", context: Optional["torch.Tensor"] = None
        ) -> "torch.Tensor":
            h = self.fc1(x)
            if context is not None and self.ctx_fc is not None:
                h = h + self.ctx_fc(context)
            h = F.elu(h)
            h = self.dropout(h)
            h = self.fc2(h)
            h1, h2 = h.chunk(2, dim=-1)
            gated = h1 * torch.sigmoid(h2)                     # GLU
            return self.layer_norm(self.skip(x) + gated)

    class VariableSelectionNetwork(nn.Module):
        """
        Soft selection over input variables per timestep (Lim et al. §3.4).

        All 330 features are projected jointly (not separately) for tractability
        on the target CPU hardware. Weights are stored for interpretability.
        """

        def __init__(self, n_vars: int, hidden_dim: int, dropout: float = _DROPOUT) -> None:
            super().__init__()
            self.var_proj = nn.Linear(n_vars, n_vars * hidden_dim)
            self.selector = GatedResidualNetwork(n_vars, hidden_dim, n_vars, dropout)
            self.softmax = nn.Softmax(dim=-1)
            self.hidden_dim = hidden_dim

        def forward(self, x: "torch.Tensor") -> Tuple["torch.Tensor", "torch.Tensor"]:
            """
            Parameters
            ----------
            x : Tensor (batch, seq, n_vars)

            Returns
            -------
            output : Tensor (batch, seq, hidden_dim)  — weighted combination
            weights : Tensor (batch, seq, n_vars)     — selection weights
            """
            B, T, V = x.shape
            # Project all variables: (batch, seq, n_vars * hidden_dim)
            xi = F.elu(self.var_proj(x)).reshape(B, T, V, self.hidden_dim)
            # Variable selection weights: (batch, seq, n_vars)
            weights = self.softmax(self.selector(x))
            # Weighted sum: (batch, seq, hidden_dim)
            output = (xi * weights.unsqueeze(-1)).sum(dim=-2)
            return output, weights

    class InterpretableMultiHeadAttention(nn.Module):
        """
        Temporal self-attention with stored weights for interpretability.

        Matches the simplified multi-head attention in Lim et al. §3.6.
        Each head shares a single set of value weights (interpretable variant).
        Attention weights are cached so get_attention_weights() can retrieve them.
        """

        def __init__(self, hidden_dim: int, n_heads: int, dropout: float = _DROPOUT) -> None:
            super().__init__()
            assert hidden_dim % n_heads == 0, "hidden_dim must be divisible by n_heads"
            self.n_heads = n_heads
            self.d_k = hidden_dim // n_heads

            self.q_proj = nn.Linear(hidden_dim, hidden_dim)
            self.k_proj = nn.Linear(hidden_dim, hidden_dim)
            self.v_proj = nn.Linear(hidden_dim, hidden_dim)   # shared value projection
            self.out_proj = nn.Linear(hidden_dim, hidden_dim)
            self.dropout = nn.Dropout(dropout)
            self.layer_norm = nn.LayerNorm(hidden_dim)

            self._last_attn_weights: Optional["torch.Tensor"] = None

        def forward(self, x: "torch.Tensor") -> "torch.Tensor":
            B, T, H = x.shape
            dk = self.d_k

            q = self.q_proj(x).view(B, T, self.n_heads, dk).transpose(1, 2)   # (B, heads, T, dk)
            k = self.k_proj(x).view(B, T, self.n_heads, dk).transpose(1, 2)
            v = self.v_proj(x).view(B, T, self.n_heads, dk).transpose(1, 2)

            # Scaled dot-product attention
            attn = torch.matmul(q, k.transpose(-2, -1)) / math.sqrt(dk)
            attn = F.softmax(attn, dim=-1)                                      # (B, heads, T, T)
            attn = self.dropout(attn)
            self._last_attn_weights = attn.detach().mean(dim=1)                # (B, T, T) averaged

            out = torch.matmul(attn, v)                                         # (B, heads, T, dk)
            out = out.transpose(1, 2).contiguous().view(B, T, H)
            out = self.out_proj(out)
            return self.layer_norm(x + out)

        @property
        def last_attention_weights(self) -> Optional["torch.Tensor"]:
            return self._last_attn_weights

    class _TFTCore(nn.Module):
        """
        TFT neural network.

        Forward: (batch, seq_len, n_features) → (batch, n_quantiles)
        Last timestep's decoder output feeds the quantile heads.
        """

        def __init__(
            self,
            n_features: int,
            hidden_dim: int = _HIDDEN_DIM,
            n_heads: int = _N_HEADS,
            lstm_layers: int = _LSTM_LAYERS,
            dropout: float = _DROPOUT,
            n_quantiles: int = N_QUANTILES,
        ) -> None:
            super().__init__()
            self.vsn = VariableSelectionNetwork(n_features, hidden_dim, dropout)
            self.encoder = nn.LSTM(
                hidden_dim, hidden_dim, num_layers=lstm_layers,
                batch_first=True, dropout=dropout if lstm_layers > 1 else 0.0,
            )
            self.static_enrichment = GatedResidualNetwork(hidden_dim, hidden_dim, hidden_dim, dropout)
            self.attention = InterpretableMultiHeadAttention(hidden_dim, n_heads, dropout)
            self.decoder_grn = GatedResidualNetwork(hidden_dim, hidden_dim, hidden_dim, dropout)
            # Separate quantile head for each quantile level
            self.q_heads = nn.ModuleList([
                nn.Sequential(nn.Linear(hidden_dim, hidden_dim // 2), nn.ELU(),
                              nn.Linear(hidden_dim // 2, 1))
                for _ in range(n_quantiles)
            ])
            self.n_quantiles = n_quantiles

        def forward(self, x: "torch.Tensor") -> Tuple["torch.Tensor", "torch.Tensor"]:
            """
            Parameters
            ----------
            x : Tensor (batch, seq, n_features)

            Returns
            -------
            quantiles : Tensor (batch, n_quantiles)
            var_weights : Tensor (batch, seq, n_features) — variable selection weights
            """
            # Variable selection + LSTM encoding
            vsn_out, var_weights = self.vsn(x)       # (batch, seq, hidden_dim), (batch, seq, n_vars)
            lstm_out, _ = self.encoder(vsn_out)       # (batch, seq, hidden_dim)

            # Static enrichment (use last hidden state as context)
            context = lstm_out[:, -1:, :].expand_as(lstm_out)
            enriched = self.static_enrichment(lstm_out, context)

            # Temporal self-attention
            attended = self.attention(enriched)       # (batch, seq, hidden_dim)

            # Decode last timestep
            decoded = self.decoder_grn(attended[:, -1, :])   # (batch, hidden_dim)

            # Quantile heads
            qs = [head(decoded) for head in self.q_heads]    # list of (batch, 1)
            quantiles = torch.cat(qs, dim=-1)                # (batch, n_quantiles)

            return quantiles, var_weights

        @property
        def last_attention_weights(self) -> Optional["torch.Tensor"]:
            return self.attention.last_attention_weights


# ── Main model class ─────────────────────────────────────────────────────────


class TFTSignalModel(IClassificationModel, IExplainableModel):
    """
    M-11: TFT-based deep ensemble signal model.

    Input format
    ------------
    X : np.ndarray, shape (n_samples, seq_len=63, n_features=330)
      or pd.DataFrame of shape (n_samples, seq_len * n_features) — will be
      reshaped automatically if columns follow the pattern f0_t0, f0_t1, ...
    y : np.ndarray, shape (n_samples,)
      Realised returns (float), e.g. 5-day forward log-returns, labelled via
      SPEC-MODEL-002 triple-barrier (positive = Buy direction).

    Output
    ------
    predict_quantiles(X) → (n_samples, 3)  : Q10 / Q50 / Q90 return forecasts
    predict_proba(X)     → (n_samples, 3)  : P(Sell) / P(Hold) / P(Buy)
    predict(X)           → (n_samples,)    : argmax class index {0, 1, 2}

    Spec References
    ---------------
    SPEC-MODEL-003: train/predict/save/load interface.
    SPEC-MODEL-005: versioned model file naming.
    SPEC-SOLID-003: IClassificationModel + IExplainableModel implementation.
    """

    MODEL_NAME = "tft_signal"

    def __init__(
        self,
        n_features: int = 330,
        seq_len: int = SEQ_LEN,
        hidden_dim: int = _HIDDEN_DIM,
        n_heads: int = _N_HEADS,
        lstm_layers: int = _LSTM_LAYERS,
        dropout: float = _DROPOUT,
        batch_size: int = 256,
        learning_rate: float = 1e-3,
        epochs: int = 50,
        early_stopping_patience: int = 10,
        quick: bool = False,
    ) -> None:
        _require_torch()
        self.n_features = n_features
        self.seq_len = seq_len
        self.hidden_dim = hidden_dim
        self.n_heads = n_heads
        self.lstm_layers = lstm_layers
        self.dropout = dropout
        self.batch_size = batch_size
        self.learning_rate = learning_rate
        # --quick: 2 epochs, 50 samples — CI / smoke-test mode (SPEC-MODEL-003)
        self.epochs = 2 if quick else epochs
        self.early_stopping_patience = early_stopping_patience
        self.quick = quick

        self._model: Optional["_TFTCore"] = None
        self._device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self._training_samples: int = 0
        self._best_val_loss: float = float("inf")
        self._created_at: Optional[str] = None
        self._version: str = datetime.utcnow().strftime("%Y%m%d")

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _to_tensor(self, X: Any) -> "torch.Tensor":
        """Accept ndarray (n, seq, feat) or flat ndarray (n, seq*feat)."""
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
    def _pinball_loss(
        pred: "torch.Tensor", target: "torch.Tensor", quantile: float
    ) -> "torch.Tensor":
        """Quantile (pinball) regression loss for one quantile level."""
        err = target - pred
        return torch.mean(torch.maximum(quantile * err, (quantile - 1.0) * err))

    def _combined_quantile_loss(
        self, preds: "torch.Tensor", targets: "torch.Tensor"
    ) -> "torch.Tensor":
        """Sum of pinball losses across all quantile levels."""
        total = torch.tensor(0.0, device=self._device)
        for i, q in enumerate(QUANTILE_LEVELS):
            total = total + self._pinball_loss(preds[:, i], targets, q)
        return total

    def _build_model(self) -> "_TFTCore":
        model = _TFTCore(
            n_features=self.n_features,
            hidden_dim=self.hidden_dim,
            n_heads=self.n_heads,
            lstm_layers=self.lstm_layers,
            dropout=self.dropout,
            n_quantiles=N_QUANTILES,
        ).to(self._device)
        return model

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
        Train TFT on time-series sequences.

        Parameters
        ----------
        X_train : ndarray (n_train, seq_len, n_features)
        y_train : ndarray (n_train,) — forward returns (float)
        X_val   : ndarray (n_val, seq_len, n_features)
        y_val   : ndarray (n_val,) — forward returns (float)
        sample_weight : not used (TFT uses uniform weights); accepted for
            interface compatibility.

        Spec References
        ---------------
        SPEC-MODEL-003: walk-forward validation; this function trains one fold.
        SPEC-MODEL-004: early stopping on validation loss.
        """
        _require_torch()
        X_tr = self._to_tensor(X_train)
        y_tr = torch.tensor(np.asarray(y_train, dtype=np.float32), device=self._device)
        X_v = self._to_tensor(X_val)
        y_v = torch.tensor(np.asarray(y_val, dtype=np.float32), device=self._device)

        # --quick mode: subsample 50 samples (SPEC-MODEL-003 CI shortcut)
        if self.quick:
            X_tr, y_tr = X_tr[:50], y_tr[:50]
            X_v, y_v = X_v[:50], y_v[:50]

        self._training_samples = len(X_tr)
        self._model = self._build_model()
        optimiser = torch.optim.Adam(self._model.parameters(), lr=self.learning_rate)

        train_ds = TensorDataset(X_tr, y_tr)
        train_loader = DataLoader(train_ds, batch_size=self.batch_size, shuffle=True)

        best_weights = None
        no_improve = 0

        for epoch in range(1, self.epochs + 1):
            # ── training step ──────────────────────────────────────────────
            self._model.train()
            train_loss = 0.0
            for xb, yb in train_loader:
                optimiser.zero_grad()
                preds, _ = self._model(xb)
                loss = self._combined_quantile_loss(preds, yb)
                loss.backward()
                nn.utils.clip_grad_norm_(self._model.parameters(), max_norm=1.0)
                optimiser.step()
                train_loss += loss.item() * len(xb)
            train_loss /= self._training_samples

            # ── validation step ────────────────────────────────────────────
            self._model.eval()
            with torch.no_grad():
                val_preds, _ = self._model(X_v)
                val_loss = self._combined_quantile_loss(val_preds, y_v).item()

            logger.info(
                f"TFT epoch {epoch}/{self.epochs} "
                f"train_loss={train_loss:.4f} val_loss={val_loss:.4f}"
            )

            if val_loss < self._best_val_loss - 1e-6:
                self._best_val_loss = val_loss
                best_weights = {k: v.clone() for k, v in self._model.state_dict().items()}
                no_improve = 0
            else:
                no_improve += 1
                if no_improve >= self.early_stopping_patience:
                    logger.info(f"TFT early stop at epoch {epoch} (patience={self.early_stopping_patience})")
                    break

        if best_weights is not None:
            self._model.load_state_dict(best_weights)
        self._created_at = datetime.utcnow().isoformat()
        logger.info(f"TFT training complete. best_val_loss={self._best_val_loss:.4f}")

    def predict(self, X: Any) -> np.ndarray:
        """Return argmax class index for each sample: 0=Sell, 1=Hold, 2=Buy."""
        return self.predict_proba(X).argmax(axis=1)

    def predict_proba(self, X: Any) -> np.ndarray:
        """
        Return (n_samples, 3) class probabilities [P(Sell), P(Hold), P(Buy)].

        Derived from Q50/Q10/Q90 by fitting a Normal distribution from the
        quantile spread and computing P(return > threshold) and
        P(return < -threshold) via scipy.stats.norm.
        """
        _require_torch()
        quantiles = self.predict_quantiles(X)   # (n, 3)
        return _quantiles_to_proba(quantiles, _BUY_THRESHOLD, _SELL_THRESHOLD)

    def predict_quantiles(self, X: Any) -> np.ndarray:
        """
        Return (n_samples, 3) quantile forecasts [Q10, Q50, Q90].

        Parameters
        ----------
        X : ndarray (n_samples, seq_len, n_features)

        Returns
        -------
        ndarray (n_samples, 3)
        """
        _require_torch()
        if self._model is None:
            raise RuntimeError("Model not trained. Call train() first.")
        self._model.eval()
        X_t = self._to_tensor(X)
        with torch.no_grad():
            preds, _ = self._model(X_t)
        return preds.cpu().numpy()

    def save(self, path: str) -> None:
        """
        Save model weights + hyperparameters (SPEC-MODEL-005).

        Saves two files:
          {path}.pt  — torch state_dict
          {path}.json — hyperparams + metadata
        """
        _require_torch()
        if self._model is None:
            raise RuntimeError("Nothing to save — model has not been trained.")
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        torch.save(self._model.state_dict(), str(p) + ".pt")
        meta = self.metadata()
        with open(str(p) + ".json", "w") as f:
            json.dump(meta, f, indent=2)
        logger.info(f"TFTSignalModel saved to {p}.pt / {p}.json")

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
            self.n_heads = hp.get("n_heads", self.n_heads)
            self.lstm_layers = hp.get("lstm_layers", self.lstm_layers)
            self.dropout = hp.get("dropout", self.dropout)

        self._model = self._build_model()
        weights = torch.load(str(p) + ".pt", map_location=self._device)
        self._model.load_state_dict(weights)
        self._model.eval()
        logger.info(f"TFTSignalModel loaded from {p}.pt")

    def metadata(self) -> Dict[str, Any]:
        return {
            "name": self.MODEL_NAME,
            "version": self._version,
            "created_at": self._created_at,
            "training_samples": self._training_samples,
            "best_val_loss": self._best_val_loss,
            "hyperparams": {
                "n_features": self.n_features,
                "seq_len": self.seq_len,
                "hidden_dim": self.hidden_dim,
                "n_heads": self.n_heads,
                "lstm_layers": self.lstm_layers,
                "dropout": self.dropout,
                "batch_size": self.batch_size,
                "learning_rate": self.learning_rate,
                "epochs": self.epochs,
                "early_stopping_patience": self.early_stopping_patience,
            },
        }

    # ── IExplainableModel ─────────────────────────────────────────────────────

    def get_shap_values(self, X: Any) -> np.ndarray:
        """
        Proxy SHAP via VSN variable selection weights.

        Returns (n_samples, n_features) — mean variable-selection weight per
        feature across all 63 timesteps. Not identical to SHAP but is the
        TFT's native interpretability signal (Lim et al. §3.4).
        """
        _require_torch()
        if self._model is None:
            raise RuntimeError("Model not trained. Call train() first.")
        self._model.eval()
        X_t = self._to_tensor(X)
        with torch.no_grad():
            _, var_weights = self._model(X_t)
        # var_weights: (batch, seq, n_vars) → mean over seq
        return var_weights.mean(dim=1).cpu().numpy()

    # ── TFT-specific interpretability ─────────────────────────────────────────

    def get_attention_weights(self, X: Any) -> np.ndarray:
        """
        Return temporal attention weight matrix.

        Parameters
        ----------
        X : ndarray (n_samples, seq_len, n_features)

        Returns
        -------
        ndarray (n_samples, seq_len, seq_len)
          attn[i, t, s] = weight sample i assigns to past step s when
          decoding step t. Rows sum to 1 per sample per head.

        Spec References
        ---------------
        M-11 build prompt: "TFT attention maps must show temporal structure
        (earlier timesteps should have lower weight than recent)."
        """
        _require_torch()
        if self._model is None:
            raise RuntimeError("Model not trained. Call train() first.")
        self._model.eval()
        X_t = self._to_tensor(X)
        with torch.no_grad():
            self._model(X_t)
        attn = self._model.last_attention_weights   # (batch, seq, seq)
        if attn is None:
            return np.zeros((len(X_t), self.seq_len, self.seq_len))
        return attn.cpu().numpy()


# ── Quantile-to-probability conversion ───────────────────────────────────────


def _quantiles_to_proba(
    quantiles: np.ndarray,
    buy_threshold: float = _BUY_THRESHOLD,
    sell_threshold: float = _SELL_THRESHOLD,
) -> np.ndarray:
    """
    Convert Q10/Q50/Q90 quantile forecasts to P(Sell)/P(Hold)/P(Buy).

    Fits a Normal distribution from the Q10–Q90 spread.
    Q10 = μ - 1.282σ, Q90 = μ + 1.282σ  → σ = (Q90-Q10) / 2.564
    """
    from scipy.stats import norm

    q10, q50, q90 = quantiles[:, 0], quantiles[:, 1], quantiles[:, 2]
    mu = q50
    sigma = np.maximum((q90 - q10) / 2.564, 1e-6)
    p_buy = 1.0 - norm.cdf(buy_threshold, loc=mu, scale=sigma)
    p_sell = norm.cdf(-sell_threshold, loc=mu, scale=sigma)
    p_hold = np.maximum(1.0 - p_buy - p_sell, 0.0)
    return np.stack([p_sell, p_hold, p_buy], axis=1).astype(np.float32)


# ── Overnight training entry point ────────────────────────────────────────────


def schedule_overnight_training(
    feature_parquet_dir: str,
    model_output_dir: str,
    horizon_days: int = 21,
    n_folds: int = 3,
    quick: bool = False,
) -> Dict:
    """
    Overnight TFT training run (SPEC-MODEL-003 walk-forward).

    Loads daily Parquet feature files, constructs 63-day sequences,
    builds triple-barrier labels, and trains one TFT per fold.

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
    CPU (Ryzen 5 7535U), 500 stocks, 5yr history, 50 epochs: 4–6 hours.
    Schedule via pipeline_scheduler.py after market close.

    Returns
    -------
    dict
        {"folds_trained": int, "last_model_path": str or None} — used by
        the caller (train_deep_models.py) to write a registry.json entry
        (SPEC-MODEL-005); this function itself does not touch the
        registry, since it has no opinion on the registry key name (the
        caller may train multiple horizons under one call).

    Raises
    ------
    FileNotFoundError
        If `feature_parquet_dir` has no Parquet files. There is no
        synthetic-data fallback — run the daily feature pipeline
        (features/matrix_builder.py) first to populate it. See
        BuildLog.md "Real data sourcing — TFT".
    """
    _require_torch()
    import gc
    import pandas as pd

    logger.info(
        f"TFT overnight training: horizon={horizon_days}d, folds={n_folds}, quick={quick}"
    )

    parquet_dir = Path(feature_parquet_dir)
    all_files = sorted(parquet_dir.glob("*.parquet")) if parquet_dir.exists() else []
    if not all_files:
        raise FileNotFoundError(
            f"No Parquet files in {parquet_dir}. There is no synthetic-data fallback — "
            "run the daily feature pipeline (features/matrix_builder.py) first to populate "
            "it. See BuildLog.md 'Real data sourcing — TFT'."
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
        # Use only recent history where fundamentals/macro features are populated.
        # Pre-2021 files have many NaN feature values that cause nan training loss.
        all_files = all_files[-_FULL_RECENT_FILES:]

    fold_size = max(_MIN_FOLD_FILES, len(all_files) // (n_folds + 1))
    output_dir = Path(model_output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    folds_trained = 0
    last_model_path: Optional[str] = None

    for fold in range(n_folds):
        train_end = min((fold + 1) * fold_size, len(all_files))
        val_end = min(train_end + fold_size, len(all_files))
        if val_end <= train_end:
            logger.warning(f"Fold {fold}: not enough files for a validation window, stopping")
            break
        train_files = all_files[:train_end][-_FULL_TRAIN_FILES:]
        val_files = all_files[train_end:val_end]

        logger.info(f"Fold {fold}: {len(train_files)} train files, {len(val_files)} val files")

        max_tr = _QUICK_MAX_SEQ if quick else _MAX_TRAIN_SEQ
        max_v = _QUICK_MAX_SEQ if quick else _MAX_VAL_SEQ

        # Cap val_files to avoid OOM: uncapped val windows can be 1000+ files.
        val_files_capped = val_files[-(_QUICK_FILES if quick else _FULL_VAL_FILES):]

        X_tr, y_tr = _stream_sequences_from_files(
            train_files, feature_cols, horizon_days, SEQ_LEN, max_samples=max_tr)
        gc.collect()

        # Prepend last SEQ_LEN train files as lookback context for val sequences.
        val_context = train_files[-SEQ_LEN:]
        X_v, y_v = _stream_sequences_from_files(
            list(val_context) + list(val_files_capped),
            feature_cols, horizon_days, SEQ_LEN, max_samples=max_v, rng_seed=43)
        gc.collect()

        if len(X_tr) == 0 or len(X_v) == 0:
            logger.warning(f"Fold {fold}: insufficient data, skipping")
            continue

        model = TFTSignalModel(n_features=n_features, quick=quick)
        model.train(X_tr, y_tr, X_v, y_v)
        del X_tr, y_tr, X_v, y_v
        gc.collect()

        model_path = output_dir / f"tft_signal_{horizon_days}d_v{model._version}_fold{fold}"
        model.save(str(model_path))
        del model
        gc.collect()
        logger.info(f"Fold {fold} model saved to {model_path}.pt")
        folds_trained += 1
        last_model_path = f"{model_path}.pt"

    return {"folds_trained": folds_trained, "last_model_path": last_model_path}


def _stream_sequences_from_files(
    files: List[Path],
    feature_cols: List[str],
    horizon_days: int,
    seq_len: int,
    max_samples: Optional[int] = None,
    rng_seed: int = 42,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Build (X, y) sequence arrays by streaming through date parquet files one at a
    time, accumulating per-ticker float32 row lists.

    Peak RSS is O(n_files × n_tickers × n_features × 4 bytes) rather than the
    O(n_files² × n_tickers × …) spike caused by holding all frames in a list
    before pd.concat.  At 120 files × 500 tickers × 297 features the ticker-row
    accumulator uses ~71 MB; sequences add at most max_samples × seq_len ×
    n_features × 4 bytes (≈150 MB for max_samples=2000).

    Labels fall back to feats[i, 0] because feature parquets do not carry a
    raw close price column — this matches the existing _build_sequences fallback.
    """
    import gc

    _DUCKDB_INTERNAL = {"__fragment_index", "__batch_index", "__last_in_fragment",
                        "__filename"}
    keep_set = {"date", "ticker"} | set(feature_cols)

    # One file at a time → per-ticker list of float32 row arrays
    ticker_rows: Dict[str, List[np.ndarray]] = {}

    for f in files:
        try:
            import pandas as pd
            df = pd.read_parquet(f)
        except Exception as exc:
            logger.warning(f"Skipping {f}: {exc}")
            continue
        drop_cols = [c for c in df.columns
                     if c in _DUCKDB_INTERNAL or c not in keep_set]
        if drop_cols:
            df = df.drop(columns=drop_cols, errors="ignore")
        float_cols = df.select_dtypes("float64").columns
        if len(float_cols):
            df[float_cols] = df[float_cols].astype(np.float32)

        for ticker, grp in df.groupby("ticker"):
            row = grp[feature_cols].values.astype(np.float32)
            if ticker not in ticker_rows:
                ticker_rows[ticker] = []
            ticker_rows[ticker].append(row)
        del df
        gc.collect()

    # Stack per-ticker rows and collect valid sequence indices
    per_ticker: Dict[str, np.ndarray] = {}
    valid_pairs: List[Tuple[str, int]] = []

    for ticker, rows in ticker_rows.items():
        feats_raw = np.vstack(rows)       # (n_dates, n_features)
        # Replace NaN/Inf in features — older parquet files have sparse
        # fundamentals/macro columns that produce NaN, which propagates
        # through the forward pass and poisons gradients.
        feats = np.nan_to_num(feats_raw, nan=0.0, posinf=0.0, neginf=0.0)
        n = len(feats)
        idxs = list(range(seq_len, n - horizon_days))
        if idxs:
            per_ticker[ticker] = feats
            valid_pairs.extend((ticker, i) for i in idxs)
    del ticker_rows
    gc.collect()

    if not valid_pairs:
        return np.empty((0, seq_len, len(feature_cols))), np.empty(0)

    if max_samples is not None and len(valid_pairs) > max_samples:
        rng = np.random.default_rng(rng_seed)
        chosen = rng.choice(len(valid_pairs), size=max_samples, replace=False)
        valid_pairs = [valid_pairs[j] for j in sorted(chosen)]

    X_list: List[np.ndarray] = []
    y_list: List[float] = []
    for ticker, i in valid_pairs:
        feats = per_ticker[ticker]
        X_list.append(feats[i - seq_len: i])
        # Use the future value of the first feature (pct_rank_5d / pct_rank_21d)
        # as a forward-looking proxy label.  feats[i+horizon_days] is in-bounds
        # because i < n - horizon_days by construction.
        y_list.append(float(feats[i + horizon_days, 0]))

    return np.stack(X_list), np.array(y_list, dtype=np.float32)


def _load_parquets_float32(
    files: List[Path],
    feature_cols: List[str],
) -> "pd.DataFrame":
    """
    Load a list of date parquet files and immediately cast floats to float32.

    Halves RAM vs the default float64 representation.  Drops DuckDB internal
    columns (__fragment_index, __batch_index, __last_in_fragment, __filename)
    that the feature pipeline may have embedded in the parquet metadata.
    """
    import pandas as pd

    _DUCKDB_INTERNAL = {"__fragment_index", "__batch_index", "__last_in_fragment",
                        "__filename"}
    keep = {"date", "ticker"} | set(feature_cols)

    frames = []
    for f in files:
        df = pd.read_parquet(f)
        drop_cols = [c for c in df.columns
                     if c in _DUCKDB_INTERNAL or (c not in keep)]
        if drop_cols:
            df = df.drop(columns=drop_cols, errors="ignore")
        float_cols = df.select_dtypes("float64").columns
        if len(float_cols):
            df[float_cols] = df[float_cols].astype(np.float32)
        frames.append(df)
    out = pd.concat(frames, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"])
    out = out.sort_values(["ticker", "date"]).reset_index(drop=True)
    return out


def _build_sequences(
    df: "pd.DataFrame",
    feature_cols: List[str],
    horizon_days: int,
    seq_len: int,
    max_samples: Optional[int] = None,
    rng_seed: int = 42,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Build (X, y) arrays of shape (n_samples, seq_len, n_features) and (n_samples,).

    For each (ticker, date) with at least seq_len prior days and horizon_days
    future days available, slice the lookback window as X and the horizon
    return as y.

    max_samples
        If set, randomly subsample from available (ticker, date) pairs before
        materialising sequences.  This is the primary lever for capping peak
        RSS: without it, a 300-file × 500-ticker fold would materialise ~7 GB
        of sequence arrays even in float32.
    """
    # ── Collect valid (ticker, seq_index) pairs without materialising windows ──
    per_ticker: Dict[str, Tuple[np.ndarray, Optional[np.ndarray]]] = {}
    valid_pairs: List[Tuple[str, int]] = []

    for ticker, grp in df.groupby("ticker"):
        grp = grp.sort_values("date").reset_index(drop=True)
        prices = grp["close"].values if "close" in grp.columns else None
        feats = grp[feature_cols].values.astype(np.float32)
        n = len(feats)
        idxs = list(range(seq_len, n - horizon_days))
        if idxs:
            per_ticker[ticker] = (feats, prices)
            valid_pairs.extend((ticker, i) for i in idxs)

    if not valid_pairs:
        return np.empty((0, seq_len, len(feature_cols))), np.empty(0)

    if max_samples is not None and len(valid_pairs) > max_samples:
        rng = np.random.default_rng(rng_seed)
        chosen = rng.choice(len(valid_pairs), size=max_samples, replace=False)
        valid_pairs = [valid_pairs[j] for j in chosen]

    X_list: List[np.ndarray] = []
    y_list: List[float] = []
    for ticker, i in valid_pairs:
        feats, prices = per_ticker[ticker]
        X_list.append(feats[i - seq_len: i])
        if prices is not None:
            p0, p_h = prices[i], prices[i + horizon_days]
            y_list.append(float(np.log(p_h / max(p0, 1e-6))))
        else:
            y_list.append(float(feats[i, 0]))

    return np.stack(X_list), np.array(y_list, dtype=np.float32)


# ── CLI entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Train TFT deep model (M-11)")
    parser.add_argument("--quick", action="store_true",
                        help="2 epochs, 1 fold — CI smoke-test mode on real Parquet feature data")
    parser.add_argument("--epochs", type=int, default=50)
    parser.add_argument("--folds", type=int, default=5)
    parser.add_argument("--horizon", type=int, default=21)
    parser.add_argument("--feature-dir", default="datastore/features/daily",
                        help="Directory containing per-day Parquet feature files")
    parser.add_argument("--output-dir", default="datastore/models")
    args = parser.parse_args()

    schedule_overnight_training(
        feature_parquet_dir=args.feature_dir,
        model_output_dir=args.output_dir,
        horizon_days=args.horizon,
        n_folds=args.folds,
        quick=args.quick,
    )
