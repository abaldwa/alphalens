"""
tests/unit/test_gainer_survival_head.py

ML33 (2026-07-13, development phase) — GAINER EXPERIMENT ONLY.
systems/ml_signal_engine_gainer/models/signal/gainer_survival_head.py's
small RandomSurvivalForest head, fit on
compute_fixed_pct_labels' (label, first_touch_day) output. Synthetic
feature/price data generated in-process (not written to any DB) —
consistent with this repo's no-synthetic-DB-writes policy.
"""

import numpy as np
import pandas as pd
import pytest

from systems.ml_signal_engine_gainer.models.signal.gainer_survival_head import GainerSurvivalHead
from systems.ml_signal_engine_gainer.training.labeling import compute_fixed_pct_labels


def _make_synthetic_panel(n_tickers: int = 8, n_days: int = 80, horizon_days: int = 21, target_pct: float = 0.10, seed: int = 42):
    rng = np.random.default_rng(seed)
    rows = []
    for t in range(n_tickers):
        ticker = f"T{t}"
        drift = rng.uniform(-0.001, 0.003)
        vol = rng.uniform(0.01, 0.03)
        closes = 100 * np.exp(np.cumsum(rng.normal(drift, vol, n_days)))
        closes_series = pd.Series(closes)
        label_df = compute_fixed_pct_labels(closes_series, horizon_days=horizon_days, target_pct=target_pct)
        feat1 = pd.Series(rng.normal(0, 1, n_days))
        feat2 = pd.Series(closes).pct_change().rolling(5).mean()
        df = pd.DataFrame(
            {
                "ticker": ticker,
                "close": closes,
                "feat1": feat1,
                "feat2": feat2,
                "label": label_df["label"].to_numpy(),
                "first_touch_day": label_df["first_touch_day"].to_numpy(),
            }
        )
        rows.append(df)
    return pd.concat(rows, ignore_index=True)


class TestGainerSurvivalHeadEndToEnd:
    def test_fit_and_predict_runs_end_to_end_on_small_sample(self):
        """Proves the RSF head trains and predicts successfully on a
        small/quick synthetic sample — the ML33 development-phase bar
        ('runs successfully end-to-end'), not a production-quality fit."""
        panel = _make_synthetic_panel()
        train = panel.dropna(subset=["label"]).reset_index(drop=True)
        assert len(train) > 20, "synthetic panel produced too few resolved-label rows to fit on"

        X = train[["feat1", "feat2"]]
        head = GainerSurvivalHead(n_estimators=10, min_samples_leaf=2)
        diagnostics = head.fit(X, train["first_touch_day"], train["label"], horizon_days=21)

        assert diagnostics["training_samples"] == len(train)
        assert 0.0 <= diagnostics["event_rate"] <= 1.0
        assert not np.isnan(diagnostics["concordance_index"])

        preds = head.predict_survival_at_days(X.head(5), days=[1, 5, 21])
        assert list(preds.columns) == ["survival_d1", "survival_d5", "survival_d21"]
        assert len(preds) == 5
        # Survival probability should be non-increasing over time for each row.
        for _, row in preds.iterrows():
            assert row["survival_d1"] >= row["survival_d5"] >= row["survival_d21"] - 1e-9

    def test_fit_raises_on_misaligned_inputs(self):
        head = GainerSurvivalHead()
        X = pd.DataFrame({"a": [1, 2, 3]})
        with pytest.raises(ValueError):
            head.fit(X, pd.Series([1, 2]), pd.Series([1, 0, 1]), horizon_days=21)

    def test_predict_before_fit_raises(self):
        head = GainerSurvivalHead()
        with pytest.raises(RuntimeError):
            head.predict_survival_at_days(pd.DataFrame({"a": [1]}), days=[1])
