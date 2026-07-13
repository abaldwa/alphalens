"""
tests/unit/test_forensic_ml.py

Coverage for systems/ml_signal_engine/models/forensic/forensic_ml.py (M-10
ForensicMLModel + compute_governance_score + load_forensic_training_data_from_db).

Uses the module's own KNOWN_FRAUD_ARCHIVE / KNOWN_CLEAN_ARCHIVE real-case
feature vectors (Satyam, DHFL, IL&FS, Vakrangee, PC Jeweller vs HDFC Bank,
TCS, Infosys, Asian Paints) — these are real, documented data, not
synthetic/fabricated rows, per the module's own docstring. Model training
here uses only these 9 rows (n_estimators kept small) — no full-universe
or large hyperparameter search.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from systems.ml_signal_engine.models.forensic.forensic_ml import (
    FLAG_LEVELS,
    FORENSIC_BLOCK_THRESHOLD,
    FORENSIC_ML_FEATURES,
    ForensicMLModel,
    _flag_for_score,
    compute_governance_score,
    load_forensic_training_data_from_db,
)


@pytest.fixture(scope="module")
def training_data():
    # min_samples=9 to accept the real archive (5 fraud + 4 clean = 9 rows)
    # without needing a DB-backed augmentation.
    X, y = load_forensic_training_data_from_db(min_samples=9)
    return X, y


class TestLoadForensicTrainingDataFromDb:
    def test_shapes_and_labels(self, training_data):
        X, y = training_data
        assert list(X.columns) == FORENSIC_ML_FEATURES
        assert len(X) == len(y) == 9
        assert set(y.unique()) == {0, 1}
        assert y.sum() == 5  # 5 known fraud cases

    def test_raises_when_below_min_samples(self):
        with pytest.raises(RuntimeError, match="need at least"):
            load_forensic_training_data_from_db(min_samples=1000)

    def test_archive_rows_have_documented_nans(self, training_data):
        X, _ = training_data
        # vae_anomaly_score is permanently out of scope -> always NaN.
        assert X["vae_anomaly_score"].isna().all()
        # Real Benford values are present for every archive row.
        assert X["benford_revenue_chi2"].notna().all()


class TestFlagForScore:
    def test_green(self):
        assert _flag_for_score(10) == "green"

    def test_yellow(self):
        assert _flag_for_score(30) == "yellow"

    def test_orange(self):
        assert _flag_for_score(50) == "orange"

    def test_red(self):
        assert _flag_for_score(70) == "red"

    def test_black(self):
        assert _flag_for_score(90) == "black"

    def test_nan_returns_none(self):
        assert _flag_for_score(float("nan")) is None

    def test_all_flag_levels_reachable(self):
        scores = [10, 30, 50, 70, 90]
        flags = [_flag_for_score(s) for s in scores]
        assert flags == list(FLAG_LEVELS)


class TestComputeGovernanceScore:
    def test_all_components_present(self):
        score = compute_governance_score(
            {"promoter_pledge_pct": 25.0, "promoter_pledge_change": 5.0, "promoter_pledge_spiral_risk": 0.5}
        )
        assert 0 <= score <= 100

    def test_no_signal_returns_nan(self):
        score = compute_governance_score({})
        assert np.isnan(score)

    def test_nan_values_skipped(self):
        score = compute_governance_score(
            {"promoter_pledge_pct": float("nan"), "promoter_pledge_change": 5.0}
        )
        assert not np.isnan(score)

    def test_negative_pledge_change_clipped_to_zero_component(self):
        # pledge_change < 0 should clip to a 0 contribution, not go negative.
        score_negative = compute_governance_score({"promoter_pledge_change": -10.0})
        score_zero = compute_governance_score({"promoter_pledge_change": 0.0})
        assert score_negative == score_zero == 0.0

    def test_high_pledge_clips_at_100(self):
        score = compute_governance_score({"promoter_pledge_pct": 200.0})
        assert score == 100.0


class TestForensicMLModelTrainPredict:
    def test_train_full_and_predict(self, training_data):
        X, y = training_data
        model = ForensicMLModel(random_state=42, n_estimators=10)
        result = model.train_full(X, y)
        assert result["training_samples"] == 9
        assert 0 < result["positive_rate"] < 1

        proba = model.predict(X)
        assert len(proba) == 9
        assert ((proba >= 0) & (proba <= 1)).all()

        proba_df = model.predict_proba(X)
        assert list(proba_df.columns) == ["clean", "fraud"]
        assert np.allclose(proba_df["clean"] + proba_df["fraud"], 1.0)

    def test_anomaly_score_bounded(self, training_data):
        X, y = training_data
        model = ForensicMLModel(random_state=42, n_estimators=10)
        model.train_full(X, y)
        scores = model.anomaly_score(X)
        assert ((scores >= 0) & (scores <= 100)).all()

    def test_predict_full_composite_and_flags(self, training_data):
        X, y = training_data
        model = ForensicMLModel(random_state=42, n_estimators=10)
        model.train_full(X, y)

        classical = pd.Series([80.0] * len(X), index=X.index)
        governance = pd.Series([70.0] * len(X), index=X.index)
        out = model.predict_full(X, classical, governance)

        expected_cols = {
            "ml_fraud_probability", "anomaly_score", "classical_score",
            "governance_score", "forensic_composite", "flag", "blocked",
        }
        assert expected_cols.issubset(out.columns)
        assert out["flag"].isin(list(FLAG_LEVELS)).all()
        assert (out["blocked"] == (out["forensic_composite"] > FORENSIC_BLOCK_THRESHOLD)).all()

    def test_predict_full_partial_missing_layer_renormalizes(self, training_data):
        X, y = training_data
        model = ForensicMLModel(random_state=42, n_estimators=10)
        model.train_full(X, y)

        classical = pd.Series([np.nan] * len(X), index=X.index)
        governance = pd.Series([50.0] * len(X), index=X.index)
        out = model.predict_full(X, classical, governance)
        # classical is entirely missing -> composite still computed from remaining layers.
        assert out["forensic_composite"].notna().all()
        assert out["classical_score"].isna().all()

    def test_predict_before_train_raises(self):
        model = ForensicMLModel()
        with pytest.raises(RuntimeError, match="before train"):
            model.predict(pd.DataFrame({"x": [1]}))

    def test_anomaly_score_before_train_raises(self):
        model = ForensicMLModel()
        with pytest.raises(RuntimeError):
            model.anomaly_score(pd.DataFrame({"x": [1]}))

    def test_predict_full_before_train_raises(self):
        model = ForensicMLModel()
        with pytest.raises(RuntimeError, match="before train_full"):
            model.predict_full(pd.DataFrame({"x": [1]}), pd.Series([1]), pd.Series([1]))

    def test_train_simple_lgbm_only(self, training_data):
        X, y = training_data
        model = ForensicMLModel(random_state=42, n_estimators=10)
        model.train(X, y)
        proba = model.predict(X)
        assert len(proba) == 9
        # train() (not train_full()) never fits an isolation forest.
        with pytest.raises(RuntimeError):
            model.anomaly_score(X)

    def test_train_row_mismatch_raises(self):
        model = ForensicMLModel()
        X = pd.DataFrame({"a": [1, 2, 3]})
        y = pd.Series([1, 0])
        with pytest.raises(ValueError, match="rows"):
            model.train(X, y)

    def test_train_all_nan_labels_raises(self):
        model = ForensicMLModel()
        X = pd.DataFrame({"a": [1, 2, 3]})
        y = pd.Series([np.nan, np.nan, np.nan])
        with pytest.raises(ValueError, match="non-NaN"):
            model.train(X, y)

    def test_save_load_roundtrip(self, training_data, tmp_path):
        X, y = training_data
        model = ForensicMLModel(random_state=42, n_estimators=10)
        model.train_full(X, y)
        path = str(tmp_path / "forensic_ml_model.joblib")
        model.save(path)

        loaded = ForensicMLModel()
        loaded.load(path)
        meta = loaded.metadata()
        assert meta["name"] == "ForensicMLModel"
        assert meta["training_samples"] == 9
        assert meta["features_count"] == len(FORENSIC_ML_FEATURES)

        proba_before = model.predict(X)
        proba_after = loaded.predict(X)
        assert np.allclose(proba_before.values, proba_after.values)

    def test_save_before_train_raises(self, tmp_path):
        model = ForensicMLModel()
        with pytest.raises(RuntimeError, match="before train"):
            model.save(str(tmp_path / "x.joblib"))

    def test_metadata_before_train(self):
        model = ForensicMLModel()
        meta = model.metadata()
        assert meta["training_samples"] is None
        assert meta["features_count"] == 0
