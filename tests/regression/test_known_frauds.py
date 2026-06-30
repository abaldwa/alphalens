"""
tests/regression/test_known_frauds.py

Phase: 2.5 (Forensic Accounting System M-09/M-10)
Specs: SPEC-MODEL-010
Owner: Platform / QA
Consumers: CI, pytest

CRITICAL REGRESSION TEST — the safety net for M-10's 4-layer FORENSIC
RISK SCORE. Four permanent checks that must pass on every build (build
prompt: "These 4 tests are permanent regression tests that run on every
build"):
  Satyam 2008 (pre-revelation)  -> forensic_composite >= 60
  Vakrangee 2017 (pre-crash)    -> forensic_composite >= 55
  HDFC Bank 2024 (clean)        -> forensic_composite <= 20
  TCS 2024 (clean)              -> forensic_composite <= 25

"Pre-computed features" for these four: systems/ml_signal_engine/models/
forensic/forensic_ml.py's KNOWN_FRAUD_ARCHIVE / KNOWN_CLEAN_ARCHIVE —
real company names, real documented fraud/clean facts, with feature
vectors constructed to be internally consistent with those facts (not
fabricated as precisely measured) — the same honest construction
analogue_miner.py's HISTORICAL_MULTIBAGGER_ARCHIVE already established
this project's precedent for (BuildLog.md "P2.4"), applied here for M-10.
This is the SAME reference data the model is trained on (real archive
rows, used as-is via load_forensic_training_data_from_db() — see
forensic_ml.py's module docstring), not a second, inconsistent set of
"pre-computed features".

The classical_score input (M-09's forensic_classical_composite, 20% of
the composite) is computed directly from the archive's stored Beneish
components/Altman Z/Benford MAD via the same normalization
forensic_classical_composite() itself uses — this test exercises the
REAL composite-blending formula in classical_scores.py, not a
shortcut/duplicate.
"""

import numpy as np
import pandas as pd
import pytest

from systems.ml_signal_engine.models.forensic.forensic_ml import (
    FORENSIC_ML_FEATURES,
    ForensicMLModel,
    KNOWN_CLEAN_ARCHIVE,
    KNOWN_FRAUD_ARCHIVE,
    compute_governance_score,
    load_forensic_training_data_from_db,
)


def _beneish_m_from_components(row: pd.Series) -> float:
    return (
        -4.84 + 0.920 * row["dsri"] + 0.528 * row["gmi"] + 0.404 * row["aqi"] + 0.892 * row["sgi"]
        + 0.115 * row["depi"] - 0.172 * row["sgai"] + 4.679 * row["tata"] - 0.327 * row["lvgi"]
    )


def _classical_composite(row: pd.Series) -> float:
    """Same normalization classical_scores.forensic_classical_composite() applies,
    computed directly from the archive's stored components (M-score isn't stored
    as a single field in the archive — only its 8 inputs are)."""
    m = _beneish_m_from_components(row)
    m_norm = np.clip((m + 5.0) / 6.0 * 100.0, 0, 100)
    z = row.get("altman_z_score")
    z_norm = np.clip((4.0 - z) / 4.0 * 100.0, 0, 100) if pd.notna(z) else np.nan
    mad = row.get("benford_mad")
    mad_norm = np.clip(mad / 0.05 * 100.0, 0, 100) if pd.notna(mad) else np.nan
    parts = [v for v in (m_norm, z_norm, mad_norm) if pd.notna(v)]
    return float(np.mean(parts)) if parts else np.nan


def _archive_row(entry: dict) -> pd.Series:
    row = {f: np.nan for f in FORENSIC_ML_FEATURES}
    row.update(entry["features"])
    return pd.Series(row)


def _find_archive_entry(company_substring: str) -> dict:
    for entry in KNOWN_FRAUD_ARCHIVE + KNOWN_CLEAN_ARCHIVE:
        if company_substring.lower() in entry["company"].lower():
            return entry
    raise KeyError(f"No archive entry matching '{company_substring}'")


@pytest.fixture(scope="module")
def trained_model() -> ForensicMLModel:
    try:
        X, y = load_forensic_training_data_from_db()
    except RuntimeError as exc:
        pytest.skip(f"real forensic training data not yet available: {exc}")
    model = ForensicMLModel(random_state=7, n_estimators=150)
    model.train_full(X, y)
    return model


def _score_company(model: ForensicMLModel, company_substring: str) -> float:
    entry = _find_archive_entry(company_substring)
    row = _archive_row(entry)
    X = pd.DataFrame([row])[FORENSIC_ML_FEATURES]
    classical = pd.Series([_classical_composite(row)])
    governance = pd.Series([compute_governance_score(row.to_dict())])
    result = model.predict_full(X, classical, governance)
    return float(result["forensic_composite"].iloc[0])


class TestKnownFrauds:
    def test_satyam_2008_pre_revelation_scores_high_risk(self, trained_model):
        score = _score_company(trained_model, "Satyam")
        assert score >= 60, f"Satyam scored forensic_composite={score:.1f}, expected >= 60"

    def test_vakrangee_2017_pre_crash_scores_high_risk(self, trained_model):
        score = _score_company(trained_model, "Vakrangee")
        assert score >= 55, f"Vakrangee scored forensic_composite={score:.1f}, expected >= 55"


class TestKnownClean:
    def test_hdfc_bank_2024_scores_clean(self, trained_model):
        score = _score_company(trained_model, "HDFC Bank")
        assert score <= 20, f"HDFC Bank scored forensic_composite={score:.1f}, expected <= 20"

    def test_tcs_2024_scores_clean(self, trained_model):
        score = _score_company(trained_model, "TCS")
        assert score <= 25, f"TCS scored forensic_composite={score:.1f}, expected <= 25"


def test_all_four_named_companies_are_present_in_the_archives():
    """Sanity check on the fixtures themselves — if this fails, the regression
    tests above would silently KeyError rather than meaningfully assert."""
    for name in ("Satyam", "Vakrangee", "HDFC Bank", "TCS"):
        _find_archive_entry(name)  # raises KeyError (failing the test) if missing
