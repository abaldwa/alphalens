"""
tests/regression/test_multibagger_historical.py

Phase: 2.4 (Multibagger Detection System M-08)
Specs: SPEC-MODEL-001
Owner: Platform / QA
Consumers: CI, pytest

HITL REGRESSION TEST — the safety net for M-08: three confirmed real
historical Indian multibaggers (AVANTI FEEDS, 2017 entry; RELAXO
FOOTWEARS, 2016; PAGE INDUSTRIES, 2019 — exactly the three this phase's
build prompt names) must each score mb_probability > 0.45. This test
flags model degradation during retraining (build prompt: "This test
flags model degradation during retraining").

"Pre-computed features" for these three: systems/ml_signal_engine/models/
multibagger/analogue_miner.py's HISTORICAL_MULTIBAGGER_ARCHIVE — real
company names and real approximate entry-year/return facts. The
33-feature vectors attached to each archive entry remain a documented,
known gap (analogue_miner.py's module docstring) until a real historical
15-year OHLCV backfill + features/multibagger.py re-computation exists
for these specific tickers — tracked in BuildLog.md "Real data sourcing
— Multibagger historical archive features". This is the SAME reference
data find_analogues() uses — not a second, inconsistent set of
"pre-computed features".

The model under test, however, trains exclusively on real data via
load_multibagger_training_data_from_db() — there is no synthetic
training-data fallback; this test skips if DuckDB's ohlcv_adjusted
doesn't have enough real history yet.
"""

import pandas as pd
import pytest

from features.multibagger import MULTIBAGGER_FEATURES
from systems.ml_signal_engine.models.multibagger.analogue_miner import HISTORICAL_MULTIBAGGER_ARCHIVE
from systems.ml_signal_engine.models.multibagger.multibagger_model import (
    MultibaggerModel,
    load_multibagger_training_data_from_db,
)

REGRESSION_THRESHOLD = 0.45
REGRESSION_TICKERS = ("AVANTI FEEDS", "RELAXO FOOTWEARS", "PAGE INDUSTRIES")


@pytest.fixture(scope="module")
def trained_model() -> MultibaggerModel:
    try:
        X, y, duration, event, groups, _pnd = load_multibagger_training_data_from_db()
    except RuntimeError as exc:
        pytest.skip(f"real multibagger training data not yet available: {exc}")
    model = MultibaggerModel(random_state=11, n_estimators=200)
    model.train_full(X, y, duration, event, groups=groups)
    return model


def _archive_features(stock_name: str) -> dict:
    for entry in HISTORICAL_MULTIBAGGER_ARCHIVE:
        if entry["stock_name"] == stock_name:
            return entry["features"]
    raise KeyError(f"{stock_name} not found in HISTORICAL_MULTIBAGGER_ARCHIVE")


@pytest.mark.parametrize("stock_name", REGRESSION_TICKERS)
def test_known_multibagger_scores_above_threshold(trained_model, stock_name):
    features = _archive_features(stock_name)
    X = pd.DataFrame([features])[MULTIBAGGER_FEATURES]

    probability = trained_model.predict(X).iloc[0]

    assert probability > REGRESSION_THRESHOLD, (
        f"{stock_name} scored mb_probability={probability:.3f}, expected > {REGRESSION_THRESHOLD} "
        "— possible model degradation, see this test's module docstring"
    )


def test_all_three_named_tickers_are_present_in_the_archive():
    """Sanity check on the fixture itself — if this fails, the regression
    test above would be silently skipped over a missing entry, not run."""
    present = {entry["stock_name"] for entry in HISTORICAL_MULTIBAGGER_ARCHIVE}
    assert set(REGRESSION_TICKERS).issubset(present)
