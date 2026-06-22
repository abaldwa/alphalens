"""
tests/unit/test_labeling.py

Phase: 1
Specs: SPEC-MODEL-002, SPEC-MODEL-006
Owner: ml_signal_engine / training
Consumers: CI, pytest

Unit tests for the native triple-barrier labeler (no mlfinlab dependency).
"""

import numpy as np
import pandas as pd
import pytest

from systems.ml_signal_engine.training.labeling import TripleBarrierLabeler, compute_triple_barrier_labels


def _series(values, start="2024-01-01"):
    idx = pd.date_range(start, periods=len(values), freq="B")
    return pd.Series(values, index=idx, dtype="float64")


def test_upper_barrier_hit_first():
    """SPEC-MODEL-002: upper barrier touched first must label +1."""
    close = _series([100, 100, 103, 100, 100, 100])
    atr = _series([1.0] * 6)

    labels = compute_triple_barrier_labels(
        close, atr, horizon_days=3, profit_multiplier=2.0,
        stop_multiplier=2.0, vertical_barrier_days=3,
    )

    assert labels.iloc[0] == 1.0


def test_lower_barrier_hit_first():
    """SPEC-MODEL-002: lower barrier touched first must label -1."""
    close = _series([100, 100, 97, 100, 100, 100])
    atr = _series([1.0] * 6)

    labels = compute_triple_barrier_labels(
        close, atr, horizon_days=3, profit_multiplier=2.0,
        stop_multiplier=2.0, vertical_barrier_days=3,
    )

    assert labels.iloc[0] == -1.0


def test_vertical_barrier_hit_labels_zero():
    """SPEC-MODEL-002: neither barrier touched within horizon must label 0."""
    close = _series([100, 100.5, 100.8, 100.2, 100, 100])
    atr = _series([1.0] * 6)

    labels = compute_triple_barrier_labels(
        close, atr, horizon_days=3, profit_multiplier=2.0,
        stop_multiplier=2.0, vertical_barrier_days=3,
    )

    assert labels.iloc[0] == 0.0


def test_no_lookahead_beyond_vertical_barrier():
    """SPEC-MODEL-002: a barrier touch beyond the horizon must NOT affect the label."""
    close = _series([100, 100.5, 100.8, 100.2, 200, 100])
    atr = _series([1.0] * 6)

    labels = compute_triple_barrier_labels(
        close, atr, horizon_days=3, profit_multiplier=2.0,
        stop_multiplier=2.0, vertical_barrier_days=3,
    )

    # The +100 jump happens at index 4, outside the 3-day window for row 0
    # (which only looks at indices 1, 2, 3). Must still resolve to 0.
    assert labels.iloc[0] == 0.0


def test_tail_rows_are_nan():
    """SPEC-MODEL-002: rows without enough forward history must be NaN, never guessed."""
    close = _series([100, 101, 102, 103, 104, 105])
    atr = _series([1.0] * 6)

    labels = compute_triple_barrier_labels(
        close, atr, horizon_days=3, profit_multiplier=2.0,
        stop_multiplier=2.0, vertical_barrier_days=3,
    )

    assert labels.iloc[-3:].isna().all()


def test_pnd_block_downgrades_positive_label():
    """SPEC-MODEL-006: P&D-blocked entry dates must never receive a +1 label."""
    close = _series([100, 100, 103, 100, 100, 100])
    atr = _series([1.0] * 6)
    pnd_block = _series([True, False, False, False, False, False]).astype(bool)

    labels = compute_triple_barrier_labels(
        close, atr, horizon_days=3, profit_multiplier=2.0,
        stop_multiplier=2.0, vertical_barrier_days=3, pnd_block=pnd_block,
    )

    assert labels.iloc[0] == 0.0


def test_labels_only_in_allowed_set():
    """SPEC-MODEL-002: labels must only ever be in {-1, 0, 1} (or NaN at the tail)."""
    rng = np.random.default_rng(42)
    close = _series(100 + np.cumsum(rng.normal(0, 1, 60)))
    atr = _series(np.abs(rng.normal(1, 0.2, 60)))

    labels = compute_triple_barrier_labels(
        close, atr, horizon_days=5, profit_multiplier=1.5,
        stop_multiplier=1.5, vertical_barrier_days=5,
    )

    non_nan = labels.dropna()
    assert set(non_nan.unique()).issubset({-1.0, 0.0, 1.0})


def test_mismatched_index_raises():
    """SPEC-MODEL-002: close and atr must be aligned; mismatched index is a hard error."""
    close = _series([100, 101, 102, 103, 104, 105])
    atr = _series([1.0] * 6, start="2024-02-01")

    with pytest.raises(ValueError):
        compute_triple_barrier_labels(
            close, atr, horizon_days=3, profit_multiplier=2.0,
            stop_multiplier=2.0, vertical_barrier_days=3,
        )


def test_non_positive_arguments_raise():
    """SPEC-MODEL-002: horizon/multiplier arguments must be positive."""
    close = _series([100, 101, 102, 103])
    atr = _series([1.0] * 4)

    with pytest.raises(ValueError):
        compute_triple_barrier_labels(
            close, atr, horizon_days=0, profit_multiplier=2.0,
            stop_multiplier=2.0, vertical_barrier_days=3,
        )


# ===== TripleBarrierLabeler (class wrapper, P1.4) =====


def test_labeler_default_barrier_config():
    """SPEC-MODEL-002 defaults: profit_multiplier=2.0, stop_multiplier=1.0, max_holding=21."""
    labeler = TripleBarrierLabeler()
    assert labeler.profit_multiplier == 2.0
    assert labeler.stop_multiplier == 1.0
    assert labeler.max_holding == 21


def test_labeler_plus_one_when_profit_target_hit_before_stop():
    """Prompt requirement: +1 label when price hits profit target before stop."""
    labeler = TripleBarrierLabeler(profit_multiplier=2.0, stop_multiplier=2.0, max_holding=3)
    close = _series([100, 100, 103, 100, 100, 100])
    atr = _series([1.0] * 6)

    labels = labeler.label(close, atr)

    assert labels.iloc[0] == 1.0


def test_labeler_zero_when_timeout_before_either_barrier():
    """Prompt requirement: 0 label when timeout occurs before either barrier."""
    labeler = TripleBarrierLabeler(profit_multiplier=2.0, stop_multiplier=2.0, max_holding=3)
    close = _series([100, 100.5, 100.8, 100.2, 100, 100])
    atr = _series([1.0] * 6)

    labels = labeler.label(close, atr)

    assert labels.iloc[0] == 0.0


def test_labeler_no_label_extends_beyond_max_holding():
    """Prompt requirement: no label extends beyond its max_holding period —
    the tail max_holding rows (insufficient forward history) must be NaN."""
    labeler = TripleBarrierLabeler(max_holding=5)
    close = _series(100 + np.cumsum(np.zeros(20)))
    atr = _series([1.0] * 20)

    labels = labeler.label(close, atr)

    assert labels.iloc[-5:].isna().all()
    assert labels.iloc[:-5].notna().all()


def test_labeler_validate_rejects_out_of_range_labels():
    labeler = TripleBarrierLabeler()
    bad_labels = pd.Series([1.0, 0.0, -1.0, 2.0])
    with pytest.raises(ValueError):
        labeler.validate(bad_labels)


def test_labeler_validate_accepts_clean_labels():
    labeler = TripleBarrierLabeler()
    labeler.validate(pd.Series([1.0, 0.0, -1.0, np.nan]))  # must not raise


def test_labeler_class_distribution_report_sums_to_100(capsys):
    labeler = TripleBarrierLabeler()
    labels = pd.Series([1.0, 1.0, 0.0, 0.0, 0.0, -1.0, np.nan])

    report = labeler.class_distribution_report(labels)

    assert pytest.approx(sum(report.values())) == 100.0
    captured = capsys.readouterr()
    assert "Class distribution" in captured.out


def test_labeler_label_panel_independent_per_ticker():
    """label_panel must label each ticker using only that ticker's own path."""
    labeler = TripleBarrierLabeler(profit_multiplier=2.0, stop_multiplier=2.0, max_holding=3)
    dates = pd.date_range("2024-01-01", periods=6, freq="B")
    df = pd.DataFrame(
        {
            "date": list(dates) * 2,
            "ticker": ["UP"] * 6 + ["FLAT"] * 6,
            "close": [100, 100, 103, 100, 100, 100] + [100, 100.5, 100.8, 100.2, 100, 100],
            "atr_14": [1.0] * 12,
        }
    )

    labels = labeler.label_panel(df)

    assert labels.loc[df["ticker"] == "UP"].iloc[0] == 1.0
    assert labels.loc[df["ticker"] == "FLAT"].iloc[0] == 0.0


def test_labeler_label_panel_missing_column_raises():
    labeler = TripleBarrierLabeler()
    df = pd.DataFrame({"ticker": ["A"], "close": [100.0]})  # no atr_14
    with pytest.raises(ValueError):
        labeler.label_panel(df)


def test_labeler_rejects_non_positive_config():
    with pytest.raises(ValueError):
        TripleBarrierLabeler(profit_multiplier=0)
    with pytest.raises(ValueError):
        TripleBarrierLabeler(max_holding=0)
