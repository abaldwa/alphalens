"""
ingestion/quality/drift_monitor.py

Phase: 0.6 (Data Quality & Observability)
Specs: SPEC-PIPE-005, SPEC-ALERT-001
Owner: Platform / Ingestion
Consumers: ingestion/scheduler/daily_pipeline, ingestion/quality/baseline_runner,
    systems/ml_signal_engine

Population Stability Index (PSI) drift monitoring for feature matrices
(SPEC-PIPE-005: "PSI: top 50 features vs baseline; alert if > 0.10").
PSIMonitor compares each feature's current-day distribution against a
stored baseline distribution (config.settings.PSI_BASELINE_PATH), binned
using baseline-derived deciles, and classifies the result per
SPEC-ALERT-001: PSI > 0.10 -> 'warning' (halve position sizing),
PSI > 0.25 -> 'halt' (halt + retrain).

No scipy dependency: PSI only needs np.percentile/np.histogram, both
already available via numpy/pandas (SPEC-LIB-004: prefer an existing
dependency over adding a new one when it already covers the need).
"""

import logging
import pickle
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from config.settings import (
    PSI_BASELINE_PATH,
    PSI_COVERAGE_SHIFT_TOLERANCE,
    PSI_MIN_MONITORED_FEATURES,
    PSI_MODERATE_THRESHOLD,
    PSI_SEVERE_THRESHOLD,
    PSI_TOP_N_FEATURES,
)

logger = logging.getLogger(__name__)

PSI_N_BINS = 10  # SPEC-PIPE-005 doesn't pin a bin count; 10 deciles is the standard PSI convention
PSI_EPSILON = 1e-4  # floor for zero-proportion bins — avoids log(0) / division by zero


def _quantile_bin_edges(baseline_values: np.ndarray, n_bins: int) -> np.ndarray:
    """
    Decile (or n_bins-ile) edges derived from baseline_values, with the
    outer edges extended to +/-inf so any current value — even outside the
    historical baseline range — always falls inside a bin.
    """
    quantiles = np.linspace(0, 100, n_bins + 1)
    edges = np.unique(np.percentile(baseline_values, quantiles))
    if len(edges) < 3:
        # Degenerate baseline (near-constant feature): one bin covers everything.
        return np.array([-np.inf, np.inf])
    edges = edges.copy()
    edges[0] = -np.inf
    edges[-1] = np.inf
    return edges


def _bin_proportions(values: np.ndarray, edges: np.ndarray) -> np.ndarray:
    """Histogram values into edges, returning per-bin proportions floored at PSI_EPSILON."""
    counts, _ = np.histogram(values, bins=edges)
    proportions = counts / max(len(values), 1)
    return np.clip(proportions, PSI_EPSILON, None)


def _psi_from_proportions(baseline_pct: np.ndarray, current_pct: np.ndarray) -> float:
    """PSI = sum((current% - baseline%) * ln(current% / baseline%)) across bins."""
    return float(np.sum((current_pct - baseline_pct) * np.log(current_pct / baseline_pct)))


class PSIMonitor:
    """
    SPEC-PIPE-005: Population Stability Index drift detection.
    SPEC-ALERT-001: classifies PSI into 'ok' / 'warning' (>0.10) / 'halt' (>0.25).
    """

    def __init__(self, baseline_path: Optional[Path] = None, n_bins: int = PSI_N_BINS) -> None:
        """
        Parameters
        ----------
        baseline_path : Path, optional
            Defaults to config.settings.PSI_BASELINE_PATH.
        n_bins : int
            Number of quantile bins used to derive PSI (default 10 = deciles).
        """
        self.baseline_path = baseline_path or PSI_BASELINE_PATH
        self.n_bins = n_bins

    def compute_psi(
        self,
        feature_name: str,
        current_values,
        baseline_values,
        bin_edges: Optional[np.ndarray] = None,
    ) -> float:
        """
        Compute the Population Stability Index for one feature.

        Parameters
        ----------
        feature_name : str
            Used for logging only; does not affect the computation.
        current_values : array-like
            Today's (or this run's) values for the feature.
        baseline_values : array-like
            Historical baseline values for the feature.
        bin_edges : np.ndarray, optional
            Explicit bin edges to use instead of deriving deciles from
            baseline_values. Mainly used internally by check_drift(), which
            reuses bin_edges cached in stats_baseline.pkl rather than
            re-deriving them from raw baseline arrays on every call.

        Returns
        -------
        float
            PSI value. 0.0 if either array is empty after dropping NaNs.

        Spec References
        ----------------
        SPEC-PIPE-005: "PSI: top 50 features vs baseline; alert if > 0.10".

        PIT Assumptions
        ----------------
        None — PSI operates on already-computed, already-PIT-correct
        feature values; it does not itself perform any temporal join.

        Raises
        ------
        None
        """
        current = np.asarray(current_values, dtype=float)
        baseline = np.asarray(baseline_values, dtype=float)
        current = current[~np.isnan(current)]
        baseline = baseline[~np.isnan(baseline)]

        if len(baseline) == 0 or len(current) == 0:
            logger.warning(f"PSI[{feature_name}]: empty current or baseline array after dropping NaNs — returning 0.0")
            return 0.0

        edges = bin_edges if bin_edges is not None else _quantile_bin_edges(baseline, self.n_bins)
        baseline_pct = _bin_proportions(baseline, edges)
        current_pct = _bin_proportions(current, edges)
        psi = _psi_from_proportions(baseline_pct, current_pct)

        logger.debug(f"PSI[{feature_name}] = {psi:.4f}")
        return psi

    def classify(self, psi: float) -> str:
        """
        Classify a PSI value per SPEC-ALERT-001 / SPEC-PIPE-005 thresholds.

        Returns
        -------
        str
            'halt' if psi > PSI_SEVERE_THRESHOLD (0.25): halt + retrain.
            'warning' if psi > PSI_MODERATE_THRESHOLD (0.10): reduce
            position sizing 50%.
            'ok' otherwise.
        """
        if psi > PSI_SEVERE_THRESHOLD:
            return "halt"
        if psi > PSI_MODERATE_THRESHOLD:
            return "warning"
        return "ok"

    def compute_baseline(self, feature_matrix: pd.DataFrame, save: bool = True) -> Dict[str, dict]:
        """
        Compute per-feature baseline bin edges + proportions from a
        historical feature matrix, and persist to self.baseline_path.

        Parameters
        ----------
        feature_matrix : pd.DataFrame
            One column per feature, one row per (date, ticker) observation
            — typically ~2 years of history (see
            ingestion/quality/baseline_runner.py).
        save : bool
            If True (default), pickle the result to self.baseline_path
            (SPEC-SCHED-010: written via temp-file-then-rename, atomic).

        Returns
        -------
        dict
            {feature_name: {'bin_edges': np.ndarray, 'baseline_pct': np.ndarray}}.
            Columns that are entirely NaN are skipped.

        Spec References
        ----------------
        SPEC-PIPE-005.

        PIT Assumptions
        ----------------
        None — feature_matrix is assumed to already be PIT-correct
        (produced by features/matrix_builder.py); this function only
        computes summary statistics over it.

        Raises
        ------
        None
        """
        baseline: Dict[str, dict] = {}
        for col in feature_matrix.columns:
            values = pd.to_numeric(feature_matrix[col], errors="coerce").to_numpy(dtype=float)
            values = values[~np.isnan(values)]
            if len(values) == 0:
                logger.warning(f"PSI baseline: feature '{col}' is entirely NaN — skipped")
                continue
            edges = _quantile_bin_edges(values, self.n_bins)
            # [2026-08-13] non_null_share is recorded so check_drift() can
            # tell a genuine distribution shift apart from a COVERAGE
            # change. Live incident 2026-08-12: delivery_pct went from
            # 93-98% NULL to ~10% NULL when the delivery UPSERT fix landed,
            # and PSI (0.272) halted inference on what was really the data
            # getting BETTER — the baseline deciles had been derived from
            # the ~5% of rows that happened to have delivery data, so the
            # two populations were never comparable in the first place.
            baseline[col] = {
                "bin_edges": edges,
                "baseline_pct": _bin_proportions(values, edges),
                "non_null_share": float(len(values) / len(feature_matrix)) if len(feature_matrix) else 0.0,
            }

        if save:
            self.baseline_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self.baseline_path.with_suffix(self.baseline_path.suffix + ".tmp")
            with open(tmp_path, "wb") as f:
                pickle.dump(baseline, f)
            tmp_path.rename(self.baseline_path)  # SPEC-SCHED-010: atomic write (temp file, then rename)
            logger.info(f"PSI baseline saved: {len(baseline)} features -> {self.baseline_path}")

        return baseline

    def load_baseline(self) -> Dict[str, dict]:
        """
        Load the persisted baseline from self.baseline_path.

        Returns
        -------
        dict
            Same shape as compute_baseline()'s return value.

        Raises
        ------
        FileNotFoundError
            If self.baseline_path does not exist — the operator must run
            ingestion/quality/baseline_runner.py first.
        """
        if not self.baseline_path.exists():
            raise FileNotFoundError(
                f"PSI baseline not found at {self.baseline_path}. "
                "Run ingestion/quality/baseline_runner.py first."
            )
        with open(self.baseline_path, "rb") as f:
            return pickle.load(f)

    def check_drift(
        self,
        feature_matrix: pd.DataFrame,
        feature_names: Optional[List[str]] = None,
        baseline: Optional[Dict[str, dict]] = None,
    ) -> Dict[str, dict]:
        """
        Run the PSI drift check for a set of features against the stored
        (or supplied) baseline.

        Parameters
        ----------
        feature_matrix : pd.DataFrame
            Today's feature matrix (one column per feature).
        feature_names : list of str, optional
            Features to check. Defaults to the first PSI_TOP_N_FEATURES
            features present in both the baseline and feature_matrix
            (SPEC-PIPE-005: "top 50 features vs baseline").
        baseline : dict, optional
            Defaults to self.load_baseline().

        Returns
        -------
        dict
            {feature_name: {'psi': float, 'status': 'ok'|'warning'|'halt'}}.
            Features missing from either the baseline or feature_matrix are
            silently skipped (logged at debug level).

        Spec References
        ----------------
        SPEC-PIPE-005, SPEC-ALERT-001.

        PIT Assumptions
        ----------------
        None.

        Raises
        ------
        FileNotFoundError
            If baseline is None and self.baseline_path does not exist.
        """
        baseline = baseline if baseline is not None else self.load_baseline()
        available = [n for n in baseline if n in feature_matrix.columns]
        names = feature_names if feature_names is not None else available[:PSI_TOP_N_FEATURES]

        # [2026-08-13] A monitor that silently monitors almost nothing is
        # worse than no monitor: it manufactures confidence. The
        # baseline/feature-matrix intersection is where coverage silently
        # collapses — stats_baseline.pkl held only 3 features (a Phase 0.6
        # stand-in over ohlcv_adjusted that was never swapped for the
        # Phase 1 panel, see ingestion/quality/baseline_runner.py), two of
        # which ('return_1d', 'volume') do not exist in the feature panel
        # at all, so the "top 50 features" check was in fact checking
        # exactly ONE feature — and every halt/ok verdict rested on it.
        if len(available) < PSI_MIN_MONITORED_FEATURES:
            logger.error(
                "PSI check_drift: only %d feature(s) are checkable (%s) — below the "
                "PSI_MIN_MONITORED_FEATURES=%d floor. The baseline covers %d feature(s) and the "
                "matrix %d; drift monitoring is DEGRADED and its verdict must not be trusted as "
                "'top %d features'. Rebuild the baseline from the feature panel "
                "(ingestion/quality/baseline_runner.py).",
                len(available), available, PSI_MIN_MONITORED_FEATURES,
                len(baseline), len(feature_matrix.columns), PSI_TOP_N_FEATURES,
            )

        results: Dict[str, dict] = {}
        for name in names:
            if name not in baseline or name not in feature_matrix.columns:
                logger.debug(f"PSI check_drift: skipping '{name}' — not in baseline and/or feature_matrix")
                continue

            current = pd.to_numeric(feature_matrix[name], errors="coerce").to_numpy(dtype=float)
            current = current[~np.isnan(current)]
            if len(current) == 0:
                continue

            edges = baseline[name]["bin_edges"]
            baseline_pct = baseline[name]["baseline_pct"]
            current_pct = _bin_proportions(current, edges)
            psi = _psi_from_proportions(baseline_pct, current_pct)
            status = self.classify(psi)

            # [2026-08-13] Coverage guard. If the feature's non-null share
            # has moved materially since the baseline was built, today's
            # values and the baseline deciles describe DIFFERENT
            # populations, and the PSI between them measures the coverage
            # change rather than any market move. Downgrade to a
            # 'stale_baseline' warning instead of halting inference: a halt
            # must mean "the market changed", never "we started collecting
            # this field properly". Live case: delivery_pct, 2026-08-12.
            baseline_share = baseline[name].get("non_null_share")
            current_share = len(current) / len(feature_matrix) if len(feature_matrix) else 0.0
            coverage_shift = (
                abs(current_share - baseline_share) if baseline_share is not None else None
            )
            if coverage_shift is not None and coverage_shift > PSI_COVERAGE_SHIFT_TOLERANCE:
                logger.warning(
                    "PSI drift [stale_baseline] %s: PSI=%.4f, but non-null coverage moved "
                    "%.1f%% -> %.1f%% (shift %.1f%% > %.0f%% tolerance). The baseline and today's "
                    "values describe different populations; this is a baseline-refresh signal, "
                    "not market drift. Rebuild via ingestion/quality/baseline_runner.py.",
                    name, psi, baseline_share * 100, current_share * 100,
                    coverage_shift * 100, PSI_COVERAGE_SHIFT_TOLERANCE * 100,
                )
                status = "stale_baseline"

            results[name] = {"psi": psi, "status": status, "coverage_shift": coverage_shift}

            if status not in ("ok", "stale_baseline"):
                logger.warning(f"PSI drift [{status}] {name}: PSI={psi:.4f}")

        return results
