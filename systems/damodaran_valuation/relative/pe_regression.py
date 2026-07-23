"""
systems/damodaran_valuation/relative/pe_regression.py

Phase: 3
Specs: SPEC-VAL-002 (Model 5)
Owner: Platform / Valuation
Consumers: systems/damodaran_valuation/valuation_engine.py

Cross-sectional P/E regression relative valuation (SPEC-VAL-002 Model 5).

Fits an OLS model:
    PE = α + β₁·EPS_growth_3y + β₂·payout_ratio + β₃·beta

on sector peer data.  Predicted P/E vs actual P/E yields the valuation
gap — positive gap means the stock is expensive relative to peers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

import numpy as np
import pandas as pd


@dataclass
class RelativeValuationResult:
    """
    Relative valuation output from P/E regression (SPEC-VAL-002 Model 5).

    Attributes
    ----------
    actual_pe : float
        Current P/E ratio of the stock.
    predicted_pe : float
        Sector-regression-implied P/E.
    gap_pct : float
        (actual_pe − predicted_pe) / predicted_pe.
        Positive → stock appears expensive vs peers; negative → cheap.
    r_squared : float
        R² of the cross-sectional regression on peer data.
    n_peers : int
        Number of peers used in fitting.
    is_overvalued : bool
        True if gap_pct > 0.10 (> 10 % premium to fair value).
    coefficients : dict
        Regression coefficients {intercept, eps_growth_3y, payout_ratio, beta}.
    """

    actual_pe: float
    predicted_pe: float
    gap_pct: float
    r_squared: float
    n_peers: int
    is_overvalued: bool
    coefficients: Dict[str, float]


class RelativePERegression:
    """
    OLS P/E regression across sector peers (SPEC-VAL-002 Model 5).

    Usage
    -----
    >>> reg = RelativePERegression()
    >>> reg.fit(peer_df)          # peer_df: [pe_ratio, eps_growth_3y, payout_ratio, beta]
    >>> result = reg.value_gap({"pe_ratio": 25, "eps_growth_3y": 0.15,
    ...                         "payout_ratio": 0.30, "beta": 1.0})

    Parameters
    ----------
    overvalued_threshold : float
        Gap percentage above which a stock is flagged as overvalued (default 10 %).
    min_peers : int
        Minimum number of peers required to fit the regression (default 20).
        A 3-factor OLS (4 params incl. intercept) with only 5 peers leaves
        near-zero degrees of freedom and an unstable, inflated R²; Damodaran's
        own regression work typically uses 20-40+ peers.
    """

    def __init__(self, overvalued_threshold: float = 0.10, min_peers: int = 20) -> None:
        self.overvalued_threshold = overvalued_threshold
        self.min_peers = min_peers
        self._coef: Optional[np.ndarray] = None
        self._r2: Optional[float] = None
        self._n_peers: Optional[int] = None

    def fit(self, peer_data: pd.DataFrame) -> None:
        """
        Fit cross-sectional OLS regression on sector peers (SPEC-VAL-002 Model 5).

        Parameters
        ----------
        peer_data : pd.DataFrame
            Must contain columns:
              pe_ratio       — trailing P/E
              eps_growth_3y  — 3-year EPS CAGR (decimal)
              payout_ratio   — DPS / EPS (decimal)
              beta           — market beta

        Raises
        ------
        ValueError
            If fewer than ``min_peers`` complete rows are available.

        Notes
        -----
        Rows with any NaN in the four columns are dropped before fitting.
        """
        required = ["pe_ratio", "eps_growth_3y", "payout_ratio", "beta"]
        df = peer_data[required].dropna()

        if len(df) < self.min_peers:
            raise ValueError(
                f"RelativePERegression.fit: need at least {self.min_peers} complete peer rows, "
                f"got {len(df)}."
            )

        y = df["pe_ratio"].values.astype(float)
        X_raw = df[["eps_growth_3y", "payout_ratio", "beta"]].values.astype(float)
        # Add intercept column
        X = np.column_stack([np.ones(len(X_raw)), X_raw])

        # OLS: β = (X'X)^{-1} X'y
        try:
            self._coef, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
        except np.linalg.LinAlgError as exc:
            raise ValueError(f"OLS fit failed: {exc}") from exc

        y_hat = X @ self._coef
        ss_res = float(np.sum((y - y_hat) ** 2))
        ss_tot = float(np.sum((y - y.mean()) ** 2))
        self._r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else float("nan")
        self._n_peers = len(df)

    def value_gap(self, ticker_data: Dict) -> RelativeValuationResult:
        """
        Compute predicted P/E and valuation gap for one stock (SPEC-VAL-002 Model 5).

        Parameters
        ----------
        ticker_data : dict
            Keys: pe_ratio, eps_growth_3y, payout_ratio, beta.

        Returns
        -------
        RelativeValuationResult
            Actual vs predicted P/E with gap percentage.

        Raises
        ------
        RuntimeError
            If ``fit()`` has not been called yet.
        """
        if self._coef is None:
            raise RuntimeError("Call fit() before value_gap().")

        x = np.array([
            1.0,
            float(ticker_data.get("eps_growth_3y") or 0.0),
            float(ticker_data.get("payout_ratio") or 0.0),
            float(ticker_data.get("beta") or 1.0),
        ])
        predicted_pe = float(x @ self._coef)
        actual_pe = float(ticker_data.get("pe_ratio") or 0.0)
        gap_pct = (actual_pe - predicted_pe) / abs(predicted_pe) if predicted_pe != 0 else float("nan")

        coef_keys = ["intercept", "eps_growth_3y", "payout_ratio", "beta"]

        return RelativeValuationResult(
            actual_pe=actual_pe,
            predicted_pe=predicted_pe,
            gap_pct=gap_pct,
            r_squared=self._r2 or float("nan"),
            n_peers=self._n_peers or 0,
            is_overvalued=gap_pct > self.overvalued_threshold if np.isfinite(gap_pct) else False,
            coefficients=dict(zip(coef_keys, self._coef.tolist())),
        )
