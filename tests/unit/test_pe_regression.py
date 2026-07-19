"""
tests/unit/test_pe_regression.py

Covers systems/damodaran_valuation/relative/pe_regression.py:
  - min_peers gate (default raised to 20, see BuildLog Fix 13)
  - basic OLS fit correctness
  - value_gap() proxy-input defaults (eps_growth_3y/payout_ratio/beta)
"""

import numpy as np
import pandas as pd
import pytest

from systems.damodaran_valuation.relative.pe_regression import RelativePERegression


def _peer_df(n: int) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    eps_growth = rng.uniform(0.05, 0.25, n)
    payout = rng.uniform(0.1, 0.5, n)
    beta = rng.uniform(0.7, 1.3, n)
    pe = 15.0 + 40.0 * eps_growth + 5.0 * payout + 2.0 * beta
    return pd.DataFrame({
        "pe_ratio": pe,
        "eps_growth_3y": eps_growth,
        "payout_ratio": payout,
        "beta": beta,
    })


class TestMinPeersGate:
    def test_default_min_peers_is_20(self):
        reg = RelativePERegression()
        assert reg.min_peers == 20

    def test_fit_rejects_fewer_than_min_peers(self):
        reg = RelativePERegression(min_peers=20)
        with pytest.raises(ValueError, match="need at least 20"):
            reg.fit(_peer_df(10))

    def test_fit_accepts_exactly_min_peers(self):
        reg = RelativePERegression(min_peers=20)
        reg.fit(_peer_df(20))
        assert reg._n_peers == 20

    def test_custom_min_peers_override(self):
        reg = RelativePERegression(min_peers=5)
        reg.fit(_peer_df(5))
        assert reg._n_peers == 5


class TestFitCorrectness:
    def test_r_squared_high_for_near_linear_data(self):
        reg = RelativePERegression(min_peers=20)
        reg.fit(_peer_df(40))
        result = reg.value_gap({
            "pe_ratio": 20.0, "eps_growth_3y": 0.10, "payout_ratio": 0.2, "beta": 1.0,
        })
        assert result.r_squared > 0.95

    def test_value_gap_before_fit_raises(self):
        reg = RelativePERegression()
        with pytest.raises(RuntimeError, match="Call fit"):
            reg.value_gap({"pe_ratio": 20.0})


class TestProxyInputDefaults:
    """
    value_gap() silently substitutes 0.0/0.0/1.0 for missing
    eps_growth_3y/payout_ratio/beta (documented behavior, not a bug) —
    callers (valuation_engine.py, routers/valuation.py) are responsible
    for surfacing these substitutions via ValuationResult.proxy_used.
    """

    def test_missing_inputs_use_documented_defaults(self):
        reg = RelativePERegression(min_peers=20)
        reg.fit(_peer_df(20))
        result_missing = reg.value_gap({"pe_ratio": 20.0})
        result_explicit = reg.value_gap({
            "pe_ratio": 20.0, "eps_growth_3y": 0.0, "payout_ratio": 0.0, "beta": 1.0,
        })
        assert result_missing.predicted_pe == pytest.approx(result_explicit.predicted_pe)
