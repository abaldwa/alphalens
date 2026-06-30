"""
tests/unit/test_fno_features.py

Phase: 2.3 (F&O Features + Signal63D + Full Phase 2 Feature Matrix)
Specs: SPEC-FEAT-004, SPEC-PIPE-001
Owner: Platform / QA
Consumers: CI, pytest

Tests features/fno_features.py using a fake DataStoreClient (SPEC-SOLID-005
— no real HTTP call), with synthetic F&O chains shaped exactly like the
real fno_data rows (ingestion/scrapers/fno.py, verified live against
NSE's UDiFF bhavcopy during this phase's build — see BuildLog.md "P2.3").
"""

from datetime import datetime
from unittest.mock import MagicMock

import numpy as np
import pytest

from features.fno_features import (
    FNO_FEATURES,
    _black_scholes_price,
    _implied_volatility,
    _max_pain,
    compute_fno_features,
    compute_fno_features_panel,
)

AS_OF = datetime(2026, 6, 22)
NEAR_EXPIRY = "2026-06-30"
FAR_EXPIRY = "2026-07-28"


def _row(
    instrument, expiry, oi, volume=100, settle_price=10.0, strike=None,
    option_type=None, oi_change=0, spot=1300.0,
):
    return {
        "trade_date": "2026-06-22",
        "ticker": "TEST",
        "instrument": instrument,
        "expiry": expiry,
        "strike": strike,
        "option_type": option_type,
        "oi": oi,
        "oi_change": oi_change,
        "volume": volume,
        "settle_price": settle_price,
        "close_price": settle_price,
        "underlying_price": spot,
    }


def _option_chain_rows(spot=1300.0, strikes=(1250, 1280, 1300, 1320, 1350), call_oi=None, put_oi=None):
    """A realistic CE+PE chain at one expiry, OI concentrated near the given strikes."""
    call_oi = call_oi or {s: 1000 for s in strikes}
    put_oi = put_oi or {s: 1000 for s in strikes}
    rows = []
    for s in strikes:
        rows.append(
            _row(
                "STO", NEAR_EXPIRY, oi=call_oi[s], settle_price=max(spot - s, 5.0),
                strike=s, option_type="CE", spot=spot,
            )
        )
        rows.append(
            _row(
                "STO", NEAR_EXPIRY, oi=put_oi[s], settle_price=max(s - spot, 5.0),
                strike=s, option_type="PE", spot=spot,
            )
        )
    return rows


class TestFNOFeatureCount:
    def test_sixteen_features(self):
        assert len(FNO_FEATURES) == 16


class TestNonFNOStockReturnsNaN:
    """A BSE SME stock (or anything with no F&O contracts) must return all-NaN, not an error."""

    def test_empty_chain_returns_all_nan(self):
        client = MagicMock()
        client.get_fno_chain.return_value = []

        feats = compute_fno_features(client, "BSESME", AS_OF)

        assert set(feats.keys()) == set(FNO_FEATURES)
        assert all(np.isnan(v) for v in feats.values())

    def test_panel_mixes_eligible_and_ineligible_tickers(self):
        client = MagicMock()

        def fake_chain(ticker, *_args, **_kwargs):
            if ticker == "ELIGIBLE":
                rows = [_row("STF", NEAR_EXPIRY, oi=10000, settle_price=1305.0, oi_change=500)]
                rows += _option_chain_rows()
                return rows
            return []

        client.get_fno_chain.side_effect = fake_chain
        panel = compute_fno_features_panel(client, ["ELIGIBLE", "BSESME"], AS_OF)

        assert list(panel["ticker"]) == ["ELIGIBLE", "BSESME"]
        assert panel.set_index("ticker").loc["BSESME"].isna().all()
        assert not panel.set_index("ticker").loc["ELIGIBLE"].isna().all()


class TestPcrOiRange:
    """pcr_oi must be in (0, 10] for any realistic chain."""

    def test_pcr_oi_in_valid_range(self):
        client = MagicMock()
        rows = [_row("STF", NEAR_EXPIRY, oi=50000, settle_price=1303.0, oi_change=100)]
        rows += _option_chain_rows(call_oi={1250: 2000, 1280: 3000, 1300: 5000, 1320: 1500, 1350: 800})
        client.get_fno_chain.return_value = rows

        feats = compute_fno_features(client, "TEST", AS_OF)

        assert feats["pcr_oi"] is not None and not np.isnan(feats["pcr_oi"])
        assert 0 < feats["pcr_oi"] <= 10

    def test_pcr_oi_skipped_not_fabricated_when_no_call_oi(self):
        client = MagicMock()
        rows = _option_chain_rows(call_oi={s: 0 for s in (1250, 1280, 1300, 1320, 1350)})
        client.get_fno_chain.return_value = rows

        feats = compute_fno_features(client, "TEST", AS_OF)

        assert np.isnan(feats["pcr_oi"])


class TestMaxPainNearATM:
    """max_pain_level must land within 5% of the ATM strike for a realistic, not-pathological chain."""

    def test_symmetric_chain_max_pain_within_5pct_of_atm(self):
        client = MagicMock()
        spot = 1300.0
        strikes = (1250, 1280, 1300, 1320, 1350)
        # Symmetric OI around the ATM strike (1300) -> max pain must land at/near 1300.
        rows = _option_chain_rows(
            spot=spot, strikes=strikes,
            call_oi={s: 5000 for s in strikes}, put_oi={s: 5000 for s in strikes},
        )
        client.get_fno_chain.return_value = rows

        feats = compute_fno_features(client, "TEST", AS_OF)

        atm_strike = min(strikes, key=lambda s: abs(s - spot))
        assert not np.isnan(feats["max_pain_level"])
        assert abs(feats["max_pain_level"] - atm_strike) / atm_strike <= 0.05

    def test_max_pain_algorithm_directly(self):
        """OI concentrated at one strike for both legs -> writers' total payout is minimized
        exactly there (both legs expire worthless at settlement == that strike)."""
        strikes = np.array([90.0, 100.0, 110.0])
        call_oi = np.array([0.0, 10000.0, 0.0])
        put_oi = np.array([0.0, 10000.0, 0.0])
        assert _max_pain(strikes, call_oi, put_oi) == 100.0


class TestBlackScholesIV:
    def test_iv_round_trips_through_bs_price(self):
        spot, strike, t_years, r, true_sigma = 1300.0, 1300.0, 0.25, 0.07, 0.22
        price = _black_scholes_price(spot, strike, t_years, r, true_sigma, is_call=True)

        recovered = _implied_volatility(price, spot, strike, t_years, r, is_call=True)

        assert recovered == pytest.approx(true_sigma, abs=1e-4)

    def test_non_positive_premium_returns_nan_not_fabricated(self):
        assert np.isnan(_implied_volatility(0.0, 1300.0, 1300.0, 0.25, 0.07, is_call=True))
        assert np.isnan(_implied_volatility(-5.0, 1300.0, 1300.0, 0.25, 0.07, is_call=True))

    def test_expired_option_returns_nan(self):
        assert np.isnan(_implied_volatility(10.0, 1300.0, 1300.0, 0.0, 0.07, is_call=True))


class TestOIBuildupUnwinding:
    def test_rising_oi_sets_buildup_flag(self):
        client = MagicMock()
        client.get_fno_chain.return_value = [_row("STF", NEAR_EXPIRY, oi=10000, oi_change=500, settle_price=1303.0)]

        feats = compute_fno_features(client, "TEST", AS_OF)

        assert feats["oi_buildup_flag"] == 1.0
        assert feats["oi_unwinding_flag"] == 0.0

    def test_falling_oi_sets_unwinding_flag(self):
        client = MagicMock()
        client.get_fno_chain.return_value = [_row("STF", NEAR_EXPIRY, oi=9000, oi_change=-500, settle_price=1303.0)]

        feats = compute_fno_features(client, "TEST", AS_OF)

        assert feats["oi_buildup_flag"] == 0.0
        assert feats["oi_unwinding_flag"] == 1.0


class TestRolloverAndBasis:
    def test_near_and_far_futures_compute_rollover_and_basis(self):
        client = MagicMock()
        client.get_fno_chain.return_value = [
            _row("STF", NEAR_EXPIRY, oi=20000, settle_price=1303.0, oi_change=200),
            _row("STF", FAR_EXPIRY, oi=8000, settle_price=1310.0, oi_change=50),
        ]

        feats = compute_fno_features(client, "TEST", AS_OF)

        assert feats["futures_basis_pct"] == pytest.approx((1303.0 - 1300.0) / 1300.0 * 100, abs=1e-6)
        assert feats["rollover_cost"] == pytest.approx(7.0, abs=1e-6)
        assert feats["rollover_pcr"] == pytest.approx(8000 / (20000 + 8000), abs=1e-6)

    def test_no_far_month_leaves_rollover_nan(self):
        client = MagicMock()
        client.get_fno_chain.return_value = [_row("STF", NEAR_EXPIRY, oi=20000, settle_price=1303.0, oi_change=200)]

        feats = compute_fno_features(client, "TEST", AS_OF)

        assert np.isnan(feats["rollover_cost"])
        assert np.isnan(feats["rollover_pcr"])
        assert not np.isnan(feats["futures_basis_pct"])
