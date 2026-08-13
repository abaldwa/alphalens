"""
tests/unit/test_rule_based_exit_policy.py

Phase: 3.x (Paper Trading Logic Fix — Exit Signal bootstrap)
Specs: SPEC-MODEL-002 (barrier convention), SPEC-SOLID-003 (predict_full contract)
Owner: Platform / QA
Consumers: CI, pytest

Mirrors tests/unit/test_exit_signal.py's "6 exit types / pnd override /
exit_type never null" coverage for RuleBasedExitPolicy — the mechanical
stand-in used to bootstrap closed-trade history before ExitSignalModel has
enough real data to train (see BuildLog.md "Paper Trading Logic Fix").
"""

import httpx
import pandas as pd
import pytest
import talib

from datastore.client import DataStoreClient
from systems.ml_signal_engine.models.exit.exit_signal import EXIT_TYPES, PND_EXIT_SCORE_THRESHOLD
from systems.ml_signal_engine.models.exit.rule_based_exit_policy import (
    ATR_PROFIT_MULTIPLIER,
    ATR_STOP_MULTIPLIER,
    RuleBasedExitPolicy,
    exit_criterion_text,
)


def _row(pnl_pct=0.0, days_held=5.0, drawdown=0.0, pnd_score=None):
    row = {"entry_price": 100.0, "days_held": days_held, "unrealised_pnl_pct": pnl_pct, "drawdown_from_peak": drawdown}
    if pnd_score is not None:
        row["pnd_score"] = pnd_score
    return pd.DataFrame([row], index=["TICK"])


class TestRuleBasedExitPolicy:
    def test_target_hit_classified_as_target_achieved_with_high_urgency(self):
        policy = RuleBasedExitPolicy(target_pct=0.15, stop_pct=-0.075, max_hold_days=21)
        out = policy.predict_full(_row(pnl_pct=0.20))
        assert out["exit_type"].iloc[0] == "target_achieved"
        assert out["exit_urgency"].iloc[0] >= 70.0

    def test_stop_hit_classified_as_thesis_broken_with_high_urgency(self):
        policy = RuleBasedExitPolicy(target_pct=0.15, stop_pct=-0.075, max_hold_days=21)
        out = policy.predict_full(_row(pnl_pct=-0.10))
        assert out["exit_type"].iloc[0] == "thesis_broken"
        assert out["exit_urgency"].iloc[0] >= 80.0

    def test_max_hold_reached_classified_as_opportunity_cost(self):
        policy = RuleBasedExitPolicy(target_pct=0.15, stop_pct=-0.075, max_hold_days=21)
        out = policy.predict_full(_row(pnl_pct=0.02, days_held=25.0))
        assert out["exit_type"].iloc[0] == "opportunity_cost"

    def test_drawdown_after_gain_classified_as_momentum_exhaustion(self):
        policy = RuleBasedExitPolicy(target_pct=0.15, stop_pct=-0.075, max_hold_days=21)
        out = policy.predict_full(_row(pnl_pct=0.05, days_held=10.0, drawdown=-0.12))
        assert out["exit_type"].iloc[0] == "momentum_exhaustion"

    def test_untriggered_position_gets_moderate_urgency_and_no_action_band(self):
        policy = RuleBasedExitPolicy(target_pct=0.15, stop_pct=-0.075, max_hold_days=21)
        out = policy.predict_full(_row(pnl_pct=0.02, days_held=3.0))
        assert 0 <= out["exit_urgency"].iloc[0] <= 60.0

    def test_pnd_score_above_threshold_forces_pnd_exit(self):
        policy = RuleBasedExitPolicy()
        out = policy.predict_full(_row(pnl_pct=0.20, pnd_score=PND_EXIT_SCORE_THRESHOLD + 25.0))
        assert out["exit_type"].iloc[0] == "pnd_exit"
        assert out["exit_urgency"].iloc[0] >= 85.0

    def test_pnd_score_below_threshold_does_not_force_override(self):
        policy = RuleBasedExitPolicy()
        out = policy.predict_full(_row(pnl_pct=0.20, pnd_score=PND_EXIT_SCORE_THRESHOLD - 10.0))
        assert out["exit_type"].iloc[0] != "pnd_exit"

    def test_exit_type_always_valid_and_non_null(self):
        policy = RuleBasedExitPolicy()
        rows = pd.concat([
            _row(pnl_pct=0.20), _row(pnl_pct=-0.10), _row(pnl_pct=0.02, days_held=25.0),
            _row(pnl_pct=0.05, days_held=10.0, drawdown=-0.12), _row(pnl_pct=0.0, days_held=1.0),
        ], ignore_index=False)
        rows.index = [f"T{i}" for i in range(len(rows))]
        out = policy.predict_full(rows)
        assert out["exit_type"].notna().all()
        assert out["exit_type"].isin(EXIT_TYPES).all()

    def test_output_columns_match_exit_signal_model_contract(self):
        """exit_action leads the contract as of STEP 4 (2026-08-13).

        Note how this assertion is written: it pins the exact column LIST,
        which is why it failed loudly when the contract gained a column instead
        of letting the change through unnoticed. That strictness is worth
        keeping — the same explicit-projection idiom appears at the end of
        every policy's predict_full and in this suite's own row-loop reference,
        and in all three places adding a column upstream was not enough to make
        it reach the caller. Two policies, including the default variant, would
        have shipped emitting no intent at all.
        """
        policy = RuleBasedExitPolicy()
        out = policy.predict_full(_row(pnl_pct=0.05))
        assert list(out.columns) == [
            "exit_action", "exit_urgency", "exit_type",
            "exit_survival_5d", "exit_survival_21d", "exit_survival_63d",
        ]
        assert out["exit_urgency"].between(0, 100).all()

    def test_missing_required_column_raises(self):
        policy = RuleBasedExitPolicy()
        with pytest.raises(ValueError):
            policy.predict_full(pd.DataFrame([{"entry_price": 100.0}], index=["TICK"]))

    def test_invalid_init_args_raise(self):
        with pytest.raises(ValueError):
            RuleBasedExitPolicy(target_pct=-0.1)
        with pytest.raises(ValueError):
            RuleBasedExitPolicy(stop_pct=0.05)
        with pytest.raises(ValueError):
            RuleBasedExitPolicy(max_hold_days=0)


class TestAtrScaledBarriers:
    """FutureDevelopment.md #28: target/stop are ATR-scaled per-row when
    `atr_pct` is present, and fall back to the flat TARGET_PCT/STOP_PCT
    bootstrap numbers when it isn't — verified against real OHLCV (no
    synthetic price series, per tests/quality/'s no-fabrication policy)."""

    def _row_with_atr(self, atr_pct, pnl_pct, days_held=5.0, drawdown=0.0):
        return pd.DataFrame(
            [{
                "entry_price": 100.0, "days_held": days_held, "unrealised_pnl_pct": pnl_pct,
                "drawdown_from_peak": drawdown, "atr_pct": atr_pct,
            }],
            index=["TICK"],
        )

    def test_atr_target_hit_uses_atr_scaled_threshold_not_flat(self):
        policy = RuleBasedExitPolicy()  # flat TARGET_PCT=0.15
        atr_pct = 0.02  # ATR_PROFIT_MULTIPLIER(2.0) * 0.02 = 0.04 target
        out = policy.predict_full(self._row_with_atr(atr_pct, pnl_pct=0.05))
        assert out["exit_type"].iloc[0] == "target_achieved"  # would be "opportunity_cost"-eligible only under flat

    def test_atr_stop_hit_uses_atr_scaled_threshold_not_flat(self):
        policy = RuleBasedExitPolicy()  # flat STOP_PCT=-0.075
        atr_pct = 0.02  # -ATR_STOP_MULTIPLIER(1.0) * 0.02 = -0.02 stop
        out = policy.predict_full(self._row_with_atr(atr_pct, pnl_pct=-0.03))
        assert out["exit_type"].iloc[0] == "thesis_broken"  # flat -7.5% stop would not have triggered at -3%

    def test_nan_atr_pct_falls_back_to_flat_percentages(self):
        policy = RuleBasedExitPolicy(target_pct=0.15, stop_pct=-0.075)
        out = policy.predict_full(self._row_with_atr(float("nan"), pnl_pct=0.05))
        assert out["exit_type"].iloc[0] != "target_achieved"  # 5% < flat 15% target

    def test_missing_atr_pct_column_still_works(self):
        policy = RuleBasedExitPolicy()
        out = policy.predict_full(_row(pnl_pct=0.20))
        assert out["exit_type"].iloc[0] == "target_achieved"

    def test_exit_criterion_text_atr_scaled_vs_flat(self):
        flat = exit_criterion_text(100.0)
        atr_scaled = exit_criterion_text(100.0, atr_pct=0.02)
        assert "15.0%" in flat and "-7.5%" in flat
        assert "4.0%" in atr_scaled and "-2.0%" in atr_scaled

    @pytest.mark.parametrize("ticker", ["RELIANCE", "TCS"])
    def test_atr_scaling_against_real_historical_ohlcv(self, ticker):
        """Real ATR(14) computed from real DataStore OHLCV (never fabricated)
        for two real NSE tickers, fed through the same target/stop math
        RuleBasedExitPolicy.predict_full() applies."""
        import pandas as pd_
        from datetime import datetime

        client = DataStoreClient()
        try:
            rows = client.get_ohlcv(ticker, datetime(2025, 1, 1), datetime(2025, 6, 1))
        except httpx.RequestError as exc:
            # ML20: this test needs a live DataStore API server (unlike the
            # rest of this repo's usual in-process TestClient(app) pattern —
            # rewriting onto TestClient(app) isn't viable here because
            # DataStoreClient is a real httpx client with no dependency-
            # injection seam for an ASGI transport). Skip cleanly when no
            # server is reachable instead of failing loud and indistinguishably
            # from a real regression (see BuildLog.md ML20).
            pytest.skip(f"DataStore API unreachable for real-OHLCV check: {exc}")
        if not rows or len(rows) < 30:
            pytest.skip(f"no real OHLCV available for {ticker} in this environment")
        df = pd_.DataFrame(rows)
        atr = talib.ATR(df["high"], df["low"], df["close"], timeperiod=14)
        entry_idx = 30
        entry_price = float(df["close"].iloc[entry_idx])
        atr_pct = float(atr.iloc[entry_idx]) / entry_price
        assert atr_pct > 0

        policy = RuleBasedExitPolicy()
        just_above_target = ATR_PROFIT_MULTIPLIER * atr_pct + 0.001
        out = policy.predict_full(self._row_with_atr(atr_pct, pnl_pct=just_above_target))
        assert out["exit_type"].iloc[0] == "target_achieved"

        just_below_stop = -ATR_STOP_MULTIPLIER * atr_pct - 0.001
        out2 = policy.predict_full(self._row_with_atr(atr_pct, pnl_pct=just_below_stop))
        assert out2["exit_type"].iloc[0] == "thesis_broken"
