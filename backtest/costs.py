"""
backtest/costs.py

Phase: 1.4 (Labeling + Backtesting Infrastructure)
Specs: SPEC-BT-002
Owner: Platform / Backtest
Consumers: backtest/portfolio.py (Phase 1.6), backtest/integrity_checker.py (check_05_costs)

IndianTransactionCosts: full Indian equity *delivery* (CNC) round-trip
cost model — STT, exchange transaction charges, SEBI turnover fees, stamp
duty, brokerage, GST on (brokerage + exchange charges), plus a liquidity-
dependent slippage estimate. Rates below are typical 2024-era discount-
broker delivery-trade rates (documented per-component since several of
these are revised periodically by SEBI/exchanges/state stamp-duty
notifications — treat as configurable defaults, not frozen constants).
Intraday (MIS) trading is out of scope: this system holds positions
5-63 trading days (SPEC-MODEL-002), always delivery, never intraday.
"""

import logging
from typing import Optional

from config.settings import MIN_ADT_INR, SMALL_CAP_SLIPPAGE_PCT, TOTAL_ROUNDTRIP_COST

logger = logging.getLogger(__name__)


class IndianTransactionCosts:
    """
    All rates are fractions of trade turnover (price * quantity) unless
    noted otherwise. Defaults model a zero-brokerage discount broker on
    NSE equity delivery (e.g. Zerodha/Upstox-style delivery pricing) —
    pass a non-zero `brokerage_pct` for a full-service broker.

    Spec References
    ----------------
    SPEC-BT-002: "All 6 cost components: brokerage, STT, exchange, GST,
    stamp, slippage. Small-cap slippage: 0.30% for ADTV < ₹1Cr.
    Round-trip total: ~0.40-0.50%."
    """

    # STT (Securities Transaction Tax): delivery equity, both buy and sell sides.
    STT_PCT = 0.001  # 0.1%
    # NSE exchange transaction charges (cash equity delivery segment).
    EXCHANGE_TXN_PCT = 0.0000297
    # SEBI turnover fee: ~Rs 10 per crore of turnover.
    SEBI_TURNOVER_PCT = 0.000001
    # Stamp duty: buy side only, per the 2020 SEBI-standardized state stamp duty circular.
    STAMP_DUTY_BUY_PCT = 0.00015
    # GST on (brokerage + exchange transaction charges) only — not on STT/stamp duty.
    GST_PCT = 0.18
    # Default slippage for liquid (non-small-cap) names; SMALL_CAP_SLIPPAGE_PCT
    # (config/settings.py) applies instead when adtv_cr is below the threshold.
    DEFAULT_SLIPPAGE_PCT = 0.0009
    SMALL_CAP_ADTV_THRESHOLD_CR = 1.0

    def __init__(self, brokerage_pct: float = 0.0) -> None:
        if brokerage_pct < 0:
            raise ValueError("brokerage_pct must be >= 0")
        self.brokerage_pct = brokerage_pct

    def _slippage_pct(self, adtv_cr: Optional[float]) -> float:
        if adtv_cr is not None and adtv_cr < self.SMALL_CAP_ADTV_THRESHOLD_CR:
            return SMALL_CAP_SLIPPAGE_PCT
        return self.DEFAULT_SLIPPAGE_PCT

    def _one_side_cost(self, turnover: float, is_buy: bool) -> float:
        brokerage = turnover * self.brokerage_pct
        exchange = turnover * self.EXCHANGE_TXN_PCT
        sebi = turnover * self.SEBI_TURNOVER_PCT
        gst = (brokerage + exchange) * self.GST_PCT
        stt = turnover * self.STT_PCT
        stamp = turnover * self.STAMP_DUTY_BUY_PCT if is_buy else 0.0
        return brokerage + exchange + sebi + gst + stt + stamp

    def compute_roundtrip_cost(self, price: float, quantity: int, adtv_cr: Optional[float] = None) -> float:
        """
        Total round-trip (buy + sell) transaction cost in INR for one position.

        Parameters
        ----------
        price : float
            Entry price per share (INR). Cost is computed on entry-side
            turnover for both legs — a simplification (the exit price
            will differ in practice); acceptable for cost *estimation*,
            not exact post-trade accounting.
        quantity : int
            Number of shares.
        adtv_cr : float, optional
            Average daily traded value in INR crore, used to select the
            slippage rate (SPEC-BT-002: 0.30% slippage when ADTV < Rs 1Cr).
            If omitted, the default (non-small-cap) slippage rate is used.

        Returns
        -------
        float
            Total round-trip cost in INR (brokerage + STT + exchange +
            SEBI + stamp duty + GST + slippage, both legs).

        Raises
        ------
        ValueError
            If price <= 0 or quantity <= 0.
        """
        if price <= 0:
            raise ValueError("price must be positive")
        if quantity <= 0:
            raise ValueError("quantity must be positive")

        turnover = price * quantity
        buy_cost = self._one_side_cost(turnover, is_buy=True)
        sell_cost = self._one_side_cost(turnover, is_buy=False)
        slippage = turnover * self._slippage_pct(adtv_cr) * 2  # both legs

        return buy_cost + sell_cost + slippage

    def compute_roundtrip_cost_pct(self, price: float, quantity: int, adtv_cr: Optional[float] = None) -> float:
        """Same as compute_roundtrip_cost, expressed as a fraction of turnover (not INR)."""
        turnover = price * quantity
        return self.compute_roundtrip_cost(price, quantity, adtv_cr) / turnover

    def validate_against_settings(self, price: float = 1000.0, quantity: int = 100, tolerance: float = 0.003) -> bool:
        """
        SPEC-BT-002 / build prompt: "Validates against TOTAL_ROUNDTRIP_COST
        in settings.py". Computes the round-trip cost % for a representative
        liquid-stock trade and checks it's within `tolerance` of the
        configured TOTAL_ROUNDTRIP_COST (0.40-0.50% per SPEC-BT-002) — a
        sanity check that the rate table hasn't drifted into something
        implausible, not an exact-match requirement (real costs vary by
        trade size/liquidity).

        Parameters
        ----------
        price, quantity : float, int
            Representative liquid-stock trade used for the sanity check.
        tolerance : float
            Maximum allowed absolute difference from TOTAL_ROUNDTRIP_COST
            (as a fraction, e.g. 0.003 = 0.3 percentage points).

        Returns
        -------
        bool
            True if within tolerance of TOTAL_ROUNDTRIP_COST.
        """
        actual_pct = self.compute_roundtrip_cost_pct(price, quantity)
        within_tolerance = abs(actual_pct - TOTAL_ROUNDTRIP_COST) <= tolerance
        if not within_tolerance:
            logger.warning(
                f"IndianTransactionCosts roundtrip {actual_pct:.4f} differs from "
                f"TOTAL_ROUNDTRIP_COST {TOTAL_ROUNDTRIP_COST} by more than tolerance {tolerance}"
            )
        return within_tolerance

    def is_liquid_enough(self, adt_inr: float) -> bool:
        """SPEC-BT-001 rule 5 / config.settings.MIN_ADT_INR: liquidity floor check."""
        return adt_inr >= MIN_ADT_INR
