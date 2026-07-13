"""
systems/ml_signal_engine_gainer/models/signal/gainer_signal_models.py

GAINER EXPERIMENT (uses a copy of base_signal_model.py, does not touch
production signal_5d/21d/63d). Three thin BaseSignalModel subclasses for
the user's literal fixed-% targets:
  - GainerSignal6DModel:  5% gain within 6 trading days
  - GainerSignal21DModel: 10% gain within 21 trading days
  - GainerSignal63DModel: 20% gain within 63 trading days

Labels come from FixedPercentLabeler (systems/ml_signal_engine_gainer/
training/labeling.py), a binary single-touch label, not the production
ATR-scaled triple barrier. Mapped onto BaseSignalModel's {-1, 0, 1}
convention as HOLD(0)/BUY(1) only — the "sell" side is unused since
there's no defined downside barrier for these targets (matches the
user's original "5% gain over next N days" spec, which is one-sided).
"""

from systems.ml_signal_engine_gainer.models.signal.base_signal_model import BaseSignalModel


class GainerSignal6DModel(BaseSignalModel):
    """5% gain within 6 trading days."""

    HORIZON_DAYS = 6
    TARGET_PCT = 0.05

    def __init__(self, **kwargs) -> None:
        super().__init__(horizon_days=self.HORIZON_DAYS, **kwargs)


class GainerSignal21DModel(BaseSignalModel):
    """10% gain within 21 trading days."""

    HORIZON_DAYS = 21
    TARGET_PCT = 0.10

    def __init__(self, **kwargs) -> None:
        super().__init__(horizon_days=self.HORIZON_DAYS, **kwargs)


class GainerSignal63DModel(BaseSignalModel):
    """20% gain within 63 trading days."""

    HORIZON_DAYS = 63
    TARGET_PCT = 0.20

    def __init__(self, **kwargs) -> None:
        super().__init__(horizon_days=self.HORIZON_DAYS, **kwargs)
