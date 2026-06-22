"""
systems/ml_signal_engine/models/signal/signal_5d.py

Phase: 1.5 (Core Signal Models)
Specs: SPEC-MODEL-002, SPEC-MODEL-003, SPEC-MODEL-004
Owner: ml_signal_engine / signal
Consumers: systems/ml_signal_engine/inference/train_all_phase1.py,
           systems/ml_signal_engine/models/signal/meta_labeler.py

M-02: Signal Model 5d. Thin subclass of BaseSignalModel fixing the
triple-barrier horizon to 5 trading days — all stacking-ensemble,
quantile-regression, HPO, and threshold-tuning logic lives in
base_signal_model.py (SPEC-SOLID-002: shared architecture, one
implementation).

[AS BUILT] 02_models.md's training snippet for M-02 uses profit_
multiplier=1.5/stop_multiplier=1.5 (vs. TripleBarrierLabeler's own
SPEC-MODEL-002 default of 2.0/1.0, set in P1.4). This prompt's text for
P1.5 doesn't specify barrier widths at all, just "horizon=5 days" — kept
BaseSignalModel's barrier multipliers as constructor-overridable
parameters defaulting to TripleBarrierLabeler's own SPEC-MODEL-002
defaults (2.0/1.0) rather than silently adopting the doc's different
numbers for this one horizon; pass profit_multiplier=1.5, stop_
multiplier=1.5 explicitly at call sites that want to match the doc's
worked example.
"""

from systems.ml_signal_engine.models.signal.base_signal_model import BaseSignalModel

HORIZON_DAYS = 5


class Signal5DModel(BaseSignalModel):
    """M-02: Buy/Hold/Sell direction + Q10/Q50/Q90 return distribution over the next 5 trading days."""

    def __init__(self, profit_multiplier: float = 2.0, stop_multiplier: float = 1.0, **kwargs) -> None:
        super().__init__(
            horizon_days=HORIZON_DAYS,
            profit_multiplier=profit_multiplier,
            stop_multiplier=stop_multiplier,
            **kwargs,
        )
