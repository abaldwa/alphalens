"""
systems/ml_signal_engine/models/signal/signal_21d.py

Phase: 1.5 (Core Signal Models)
Specs: SPEC-MODEL-002, SPEC-MODEL-003, SPEC-MODEL-004
Owner: ml_signal_engine / signal
Consumers: systems/ml_signal_engine/inference/train_all_phase1.py,
           systems/ml_signal_engine/models/signal/meta_labeler.py

M-03: Signal Model 21d. "Same architecture as Signal5D" per the build
prompt — thin subclass of BaseSignalModel fixing the triple-barrier
horizon to 21 trading days. signal_63d.py is explicitly out of scope for
this prompt (02_models.md: "63d model only trains after Phase 2
fundamentals are flowing" — no fundamentals are ingested yet).

[AS BUILT] 02_models.md notes "Barrier widths: 21d = 3x ATR" — same
reconciliation as signal_5d.py: barrier multipliers are constructor-
overridable, defaulting to TripleBarrierLabeler's own SPEC-MODEL-002
defaults (2.0/1.0) rather than silently adopting the doc's per-horizon
numbers; pass profit_multiplier=3.0, stop_multiplier=3.0 explicitly to
match the doc's worked example.
"""

from systems.ml_signal_engine.models.signal.base_signal_model import BaseSignalModel

HORIZON_DAYS = 21


class Signal21DModel(BaseSignalModel):
    """M-03: Buy/Hold/Sell direction + Q10/Q50/Q90 return distribution over the next 21 trading days."""

    def __init__(self, profit_multiplier: float = 2.0, stop_multiplier: float = 1.0, **kwargs) -> None:
        super().__init__(
            horizon_days=HORIZON_DAYS,
            profit_multiplier=profit_multiplier,
            stop_multiplier=stop_multiplier,
            **kwargs,
        )
