"""
systems/ml_signal_engine/models/signal/signal_63d.py

Phase: 2.3 (F&O Features + Signal63D + Full Phase 2 Feature Matrix)
Specs: SPEC-MODEL-002, SPEC-MODEL-003, SPEC-MODEL-004, SPEC-MODEL-008
Owner: ml_signal_engine / signal
Consumers: systems/ml_signal_engine/inference/retrain_phase2.py,
           systems/ml_signal_engine/models/signal/meta_labeler.py

M-03 (63d half): Signal Model 63d. Same "thin subclass of BaseSignalModel"
architecture as Signal5DModel/Signal21DModel — all stacking-ensemble,
quantile-regression, HPO, and threshold-tuning logic lives in
base_signal_model.py (SPEC-SOLID-002: shared architecture, one
implementation). signal_21d.py's own docstring explicitly deferred this
file: "63d model only trains after Phase 2 fundamentals are flowing" —
true as of this phase (P2.1 built screener.py fundamentals ingestion;
P2.3's matrix_builder.py now wires fundamental/governance/MF-holdings/
corp-action/F&O panels into the feature matrix this model trains on).

[AS BUILT] 02_models.md: "Barrier widths: 21d = 3x ATR, 63d = 5x ATR" —
same reconciliation as signal_5d.py/signal_21d.py: BaseSignalModel's
profit_multiplier/stop_multiplier default to TripleBarrierLabeler's own
SPEC-MODEL-002 defaults (2.0/1.0), constructor-overridable rather than
silently adopting the doc's per-horizon number; pass
profit_multiplier=5.0, stop_multiplier=5.0 explicitly at call sites that
want to match the doc's "5x ATR" worked example (retrain_phase2.py does).

[AS BUILT] SPEC-MODEL-008's retrain protocol ("Retrain trigger: when new
quarterly fundamentals are announced") is a *scheduling* concern (when to
call train_full() again), not something this model class enforces itself
— same separation as signal_21d.py's "monthly retrain" cadence, which
also lives in the scheduler/retrain script, not in the model file. No
code here decides when to retrain; systems/ml_signal_engine/inference/
retrain_phase2.py is the entry point an operator or scheduled job calls.

`predict_signals(X)` uses whatever feature set X was trained with — this
model imposes no fixed 268/236-column requirement of its own (it just
trains on `list(X.columns)`, like every other BaseSignalModel subclass);
features.matrix_builder.ALL_FEATURE_COLUMNS is the Phase 2 caller's
actual feature set.
"""

from systems.ml_signal_engine.models.signal.base_signal_model import BaseSignalModel

HORIZON_DAYS = 63


class Signal63DModel(BaseSignalModel):
    """M-03 (63d): Buy/Hold/Sell direction + Q10/Q50/Q90 return distribution over the next 63 trading days."""

    def __init__(self, profit_multiplier: float = 2.0, stop_multiplier: float = 1.0, **kwargs) -> None:
        super().__init__(
            horizon_days=HORIZON_DAYS,
            profit_multiplier=profit_multiplier,
            stop_multiplier=stop_multiplier,
            **kwargs,
        )
