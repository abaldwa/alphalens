"""
backtest/core/engine.py

Phase: Unified Backtest & Paper Trading Umbrella, Phase 1
Owner: Platform / Backtest
Consumers: backtest/adapters/*.py (technical, fundamental, momentum —
Phase 2; ml_adapter.py wraps backtest/engine.py instead, see below),
backtest/walk_forward/day_driver.py (Phase 2.5), backtest/paper_trading
(Phase 5)

The shared orchestrator every channel's adapter plugs into, implementing
the Standard Backtesting Algorithm (BacktestUmbrellaPlan.md): point-in-
time universe construction, rebalance-date iteration, signal generation,
corporate-action/delisting reconciliation, horizon-bucket-driven position
sizing, SIP cash-flow injection, cost-aware execution, and standardized
metrics — the same loop regardless of channel.

Built NET-NEW rather than extracted from backtest/engine.py's
BacktestEngine (confirmed 2026-07-20, "backtest/engine.py: wrap, don't
refactor" — see BacktestUmbrellaPlan.md): that module is left completely
untouched. adapters/ml_adapter.py (Phase 2) wraps BacktestEngine as a
StrategyAdapter-conforming black box instead of this orchestrator driving
its internals directly.

No-Mock-Data Policy: this module never fabricates a price or feature
value. When price_lookup returns None for a ticker/date, that
ticker/date is EXCLUDED from the run and recorded in the returned
BacktestRunResult.data_gaps — never interpolated or defaulted.
"""

import csv
import logging
from dataclasses import dataclass, field
from datetime import date as date_type
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Literal, Optional, Protocol

import pandas as pd

from backtest.core.horizon import HorizonBucket, sizing_for
from backtest.instrumentation import NullTimer, RunTimer
from backtest.ledger_invariants import check_all
from systems.ml_signal_engine.models.exit.exit_intent import (
    EXIT_ACTION_EXIT,
    EXIT_ACTION_REDUCE,
    action_from_urgency,
    validate_actions,
)
from backtest.core.metrics import compute_metrics
from backtest.core.portfolio import (
    DEFAULT_N_TARGET_POSITIONS,
    AnnualResetConfig,
    SipConfig,
    StrategyPortfolio,
)
from backtest.portfolio import (
    EXIT_REDUCE_THRESHOLD,
    EXIT_URGENT_THRESHOLD,
    MONITOR_THRESHOLD,
    Position,
)
from backtest.core.regime_breakdown import compute_regime_breakdown
from backtest.core.run_context import BacktestRun, BacktestRunResult
from backtest.core.signal_ledger import SignalLedgerRecorder
from backtest.core.tax import fy_tax_cash_flows
from config.settings import MIN_ADT_INR

if TYPE_CHECKING:  # import-graph hygiene: runtime imports stay local (see below)
    from backtest.core.feature_log import FeatureLogWriter


class ExitModel(Protocol):
    """Structural type of the exit policies this engine drives (every
    systems/ml_signal_engine/models/exit/*Policy and ExitSignalModel).
    Typing-only: declared here so exit_model params/returns have an honest
    type without adding a runtime import of systems.ml_signal_engine, which
    the local-import comments below deliberately avoid."""

    def predict_full(self, X: pd.DataFrame) -> pd.DataFrame:
        ...

# Same directory the run_*_backtest.py scripts use for their
# ``orchestrator_{run_id}.json`` / ``phase*_{run_id}.json`` reports, so the
# trade-log CSV written below always lands next to the JSON report for a run.
REPORTS_DIR = Path(__file__).resolve().parent.parent / "reports"

logger = logging.getLogger(__name__)

# Effectively "no forced max-holding-days exit" for the orchestrator's default
# exit policy (explicit user requirement, 2026-07-24: positions run until a
# real stop-loss/target/urgency signal fires, never force-closed on elapsed
# days alone). RuleBasedExitPolicy's max_hold_days must be a positive int
# (its __init__ rejects <= 0 and float('inf') can't survive
# PerTemplateExitPolicy's int(...) cast), so instead of disabling the
# day-count barrier we set it to a sentinel far beyond any realistic
# backtest/paper-trading holding period — the stop/target/PnD barriers
# remain the only day-to-day triggers in practice.
_NO_MAX_HOLD_DAYS_SENTINEL = 10**9


def _build_default_exit_model(max_hold_days: int = _NO_MAX_HOLD_DAYS_SENTINEL) -> "ExitModel":
    """PerTemplateExitPolicy(build_default_template_params()), with every
    template's (and the untagged-position default's) max_hold_days
    replaced by `max_hold_days` (default _NO_MAX_HOLD_DAYS_SENTINEL — see
    that constant's docstring; 2026-08-01: now overridable so
    build_exit_model_for_variant's own max_hold_days param can sweep real
    day-count exit horizons instead of always disabling the barrier).
    Local imports: keeps engine.py's module-level import graph free of a
    hard dependency on systems/ml_signal_engine for callers (e.g. plain
    orchestration tests) that never touch exit_model."""
    from systems.ml_signal_engine.models.exit.per_template_exit_policy import (
        PerTemplateExitPolicy,
        build_default_template_params,
    )
    from systems.ml_signal_engine.models.exit.rule_based_exit_policy import RuleBasedExitPolicy

    template_params = {
        name: {**params, "max_hold_days": max_hold_days}
        for name, params in build_default_template_params().items()
    }
    # Bound to a typed local before returning (here and at every return in
    # build_exit_model_for_variant below): mypy runs with
    # follow_imports = "skip", so every systems.ml_signal_engine class is Any
    # to it and a bare `return Cls(...)` trips no-any-return. The binding is
    # purely a type assertion — no runtime behaviour changes.
    model: ExitModel = PerTemplateExitPolicy(
        template_params,
        default_policy=RuleBasedExitPolicy(max_hold_days=max_hold_days),
    )
    return model


# Exit regimes carried into the 2026-08 Technical re-run.
#
# "baseline" was RETIRED (2026-08-13), not merely deprioritised. It nominally
# had the same three barriers as "risk_managed", but emitted urgency bands that
# could not reach EXIT_URGENT_THRESHOLD for max-hold (<=65) or momentum
# exhaustion (<=79), so three of its four triggers were structurally dead:
# across 65 baseline runs and 108,762 model-driven exits, 0.00% were time
# exits. Keeping it would spend a full slice of every sweep re-measuring a
# known defect, and — worse — its results are not what its name claims, which
# is how they got read as a real stop/target/max-hold arm for months.
# "risk_managed" is its correct replacement: identical template parameters,
# urgency bands above the threshold, so the barriers actually fire.
#
# "regime_conditional" was also dropped from the carried set. It multiplies the
# grid by a whole dimension while answering a narrower question than the others
# (should barriers tighten in a bear market), and that question is only
# meaningful once the barriers themselves are known to work — which is exactly
# what this round establishes. The policy class is retained and can be re-added
# to a later sweep; only its place in the default grid is removed.
EXIT_POLICY_VARIANTS = (
    # The reference: no engine-imposed barrier of any kind, so every other
    # variant is measured as its improvement over letting the strategy signal
    # run. Also the ONLY variant whose trades can be used to DERIVE barriers
    # (backtest/derive_exit_params.py) — every other variant's trades are
    # truncated by its own barriers, making that derivation circular.
    "unconstrained",
    # Per-template stop/target/max-hold, all reachable, parameters derived from
    # the unconstrained runs' actual MAE/MFE rather than hand-chosen.
    "risk_managed",
    # "Exit when the entry thesis breaks", independent of P&L. NOTE: this has
    # never actually been exercised on the Technical channel — its 13 runs to
    # date were fundamental/momentum, where it needs a `template` column it
    # never receives, and it fired 0 exits in all of them. This round is its
    # first real test.
    "condition",
    # Barriers OR thesis-break, whichever comes first. Redefined 2026-08-13 to
    # compose risk_managed rather than the retired baseline.
    "combined",
    # Path-dependent: ratchets with price instead of capping at a fixed level,
    # so it is the one variant that can hold the right tail a fixed target cuts.
    "trailing",
    # Barriers scaled by ATR. Well motivated here specifically: median holding
    # period under no constraint clusters at 7 / 31 / 93 days across templates,
    # and a -5% stop costs 15% of winners in the short cluster but 60% in the
    # long one — a single fixed percentage cannot be right for all three.
    "atr_adaptive",
)

# Retired / not in the default grid, but still constructible by name so
# historical runs can be reproduced and so a targeted sweep can opt in.
RETIRED_EXIT_POLICY_VARIANTS = ("baseline", "regime_conditional")

ALL_EXIT_POLICY_VARIANTS = EXIT_POLICY_VARIANTS + RETIRED_EXIT_POLICY_VARIANTS

# Effectively-disabled stop/target bounds for the "unconstrained" variant
# below — RuleBasedExitPolicy requires target_pct > 0 and stop_pct < 0
# (no literal inf/0), so these are the widest bounds that still satisfy
# that validation: +1000% target, -99% stop. Paired with
# _NO_MAX_HOLD_DAYS_SENTINEL (already used by every other variant here),
# a position under this policy is only ever closed by the strategy's own
# signal-based exit — no engine-imposed stop/target/day-count barrier.
_UNCONSTRAINED_TARGET_PCT = 10.0
_UNCONSTRAINED_STOP_PCT = -0.99


def build_exit_model_for_variant(
    variant: str, regime_conn: Any = None, regime_index_name: str = "Nifty 500",
    max_hold_days: Optional[int] = None,
) -> "ExitModel":
    """Constructs the exit_model for one of EXIT_POLICY_VARIANTS — the
    factory backtest/run_orchestrator_backtest.py's --exit-variant CLI flag
    calls into. "baseline" (the default) reproduces today's
    _build_default_exit_model() exactly, so omitting --exit-variant is a
    no-op for every existing caller.

    max_hold_days : optional (2026-08-01, Technical-strategy timeframe
        sweep request) — overrides the day-count exit barrier for every
        variant that has one (baseline/condition-combined's default
        component/trailing/atr_adaptive/regime_conditional). None (the
        default) preserves exactly today's behavior: every one of those
        variants uses _NO_MAX_HOLD_DAYS_SENTINEL (i.e. the day-count
        barrier is effectively off, stop/target/PnD are the only day-to-day
        triggers) — this param was previously not exposed at all, so every
        existing caller omitting it is unaffected. Deliberately NOT applied
        to "unconstrained" (a fixed control variant whose whole point is no
        engine-imposed barrier of any kind) or "condition"
        (ConditionBasedExitPolicy has no day-count concept).

    Local imports: same rationale as _build_default_exit_model() above —
    keeps this module's import graph free of a hard systems.ml_signal_engine
    dependency for callers that never touch exit_model.
    """
    from systems.ml_signal_engine.models.exit.atr_adaptive_exit_policy import ATRAdaptiveExitPolicy
    from systems.ml_signal_engine.models.exit.composite_exit_policy import CompositeExitPolicy
    from systems.ml_signal_engine.models.exit.condition_based_exit_policy import ConditionBasedExitPolicy
    from systems.ml_signal_engine.models.exit.per_template_exit_policy import (
        PerTemplateExitPolicy,
        build_default_template_params,
    )
    from systems.ml_signal_engine.models.exit.regime_conditional_exit_policy import RegimeConditionalExitPolicy
    from systems.ml_signal_engine.models.exit.trailing_stop_exit_policy import TrailingStopExitPolicy

    hold_days = max_hold_days if max_hold_days is not None else _NO_MAX_HOLD_DAYS_SENTINEL

    if variant == "baseline":
        return _build_default_exit_model(hold_days)

    if variant == "condition":
        condition_model: ExitModel = ConditionBasedExitPolicy()
        return condition_model

    if variant == "combined":
        # Composes risk_managed, NOT the retired baseline: "barriers OR thesis
        # break" is only a meaningful arm if the barrier half can actually
        # fire, and baseline's could not.
        combined_model: ExitModel = CompositeExitPolicy(
            [build_exit_model_for_variant("risk_managed", max_hold_days=max_hold_days),
             ConditionBasedExitPolicy()]
        )
        return combined_model

    if variant == "trailing":
        # TrailingStopExitPolicy has no per-template router of its own yet
        # (unlike PerTemplateExitPolicy) — a single global TrailingStopExitPolicy
        # (bootstrap flat target/stop numbers) is used here; a per-template
        # trailing-stop router is a documented follow-up, not built today.
        trailing_model: ExitModel = TrailingStopExitPolicy(max_hold_days=hold_days)
        return trailing_model

    if variant == "atr_adaptive":
        atr_model: ExitModel = ATRAdaptiveExitPolicy(max_hold_days=hold_days)
        return atr_model

    if variant == "regime_conditional":
        template_params = {
            name: {**params, "max_hold_days": hold_days}
            for name, params in build_default_template_params().items()
        }
        regime_model: ExitModel = RegimeConditionalExitPolicy(template_params)
        return regime_model

    if variant == "risk_managed":
        # Per-template stop/target/max-hold, all reachable. Uses the same
        # template params as "baseline" so the two are directly comparable:
        # the only difference is whether the barriers can fire at all.
        from systems.ml_signal_engine.models.exit.risk_managed_exit_policy import RiskManagedExitPolicy

        template_params = {
            name: {**params, "max_hold_days": hold_days if max_hold_days is not None else params["max_hold_days"]}
            for name, params in build_default_template_params().items()
        }
        risk_managed_model: ExitModel = PerTemplateExitPolicy(
            template_params,
            default_policy=RiskManagedExitPolicy(),
            policy_cls=RiskManagedExitPolicy,
        )
        return risk_managed_model

    if variant == "unconstrained":
        # 2026-07-27: control variant for the CAGR-regression investigation
        # — no engine-imposed stop/target/day-count barrier (see
        # _UNCONSTRAINED_TARGET_PCT/_UNCONSTRAINED_STOP_PCT above), to test
        # whether the pre-per-template-exit-policy runs' higher CAGRs were
        # simply a strategy riding out drawdowns to a natural signal-based
        # exit rather than being stopped/target-capped early. max_hold_days
        # is intentionally NOT threaded in here — this variant's contract
        # is "no barrier of any kind", overriding that would defeat its
        # purpose as a fixed control.
        from systems.ml_signal_engine.models.exit.rule_based_exit_policy import RuleBasedExitPolicy as _RBEP

        unconstrained_model: ExitModel = _RBEP(
            target_pct=_UNCONSTRAINED_TARGET_PCT, stop_pct=_UNCONSTRAINED_STOP_PCT,
            max_hold_days=_NO_MAX_HOLD_DAYS_SENTINEL,
        )
        return unconstrained_model

    raise ValueError(
        f"unknown exit_policy_variant {variant!r}; must be one of {ALL_EXIT_POLICY_VARIANTS} "
        f"(carried grid: {EXIT_POLICY_VARIANTS}; retired but reproducible: {RETIRED_EXIT_POLICY_VARIANTS})"
    )


SignalAction = str  # "buy" | "sell" | "forced_close" | "hold"


@dataclass(frozen=True)
class Signal:
    ticker: str
    action: SignalAction
    sector: str = "Unknown"
    conviction: float = 0.0  # higher = prioritized first when capital is constrained
    adtv_cr: Optional[float] = None
    template: Optional[str] = None  # screener template name / fundamental preset that generated this signal (strategy identity for PerTemplateExitPolicy routing); None if the adapter doesn't track one
    # Per-ticker position-size multiplier applied on top of the portfolio's
    # own equal-weight slot (2026-08-05, Momentum volume-weighted sizing).
    # None = "no opinion, size normally" — every channel that doesn't set it
    # is completely unaffected.
    size_multiplier: Optional[float] = None
    # Regime exposure multiplier (0.0-1.0); set by orchestrator if regime_type is configured
    regime_exposure: Optional[float] = None
    # Regime name (BULL, BULL_WEAK, CHOPPY, CHOPPY_BEARISH, BEAR, UNDEFINED); for trade logging
    regime: Optional[str] = None
    # Nifty 50 RSI(14) at signal date; for regime analysis
    nifty_rsi_14: Optional[float] = None
    # Nifty 50 EMA(5) at signal date
    nifty_ema_5: Optional[float] = None
    # Nifty 50 EMA(10) at signal date
    nifty_ema_10: Optional[float] = None


class StrategyAdapter(Protocol):
    """The contract every channel adapter implements (BacktestUmbrellaPlan.md
    Architecture section). ml_adapter.py implements this by delegating to the
    existing, unmodified backtest/engine.py::BacktestEngine internally."""

    channel: str

    def generate_signals(self, universe: List[str], as_of_date: date_type, horizon_bucket: HorizonBucket) -> List[Signal]:
        ...

    def feature_vector(self, ticker: str, as_of_date: date_type) -> Dict[str, Any]:
        ...


PriceLookup = Callable[[str, date_type], Optional[float]]
TechnicalFeatureLookup = Callable[[str, date_type], Optional[Dict[str, Any]]]
UniverseProvider = Callable[[date_type], List[str]]
SectorLookup = Callable[[str], str]
IsDelistedCheck = Callable[[str, date_type], bool]


@dataclass(frozen=True)
class CorporateActionEvent:
    """One MERGER/SPINOFF event affecting a held ticker as of a given date
    (BacktestUmbrellaPlan.md Truthful Review Gap #4 fix, 2026-07-20).

    successor_ticker/swap_ratio are a documented EXTENSION POINT, not
    exercised logic today: no real ingestion pipeline in this codebase
    currently populates a successor ticker or swap ratio for a MERGER/
    SPINOFF corporate_actions row (corporate_actions.action_type is
    free-text VARCHAR with no MERGER/SPINOFF values produced by any real
    scraper as of this fix) — building stock-swap modeling against data
    that doesn't exist would be fabrication, not a fix. When both are
    real and present, BacktestOrchestrator swaps the position into the
    successor ticker at the given ratio; when either is missing (the
    only case reachable with real data today), it force-closes at the
    last known real price instead — the plan's own documented "mark-to-
    last-price-and-close" policy option.
    """

    action_type: str  # "MERGER" | "SPINOFF"
    successor_ticker: Optional[str] = None
    swap_ratio: Optional[float] = None


CorporateActionLookup = Callable[[str, date_type], Optional[CorporateActionEvent]]


@dataclass
class DataGap:
    ticker: str
    as_of_date: date_type
    reason: str


@dataclass
class RefitEvent:
    as_of_date: date_type
    model_version: str


def canonical_strategy_key(run: BacktestRun) -> Optional[str]:
    """"{channel}:{name}" -- the cross-application strategy identity (A89).

    `name` prefers the template/preset the run actually executed, because that
    is what the registry and the UI key on; strategy_id is the fallback, since
    it embeds a run date and would make every re-run look like a different
    strategy.

    A colon in the name would make the key ambiguous to split, so it is
    rejected rather than silently producing a key that parses to something
    else.
    """
    channel = getattr(run, "channel", None)
    # template_name/preset live in run.config, NOT as attributes on the run --
    # run_orchestrator_backtest puts them in the config dict. Reading only the
    # attributes (as this did) always fell through to strategy_id, so every
    # run was keyed "technical:ta_a1_63d_20260814" instead of "technical:A1".
    # That silently broke three things at once: ledger rows filed under a key
    # the registry does not know, strategy_version therefore unresolvable, and
    # the A103 readiness check unable to find the strategy at all.
    cfg = getattr(run, "config", None) or {}
    name = (
        # [ML40-2.4, 2026-08-15] The channel-agnostic declared name, written by
        # run_orchestrator_backtest as the run's `descriptor`. Tried FIRST and
        # for every channel, not just momentum: for technical and fundamental
        # it IS the template/preset, so those keys are unchanged, and momentum
        # -- which has neither -- stops falling through to strategy_id.
        #
        # That fall-through was the defect. strategy_id embeds a prefix, a
        # horizon and a run date (mom_..._5d_20260815), so every momentum run
        # was keyed to something the registry could never contain: measured
        # 2026-08-14, 10 of 93 ledger keys resolved to no row, all of them
        # momentum.
        cfg.get("strategy_name")
        or getattr(run, "template_name", None)
        or cfg.get("template_name")
        or getattr(run, "preset", None)
        or cfg.get("preset")
        or getattr(run, "strategy_id", None)
    )
    if not channel or not name:
        return None
    name = str(name)
    if ":" in name:
        logger.warning(
            "strategy name %r contains a colon; omitting strategy_key rather "
            "than emitting one that parses back to a different strategy", name,
        )
        return None
    return f"{channel}:{name}"


def resolve_strategy_version(strategy_key: Optional[str]) -> Optional[int]:
    """The strategy_registry version this run is executing, or None.

    Recorded so a result can be tied to the exact definition that produced it.
    Two things depend on getting this right, and both fail silently without it:

    * A deployment pins a version (strategy_deployments.strategy_version). If
      the run cannot say which version it tested, the deploy hand-off pins
      whatever is CURRENT at deploy time -- so a definition revised between
      backtest and deployment ships the new rules with the old evidence, which
      is exactly what AGENTS.md invariant 6 forbids.
    * Ledger rows keyed to the wrong version make a signal untraceable to the
      rules that emitted it.

    None (not a default of 1) when the strategy is absent from the registry:
    stamping a version number that was never executed is worse than admitting
    the run predates the registry, because it looks authoritative.
    """
    if not strategy_key:
        return None
    from config.settings import BACKTEST_DUCKDB_PATH
    from strategies.registry import get_strategy

    try:
        strategy = get_strategy(strategy_key, db_path=BACKTEST_DUCKDB_PATH)
    except Exception:  # registry table absent, DB locked, key unreadable
        # exception(), not warning(): a broad except around a registry read is
        # how "version could not be resolved" silently becomes the permanent
        # answer. The traceback is the only way to tell a missing table from a
        # genuine lookup miss.
        logger.exception(
            "Could not resolve a registry version for %r; the run will not "
            "record which strategy definition it executed.", strategy_key,
        )
        return None
    if strategy is None:
        return None
    version = strategy.get("version")
    return int(version) if version is not None else None


class ReadinessNotMet(RuntimeError):
    """A run was refused because its data prerequisites were unmet (A103)."""


def _enforce_run_readiness(
    *, run: BacktestRun, config: "OrchestratorConfig", strategy_key: str,
    strategy_version: Optional[int],
) -> None:
    """Probe a few dates across the run window; refuse if inputs are missing.

    Sampling, not exhaustive checking, is the point: the failure this exists
    to catch is a feature that was never backfilled across the window, and a
    handful of probes answers that as well as 860 would. A genuinely
    intermittent single-date hole is what `data_gaps` is for.
    """
    from backtest.core.readiness import ReadinessChecker, record_blocked

    # Explicit None checks, not truthiness: trading_days is a pandas
    # DatetimeIndex, and `index or []` raises "truth value is ambiguous".
    raw_days = getattr(config, "trading_days", None)
    days = list(raw_days) if raw_days is not None else []

    n = max(1, int(config.readiness_sample_dates))
    step = max(1, len(days) // n) if days else 1
    sample = days[::step][:n]

    # OrchestratorConfig exposes universe_provider(as_of), NOT a `universe`
    # attribute -- the universe is point-in-time and differs per date. An
    # earlier version of this function looked for config.universe, found
    # nothing, and logged "readiness not enforced" on every real run: the
    # gate was inert in exactly the case it was written for, and only the
    # smoke test caught it. Take the universe from the provider, per sampled
    # date, which is also the universe the generator will actually see.
    provider = getattr(config, "universe_provider", None)
    if not days or provider is None:
        # Nothing to probe against. Silence here would be indistinguishable
        # from a pass, so say so.
        logger.warning(
            "%s: readiness not enforced — no trading_days/universe_provider on the config",
            strategy_key,
        )
        return

    from strategies.registry import get_strategy
    from config.settings import BACKTEST_DUCKDB_PATH

    # A strategy that is not in the registry cannot have its prerequisites
    # derived (they come from its entry predicates), so there is nothing to
    # check. Momentum is the live case: its 1,200 registry rows are not keyed
    # by the run's strategy_id yet (A89/ML41). Say so once, rather than
    # raising ReadinessError on every sampled date.
    try:
        if get_strategy(strategy_key, db_path=BACKTEST_DUCKDB_PATH) is None:
            logger.info(
                "%s: not in strategy_registry — readiness not checked (prerequisites "
                "are derived from the registered entry predicates)", strategy_key,
            )
            return
    except Exception:
        logger.exception("%s: registry lookup failed; skipping readiness", strategy_key)
        return

    checker = ReadinessChecker()

    unready = []
    for as_of in sample:
        as_of_date = as_of.date() if hasattr(as_of, "date") else as_of
        try:
            universe = list(provider(as_of_date) or [])
            if not universe:
                # A date with no tradeable universe is a real condition (before
                # ADTV history exists, for instance), not an unready one.
                continue
            verdict = checker.check(
                run.channel, as_of_date, universe=universe, strategy_key=strategy_key,
            )
        except Exception:
            logger.exception(
                "%s: readiness check failed to run for %s — not treating that as a pass",
                strategy_key, as_of,
            )
            continue
        if not verdict.ready:
            unready.append(verdict)

    if not unready:
        return

    detail = "; ".join(m.detail for v in unready for m in v.missing)
    for verdict in unready:
        try:
            record_blocked(
                verdict, strategy_key=strategy_key,
                strategy_version=strategy_version or 0,
                db_path=config.signal_ledger_db_path,
            )
        except Exception:
            logger.exception("Could not record the blocked run for %s", strategy_key)

    message = (
        f"{strategy_key}: {len(unready)} of {len(sample)} sampled dates are missing "
        f"required inputs — {detail}. The run would complete and report a plausible "
        "number computed on a silently reduced universe."
    )
    if config.readiness_is_fatal:
        raise ReadinessNotMet(message)
    logger.error("%s (continuing: readiness_is_fatal=False)", message)


def _curve_to_rows(curve: Optional[pd.Series]) -> List[Dict[str, Any]]:
    """A pandas equity curve -> [{"date": "YYYY-MM-DD", "equity": float}].

    Deliberately the same shape, key and sampling as the strategy
    `equity_curve` built inline below, so the two series can be zipped by date
    without either side guessing at the other's convention. Downsampling for
    display is the report layer's job (A83), not the engine's -- an engine
    that thins the series decides for every future consumer how much detail
    they are allowed.

    NaN points are dropped rather than serialised as nulls: a gap in the index
    series is missing data, and emitting it as a point invites a chart to draw
    a line through it.
    """
    if curve is None or len(curve) == 0:
        return []
    return [
        {"date": ts.date().isoformat(), "equity": float(value)}
        for ts, value in curve.items()
        if value == value
    ]


@dataclass
class OrchestratorConfig:
    trading_days: pd.DatetimeIndex
    universe_provider: UniverseProvider
    price_lookup: PriceLookup
    sector_lookup: SectorLookup = field(default=lambda ticker: "Unknown")
    is_delisted: Optional[IsDelistedCheck] = None
    corporate_action_lookup: Optional[CorporateActionLookup] = None
    rebalance_cadence_days: Optional[int] = None  # None -> use horizon_bucket's default
    refit_cadence_days: Optional[int] = None  # Walk-Forward retrain cadence; None -> never refit (plain backtest)
    # REV17 (2026-07-21 review): signals generated at as_of_date were always
    # filled at that SAME day's own close — an undocumented, silent
    # simplification that overstates fill quality (the signal couldn't
    # actually have been acted on until the price was already known).
    # Default unchanged (same_day_close) for full backward compatibility;
    # "next_day_open" is the explicit, tested alternative this review asked
    # for, so the convention is now a decided, visible choice rather than a
    # silent one — see _resolve_execution_date below.
    execution_timing: Literal["same_day_close", "next_day_open"] = "same_day_close"
    # STEP 3b: collect per-phase wall-clock timings (backtest/instrumentation.py).
    # Default True — the overhead is a perf_counter pair per phase and the
    # measurement is the whole point of the speed work. NOT related to
    # execution_timing directly above, which is a fill-price policy.
    collect_timings: bool = True
    # (ticker, date) -> True when that bar was circuit-locked. A locked bar
    # has no opposing side, so a fill at its price is money the simulation
    # grants itself. None disables the check, preserving prior behaviour for
    # every caller that does not supply one.
    circuit_locked_lookup: Optional[Callable[[str, date_type], bool]] = None
    # Force-close an open position after this many CONSECUTIVE trading days
    # with no bar for it. None disables the check (prior behaviour: the
    # position is carried at its last known price for as long as the data is
    # missing, however long that is).
    max_blackout_sessions: Optional[int] = None
    # A94: persist every emitted signal to the strategy_signals ledger, so a
    # trade can be traced back to the signal that produced it. Default ON —
    # a ledger that is only populated when someone remembers to ask for it
    # is not an audit trail. Set False for throwaway runs (unit tests,
    # parameter probes) where the DB write is pure overhead.
    #
    # Writes are buffered for the WHOLE run and flushed once at the end
    # (backtest/core/signal_ledger.py) rather than per rebalance date —
    # ~860 lock acquisitions per job would reintroduce exactly the DuckDB
    # write-lock contention defer_db_writes exists to avoid.
    persist_signals: bool = True
    # None -> strategies.signals' own default, BACKTEST_DUCKDB_PATH (where
    # the A94 tables live, alongside backtest_runs). Tests point this at a
    # tmp_path DB; nothing writes signals to the real database in a test.
    signal_ledger_db_path: Optional[Any] = None
    # Which strategy_registry version this run executed. None means "ask the
    # registry" (resolve_strategy_version). Set it only to OVERRIDE that —
    # e.g. replaying a historical version deliberately. It used to default to
    # a literal 1, which stamped every ledger row and every deploy hand-off
    # with a version the run had not necessarily executed.
    signal_ledger_strategy_version: Optional[int] = None
    # A103. Checked once at run start (see _enforce_run_readiness), not per
    # rebalance date. Default ON: a backtest that silently screened a reduced
    # universe produces a plausible number that drives a deploy decision.
    enforce_readiness: bool = True
    # How many dates across the window to sample. The question being asked --
    # "was this indicator ever backfilled here?" -- is answered by a handful
    # of probes; 860 would answer it 860 times at 860x the cost.
    readiness_sample_dates: int = 5
    # Raise on an unready run, vs log loudly and continue. Default False:
    # aborting mid-sweep would fail a 400-job queue on its first strategy
    # whose feature happened to be sparse. The refusal is always recorded.
    readiness_is_fatal: bool = False
    # 2026-08-14. "daily" = the exit policy is evaluated every trading day,
    # independent of rebalance cadence (the original behaviour, and correct
    # for Technical/ML, whose signals ARE daily). "rebalance" = it is
    # evaluated only on rebalance dates, alongside rotation.
    #
    # Momentum must use "rebalance". Momentum is a periodic strategy: it ranks
    # the universe every N days and holds the top_n until the next ranking.
    # Under a daily exit pass a position stopped out on day 3 left its slot
    # empty for the remaining 60 days of a 63-day cadence, because entries are
    # only ever created on rebalance dates. Measured on 2026-08-14: lb6
    # top_n=10 held a MEDIAN of 1 position and a mean of 1.55; lb3 top_n=10
    # held a mean of 3.86. Those runs' CAGRs describe a book that was 15-40%
    # deployed with the rest idle in cash, which is not the strategy anyone
    # configured or read the results of.
    exit_policy_cadence: Literal["daily", "rebalance"] = "daily"
    # [2026-08-18, user decision] How many equal-weight slots the book is
    # divided into. None -> the adapter's own top_n, which is what a top-N
    # strategy means by "a slot"; only if the adapter declares no top_n does
    # StrategyPortfolio's default of 10 apply.
    #
    # Until now the orchestrator never passed this, so EVERY strategy sized
    # for 10 slots regardless of its top_n: a top_n=15 strategy computed
    # 10% equal-weight slots for a book it intended to split 15 ways.
    n_target_positions: Optional[int] = None
    # Per-run overrides onto the horizon bucket's HorizonSizingPolicy
    # (max_position_pct / max_sector_pct / min_holding_days). None keeps the
    # bucket's defaults, which is right for Technical/Fundamental/ML.
    #
    # Momentum overrides both caps by explicit user decision (2026-08-18):
    # it is fully invested (investable_pct=1.0, so the equal-weight 1/top_n
    # slot must not be clipped by a 3% ceiling), and it takes NO sector
    # diversification — its risk control is the universe itself, the top 800
    # names by ADTV. See run_orchestrator_backtest.py::_sizing_overrides_for.
    sizing_overrides: Optional[Dict[str, Any]] = None


class BacktestOrchestrator:
    """
    Runs one BacktestRun end-to-end against one adapter, per the Standard
    Backtesting Algorithm. Stateless across runs — construct fresh (or
    reuse) per call to run(); all mutable state lives in the
    StrategyPortfolio created inside run().
    """

    def __init__(
        self, feature_log_writer: Optional["FeatureLogWriter"] = None, regime_conn: Any = None,
        regime_index_name: str = "Nifty 500",
        exit_model: Optional["ExitModel"] = None,
        technical_feature_lookup: Optional[TechnicalFeatureLookup] = None,
        exit_policy_variant: Optional[str] = None,
        regime_method: Optional[str] = None, benchmark_index_name: Optional[str] = None,
        regime_type: Optional[str] = None,
    ) -> None:
        """feature_log_writer: optional backtest.core.feature_log.FeatureLogWriter.
        None is valid — orchestration/metrics tests that don't need a live
        DuckDB connection can omit it; production callers always supply one.

        regime_conn: optional read-only DuckDB connection to config.settings.
        DUCKDB_PATH (the normalised-schema DB market_regimes lives in — a
        DIFFERENT file from BACKTEST_DUCKDB_PATH, so this is deliberately a
        second connection, not the same one feature_log_writer uses). When
        given, the result's regime_breakdown is populated
        (backtest/core/regime_breakdown.py); when None (the default), it's
        left empty — regime breakdown is opt-in, not required for every run.

        exit_model: optional object implementing predict_full(exit_ctx_df) ->
        DataFrame[exit_urgency, ...] (same contract as PerTemplateExitPolicy/
        RuleBasedExitPolicy/ExitSignalModel). Checked EVERY trading day (not
        just rebalance dates) against every open position, independent of
        the rebalance-cadence rotation logic (see run()'s daily exit-policy
        pass). Defaults to PerTemplateExitPolicy(build_default_template_
        params()) with the max-holding-days barrier disabled (this IS the
        intended production default now, not an opt-in) — pass an explicit
        exit_model only to override it (e.g. tests, or a future ML
        ExitSignalModel once trained).

        technical_feature_lookup: optional Callable[[str, date], Dict[str,
        float]] returning that ticker's real technical indicator snapshot
        (sma_200_ratio, rsi_14, adx_14, macd_hist, etc. — the SAME values
        TechnicalAdapter/ScreenerEngine already read from the daily feature
        Parquet for entry screening, see backtest/run_orchestrator_backtest.
        py's build_technical_feature_lookup()) as of that date. When given,
        every column it returns is merged into the daily exit_ctx passed to
        exit_model.predict_full() — this is how ConditionBasedExitPolicy
        (systems/ml_signal_engine/models/exit/condition_based_exit_policy.
        py) gets the live indicator values it needs to re-check each
        template's own entry conditions. None (the default) is valid — the
        exit_ctx simply won't carry those columns, and any exit policy that
        references them (only ConditionBasedExitPolicy today) treats the
        corresponding rules as never-triggered rather than erroring
        (see that policy's docstring) — no behavior change for existing
        callers that don't pass this.

        exit_policy_variant: optional label naming which of EXIT_POLICY_VARIANTS
        produced `exit_model` (e.g. via build_exit_model_for_variant()) — purely
        descriptive, carried through onto BacktestRunResult.exit_policy_variant
        for experiment comparison in the backtest_runs table. None (the default)
        is valid — callers that construct exit_model directly rather than via
        the variant factory simply get a NULL exit_policy_variant in the saved
        run row.

        regime_method: which market_regimes classification threshold to use
        for BOTH the exit-policy `regime` column (RegimeConditionalExitPolicy)
        and regime_breakdown reporting - e.g. "20pct_threshold_v1" (default,
        systems.regime.market_regime.METHOD_NAME), "15pct_threshold_v1",
        "10pct_threshold_v1", "5pct_threshold_v1" (backfilled by
        scripts/backfill_market_regimes.py). None (the default) resolves to
        METHOD_NAME at first use - lets experiment queues re-run the SAME
        strategy/exit-variant combo against every threshold to see how
        regime-sensitivity assumptions hold up.
        """
        self._feature_log_writer = feature_log_writer
        self._regime_conn = regime_conn
        self._regime_index_name = regime_index_name
        # A98: the index a strategy is COMPARED AGAINST is a different
        # parameter from the index its REGIME is detected on. One name drove
        # both, so choosing a fairer benchmark for a midcap strategy also
        # silently changed which periods it was allowed to trade in — the
        # comparison and the results moved together and the run was no longer
        # the same experiment.
        #
        # Defaults to the regime index so existing callers are unaffected,
        # and every run records which index it actually used.
        self._benchmark_index_name = benchmark_index_name or regime_index_name
        self._regime_method = regime_method
        self._exit_model = exit_model if exit_model is not None else _build_default_exit_model()
        self._technical_feature_lookup = technical_feature_lookup
        self._exit_policy_variant = exit_policy_variant
        self._regime_type = regime_type
        # Bull/Bear/Sideways segments (systems/regime/regime_store.
        # list_regime_segments), fetched once and cached for the life of
        # this orchestrator instance — used to tag each exit_ctx row with
        # its day's `regime` for RegimeConditionalExitPolicy. None until the
        # first _regime_for_date() call (lazy: a run with no regime_conn,
        # or whose exit_model never looks at `regime`, never pays this
        # query's cost).
        self._regime_segments_cache: Optional[List[Dict[str, Any]]] = None
        # EMA-RSI regime data, lazily loaded if regime_type is set
        self._regime_data: Optional[pd.DataFrame] = None
        # Date → RegimeData cache for trade logging; populated during run() after _load_regime_data()
        self._regime_cache: Dict[date_type, Any] = {}
        # {as_of_date: {ticker: rank}} — a genuinely point-in-time market-cap
        # rank map, one entry per DISTINCT buy date actually encountered (not
        # per ticker, not per trade), lazily computed and cached the first
        # time any ticker needs a rank for that date so every other buy on
        # the same rebalance date reuses the same day's rank map instead of
        # re-querying PIT fundamentals/prices per trade. See
        # _get_market_cap_rank_for_date()'s docstring.
        self._market_cap_rank_maps_by_date: Dict[Any, Dict[str, int]] = {}
        # Count of exit decisions resolved through the legacy urgency->action
        # mapping because the policy emitted no exit_action. Non-zero means a
        # policy is still on the contract that lost max-hold and
        # momentum-exhaustion exits entirely, so it is surfaced rather than
        # left for a reader to infer.
        self._legacy_urgency_fallbacks: int = 0

    def _load_regime_data(self, start_date: date_type, end_date: date_type) -> None:
        """Load regime data from feature store if regime_type is configured.
        Cached in _regime_data for the life of this orchestrator instance."""
        if self._regime_type is None or self._regime_data is not None:
            return

        try:
            from config.regime_features import REGIME_REGISTRY

            if self._regime_type not in REGIME_REGISTRY:
                logger.error(f"Regime type {self._regime_type!r} not found in registry")
                return

            loader = REGIME_REGISTRY[self._regime_type]
            self._regime_data = loader(start_date, end_date)
            logger.info(f"Loaded {len(self._regime_data)} regime records for {self._regime_type}")
        except Exception as e:
            logger.error(f"Failed to load regime data: {e}", exc_info=True)

    def get_regime_for_date(self, as_of_date: date_type) -> Optional[Any]:
        """Lookup regime for a specific date. Returns None if no regime_type is configured or date not found."""
        if self._regime_type is None or self._regime_data is None:
            return None

        try:
            from config.regime_features import RegimeData

            # Convert date to Timestamp for comparison (DataFrame has datetime64[ns])
            target_date = pd.Timestamp(as_of_date)
            row = self._regime_data[self._regime_data['date'] == target_date]
            if row.empty:
                return None

            row = row.iloc[0]
            return RegimeData(
                regime=row['regime'],
                exposure=row['exposure'],
                ema_5=row['ema_5'],
                ema_10=row['ema_10'],
                rsi_14=row['rsi_14'],
                date=row['date'].date() if hasattr(row['date'], 'date') else row['date'],
            )
        except Exception as e:
            logger.error(f"Failed to lookup regime for {as_of_date}: {e}", exc_info=True)
            return None

    def apply_regime_to_signals(self, signals: List[Signal], as_of_date: date_type) -> List[Signal]:
        """Add regime exposure and state to signals. Returns signals unchanged if no regime_type is configured.
        Since Signal is frozen, this creates new Signal objects with regime fields set."""
        if self._regime_type is None:
            return signals

        regime = self.get_regime_for_date(as_of_date)
        if regime is None:
            return signals

        # Reconstruct signals with regime fields added
        enriched_signals = []
        for signal in signals:
            # Create new signal with regime fields
            enriched = Signal(
                ticker=signal.ticker,
                action=signal.action,
                sector=signal.sector,
                conviction=signal.conviction,
                adtv_cr=signal.adtv_cr,
                template=signal.template,
                size_multiplier=signal.size_multiplier,
                regime_exposure=regime.exposure,
                regime=regime.regime,
                nifty_rsi_14=regime.rsi_14,
                nifty_ema_5=regime.ema_5,
                nifty_ema_10=regime.ema_10,
            )
            enriched_signals.append(enriched)

        return enriched_signals

    def _populate_regime_cache(self) -> None:
        """Populate _regime_cache from _regime_data for fast trade-log lookups.
        Call after _load_regime_data() is successful."""
        if self._regime_data is None or self._regime_data.empty:
            return

        from config.regime_features import RegimeData

        for _, row in self._regime_data.iterrows():
            regime_date = row['date'].date() if hasattr(row['date'], 'date') else row['date']
            self._regime_cache[regime_date] = RegimeData(
                regime=row['regime'],
                exposure=row['exposure'],
                ema_5=row['ema_5'],
                ema_10=row['ema_10'],
                rsi_14=row['rsi_14'],
                date=regime_date,
            )

    def _get_market_cap_rank_for_date(self, ticker: str, as_of_date: date_type, all_buy_tickers_this_date: Optional[List[str]] = None) -> Optional[int]:
        """Point-in-time {ticker: rank} for `as_of_date`, batched by date and
        cached per orchestrator instance (config.universe.
        get_market_cap_rank_map_as_of, ranked by shares_outstanding * close
        as actually knowable on as_of_date — no lookahead). Requires
        self._regime_conn (the normalised-schema DB connection hosting both
        fundamentals_history and ohlcv_adjusted — see __init__'s docstring);
        with no regime_conn, or if a ticker has no PIT shares_outstanding/
        price as of as_of_date, this returns None rather than falling back
        to any static/current-snapshot rank — a silent fallback would
        reintroduce the exact lookahead bug this replaces get_market_cap_
        rank_map() to fix."""
        if as_of_date not in self._market_cap_rank_maps_by_date:
            if self._regime_conn is None:
                logger.warning(
                    "no regime_conn (normalised-schema DB) available; trade_log stock_rank will be blank for %s",
                    as_of_date,
                )
                self._market_cap_rank_maps_by_date[as_of_date] = {}
            else:
                try:
                    from config.universe import get_market_cap_rank_map_as_of

                    from backtest import shared_panels

                    # The rank MAP stays per-orchestrator (it ranks within
                    # this date's buy list, so it is not shareable across
                    # strategies). Only the PIT shares lookup underneath it is
                    # shared -- that IS per-ticker and identical for everyone.
                    self._market_cap_rank_maps_by_date[as_of_date] = get_market_cap_rank_map_as_of(
                        self._regime_conn, all_buy_tickers_this_date or [ticker], as_of_date,
                        shares_cache=shared_panels.pit_shares_cache(),
                    )
                except Exception:
                    logger.warning(
                        "PIT market cap rank map unavailable for %s; trade_log stock_rank will be blank",
                        as_of_date, exc_info=True,
                    )
                    self._market_cap_rank_maps_by_date[as_of_date] = {}
        return self._market_cap_rank_maps_by_date[as_of_date].get(ticker)

    def _reconcile_corporate_actions(
        self, portfolio: StrategyPortfolio, config: "OrchestratorConfig", as_of: date_type,
        data_gaps: List[DataGap],
    ) -> None:
        """Merger/spinoff swaps and delisting force-closes for one date.

        Extracted from run()'s loop (2026-08-13, STEP 3b) so the phase can be
        timed by wrapping ONE call rather than re-indenting sixty lines under a
        `with` block. That mattered: a diff that simultaneously moves code and
        changes its indentation is one no reviewer can check line-by-line, and
        this block force-closes positions — the last place to accept an
        unreviewable diff. Behaviour is unchanged; only the enclosing scope is.
        """
        # Corporate-action/delisting reconciliation BEFORE new sizing (Standard
        # Backtesting Algorithm step 3b, Truthful Review Gap #4) — always runs,
        # regardless of what the adapter's signals say this period.
        #
        # MERGER/SPINOFF checked FIRST, separately from delisting: a merged/
        # spun-off company may never appear in delisted_companies at all (it
        # didn't fail or get suspended, it stopped existing as a distinct
        # security for a completely different reason), so this must not be
        # folded into the is_delisted branch below.
        if config.corporate_action_lookup is not None:
            for ticker in list(portfolio.positions.keys()):
                event = config.corporate_action_lookup(ticker, as_of)
                if event is None or event.action_type not in ("MERGER", "SPINOFF"):
                    continue
                successor_price = (
                    config.price_lookup(event.successor_ticker, as_of)
                    if event.successor_ticker else None
                )
                if event.successor_ticker and event.swap_ratio and successor_price is not None:
                    # Real swap data available: close the original and open the
                    # successor position at the disclosed ratio — see
                    # CorporateActionEvent's docstring; unexercised with today's
                    # real data (no ingestion source populates these fields yet).
                    old_position = portfolio.positions.get(ticker)
                    quantity = old_position.quantity if old_position else 0
                    # swap_ratio = new shares received per 1 old share; the
                    # value received per old share is therefore
                    # successor_price * swap_ratio, not successor_price alone.
                    portfolio.force_close(ticker, successor_price * event.swap_ratio, as_of, reason=f"{event.action_type.lower()}_swap")
                    new_quantity = int(quantity * event.swap_ratio)
                    if new_quantity > 0:
                        portfolio.positions[event.successor_ticker] = Position(
                            ticker=event.successor_ticker,
                            sector=config.sector_lookup(event.successor_ticker),
                            quantity=new_quantity,
                            entry_price=successor_price,
                            entry_date=as_of,
                        )
                else:
                    # No real successor/ratio data (the only reachable case
                    # today) — the plan's documented "mark-to-last-price-and-
                    # close" policy: realize P&L at the last known real price,
                    # release capital, never fabricate a swap.
                    price = config.price_lookup(ticker, as_of)
                    if price is None:
                        data_gaps.append(DataGap(ticker, as_of, f"{event.action_type.lower()}_and_no_close_price"))
                        continue
                    portfolio.force_close(ticker, price, as_of, reason=f"{event.action_type.lower()}_forced_close")

        if config.is_delisted is not None:
            for ticker in list(portfolio.positions.keys()):
                if config.is_delisted(ticker, as_of):
                    price = config.price_lookup(ticker, as_of)
                    if price is None:
                        data_gaps.append(DataGap(ticker, as_of, "delisted_and_no_close_price"))
                        continue
                    portfolio.force_close(ticker, price, as_of, reason="forced_close")

    def _check_blackouts(
        self, portfolio: StrategyPortfolio, config: "OrchestratorConfig",
        prices: Dict[str, float], as_of: date_type,
        blackout_streak: Dict[str, int], last_seen_price: Dict[str, float],
        data_gaps: List[DataGap],
    ) -> None:
        """Force-close positions whose bars have stopped arriving.

        Previously a position whose data went missing was carried at its last
        known price for as long as the gap lasted — INDOTECH's 601-day trade
        spans a 209-calendar-day hole and was marked at whatever price appeared
        when data resumed. No stop, target or max-hold can fire on a day with
        no bar, so holding through a blackout is not a decision the strategy
        made; it is the absence of one.

        Called from BOTH the rebalance and non-rebalance branches of the daily
        loop. That is not incidental: the first version of this lived inline in
        the rebalance branch only, so the streak counted REBALANCE dates rather
        than trading days and a 21-day cadence turned a 5-session threshold
        into a 105-session one. A blackout is measured in trading days, so the
        check has to run on every trading day.

        Closes at the LAST KNOWN price deliberately. Closing at the price that
        appears when data resumes would book a return across a period nobody
        could have traded, which is the whole thing being removed.
        """
        if config.max_blackout_sessions is None:
            return
        for ticker in list(portfolio.positions.keys()):
            if ticker in prices:
                blackout_streak.pop(ticker, None)
                continue
            blackout_streak[ticker] = blackout_streak.get(ticker, 0) + 1
            if blackout_streak[ticker] <= config.max_blackout_sessions:
                continue
            last_price = last_seen_price.get(ticker)
            if last_price is None:
                # Never seen a real price for a position we hold. Recorded
                # rather than closed at a fabricated value.
                data_gaps.append(DataGap(ticker, as_of, "blackout_and_no_last_known_price"))
                continue
            portfolio.force_close(ticker, last_price, as_of, reason="data_blackout_forced_close")
            data_gaps.append(DataGap(
                ticker, as_of,
                f"data_blackout_forced_close_after_{blackout_streak[ticker]}_sessions",
            ))
            blackout_streak.pop(ticker, None)

    def run(self, run: BacktestRun, adapter: StrategyAdapter, config: OrchestratorConfig) -> BacktestRunResult:
        if run.channel != adapter.channel:
            raise ValueError(f"run.channel={run.channel!r} does not match adapter.channel={adapter.channel!r}")

        # STEP 3b instrumentation. On by default: one perf_counter pair per
        # phase against phases measured in milliseconds, and the reason this
        # refactor had no speed target is that no run has ever carried a
        # timing. Set config.collect_timings=False to opt out.
        timer = RunTimer() if config.collect_timings else NullTimer()

        sizing = sizing_for(run.horizon_bucket)
        cadence = config.rebalance_cadence_days or sizing.default_rebalance_cadence_days
        rebalance_dates = config.trading_days[::cadence]
        if len(rebalance_dates) == 0:
            raise ValueError("no rebalance dates in the supplied trading_days for this cadence")
        rebalance_date_set = set(rebalance_dates)

        sip = SipConfig(amount=run.sip_amount) if run.capital_mode == "sip" and run.sip_amount else None
        # capital_mode="annual_reset" (2026-08-12, the user's third measure) —
        # see AnnualResetConfig's docstring. None for every other capital_mode,
        # which keeps lump/sip on exactly their existing code path.
        annual_reset = None
        if run.capital_mode == "annual_reset":
            _rate = getattr(run, "annual_reset_ltcg_rate", None)
            _exempt = getattr(run, "annual_reset_ltcg_exemption", None)
            _label = getattr(run, "annual_reset_regime_label", None)
            # Fail loudly rather than defaulting. If this silently fell back to
            # one rate, the two LTCG-regime sweeps would produce byte-identical
            # results and look like a legitimate finding ("regime makes no
            # difference") instead of a plumbing bug. The regime is what makes
            # the two runs different, so an unspecified regime is never a
            # sensible default here.
            if _rate is None or _label is None:
                raise ValueError(
                    "capital_mode='annual_reset' requires annual_reset_ltcg_rate and "
                    "annual_reset_regime_label to be set explicitly — the LTCG regime "
                    "determines the FY withdrawal and therefore the trades taken, so it "
                    "cannot be defaulted (both regimes would come out identical)."
                )
            # top_up_after_loss defaults True, i.e. the original measure, so a
            # run that does not set it behaves exactly as before.
            annual_reset = AnnualResetConfig(
                base_capital=run.initial_capital,
                ltcg_rate=float(_rate),
                ltcg_exemption=float(_exempt or 0.0),
                regime_label=str(_label),
                top_up_after_loss=bool(getattr(run, "annual_reset_top_up_after_loss", True)),
            )
        # The adapter's top_n IS the slot count for a top-N strategy; an
        # explicit config value overrides it, and only an adapter with no
        # top_n at all falls through to StrategyPortfolio's own default.
        n_target = (
            config.n_target_positions
            or getattr(adapter, "top_n", None)
            or DEFAULT_N_TARGET_POSITIONS
        )
        portfolio = StrategyPortfolio(
            initial_capital=run.initial_capital, horizon_bucket=run.horizon_bucket, sip=sip,
            annual_reset=annual_reset,
            sizing_overrides=config.sizing_overrides,
            n_target_positions=int(n_target),
        )
        portfolio.prime_sip_schedule(config.trading_days)
        portfolio.prime_annual_reset_schedule(config.trading_days)
        portfolio.prime_tax_schedule(config.trading_days)

        data_gaps: List[DataGap] = []
        # {ticker: consecutive trading days with no bar} for open positions.
        blackout_streak: Dict[str, int] = {}
        # {ticker: most recent real close seen}. Position carries no such field
        # and price_lookup returns None precisely during a blackout — which is
        # what makes the blackout a blackout — so the last tradeable price has
        # to be remembered here or the force-close below can never fire.
        last_seen_price: Dict[str, float] = {}
        distinct_tickers: List[str] = []
        refit_log: List[RefitEvent] = []
        refit_dates = (
            set(config.trading_days[:: config.refit_cadence_days])
            if config.refit_cadence_days else set()
        )

        # A94 signal ledger. canonical_strategy_key can legitimately return
        # None (no channel, or a name containing a colon) — in that case the
        # run simply does not write signals, because a row filed under a
        # guessed key is worse than an absent row: it attributes decisions to
        # a strategy that may not be the one that made them.
        signal_recorder = None
        run_strategy_key = canonical_strategy_key(run)
        # Resolved once here, not per-signal: it is a registry read, and every
        # row of this run belongs to the same definition by construction.
        # An explicit config override wins (a caller replaying a historical
        # version knows better than the registry's current head).
        run_strategy_version = (
            config.signal_ledger_strategy_version
            if config.signal_ledger_strategy_version is not None
            else resolve_strategy_version(run_strategy_key)
        )
        # A103 readiness gate, as a RUN-START precondition rather than a
        # per-rebalance-date check. Per-date would mean ~860 DB round-trips
        # per job (times the parallel queue's concurrency) to re-answer a
        # question whose answer barely varies across a run — and the engine
        # already records genuine per-date holes as `data_gaps`.
        #
        # What this catches instead is the failure the per-date check would
        # mostly miss: a strategy gated on an indicator that was NEVER
        # backfilled over the window. The screener treats a NULL feature as
        # "condition unmet" rather than raising (see A81/A74), so such a run
        # completes, reports a plausible CAGR, and silently screened a
        # reduced universe the whole way. That result then drives a deploy
        # decision, which is what makes a wrong backtest as costly as a wrong
        # live signal.
        if config.enforce_readiness and run_strategy_key:
            _enforce_run_readiness(
                run=run, config=config, strategy_key=run_strategy_key,
                strategy_version=run_strategy_version,
            )

        if config.persist_signals:
            ledger_key = run_strategy_key
            if ledger_key:
                signal_recorder = SignalLedgerRecorder(
                    strategy_key=ledger_key,
                    source="backtest",
                    run_id=run.run_id,
                    strategy_version=run_strategy_version,
                    channel=run.channel,
                    db_path=config.signal_ledger_db_path,
                )
            else:
                logger.warning(
                    "persist_signals is on but this run has no canonical strategy_key "
                    "(channel=%r, strategy_id=%r) — skipping the signal ledger rather "
                    "than filing signals under a guessed key",
                    getattr(run, "channel", None), getattr(run, "strategy_id", None),
                )

        # Load regime data if configured
        if self._regime_type is not None:
            if config.trading_days:
                start_date = config.trading_days[0] if isinstance(config.trading_days[0], date_type) else config.trading_days[0].date()
                end_date = config.trading_days[-1] if isinstance(config.trading_days[-1], date_type) else config.trading_days[-1].date()
                self._load_regime_data(start_date, end_date)
                self._populate_regime_cache()

        for as_of_date in config.trading_days:
            as_of = as_of_date.date() if hasattr(as_of_date, "date") else as_of_date
            is_rebalance_date = as_of_date in rebalance_date_set
            # Tickers the rotation logic below already bought/sold THIS day —
            # excluded from the daily exit-policy pass so it never double-
            # handles a same-day rotation decision (e.g. re-evaluating a
            # ticker sold moments ago, or a brand-new buy with days_held=0).
            executed_tickers: set[str] = set()
            prices: Dict[str, float] = {}

            if not is_rebalance_date:
                # Non-rebalance day: no re-screening/re-sizing/corporate-
                # action reconciliation (those remain cadence-driven, per
                # the task's "rebalance cadence continues operating
                # unchanged" instruction) — just mark held positions to
                # market so the daily exit-policy pass below and the equity
                # curve both see today's real prices.
                for ticker in list(portfolio.positions.keys()):
                    price = config.price_lookup(ticker, as_of)
                    if price is not None:
                        prices[ticker] = price
                        last_seen_price[ticker] = price
                    else:
                        data_gaps.append(DataGap(ticker, as_of, "no_price_marking_open_position_at_last_known_price"))
                self._check_blackouts(
                    portfolio, config, prices, as_of, blackout_streak, last_seen_price, data_gaps,
                )
                # Skipped entirely under exit_policy_cadence="rebalance": for a
                # periodic strategy an exit between rebalances is a slot that
                # cannot be refilled until the next one (see the field's note).
                if config.exit_policy_cadence == "daily":
                    self._apply_exit_policy(portfolio, prices, as_of, executed_tickers)
                portfolio.record_equity(as_of, prices)
                # Feed portfolio equity to adapter if it supports overlays (Phase 7/8)
                if hasattr(adapter, "update_portfolio_equity"):
                    adapter.update_portfolio_equity(as_of, portfolio.equity_curve.iloc[-1])
                continue

            # Walk-Forward retraining (Phase 2.5, BacktestUmbrellaPlan.md "Walk-Forward
            # Module"): only called for adapters that implement an optional refit()
            # method — none of Phase 2's adapters need one today (their signals are
            # already point-in-time-pure recomputations, not fitted models), but this
            # is the hook a future ML-style adapter plugs a retrain step into.
            if as_of_date in refit_dates and hasattr(adapter, "refit"):
                model_version = adapter.refit(as_of)
                refit_log.append(RefitEvent(as_of_date=as_of, model_version=str(model_version)))

            # Tax before SIP and before this date's sizing, so the year's
            # first buys size against capital that has actually paid last
            # year's bill rather than against money already owed.
            portfolio.apply_due_fy_tax(as_of)
            portfolio.apply_due_sip_injections(as_of)

            with timer.phase("corp_actions"):
                self._reconcile_corporate_actions(portfolio, config, as_of, data_gaps)

            with timer.phase("universe"):
                universe = config.universe_provider(as_of)
            with timer.phase("signals"):
                signals = adapter.generate_signals(universe, as_of, run.horizon_bucket)
                # Apply regime exposure if configured
                signals = self.apply_regime_to_signals(signals, as_of)

            prices = {}
            with timer.phase("prices"):
                for ticker in set(list(portfolio.positions.keys()) + [s.ticker for s in signals]):
                    price = config.price_lookup(ticker, as_of)
                    if price is not None:
                        prices[ticker] = price
                        last_seen_price[ticker] = price
                    elif ticker in portfolio.positions:
                        data_gaps.append(
                            DataGap(ticker, as_of, "no_price_marking_open_position_at_last_known_price")
                        )

            # Annual-reset boundary handling runs BEFORE this date's sizing, so
            # the year's first buys size against the adjusted capital rather
            # than last year's. Needs prices (mark-to-market equity decides the
            # withdrawal), unlike the SIP call above. No-op unless
            # capital_mode="annual_reset".
            #
            # [BUG FIX 2026-08-18] This call used to sit above, next to
            # apply_due_sip_injections -- and on a rebalance date `prices` is
            # still the empty dict initialised at the top of the iteration,
            # because only the non-rebalance branch (which `continue`s) fills
            # it, and the rebalance branch's own price pass runs further down.
            # total_equity() falls back to `pos.entry_price` for any ticker
            # absent from the map, so the reset was deciding on COST BASIS
            # rather than mark-to-market equity -- the exact thing this call's
            # own comment says it needs prices for. A year of gains therefore
            # withdrew too little, and a losing year topped up too little.
            # Moved below the price pass; still before any sizing, which is
            # the ordering constraint that matters.
            portfolio.apply_due_annual_reset(as_of, prices)

            self._check_blackouts(
                portfolio, config, prices, as_of, blackout_streak, last_seen_price, data_gaps,
            )

            # One buffered batch per rebalance date, never per ticker. The
            # actual DB write happens once, after the loop (see flush below).
            if signal_recorder is not None:
                signal_recorder.record(as_of, signals)

            for signal in signals:
                self._log_feature(run.run_id, signal.ticker, as_of, run.horizon_bucket, adapter, signal.action)
                distinct_tickers.append(signal.ticker)
                executed_tickers.add(signal.ticker)

            # REV17 (2026-07-21 review): a signal generated at as_of used to
            # always fill at that SAME day's own close — overstating fill
            # quality, since the signal couldn't actually have been acted on
            # until that price was already known. execution_timing="same_day_close"
            # (default) preserves this exact prior behavior; "next_day_open"
            # fills at the NEXT trading day's price_lookup value instead (this
            # engine has one generic per-adapter price_lookup, not a separate
            # open/close pair, so "next_day_open" means "priced at the next
            # trading day", whatever convention that adapter's price_lookup
            # itself uses). Position-sizing equity valuation (`prices` above)
            # deliberately stays as_of-priced — sizing is a decision made with
            # information known at signal time, only the FILL is delayed.
            execution_date = as_of
            if config.execution_timing == "next_day_open":
                later_dates = config.trading_days[config.trading_days > as_of_date]
                if len(later_dates) > 0:
                    next_date = later_dates[0]
                    execution_date = next_date.date() if hasattr(next_date, "date") else next_date
                else:
                    data_gaps.append(
                        DataGap(
                            "__execution_timing__", as_of,
                            "next_day_open_unavailable_at_last_rebalance_fell_back_to_same_day_close",
                        )
                    )

            fill_prices: Dict[str, float] = {}
            if signals:
                fill_tickers = {s.ticker for s in signals}
                if execution_date == as_of:
                    fill_prices = {t: prices[t] for t in fill_tickers if t in prices}
                else:
                    for ticker in fill_tickers:
                        price = config.price_lookup(ticker, execution_date)
                        if price is not None:
                            fill_prices[ticker] = price

            # All tickers that will be bought at execution_date THIS rebalance
            # — passed to _get_market_cap_rank_for_date so the first buy of
            # the date computes (and caches) the whole day's PIT rank map in
            # one batched query, instead of one PIT query per ticker.
            buy_tickers_this_execution_date = [s.ticker for s in signals if s.action == "buy"]

            # sells before buys, so freed cash is available for the same rebalance's buys
            for signal in sorted((s for s in signals if s.action == "sell"), key=lambda s: -s.conviction):
                if signal.ticker not in fill_prices:
                    data_gaps.append(DataGap(signal.ticker, execution_date, "no_price_for_sell_signal"))
                    continue
                if config.circuit_locked_lookup is not None and config.circuit_locked_lookup(
                    signal.ticker, execution_date
                ):
                    # Recorded as a visible gap rather than skipped silently:
                    # a position we intended to exit and could not is a real
                    # risk the run must show, not an absence of a trade.
                    data_gaps.append(DataGap(signal.ticker, execution_date, "circuit_locked_sell_not_fillable"))
                    continue
                portfolio.sell(
                    signal.ticker, fill_prices[signal.ticker], execution_date, reason="signal", adtv_cr=signal.adtv_cr,
                )

            for signal in sorted((s for s in signals if s.action == "buy"), key=lambda s: -s.conviction):
                if signal.ticker not in fill_prices:
                    data_gaps.append(DataGap(signal.ticker, execution_date, "no_price_for_buy_signal"))
                    continue
                if config.circuit_locked_lookup is not None and config.circuit_locked_lookup(
                    signal.ticker, execution_date
                ):
                    data_gaps.append(DataGap(signal.ticker, execution_date, "circuit_locked_buy_not_fillable"))
                    continue
                if signal.adtv_cr is None:
                    # 2026-07-20 (Truthful Review Gap #6): core/portfolio.py's
                    # position_size() only enforces the ADTV hard cap when
                    # adtv_cr is provided — silently skipping it otherwise.
                    # Recording this as a visible data_gap (not just sizing
                    # uncapped without comment) so a channel/strategy that
                    # never populates Signal.adtv_cr shows up honestly in
                    # results instead of looking like the cap was checked
                    # and passed.
                    data_gaps.append(DataGap(signal.ticker, execution_date, "no_adtv_data_position_sized_uncapped"))
                elif signal.adtv_cr * 1e7 < MIN_ADT_INR:
                    # [BUG FIX, 6th fundamental-strategies review, item 3]
                    # StrategyPortfolio.can_buy now hard-rejects this trade
                    # (see core/portfolio.py) rather than only sizing it
                    # down — record it as a visible gap for audit-trail
                    # parity with the "uncapped" case above, so a reader can
                    # tell "excluded for being below the liquidity floor"
                    # apart from every other can_buy rejection reason.
                    data_gaps.append(DataGap(signal.ticker, execution_date, "skipped_illiquid_below_min_adt_floor"))
                    continue
                portfolio.buy(
                    signal.ticker, config.sector_lookup(signal.ticker), fill_prices[signal.ticker], execution_date,
                    prices, adtv_cr=signal.adtv_cr, template=signal.template, pillar=adapter.channel,
                    market_cap_rank=self._get_market_cap_rank_for_date(
                        signal.ticker, execution_date, buy_tickers_this_execution_date,
                    ),
                    # 2026-08-01 (Technical signal-failure analysis): snapshot
                    # the adapter's own entry-time feature_vector (screener
                    # match score/matched_conditions for Technical; other
                    # channels' feature_vector() may return less/nothing —
                    # never fabricated, just whatever the adapter itself
                    # already returns) onto the position so a losing trade
                    # can be inspected against its actual entry signal later.
                    entry_feature_vector=adapter.feature_vector(signal.ticker, execution_date),
                    # 2026-08-05: an adapter with a per-ticker weighting
                    # scheme (Momentum's volume_weighted sizing) scales its
                    # own slot here; None means "size normally", which is
                    # every other channel.
                    weight_multiplier=signal.size_multiplier if signal.size_multiplier is not None else 1.0,
                )

            # Daily exit-policy pass (task requirement: checked EVERY trading
            # day, not just rebalance dates) — runs on rebalance days too,
            # AFTER rotation's own sells/buys, skipping any ticker rotation
            # already touched today via executed_tickers.
            with timer.phase("exit_policy"):
                self._apply_exit_policy(portfolio, prices, as_of, executed_tickers)

            with timer.phase("equity"):
                portfolio.record_equity(as_of, prices)
                # Feed portfolio equity to adapter if it supports overlays (Phase 7/8)
                if hasattr(adapter, "update_portfolio_equity"):
                    adapter.update_portfolio_equity(as_of, portfolio.equity_curve.iloc[-1])

        if self._feature_log_writer is not None:
            self._feature_log_writer.flush()

        # A94: the run's single ledger write, deliberately next to the
        # feature-log flush so all of this job's DB work sits in one tail
        # rather than being spread across the run's rebalance dates.
        if signal_recorder is not None:
            # finalize() = flush + supersede: one set of backtest signals per
            # strategy per window, so a regenerated period does not leave two.
            signal_recorder.finalize()

        with timer.phase("finalize"):
            result = self._finalize(
                run, portfolio, data_gaps, distinct_tickers, config.trading_days, refit_log,
                execution_timing=config.execution_timing,
            )
        # Attached AFTER _finalize so the timing includes finalize itself. The
        # field is named phase_timings, never `execution_timing` — that name is
        # already taken by the fill-timing POLICY ("same_day_close" /
        # "next_day_open"), and its resemblance to instrumentation is precisely
        # why this project believed it had timings for months while having none.
        # {} when disabled, never a zero-filled dict. A phase reading
        # "0.000s, 0 calls" is indistinguishable from a phase that ran for free,
        # and "we measured nothing" must not look like "everything was instant"
        # — that ambiguity is a smaller version of the execution_timing
        # confusion this step exists to fix.
        result.phase_timings = timer.finish().as_dict() if config.collect_timings else {}
        return result

    def _build_benchmark_curve(
        self, start_date: date_type, end_date: date_type, starting_capital: float,
    ) -> Optional[pd.Series]:
        """Real buy-and-hold equity curve for self._benchmark_index_name over
        [start_date, end_date], normalised to `starting_capital` at the
        first real index bar in the window — the orchestrator's counterpart
        to BacktestEngine._build_benchmark_curve() (backtest/engine.py),
        which the ML walk-forward path has always had and this one lacked.

        Reads index_ohlcv through self._regime_conn (already open, read-only,
        pointing at the normalised-schema DB that hosts both market_regimes
        and index_ohlcv). Returns None — never a synthetic or interpolated
        stand-in — when there is no connection, no real rows for the window,
        or a non-positive entry price.
        """
        if self._regime_conn is None or starting_capital <= 0:
            return None
        try:
            rows = self._regime_conn.execute(
                """
                SELECT date, close FROM index_ohlcv
                WHERE index_name = ? AND date BETWEEN ? AND ? AND close > 0
                ORDER BY date
                """,
                [self._benchmark_index_name, start_date, end_date],
            ).fetchall()
        except Exception:
            logger.warning("benchmark curve unavailable; benchmark_cagr/excess_return stay unset", exc_info=True)
            return None
        if len(rows) < 2:
            return None
        entry_price = float(rows[0][1])
        if entry_price <= 0:
            return None
        shares = starting_capital / entry_price
        return pd.Series(
            [float(close) * shares for _, close in rows],
            index=pd.DatetimeIndex([pd.Timestamp(d) for d, _ in rows]),
        )

    def _regime_for_date(self, as_of: date_type) -> Optional[str]:
        """Bull/Bear/Sideways label (lowercase, systems.regime.market_regime.
        Regime's own convention) confirmed as of `as_of`, or None with no
        regime_conn / no segment covers that date. Segments are fetched
        once per orchestrator instance (self._regime_segments_cache) since
        run() calls this once per trading day per open position — refetching
        per call would be one DuckDB round-trip per day for no benefit
        (market_regimes doesn't change mid-run)."""
        if self._regime_conn is None:
            return None
        if self._regime_segments_cache is None:
            from systems.regime.market_regime import METHOD_NAME
            from systems.regime.regime_store import list_regime_segments

            try:
                # market_regimes stores multiple thresholds' segments side by
                # side (see datastore/api/routers/regime.py) — exit-policy
                # regime gating must use one fixed, deliberately-chosen
                # method, defaulting to the canonical 20% threshold but
                # overridable via self._regime_method for experimentation.
                self._regime_segments_cache = list_regime_segments(
                    self._regime_conn, self._regime_index_name,
                    method=self._regime_method or METHOD_NAME,
                )
            except Exception:
                logger.warning("regime segments unavailable; exit_ctx rows will have no `regime`", exc_info=True)
                self._regime_segments_cache = []
        from systems.regime.regime_store import regime_known_as_of

        label: Optional[str] = regime_known_as_of(self._regime_segments_cache, as_of)
        return label

    def _apply_exit_policy(
        self, portfolio: StrategyPortfolio, prices: Dict[str, float], as_of: date_type,
        executed_tickers: set[str],
    ) -> None:
        """Per-position stop-loss/target/urgency exit check, run every trading
        day (see run()'s docstring/comments) independent of rebalance-cadence
        rotation. Builds the same exit_ctx shape PerTemplateExitPolicy/
        RuleBasedExitPolicy expect (entry_price, days_held, unrealised_pnl_pct,
        drawdown_from_peak, atr_pct, momentum_3m, pnd_score, template, pillar)
        from real, already-known position/price data only — momentum_3m and
        pnd_score default to 0.0 (neutral/no-signal) since this orchestrator
        has no momentum-panel or pump-and-dump feature source wired in yet;
        atr_pct comes from Position.entry_atr_pct when a caller populated it,
        else NaN (RuleBasedExitPolicy's documented flat-percentage fallback).

        Binary action mapping (backtest/portfolio.py's
        PortfolioSimulator.exit_action_for_urgency, reused rather than
        reimplemented): only 'immediate_exit' triggers a real sell here —
        StrategyPortfolio has no partial-reduce operation, so 'reduce_position'
        cannot be honored and is treated the same as 'monitor'/'hold' (no
        portfolio action). Uses reason="exit_model_urgent" — already the
        recognized reason string StrategyPortfolio.sell() bypasses the
        horizon bucket's min_holding_days floor for (a real stop-loss must be
        able to fire before that floor elapses), and is distinct from
        rotation's reason="signal" / corporate-action reasons in trade logs.
        """
        candidate_tickers = [t for t in portfolio.positions if t not in executed_tickers]
        rows = []
        row_tickers = []
        # Computed once per call (one calendar day), not per ticker — the
        # regime doesn't vary by ticker, only by date.
        regime = self._regime_for_date(as_of)
        for ticker in candidate_tickers:
            price = prices.get(ticker)
            if price is None:
                continue  # no real price today for this ticker — never fabricate one to evaluate barriers
            position = portfolio.positions[ticker]
            if price > position.peak_price:
                position.peak_price = price
            days_held = (pd.Timestamp(as_of) - pd.Timestamp(position.entry_date)).days
            unrealised_pnl_pct = (price - position.entry_price) / position.entry_price if position.entry_price else 0.0
            drawdown_from_peak = (price - position.peak_price) / position.peak_price if position.peak_price else 0.0
            row = {
                "entry_price": position.entry_price,
                "price": price,
                "peak_price": position.peak_price,
                "days_held": days_held,
                "unrealised_pnl_pct": unrealised_pnl_pct,
                "drawdown_from_peak": drawdown_from_peak,
                "atr_pct": position.entry_atr_pct if position.entry_atr_pct is not None else float("nan"),
                "momentum_3m": 0.0,
                "pnd_score": 0.0,
                "template": position.template,
                "pillar": position.pillar,
                "regime": regime,
            }
            if self._technical_feature_lookup is not None:
                try:
                    row.update(self._technical_feature_lookup(ticker, as_of) or {})
                except Exception:
                    logger.warning(
                        "technical_feature_lookup failed for %s on %s; exit_ctx row has no live indicator "
                        "values (condition-based exit rules simply won't trigger for it)", ticker, as_of, exc_info=True,
                    )
            rows.append(row)
            row_tickers.append(ticker)

        if not rows:
            return

        exit_ctx = pd.DataFrame(rows, index=row_tickers)
        predictions = self._exit_model.predict_full(exit_ctx)

        # [STEP 4, 2026-08-13] Consume the policy's stated INTENT.
        #
        # This used to call PortfolioSimulator.exit_action_for_urgency — a
        # STATIC method on a class whose portfolio this loop does not use — to
        # re-derive an action from a score the policy had already decided from.
        # StrategyPortfolio has no partial-reduce operation, so every
        # 'reduce_position' fell out of the if/elif chain and did nothing at
        # all: no sell, no counter, no log line. The entire 60-80 urgency band
        # evaporated, which is also the band baseline's max-hold (<=65) and
        # momentum-exhaustion (<=79) triggers emitted into.
        if "exit_action" in predictions.columns:
            actions = predictions["exit_action"]
        else:
            # A policy still on the legacy contract. Counted, not assumed:
            # "which policies have not been migrated" must be a fact in the run
            # record rather than something a reader infers.
            self._legacy_urgency_fallbacks += len(row_tickers)
            actions = action_from_urgency(
                predictions["exit_urgency"],
                urgent_threshold=EXIT_URGENT_THRESHOLD,
                reduce_threshold=EXIT_REDUCE_THRESHOLD,
                monitor_threshold=MONITOR_THRESHOLD,
            )

        validate_actions(actions.unique())
        for ticker in row_tickers:
            action = actions.loc[ticker]
            if action == EXIT_ACTION_EXIT:
                portfolio.sell(ticker, prices[ticker], as_of, reason="exit_model_urgent")
            elif action == EXIT_ACTION_REDUCE:
                # Raises if the portfolio cannot do it, rather than resolving
                # to nothing. A partial exit that silently does not happen
                # produces a backtest of a strategy nobody ran.
                portfolio.reduce_position(
                    ticker, prices[ticker], as_of, reason="exit_model_reduce",
                )

    def _log_feature(
        self, run_id: str, ticker: str, as_of: date_type, horizon_bucket: HorizonBucket,
        adapter: StrategyAdapter, action: str,
    ) -> None:
        if self._feature_log_writer is None:
            return
        self._feature_log_writer.record(
            run_id=run_id, ticker=ticker, as_of_date=as_of, horizon_bucket=horizon_bucket,
            feature_vector=adapter.feature_vector(ticker, as_of), decision_taken=action,
        )

    def _write_trade_log(self, run_id: str, trades: List[Any]) -> Path:
        """Write one CSV row per closed trade for audit purposes.

        Always writes a file for every run (even zero-trade runs, in which
        case only the header row is written) so trade-level detail is never
        opt-in or lost.

        stock_rank is `t.entry_market_cap_rank` — a genuinely point-in-time
        market-cap rank (rank 1 = largest) as of the trade's actual
        execution/buy date, computed by
        _get_market_cap_rank_for_date()/config.universe.
        get_market_cap_rank_map_as_of(). This used to be a single static
        snapshot of the CURRENT universe CSV's market_cap_cr column (see the
        now-superseded get_market_cap_rank_map()), applied identically
        regardless of how long ago the trade actually happened — not a true
        historical rank. It is blank ("") when PIT fundamentals/price data
        wasn't available for that ticker/date, never a fallback to that
        static snapshot.

        pnl_inr/exit_reason (2026-07-24 addition) are read straight off the
        already-computed Trade record (backtest/portfolio.py) — this method
        only writes MORE of what StrategyPortfolio._close() already
        recorded; it does not change what trades get made or how. Added so
        backtest/export_trade_book.py can build a full trade book (entry/
        exit reason + P&L) without re-deriving pnl or guessing which exit
        condition fired.

        regime/regime_exposure/nifty_rsi_14/nifty_ema_5/nifty_ema_10 (Phase 2 Trade Logging)
        are looked up from _regime_cache by entry_date if regime_type is configured.
        Blank ("") if regime data is not available for that entry_date.
        """
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        csv_path = REPORTS_DIR / f"trade_log_{run_id}.csv"
        with open(csv_path, "w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow([
                "ticker", "qty", "buy_date", "buy_price", "sale_date", "sale_price", "stock_rank",
                "pnl_inr", "pnl_pct", "exit_reason", "regime", "regime_exposure", "nifty_rsi_14", "nifty_ema_5", "nifty_ema_10",
            ])
            for t in trades:
                # Lookup regime for trade entry date
                entry_date_obj = t.entry_date if isinstance(t.entry_date, date_type) else t.entry_date
                regime_data = self._regime_cache.get(entry_date_obj)
                regime_str = regime_data.regime if regime_data else ""
                regime_exp = regime_data.exposure if regime_data else ""
                rsi = regime_data.rsi_14 if regime_data else ""
                ema5 = regime_data.ema_5 if regime_data else ""
                ema10 = regime_data.ema_10 if regime_data else ""

                writer.writerow([
                    t.ticker,
                    t.quantity,
                    t.entry_date,
                    t.entry_price,
                    t.exit_date,
                    t.exit_price,
                    t.entry_market_cap_rank if t.entry_market_cap_rank is not None else "",
                    t.pnl_inr,
                    t.pnl_pct,
                    t.exit_reason,
                    regime_str,
                    regime_exp,
                    rsi,
                    ema5,
                    ema10,
                ])
        return csv_path

    def _finalize(
        self, run: BacktestRun, portfolio: StrategyPortfolio, data_gaps: List[DataGap],
        distinct_tickers: List[str], trading_days: pd.DatetimeIndex, refit_log: Optional[List[RefitEvent]] = None,
        execution_timing: str = "same_day_close",
    ) -> BacktestRunResult:
        # [STEP 5, 2026-08-13] Tax is now paid in cash at each FY boundary
        # (StrategyPortfolio.apply_due_fy_tax), so it is already inside
        # portfolio.cash_flows and already out of the equity curve. Re-deriving
        # it here and subtracting again would charge it twice.
        #
        # The only amount left to settle is any liability the book could not
        # fund from cash — a near-fully-invested portfolio can owe more at
        # 31 March than it holds. That balance is real and unpaid, so it comes
        # off the closing value.
        equity_curve = portfolio.equity_curve

        # [FIX 2026-08-19] Two defects lived in this block, and between them
        # they made "pre-tax" and "post-tax" incomparable on the report:
        #
        # 1. A pre-tax run had its ENTIRE cumulative FY tax docked off the
        #    final equity point. Over a 17-year, ~58-trades-a-year book that
        #    lump dwarfs any one year's bill, so the run labelled `pre_tax`
        #    reported a CAGR *below* the post-tax run of the same strategy
        #    (4.9%/yr against 19.0%/yr). A pre-tax run is now genuinely
        #    pre-tax; its post-tax counterpart comes from compute_metrics'
        #    apply_tax_to_curve, which charges each FY on the day it fell due.
        # 2. Tax was fed to XIRR as an investor cash flow on BOTH paths, with
        #    opposite signs. Tax is an expense of the book, not a receipt of
        #    the investor's, and on the post-tax path it is already out of the
        #    equity curve — counting it again credited the investor with money
        #    that went to the taxman. XIRR now sees contributions, real
        #    withdrawals and the terminal value, on whatever basis the curve is.
        cash_flows = [
            (cf["date"], cf["amount"])
            for cf in portfolio.cash_flows
            if cf.get("kind") != "tax"
        ]
        if portfolio.deduct_tax_annually and portfolio.annual_reset is None:
            # The only amount left to settle is a liability the book could not
            # fund from cash — a near-fully-invested portfolio can owe more at
            # 31 March than it holds. That balance is real and unpaid, so it
            # comes off the closing value.
            outstanding = portfolio.deferred_tax_liability
            tax_ledger = list(portfolio.tax_ledger)
        else:
            # annual_reset nets tax out of its own withdrawal, so it owes
            # nothing further. A plain pre-tax run owes nothing either — it
            # simply never paid, which is what pre-tax MEANS.
            outstanding = 0.0
            # Synthesised so the pre-tax run can still report the other basis
            # and a total tax figure: same {fy_end, paid} shape the post-tax
            # path's real ledger uses, so compute_metrics needs no branch.
            tax_ledger = (
                [] if portfolio.annual_reset is not None
                else [
                    {"fy_end": d.isoformat(), "assessed": -amt, "paid": -amt, "deferred": 0.0}
                    for d, amt in fy_tax_cash_flows(portfolio.tax_transactions())
                ]
            )
        if len(equity_curve) and outstanding:
            equity_curve = equity_curve.copy()
            equity_curve.iloc[-1] = equity_curve.iloc[-1] - outstanding

        # STEP 5 gate. Recorded on every run, never only logged: an invariant
        # whose result can be discarded silently is not a gate. These are
        # accounting identities, so a violation is a bug in the simulation
        # rather than a poor result — and each defect this refactor found was
        # invisible in the metrics while breaking one of them.
        ledger_violations = [str(v) for v in check_all(portfolio)]
        if ledger_violations:
            logger.error(
                "ledger invariant violation(s) in run %s: %s",
                run.run_id, "; ".join(ledger_violations),
            )

        trade_pnls = [t.pnl_inr for t in portfolio.trades]
        trade_values = [t.entry_price * t.quantity for t in portfolio.trades]
        holding_days = [
            (pd.Timestamp(t.exit_date) - pd.Timestamp(t.entry_date)).days for t in portfolio.trades
        ]
        start_date = trading_days[0].date() if hasattr(trading_days[0], "date") else trading_days[0]
        end_date = trading_days[-1].date() if hasattr(trading_days[-1], "date") else trading_days[-1]
        run_end_ts = pd.Timestamp(trading_days[-1])
        open_holding_days = [
            (run_end_ts - pd.Timestamp(pos.entry_date)).days for pos in portfolio.positions.values()
        ]

        # [BUG FIX 2026-08-08] The orchestrator never built a benchmark curve
        # at all, so compute_metrics defaulted benchmark_equity_curve=None and
        # EVERY orchestrator run (technical/fundamental/momentum) reported
        # benchmark_cagr/excess_return as null — the Backtest page has never
        # shown an index comparison for this channel. Real Nifty 500 history
        # lives in index_ohlcv in the same normalised-schema DB self._regime_conn
        # already points at, so this needs no new connection or data source.
        # Buy-and-hold, normalised to the run's own starting capital, so the
        # two curves are directly comparable. None (no regime_conn, or no real
        # index rows for the window) leaves the metrics null exactly as before
        # — never a synthetic benchmark (CLAUDE.md Absolute Rule 6).
        benchmark_curve = self._build_benchmark_curve(
            start_date, end_date, float(equity_curve.iloc[0]) if len(equity_curve) else 0.0
        )

        metrics = compute_metrics(
            equity_curve=equity_curve, cash_flows=cash_flows, trade_pnls=trade_pnls,
            trade_values=trade_values, distinct_tickers=distinct_tickers,
            benchmark_equity_curve=benchmark_curve,
            start_date=start_date, end_date=end_date, total_contributed=portfolio.total_contributed,
            cash_position_series=portfolio.cash_position_series,
            holding_days=holding_days,
            # 2026-08-01 Momentum-parity additions — see compute_metrics'
            # docstring. pnl_pct is already net-of-cost (fraction -> %).
            trade_returns_pct=[t.pnl_pct * 100 for t in portfolio.trades],
            n_open_positions=len(portfolio.positions),
            holding_days_all=holding_days + open_holding_days,
            benchmark_index_name=self._benchmark_index_name,
            # A86: both tax bases from one execution. The ledger says what was
            # actually paid and when; deduct_tax_annually says which basis the
            # headline CAGR is therefore stated on.
            tax_ledger=tax_ledger,
            deduct_tax_annually=bool(getattr(portfolio, "deduct_tax_annually", False))
            and getattr(portfolio, "annual_reset", None) is None,
        )

        from systems.regime.market_regime import METHOD_NAME

        regime_breakdown: List[Dict[str, Any]] = []
        segments: List[Dict[str, Any]] = []
        if self._regime_conn is not None:
            from dataclasses import asdict as _asdict

            from systems.regime.regime_store import list_regime_segments

            segments = list_regime_segments(
                self._regime_conn, self._regime_index_name, start_date=start_date, end_date=end_date,
                method=self._regime_method or METHOD_NAME,
            )
            regime_breakdown = [
                _asdict(row)
                for row in compute_regime_breakdown(equity_curve, portfolio.trades, start_date, end_date, segments)
            ]

        # 2026-07-26 (REV1/REV4/REV6 wiring): post-run integrity checks fed
        # REAL derived inputs (this run's own trades/data_gaps/regime
        # segments) — see backtest/core/post_run_checks.py's module
        # docstring for the model-review-corrected design (regime-segment
        # sub-periods, not calendar slices; check_10 skipped for rule-based
        # channels). None/{} (unchanged prior behavior) if this raises —
        # a bug in this NEW wiring must never fail an otherwise-successful
        # run's save.
        integrity_passed: Optional[bool] = None
        integrity_detail: Dict[str, Any] = {}
        try:
            from backtest.core.post_run_checks import run_post_run_integrity

            integrity_passed, integrity_detail = run_post_run_integrity(
                channel=run.channel, trades=portfolio.trades,
                data_gaps=[{"reason": g.reason} for g in data_gaps],
                equity_curve=equity_curve, run_start=start_date, run_end=end_date,
                regime_segments=segments, regime_conn=self._regime_conn,
                regime_index_name=self._regime_index_name,
                regime_method=self._regime_method or METHOD_NAME,
            )
        except Exception:
            logger.warning("post-run integrity checks failed to run for %s; leaving unset", run.run_id, exc_info=True)

        trade_log_path = self._write_trade_log(run.run_id, portfolio.trades)

        # Dominant-regime convenience label: only populated when one regime
        # holds a strict majority of this run's n_days — otherwise NULL,
        # since regime_breakdown (already carried in full) is the source of
        # truth for ambiguous/mixed runs and we don't want to fabricate a
        # misleading single label for them.
        regime_label: Optional[str] = None
        total_days = sum(row["n_days"] for row in regime_breakdown)
        if total_days > 0:
            dominant = max(regime_breakdown, key=lambda row: row["n_days"])
            if dominant["n_days"] / total_days > 0.5:
                regime_label = dominant["regime"]

        from dataclasses import asdict
        return BacktestRunResult(
            run=run,
            metrics=asdict(metrics),
            integrity_passed=integrity_passed,
            integrity_detail=integrity_detail,
            data_gaps=[{"ticker": g.ticker, "as_of_date": g.as_of_date.isoformat(), "reason": g.reason} for g in data_gaps],
            refit_log=[{"as_of_date": r.as_of_date.isoformat(), "model_version": r.model_version} for r in (refit_log or [])],
            execution_timing=execution_timing,
            regime_breakdown=regime_breakdown,
            # capital_mode="annual_reset" only; empty list for lump/sip. This is
            # measure 3's actual deliverable — without carrying it out of the
            # portfolio here, an annual_reset run silently produces no ledger.
            fy_ledger=list(portfolio.fy_ledger),
            # STEP 5: per-FY tax actually paid, and any balance the book could
            # not fund from cash. Carried out of the portfolio for the same
            # reason fy_ledger is — a ledger the run computes and then discards
            # is a ledger nobody can check.
            tax_ledger=list(portfolio.tax_ledger),
            ledger_violations=ledger_violations,
            # A90: the benchmark's path, in the same shape and on the same
            # dates as equity_curve below. It was computed to derive excess
            # return and then dropped, so a report could draw the strategy's
            # line but never the one it is judged against.
            benchmark_curve=_curve_to_rows(benchmark_curve),
            # A89: the identity every other surface already uses. Built from
            # the run's own channel and strategy_id rather than re-derived
            # downstream, so the engine, the registry, the API and the UI
            # cannot disagree about what to call the same strategy.
            strategy_key=canonical_strategy_key(run),
            exit_policy_variant=self._exit_policy_variant,
            regime_label=regime_label,
            trade_log_path=str(trade_log_path),
            # Post-tax equity curve (the same series compute_metrics scored),
            # so a rolling-window return computed downstream matches the
            # run's own reported CAGR/drawdown rather than a parallel series.
            equity_curve=[
                {"date": ts.date().isoformat(), "equity": float(value)}
                for ts, value in equity_curve.items()
            ],
        )
