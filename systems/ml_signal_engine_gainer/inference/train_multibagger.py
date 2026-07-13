"""
systems/ml_signal_engine_gainer/inference/train_multibagger.py

GAINER EXPERIMENT (copy of systems/ml_signal_engine/inference/train_multibagger.py,
not used by production, does not touch production models/registry).

Trains 3 independent MultibaggerModel instances — 2x/12mo, 3x/24mo,
5x/36mo — using the parametrized (return_multiplier, window_years) and
run-level dedup added in the gainer copy of multibagger_model.py. Each
variant is saved under its own key to a SEPARATE models directory/registry
(datastore/models/_gainer_experiment/) so this never overwrites or
interferes with the production `multibagger` model or datastore/models/registry.json.
"""

import argparse
import json
import logging
from typing import Dict, List, NamedTuple

from config.settings import MODELS_DIR
from config.timezone import now_ist
from systems.ml_signal_engine_gainer.models.multibagger.multibagger_model import (
    MultibaggerModel,
    load_multibagger_training_data_from_db,
)

logger = logging.getLogger(__name__)

GAINER_MODELS_DIR = MODELS_DIR / "_gainer_experiment"
MODEL_VERSION_DATE_FORMAT = "%Y%m%d"


class MultibaggerTarget(NamedTuple):
    name: str
    return_multiplier: float
    window_years: float
    label_window_days: int  # forward-runway requirement for censored rows


# 12/24/36-month variants requested — note these are SHORTER windows than
# production's single 2x/3yr model, so label_window_days (used only to
# filter out censored rows too close to the end of history) scales down too.
MULTIBAGGER_TARGETS: List[MultibaggerTarget] = [
    MultibaggerTarget("multibagger_2x_12m", return_multiplier=2.0, window_years=1.0, label_window_days=252),
    MultibaggerTarget("multibagger_3x_24m", return_multiplier=3.0, window_years=2.0, label_window_days=504),
    MultibaggerTarget("multibagger_5x_36m", return_multiplier=5.0, window_years=3.0, label_window_days=756),
]


def _save_model_gainer(model, name: str, run_date, registry: Dict, metadata_extra=None):
    """Same {name}_v{YYYYMMDD}_fold0.pkl + `_current.pkl` convention as production's
    _save_model, but rooted under GAINER_MODELS_DIR so nothing production reads/writes is touched."""
    model_dir = GAINER_MODELS_DIR / name
    model_dir.mkdir(parents=True, exist_ok=True)
    version = run_date.strftime(MODEL_VERSION_DATE_FORMAT)
    versioned_path = model_dir / f"{name}_v{version}_fold0.pkl"
    current_path = model_dir / f"{name}_current.pkl"

    model.save(str(versioned_path))
    model.save(str(current_path))

    meta = model.metadata() if hasattr(model, "metadata") else {}
    meta = {**meta, **(metadata_extra or {})}
    meta["saved_path"] = str(versioned_path)
    meta["saved_at"] = run_date.isoformat()
    registry[name] = meta
    logger.info(f"Saved {name} -> {versioned_path}")
    return versioned_path


def train_multibagger_variant(
    target: MultibaggerTarget,
    lookback_days: int = 1260,
    snapshot_stride_days: int = 5,
    cooldown_days: int = 15,
    n_estimators: int = 200,
    save: bool = True,
    seed: int = 42,
    tickers: list = None,
) -> Dict:
    """Train one (return_multiplier, window_years) multibagger variant end-to-end."""
    run_date = now_ist()
    logger.info(
        "Multibagger[%s] retrain starting: %.1fx in %.1fy, lookback=%d days, "
        "label_window=%d days, snapshot_stride=%d days, cooldown=%d days",
        target.name, target.return_multiplier, target.window_years, lookback_days,
        target.label_window_days, snapshot_stride_days, cooldown_days,
    )

    X, y, duration_months, event, groups, _pnd_scores = load_multibagger_training_data_from_db(
        lookback_days=lookback_days,
        label_window_days=target.label_window_days,
        min_return_multiplier=target.return_multiplier,
        label_window_years=target.window_years,
        snapshot_stride_days=snapshot_stride_days,
        cooldown_days=cooldown_days,
        tickers=tickers,
        pipeline_name=target.name,
    )

    model = MultibaggerModel(random_state=seed, n_estimators=n_estimators)
    diagnostics = model.train_full(X, y, duration_months, event, groups=groups)
    logger.info(
        "Multibagger[%s] trained: %d samples, positive_rate=%.4f, event_rate=%.4f, rsf_concordance=%.4f",
        target.name, diagnostics["training_samples"], diagnostics["positive_rate"],
        diagnostics["event_rate"], diagnostics["rsf_concordance_index"],
    )

    registry: Dict = {}
    if save:
        _save_model_gainer(
            model, target.name, run_date, registry,
            metadata_extra={
                "diagnostics": diagnostics,
                "return_multiplier": target.return_multiplier,
                "window_years": target.window_years,
            },
        )
        _write_gainer_registry(registry)

    return {"registry": registry, "diagnostics": diagnostics}


def _write_gainer_registry(registry: Dict) -> None:
    GAINER_MODELS_DIR.mkdir(parents=True, exist_ok=True)
    registry_path = GAINER_MODELS_DIR / "registry.json"
    existing = {}
    if registry_path.exists():
        try:
            existing = json.loads(registry_path.read_text())
        except (json.JSONDecodeError, OSError):
            existing = {}
    existing.update({k: (v if not hasattr(v, "isoformat") else v.isoformat()) for k, v in registry.items()})
    registry_path.write_text(json.dumps(existing, indent=2, default=str))
    logger.info("Updated gainer-experiment model registry: %s", registry_path)


def train_all_multibagger_variants(
    lookback_days: int = 1260,
    snapshot_stride_days: int = 5,
    cooldown_days: int = 15,
    n_estimators: int = 200,
    save: bool = True,
    seed: int = 42,
    tickers: list = None,
) -> Dict[str, Dict]:
    """Train all 3 multibagger variants (2x/12mo, 3x/24mo, 5x/36mo)."""
    results = {}
    for target in MULTIBAGGER_TARGETS:
        results[target.name] = train_multibagger_variant(
            target, lookback_days=lookback_days, snapshot_stride_days=snapshot_stride_days,
            cooldown_days=cooldown_days, n_estimators=n_estimators, save=save, seed=seed, tickers=tickers,
        )
    return results


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="[GAINER EXPERIMENT] Train 2x/12mo, 3x/24mo, 5x/36mo multibagger variants")
    parser.add_argument("--lookback-days", type=int, default=1260)
    parser.add_argument("--snapshot-stride-days", type=int, default=5)
    parser.add_argument("--cooldown-days", type=int, default=15)
    parser.add_argument("--n-estimators", type=int, default=200)
    parser.add_argument("--tickers", type=str, default=None, help="Comma-separated ticker subset for a quick smoke run")
    args = parser.parse_args()

    tickers = args.tickers.split(",") if args.tickers else None
    results = train_all_multibagger_variants(
        lookback_days=args.lookback_days, snapshot_stride_days=args.snapshot_stride_days,
        cooldown_days=args.cooldown_days, n_estimators=args.n_estimators, tickers=tickers,
    )
    for name, result in results.items():
        diag = result["diagnostics"]
        print(
            f"\n{name}: {diag['training_samples']} samples, "
            f"positive_rate={diag['positive_rate']:.4f}, event_rate={diag['event_rate']:.4f}, "
            f"rsf_concordance={diag['rsf_concordance_index']:.4f}"
        )


if __name__ == "__main__":
    main()
