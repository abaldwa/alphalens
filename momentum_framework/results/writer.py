"""
ResultsWriter - persists a BacktestResult using the standard nomenclature,
into results/runs/ (the "different table" for framework reruns, kept
separate from the legacy backtest/reports/ directory).
"""

import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional

from momentum_framework.backtesting.result import BacktestResult
from momentum_framework.metrics.nomenclature import build_result_filename

RESULTS_DIR = Path(__file__).resolve().parent / "runs"


def _config_hash(config: Dict[str, Any]) -> str:
    """Short, stable hash of a config dict — disambiguates same-strategy_id
    runs that differ in a parameter not captured by the id itself."""
    serialized = json.dumps(config, sort_keys=True, default=str)
    return hashlib.sha1(serialized.encode()).hexdigest()[:8]


class ResultsWriter:
    """Writes BacktestResult objects to results/runs/ with standard naming."""

    def __init__(self, output_dir: Path = RESULTS_DIR):
        self.output_dir = output_dir

    def write(self, result: BacktestResult, result_date: Optional[str] = None) -> Path:
        result_date = result_date or date.today().isoformat()
        config_hash = _config_hash(result.config)
        filename = build_result_filename(result.strategy_id, result_date, config_hash)

        payload = {
            "metadata": {
                "result_date": result_date,
                "framework_version": result.framework_version,
                "strategy_id": result.strategy_id,
                "run_id": result.run_id,
                "source_commit": result.source_commit,
                "config_hash": config_hash,
            },
            "config": result.config,
            "metrics": result.metrics,
            "integrity": {
                "passed": result.integrity_passed,
                "detail": result.integrity_detail,
            },
            "data_gaps": result.data_gaps,
            "trade_log_path": result.trade_log_path,
            "trade_count": result.trade_count,
        }

        self.output_dir.mkdir(parents=True, exist_ok=True)
        out_path = self.output_dir / filename
        out_path.write_text(json.dumps(payload, indent=2, default=str))
        return out_path
