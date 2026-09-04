"""
ResultsReader - loads standardized results from results/runs/ and builds
comparison tables across strategies/bands.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from momentum_framework.results.writer import RESULTS_DIR


class ResultsReader:
    """Reads standardized result files and builds comparison DataFrames."""

    def __init__(self, results_dir: Path = RESULTS_DIR):
        self.results_dir = results_dir

    def load_all(self, pattern: str = "*.json") -> List[Dict[str, Any]]:
        """Load every result file matching pattern into memory."""
        results = []
        for path in sorted(self.results_dir.glob(pattern)):
            with open(path) as f:
                results.append(json.load(f))
        return results

    def load_by_strategy(self, strategy_code: str) -> List[Dict[str, Any]]:
        """Load all results for a given strategy code (e.g. 'R01')."""
        return self.load_all(pattern=f"*_{strategy_code}_*.json")

    def to_dataframe(self, results: Optional[List[Dict[str, Any]]] = None) -> pd.DataFrame:
        """
        Flatten results into a comparison table: one row per run, with
        strategy_id, config fields, and standard metrics as columns.
        """
        results = results if results is not None else self.load_all()

        rows = []
        for r in results:
            row = {
                "strategy_id": r["metadata"]["strategy_id"],
                "run_id": r["metadata"]["run_id"],
                "result_date": r["metadata"]["result_date"],
                "integrity_passed": r["integrity"]["passed"],
            }
            row.update(r.get("config", {}))
            row.update(r.get("metrics", {}))
            rows.append(row)

        return pd.DataFrame(rows)

    def compare_strategies(self, strategy_codes: List[str],
                            sort_by: str = "sharpe_ratio") -> pd.DataFrame:
        """Build a side-by-side comparison table across multiple strategies."""
        all_results = []
        for code in strategy_codes:
            all_results.extend(self.load_by_strategy(code))

        df = self.to_dataframe(all_results)
        if sort_by in df.columns:
            df = df.sort_values(sort_by, ascending=False)
        return df

    def best_by_band(self, strategy_code: str, metric: str = "sharpe_ratio") -> pd.DataFrame:
        """For a given strategy, find the best-performing config per M-band."""
        df = self.to_dataframe(self.load_by_strategy(strategy_code))
        if df.empty or "rank_band_id" not in df.columns:
            return df
        idx = df.groupby("rank_band_id")[metric].idxmax()
        return df.loc[idx].sort_values("rank_band_id")
