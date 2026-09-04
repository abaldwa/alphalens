"""
Results Management

Standardized reading/writing of backtest results using the naming
convention from metrics/nomenclature.py. This is the "different table"
the user asked to post reruns into — separate from the legacy
backtest/reports/ directory so old and new results never collide.
"""

from momentum_framework.results.writer import ResultsWriter
from momentum_framework.results.reader import ResultsReader
from momentum_framework.metrics.nomenclature import build_result_filename as ResultsNomenclature

__all__ = ["ResultsWriter", "ResultsReader", "ResultsNomenclature"]
