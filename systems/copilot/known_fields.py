"""
systems/copilot/known_fields.py

Real, already-computed feature/column names Co-Pilot is allowed to build
conditions against. Anything the LLM asks for that isn't in one of these
sets is rejected into StrategySpec.unresolved rather than silently
accepted — this is the concrete mechanism behind "be upfront about
missing data" (Absolute Rule 6).

Deliberately imports only the catalogs, not the feature-computation
modules themselves, to stay lightweight.
"""

from features.advanced_technical import ADVANCED_TECHNICAL_FEATURES
from features.pattern_scores import PATTERN_FEATURES
from features.technical import CORE_TECHNICAL_FEATURES

TECHNICAL_FIELDS = frozenset(
    [*CORE_TECHNICAL_FEATURES, *ADVANCED_TECHNICAL_FEATURES, *PATTERN_FEATURES]
)

# Mirrors datastore/api/schemas.py::FundamentalsWrite's numeric fields —
# the real, PIT-disclosed fundamentals columns available for screening.
FUNDAMENTAL_FIELDS = frozenset(
    [
        "revenue", "ebitda", "ebit", "pat", "eps", "operating_margin",
        "ebitda_margin", "net_margin", "roe", "roce", "debt_to_equity",
        "interest_coverage", "fcf", "asset_turnover", "inventory_days",
        "receivable_days", "payable_days", "book_value_per_share",
        "gross_profit", "capex", "current_assets", "current_liabilities",
        "total_debt", "cash_and_equivalents", "depreciation",
        "total_equity", "retained_earnings", "total_assets", "cwip",
        "goodwill", "inventories", "trade_receivables_current",
        "trade_payables_current", "total_liabilities",
    ]
)

# Mirrors systems/damodaran_valuation/valuation_engine.py::ValuationResult's
# real, computed output fields.
VALUATION_FIELDS = frozenset(
    [
        "intrinsic_value", "current_price", "valuation_gap_pct",
        "margin_of_safety", "wacc", "cost_of_equity", "terminal_value_pct",
        "scenario_bull", "scenario_base", "scenario_bear",
    ]
)


def resolve_section(section: str) -> frozenset:
    return {
        "technical": TECHNICAL_FIELDS,
        "fundamental": FUNDAMENTAL_FIELDS,
        "valuation": VALUATION_FIELDS,
    }[section]
