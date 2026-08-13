"""
strategies/

The declarative strategy layer (A92-A95). Strategy definitions, filters and
generated signals live in DuckDB tables rather than in Python modules, so that
backtest, API and frontend all read the same rows and a deployed strategy
cannot silently diverge from the one that was backtested.

See AGENTS.md "Architectural invariants" for the rules, and
datastore/schema/create_strategy_registry.py for the schema.
"""
