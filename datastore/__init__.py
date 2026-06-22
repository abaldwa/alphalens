"""
datastore package.

Phase: 0.2 (DataStore Schema & API Shell)
Specs: SPEC-DS-001 through SPEC-DS-007
Owner: Platform / DataStore
Consumers: systems/*, backtest, ingestion/*, dashboard

The central data layer (six stores) and the FastAPI/httpx access pattern
around it. See datastore/api (server) and datastore/client.py (consumer-side
httpx client, SPEC-DS-002).
"""
