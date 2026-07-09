"""
datastore/staging

Phase: A25 (Write-Audit-Publish Architecture)
Specs: SPEC-QUALITY-002 (flag, don't silently drop/write)
Owner: Platform / DataStore

Landing -> staging -> publish pipeline for DuckDB ingestion. See
datastore/staging/gate.py, publish.py, snapshot.py.
"""
