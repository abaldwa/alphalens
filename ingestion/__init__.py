"""
ingestion package.

Phase: 0.1 (Project Skeleton)
Specs: SPEC-DS-001, SPEC-PIPE-001, SPEC-QUALITY-002, SPEC-QUALITY-003
Owner: Platform / DataStore
Consumers: datastore, scheduler, observability

Data ingestion pipeline: raw data acquisition, normalization, quality checks.
Sources: NSE bhavcopy, Fyers broker API, company fundamentals, external data.
Subpackages: scheduler (job orchestration), scrapers (data sources),
adjust (splits/dividends), quality (validation/drift detection).
"""
