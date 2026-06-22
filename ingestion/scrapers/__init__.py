"""
ingestion.scrapers package.

Phase: 0.1 (Project Skeleton)
Specs: SPEC-PIPE-001, SPEC-PIPE-006, SPEC-DS-001, SPEC-QUALITY-002
Owner: Platform / Ingestion
Consumers: datastore raw layer

Data scrapers and API clients for raw data acquisition.
Sources: NSE bhavcopy (OHLCV), Fyers broker (intraday), fundamental scrapers, external APIs.
SOLID: Each scraper implements a common interface for uniform error handling.
"""
