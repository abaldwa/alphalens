#!/usr/bin/env python3
"""
Small helper to exchange a FYERS auth_code (or redirected URL) for a
daily access token and cache it in the project's token cache.

Usage:
  FYERS_AUTH_EXCHANGE='<code_or_url>' python3 scripts/update_fyers_auth.py
  or
  python3 scripts/update_fyers_auth.py <code_or_url>
"""
import os
import sys

from ingestion.scrapers.fyers_backfill import FYERSBackfill


def main(argv: list[str]) -> int:
    code = os.environ.get("FYERS_AUTH_EXCHANGE")
    if not code and len(argv) > 1:
        code = argv[1]
    if not code:
        print("Provide the auth_code (or full redirect URL) via FYERS_AUTH_EXCHANGE env var or as the first argument.")
        return 2

    fb = FYERSBackfill(non_interactive=True)
    try:
        token = fb.exchange_auth_code(code)
    except Exception as exc:
        print(f"Failed to exchange auth code: {exc}")
        return 1

    print("Successfully exchanged auth code. New token cached to disk.")
    print(token)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
